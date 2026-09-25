use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::{Stream, stream};
use lattice_discovery::provider::{
    CoordinatorDirectorySnapshot, CoordinatorDiscovery, DiscoveryError, DiscoveryOrigin,
    DiscoverySource, DiscoveryTarget,
};
use lattice_model::cluster::{ClusterId, CoordinatorScope, NodeEndpoint, NodeIncarnation};
use lattice_remoting::{
    bootstrap::{BootstrapHandler, BootstrapLeader, BootstrapRequest, BootstrapRoute},
    handshake::NodeIdentity,
};
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use super::super::{
    BootstrapView, JoinController, JoinError, JoinEvent, RetryBackoff, probe_snapshot,
    wait_for_retry,
};
use super::{endpoint, network_test_guard, unused_address};
use crate::config::ClusterJoinConfig;

struct CountingView {
    view: BootstrapView,
    probes: AtomicUsize,
}

impl BootstrapHandler for CountingView {
    fn route(&self, request: &BootstrapRequest) -> BootstrapRoute {
        self.probes.fetch_add(1, Ordering::SeqCst);
        self.view.route(request)
    }
}

fn target(identity: &NodeIdentity) -> DiscoveryTarget {
    DiscoveryTarget {
        address: identity.address.clone(),
        expected_node_id: Some(identity.node_id.clone()),
        source: DiscoverySource::single(DiscoveryOrigin::Static {
            name: "test".into(),
        }),
        priority: 0,
    }
}

#[tokio::test]
async fn probes_hint_first_and_falls_back_when_it_no_longer_knows_a_leader() {
    let _network = network_test_guard().await;
    let cluster = ClusterId::new("hint-first-test").unwrap();
    let client_identity = NodeIdentity {
        cluster_id: cluster.clone(),
        node_id: "client".into(),
        address: unused_address().await,
        incarnation: NodeIncarnation::new(1).unwrap(),
    };
    let (client, _) = endpoint(client_identity);
    client.bind().await.unwrap();
    let hint_identity = NodeIdentity {
        cluster_id: cluster.clone(),
        node_id: "hint".into(),
        address: unused_address().await,
        incarnation: NodeIncarnation::new(2).unwrap(),
    };
    let (hint, _) = endpoint(hint_identity.clone());
    let hint_view = Arc::new(CountingView {
        view: BootstrapView::new(hint_identity.clone()),
        probes: AtomicUsize::new(0),
    });
    let old_leader = BootstrapLeader {
        scope: CoordinatorScope::Cluster,
        identity: hint_identity.clone(),
        term: 1,
    };
    hint_view.view.install(old_leader.clone());
    hint.install_bootstrap_handler(hint_view.clone());
    hint.bind().await.unwrap();
    let seed_identity = NodeIdentity {
        cluster_id: cluster,
        node_id: "seed".into(),
        address: unused_address().await,
        incarnation: NodeIncarnation::new(3).unwrap(),
    };
    let (seed, _) = endpoint(seed_identity.clone());
    let seed_view = Arc::new(CountingView {
        view: BootstrapView::new(seed_identity.clone()),
        probes: AtomicUsize::new(0),
    });
    let new_leader = BootstrapLeader {
        scope: CoordinatorScope::Cluster,
        identity: seed_identity.clone(),
        term: 2,
    };
    seed_view.view.install(new_leader.clone());
    seed.install_bootstrap_handler(seed_view.clone());
    seed.bind().await.unwrap();

    let snapshot = CoordinatorDirectorySnapshot {
        scope: CoordinatorScope::Cluster,
        generation: 1,
        leader_hint: Some(target(&hint_identity)),
        targets: vec![target(&hint_identity), target(&seed_identity)],
    };
    assert_eq!(
        probe_snapshot(&client, snapshot.clone(), 2).await.unwrap(),
        old_leader
    );
    assert_eq!(seed_view.probes.load(Ordering::SeqCst), 0);
    hint_view.view.clear(&CoordinatorScope::Cluster);
    assert_eq!(
        probe_snapshot(&client, snapshot, 2).await.unwrap(),
        new_leader
    );
    assert_eq!(
        hint_view.probes.load(Ordering::SeqCst),
        2,
        "duplicate hint is not retried as a seed"
    );
    assert_eq!(seed_view.probes.load(Ordering::SeqCst), 1);
    client.shutdown().await.unwrap();
    hint.shutdown().await.unwrap();
    seed.shutdown().await.unwrap();
}

struct SilentDiscovery;

#[tokio::test(start_paused = true)]
async fn discovery_updates_do_not_skip_or_extend_the_retry_deadline() {
    let mut updates = stream::iter((1..=100).map(|generation| {
        Ok(CoordinatorDirectorySnapshot {
            scope: CoordinatorScope::Cluster,
            generation,
            leader_hint: None,
            targets: Vec::new(),
        })
    }));
    let mut latest = None;
    let mut closed = false;
    let (_shutdown, mut shutdown) = watch::channel(false);
    let retry_at = Instant::now() + Duration::from_secs(5);
    assert!(
        wait_for_retry(
            retry_at,
            &mut updates,
            &mut latest,
            &mut closed,
            &mut shutdown
        )
        .await
    );
    assert_eq!(Instant::now(), retry_at);
    assert_eq!(latest.unwrap().generation, 100);
    assert!(closed);
}

#[test]
fn retry_delay_is_bounded_and_reset_does_not_restart_the_jitter_sequence() {
    let config = ClusterJoinConfig {
        retry_initial: Duration::from_millis(10),
        retry_max: Duration::from_millis(20),
        retry_multiplier: f64::MAX,
        retry_jitter: 0.9,
        ..ClusterJoinConfig::default()
    };
    let mut backoff = RetryBackoff::new(config.clone());
    backoff.sequence = 10;
    for _ in 0..100 {
        assert!(backoff.next_delay() <= config.retry_max);
    }
    let sequence = backoff.sequence;
    backoff.reset();
    assert_eq!(backoff.sequence, sequence);
    assert_eq!(backoff.current, config.retry_initial);
}

impl CoordinatorDiscovery for SilentDiscovery {
    fn scope(&self) -> &CoordinatorScope {
        &CoordinatorScope::Cluster
    }
    fn snapshots(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<CoordinatorDirectorySnapshot, DiscoveryError>> + Send + '_>>
    {
        Box::pin(stream::pending())
    }
}

#[tokio::test(start_paused = true)]
async fn join_deadline_covers_a_provider_that_never_emits() {
    let identity = NodeIdentity {
        cluster_id: ClusterId::new("silent-provider").unwrap(),
        node_id: "client".into(),
        address: NodeEndpoint::new("127.0.0.1", 7447).unwrap(),
        incarnation: NodeIncarnation::new(1).unwrap(),
    };
    let (endpoint, associations) = endpoint(identity);
    let controller = Arc::new(
        JoinController::new(
            Arc::new(SilentDiscovery),
            endpoint,
            associations,
            ClusterJoinConfig {
                join_timeout: Some(Duration::from_secs(2)),
                ..ClusterJoinConfig::default()
            },
        )
        .unwrap(),
    );
    let (events, mut received) = mpsc::channel(1);
    let (_shutdown, shutdown) = watch::channel(false);
    tokio::time::timeout(Duration::from_secs(3), controller.run(events, shutdown))
        .await
        .unwrap();
    assert!(matches!(
        received.recv().await,
        Some(JoinEvent::TerminalFailure(JoinError::JoinTimeout))
    ));
}

#[tokio::test]
async fn shutdown_cancels_a_provider_that_never_emits() {
    let identity = NodeIdentity {
        cluster_id: ClusterId::new("silent-cancel").unwrap(),
        node_id: "client".into(),
        address: NodeEndpoint::new("127.0.0.1", 7447).unwrap(),
        incarnation: NodeIncarnation::new(1).unwrap(),
    };
    let (endpoint, associations) = endpoint(identity);
    let controller = Arc::new(
        JoinController::new(
            Arc::new(SilentDiscovery),
            endpoint,
            associations,
            ClusterJoinConfig::default(),
        )
        .unwrap(),
    );
    let (events, _received) = mpsc::channel(1);
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(controller.run(events, receiver));
    tokio::task::yield_now().await;
    shutdown.send_replace(true);
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
}
