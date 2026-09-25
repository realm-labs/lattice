use super::support::node_config;
use crate::{
    builder::LatticeService,
    config::ClusterJoinConfig,
    error::ServiceError,
    lifecycle::NodeLifecycleState,
    shutdown::ClusterStopHook,
    test_support::{network_test_guard, unused_address},
};
use async_trait::async_trait;
use lattice_coordination::{
    administration::{AdminPermissions, AdministratorAllowlist},
    cluster_session::ShutdownRequestError,
    control::{DEFAULT_MAX_CONTROL_PAYLOAD, PlacementControlRouter},
    runtime::host::{CoordinatorHost, CoordinatorHostConfig},
    shutdown::ShutdownRequestRejection,
    storage::{
        ClusterLifecycleStore, CoordinatorLeaseStore, InMemoryCoordinationStore,
        candidates::provision_candidate,
    },
    types::NodeKey,
};
use lattice_discovery::static_provider::{StaticDiscovery, StaticEndpoint};
use lattice_model::{
    cluster::{ClusterId, CoordinatorScope, NodeIncarnation},
    run::{ControlOperationId, RunCompletion, RunPhase},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;
#[cfg(feature = "tls")]
pub(super) mod tls;

struct StopHook {
    allow: AtomicBool,
    calls: AtomicUsize,
}
#[async_trait]
impl ClusterStopHook for StopHook {
    async fn stop(&self) -> Result<(), String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.allow.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err("application work still running".to_owned())
        }
    }
}

#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_stop_waits_for_application_evidence_and_keeps_control_alive() {
    shutdown_request_contract(true).await;
}

#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_stop_returns_explicit_authorization_rejection() {
    shutdown_request_contract(false).await;
}

#[cfg(feature = "tls")]
async fn shutdown_request_contract(authorized: bool) {
    let _network = network_test_guard().await;
    let cluster = ClusterId::new("cluster-stop").unwrap();
    let coordinator_address = unused_address().await;
    let coordinator_node = NodeKey {
        node_id: "coordinator".to_owned(),
        address: coordinator_address.clone(),
        incarnation: NodeIncarnation::new(81).unwrap(),
    };
    let member_address = unused_address().await;
    let coordinator_identity = lattice_remoting::handshake::NodeIdentity {
        cluster_id: cluster.clone(),
        node_id: "coordinator".to_owned(),
        address: coordinator_address.clone(),
        incarnation: coordinator_node.incarnation,
    };
    let member_identity = lattice_remoting::handshake::NodeIdentity {
        cluster_id: cluster.clone(),
        node_id: "member".to_owned(),
        address: member_address.clone(),
        incarnation: NodeIncarnation::new(82).unwrap(),
    };
    let (coordinator_security, member_security) =
        tls::pair(&coordinator_identity, &member_identity);
    let administrators = Arc::new(AdministratorAllowlist::new(
        cluster.clone(),
        BTreeMap::from([(
            "member".to_owned(),
            AdminPermissions {
                shutdown_cluster: authorized,
                ..AdminPermissions::default()
            },
        )]),
    ));
    let store = Arc::new(InMemoryCoordinationStore::new(16, 16).unwrap());
    store.ensure_framework().await.unwrap();
    provision_candidate(
        store.as_ref(),
        CoordinatorScope::Cluster,
        "coordinator".to_owned(),
    )
    .await
    .unwrap();
    let builder = LatticeService::builder(node_config(
        cluster.clone(),
        "coordinator",
        coordinator_address.clone(),
        coordinator_node.incarnation,
    ))
    .unwrap()
    .endpoint_security(coordinator_security);
    let host = CoordinatorHost::elect(
        store.clone(),
        builder.association_manager(),
        coordinator_node,
        BTreeSet::new(),
        CoordinatorHostConfig {
            administrators: Some((*administrators).clone()),
            renewal_interval: Duration::from_millis(50),
            election_interval: Duration::from_millis(50),
            maximum_candidate_jitter: Duration::ZERO,
            ..CoordinatorHostConfig::default()
        },
    )
    .await
    .unwrap();
    let (dispatch, controls) =
        PlacementControlRouter::bounded(64, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
    let coordinator = builder
        .coordinator_host(Arc::new(dispatch), host, controls)
        .build()
        .unwrap();
    coordinator.start().await.unwrap();
    let hook = Arc::new(StopHook {
        allow: AtomicBool::new(false),
        calls: AtomicUsize::new(0),
    });
    let member = LatticeService::builder(node_config(
        cluster,
        "member",
        member_address,
        NodeIncarnation::new(82).unwrap(),
    ))
    .unwrap()
    .endpoint_security(member_security)
    .cluster_stop_hook(hook.clone())
    .coordinator_discovery(Arc::new(
        StaticDiscovery::new(
            CoordinatorScope::Cluster,
            "seed",
            vec![StaticEndpoint {
                address: coordinator_address,
                expected_node_id: Some("coordinator".to_owned()),
                priority: 0,
            }],
        )
        .unwrap(),
    ))
    .unwrap()
    .join_config(ClusterJoinConfig {
        retry_initial: Duration::from_millis(10),
        retry_max: Duration::from_millis(100),
        join_timeout: Some(Duration::from_secs(5)),
        shutdown_timeout: Duration::from_secs(1),
        leave_timeout: Duration::from_secs(1),
        ..ClusterJoinConfig::default()
    })
    .build()
    .unwrap();
    member.start().await.unwrap();
    tokio::time::timeout(Duration::from_secs(8), async {
        let mut state = member.subscribe_node_lifecycle();
        while *state.borrow() != NodeLifecycleState::Ready {
            state.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    let acceptance = member
        .request_cluster_shutdown(ControlOperationId::new("stop-test").unwrap())
        .await;
    if !authorized {
        assert!(matches!(
            acceptance,
            Err(ServiceError::ClusterShutdownRequest(
                ShutdownRequestError::Rejected(ShutdownRequestRejection::Unauthorized)
            ))
        ));
        assert!(store.lifecycle().await.unwrap().is_running());
        assert_eq!(hook.calls.load(Ordering::SeqCst), 0);
        member.force_shutdown().await.unwrap();
        coordinator.force_shutdown().await.unwrap();
        return;
    }
    let epoch = acceptance.unwrap();
    assert!(matches!(
        store.lifecycle().await.unwrap().phase,
        RunPhase::Closing { .. }
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        while hook.calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        store.lifecycle().await.unwrap().phase,
        RunPhase::Closing { .. }
    ));
    assert_eq!(member.node_lifecycle_state(), NodeLifecycleState::Draining);
    assert!(matches!(
        member
            .wait_cluster_shutdown(epoch, Instant::now() + Duration::from_millis(20))
            .await,
        Err(ServiceError::ClusterShutdownTimeout)
    ));
    assert!(matches!(
        store.lifecycle().await.unwrap().phase,
        RunPhase::Closing { .. }
    ));
    assert_eq!(
        member
            .request_cluster_shutdown(ControlOperationId::new("concurrent-stop").unwrap())
            .await
            .unwrap(),
        epoch,
    );
    assert!(matches!(member.cluster_shutdown_status().unwrap().phase,
        RunPhase::Closing { operation, .. } if operation.as_str() == "stop-test"));
    hook.allow.store(true, Ordering::SeqCst);
    let closed = member
        .wait_cluster_shutdown(epoch, Instant::now() + Duration::from_secs(8))
        .await
        .unwrap();
    assert!(matches!(
        closed.phase,
        RunPhase::Closed {
            completion: RunCompletion::Graceful,
            ..
        }
    ));
    member.force_shutdown().await.unwrap();
    coordinator.force_shutdown().await.unwrap();
}
