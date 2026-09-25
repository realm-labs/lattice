use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use lattice_discovery::static_provider::{StaticDiscovery, StaticEndpoint};
use lattice_model::{
    cluster::CoordinatorScope,
    cluster::{ClusterId, NodeEndpoint, NodeIncarnation},
};
use lattice_remoting::{
    association::{AssociationManager, AssociationState},
    bootstrap::BootstrapLeader,
    config::RemotingConfig,
    endpoint::RemotingEndpoint,
    handshake::NodeIdentity,
    messaging::{
        error::RemoteMessageError, inbound::InboundDispatch, outbound::OutboundMessaging,
        target::ExactActorTarget,
    },
};
use tokio::sync::watch;

use super::{
    BootstrapView, JoinController, JoinError, JoinEvent, leadership_replaced, select_leader,
};
use crate::{
    config::ClusterJoinConfig,
    test_support::{network_test_guard, unused_address},
};

mod discovery;

fn leader(node: &str, term: u64, incarnation: u64) -> BootstrapLeader {
    BootstrapLeader {
        scope: CoordinatorScope::Cluster,
        identity: NodeIdentity {
            cluster_id: ClusterId::new("cluster").unwrap(),
            node_id: node.to_string(),
            address: NodeEndpoint::new(node, 7447).unwrap(),
            incarnation: NodeIncarnation::new(u128::from(incarnation)).unwrap(),
        },
        term,
    }
}

#[test]
fn selects_highest_term() {
    assert_eq!(
        select_leader(vec![leader("a", 1, 9), leader("b", 2, 1)])
            .unwrap()
            .identity
            .node_id,
        "b"
    );
}

#[test]
fn rejects_different_leaders_in_same_term() {
    assert!(matches!(
        select_leader(vec![leader("a", 2, 1), leader("b", 2, 2)]),
        Err(JoinError::ConflictingLeaders)
    ));
}

#[test]
fn active_association_is_reconciled_when_its_leadership_term_changes() {
    let current = leader("a", 1, 1);
    assert!(leadership_replaced(&current, &leader("a", 2, 1)));
    assert!(leadership_replaced(&current, &leader("b", 2, 1)));
    assert!(!leadership_replaced(&current, &current));
    assert!(!leadership_replaced(&current, &leader("a", 0, 1)));
}

struct RejectDispatch;

#[async_trait]
impl InboundDispatch for RejectDispatch {
    async fn tell(
        &self,
        _target: ExactActorTarget,
        _message_id: u64,
        _payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }

    async fn ask(
        &self,
        _target: ExactActorTarget,
        _message_id: u64,
        _payload: Bytes,
        _deadline: std::time::Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }
}

fn endpoint(identity: NodeIdentity) -> (Arc<RemotingEndpoint>, Arc<AssociationManager>) {
    let config = RemotingConfig {
        heartbeat_interval: Duration::from_millis(50),
        ..RemotingConfig::default()
    };
    let associations = Arc::new(
        AssociationManager::new(
            identity.address.clone(),
            identity.incarnation,
            config.clone(),
        )
        .unwrap(),
    );
    let endpoint = Arc::new(
        RemotingEndpoint::builder(
            identity,
            config,
            associations.clone(),
            Arc::new(OutboundMessaging::new(16).unwrap()),
            Arc::new(RejectDispatch),
        )
        .build()
        .unwrap(),
    );
    (endpoint, associations)
}

#[tokio::test]
async fn refreshes_leadership_while_the_transport_association_stays_active() {
    let _network = network_test_guard().await;
    let first = unused_address().await;
    let second = unused_address().await;
    let (client_address, server_address) = if first < second {
        (first, second)
    } else {
        (second, first)
    };
    let cluster_id = ClusterId::new("join-refresh-test").unwrap();
    let client_identity = NodeIdentity {
        cluster_id: cluster_id.clone(),
        node_id: "client".to_owned(),
        address: client_address,
        incarnation: NodeIncarnation::new(1).unwrap(),
    };
    let server_identity = NodeIdentity {
        cluster_id,
        node_id: "coordinator".to_owned(),
        address: server_address.clone(),
        incarnation: NodeIncarnation::new(2).unwrap(),
    };
    let (client, client_associations) = endpoint(client_identity);
    let (server, _) = endpoint(server_identity.clone());
    let view = Arc::new(BootstrapView::new(server_identity.clone()));
    let current = BootstrapLeader {
        scope: CoordinatorScope::Cluster,
        identity: server_identity.clone(),
        term: 1,
    };
    view.install(current.clone());
    server.install_bootstrap_handler(view.clone());
    client.bind().await.unwrap();
    server.bind().await.unwrap();

    let discovery = Arc::new(
        StaticDiscovery::new(
            CoordinatorScope::Cluster,
            "join-refresh",
            vec![StaticEndpoint {
                address: server_address,
                expected_node_id: Some(server_identity.node_id.clone()),
                priority: 1,
            }],
        )
        .unwrap(),
    );
    let controller = Arc::new(
        JoinController::new(
            discovery,
            client,
            client_associations,
            ClusterJoinConfig {
                retry_initial: Duration::from_millis(10),
                retry_max: Duration::from_millis(20),
                retry_jitter: 0.0,
                leadership_refresh_interval: Duration::from_millis(25),
                discovery_stale_grace: Duration::from_millis(100),
                join_timeout: Some(Duration::from_secs(2)),
                ..ClusterJoinConfig::default()
            },
        )
        .unwrap(),
    );
    let (events_tx, mut events) = tokio::sync::mpsc::channel(8);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(controller.run(events_tx, shutdown_rx));

    let initial_association = match tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .unwrap()
        .unwrap()
    {
        JoinEvent::Coordinator {
            leader,
            association,
        } => {
            assert_eq!(leader.term, 1);
            association
        }
        event => panic!("unexpected initial join event: {event:?}"),
    };
    view.install(BootstrapLeader { term: 2, ..current });
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .unwrap(),
        Some(JoinEvent::CoordinatorLost { leader }) if leader.term == 1
    ));
    match tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .unwrap()
        .unwrap()
    {
        JoinEvent::Coordinator {
            leader,
            association,
        } => {
            assert_eq!(leader.term, 2);
            assert!(Arc::ptr_eq(&initial_association, &association));
        }
        event => panic!("unexpected refreshed join event: {event:?}"),
    }
    view.clear(&CoordinatorScope::Cluster);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .unwrap(),
        Some(JoinEvent::CoordinatorLost { leader }) if leader.term == 2
    ));
    assert_eq!(initial_association.state(), AssociationState::Active);
    shutdown.send_replace(true);
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
}
