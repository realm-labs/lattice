use std::{collections::BTreeSet, sync::Arc};

use lattice_model::{
    cluster::{ClusterId, CoordinatorScope, NodeEndpoint, NodeIncarnation},
    run::ControlOperationId,
};
use lattice_remoting::{
    association::AssociationManager, config::RemotingConfig, control::CommandId,
};
use tokio::sync::oneshot;

use super::{CoordinatorHost, CoordinatorHostConfig};
use crate::{
    candidate_fixture::elect_host,
    control::{
        InboundPlacementControl, PlacementControlCommand, PlacementControlEvent,
        PlacementControlEventKind,
    },
    coordinator::{ClusterLeaderGuard, MemberHello},
    shutdown::{ClusterShutdown, ClusterShutdownStore},
    storage::{ClusterLifecycleStore, InMemoryCoordinationStore, MembershipStore, StorageError},
    types::{MembershipVersion, NodeKey, Revision},
};

#[tokio::test]
async fn closing_acknowledges_queued_membership_commands_without_admission_or_stop_proof() {
    let store = Arc::new(InMemoryCoordinationStore::new(8, 8).unwrap());
    let node = |name: &str, port, incarnation| NodeKey {
        node_id: name.to_owned(),
        address: NodeEndpoint::new("127.0.0.1", port).unwrap(),
        incarnation: NodeIncarnation::new(incarnation).unwrap(),
    };
    let local = node("leader", 34831, 1);
    let member = node("member", 34832, 2);
    let associations = Arc::new(
        AssociationManager::new(
            local.address.clone(),
            local.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let peer = associations
        .get_or_create(
            ClusterId::new("closing-queue").unwrap(),
            member.address.clone(),
            member.incarnation,
        )
        .unwrap();
    let mut host = elect_host(
        store.clone(),
        associations.clone(),
        local.clone(),
        BTreeSet::new(),
        CoordinatorHostConfig::default(),
    )
    .await
    .unwrap();
    let hello = MemberHello {
        node: member.clone(),
        roles: Default::default(),
        failure_domains: Default::default(),
        protocols: Vec::new(),
        remoting_capabilities: Default::default(),
    };
    let record = host
        .membership
        .as_mut()
        .unwrap()
        .join(hello.clone())
        .await
        .unwrap();
    host.pending_member_hellos.insert(member.incarnation, hello);
    host.membership_associations
        .insert(member.incarnation, peer.key().clone());
    let leader = host.membership.as_ref().unwrap().leader().clone();
    let driver = ClusterShutdown::new(
        store.clone(),
        ClusterLeaderGuard::new(leader.clone()).unwrap(),
    );
    let operation = ControlOperationId::new("closing-queue").unwrap();
    store
        .begin_shutdown(
            &ClusterLeaderGuard::new(leader.clone()).unwrap(),
            operation.clone(),
        )
        .await
        .unwrap();
    host.lifecycle_events
        .send_replace(store.lifecycle().await.unwrap());
    for command in [
        PlacementControlCommand::NodeHeartbeat {
            incarnation: member.incarnation,
            sequence: 1,
        },
        PlacementControlCommand::JoinReady {
            snapshot_version: MembershipVersion::new(leader.term, Revision::new(1).unwrap()),
        },
    ] {
        let (completion, completed) = oneshot::channel();
        host.route_control(PlacementControlEvent {
            kind: PlacementControlEventKind::Command(Box::new(InboundPlacementControl {
                association: peer.key().clone(),
                scope: CoordinatorScope::Cluster,
                coordinator_term: Some(leader.term.get()),
                command_id: CommandId::generate(),
                command,
            })),
            completion,
        })
        .await;
        assert!(completed.await.unwrap().is_ok());
    }
    assert_eq!(
        store.get_member(&member.node_id).await.unwrap(),
        Some(record)
    );
    assert!(matches!(
        driver.advance(&operation).await,
        Err(StorageError::ShutdownBlocked)
    ));

    // A cold Closed observer does not register or campaign. The actual terminal
    // bootstrap codec/query is covered by the service's no-leader receipt test.
    store
        .record_node_stopped(&ClusterLeaderGuard::new(leader).unwrap(), &member)
        .await
        .unwrap();
    while !driver.advance(&operation).await.unwrap().complete {}
    let observer = CoordinatorHost::elect(
        store.clone(),
        associations,
        local,
        BTreeSet::new(),
        CoordinatorHostConfig::default(),
    )
    .await
    .unwrap();
    assert!(observer.membership.is_none());
    assert_eq!(
        *observer.subscribe_lifecycle().borrow(),
        store.lifecycle().await.unwrap()
    );
}
