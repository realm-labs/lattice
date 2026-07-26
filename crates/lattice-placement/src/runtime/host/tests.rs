use std::{collections::BTreeSet, sync::Arc, time::Duration};

use lattice_core::{
    actor_ref::{NodeAddress, NodeIncarnation, PlacementDomainId},
    coordinator::CoordinatorScope,
};
use lattice_remoting::{
    association::{AssociationKey, AssociationManager},
    config::RemotingConfig,
    control::{CommandId, ControlDispatchError},
};
use tokio::sync::{mpsc, oneshot};

use super::{
    CoordinatorHost, CoordinatorHostConfig, CoordinatorHostScopeState, CoordinatorRuntimeError,
    MembershipLeaderConfig, PlacementDomainLeaderConfig, helpers::dispatch_error,
};
use crate::{
    control::{
        InboundPlacementControl, PlacementControlCommand, PlacementControlEvent,
        PlacementControlEventKind,
    },
    coordinator::{MemberChange, MemberEvent, MemberRemovalReason},
    storage::InMemoryPlacementStore,
    types::{MembershipVersion, NodeKey},
};

fn node(id: &str, incarnation: u128, port: u16) -> NodeKey {
    NodeKey {
        node_id: id.to_owned(),
        address: NodeAddress::new("127.0.0.1", port).unwrap(),
        incarnation: NodeIncarnation::new(incarnation).unwrap(),
    }
}

fn associations(node: &NodeKey) -> Arc<AssociationManager> {
    Arc::new(
        AssociationManager::new(
            node.address.clone(),
            node.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    )
}

fn config() -> CoordinatorHostConfig {
    CoordinatorHostConfig {
        membership: MembershipLeaderConfig {
            leader_lease_ttl: Duration::from_millis(500),
            member_lease_ttl: Duration::from_millis(500),
            renewal_interval: Duration::from_millis(50),
            ..MembershipLeaderConfig::default()
        },
        placement: PlacementDomainLeaderConfig {
            leader_lease_ttl: Duration::from_millis(500),
            member_lease_ttl: Duration::from_millis(500),
            claim_ttl: Duration::from_millis(500),
            renewal_interval: Duration::from_millis(50),
            ..PlacementDomainLeaderConfig::default()
        },
        renewal_interval: Duration::from_millis(50),
        ..CoordinatorHostConfig::default()
    }
}

#[test]
fn unknown_membership_session_is_acknowledged_as_stale_control() {
    assert_eq!(
        dispatch_error(CoordinatorRuntimeError::UnknownSession),
        ControlDispatchError::InvalidCommand
    );
}

#[tokio::test]
async fn membership_removal_reaches_domains_from_the_event_stream() {
    let store = Arc::new(InMemoryPlacementStore::new(32, 32).unwrap());
    let local = node("removal-host", 50, 33050);
    let departed = node("departed", 51, 33051);
    let domain = PlacementDomainId::new("removal-domain").unwrap();
    let mut host = CoordinatorHost::elect(
        store,
        associations(&local),
        local,
        BTreeSet::from([domain.clone()]),
        CoordinatorHostConfig {
            member_reconciliation_interval: Duration::from_secs(3600),
            ..config()
        },
    )
    .await
    .unwrap();
    let (sender, mut receiver) = mpsc::channel(4);
    host.domains.get_mut(&domain).unwrap().sender = Some(sender);
    let expected = departed.clone();
    let observer = tokio::spawn(async move {
        let event = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("domain receives the removal without the periodic scan")
            .expect("removal event is delivered");
        let matched = matches!(
            &event.kind,
            PlacementControlEventKind::GlobalMemberRemoved { node, reason }
                if node == &expected && *reason == MemberRemovalReason::FailureDetected
        );
        let _ = event.completion.send(Ok(()));
        matched
    });

    host.apply_membership_event(MemberEvent {
        version: MembershipVersion::new(
            crate::types::CoordinatorTerm::new(1).unwrap(),
            crate::types::Revision::new(2).unwrap(),
        ),
        change: MemberChange::Removed {
            node: departed,
            reason: MemberRemovalReason::FailureDetected,
        },
    })
    .await
    .unwrap();

    assert!(observer.await.unwrap());
}

#[tokio::test]
async fn stale_coordinator_term_is_fenced_before_membership_dispatch() {
    let store = Arc::new(InMemoryPlacementStore::new(32, 32).unwrap());
    let local = node("membership-host", 20, 33020);
    let remote = node("member", 21, 33021);
    let manager = associations(&local);
    let mut host = CoordinatorHost::elect(store, manager, local.clone(), BTreeSet::new(), config())
        .await
        .unwrap();
    let active_term = host
        .active_term(&CoordinatorScope::Membership)
        .expect("membership leader is active");
    let (completion, result) = oneshot::channel();
    host.route_control(PlacementControlEvent {
        kind: PlacementControlEventKind::Command(Box::new(InboundPlacementControl {
            association: AssociationKey {
                cluster_id: lattice_core::actor_ref::ClusterId::new("term-fencing").unwrap(),
                local_incarnation: local.incarnation,
                remote_address: remote.address,
                remote_incarnation: remote.incarnation,
            },
            command_id: CommandId::generate(),
            scope: CoordinatorScope::Membership,
            coordinator_term: Some(active_term.saturating_add(1)),
            command: PlacementControlCommand::NodeHeartbeat {
                incarnation: remote.incarnation,
                sequence: 1,
            },
        })),
        completion,
    })
    .await;
    assert_eq!(
        result.await.unwrap(),
        Err(ControlDispatchError::InvalidCommand)
    );
}

#[tokio::test]
async fn standby_scope_fences_old_control_instead_of_retrying_it() {
    let store = Arc::new(InMemoryPlacementStore::new(32, 32).unwrap());
    let leader_node = node("leader", 30, 33030);
    let standby_node = node("standby", 31, 33031);
    let remote = node("member", 32, 33032);
    let leader = CoordinatorHost::elect(
        store.clone(),
        associations(&leader_node),
        leader_node,
        BTreeSet::new(),
        config(),
    )
    .await
    .unwrap();
    let mut standby = CoordinatorHost::elect(
        store,
        associations(&standby_node),
        standby_node.clone(),
        BTreeSet::new(),
        config(),
    )
    .await
    .unwrap();
    assert!(matches!(
        standby.scope_state(&CoordinatorScope::Membership),
        Some(CoordinatorHostScopeState::Standby)
    ));

    let (completion, result) = oneshot::channel();
    standby
        .route_control(PlacementControlEvent {
            kind: PlacementControlEventKind::Command(Box::new(InboundPlacementControl {
                association: AssociationKey {
                    cluster_id: lattice_core::actor_ref::ClusterId::new("standby-fencing").unwrap(),
                    local_incarnation: standby_node.incarnation,
                    remote_address: remote.address,
                    remote_incarnation: remote.incarnation,
                },
                command_id: CommandId::generate(),
                scope: CoordinatorScope::Membership,
                coordinator_term: Some(leader.active_term(&CoordinatorScope::Membership).unwrap()),
                command: PlacementControlCommand::NodeHeartbeat {
                    incarnation: remote.incarnation,
                    sequence: 1,
                },
            })),
            completion,
        })
        .await;
    assert_eq!(
        result.await.unwrap(),
        Err(ControlDispatchError::InvalidCommand)
    );
}
