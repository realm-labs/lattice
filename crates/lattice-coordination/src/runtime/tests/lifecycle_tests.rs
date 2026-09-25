use super::*;
use crate::candidate_fixture::{elect_cluster, elect_group};
use crate::runtime::cluster::ClusterCoordinatorConfig;

#[tokio::test]
async fn membership_rejoin_replaces_stale_group_session_and_ignores_late_removal() {
    let cluster = ClusterId::new("membership-rejoin").unwrap();
    let (coordinator, _) = node(&cluster, "coordinator", 34911, 1);
    let (member, _) = node(&cluster, "member", 34912, 2);
    let associations = Arc::new(
        AssociationManager::new(
            coordinator.address.clone(),
            coordinator.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let key = attach_test_session(
        &associations,
        &cluster,
        coordinator.incarnation,
        &member,
        100,
    );
    let store = Arc::new(InMemoryCoordinationStore::new(16, 16).unwrap());
    let mut membership = elect_cluster(
        store.clone(),
        coordinator.clone(),
        CoordinatorTerm::new(1).unwrap(),
        ClusterCoordinatorConfig::default(),
    )
    .await
    .unwrap();
    let hello = empty_hello(member.clone());
    membership.join(hello.member.clone()).await.unwrap();
    membership.mark_up(&member).await.unwrap();
    let mut leader = elect_group(
        store.clone(),
        associations,
        coordinator,
        CoordinatorScope::Group(group()),
        CoordinatorTerm::new(1).unwrap(),
        GroupCoordinatorConfig::default(),
    )
    .await
    .unwrap();
    leader
        .register(hello.group.clone(), key.clone())
        .await
        .unwrap();
    let old_lease = leader.sessions[&member.incarnation].lease_id;
    membership
        .remove(&member, MemberRemovalReason::FailureDetected)
        .await
        .unwrap();
    membership.join(hello.member.clone()).await.unwrap();
    membership.mark_up(&member).await.unwrap();
    leader.register(hello.group, key).await.unwrap();
    let current = store.get_member(&member.node_id).await.unwrap().unwrap();
    assert_ne!(old_lease, current.lease_id);
    assert_eq!(leader.sessions[&member.incarnation].record, current);
    leader
        .handle_control(PlacementControlEventKind::GlobalMemberRemoved {
            node: member.clone(),
            reason: MemberRemovalReason::FailureDetected,
        })
        .await
        .unwrap();
    assert!(leader.sessions.contains_key(&member.incarnation));
    assert!(
        store
            .get_group_member(&group(), &member.node_id)
            .await
            .unwrap()
            .is_some()
    );
    // A current removal still removes participation normally.
    membership
        .remove(&member, MemberRemovalReason::FailureDetected)
        .await
        .unwrap();
    leader
        .handle_control(PlacementControlEventKind::GlobalMemberRemoved {
            node: member.clone(),
            reason: MemberRemovalReason::FailureDetected,
        })
        .await
        .unwrap();
    assert!(!leader.sessions.contains_key(&member.incarnation));
    assert!(
        store
            .get_group_member(&group(), &member.node_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn placement_drain_commit_replays_from_durable_absence_after_leader_replacement() {
    let cluster = ClusterId::new("placement-drain-replay").unwrap();
    let (coordinator, _) = node(&cluster, "coordinator", 34901, 1);
    let (departing, _) = node(&cluster, "departing", 34902, 2);
    let associations = Arc::new(
        AssociationManager::new(
            coordinator.address.clone(),
            coordinator.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let key = attach_test_session(
        &associations,
        &cluster,
        coordinator.incarnation,
        &departing,
        100,
    );
    let store = Arc::new(InMemoryCoordinationStore::new(16, 16).unwrap());
    let mut membership = elect_cluster(
        store.clone(),
        coordinator.clone(),
        CoordinatorTerm::new(1).unwrap(),
        ClusterCoordinatorConfig::default(),
    )
    .await
    .unwrap();
    let hello = empty_hello(departing.clone());
    membership.join(hello.member).await.unwrap();
    membership.mark_up(&departing).await.unwrap();
    let mut leader = elect_group(
        store.clone(),
        associations.clone(),
        coordinator.clone(),
        CoordinatorScope::Group(group()),
        CoordinatorTerm::new(1).unwrap(),
        GroupCoordinatorConfig::default(),
    )
    .await
    .unwrap();
    leader.register(hello.group, key.clone()).await.unwrap();
    leader
        .mark_member_up(departing.incarnation, leader.membership_version, &key)
        .await
        .unwrap();
    leader
        .begin_member_drain(
            departing.incarnation,
            "leave".to_owned(),
            departing.incarnation,
        )
        .await
        .unwrap();
    let request = |term| {
        PlacementControlEventKind::Command(Box::new(InboundPlacementControl {
            association: key.clone(),
            command_id: CommandId::generate(),
            scope: CoordinatorScope::Group(group()),
            coordinator_term: Some(term),
            command: PlacementControlCommand::DrainComplete {
                operation_id: "leave".to_owned(),
                node_id: departing.node_id.clone(),
                expected_incarnation: departing.incarnation,
            },
        }))
    };
    leader.handle_control(request(1)).await.unwrap();
    assert!(
        store
            .get_group_member(&group(), &departing.node_id)
            .await
            .unwrap()
            .is_none()
    );
    leader.handle_control(request(1)).await.unwrap();
    let old_lease = leader.leader_lease_id;
    drop(leader);
    store.revoke_lease(old_lease).await.unwrap();
    let mut replacement = elect_group(
        store,
        associations.clone(),
        coordinator,
        CoordinatorScope::Group(group()),
        CoordinatorTerm::new(2).unwrap(),
        GroupCoordinatorConfig::default(),
    )
    .await
    .unwrap();
    replacement.handle_control(request(2)).await.unwrap();
    let peer = associations.get(&key).unwrap();
    let confirmations = peer.replay_control_frames().into_iter().filter_map(|frame| {
        let envelope = decode_control_envelope(&frame).ok()?;
        decode_control_command(&envelope.payload, DEFAULT_MAX_CONTROL_PAYLOAD).ok()
    }).filter(|command| matches!(&command.command, PlacementControlCommand::DrainCommitted { operation_id, expected_incarnation } if operation_id == "leave" && *expected_incarnation == departing.incarnation)).collect::<Vec<_>>();
    assert_eq!(confirmations.len(), 3);
    assert_eq!(confirmations.last().unwrap().coordinator_term, Some(2));
    membership.shutdown().await.unwrap();
}

/// A member that hosts nothing drains for free, so the drain that matters is the one that has to
/// hand a shard over first. Marking the member `Leaving` must not take it out of the placement view
/// the drain rebalance reads: the shard it still owns would then have an owner no node in the view
/// matches, which the strategy is entitled to call a corrupt view.
#[tokio::test]
async fn draining_a_member_that_owns_a_shard_plans_the_handoff_off_it() {
    let cluster_id = ClusterId::new("drain-handoff").unwrap();
    let (coordinator_node, _) = node(&cluster_id, "coordinator", 26500, 500);
    let (source, _) = node(&cluster_id, "source", 26501, 501);
    let (target, _) = node(&cluster_id, "target", 26502, 502);
    let associations = Arc::new(
        AssociationManager::new(
            coordinator_node.address.clone(),
            coordinator_node.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let source_key = attach_test_session(
        &associations,
        &cluster_id,
        coordinator_node.incarnation,
        &source,
        5_000,
    );
    let target_key = attach_test_session(
        &associations,
        &cluster_id,
        coordinator_node.incarnation,
        &target,
        6_000,
    );
    let entity_type = EntityType::new("drained-entity").unwrap();
    let shard_id = ShardId::new(0);
    let slot_key = PlacementSlotKey::Shard {
        group: group(),
        entity_type: entity_type.clone(),
        shard_id,
    };
    let store = Arc::new(InMemoryCoordinationStore::new(16, 16).unwrap());
    let mut leader = elect_group(
        store.clone(),
        associations,
        coordinator_node,
        CoordinatorScope::Group(group()),
        CoordinatorTerm::new(1).unwrap(),
        GroupCoordinatorConfig::default(),
    )
    .await
    .unwrap();
    let protocol_id = ProtocolId::new(91).unwrap();
    let entity_config = EntityConfig::new(
        group(),
        entity_type.clone(),
        protocol_id,
        1,
        "weighted-least-load",
        1,
        Vec::new(),
    )
    .unwrap();
    let descriptor = ProtocolDescriptor {
        protocol_id,
        fingerprint: ProtocolFingerprint::new([11; 32]),
    };
    let hello = |node: NodeKey| {
        test_hello(
            node,
            TestHelloSpec {
                capacity_units: 1,
                hosted_entity_types: [entity_type.clone()].into_iter().collect(),
                protocols: vec![descriptor.clone()],
                entity_configs: vec![entity_config.clone()],
                ..TestHelloSpec::default()
            },
        )
    };
    let source_hello = hello(source.clone());
    seed_running_slot(
        &mut leader,
        PlacementSlot {
            key: slot_key.clone(),
            config_fingerprint: entity_config.fingerprint(),
            owner: Some(source.clone()),
            target: None,
            assignment_generation: AssignmentGeneration::new(1).unwrap(),
            version: PlacementVersion::new(
                group(),
                CoordinatorTerm::new(1).unwrap(),
                Revision::new(1).unwrap(),
            ),
            state: PlacementSlotState::Running,
            active_move: None,
            barrier_sessions: Default::default(),
        },
        Some(&source_hello),
    )
    .await;
    register_up(&mut leader, source_hello, source_key).await;
    register_up(&mut leader, hello(target.clone()), target_key).await;

    leader
        .begin_member_drain(
            source.incarnation,
            "drain-shard".to_owned(),
            source.incarnation,
        )
        .await
        .unwrap();

    let handed_over = store.get_slot(&slot_key).await.unwrap().unwrap();
    assert_eq!(handed_over.state, PlacementSlotState::BeginHandoff);
    assert_eq!(handed_over.owner.as_ref(), Some(&source));
    assert_eq!(handed_over.target.as_ref(), Some(&target));
    assert_eq!(
        leader
            .plans
            .values()
            .filter(|plan| plan.reason == PlanReason::Drain)
            .count(),
        1
    );
    assert!(
        handed_over.barrier_sessions.contains(&source.incarnation),
        "the drained member still has to acknowledge the barrier its own handoff installs"
    );
    let barrier_version = leader.handoffs[&slot_key].barrier_version();
    assert!(
        sent_commands(&leader, &source).iter().any(|command| matches!(
            command,
            PlacementControlCommand::StateDelta(delta) if delta.version.satisfies(&barrier_version)
        )),
        "a leaving member that never receives the barrier revision can never acknowledge it"
    );

    // A leaving member re-sends its request while it waits, so a repeat of the same operation must
    // rejoin the drain already in flight rather than start or refuse a second one.
    leader
        .begin_member_drain(
            source.incarnation,
            "drain-shard".to_owned(),
            source.incarnation,
        )
        .await
        .unwrap();
    assert_eq!(
        leader
            .plans
            .values()
            .filter(|plan| plan.reason == PlanReason::Drain)
            .count(),
        1
    );
    assert_eq!(
        leader.handoffs[&slot_key].barrier_version(),
        barrier_version
    );
    assert!(matches!(
        leader
            .begin_member_drain(
                source.incarnation,
                "another-drain".to_owned(),
                source.incarnation,
            )
            .await,
        Err(CoordinatorRuntimeError::IdempotencyConflict)
    ));

    for member in [&source, &target] {
        leader
            .transition_handoff(
                slot_key.clone(),
                HandoffEvent::AppliedRevision {
                    session: member.incarnation,
                    version: barrier_version.clone(),
                },
            )
            .await
            .unwrap();
    }
    assert_eq!(
        store.get_slot(&slot_key).await.unwrap().unwrap().state,
        PlacementSlotState::Stopping
    );
    assert!(
        sent_commands(&leader, &source)
            .iter()
            .any(|command| matches!(command, PlacementControlCommand::DrainSlot { slot, .. } if slot == &slot_key))
    );

    leader
        .transition_handoff(
            slot_key.clone(),
            HandoffEvent::SourceDrained {
                source: source.clone(),
                generation: AssignmentGeneration::new(1).unwrap(),
            },
        )
        .await
        .unwrap();
    let replaced = store.get_slot(&slot_key).await.unwrap().unwrap();
    assert_eq!(replaced.owner.as_ref(), Some(&target));
    leader
        .transition_handoff(
            slot_key.clone(),
            HandoffEvent::TargetReady {
                target: target.clone(),
                generation: replaced.assignment_generation,
            },
        )
        .await
        .unwrap();
    let running = store.get_slot(&slot_key).await.unwrap().unwrap();
    assert_eq!(running.state, PlacementSlotState::Running);
    assert_eq!(running.owner.as_ref(), Some(&target));

    leader
        .maybe_send_drain_ready(source.incarnation)
        .await
        .unwrap();
    assert!(
        sent_commands(&leader, &source).iter().any(|command| matches!(
            command,
            PlacementControlCommand::DrainReady { operation_id, .. } if operation_id == "drain-shard"
        )),
        "the graceful leave never completes unless the coordinator releases the drained member"
    );
}

fn sent_commands(
    leader: &GroupCoordinator<InMemoryCoordinationStore>,
    node: &NodeKey,
) -> Vec<PlacementControlCommand> {
    let association = leader
        .associations
        .get(&leader.sessions[&node.incarnation].association)
        .unwrap();
    association
        .replay_control_frames()
        .into_iter()
        .filter_map(|frame| decode_control_envelope(&frame).ok())
        .filter_map(|envelope| {
            decode_control_command(&envelope.payload, DEFAULT_MAX_CONTROL_PAYLOAD).ok()
        })
        .map(|scoped| scoped.command)
        .collect()
}

#[tokio::test]
async fn join_drain_and_force_remove_are_revisioned_idempotent_and_fenced() {
    let cluster = ClusterId::new("member-lifecycle").unwrap();
    let (coordinator, coordinator_identity) = node(&cluster, "coordinator", 30100, 100);
    let (joining, _) = node(&cluster, "joining", 30101, 101);
    let (forced, _) = node(&cluster, "forced", 30102, 102);
    let (old_reused, _) = node(&cluster, "reused", 30103, 103);
    let (new_reused, _) = node(&cluster, "reused", 30104, 104);
    let config = RemotingConfig::default();
    let associations = Arc::new(
        AssociationManager::new(coordinator.address.clone(), coordinator.incarnation, config)
            .unwrap(),
    );
    let joining_key = attach_test_session(
        &associations,
        &cluster,
        coordinator_identity.incarnation,
        &joining,
        1000,
    );
    let forced_key = attach_test_session(
        &associations,
        &cluster,
        coordinator_identity.incarnation,
        &forced,
        2000,
    );
    let old_reused_key = attach_test_session(
        &associations,
        &cluster,
        coordinator_identity.incarnation,
        &old_reused,
        3000,
    );
    let new_reused_key = attach_test_session(
        &associations,
        &cluster,
        coordinator_identity.incarnation,
        &new_reused,
        4000,
    );
    let store = Arc::new(InMemoryCoordinationStore::new(16, 16).unwrap());
    let mut membership = elect_cluster(
        store.clone(),
        coordinator.clone(),
        CoordinatorTerm::new(1).unwrap(),
        ClusterCoordinatorConfig::default(),
    )
    .await
    .unwrap();
    let mut leader = elect_group(
        store.clone(),
        associations,
        coordinator,
        CoordinatorScope::Group(group()),
        CoordinatorTerm::new(1).unwrap(),
        GroupCoordinatorConfig::default(),
    )
    .await
    .unwrap();

    let joining_hello = empty_hello(joining.clone());
    membership.join(joining_hello.member.clone()).await.unwrap();
    assert!(matches!(
        leader
            .register(joining_hello.group.clone(), joining_key.clone())
            .await,
        Err(CoordinatorRuntimeError::MemberNotReady)
    ));
    membership.mark_up(&joining).await.unwrap();
    leader
        .register(joining_hello.group, joining_key.clone())
        .await
        .unwrap();
    let joining_version = leader.membership_version;
    assert_eq!(
        store.get_member("joining").await.unwrap().unwrap().status,
        MemberStatus::Up
    );
    assert!(matches!(
        leader
            .mark_member_up(
                joining.incarnation,
                joining_version.next_revision().unwrap(),
                &joining_key,
            )
            .await,
        Err(CoordinatorRuntimeError::StaleMember)
    ));
    leader
        .mark_member_up(joining.incarnation, joining_version, &joining_key)
        .await
        .unwrap();
    let up = store.get_member("joining").await.unwrap().unwrap();
    assert_eq!(up.status, MemberStatus::Up);
    leader
        .mark_member_up(joining.incarnation, joining_version, &joining_key)
        .await
        .unwrap();

    assert!(
        leader
            .begin_member_drain(
                joining.incarnation,
                "drain-1".to_string(),
                NodeIncarnation::new(999).unwrap(),
            )
            .await
            .is_err()
    );
    leader
        .begin_member_drain(
            joining.incarnation,
            "drain-1".to_string(),
            joining.incarnation,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .get_group_member(&group(), "joining")
            .await
            .unwrap()
            .unwrap()
            .status,
        GroupMemberStatus::Leaving
    );
    assert!(
        leader
            .complete_member_drain(joining.incarnation, "other", joining.incarnation)
            .await
            .is_err()
    );
    leader
        .complete_member_drain(joining.incarnation, "drain-1", joining.incarnation)
        .await
        .unwrap();
    assert!(
        leader
            .gracefully_removed_sessions
            .contains(&joining.incarnation)
    );
    assert!(
        store
            .get_group_member(&group(), "joining")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.get_member("joining").await.unwrap().unwrap().status,
        MemberStatus::Up
    );
    membership.begin_leave(&joining).await.unwrap();
    membership
        .remove(&joining, MemberRemovalReason::GracefulLeave)
        .await
        .unwrap();

    let forced_hello = empty_hello(forced.clone());
    membership.join(forced_hello.member.clone()).await.unwrap();
    membership.mark_up(&forced).await.unwrap();
    register_up(&mut leader, forced_hello, forced_key).await;
    let request = ForceRemoveRequest {
        group: group(),
        operation_id: leader.inspect().await.unwrap().new_operation_id(),
        node_id: forced.node_id.clone(),
        expected_incarnation: forced.incarnation,
    };
    assert!(
        leader
            .force_remove(ForceRemoveRequest {
                expected_incarnation: NodeIncarnation::new(999).unwrap(),
                ..request.clone()
            })
            .await
            .is_err()
    );
    leader.force_remove(request.clone()).await.unwrap();
    assert!(
        !leader
            .gracefully_removed_sessions
            .contains(&forced.incarnation)
    );
    leader.force_remove(request).await.unwrap();
    assert!(
        store
            .get_group_member(&group(), "forced")
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.get_member("forced").await.unwrap().is_some());

    let old_reused_hello = empty_hello(old_reused.clone());
    membership
        .join(old_reused_hello.member.clone())
        .await
        .unwrap();
    membership.mark_up(&old_reused).await.unwrap();
    register_up(&mut leader, old_reused_hello, old_reused_key).await;
    let reused_hello = empty_hello(new_reused.clone());
    assert!(matches!(
            membership.join(reused_hello.member.clone())
            .await,
        Err(CoordinatorRuntimeError::IncarnationPending {
            predecessor,
            remaining_ttl: Some(_),
        }) if predecessor == old_reused.incarnation
    ));
    membership.begin_leave(&old_reused).await.unwrap();
    membership
        .remove(&old_reused, MemberRemovalReason::IncarnationReplaced)
        .await
        .unwrap();
    let reused_hello = empty_hello(new_reused.clone());
    membership.join(reused_hello.member.clone()).await.unwrap();
    membership.mark_up(&new_reused).await.unwrap();
    leader
        .register(reused_hello.group, new_reused_key)
        .await
        .unwrap();
    let current = store.get_member("reused").await.unwrap().unwrap();
    assert_eq!(current.node, new_reused);
    assert_eq!(current.status, MemberStatus::Up);
}
