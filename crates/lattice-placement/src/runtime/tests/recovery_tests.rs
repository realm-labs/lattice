use lattice_core::actor_ref::{ProtocolId, SingletonKind};
use lattice_remoting::protocol::{ProtocolDescriptor, ProtocolFingerprint};

use super::*;

#[tokio::test]
async fn leader_recovery_resumes_persisted_handoff() {
    use crate::allocation::{ProposedMove, RebalanceProposal, RebalanceTrigger};

    let cluster_id = ClusterId::new("recovery-test").unwrap();
    let (coordinator, _) = node(&cluster_id, "coordinator", 26300, 300);
    let (source, _) = node(&cluster_id, "source", 26301, 301);
    let (target, _) = node(&cluster_id, "target", 26302, 302);
    let entity_type = EntityType::new("recovery-entity").unwrap();
    let shard_id = ShardId::new(4);
    let proposal = |expected_generation| RebalanceProposal {
        domain: domain(),
        policy_id: "test",
        policy_version: 1,
        base_version: PlacementVersion::new(
            domain(),
            CoordinatorTerm::new(1).unwrap(),
            Revision::new(1).unwrap(),
        ),
        trigger: RebalanceTrigger::Manual {
            source: Some(source.clone()),
            target: Some(target.clone()),
            bypass_improvement: true,
        },
        moves: vec![ProposedMove {
            domain: domain(),
            entity_type: entity_type.clone(),
            shard_id,
            expected_generation,
            source: source.clone(),
            target: target.clone(),
            estimated_weight: 1,
        }],
    };
    let mut started = RebalancePlan::from_proposal(
        proposal(AssignmentGeneration::new(1).unwrap()),
        entity_type.clone(),
        CoordinatorTerm::new(1).unwrap(),
        4,
    )
    .unwrap();
    let pending = started.clone();
    started
        .begin_move(shard_id, AssignmentGeneration::new(1).unwrap(), None)
        .unwrap();
    let slot_key = PlacementSlotKey::Shard {
        domain: domain(),
        entity_type,
        shard_id,
    };
    let store = Arc::new(InMemoryPlacementStore::new(8, 8).unwrap());
    let associations = Arc::new(
        AssociationManager::new(
            coordinator.address.clone(),
            coordinator.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let mut leader = PlacementDomainLeader::elect(
        store.clone(),
        associations,
        coordinator,
        CoordinatorScope::Placement(domain()),
        CoordinatorTerm::new(1).unwrap(),
        PlacementDomainLeaderConfig::default(),
    )
    .await
    .unwrap();
    seed_running_slot(
        &mut leader,
        PlacementSlot {
            key: slot_key.clone(),
            config_fingerprint: ConfigFingerprint::new([7; 32]),
            owner: Some(source.clone()),
            target: None,
            assignment_generation: AssignmentGeneration::new(1).unwrap(),
            version: PlacementVersion::new(
                domain(),
                CoordinatorTerm::new(1).unwrap(),
                Revision::new(1).unwrap(),
            ),
            state: PlacementSlotState::Running,
            active_move: None,
            barrier_sessions: Default::default(),
        },
        None,
    )
    .await;
    store
        .create_plan(
            &leader.leader_guard,
            CreatePlan {
                plan: pending.clone(),
            },
        )
        .await
        .unwrap();
    let barrier_version = leader.next_version().unwrap();
    started
        .install_barrier(shard_id, barrier_version.clone(), Default::default())
        .unwrap();
    started.record_revision = started.record_revision.next().unwrap();
    let expected_slot = store.get_slot(&slot_key).await.unwrap().unwrap();
    let mut handoff_slot = expected_slot.clone();
    handoff_slot.target = Some(target);
    handoff_slot.state = PlacementSlotState::BeginHandoff;
    handoff_slot.active_move = Some(started.plan_id);
    handoff_slot.version = barrier_version.clone();
    store
        .reserve_move(
            &leader.leader_guard,
            ReserveMove {
                expected_plan: pending,
                plan: started.clone(),
                expected_slot,
                slot: handoff_slot,
            },
        )
        .await
        .unwrap();
    leader.version = barrier_version;
    leader.plans.insert(started.plan_id, started);
    leader.recover_persisted_plans().await.unwrap();
    assert_eq!(
        store.get_slot(&slot_key).await.unwrap().unwrap().state,
        PlacementSlotState::Stopping
    );
    assert_eq!(leader.handoffs[&slot_key].phase, HandoffPhase::Draining);
}

#[tokio::test]
async fn singleton_owner_loss_recovers_forward_after_leader_restart() {
    let cluster_id = ClusterId::new("singleton-recovery-test").unwrap();
    let (coordinator, _) = node(&cluster_id, "coordinator", 26400, 400);
    let (source, _) = node(&cluster_id, "source", 26401, 401);
    let (target, _) = node(&cluster_id, "target", 26402, 402);
    let associations = Arc::new(
        AssociationManager::new(
            coordinator.address.clone(),
            coordinator.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let source_key = attach_test_session(
        &associations,
        &cluster_id,
        coordinator.incarnation,
        &source,
        50,
    );
    let target_key = attach_test_session(
        &associations,
        &cluster_id,
        coordinator.incarnation,
        &target,
        60,
    );
    let store = Arc::new(InMemoryPlacementStore::new(8, 8).unwrap());
    let mut leader = PlacementDomainLeader::elect(
        store.clone(),
        associations,
        coordinator,
        CoordinatorScope::Placement(domain()),
        CoordinatorTerm::new(1).unwrap(),
        PlacementDomainLeaderConfig {
            member_heartbeat_timeout: Duration::from_millis(10),
            ..PlacementDomainLeaderConfig::default()
        },
    )
    .await
    .unwrap();
    let kind = SingletonKind::new("recovering-singleton").unwrap();
    let protocol_id = ProtocolId::new(77).unwrap();
    let singleton_config = SingletonConfig::new(domain(), kind.clone(), protocol_id);
    let descriptor = ProtocolDescriptor {
        protocol_id,
        fingerprint: ProtocolFingerprint::new([5; 32]),
    };
    let hello = |node: NodeKey| {
        test_hello(
            node,
            TestHelloSpec {
                capacity_units: 1,
                singleton_eligibility: [kind.clone()].into_iter().collect(),
                used_singletons: [kind.clone()].into_iter().collect(),
                protocols: vec![descriptor.clone()],
                singleton_configs: vec![singleton_config.clone()],
                ..TestHelloSpec::default()
            },
        )
    };
    register_up(&mut leader, hello(source.clone()), source_key).await;
    register_up(&mut leader, hello(target.clone()), target_key).await;
    leader
        .ensure_singleton_allocated(kind.clone())
        .await
        .unwrap();
    let slot_key = PlacementSlotKey::Singleton {
        domain: domain(),
        kind,
    };
    let initial = store.get_slot(&slot_key).await.unwrap().unwrap();
    assert_eq!(initial.owner.as_ref(), Some(&source));
    leader
        .complete_initial_ready(&slot_key, &source, initial.assignment_generation)
        .await
        .unwrap();

    leader
        .sessions
        .get_mut(&source.incarnation)
        .unwrap()
        .last_heartbeat = Instant::now() - Duration::from_secs(1);
    leader.renew().await.unwrap();
    let persisted = store.get_slot(&slot_key).await.unwrap().unwrap();
    assert_eq!(persisted.state, PlacementSlotState::BeginHandoff);
    assert_eq!(persisted.target.as_ref(), Some(&target));
    assert!(store.get_claim(&slot_key).await.unwrap().is_none());

    leader.handoffs.clear();
    leader.recover_persisted_plans().await.unwrap();
    assert_eq!(leader.handoffs[&slot_key].phase, HandoffPhase::Invalidating);
    leader
        .transition_handoff(
            slot_key.clone(),
            HandoffEvent::AppliedRevision {
                session: target.incarnation,
                version: persisted.version,
            },
        )
        .await
        .unwrap();
    let allocating = store.get_slot(&slot_key).await.unwrap().unwrap();
    assert_eq!(allocating.state, PlacementSlotState::Allocating);
    assert_eq!(allocating.owner.as_ref(), Some(&target));
    assert_eq!(allocating.assignment_generation.get(), 2);
    assert_eq!(
        store
            .get_claim(&slot_key)
            .await
            .unwrap()
            .unwrap()
            .grant
            .owner,
        target
    );

    leader
        .transition_handoff(
            slot_key.clone(),
            HandoffEvent::TargetReady {
                target: allocating.owner.unwrap(),
                generation: allocating.assignment_generation,
            },
        )
        .await
        .unwrap();
    let active = store.get_slot(&slot_key).await.unwrap().unwrap();
    assert_eq!(active.state, PlacementSlotState::Running);
    assert!(active.active_move.is_none());
    assert!(active.barrier_sessions.is_empty());
}
