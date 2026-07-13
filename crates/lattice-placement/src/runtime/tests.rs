use super::*;
use async_trait::async_trait;
use lattice_core::actor_ref::{
    ClusterId, ConfigFingerprint, EntityType, NodeAddress, NodeIncarnation,
};
use lattice_remoting::config::RemotingConfig;
use lattice_remoting::endpoint::RemotingEndpoint;
use lattice_remoting::handshake::NodeIdentity;
use lattice_remoting::messaging::error::RemoteMessageError;
use lattice_remoting::messaging::inbound::InboundDispatch;
use lattice_remoting::messaging::outbound::OutboundMessaging;
use lattice_remoting::messaging::target::ExactActorTarget;

use crate::authority::AuthorityEffect;
use crate::control::PlacementControlRouter;
use crate::session::{LogicCoordinatorConfig, LogicCoordinatorSession};
use crate::storage::{InMemoryPlacementStore, PlacementStore};
use crate::types::{AssignmentGeneration, PlacementSlot, PlacementSlotState, ShardId};

fn attach_test_session(
    associations: &AssociationManager,
    cluster_id: &ClusterId,
    coordinator_incarnation: NodeIncarnation,
    remote: &NodeKey,
    nonce_base: u128,
) -> lattice_remoting::association::AssociationKey {
    let association = associations
        .get_or_create(
            cluster_id.clone(),
            remote.address.clone(),
            remote.incarnation,
        )
        .unwrap();
    let key = lattice_remoting::association::AssociationKey {
        cluster_id: cluster_id.clone(),
        local_incarnation: coordinator_incarnation,
        remote_address: remote.address.clone(),
        remote_incarnation: remote.incarnation,
    };
    for (lane, nonce) in [
        (lattice_remoting::association::LaneKind::Control, nonce_base),
        (
            lattice_remoting::association::LaneKind::Interactive,
            nonce_base + 1,
        ),
        (
            lattice_remoting::association::LaneKind::Bulk(0),
            nonce_base + 2,
        ),
    ] {
        association
            .attach(lattice_remoting::association::LaneAttachment {
                association_id: association.id(),
                key: key.clone(),
                lane,
                connection_nonce: nonce,
            })
            .unwrap();
    }
    key
}

async fn register_up(
    leader: &mut CoordinatorLeader<InMemoryPlacementStore>,
    hello: NodeHello,
    association: lattice_remoting::association::AssociationKey,
) {
    let incarnation = hello.node.incarnation;
    leader.register(hello, association.clone()).await.unwrap();
    leader
        .mark_member_up(incarnation, leader.revision, &association)
        .await
        .unwrap();
}

struct NoActors;

#[async_trait]
impl InboundDispatch for NoActors {
    async fn tell(
        &self,
        _target: ExactActorTarget,
        _message_id: u64,
        _payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        Err(RemoteMessageError::UnsupportedProtocol)
    }

    async fn ask(
        &self,
        _target: ExactActorTarget,
        _message_id: u64,
        _payload: Bytes,
        _deadline: std::time::Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        Err(RemoteMessageError::UnsupportedProtocol)
    }
}

fn node(
    cluster_id: &ClusterId,
    node_id: &str,
    port: u16,
    incarnation: u128,
) -> (NodeKey, NodeIdentity) {
    let address = NodeAddress::new("127.0.0.1", port).unwrap();
    let incarnation = NodeIncarnation::new(incarnation).unwrap();
    (
        NodeKey {
            node_id: node_id.to_owned(),
            address: address.clone(),
            incarnation,
        },
        NodeIdentity {
            cluster_id: cluster_id.clone(),
            node_id: node_id.to_owned(),
            address,
            incarnation,
        },
    )
}

fn empty_hello(node: NodeKey) -> NodeHello {
    NodeHello {
        node,
        roles: Default::default(),
        capacity_units: 1,
        hosted_entity_types: Default::default(),
        proxied_entity_types: Default::default(),
        singleton_eligibility: Default::default(),
        used_singletons: Default::default(),
        protocols: Vec::new(),
        entity_configs: Vec::new(),
        singleton_configs: Vec::new(),
    }
}

#[tokio::test]
async fn real_control_session_installs_snapshot_and_matching_claim() {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coordinator_port = probe.local_addr().unwrap().port();
    drop(probe);
    let logic_port = coordinator_port - 1;
    let cluster_id = ClusterId::new("coordinator-test").unwrap();
    let (logic_node, logic_identity) = node(&cluster_id, "logic", logic_port, 1);
    let (coordinator_node, coordinator_identity) =
        node(&cluster_id, "coordinator", coordinator_port, 2);
    let remoting = RemotingConfig {
        heartbeat_interval: Duration::from_millis(100),
        shutdown_timeout: Duration::from_secs(2),
        ..RemotingConfig::default()
    };
    let logic_associations = Arc::new(
        AssociationManager::new(
            logic_identity.address.clone(),
            logic_identity.incarnation,
            remoting.clone(),
        )
        .unwrap(),
    );
    let coordinator_associations = Arc::new(
        AssociationManager::new(
            coordinator_identity.address.clone(),
            coordinator_identity.incarnation,
            remoting.clone(),
        )
        .unwrap(),
    );
    let (logic_router, logic_controls) =
        PlacementControlRouter::bounded(64, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
    let (coordinator_router, coordinator_controls) =
        PlacementControlRouter::bounded(64, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
    let logic_endpoint = Arc::new(
        RemotingEndpoint::new_with_control(
            logic_identity.clone(),
            remoting.clone(),
            logic_associations.clone(),
            Arc::new(OutboundMessaging::new(32).unwrap()),
            Arc::new(NoActors),
            Arc::new(logic_router),
            Vec::new(),
        )
        .unwrap(),
    );
    let coordinator_endpoint = Arc::new(
        RemotingEndpoint::new_with_control(
            coordinator_identity.clone(),
            remoting,
            coordinator_associations.clone(),
            Arc::new(OutboundMessaging::new(32).unwrap()),
            Arc::new(NoActors),
            Arc::new(coordinator_router),
            Vec::new(),
        )
        .unwrap(),
    );
    coordinator_endpoint.bind().await.unwrap();
    let logic_to_coordinator = logic_endpoint
        .connect_peer(coordinator_identity)
        .await
        .unwrap();

    let entity_type = EntityType::new("player").unwrap();
    let slot_key = PlacementSlotKey::Shard {
        entity_type: entity_type.clone(),
        shard_id: ShardId::new(3),
    };
    let store = Arc::new(InMemoryPlacementStore::new(16, 16).unwrap());
    store
        .compare_and_put_slot(
            None,
            PlacementSlot {
                key: slot_key.clone(),
                config_fingerprint: ConfigFingerprint::new([7; 32]),
                owner: Some(logic_node.clone()),
                target: None,
                assignment_generation: AssignmentGeneration::new(1).unwrap(),
                coordinator_term: CoordinatorTerm::new(1).unwrap(),
                revision: Revision::new(1).unwrap(),
                state: PlacementSlotState::Running,
                active_move: None,
                barrier_sessions: Default::default(),
            },
        )
        .await
        .unwrap();
    let leader = CoordinatorLeader::elect(
        store,
        coordinator_associations,
        coordinator_node,
        CoordinatorTerm::new(1).unwrap(),
        2,
        CoordinatorLeaderConfig {
            renewal_interval: Duration::from_secs(1),
            ..CoordinatorLeaderConfig::default()
        },
    )
    .await
    .unwrap();
    let hello = NodeHello {
        node: logic_node,
        roles: ["logic".to_owned()].into_iter().collect(),
        capacity_units: 1,
        hosted_entity_types: [entity_type].into_iter().collect(),
        proxied_entity_types: Default::default(),
        singleton_eligibility: Default::default(),
        used_singletons: Default::default(),
        protocols: Vec::new(),
        entity_configs: Vec::new(),
        singleton_configs: Vec::new(),
    };
    let (logic, mut effects) = LogicCoordinatorSession::new(
        hello,
        logic_to_coordinator.key().clone(),
        logic_associations,
        LogicCoordinatorConfig::default(),
        32,
    )
    .unwrap();
    let state = logic.state();
    logic
        .register_authority(slot_key.clone(), Duration::from_secs(2))
        .unwrap();
    let (leader_shutdown_tx, leader_shutdown_rx) = watch::channel(false);
    let (logic_shutdown_tx, logic_shutdown_rx) = watch::channel(false);
    let leader_task = tokio::spawn(leader.run(coordinator_controls, leader_shutdown_rx));
    let logic_task = tokio::spawn(logic.run(logic_controls, logic_shutdown_rx));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if state
                .lock()
                .expect("logic state poisoned")
                .admission_open(&slot_key)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut observed = Vec::new();
    while let Ok(effect) = effects.try_recv() {
        if let crate::session::LogicPlacementEffect::Authority { effect, .. } = effect {
            observed.push(effect);
        }
    }
    assert!(observed.contains(&AuthorityEffect::StartSlot));
    assert!(observed.contains(&AuthorityEffect::OpenAdmission));
    assert!(observed.contains(&AuthorityEffect::PublishReady));
    logic_endpoint.shutdown().await.unwrap();
    coordinator_endpoint.shutdown().await.unwrap();
    logic_shutdown_tx.send(true).unwrap();
    leader_shutdown_tx.send(true).unwrap();
    logic_task.await.unwrap().unwrap();
    leader_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn persisted_handoff_barrier_replaces_claim_forward() {
    let cluster_id = ClusterId::new("handoff-test").unwrap();
    let (coordinator_node, _) = node(&cluster_id, "coordinator", 26100, 100);
    let (source, _) = node(&cluster_id, "source", 26101, 101);
    let (target, _) = node(&cluster_id, "target", 26102, 102);
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
        10,
    );
    let target_key = attach_test_session(
        &associations,
        &cluster_id,
        coordinator_node.incarnation,
        &target,
        20,
    );
    let entity_type = EntityType::new("handoff-entity").unwrap();
    let slot_key = PlacementSlotKey::Shard {
        entity_type: entity_type.clone(),
        shard_id: ShardId::new(1),
    };
    let store = Arc::new(InMemoryPlacementStore::new(16, 16).unwrap());
    store
        .compare_and_put_slot(
            None,
            PlacementSlot {
                key: slot_key.clone(),
                config_fingerprint: ConfigFingerprint::new([9; 32]),
                owner: Some(source.clone()),
                target: None,
                assignment_generation: AssignmentGeneration::new(1).unwrap(),
                coordinator_term: CoordinatorTerm::new(1).unwrap(),
                revision: Revision::new(1).unwrap(),
                state: PlacementSlotState::Running,
                active_move: None,
                barrier_sessions: Default::default(),
            },
        )
        .await
        .unwrap();
    let mut leader = CoordinatorLeader::elect(
        store.clone(),
        associations,
        coordinator_node,
        CoordinatorTerm::new(1).unwrap(),
        2,
        CoordinatorLeaderConfig::default(),
    )
    .await
    .unwrap();
    let protocol_id = lattice_core::actor_ref::ProtocolId::new(77).unwrap();
    let entity_config = crate::region::EntityConfig::new(
        entity_type.clone(),
        protocol_id,
        8,
        "weighted-least-load",
        1,
        Vec::new(),
    )
    .unwrap();
    let descriptor = lattice_remoting::protocol::ProtocolDescriptor {
        protocol_id,
        fingerprint: lattice_remoting::protocol::ProtocolFingerprint::new([7; 32]),
    };
    let hello = |node: NodeKey| NodeHello {
        node,
        roles: Default::default(),
        capacity_units: 10,
        hosted_entity_types: [entity_type.clone()].into_iter().collect(),
        proxied_entity_types: Default::default(),
        singleton_eligibility: Default::default(),
        used_singletons: Default::default(),
        protocols: vec![descriptor.clone()],
        entity_configs: vec![entity_config.clone()],
        singleton_configs: Vec::new(),
    };
    register_up(&mut leader, hello(source.clone()), source_key).await;
    register_up(&mut leader, hello(target.clone()), target_key).await;
    let relocation = ManualRelocationRequest {
        operation_id: "manual-1".to_owned(),
        entity_type: entity_type.clone(),
        shard_id: ShardId::new(1),
        expected_generation: AssignmentGeneration::new(1).unwrap(),
        target_node_id: target.node_id.clone(),
    };
    let plan_id = leader.manual_relocate(relocation.clone()).await.unwrap();
    assert_eq!(
        leader.manual_relocate(relocation.clone()).await.unwrap(),
        plan_id
    );
    assert!(matches!(
        leader
            .manual_relocate(ManualRelocationRequest {
                target_node_id: source.node_id.clone(),
                ..relocation
            })
            .await,
        Err(CoordinatorRuntimeError::IdempotencyConflict)
    ));
    let barrier_revision = leader.handoffs[&slot_key].barrier_revision();
    leader
        .transition_handoff(
            slot_key.clone(),
            HandoffEvent::AppliedRevision {
                session: source.incarnation,
                revision: barrier_revision,
            },
        )
        .await
        .unwrap();
    leader
        .transition_handoff(
            slot_key.clone(),
            HandoffEvent::AppliedRevision {
                session: target.incarnation,
                revision: barrier_revision,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        store.get_slot(&slot_key).await.unwrap().unwrap().state,
        PlacementSlotState::Stopping
    );
    leader
        .transition_handoff(
            slot_key.clone(),
            HandoffEvent::SourceDrained {
                source,
                generation: AssignmentGeneration::new(1).unwrap(),
            },
        )
        .await
        .unwrap();
    let allocating = store.get_slot(&slot_key).await.unwrap().unwrap();
    assert_eq!(allocating.state, PlacementSlotState::Allocating);
    assert_eq!(allocating.owner.as_ref(), Some(&target));
    assert_eq!(
        store.get_claim(&slot_key).await.unwrap().unwrap().owner,
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
    let plan = store.get_plan(plan_id).await.unwrap().unwrap();
    assert_eq!(plan.status, PlanStatus::Completed);
}

#[tokio::test]
async fn first_resolution_allocates_shard_and_singleton_to_declared_host() {
    let cluster_id = ClusterId::new("allocation-test").unwrap();
    let (coordinator_node, _) = node(&cluster_id, "coordinator", 26200, 200);
    let (proxy, _) = node(&cluster_id, "proxy", 26201, 201);
    let (host, _) = node(&cluster_id, "host", 26202, 202);
    let associations = Arc::new(
        AssociationManager::new(
            coordinator_node.address.clone(),
            coordinator_node.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let proxy_key = attach_test_session(
        &associations,
        &cluster_id,
        coordinator_node.incarnation,
        &proxy,
        30,
    );
    let host_key = attach_test_session(
        &associations,
        &cluster_id,
        coordinator_node.incarnation,
        &host,
        40,
    );
    let store = Arc::new(InMemoryPlacementStore::new(16, 16).unwrap());
    let mut leader = CoordinatorLeader::elect(
        store.clone(),
        associations,
        coordinator_node,
        CoordinatorTerm::new(1).unwrap(),
        2,
        CoordinatorLeaderConfig::default(),
    )
    .await
    .unwrap();
    let entity_type = EntityType::new("allocated-entity").unwrap();
    let protocol_id = lattice_core::actor_ref::ProtocolId::new(55).unwrap();
    let entity_config = crate::region::EntityConfig::new(
        entity_type.clone(),
        protocol_id,
        8,
        "weighted-least-load",
        1,
        Vec::new(),
    )
    .unwrap();
    let singleton_kind =
        lattice_core::actor_ref::SingletonKind::new("allocated-singleton").unwrap();
    let singleton_config = SingletonConfig {
        kind: singleton_kind.clone(),
        protocol_id,
        config_fingerprint: ConfigFingerprint::new([6; 32]),
    };
    let descriptor = lattice_remoting::protocol::ProtocolDescriptor {
        protocol_id,
        fingerprint: lattice_remoting::protocol::ProtocolFingerprint::new([8; 32]),
    };
    register_up(
        &mut leader,
        NodeHello {
            node: proxy,
            roles: Default::default(),
            capacity_units: 1,
            hosted_entity_types: Default::default(),
            proxied_entity_types: [entity_type.clone()].into_iter().collect(),
            singleton_eligibility: Default::default(),
            used_singletons: [singleton_kind.clone()].into_iter().collect(),
            protocols: vec![descriptor.clone()],
            entity_configs: Vec::new(),
            singleton_configs: Vec::new(),
        },
        proxy_key,
    )
    .await;
    register_up(
        &mut leader,
        NodeHello {
            node: host.clone(),
            roles: Default::default(),
            capacity_units: 10,
            hosted_entity_types: [entity_type.clone()].into_iter().collect(),
            proxied_entity_types: Default::default(),
            singleton_eligibility: [singleton_kind.clone()].into_iter().collect(),
            used_singletons: Default::default(),
            protocols: vec![descriptor],
            entity_configs: vec![entity_config],
            singleton_configs: vec![singleton_config],
        },
        host_key,
    )
    .await;
    leader
        .ensure_shard_allocated(entity_type.clone(), ShardId::new(3))
        .await
        .unwrap();
    let shard_key = PlacementSlotKey::Shard {
        entity_type,
        shard_id: ShardId::new(3),
    };
    let shard = store.get_slot(&shard_key).await.unwrap().unwrap();
    assert_eq!(shard.owner.as_ref(), Some(&host));
    assert_eq!(shard.state, PlacementSlotState::Allocating);
    leader
        .complete_initial_ready(&shard_key, &host, shard.assignment_generation)
        .await
        .unwrap();
    leader
        .ensure_singleton_allocated(singleton_kind.clone())
        .await
        .unwrap();
    let singleton_key = PlacementSlotKey::Singleton(singleton_kind);
    let singleton = store.get_slot(&singleton_key).await.unwrap().unwrap();
    assert_eq!(singleton.owner.as_ref(), Some(&host));
    leader
        .complete_initial_ready(&singleton_key, &host, singleton.assignment_generation)
        .await
        .unwrap();
    assert_eq!(
        store.get_slot(&shard_key).await.unwrap().unwrap().state,
        PlacementSlotState::Running
    );
    assert_eq!(
        store.get_slot(&singleton_key).await.unwrap().unwrap().state,
        PlacementSlotState::Running
    );
}

#[tokio::test]
async fn admin_pause_is_idempotent_fingerprinted_and_inspectable() {
    let cluster_id = ClusterId::new("admin-test").unwrap();
    let (coordinator, _) = node(&cluster_id, "coordinator", 26300, 300);
    let associations = Arc::new(
        AssociationManager::new(
            coordinator.address.clone(),
            coordinator.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let store = Arc::new(InMemoryPlacementStore::new(8, 8).unwrap());
    let mut leader = CoordinatorLeader::elect(
        store,
        associations,
        coordinator,
        CoordinatorTerm::new(1).unwrap(),
        2,
        CoordinatorLeaderConfig::default(),
    )
    .await
    .unwrap();
    let entity_type = EntityType::new("admin-entity").unwrap();
    leader
        .set_automatic_paused("pause-1".to_owned(), Some(entity_type.clone()), true)
        .await
        .unwrap();
    leader
        .set_automatic_paused("pause-1".to_owned(), Some(entity_type.clone()), true)
        .await
        .unwrap();
    assert!(matches!(
        leader
            .set_automatic_paused("pause-1".to_owned(), Some(entity_type.clone()), false)
            .await,
        Err(CoordinatorRuntimeError::IdempotencyConflict)
    ));
    let inspection = leader.inspect().await.unwrap();
    assert_eq!(inspection.term, CoordinatorTerm::new(1).unwrap());
    assert_eq!(inspection.paused_entity_types, vec![entity_type]);

    leader
        .record_admin_operation("relocate-1".to_owned(), "move:a".to_owned(), Some(42))
        .unwrap();
    assert_eq!(
        leader
            .prior_admin_operation("relocate-1", "move:a")
            .unwrap(),
        Some(Some(42))
    );
    assert!(matches!(
        leader.prior_admin_operation("relocate-1", "move:b"),
        Err(CoordinatorRuntimeError::IdempotencyConflict)
    ));
}

#[tokio::test]
async fn terminal_plan_history_compacts_oldest_persisted_record() {
    let cluster_id = ClusterId::new("history-test").unwrap();
    let (coordinator, _) = node(&cluster_id, "coordinator", 26310, 310);
    let associations = Arc::new(
        AssociationManager::new(
            coordinator.address.clone(),
            coordinator.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let store = Arc::new(InMemoryPlacementStore::new(8, 8).unwrap());
    let mut leader = CoordinatorLeader::elect(
        store.clone(),
        associations,
        coordinator,
        CoordinatorTerm::new(1).unwrap(),
        2,
        CoordinatorLeaderConfig {
            maximum_completed_plan_history: 2,
            ..CoordinatorLeaderConfig::default()
        },
    )
    .await
    .unwrap();
    let entity_type = EntityType::new("history-entity").unwrap();
    for id in 1..=3_u128 {
        let plan = RebalancePlan {
            plan_id: id,
            entity_type: entity_type.clone(),
            reason: PlanReason::Manual,
            coordinator_term: CoordinatorTerm::new(1).unwrap(),
            base_revision: Revision::new(id as u64).unwrap(),
            revision: Revision::new(1).unwrap(),
            policy_id: "test".to_owned(),
            policy_version: 1,
            status: PlanStatus::Completed,
            moves: Vec::new(),
        };
        store
            .compare_and_put_plan(None, plan.clone(), plan.revision)
            .await
            .unwrap();
        leader.plans.insert(id, plan);
    }
    leader.compact_plan_history().await.unwrap();
    assert!(store.get_plan(1).await.unwrap().is_none());
    assert!(store.get_plan(2).await.unwrap().is_some());
    assert!(store.get_plan(3).await.unwrap().is_some());
    assert_eq!(leader.plans.len(), 2);
}

#[tokio::test]
async fn leader_recovery_resumes_handoff_and_cancels_stale_pending_move() {
    use crate::allocation::{ProposedMove, RebalanceProposal, RebalanceTrigger};

    let cluster_id = ClusterId::new("recovery-test").unwrap();
    let (coordinator, _) = node(&cluster_id, "coordinator", 26300, 300);
    let (source, _) = node(&cluster_id, "source", 26301, 301);
    let (target, _) = node(&cluster_id, "target", 26302, 302);
    let entity_type = EntityType::new("recovery-entity").unwrap();
    let shard_id = ShardId::new(4);
    let proposal = |expected_generation| RebalanceProposal {
        policy_id: "test",
        policy_version: 1,
        base_revision: Revision::new(1).unwrap(),
        trigger: RebalanceTrigger::Manual {
            source: Some(source.clone()),
            target: Some(target.clone()),
            bypass_improvement: true,
        },
        moves: vec![ProposedMove {
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
    started
        .begin_move(shard_id, AssignmentGeneration::new(1).unwrap(), None)
        .unwrap();
    started
        .install_barrier(shard_id, Revision::new(2).unwrap(), Default::default())
        .unwrap();
    let stale = RebalancePlan::from_proposal(
        proposal(AssignmentGeneration::new(9).unwrap()),
        entity_type.clone(),
        CoordinatorTerm::new(1).unwrap(),
        4,
    )
    .unwrap();
    let slot_key = PlacementSlotKey::Shard {
        entity_type,
        shard_id,
    };
    let store = Arc::new(InMemoryPlacementStore::new(8, 8).unwrap());
    store
        .compare_and_put_slot(
            None,
            PlacementSlot {
                key: slot_key.clone(),
                config_fingerprint: ConfigFingerprint::new([7; 32]),
                owner: Some(source),
                target: Some(target),
                assignment_generation: AssignmentGeneration::new(1).unwrap(),
                coordinator_term: CoordinatorTerm::new(1).unwrap(),
                revision: Revision::new(2).unwrap(),
                state: PlacementSlotState::BeginHandoff,
                active_move: Some(started.plan_id),
                barrier_sessions: Default::default(),
            },
        )
        .await
        .unwrap();
    store
        .compare_and_put_plan(None, started.clone(), started.revision)
        .await
        .unwrap();
    store
        .compare_and_put_plan(None, stale.clone(), stale.revision)
        .await
        .unwrap();
    let associations = Arc::new(
        AssociationManager::new(
            coordinator.address.clone(),
            coordinator.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let leader = CoordinatorLeader::elect(
        store.clone(),
        associations,
        coordinator,
        CoordinatorTerm::new(1).unwrap(),
        2,
        CoordinatorLeaderConfig::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        store.get_slot(&slot_key).await.unwrap().unwrap().state,
        PlacementSlotState::Stopping
    );
    assert_eq!(leader.handoffs[&slot_key].phase, HandoffPhase::Draining);
    assert_eq!(
        store.get_plan(stale.plan_id).await.unwrap().unwrap().status,
        PlanStatus::Cancelled
    );
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
    let mut leader = CoordinatorLeader::elect(
        store.clone(),
        associations,
        coordinator,
        CoordinatorTerm::new(1).unwrap(),
        2,
        CoordinatorLeaderConfig {
            member_heartbeat_timeout: Duration::from_millis(10),
            ..CoordinatorLeaderConfig::default()
        },
    )
    .await
    .unwrap();
    let kind = lattice_core::actor_ref::SingletonKind::new("recovering-singleton").unwrap();
    let protocol_id = lattice_core::actor_ref::ProtocolId::new(77).unwrap();
    let singleton_config = SingletonConfig {
        kind: kind.clone(),
        protocol_id,
        config_fingerprint: ConfigFingerprint::new([4; 32]),
    };
    let descriptor = lattice_remoting::protocol::ProtocolDescriptor {
        protocol_id,
        fingerprint: lattice_remoting::protocol::ProtocolFingerprint::new([5; 32]),
    };
    let hello = |node: NodeKey| NodeHello {
        node,
        roles: Default::default(),
        capacity_units: 1,
        hosted_entity_types: Default::default(),
        proxied_entity_types: Default::default(),
        singleton_eligibility: [kind.clone()].into_iter().collect(),
        used_singletons: [kind.clone()].into_iter().collect(),
        protocols: vec![descriptor.clone()],
        entity_configs: Vec::new(),
        singleton_configs: vec![singleton_config.clone()],
    };
    register_up(&mut leader, hello(source.clone()), source_key).await;
    register_up(&mut leader, hello(target.clone()), target_key).await;
    leader
        .ensure_singleton_allocated(kind.clone())
        .await
        .unwrap();
    let slot_key = PlacementSlotKey::Singleton(kind);
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
                revision: persisted.revision,
            },
        )
        .await
        .unwrap();
    let allocating = store.get_slot(&slot_key).await.unwrap().unwrap();
    assert_eq!(allocating.state, PlacementSlotState::Allocating);
    assert_eq!(allocating.owner.as_ref(), Some(&target));
    assert_eq!(allocating.assignment_generation.get(), 2);
    assert_eq!(
        store.get_claim(&slot_key).await.unwrap().unwrap().owner,
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
    let store = Arc::new(InMemoryPlacementStore::new(16, 16).unwrap());
    let mut leader = CoordinatorLeader::elect(
        store.clone(),
        associations,
        coordinator,
        CoordinatorTerm::new(1).unwrap(),
        3,
        CoordinatorLeaderConfig::default(),
    )
    .await
    .unwrap();

    leader
        .register(empty_hello(joining.clone()), joining_key.clone())
        .await
        .unwrap();
    let joining_revision = leader.revision;
    assert_eq!(
        store.get_member("joining").await.unwrap().unwrap().status,
        MemberStatus::Joining
    );
    assert!(matches!(
        leader
            .mark_member_up(
                joining.incarnation,
                joining_revision.next().unwrap(),
                &joining_key,
            )
            .await,
        Err(CoordinatorRuntimeError::StaleMember)
    ));
    leader
        .mark_member_up(joining.incarnation, joining_revision, &joining_key)
        .await
        .unwrap();
    let up = store.get_member("joining").await.unwrap().unwrap();
    assert_eq!(up.status, MemberStatus::Up);
    leader
        .mark_member_up(joining.incarnation, joining_revision, &joining_key)
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
        store.get_member("joining").await.unwrap().unwrap().status,
        MemberStatus::Leaving
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
    assert!(store.get_member("joining").await.unwrap().is_none());

    register_up(&mut leader, empty_hello(forced.clone()), forced_key).await;
    let request = ForceRemoveRequest {
        operation_id: "force-1".to_string(),
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
    leader.force_remove(request).await.unwrap();
    assert!(store.get_member("forced").await.unwrap().is_none());

    register_up(&mut leader, empty_hello(old_reused.clone()), old_reused_key).await;
    assert!(
        leader
            .register(empty_hello(new_reused.clone()), new_reused_key.clone())
            .await
            .is_err()
    );
    leader
        .sessions
        .get_mut(&old_reused.incarnation)
        .unwrap()
        .last_heartbeat = Instant::now() - Duration::from_secs(60);
    leader
        .register(empty_hello(new_reused.clone()), new_reused_key)
        .await
        .unwrap();
    let current = store.get_member("reused").await.unwrap().unwrap();
    assert_eq!(current.node, new_reused);
    assert_eq!(current.status, MemberStatus::Joining);
}

#[tokio::test]
async fn member_store_allows_one_incarnation_and_exact_record_cas_only() {
    let store = InMemoryPlacementStore::new(8, 8).unwrap();
    store.ensure_schema_generation().await.unwrap();
    let first_lease = store.grant_lease(Duration::from_secs(5)).await.unwrap();
    let second_lease = store.grant_lease(Duration::from_secs(5)).await.unwrap();
    let cluster = ClusterId::new("member-store").unwrap();
    let (first, _) = node(&cluster, "same-id", 30200, 1);
    let (second, _) = node(&cluster, "same-id", 30201, 2);
    let joining = MemberRecord {
        node: first.clone(),
        hello: empty_hello(first),
        status: MemberStatus::Joining,
        revision: Revision::new(1).unwrap(),
        lease_id: first_lease,
    };
    let replacement = MemberRecord {
        node: second.clone(),
        hello: empty_hello(second),
        status: MemberStatus::Joining,
        revision: Revision::new(2).unwrap(),
        lease_id: second_lease,
    };
    store.create_member(&joining).await.unwrap();
    assert!(matches!(
        store.create_member(&replacement).await,
        Err(StorageError::IncarnationConflict)
    ));
    let mut up = joining.clone();
    up.status = MemberStatus::Up;
    up.revision = Revision::new(2).unwrap();
    store.compare_and_put_member(&joining, &up).await.unwrap();
    assert!(matches!(
        store.compare_and_delete_member(&joining).await,
        Err(StorageError::CompareFailed)
    ));
    store.compare_and_delete_member(&up).await.unwrap();
    store.create_member(&replacement).await.unwrap();
}
