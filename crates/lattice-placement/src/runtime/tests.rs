use super::*;
use async_trait::async_trait;
use lattice_core::actor_ref::{
    ActorRef, ClusterId, ConfigFingerprint, EntityType, NodeAddress, NodeIncarnation,
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
use crate::storage::domain::{
    ActivateAuthority, AllocateInitial, CreatePlan, LeasedClaim, ReserveMove,
};
use crate::storage::{InMemoryPlacementStore, PlacementStore};
use crate::types::{
    AssignmentGeneration, GrantSequence, PlacementSlot, PlacementSlotState, PlanRevision, Revision,
    ShardId, StateVersion,
};

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
        .mark_member_up(incarnation, leader.version, &association)
        .await
        .unwrap();
}

async fn seed_running_slot(
    leader: &mut CoordinatorLeader<InMemoryPlacementStore>,
    mut slot: PlacementSlot,
) {
    let owner = slot.owner.clone().unwrap();
    slot.version.term = leader.leader.term;
    slot.state = PlacementSlotState::Allocating;
    slot.active_move = None;
    slot.target = None;
    slot.version = leader.next_version().unwrap();
    let lease_id = leader
        .store
        .grant_lease(leader.config.claim_ttl)
        .await
        .unwrap();
    let grant = ClaimGrant {
        slot: slot.key.clone(),
        owner,
        coordinator_term: leader.leader.term,
        assignment_generation: slot.assignment_generation,
        grant_sequence: GrantSequence::new(1).unwrap(),
        ttl: leader.config.claim_ttl,
    };
    let committed = leader
        .store
        .allocate_initial(
            &leader.leader_guard,
            AllocateInitial {
                slot,
                claim: LeasedClaim {
                    grant: grant.clone(),
                    lease_id,
                },
            },
        )
        .await
        .unwrap();
    leader.version = committed.slot.version;
    leader.claims.insert(
        committed.slot.key.clone(),
        ClaimLease {
            lease_id,
            grant: grant.clone(),
        },
    );
    let expected_slot = committed.slot;
    let mut running = expected_slot.clone();
    running.state = PlacementSlotState::Running;
    running.version = leader.next_version().unwrap();
    let committed = leader
        .store
        .activate_authority(
            &leader.leader_guard,
            ActivateAuthority {
                expected_slot,
                expected_claim: grant,
                slot: running,
            },
        )
        .await
        .unwrap();
    leader.version = committed.slot.version;
}

struct NoActors;

#[async_trait]
impl InboundDispatch for NoActors {
    async fn tell(
        &self,
        _sender: Option<ActorRef>,
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
    let mut leader = CoordinatorLeader::elect(
        store.clone(),
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
    seed_running_slot(
        &mut leader,
        PlacementSlot {
            key: slot_key.clone(),
            config_fingerprint: ConfigFingerprint::new([7; 32]),
            owner: Some(logic_node.clone()),
            target: None,
            assignment_generation: AssignmentGeneration::new(1).unwrap(),
            version: StateVersion::new(CoordinatorTerm::new(1).unwrap(), Revision::new(1).unwrap()),
            state: PlacementSlotState::Running,
            active_move: None,
            barrier_sessions: Default::default(),
        },
    )
    .await;
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
    seed_running_slot(
        &mut leader,
        PlacementSlot {
            key: slot_key.clone(),
            config_fingerprint: ConfigFingerprint::new([9; 32]),
            owner: Some(source.clone()),
            target: None,
            assignment_generation: AssignmentGeneration::new(1).unwrap(),
            version: StateVersion::new(CoordinatorTerm::new(1).unwrap(), Revision::new(1).unwrap()),
            state: PlacementSlotState::Running,
            active_move: None,
            barrier_sessions: Default::default(),
        },
    )
    .await;
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
                ..relocation.clone()
            })
            .await,
        Err(CoordinatorRuntimeError::IdempotencyConflict)
    ));
    let barrier_version = leader.handoffs[&slot_key].barrier_version();
    leader
        .transition_handoff(
            slot_key.clone(),
            HandoffEvent::AppliedRevision {
                session: source.incarnation,
                version: barrier_version,
            },
        )
        .await
        .unwrap();
    leader
        .transition_handoff(
            slot_key.clone(),
            HandoffEvent::AppliedRevision {
                session: target.incarnation,
                version: barrier_version,
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
    let plan = store.get_plan(plan_id).await.unwrap().unwrap();
    assert_eq!(plan.status, PlanStatus::Completed);
    store.revoke_lease(leader.leader_lease_id).await.unwrap();
    let (successor_node, _) = node(&cluster_id, "successor", 26203, 203);
    let mut successor = CoordinatorLeader::elect(
        store,
        Arc::new(
            AssociationManager::new(
                successor_node.address.clone(),
                successor_node.incarnation,
                RemotingConfig::default(),
            )
            .unwrap(),
        ),
        successor_node,
        CoordinatorTerm::new(2).unwrap(),
        2,
        CoordinatorLeaderConfig::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        successor.manual_relocate(relocation.clone()).await.unwrap(),
        plan_id
    );
    assert!(matches!(
        successor
            .manual_relocate(ManualRelocationRequest {
                target_node_id: "different-target".to_owned(),
                ..relocation
            })
            .await,
        Err(CoordinatorRuntimeError::IdempotencyConflict)
    ));
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
        store.clone(),
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
    assert_eq!(inspection.version.term, CoordinatorTerm::new(1).unwrap());
    assert_eq!(inspection.paused_entity_types, vec![entity_type]);

    assert!(store.get_automatic_settings().await.unwrap().is_some());
    assert!(
        store
            .get_admin_operation("pause-1")
            .await
            .unwrap()
            .is_some()
    );
    assert!(matches!(
        leader.prior_admin_operation("pause-1", "move:b"),
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
            base_version: StateVersion::new(
                CoordinatorTerm::new(1).unwrap(),
                Revision::new(id as u64).unwrap(),
            ),
            record_revision: PlanRevision::new(1).unwrap(),
            policy_id: "test".to_owned(),
            policy_version: 1,
            status: PlanStatus::Completed,
            moves: Vec::new(),
        };
        store
            .create_plan(&leader.leader_guard, CreatePlan { plan: plan.clone() })
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
async fn leader_recovery_resumes_persisted_handoff() {
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
        base_version: StateVersion::new(
            CoordinatorTerm::new(1).unwrap(),
            Revision::new(1).unwrap(),
        ),
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
    let pending = started.clone();
    started
        .begin_move(shard_id, AssignmentGeneration::new(1).unwrap(), None)
        .unwrap();
    let slot_key = PlacementSlotKey::Shard {
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
    let mut leader = CoordinatorLeader::elect(
        store.clone(),
        associations,
        coordinator,
        CoordinatorTerm::new(1).unwrap(),
        2,
        CoordinatorLeaderConfig::default(),
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
            version: StateVersion::new(CoordinatorTerm::new(1).unwrap(), Revision::new(1).unwrap()),
            state: PlacementSlotState::Running,
            active_move: None,
            barrier_sessions: Default::default(),
        },
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
        .install_barrier(shard_id, barrier_version, Default::default())
        .unwrap();
    started.record_revision = started.record_revision.next().unwrap();
    let expected_slot = store.get_slot(&slot_key).await.unwrap().unwrap();
    let mut handoff_slot = expected_slot.clone();
    handoff_slot.target = Some(target);
    handoff_slot.state = PlacementSlotState::BeginHandoff;
    handoff_slot.active_move = Some(started.plan_id);
    handoff_slot.version = barrier_version;
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

mod lifecycle_tests;
