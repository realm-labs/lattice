use super::*;
use async_trait::async_trait;
use lattice_core::actor_ref::{
    ActorRef, ClusterId, ConfigFingerprint, EntityType, NodeAddress, NodeIncarnation,
    PlacementDomainId,
};
use std::collections::BTreeSet;

fn domain() -> PlacementDomainId {
    PlacementDomainId::new("runtime-test").unwrap()
}
use lattice_remoting::config::RemotingConfig;
use lattice_remoting::endpoint::RemotingEndpoint;
use lattice_remoting::handshake::NodeIdentity;
use lattice_remoting::messaging::error::RemoteMessageError;
use lattice_remoting::messaging::inbound::InboundDispatch;
use lattice_remoting::messaging::outbound::OutboundMessaging;
use lattice_remoting::messaging::target::ExactActorTarget;

use crate::authority::AuthorityEffect;
use crate::control::PlacementControlRouter;
use crate::coordinator::{MemberHello, MembershipLeaderGuard};
use crate::session::{LogicCoordinatorConfig, PlacementDomainSession};
use crate::storage::domain::{
    ActivateAuthority, AllocateInitial, CreateDomainMember, CreateMember, CreatePlan, LeasedClaim,
    ReserveMove,
};
use crate::storage::{InMemoryPlacementStore, PlacementDomainStore};
use crate::types::{
    AssignmentGeneration, GrantSequence, PlacementSlot, PlacementSlotState, PlacementVersion,
    PlanRevision, Revision, ShardId,
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

async fn ensure_test_global_member(
    leader: &mut PlacementDomainLeader<InMemoryPlacementStore>,
    hello: &MemberHello,
) -> MemberRecord {
    if leader
        .store
        .get_member(&hello.node.node_id)
        .await
        .unwrap()
        .is_none()
    {
        let membership_record = if let Some(record) = leader
            .store
            .get_leader(&CoordinatorScope::Membership)
            .await
            .unwrap()
        {
            record
        } else {
            let lease = leader
                .store
                .grant_lease(Duration::from_secs(30))
                .await
                .unwrap();
            let record = LeaderRecord {
                scope: CoordinatorScope::Membership,
                node: leader.leader.node.clone(),
                protocol_generation: COORDINATOR_PROTOCOL_GENERATION,
                term: CoordinatorTerm::new(1).unwrap(),
            };
            assert!(leader.store.campaign_leader(&record, lease).await.unwrap());
            record
        };
        let member_lease = leader
            .store
            .grant_lease(Duration::from_secs(30))
            .await
            .unwrap();
        let member = MemberRecord {
            node: hello.node.clone(),
            hello: hello.clone(),
            status: MemberStatus::Up,
            version: MembershipVersion::new(
                membership_record.term,
                leader
                    .store
                    .get_membership_revision()
                    .await
                    .unwrap()
                    .next()
                    .unwrap(),
            ),
            lease_id: member_lease,
        };
        leader
            .store
            .create_member(
                &MembershipLeaderGuard::new(membership_record).unwrap(),
                CreateMember { member },
            )
            .await
            .unwrap();
    }
    leader
        .store
        .get_member(&hello.node.node_id)
        .await
        .unwrap()
        .unwrap()
}

async fn register_up(
    leader: &mut PlacementDomainLeader<InMemoryPlacementStore>,
    hello: TestHello,
    association: lattice_remoting::association::AssociationKey,
) {
    let incarnation = hello.member.node.incarnation;
    ensure_test_global_member(leader, &hello.member).await;
    leader
        .register(hello.domain, association.clone())
        .await
        .unwrap();
    leader
        .mark_member_up(incarnation, leader.membership_version, &association)
        .await
        .unwrap();
}

async fn seed_running_slot(
    leader: &mut PlacementDomainLeader<InMemoryPlacementStore>,
    mut slot: PlacementSlot,
    authority_hello: Option<&TestHello>,
) {
    let owner = slot.owner.clone().unwrap();
    let member_hello = authority_hello
        .map(|hello| hello.member.clone())
        .unwrap_or_else(|| MemberHello {
            node: owner.clone(),
            roles: BTreeSet::new(),
            failure_domains: BTreeMap::new(),
            protocols: Vec::new(),
            remoting_capabilities: BTreeSet::new(),
        });
    let expected_global_member = ensure_test_global_member(leader, &member_hello).await;
    let expected_domain_member = if let Some(member) = leader
        .store
        .get_domain_member(&leader.version.domain, &owner.node_id)
        .await
        .unwrap()
    {
        member
    } else {
        let member = DomainMemberRecord {
            node: owner.clone(),
            hello: authority_hello
                .map(|hello| hello.domain.clone())
                .unwrap_or_else(|| {
                    PlacementDomainHello::new(
                        owner.clone(),
                        leader.version.domain.clone(),
                        1,
                        BTreeSet::new(),
                        BTreeSet::new(),
                        BTreeSet::new(),
                        BTreeSet::new(),
                        Vec::new(),
                        Vec::new(),
                        BTreeMap::new(),
                    )
                }),
            status: DomainMemberStatus::Up,
            version: leader.next_version().unwrap(),
        };
        let committed = leader
            .store
            .create_domain_member(
                &leader.leader_guard,
                CreateDomainMember {
                    expected_global_member: expected_global_member.clone(),
                    member,
                },
            )
            .await
            .unwrap();
        leader.version = committed.member.version.clone();
        committed.member
    };
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
        domain: slot.key.domain().clone(),
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
                expected_global_member,
                expected_domain_member,
                slot,
                claim: LeasedClaim {
                    grant: grant.clone(),
                    lease_id,
                },
            },
        )
        .await
        .unwrap();
    leader.version = committed.slot.version.clone();
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

#[derive(Clone)]
struct TestHello {
    member: MemberHello,
    domain: PlacementDomainHello,
}

#[allow(clippy::too_many_arguments)]
fn test_hello(
    node: NodeKey,
    roles: BTreeSet<String>,
    capacity_units: u64,
    hosted_entity_types: BTreeSet<EntityType>,
    proxied_entity_types: BTreeSet<EntityType>,
    singleton_eligibility: BTreeSet<lattice_core::actor_ref::SingletonKind>,
    used_singletons: BTreeSet<lattice_core::actor_ref::SingletonKind>,
    protocols: Vec<lattice_remoting::protocol::ProtocolDescriptor>,
    entity_configs: Vec<crate::region::EntityConfig>,
    singleton_configs: Vec<SingletonConfig>,
) -> TestHello {
    TestHello {
        member: MemberHello {
            node: node.clone(),
            roles,
            failure_domains: Default::default(),
            protocols,
            remoting_capabilities: Default::default(),
        },
        domain: PlacementDomainHello::new(
            node,
            domain(),
            capacity_units,
            hosted_entity_types,
            proxied_entity_types,
            singleton_eligibility,
            used_singletons,
            entity_configs,
            singleton_configs,
            Default::default(),
        ),
    }
}

fn empty_hello(node: NodeKey) -> TestHello {
    test_hello(
        node,
        Default::default(),
        1,
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
}

#[tokio::test]
async fn joining_domain_member_advances_existing_sessions_to_latest_revision() {
    let cluster_id = ClusterId::new("domain-join-revision-test").unwrap();
    let (coordinator, _) = node(&cluster_id, "coordinator", 26000, 10);
    let (first, _) = node(&cluster_id, "first", 26001, 11);
    let (second, _) = node(&cluster_id, "second", 26002, 12);
    let associations = Arc::new(
        AssociationManager::new(
            coordinator.address.clone(),
            coordinator.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let first_key = attach_test_session(
        &associations,
        &cluster_id,
        coordinator.incarnation,
        &first,
        100,
    );
    let second_key = attach_test_session(
        &associations,
        &cluster_id,
        coordinator.incarnation,
        &second,
        200,
    );
    let store = Arc::new(InMemoryPlacementStore::new(16, 16).unwrap());
    let mut leader = PlacementDomainLeader::elect(
        store,
        associations.clone(),
        coordinator,
        CoordinatorScope::Placement(domain()),
        CoordinatorTerm::new(1).unwrap(),
        PlacementDomainLeaderConfig::default(),
    )
    .await
    .unwrap();

    register_up(&mut leader, empty_hello(first.clone()), first_key.clone()).await;
    let applied_version = leader.version.clone();
    leader
        .sessions
        .get_mut(&first.incarnation)
        .unwrap()
        .applied_version = Some(applied_version);
    let first_association = associations.get(&first_key).unwrap();
    let mut first_control = first_association
        .take_lane_receiver(lattice_remoting::association::LaneKind::Control)
        .unwrap();
    while first_control.try_recv().is_ok() {}

    register_up(&mut leader, empty_hello(second), second_key).await;

    let mut delta_versions = Vec::new();
    while let Ok(frame) = first_control.try_recv() {
        let envelope = lattice_remoting::control::decode_control_envelope(&frame).unwrap();
        let scoped =
            crate::control::decode_control_command(&envelope.payload, DEFAULT_MAX_CONTROL_PAYLOAD)
                .unwrap();
        if let PlacementControlCommand::StateDelta(delta) = scoped.command {
            assert!(delta.records.is_empty());
            delta_versions.push(delta.version);
        }
    }

    assert_eq!(delta_versions, vec![leader.version.clone()]);
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
    let coordinator_to_logic = lattice_remoting::association::AssociationKey {
        cluster_id: logic_to_coordinator.key().cluster_id.clone(),
        local_incarnation: logic_to_coordinator.key().remote_incarnation,
        remote_address: logic_node.address.clone(),
        remote_incarnation: logic_to_coordinator.key().local_incarnation,
    };

    let entity_type = EntityType::new("player").unwrap();
    let slot_key = PlacementSlotKey::Shard {
        domain: domain(),
        entity_type: entity_type.clone(),
        shard_id: ShardId::new(3),
    };
    let store = Arc::new(InMemoryPlacementStore::new(16, 16).unwrap());
    let mut leader = PlacementDomainLeader::elect(
        store.clone(),
        coordinator_associations,
        coordinator_node,
        CoordinatorScope::Placement(domain()),
        CoordinatorTerm::new(1).unwrap(),
        PlacementDomainLeaderConfig {
            renewal_interval: Duration::from_secs(1),
            ..PlacementDomainLeaderConfig::default()
        },
    )
    .await
    .unwrap();
    let hello = test_hello(
        logic_node.clone(),
        ["logic".to_owned()].into_iter().collect(),
        1,
        [entity_type.clone()].into_iter().collect(),
        Default::default(),
        Default::default(),
        Default::default(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    seed_running_slot(
        &mut leader,
        PlacementSlot {
            key: slot_key.clone(),
            config_fingerprint: ConfigFingerprint::new([7; 32]),
            owner: Some(logic_node.clone()),
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
        Some(&hello),
    )
    .await;
    leader
        .register(hello.domain.clone(), coordinator_to_logic)
        .await
        .unwrap();
    let (logic, mut effects) = PlacementDomainSession::new(
        hello.domain,
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
        domain: domain(),
        entity_type: entity_type.clone(),
        shard_id: ShardId::new(1),
    };
    let store = Arc::new(InMemoryPlacementStore::new(16, 16).unwrap());
    let mut leader = PlacementDomainLeader::elect(
        store.clone(),
        associations,
        coordinator_node,
        CoordinatorScope::Placement(domain()),
        CoordinatorTerm::new(1).unwrap(),
        PlacementDomainLeaderConfig::default(),
    )
    .await
    .unwrap();
    let protocol_id = lattice_core::actor_ref::ProtocolId::new(77).unwrap();
    let entity_config = crate::region::EntityConfig::new(
        domain(),
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
    let hello = |node: NodeKey| {
        test_hello(
            node,
            Default::default(),
            10,
            [entity_type.clone()].into_iter().collect(),
            Default::default(),
            Default::default(),
            Default::default(),
            vec![descriptor.clone()],
            vec![entity_config.clone()],
            Vec::new(),
        )
    };
    let source_hello = hello(source.clone());
    let target_hello = hello(target.clone());
    seed_running_slot(
        &mut leader,
        PlacementSlot {
            key: slot_key.clone(),
            config_fingerprint: ConfigFingerprint::new([9; 32]),
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
        Some(&source_hello),
    )
    .await;
    register_up(&mut leader, source_hello, source_key).await;
    register_up(&mut leader, target_hello, target_key).await;
    let relocation = ManualRelocationRequest {
        domain: domain(),
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
                version: barrier_version.clone(),
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
    let plan = store.get_plan(&domain(), plan_id).await.unwrap().unwrap();
    assert_eq!(plan.status, PlanStatus::Completed);
    store.revoke_lease(leader.leader_lease_id).await.unwrap();
    let (successor_node, _) = node(&cluster_id, "successor", 26203, 203);
    let mut successor = PlacementDomainLeader::elect(
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
        CoordinatorScope::Placement(domain()),
        CoordinatorTerm::new(2).unwrap(),
        PlacementDomainLeaderConfig::default(),
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
    let mut leader = PlacementDomainLeader::elect(
        store.clone(),
        associations,
        coordinator_node,
        CoordinatorScope::Placement(domain()),
        CoordinatorTerm::new(1).unwrap(),
        PlacementDomainLeaderConfig::default(),
    )
    .await
    .unwrap();
    let entity_type = EntityType::new("allocated-entity").unwrap();
    let protocol_id = lattice_core::actor_ref::ProtocolId::new(55).unwrap();
    let entity_config = crate::region::EntityConfig::new(
        domain(),
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
    let singleton_config = SingletonConfig::new(domain(), singleton_kind.clone(), protocol_id);
    let descriptor = lattice_remoting::protocol::ProtocolDescriptor {
        protocol_id,
        fingerprint: lattice_remoting::protocol::ProtocolFingerprint::new([8; 32]),
    };
    register_up(
        &mut leader,
        test_hello(
            proxy,
            Default::default(),
            1,
            Default::default(),
            [entity_type.clone()].into_iter().collect(),
            Default::default(),
            [singleton_kind.clone()].into_iter().collect(),
            vec![descriptor.clone()],
            Vec::new(),
            Vec::new(),
        ),
        proxy_key,
    )
    .await;
    register_up(
        &mut leader,
        test_hello(
            host.clone(),
            Default::default(),
            10,
            [entity_type.clone()].into_iter().collect(),
            Default::default(),
            [singleton_kind.clone()].into_iter().collect(),
            Default::default(),
            vec![descriptor],
            vec![entity_config],
            vec![singleton_config],
        ),
        host_key,
    )
    .await;
    leader
        .ensure_shard_allocated(entity_type.clone(), ShardId::new(3))
        .await
        .unwrap();
    let shard_key = PlacementSlotKey::Shard {
        domain: domain(),
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
    let singleton_key = PlacementSlotKey::Singleton {
        domain: domain(),
        kind: singleton_kind,
    };
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
async fn resolution_reassigns_fenced_slots_after_owner_restart() {
    let cluster_id = ClusterId::new("fenced-recovery-test").unwrap();
    let (coordinator_node, _) = node(&cluster_id, "coordinator", 26210, 210);
    let (old_host, _) = node(&cluster_id, "host", 26211, 211);
    let (host, _) = node(&cluster_id, "host", 26212, 212);
    let associations = Arc::new(
        AssociationManager::new(
            coordinator_node.address.clone(),
            coordinator_node.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let host_key = attach_test_session(
        &associations,
        &cluster_id,
        coordinator_node.incarnation,
        &host,
        50,
    );
    let store = Arc::new(InMemoryPlacementStore::new(16, 16).unwrap());
    let entity_type = EntityType::new("recovered-entity").unwrap();
    let protocol_id = lattice_core::actor_ref::ProtocolId::new(56).unwrap();
    let entity_config = crate::region::EntityConfig::new(
        domain(),
        entity_type.clone(),
        protocol_id,
        8,
        "weighted-least-load",
        1,
        Vec::new(),
    )
    .unwrap();
    let singleton_kind =
        lattice_core::actor_ref::SingletonKind::new("recovered-singleton").unwrap();
    let singleton_config = SingletonConfig::new(domain(), singleton_kind.clone(), protocol_id);
    let shard_key = PlacementSlotKey::Shard {
        domain: domain(),
        entity_type: entity_type.clone(),
        shard_id: ShardId::new(3),
    };
    let singleton_key = PlacementSlotKey::Singleton {
        domain: domain(),
        kind: singleton_kind.clone(),
    };
    store.insert_generation_three_slot(PlacementSlot {
        key: shard_key.clone(),
        config_fingerprint: entity_config.fingerprint(),
        owner: Some(old_host.clone()),
        target: None,
        assignment_generation: AssignmentGeneration::new(7).unwrap(),
        version: PlacementVersion::new(
            domain(),
            CoordinatorTerm::new(1).unwrap(),
            Revision::new(1).unwrap(),
        ),
        state: PlacementSlotState::Fenced,
        active_move: None,
        barrier_sessions: Default::default(),
    });
    store.insert_generation_three_slot(PlacementSlot {
        key: singleton_key.clone(),
        config_fingerprint: singleton_config.fingerprint(),
        owner: Some(old_host),
        target: None,
        assignment_generation: AssignmentGeneration::new(9).unwrap(),
        version: PlacementVersion::new(
            domain(),
            CoordinatorTerm::new(1).unwrap(),
            Revision::new(2).unwrap(),
        ),
        state: PlacementSlotState::Fenced,
        active_move: None,
        barrier_sessions: Default::default(),
    });
    let mut leader = PlacementDomainLeader::elect(
        store.clone(),
        associations,
        coordinator_node,
        CoordinatorScope::Placement(domain()),
        CoordinatorTerm::new(1).unwrap(),
        PlacementDomainLeaderConfig::default(),
    )
    .await
    .unwrap();
    let descriptor = lattice_remoting::protocol::ProtocolDescriptor {
        protocol_id,
        fingerprint: lattice_remoting::protocol::ProtocolFingerprint::new([9; 32]),
    };
    register_up(
        &mut leader,
        test_hello(
            host.clone(),
            Default::default(),
            10,
            [entity_type.clone()].into_iter().collect(),
            Default::default(),
            [singleton_kind.clone()].into_iter().collect(),
            Default::default(),
            vec![descriptor],
            vec![entity_config],
            vec![singleton_config],
        ),
        host_key,
    )
    .await;

    leader
        .ensure_shard_allocated(entity_type, ShardId::new(3))
        .await
        .unwrap();
    let shard = store.get_slot(&shard_key).await.unwrap().unwrap();
    assert_eq!(shard.owner.as_ref(), Some(&host));
    assert_eq!(shard.assignment_generation.get(), 8);
    assert_eq!(shard.state, PlacementSlotState::Allocating);
    assert!(store.get_claim(&shard_key).await.unwrap().is_some());

    leader
        .ensure_singleton_allocated(singleton_kind)
        .await
        .unwrap();
    let singleton = store.get_slot(&singleton_key).await.unwrap().unwrap();
    assert_eq!(singleton.owner.as_ref(), Some(&host));
    assert_eq!(singleton.assignment_generation.get(), 10);
    assert_eq!(singleton.state, PlacementSlotState::Allocating);
    assert!(store.get_claim(&singleton_key).await.unwrap().is_some());
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

    assert!(
        store
            .get_automatic_settings(&domain())
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .get_admin_operation(&domain(), "pause-1")
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
    let mut leader = PlacementDomainLeader::elect(
        store.clone(),
        associations,
        coordinator,
        CoordinatorScope::Placement(domain()),
        CoordinatorTerm::new(1).unwrap(),
        PlacementDomainLeaderConfig {
            maximum_completed_plan_history: 2,
            ..PlacementDomainLeaderConfig::default()
        },
    )
    .await
    .unwrap();
    let entity_type = EntityType::new("history-entity").unwrap();
    for id in 1..=3_u128 {
        let plan = RebalancePlan {
            domain: domain(),
            plan_id: id,
            entity_type: entity_type.clone(),
            reason: PlanReason::Manual,
            coordinator_term: CoordinatorTerm::new(1).unwrap(),
            base_version: PlacementVersion::new(
                domain(),
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
    assert!(store.get_plan(&domain(), 1).await.unwrap().is_none());
    assert!(store.get_plan(&domain(), 2).await.unwrap().is_some());
    assert!(store.get_plan(&domain(), 3).await.unwrap().is_some());
    assert_eq!(leader.plans.len(), 2);
}

mod lifecycle_tests;
mod recovery_tests;
