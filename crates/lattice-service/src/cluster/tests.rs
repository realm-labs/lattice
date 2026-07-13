use super::*;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::BytesMut;
use lattice_actor::actor_protocol;
use lattice_actor::context::ActorContext;
use lattice_actor::error::ActorError;
use lattice_actor::host::ProtocolHostRegistry;
use lattice_actor::protocol::{CodecDescriptor, DecodeError, EncodeError, WireCodec};
use lattice_actor::registry::{ActorCreateContext, ActorRefConfig, ActorRegistryConfig};
use lattice_actor::reply::ReplyTo;
use lattice_actor::traits::{Request, Responder};
use lattice_core::actor_kind;
use lattice_core::actor_ref::{ClusterId, EntityId, NodeAddress, NodeIncarnation, ProtocolId};
use lattice_placement::control::{
    DEFAULT_MAX_CONTROL_PAYLOAD, PlacementControlCommand, PlacementControlRouter,
    encode_control_command,
};
use lattice_placement::coordinator::{SnapshotLimits, SnapshotRecord, build_snapshot};
use lattice_placement::session::LogicCoordinatorConfig;
use lattice_placement::session::LogicCoordinatorSession;
use lattice_placement::types::AssignmentGeneration;
use lattice_placement::types::ClaimGrant;
use lattice_placement::types::CoordinatorTerm;
use lattice_placement::types::GrantSequence;
use lattice_placement::types::PlacementSlot;
use lattice_placement::types::Revision;
use lattice_remoting::association::AssociationKey;
use lattice_remoting::association::LaneAttachment;
use lattice_remoting::association::LaneKind;
use lattice_remoting::config::RemotingConfig;
use lattice_remoting::control::CommandId;
use lattice_remoting::control::ControlDispatch;
use lattice_remoting::endpoint::RemotingEndpoint;
use lattice_remoting::handshake::NodeIdentity;
use lattice_remoting::protocol::ProtocolDescriptor;
use tokio::sync::watch;

use crate::backend::ServiceInboundDispatch;

const TEST_PROTOCOL_ID: u64 = 77;

#[derive(Clone)]
struct GetValue(u64);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Value(u64);

impl Request for GetValue {
    type Response = Value;
}

#[derive(Clone, Copy)]
struct GetCodec;

impl WireCodec<GetValue> for GetCodec {
    const DESCRIPTOR: CodecDescriptor = CodecDescriptor::new(1, 1);

    fn encode(&self, value: &GetValue, output: &mut BytesMut) -> Result<(), EncodeError> {
        output.extend_from_slice(&value.0.to_be_bytes());
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<GetValue, DecodeError> {
        Ok(GetValue(u64::from_be_bytes(input.try_into().map_err(
            |_| DecodeError::new("GetValue requires eight bytes"),
        )?)))
    }
}

#[derive(Clone, Copy)]
struct ValueCodec;

impl WireCodec<Value> for ValueCodec {
    const DESCRIPTOR: CodecDescriptor = CodecDescriptor::new(1, 1);

    fn encode(&self, value: &Value, output: &mut BytesMut) -> Result<(), EncodeError> {
        output.extend_from_slice(&value.0.to_be_bytes());
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<Value, DecodeError> {
        Ok(Value(u64::from_be_bytes(input.try_into().map_err(
            |_| DecodeError::new("Value requires eight bytes"),
        )?)))
    }
}

struct EntityActor {
    value: u64,
}

#[async_trait]
impl Actor for EntityActor {
    type Error = ActorError;
}

#[async_trait]
impl Responder<GetValue> for EntityActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        request: GetValue,
        reply_to: ReplyTo<Value>,
    ) -> Result<(), ActorError> {
        let _ = reply_to.send(Value(self.value + request.0));
        Ok(())
    }
}

actor_protocol! {
    EntityProtocol for EntityActor {
        protocol_id: TEST_PROTOCOL_ID;
        name: "cluster-router-test/v1";
        ask 1 => GetValue {
            request_schema_version: 1,
            response_schema_version: 1,
            request_codec: GetCodec,
            response_codec: ValueCodec,
        }
    }
}

#[derive(Clone)]
struct CountingLoader(Arc<AtomicUsize>);

#[async_trait]
impl ActorLoader<EntityActor> for CountingLoader {
    async fn load(&self, _ctx: ActorCreateContext) -> Result<EntityActor, ActorError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(EntityActor { value: 40 })
    }
}

fn attach_coordinator(
    associations: &AssociationManager,
    cluster_id: &ClusterId,
    local_incarnation: NodeIncarnation,
    coordinator_address: NodeAddress,
    coordinator_incarnation: NodeIncarnation,
) -> AssociationKey {
    let association = associations
        .get_or_create(
            cluster_id.clone(),
            coordinator_address.clone(),
            coordinator_incarnation,
        )
        .unwrap();
    let key = AssociationKey {
        cluster_id: cluster_id.clone(),
        local_incarnation,
        remote_address: coordinator_address,
        remote_incarnation: coordinator_incarnation,
    };
    for (lane, nonce) in [
        (LaneKind::Control, 1),
        (LaneKind::Interactive, 2),
        (LaneKind::Bulk(0), 3),
    ] {
        association
            .attach(LaneAttachment {
                association_id: association.id(),
                key: key.clone(),
                lane,
                connection_nonce: nonce,
            })
            .unwrap();
    }
    key
}

async fn unused_address() -> NodeAddress {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    NodeAddress::new("127.0.0.1", port).unwrap()
}

async fn stage_logic_runtime(
    hello: lattice_placement::coordinator::NodeHello,
    coordinator: AssociationKey,
    associations: Arc<AssociationManager>,
    slots: Vec<PlacementSlot>,
) -> (
    Arc<Mutex<LogicPlacementState>>,
    Arc<PlacementControlRouter>,
    watch::Sender<bool>,
    tokio::task::JoinHandle<Result<(), lattice_placement::session::LogicSessionError>>,
) {
    let (control, controls) =
        PlacementControlRouter::bounded(64, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
    let control = Arc::new(control);
    let (logic, _effects) = LogicCoordinatorSession::new(
        hello.clone(),
        coordinator.clone(),
        associations,
        LogicCoordinatorConfig::default(),
        64,
    )
    .unwrap();
    for slot in &slots {
        if slot.owner.as_ref() == Some(&hello.node) {
            logic
                .register_authority(slot.key.clone(), Duration::from_millis(10))
                .unwrap();
        }
    }
    let state = logic.state();
    let (shutdown, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(logic.run(controls, shutdown_rx));
    let revision = slots.iter().map(|slot| slot.revision).max().unwrap();
    let records = slots
        .iter()
        .map(|slot| {
            let key = match &slot.key {
                PlacementSlotKey::Shard {
                    entity_type,
                    shard_id,
                } => format!("shard/{}/{}", entity_type.as_str(), shard_id.get()),
                PlacementSlotKey::Singleton(kind) => {
                    format!("singleton/{}", kind.as_str())
                }
            };
            SnapshotRecord {
                key,
                value: serde_json::to_vec(slot).unwrap().into(),
            }
        })
        .collect();
    let limits = SnapshotLimits::default();
    let (begin, chunks, end) = build_snapshot(revision, records, &limits).unwrap();
    let mut commands = vec![PlacementControlCommand::SnapshotBegin(begin)];
    commands.extend(
        chunks
            .into_iter()
            .map(PlacementControlCommand::SnapshotChunk),
    );
    commands.push(PlacementControlCommand::SnapshotEnd(end));
    for slot in slots {
        if slot.owner.as_ref() == Some(&hello.node) {
            commands.push(PlacementControlCommand::ClaimGranted(ClaimGrant {
                slot: slot.key,
                owner: hello.node.clone(),
                coordinator_term: slot.coordinator_term,
                assignment_generation: slot.assignment_generation,
                grant_sequence: GrantSequence::new(1).unwrap(),
                ttl: Duration::from_secs(5),
            }));
        }
    }
    for command in commands {
        control
            .apply(
                coordinator.clone(),
                CommandId::generate(),
                encode_control_command(&command, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap(),
            )
            .await
            .unwrap();
    }
    (state, control, shutdown, task)
}

#[tokio::test]
async fn stale_generation_never_reaches_entity_loader() {
    let cluster_id = ClusterId::new("router-test").unwrap();
    let local_incarnation = NodeIncarnation::new(1).unwrap();
    let coordinator_incarnation = NodeIncarnation::new(2).unwrap();
    let local_address = NodeAddress::new("127.0.0.1", 25570).unwrap();
    let coordinator_address = NodeAddress::new("127.0.0.1", 25571).unwrap();
    let local_node = NodeKey {
        node_id: "logic".to_owned(),
        address: local_address.clone(),
        incarnation: local_incarnation,
    };
    let remoting = RemotingConfig::default();
    let associations = Arc::new(
        AssociationManager::new(local_address.clone(), local_incarnation, remoting.clone())
            .unwrap(),
    );
    let association = associations
        .get_or_create(
            cluster_id.clone(),
            coordinator_address.clone(),
            coordinator_incarnation,
        )
        .unwrap();
    let association_key = AssociationKey {
        cluster_id: cluster_id.clone(),
        local_incarnation,
        remote_address: coordinator_address,
        remote_incarnation: coordinator_incarnation,
    };
    for (lane, nonce) in [
        (LaneKind::Control, 1),
        (LaneKind::Interactive, 2),
        (LaneKind::Bulk(0), 3),
    ] {
        association
            .attach(LaneAttachment {
                association_id: association.id(),
                key: association_key.clone(),
                lane,
                connection_nonce: nonce,
            })
            .unwrap();
    }
    let entity_config = EntityConfig::new(
        EntityType::new("entity").unwrap(),
        ProtocolId::new(TEST_PROTOCOL_ID).unwrap(),
        16,
        "weighted-least-load",
        1,
        Vec::new(),
    )
    .unwrap();
    let entity_id = EntityId::new(b"player-42".to_vec()).unwrap();
    let slot_key = PlacementSlotKey::Shard {
        entity_type: entity_config.entity_type.clone(),
        shard_id: entity_config.shard_for(&entity_id),
    };
    let hello = lattice_placement::coordinator::NodeHello {
        node: local_node.clone(),
        roles: BTreeSet::new(),
        capacity_units: 1,
        hosted_entity_types: [entity_config.entity_type.clone()].into_iter().collect(),
        proxied_entity_types: BTreeSet::new(),
        singleton_eligibility: BTreeSet::new(),
        used_singletons: BTreeSet::new(),
        protocols: Vec::new(),
        entity_configs: Vec::new(),
        singleton_configs: Vec::new(),
    };
    let (control_router, controls) =
        PlacementControlRouter::bounded(32, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
    let control_router = Arc::new(control_router);
    let (logic, _effects) = LogicCoordinatorSession::new(
        hello,
        association_key.clone(),
        associations.clone(),
        LogicCoordinatorConfig::default(),
        32,
    )
    .unwrap();
    let state = logic.state();
    logic
        .register_authority(slot_key.clone(), Duration::from_secs(2))
        .unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let logic_task = tokio::spawn(logic.run(controls, shutdown_rx));
    let slot = PlacementSlot {
        key: slot_key.clone(),
        config_fingerprint: entity_config.fingerprint(),
        owner: Some(local_node.clone()),
        target: None,
        assignment_generation: AssignmentGeneration::new(2).unwrap(),
        coordinator_term: CoordinatorTerm::new(1).unwrap(),
        revision: Revision::new(1).unwrap(),
        state: PlacementSlotState::Running,
        active_move: None,
        barrier_sessions: Default::default(),
    };
    let limits = SnapshotLimits::default();
    let (begin, chunks, end) = build_snapshot(
        Revision::new(1).unwrap(),
        vec![SnapshotRecord {
            key: "shard/entity/0".to_owned(),
            value: Bytes::from(serde_json::to_vec(&slot).unwrap()),
        }],
        &limits,
    )
    .unwrap();
    let commands = std::iter::once(PlacementControlCommand::SnapshotBegin(begin))
        .chain(
            chunks
                .into_iter()
                .map(PlacementControlCommand::SnapshotChunk),
        )
        .chain(std::iter::once(PlacementControlCommand::SnapshotEnd(end)))
        .chain(std::iter::once(PlacementControlCommand::ClaimGranted(
            ClaimGrant {
                slot: slot_key.clone(),
                owner: local_node.clone(),
                coordinator_term: CoordinatorTerm::new(1).unwrap(),
                assignment_generation: AssignmentGeneration::new(2).unwrap(),
                grant_sequence: GrantSequence::new(1).unwrap(),
                ttl: Duration::from_secs(15),
            },
        )));
    for command in commands {
        control_router
            .apply(
                association_key.clone(),
                CommandId::generate(),
                encode_control_command(&command, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap(),
            )
            .await
            .unwrap();
    }
    let protocol = Arc::new(EntityProtocol::build().unwrap());
    let registry = Arc::new(ActorRegistry::new(
        actor_kind!("Entity"),
        ActorRegistryConfig {
            actor_ref: Some(ActorRefConfig {
                cluster_id: cluster_id.clone(),
                node_address: local_address.clone(),
                node_incarnation: local_incarnation,
                protocol_id: ProtocolId::new(TEST_PROTOCOL_ID).unwrap(),
            }),
            ..ActorRegistryConfig::default()
        },
    ));
    let loads = Arc::new(AtomicUsize::new(0));
    let mut router = ClusterLogicalRouter::new(
        local_node.clone(),
        state,
        associations,
        Arc::new(OutboundMessaging::new(8).unwrap()),
        association_key,
        LogicalBufferConfig::default(),
        8,
    )
    .unwrap();
    router
        .register_entity(
            entity_config.clone(),
            registry,
            protocol.clone(),
            CountingLoader(loads.clone()),
        )
        .unwrap();
    let reference = entity_config.entity_ref::<()>(cluster_id, entity_id);
    let (_, request) = protocol
        .encode_request(DispatchMode::Ask, &GetValue(2))
        .unwrap();
    let stale = router
        .receive_entity_ask(
            LogicalEntityTarget {
                reference: reference.clone(),
                owner_address: local_address.clone(),
                owner_incarnation: local_incarnation,
                assignment_generation: 1,
            },
            1,
            request.clone(),
            Instant::now() + Duration::from_secs(1),
        )
        .await;
    assert_eq!(stale.unwrap_err(), RemoteMessageError::StaleAuthority);
    assert_eq!(loads.load(Ordering::SeqCst), 0);
    let reply = router
        .receive_entity_ask(
            LogicalEntityTarget {
                reference,
                owner_address: local_address,
                owner_incarnation: local_incarnation,
                assignment_generation: 2,
            },
            1,
            request,
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(
        protocol.decode_response::<GetValue>(1, &reply).unwrap(),
        Value(42)
    );
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    shutdown_tx.send(true).unwrap();
    logic_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn remote_entity_ask_reaches_only_claimed_owner() {
    let cluster_id = ClusterId::new("remote-entity-test").unwrap();
    let source_address = unused_address().await;
    let owner_address = unused_address().await;
    let coordinator_address = unused_address().await;
    let source_incarnation = NodeIncarnation::new(11).unwrap();
    let owner_incarnation = NodeIncarnation::new(12).unwrap();
    let coordinator_incarnation = NodeIncarnation::new(13).unwrap();
    let source_node = NodeKey {
        node_id: "source".to_owned(),
        address: source_address.clone(),
        incarnation: source_incarnation,
    };
    let owner_node = NodeKey {
        node_id: "owner".to_owned(),
        address: owner_address.clone(),
        incarnation: owner_incarnation,
    };
    let remoting = RemotingConfig {
        heartbeat_interval: Duration::from_millis(100),
        shutdown_timeout: Duration::from_secs(2),
        ..RemotingConfig::default()
    };
    let source_associations = Arc::new(
        AssociationManager::new(source_address.clone(), source_incarnation, remoting.clone())
            .unwrap(),
    );
    let owner_associations = Arc::new(
        AssociationManager::new(owner_address.clone(), owner_incarnation, remoting.clone())
            .unwrap(),
    );
    let source_coordinator = attach_coordinator(
        &source_associations,
        &cluster_id,
        source_incarnation,
        coordinator_address.clone(),
        coordinator_incarnation,
    );
    let owner_coordinator = attach_coordinator(
        &owner_associations,
        &cluster_id,
        owner_incarnation,
        coordinator_address,
        coordinator_incarnation,
    );
    let entity_config = EntityConfig::new(
        EntityType::new("remote-entity").unwrap(),
        ProtocolId::new(TEST_PROTOCOL_ID).unwrap(),
        16,
        "weighted-least-load",
        1,
        Vec::new(),
    )
    .unwrap();
    let entity_id = EntityId::new(b"account-42".to_vec()).unwrap();
    let entity_slot = PlacementSlot {
        key: PlacementSlotKey::Shard {
            entity_type: entity_config.entity_type.clone(),
            shard_id: entity_config.shard_for(&entity_id),
        },
        config_fingerprint: entity_config.fingerprint(),
        owner: Some(owner_node.clone()),
        target: None,
        assignment_generation: AssignmentGeneration::new(7).unwrap(),
        coordinator_term: CoordinatorTerm::new(3).unwrap(),
        revision: Revision::new(9).unwrap(),
        state: PlacementSlotState::Running,
        active_move: None,
        barrier_sessions: Default::default(),
    };
    let singleton_kind = SingletonKind::new("remote-singleton").unwrap();
    let singleton_fingerprint = ConfigFingerprint::new([5; 32]);
    let singleton_slot = PlacementSlot {
        key: PlacementSlotKey::Singleton(singleton_kind.clone()),
        config_fingerprint: singleton_fingerprint,
        owner: Some(owner_node.clone()),
        target: None,
        assignment_generation: AssignmentGeneration::new(4).unwrap(),
        coordinator_term: CoordinatorTerm::new(3).unwrap(),
        revision: Revision::new(9).unwrap(),
        state: PlacementSlotState::Running,
        active_move: None,
        barrier_sessions: Default::default(),
    };
    let hello = |node: NodeKey| lattice_placement::coordinator::NodeHello {
        node,
        roles: BTreeSet::new(),
        capacity_units: 1,
        hosted_entity_types: [entity_config.entity_type.clone()].into_iter().collect(),
        proxied_entity_types: BTreeSet::new(),
        singleton_eligibility: [singleton_kind.clone()].into_iter().collect(),
        used_singletons: [singleton_kind.clone()].into_iter().collect(),
        protocols: Vec::new(),
        entity_configs: Vec::new(),
        singleton_configs: Vec::new(),
    };
    let (source_state, source_control, source_shutdown, source_logic) = stage_logic_runtime(
        hello(source_node.clone()),
        source_coordinator.clone(),
        source_associations.clone(),
        vec![entity_slot.clone(), singleton_slot.clone()],
    )
    .await;
    let (owner_state, owner_control, owner_shutdown, owner_logic) = stage_logic_runtime(
        hello(owner_node.clone()),
        owner_coordinator.clone(),
        owner_associations.clone(),
        vec![entity_slot, singleton_slot],
    )
    .await;
    let protocol = Arc::new(EntityProtocol::build().unwrap());
    let source_loads = Arc::new(AtomicUsize::new(0));
    let owner_loads = Arc::new(AtomicUsize::new(0));
    let registry = |address: NodeAddress, incarnation: NodeIncarnation| {
        Arc::new(ActorRegistry::new(
            actor_kind!("RemoteEntity"),
            ActorRegistryConfig {
                actor_ref: Some(ActorRefConfig {
                    cluster_id: cluster_id.clone(),
                    node_address: address,
                    node_incarnation: incarnation,
                    protocol_id: ProtocolId::new(TEST_PROTOCOL_ID).unwrap(),
                }),
                ..ActorRegistryConfig::default()
            },
        ))
    };
    let source_messaging = Arc::new(OutboundMessaging::new(32).unwrap());
    let owner_messaging = Arc::new(OutboundMessaging::new(32).unwrap());
    let source_registry = registry(source_address.clone(), source_incarnation);
    let owner_registry = registry(owner_address.clone(), owner_incarnation);
    let mut source_router = ClusterLogicalRouter::new(
        source_node.clone(),
        source_state,
        source_associations.clone(),
        source_messaging.clone(),
        source_coordinator,
        LogicalBufferConfig::default(),
        8,
    )
    .unwrap();
    source_router
        .register_entity(
            entity_config.clone(),
            source_registry.clone(),
            protocol.clone(),
            CountingLoader(source_loads.clone()),
        )
        .unwrap();
    source_router
        .register_singleton(
            singleton_kind.clone(),
            singleton_fingerprint,
            ProtocolId::new(TEST_PROTOCOL_ID).unwrap(),
            source_registry,
            protocol.clone(),
            CountingLoader(source_loads.clone()),
        )
        .unwrap();
    let mut owner_router = ClusterLogicalRouter::new(
        owner_node.clone(),
        owner_state,
        owner_associations.clone(),
        owner_messaging.clone(),
        owner_coordinator,
        LogicalBufferConfig::default(),
        8,
    )
    .unwrap();
    owner_router
        .register_entity(
            entity_config.clone(),
            owner_registry.clone(),
            protocol.clone(),
            CountingLoader(owner_loads.clone()),
        )
        .unwrap();
    owner_router
        .register_singleton(
            singleton_kind.clone(),
            singleton_fingerprint,
            ProtocolId::new(TEST_PROTOCOL_ID).unwrap(),
            owner_registry,
            protocol.clone(),
            CountingLoader(owner_loads.clone()),
        )
        .unwrap();
    let source_router: Arc<dyn LogicalRouter> = Arc::new(source_router);
    let owner_router: Arc<dyn LogicalRouter> = Arc::new(owner_router);
    let descriptor = ProtocolDescriptor {
        protocol_id: ProtocolId::new(TEST_PROTOCOL_ID).unwrap(),
        fingerprint: protocol.fingerprint(),
    };
    let endpoint = |identity: NodeIdentity,
                    associations: Arc<AssociationManager>,
                    messaging: Arc<OutboundMessaging>,
                    logical: Arc<dyn LogicalRouter>,
                    control: Arc<PlacementControlRouter>| {
        Arc::new(
            RemotingEndpoint::new_with_control(
                identity,
                remoting.clone(),
                associations,
                messaging,
                Arc::new(ServiceInboundDispatch {
                    hosts: Arc::new(ProtocolHostRegistry::new(1).unwrap()),
                    logical: Some(logical),
                }),
                control,
                vec![descriptor.clone()],
            )
            .unwrap(),
        )
    };
    let source_identity = NodeIdentity {
        cluster_id: cluster_id.clone(),
        node_id: source_node.node_id.clone(),
        address: source_address,
        incarnation: source_incarnation,
    };
    let owner_identity = NodeIdentity {
        cluster_id: cluster_id.clone(),
        node_id: owner_node.node_id.clone(),
        address: owner_address,
        incarnation: owner_incarnation,
    };
    let source_endpoint = endpoint(
        source_identity.clone(),
        source_associations.clone(),
        source_messaging,
        source_router.clone(),
        source_control,
    );
    let owner_endpoint = endpoint(
        owner_identity.clone(),
        owner_associations,
        owner_messaging,
        owner_router,
        owner_control,
    );
    source_endpoint.bind().await.unwrap();
    owner_endpoint.bind().await.unwrap();
    if source_associations.should_dial(&owner_identity.address, owner_identity.incarnation) {
        source_endpoint.connect_peer(owner_identity).await.unwrap();
    } else {
        owner_endpoint.connect_peer(source_identity).await.unwrap();
    }
    let reference = entity_config.entity_ref::<()>(cluster_id.clone(), entity_id);
    assert!(
        source_router
            .resolve_entity_current(reference.clone())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(owner_loads.load(Ordering::SeqCst), 0);
    let (_, request) = protocol
        .encode_request(DispatchMode::Ask, &GetValue(2))
        .unwrap();
    let reply = source_router
        .ask_entity(
            reference.clone(),
            protocol.fingerprint(),
            1,
            request,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(
        protocol.decode_response::<GetValue>(1, &reply).unwrap(),
        Value(42)
    );
    let current = source_router
        .resolve_entity_current(reference)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.node_address(), &owner_node.address);
    assert_eq!(current.node_incarnation(), owner_node.incarnation);
    let singleton = SingletonRef::new(
        cluster_id,
        singleton_kind,
        ProtocolId::new(TEST_PROTOCOL_ID).unwrap(),
        singleton_fingerprint,
    );
    assert!(
        source_router
            .resolve_singleton_current(singleton.clone())
            .await
            .unwrap()
            .is_none()
    );
    let (_, request) = protocol
        .encode_request(DispatchMode::Ask, &GetValue(3))
        .unwrap();
    let reply = source_router
        .ask_singleton(
            singleton.clone(),
            protocol.fingerprint(),
            1,
            request,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(
        protocol.decode_response::<GetValue>(1, &reply).unwrap(),
        Value(43)
    );
    let current = source_router
        .resolve_singleton_current(singleton)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.node_address(), &owner_node.address);
    assert_eq!(current.node_incarnation(), owner_node.incarnation);
    assert_eq!(source_loads.load(Ordering::SeqCst), 0);
    assert_eq!(owner_loads.load(Ordering::SeqCst), 2);
    source_endpoint.shutdown().await.unwrap();
    owner_endpoint.shutdown().await.unwrap();
    source_shutdown.send(true).unwrap();
    owner_shutdown.send(true).unwrap();
    source_logic.await.unwrap().unwrap();
    owner_logic.await.unwrap().unwrap();
}
