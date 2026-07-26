//! Cross-node dispatch: asks must reach the claimed owner and nothing else.

use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use lattice_actor::{
    host::ProtocolHostRegistry,
    registry::{ActorRefConfig, ActorRegistryConfig},
};
use lattice_core::{
    actor_kind,
    actor_ref::{ClusterId, EntityId, EntityType, NodeAddress, NodeIncarnation, ProtocolId},
};
use lattice_placement::{
    control::PlacementControlRouter,
    coordinator::SingletonConfig,
    types::{AssignmentGeneration, CoordinatorTerm, PlacementSlot, PlacementVersion, Revision},
};
use lattice_remoting::{
    config::RemotingConfig, endpoint::RemotingEndpoint, handshake::NodeIdentity,
    protocol::ProtocolDescriptor,
};

use super::support::*;
use crate::{
    backend::ServiceInboundDispatch,
    cluster::*,
    lifecycle::NodeAdmissionGate,
    test_support::{network_test_guard, unused_address},
};

#[tokio::test]
async fn remote_entity_ask_reaches_only_claimed_owner() {
    let _network = network_test_guard().await;
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
        domain(),
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
            domain: domain(),
            entity_type: entity_config.entity_type.clone(),
            shard_id: entity_config.shard_for(&entity_id).unwrap(),
        },
        config_fingerprint: entity_config.fingerprint(),
        owner: Some(owner_node.clone()),
        target: None,
        assignment_generation: AssignmentGeneration::new(7).unwrap(),
        version: PlacementVersion::new(
            domain(),
            CoordinatorTerm::new(3).unwrap(),
            Revision::new(9).unwrap(),
        ),
        state: PlacementSlotState::Running,
        active_move: None,
        barrier_sessions: Default::default(),
    };
    let singleton_kind = SingletonKind::new("remote-singleton").unwrap();
    let singleton_config = SingletonConfig::new(
        domain(),
        singleton_kind.clone(),
        ProtocolId::new(TEST_PROTOCOL_ID).unwrap(),
    );
    let singleton_fingerprint = singleton_config.fingerprint();
    let singleton_slot = PlacementSlot {
        key: PlacementSlotKey::Singleton {
            domain: domain(),
            kind: singleton_kind.clone(),
        },
        config_fingerprint: singleton_fingerprint,
        owner: Some(owner_node.clone()),
        target: None,
        assignment_generation: AssignmentGeneration::new(4).unwrap(),
        version: PlacementVersion::new(
            domain(),
            CoordinatorTerm::new(3).unwrap(),
            Revision::new(9).unwrap(),
        ),
        state: PlacementSlotState::Running,
        active_move: None,
        barrier_sessions: Default::default(),
    };
    let hello = |node: NodeKey| {
        test_hello(
            node,
            [entity_config.entity_type.clone()].into_iter().collect(),
            [singleton_kind.clone()].into_iter().collect(),
            [singleton_kind.clone()].into_iter().collect(),
        )
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
    let binding = Arc::new(EntityProtocol::bind::<EntityActor>().unwrap());
    let source_loads = Arc::new(AtomicUsize::new(0));
    let owner_loads = Arc::new(AtomicUsize::new(0));
    let registry = |address: NodeAddress, incarnation: NodeIncarnation| {
        Arc::new(ActorRegistry::new_bound(
            actor_kind!("RemoteEntity"),
            ActorRegistryConfig {
                actor_ref: Some(ActorRefConfig {
                    cluster_id: cluster_id.clone(),
                    node_address: address,
                    node_incarnation: incarnation,
                }),
                ..ActorRegistryConfig::default()
            },
            binding.as_ref(),
        ))
    };
    let source_messaging = Arc::new(OutboundMessaging::new(32).unwrap());
    let owner_messaging = Arc::new(OutboundMessaging::new(32).unwrap());
    let source_registry = registry(source_address.clone(), source_incarnation);
    let owner_registry = registry(owner_address.clone(), owner_incarnation);
    let mut source_router = DomainLogicalRouter::new(
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
            binding.clone(),
            CountingLoader(source_loads.clone()),
        )
        .unwrap();
    source_router
        .register_singleton(
            singleton_config.clone(),
            source_registry,
            binding.clone(),
            CountingLoader(source_loads.clone()),
        )
        .unwrap();
    let mut owner_router = DomainLogicalRouter::new(
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
            binding.clone(),
            CountingLoader(owner_loads.clone()),
        )
        .unwrap();
    owner_router
        .register_singleton(
            singleton_config,
            owner_registry,
            binding,
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
            RemotingEndpoint::builder(
                identity,
                remoting.clone(),
                associations,
                messaging,
                Arc::new(ServiceInboundDispatch {
                    hosts: Arc::new(ProtocolHostRegistry::new(1).unwrap()),
                    logical: Some(logical),
                    admission: NodeAdmissionGate::opened(),
                }),
            )
            .control_dispatch(control)
            .catalogue(vec![descriptor.clone()])
            .build()
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
    let reference = entity_config
        .entity_ref(cluster_id.clone(), entity_id)
        .unwrap();
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
        domain(),
        singleton_kind,
        ProtocolId::new(TEST_PROTOCOL_ID).unwrap(),
        singleton_fingerprint,
    )
    .unwrap();
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
