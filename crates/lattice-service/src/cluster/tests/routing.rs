//! Local slot resolution: single-flight failure propagation and authority fencing.

use std::{
    collections::BTreeSet,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use lattice_actor::registry::{ActorRefConfig, ActorRegistryConfig};
use lattice_core::{
    actor_kind,
    actor_ref::{ClusterId, EntityId, EntityType, NodeAddress, NodeIncarnation, ProtocolId},
    coordinator::CoordinatorScope,
};
use lattice_placement::{
    control::{
        DEFAULT_MAX_CONTROL_PAYLOAD, PlacementControlCommand, PlacementControlRouter,
        PlacementResolutionFailure, decode_control_command,
    },
    coordinator::{
        SingletonConfig, SnapshotLimits, SnapshotRecord, SnapshotVersion, build_snapshot,
    },
    session::{LogicCoordinatorConfig, PlacementDomainSession},
    types::{
        AssignmentGeneration, ClaimGrant, CoordinatorTerm, GrantSequence, PlacementSlot,
        PlacementVersion, Revision,
    },
};
use lattice_remoting::{
    association::{AssociationKey, LaneAttachment, LaneKind},
    config::RemotingConfig,
    control::{CommandId, ControlDispatch, decode_control_envelope},
};
use tokio::sync::watch;

use super::support::*;
use crate::{
    cluster::*,
    test_support::{network_test_guard, unused_address},
};

#[tokio::test]
async fn unavailable_resolution_fails_fast_and_clears_route_single_flight() {
    let _network = network_test_guard().await;
    let cluster_id = ClusterId::new("unavailable-route-test").unwrap();
    let local_incarnation = NodeIncarnation::new(31).unwrap();
    let coordinator_incarnation = NodeIncarnation::new(32).unwrap();
    let local_address = unused_address().await;
    let coordinator_address = unused_address().await;
    let local_node = NodeKey {
        node_id: "proxy".to_owned(),
        address: local_address.clone(),
        incarnation: local_incarnation,
    };
    let associations = Arc::new(
        AssociationManager::new(local_address, local_incarnation, RemotingConfig::default())
            .unwrap(),
    );
    let coordinator = attach_coordinator(
        &associations,
        &cluster_id,
        local_incarnation,
        coordinator_address,
        coordinator_incarnation,
    );
    let entity_config = EntityConfig::new(
        domain(),
        EntityType::new("unavailable-entity").unwrap(),
        ProtocolId::new(TEST_PROTOCOL_ID).unwrap(),
        16,
        "weighted-least-load",
        1,
        Vec::new(),
    )
    .unwrap();
    let singleton_config = SingletonConfig::new(
        domain(),
        SingletonKind::new("unavailable-singleton").unwrap(),
        ProtocolId::new(TEST_PROTOCOL_ID).unwrap(),
    );
    let hello = test_hello(
        local_node.clone(),
        [entity_config.entity_type.clone()].into_iter().collect(),
        BTreeSet::new(),
        [singleton_config.kind.clone()].into_iter().collect(),
    );
    let (control, controls) =
        PlacementControlRouter::bounded(32, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
    let control = Arc::new(control);
    let (logic, _effects) = PlacementDomainSession::new(
        hello.domain,
        coordinator.clone(),
        associations.clone(),
        LogicCoordinatorConfig::default(),
        32,
        1,
    )
    .unwrap();
    let state = logic.state();
    let (shutdown, shutdown_rx) = watch::channel(false);
    let logic_task = tokio::spawn(logic.run(controls, shutdown_rx));
    let protocol = EntityProtocol::build().unwrap();
    let fingerprint = protocol.fingerprint();
    let mut router = DomainLogicalRouter::new(
        local_node,
        state,
        associations.clone(),
        Arc::new(OutboundMessaging::new(8).unwrap()),
        coordinator.clone(),
        LogicalBufferConfig {
            maximum_residence: Duration::from_secs(10),
            ..LogicalBufferConfig::default()
        },
        4,
    )
    .unwrap();
    router
        .register_entity_proxy(entity_config.clone(), fingerprint)
        .unwrap();
    router
        .register_singleton_proxy(singleton_config.clone(), fingerprint)
        .unwrap();
    let router = Arc::new(router);
    let association = associations.get(&coordinator).unwrap();
    let reference = entity_config
        .entity_ref(
            cluster_id.clone(),
            EntityId::new(b"missing-host".to_vec()).unwrap(),
        )
        .unwrap();
    let shard_key = PlacementSlotKey::Shard {
        domain: domain(),
        entity_type: entity_config.entity_type.clone(),
        shard_id: entity_config.shard_for(reference.entity_id()).unwrap(),
    };

    let find_resolution = |expected_slot: PlacementSlotKey, excluded: Option<u128>| {
        let association = association.clone();
        async move {
            tokio::time::timeout(Duration::from_secs(1), async move {
                loop {
                    for frame in association.replay_control_frames() {
                        let Ok(envelope) = decode_control_envelope(&frame) else {
                            continue;
                        };
                        let Ok(scoped) =
                            decode_control_command(&envelope.payload, DEFAULT_MAX_CONTROL_PAYLOAD)
                        else {
                            continue;
                        };
                        let resolved = match scoped.command {
                            PlacementControlCommand::ResolveShard {
                                request_id,
                                domain,
                                entity_type,
                                shard_id,
                            } => Some((
                                request_id,
                                PlacementSlotKey::Shard {
                                    domain,
                                    entity_type,
                                    shard_id,
                                },
                            )),
                            PlacementControlCommand::ResolveSingleton {
                                request_id,
                                domain,
                                kind,
                            } => Some((request_id, PlacementSlotKey::Singleton { domain, kind })),
                            _ => None,
                        };
                        if let Some((request_id, slot)) = resolved
                            && slot == expected_slot
                            && excluded != Some(request_id)
                        {
                            return request_id;
                        }
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap()
        }
    };
    let fail_resolution = |request_id, slot: PlacementSlotKey| {
        let control = control.clone();
        let coordinator = coordinator.clone();
        async move {
            control
                .apply(
                    coordinator,
                    CommandId::generate(),
                    lattice_placement::control::encode_control_command_for_term(
                        &CoordinatorScope::Placement(domain()),
                        1,
                        &PlacementControlCommand::ResolutionFailed {
                            request_id,
                            slot,
                            reason: PlacementResolutionFailure::NoEligibleHost,
                        },
                        DEFAULT_MAX_CONTROL_PAYLOAD,
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
        }
    };

    let first = tokio::spawn({
        let router = router.clone();
        let reference = reference.clone();
        async move {
            router
                .tell_entity(None, reference, fingerprint, 1, Bytes::new())
                .await
        }
    });
    let concurrent = tokio::spawn({
        let router = router.clone();
        let reference = reference.clone();
        async move {
            router
                .tell_entity(None, reference, fingerprint, 2, Bytes::new())
                .await
        }
    });
    let first_request = find_resolution(shard_key.clone(), None).await;
    tokio::task::yield_now().await;
    let shard_request_ids = association
        .replay_control_frames()
        .into_iter()
        .filter_map(|frame| decode_control_envelope(&frame).ok())
        .filter_map(|envelope| {
            decode_control_command(&envelope.payload, DEFAULT_MAX_CONTROL_PAYLOAD).ok()
        })
        .filter_map(|scoped| match scoped.command {
            PlacementControlCommand::ResolveShard {
                request_id,
                domain,
                entity_type,
                shard_id,
            } => (PlacementSlotKey::Shard {
                domain,
                entity_type,
                shard_id,
            } == shard_key)
                .then_some(request_id),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(shard_request_ids, [first_request].into_iter().collect());
    fail_resolution(first_request, shard_key.clone()).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), first)
            .await
            .unwrap()
            .unwrap(),
        Err(RemoteMessageError::ShardUnavailable)
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), concurrent)
            .await
            .unwrap()
            .unwrap(),
        Err(RemoteMessageError::ShardUnavailable)
    );

    let second = tokio::spawn({
        let router = router.clone();
        let reference = reference.clone();
        async move {
            router
                .tell_entity(None, reference, fingerprint, 3, Bytes::new())
                .await
        }
    });
    let second_request = find_resolution(shard_key.clone(), Some(first_request)).await;
    assert_ne!(second_request, first_request);
    fail_resolution(second_request, shard_key).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .unwrap()
            .unwrap(),
        Err(RemoteMessageError::ShardUnavailable)
    );

    let singleton = SingletonRef::new(
        cluster_id,
        domain(),
        singleton_config.kind.clone(),
        singleton_config.protocol_id,
        singleton_config.fingerprint(),
    )
    .unwrap();
    let singleton_key = PlacementSlotKey::Singleton {
        domain: domain(),
        kind: singleton_config.kind,
    };
    let singleton_call = tokio::spawn({
        let router = router.clone();
        async move {
            router
                .tell_singleton(None, singleton, fingerprint, 4, Bytes::new())
                .await
        }
    });
    let singleton_request = find_resolution(singleton_key.clone(), None).await;
    fail_resolution(singleton_request, singleton_key).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), singleton_call)
            .await
            .unwrap()
            .unwrap(),
        Err(RemoteMessageError::ShardUnavailable)
    );

    shutdown.send(true).unwrap();
    logic_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn stale_generation_never_reaches_entity_loader() {
    let _network = network_test_guard().await;
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
        domain(),
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
        domain: domain(),
        entity_type: entity_config.entity_type.clone(),
        shard_id: entity_config.shard_for(&entity_id).unwrap(),
    };
    let hello = test_hello(
        local_node.clone(),
        [entity_config.entity_type.clone()].into_iter().collect(),
        BTreeSet::new(),
        BTreeSet::new(),
    );
    let (control_router, controls) =
        PlacementControlRouter::bounded(32, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
    let control_router = Arc::new(control_router);
    let (logic, _effects) = PlacementDomainSession::new(
        hello.domain,
        association_key.clone(),
        associations.clone(),
        LogicCoordinatorConfig::default(),
        32,
        1,
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
        version: PlacementVersion::new(
            domain(),
            CoordinatorTerm::new(1).unwrap(),
            Revision::new(1).unwrap(),
        ),
        state: PlacementSlotState::Running,
        active_move: None,
        barrier_sessions: Default::default(),
    };
    let limits = SnapshotLimits::default();
    let (begin, chunks, end) = build_snapshot(
        SnapshotVersion::Placement(slot.version.clone()),
        vec![SnapshotRecord {
            key: format!(
                "domain/{}/shard/{}/{}",
                domain().as_str(),
                entity_config.entity_type.as_str(),
                entity_config.shard_for(&entity_id).unwrap().get()
            ),
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
                domain: domain(),
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
                lattice_placement::control::encode_control_command_for_term(
                    &CoordinatorScope::Placement(domain()),
                    1,
                    &command,
                    DEFAULT_MAX_CONTROL_PAYLOAD,
                )
                .unwrap(),
            )
            .await
            .unwrap();
    }
    let protocol = Arc::new(EntityProtocol::build().unwrap());
    let binding = Arc::new(EntityProtocol::bind::<EntityActor>().unwrap());
    let registry = Arc::new(ActorRegistry::new_bound(
        actor_kind!("Entity"),
        ActorRegistryConfig {
            actor_ref: Some(ActorRefConfig {
                cluster_id: cluster_id.clone(),
                node_address: local_address.clone(),
                node_incarnation: local_incarnation,
            }),
            ..ActorRegistryConfig::default()
        },
        binding.as_ref(),
    ));
    let loads = Arc::new(AtomicUsize::new(0));
    let mut router = DomainLogicalRouter::new(
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
            binding,
            CountingLoader(loads.clone()),
        )
        .unwrap();
    let reference = entity_config.entity_ref(cluster_id, entity_id).unwrap();
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
