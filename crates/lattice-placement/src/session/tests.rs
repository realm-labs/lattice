use lattice_core::actor_ref::{
    ClusterId, ConfigFingerprint, EntityType, NodeAddress, NodeIncarnation,
};
use lattice_remoting::{
    association::{LaneAttachment, LaneKind},
    config::RemotingConfig,
    control::decode_control_envelope,
    control::{CommandId, ControlDispatchError},
    wire::FrameKind,
};

use super::*;
use crate::authority::{AuthorityEffect, AuthorityEvent};
use crate::control::InboundPlacementControl;
use crate::types::{
    AssignmentGeneration, ClaimGrant, CoordinatorTerm, GrantSequence, PlacementVersion, Revision,
    ShardId,
};

/// A frozen process keeps whatever admission flag it cached and never runs the fencing tick, so
/// the only thing that can stop it answering after it is thawed is the deadline the grant it
/// holds was installed with.
#[tokio::test(start_paused = true)]
async fn admission_closes_on_the_installed_deadline_even_though_no_tick_ran() {
    let domain = PlacementDomainId::new("frozen-owner").unwrap();
    let local = NodeKey {
        node_id: "owner".to_owned(),
        address: NodeAddress::new("127.0.0.1", 34200).unwrap(),
        incarnation: NodeIncarnation::new(1).unwrap(),
    };
    let key = PlacementSlotKey::Shard {
        domain: domain.clone(),
        entity_type: EntityType::new("frozen-entity").unwrap(),
        shard_id: ShardId::new(0),
    };
    let slot = PlacementSlot {
        key: key.clone(),
        config_fingerprint: ConfigFingerprint::new([3; 32]),
        owner: Some(local.clone()),
        target: None,
        assignment_generation: AssignmentGeneration::new(1).unwrap(),
        version: PlacementVersion::new(
            domain.clone(),
            CoordinatorTerm::new(1).unwrap(),
            Revision::new(1).unwrap(),
        ),
        state: PlacementSlotState::Running,
        active_move: None,
        barrier_sessions: Default::default(),
    };
    let origin = Instant::now();
    let mut authority = PlacementAuthority::new(local.clone(), Duration::from_secs(2)).unwrap();
    authority
        .transition(AuthorityEvent::ReconcileSlot(slot.clone()))
        .unwrap();
    authority
        .transition(AuthorityEvent::InstallGrant {
            grant: ClaimGrant {
                domain: domain.clone(),
                slot: key.clone(),
                owner: local.clone(),
                coordinator_term: slot.version.term,
                assignment_generation: slot.assignment_generation,
                grant_sequence: GrantSequence::new(1).unwrap(),
                ttl: Duration::from_secs(15),
            },
            now: monotonic_since(origin),
        })
        .unwrap();
    let state = LogicPlacementState {
        local_node: local.clone(),
        coordinator_term: 1,
        session: PlacementDomainState::new(domain),
        slots: [(key.clone(), slot)].into_iter().collect(),
        authorities: [(key.clone(), authority)].into_iter().collect(),
        resolution_failures: BTreeMap::new(),
        domain_up: true,
        origin,
        changed: Arc::new(Notify::new()),
    };

    let load = state.baseline_node_load(7);
    assert_eq!(load.node, local);
    assert_eq!(load.sequence, 7);
    assert_eq!(load.total_weight, 1);
    assert!(state.admission_open(&key));
    tokio::time::advance(Duration::from_secs(12)).await;
    assert!(state.admission_open(&key));
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(!state.admission_open(&key));
    tokio::time::advance(Duration::from_secs(26)).await;
    assert!(!state.admission_open(&key));
}

#[tokio::test]
async fn effect_backpressure_waits_for_capacity_without_terminating_the_session() {
    let cluster_id = ClusterId::new("effect-backpressure").unwrap();
    let domain = PlacementDomainId::new("effect-backpressure").unwrap();
    let local = NodeKey {
        node_id: "logic".to_owned(),
        address: NodeAddress::new("127.0.0.1", 34080).unwrap(),
        incarnation: NodeIncarnation::new(1).unwrap(),
    };
    let remote_address = NodeAddress::new("127.0.0.1", 34081).unwrap();
    let remote_incarnation = NodeIncarnation::new(2).unwrap();
    let associations = Arc::new(
        AssociationManager::new(
            local.address.clone(),
            local.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let coordinator = AssociationKey {
        cluster_id,
        local_incarnation: local.incarnation,
        remote_address,
        remote_incarnation,
    };
    let (session, mut effects) = PlacementDomainSession::new(
        PlacementDomainHello::builder(local, domain.clone(), 1).build(),
        coordinator,
        associations,
        LogicCoordinatorConfig::default(),
        1,
        1,
    )
    .unwrap();
    let slot = PlacementSlotKey::Shard {
        domain,
        entity_type: EntityType::new("entity").unwrap(),
        shard_id: ShardId::new(0),
    };

    session
        .publish_effects(slot.clone(), vec![AuthorityEffect::FenceAdmission])
        .await
        .unwrap();
    let mut blocked =
        Box::pin(session.publish_effects(slot.clone(), vec![AuthorityEffect::StopSlot]));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), blocked.as_mut())
            .await
            .is_err()
    );
    assert!(matches!(
        effects.recv().await,
        Some(LogicPlacementEffect::Authority {
            effect: AuthorityEffect::FenceAdmission,
            ..
        })
    ));
    blocked.await.unwrap();
    assert!(matches!(
        effects.recv().await,
        Some(LogicPlacementEffect::Authority {
            effect: AuthorityEffect::StopSlot,
            ..
        })
    ));
}

#[tokio::test]
async fn runtime_progress_never_consumes_the_reliable_control_outbox() {
    let cluster_id = ClusterId::new("runtime-progress-outbox").unwrap();
    let domain = PlacementDomainId::new("runtime-progress-outbox").unwrap();
    let local = NodeKey {
        node_id: "logic".to_owned(),
        address: NodeAddress::new("127.0.0.1", 34085).unwrap(),
        incarnation: NodeIncarnation::new(1).unwrap(),
    };
    let remote_address = NodeAddress::new("127.0.0.1", 34086).unwrap();
    let remote_incarnation = NodeIncarnation::new(2).unwrap();
    let associations = Arc::new(
        AssociationManager::new(
            local.address.clone(),
            local.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let association = associations
        .get_or_create(
            cluster_id.clone(),
            remote_address.clone(),
            remote_incarnation,
        )
        .unwrap();
    let coordinator = AssociationKey {
        cluster_id,
        local_incarnation: local.incarnation,
        remote_address,
        remote_incarnation,
    };
    for (lane, nonce) in [
        (LaneKind::Control, 1_u128),
        (LaneKind::Interactive, 2),
        (LaneKind::Bulk(0), 3),
    ] {
        association
            .attach(LaneAttachment {
                association_id: association.id(),
                key: coordinator.clone(),
                lane,
                connection_nonce: nonce,
            })
            .unwrap();
    }
    let (session, _effects) = PlacementDomainSession::new(
        PlacementDomainHello::builder(local.clone(), domain.clone(), 1).build(),
        coordinator,
        associations,
        LogicCoordinatorConfig::default(),
        8,
        1,
    )
    .unwrap();
    session.send_hello().unwrap();

    let version = PlacementVersion::new(
        domain.clone(),
        CoordinatorTerm::new(1).unwrap(),
        Revision::new(1).unwrap(),
    );
    session
        .send_runtime_progress(PlacementControlCommand::AppliedRevision(version.clone()))
        .unwrap();
    let key = PlacementSlotKey::Shard {
        domain,
        entity_type: EntityType::new("entity").unwrap(),
        shard_id: ShardId::new(0),
    };
    session
        .state
        .lock()
        .expect("logic placement state poisoned")
        .slots
        .insert(
            key.clone(),
            PlacementSlot {
                key: key.clone(),
                config_fingerprint: ConfigFingerprint::new([7; 32]),
                owner: Some(local),
                target: None,
                assignment_generation: AssignmentGeneration::new(1).unwrap(),
                version,
                state: PlacementSlotState::Allocating,
                active_move: None,
                barrier_sessions: Default::default(),
            },
        );
    session.control_handle().publish_ready(&key).unwrap();

    let replayed = association
        .replay_control_frames()
        .into_iter()
        .filter_map(|frame| decode_control_envelope(&frame).ok())
        .filter_map(|envelope| {
            crate::control::decode_control_command(&envelope.payload, DEFAULT_MAX_CONTROL_PAYLOAD)
                .ok()
        })
        .map(|scoped| scoped.command.name())
        .collect::<Vec<_>>();
    assert_eq!(replayed, vec!["PlacementDomainHello"]);
}

#[tokio::test]
async fn stale_generation_does_not_terminate_the_session() {
    let cluster_id = ClusterId::new("stale-generation-session").unwrap();
    let domain = PlacementDomainId::new("stale-generation-session").unwrap();
    let local = NodeKey {
        node_id: "logic".to_owned(),
        address: NodeAddress::new("127.0.0.1", 34090).unwrap(),
        incarnation: NodeIncarnation::new(1).unwrap(),
    };
    let remote_address = NodeAddress::new("127.0.0.1", 34091).unwrap();
    let remote_incarnation = NodeIncarnation::new(2).unwrap();
    let associations = Arc::new(
        AssociationManager::new(
            local.address.clone(),
            local.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let association = associations
        .get_or_create(
            cluster_id.clone(),
            remote_address.clone(),
            remote_incarnation,
        )
        .unwrap();
    let coordinator = AssociationKey {
        cluster_id,
        local_incarnation: local.incarnation,
        remote_address,
        remote_incarnation,
    };
    for (lane, nonce) in [
        (LaneKind::Control, 1_u128),
        (LaneKind::Interactive, 2),
        (LaneKind::Bulk(0), 3),
    ] {
        association
            .attach(LaneAttachment {
                association_id: association.id(),
                key: coordinator.clone(),
                lane,
                connection_nonce: nonce,
            })
            .unwrap();
    }
    let (session, _effects) = PlacementDomainSession::new(
        PlacementDomainHello::builder(local, domain.clone(), 1).build(),
        coordinator.clone(),
        associations,
        LogicCoordinatorConfig::default(),
        8,
        1,
    )
    .unwrap();
    let (controls, control_rx) = mpsc::channel(4);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(session.run(control_rx, shutdown_rx));
    tokio::task::yield_now().await;

    let slot = PlacementSlotKey::Shard {
        domain: domain.clone(),
        entity_type: EntityType::new("entity").unwrap(),
        shard_id: ShardId::new(0),
    };
    let (completion, result) = tokio::sync::oneshot::channel();
    controls
        .send(PlacementControlEvent {
            kind: PlacementControlEventKind::Command(Box::new(InboundPlacementControl {
                association: coordinator,
                command_id: CommandId::generate(),
                scope: CoordinatorScope::Placement(domain.clone()),
                coordinator_term: Some(1),
                command: PlacementControlCommand::DrainSlot {
                    slot,
                    generation: AssignmentGeneration::new(1).unwrap(),
                    version: PlacementVersion::new(
                        domain,
                        CoordinatorTerm::new(1).unwrap(),
                        Revision::new(1).unwrap(),
                    ),
                },
            })),
            completion,
        })
        .await
        .unwrap();
    assert_eq!(
        result.await.unwrap(),
        Err(ControlDispatchError::InvalidCommand)
    );

    shutdown.send(true).unwrap();
    assert!(task.await.unwrap().is_ok());
}

#[tokio::test(start_paused = true)]
async fn an_unacknowledged_drain_completion_gives_up_instead_of_polling_forever() {
    let cluster_id = ClusterId::new("drain-timeout").unwrap();
    let local = NodeKey {
        node_id: "logic".to_owned(),
        address: NodeAddress::new("127.0.0.1", 34100).unwrap(),
        incarnation: NodeIncarnation::new(1).unwrap(),
    };
    let remote_address = NodeAddress::new("127.0.0.1", 34101).unwrap();
    let remote_incarnation = NodeIncarnation::new(2).unwrap();
    let associations = Arc::new(
        AssociationManager::new(
            local.address.clone(),
            local.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let association = associations
        .get_or_create(
            cluster_id.clone(),
            remote_address.clone(),
            remote_incarnation,
        )
        .unwrap();
    let coordinator = AssociationKey {
        cluster_id,
        local_incarnation: local.incarnation,
        remote_address,
        remote_incarnation,
    };
    for (lane, nonce) in [
        (LaneKind::Control, 1_u128),
        (LaneKind::Interactive, 2),
        (LaneKind::Bulk(0), 3),
    ] {
        association
            .attach(LaneAttachment {
                association_id: association.id(),
                key: coordinator.clone(),
                lane,
                connection_nonce: nonce,
            })
            .unwrap();
    }
    let config = LogicCoordinatorConfig {
        drain_acknowledgement_timeout: Duration::from_secs(4),
        ..LogicCoordinatorConfig::default()
    };
    let (session, _effects) = PlacementDomainSession::new(
        PlacementDomainHello::builder(local, PlacementDomainId::new("drain-timeout").unwrap(), 1)
            .build(),
        coordinator,
        associations,
        config.clone(),
        8,
        1,
    )
    .unwrap();

    let started = Instant::now();
    let outcome = tokio::time::timeout(
        config.drain_acknowledgement_timeout * 4,
        session
            .control_handle()
            .complete_member_drain("drain-operation".to_owned()),
    )
    .await
    .expect("an unacknowledged drain must not poll forever");

    assert!(matches!(
        outcome,
        Err(LogicSessionError::DrainNotAcknowledged)
    ));
    assert!(started.elapsed() >= config.drain_acknowledgement_timeout);
    assert!(started.elapsed() < config.drain_acknowledgement_timeout * 2);
}

#[tokio::test(start_paused = true)]
async fn heartbeat_publishes_a_fresh_baseline_node_load_sample() {
    let cluster_id = ClusterId::new("automatic-node-load").unwrap();
    let local = NodeKey {
        node_id: "logic".to_owned(),
        address: NodeAddress::new("127.0.0.1", 34300).unwrap(),
        incarnation: NodeIncarnation::new(11).unwrap(),
    };
    let remote_address = NodeAddress::new("127.0.0.1", 34301).unwrap();
    let remote_incarnation = NodeIncarnation::new(12).unwrap();
    let associations = Arc::new(
        AssociationManager::new(
            local.address.clone(),
            local.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let association = associations
        .get_or_create(
            cluster_id.clone(),
            remote_address.clone(),
            remote_incarnation,
        )
        .unwrap();
    let coordinator = AssociationKey {
        cluster_id,
        local_incarnation: local.incarnation,
        remote_address,
        remote_incarnation,
    };
    for (lane, nonce) in [
        (LaneKind::Control, 1_u128),
        (LaneKind::Interactive, 2),
        (LaneKind::Bulk(0), 3),
    ] {
        association
            .attach(LaneAttachment {
                association_id: association.id(),
                key: coordinator.clone(),
                lane,
                connection_nonce: nonce,
            })
            .unwrap();
    }
    let mut outbound = association.take_lane_receiver(LaneKind::Control).unwrap();
    let config = LogicCoordinatorConfig {
        heartbeat_interval: Duration::from_secs(5),
        ..LogicCoordinatorConfig::default()
    };
    let (session, _effects) = PlacementDomainSession::new(
        PlacementDomainHello::builder(
            local.clone(),
            PlacementDomainId::new("automatic-node-load").unwrap(),
            1,
        )
        .build(),
        coordinator.clone(),
        associations,
        config.clone(),
        8,
        1,
    )
    .unwrap();
    let (controls, control_rx) = mpsc::channel(4);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(session.run(control_rx, shutdown_rx));

    tokio::task::yield_now().await;
    tokio::time::advance(config.heartbeat_interval).await;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    let mut hello_count = 0;
    while let Ok(frame) = outbound.try_recv() {
        let payload = match frame.kind {
            FrameKind::CoordinatorEvent => frame.payload().to_vec(),
            FrameKind::ControlEnvelope => decode_control_envelope(&frame).unwrap().payload.to_vec(),
            _ => continue,
        };
        let scoped =
            crate::control::decode_control_command(&payload, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
        if matches!(
            scoped.command,
            PlacementControlCommand::PlacementDomainHello(_)
        ) {
            hello_count += 1;
        }
    }
    assert!(
        hello_count >= 2,
        "domain hello should be retried before registration"
    );

    let (completion, result) = tokio::sync::oneshot::channel();
    controls
        .send(PlacementControlEvent {
            kind: PlacementControlEventKind::Command(Box::new(InboundPlacementControl {
                association: coordinator,
                command_id: CommandId::generate(),
                scope: CoordinatorScope::Placement(
                    PlacementDomainId::new("automatic-node-load").unwrap(),
                ),
                coordinator_term: Some(1),
                command: PlacementControlCommand::SnapshotBegin(
                    crate::coordinator::SnapshotBegin {
                        snapshot_id: 1,
                        version: crate::coordinator::SnapshotVersion::Placement(
                            PlacementVersion::new(
                                PlacementDomainId::new("automatic-node-load").unwrap(),
                                CoordinatorTerm::new(1).unwrap(),
                                Revision::new(1).unwrap(),
                            ),
                        ),
                        record_count: 0,
                        total_bytes: 0,
                        chunk_count: 0,
                        digest: [0; 32],
                    },
                ),
            })),
            completion,
        })
        .await
        .unwrap();
    assert_eq!(result.await.unwrap(), Ok(()));

    tokio::time::advance(config.heartbeat_interval).await;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    let mut load = None;
    while let Ok(frame) = outbound.try_recv() {
        let payload = match frame.kind {
            FrameKind::CoordinatorEvent => frame.payload().to_vec(),
            FrameKind::ControlEnvelope => decode_control_envelope(&frame).unwrap().payload.to_vec(),
            _ => continue,
        };
        let scoped =
            crate::control::decode_control_command(&payload, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
        if let PlacementControlCommand::NodeLoad(report) = scoped.command {
            load = Some(report);
        }
    }

    let load = load.expect("heartbeat must publish a baseline node load sample");
    assert_eq!(load.node, local);
    assert_eq!(load.sequence, 1);
    assert_eq!(load.total_weight, 0);
    shutdown.send(true).unwrap();
    assert!(task.await.unwrap().is_ok());
}
