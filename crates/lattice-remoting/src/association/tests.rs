use std::sync::Barrier;

use lattice_core::actor_ref::ProtocolId;

use super::*;
use crate::protocol::{CatalogueDecision, ProtocolDescriptor, ProtocolFingerprint};
use crate::wire::FrameKind;

fn key() -> AssociationKey {
    AssociationKey {
        cluster_id: ClusterId::new("test").unwrap(),
        local_incarnation: NodeIncarnation::new(1).unwrap(),
        remote_address: NodeAddress::new("remote", 25520).unwrap(),
        remote_incarnation: NodeIncarnation::new(2).unwrap(),
    }
}

fn manager() -> AssociationManager {
    manager_with_config(RemotingConfig::default())
}

fn manager_with_config(config: RemotingConfig) -> AssociationManager {
    AssociationManager::new(
        NodeAddress::new("local", 25519).unwrap(),
        NodeIncarnation::new(1).unwrap(),
        config,
    )
    .unwrap()
}

fn take_over(
    manager: &AssociationManager,
    id: AssociationId,
) -> Result<Arc<Association>, AssociationError> {
    manager.get_or_accept(
        key().cluster_id,
        key().remote_address,
        key().remote_incarnation,
        id,
    )
}

fn accept(manager: &AssociationManager, id: AssociationId) -> Arc<Association> {
    manager
        .get_or_accept(
            key().cluster_id,
            key().remote_address,
            key().remote_incarnation,
            id,
        )
        .unwrap()
}

fn lane_group() -> [(LaneKind, u128); 3] {
    [
        (LaneKind::Control, 1),
        (LaneKind::Interactive, 2),
        (LaneKind::Bulk(0), 3),
    ]
}

fn attach_lane_group(association: &Association) {
    for (lane, nonce) in lane_group() {
        association
            .attach(LaneAttachment {
                association_id: association.id(),
                key: association.key().clone(),
                lane,
                connection_nonce: nonce,
            })
            .unwrap();
    }
}

#[test]
fn duplicate_lane_keeps_lowest_nonce_and_control_loss_closes_admission() {
    let association = Association::new(key(), RemotingConfig::default()).unwrap();
    for (lane, nonce) in [
        (LaneKind::Control, 20),
        (LaneKind::Interactive, 21),
        (LaneKind::Bulk(0), 22),
    ] {
        association
            .attach(LaneAttachment {
                association_id: association.id(),
                key: key(),
                lane,
                connection_nonce: nonce,
            })
            .unwrap();
    }
    assert_eq!(association.state(), AssociationState::Active);
    assert_eq!(
        association
            .attach(LaneAttachment {
                association_id: association.id(),
                key: key(),
                lane: LaneKind::Control,
                connection_nonce: 10,
            })
            .unwrap(),
        AttachmentDecision::ReplacedDuplicate
    );
    association.detach(LaneKind::Control, 10);
    assert_eq!(association.state(), AssociationState::Reconnecting);
    assert!(matches!(
        association.try_admit_interactive(Frame::new(FrameKind::Ask, bytes::Bytes::new())),
        Err(AssociationError::NotActive)
    ));
}

#[test]
fn active_association_tolerates_a_transient_data_lane_disconnect() {
    let association = Association::new(key(), RemotingConfig::default()).unwrap();
    for (lane, nonce) in [
        (LaneKind::Control, 1),
        (LaneKind::Interactive, 2),
        (LaneKind::Bulk(0), 3),
    ] {
        association
            .attach(LaneAttachment {
                association_id: association.id(),
                key: key(),
                lane,
                connection_nonce: nonce,
            })
            .unwrap();
    }

    association.detach(LaneKind::Interactive, 2);

    assert_eq!(association.state(), AssociationState::Active);
    association
        .try_admit_interactive(Frame::new(FrameKind::Ask, bytes::Bytes::new()))
        .unwrap();
}

#[test]
fn single_bulk_stripe_skips_route_hashing() {
    let association = Association::new(
        key(),
        RemotingConfig {
            bulk_queue_frames_per_stripe: 1,
            ..RemotingConfig::default()
        },
    )
    .unwrap();
    for (lane, nonce) in [
        (LaneKind::Control, 1),
        (LaneKind::Interactive, 2),
        (LaneKind::Bulk(0), 3),
    ] {
        association
            .attach(LaneAttachment {
                association_id: association.id(),
                key: key(),
                lane,
                connection_nonce: nonce,
            })
            .unwrap();
    }

    let frame = Frame::new(FrameKind::Tell, bytes::Bytes::from_static(b"message"));
    let (stripe, admission) = association
        .try_reserve_bulk(
            |_| panic!("single-stripe admission must not hash the route"),
            frame.payload_len(),
        )
        .unwrap();
    admission.send(frame);

    assert_eq!(stripe, 0);
    assert!(matches!(
        association.try_reserve_prepared_bulk(0, 1),
        Err(AssociationError::QueueFull)
    ));
    assert_eq!(association.metrics().outbound_queue_rejections, 1);
}

#[test]
fn peer_catalogue_allows_idempotent_reinstall_but_rejects_changes() {
    let association = Association::new(key(), RemotingConfig::default()).unwrap();
    let protocol_id = ProtocolId::new(7).unwrap();
    let original = ProtocolFingerprint::digest(b"original");
    let changed = ProtocolFingerprint::digest(b"changed");
    let descriptor = |fingerprint| ProtocolDescriptor {
        protocol_id,
        fingerprint,
    };

    association
        .install_peer_catalogue([descriptor(original)])
        .unwrap();
    association
        .install_peer_catalogue([descriptor(original)])
        .unwrap();
    assert!(matches!(
        association.install_peer_catalogue([descriptor(changed)]),
        Err(AssociationError::Catalogue(
            CatalogueError::ChangedAfterInstall
        ))
    ));
    assert_eq!(
        association.protocol_decision(protocol_id, original),
        CatalogueDecision::Enabled
    );
    assert!(matches!(
        association.protocol_decision(protocol_id, changed),
        CatalogueDecision::FingerprintMismatch { actual } if actual == original
    ));
}

#[test]
fn activation_is_reported_when_a_non_control_lane_completes_the_group() {
    let association = Association::new(key(), RemotingConfig::default()).unwrap();
    let (_, control_activated) = association
        .attach_with_activation(LaneAttachment {
            association_id: association.id(),
            key: key(),
            lane: LaneKind::Control,
            connection_nonce: 1,
        })
        .unwrap();
    let (_, interactive_activated) = association
        .attach_with_activation(LaneAttachment {
            association_id: association.id(),
            key: key(),
            lane: LaneKind::Interactive,
            connection_nonce: 2,
        })
        .unwrap();
    let (_, bulk_activated) = association
        .attach_with_activation(LaneAttachment {
            association_id: association.id(),
            key: key(),
            lane: LaneKind::Bulk(0),
            connection_nonce: 3,
        })
        .unwrap();
    let (_, duplicate_activated) = association
        .attach_with_activation(LaneAttachment {
            association_id: association.id(),
            key: key(),
            lane: LaneKind::Interactive,
            connection_nonce: 4,
        })
        .unwrap();

    assert!(!control_activated);
    assert!(!interactive_activated);
    assert!(bulk_activated);
    assert!(!duplicate_activated);
    assert_eq!(association.state(), AssociationState::Active);
}

#[test]
fn queued_reliable_control_replays_when_a_non_control_lane_activates() {
    let association = Association::new(key(), RemotingConfig::default()).unwrap();
    association
        .admit_control_command(bytes::Bytes::from_static(b"queued"))
        .unwrap();
    for (lane, nonce) in [
        (LaneKind::Control, 1),
        (LaneKind::Interactive, 2),
        (LaneKind::Bulk(0), 3),
    ] {
        association
            .attach_and_replay(LaneAttachment {
                association_id: association.id(),
                key: key(),
                lane,
                connection_nonce: nonce,
            })
            .unwrap();
    }

    let mut control = association.take_lane_receiver(LaneKind::Control).unwrap();
    let envelope = crate::control::decode_control_envelope(&control.try_recv().unwrap()).unwrap();
    assert_eq!(envelope.sequence, 1);
    assert_eq!(envelope.payload, bytes::Bytes::from_static(b"queued"));
}

#[test]
fn concurrent_reliable_admission_preserves_control_sequence_order() {
    let config = RemotingConfig {
        control_queue_frames: 1024,
        max_control_outbox_frames: 1024,
        ..RemotingConfig::default()
    };
    let association = Arc::new(Association::new(key(), config).unwrap());
    for (lane, nonce) in [
        (LaneKind::Control, 1),
        (LaneKind::Interactive, 2),
        (LaneKind::Bulk(0), 3),
    ] {
        association
            .attach(LaneAttachment {
                association_id: association.id(),
                key: key(),
                lane,
                connection_nonce: nonce,
            })
            .unwrap();
    }
    let mut control = association.take_lane_receiver(LaneKind::Control).unwrap();
    let barrier = Arc::new(Barrier::new(8));
    let workers = (0..8)
        .map(|_| {
            let association = association.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..64 {
                    association
                        .admit_control_command(bytes::Bytes::from_static(b"command"))
                        .unwrap();
                }
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().unwrap();
    }

    let sequences = (0..512)
        .map(|_| {
            let frame = control.try_recv().unwrap();
            crate::control::decode_control_envelope(&frame)
                .unwrap()
                .sequence
        })
        .collect::<Vec<_>>();
    assert_eq!(sequences, (1..=512).collect::<Vec<_>>());
}

#[test]
fn reused_address_rejects_old_incarnation_after_explicit_replacement() {
    let config = RemotingConfig {
        max_associations: 1,
        ..RemotingConfig::default()
    };
    let manager = AssociationManager::new(
        NodeAddress::new("local", 25519).unwrap(),
        NodeIncarnation::new(1).unwrap(),
        config,
    )
    .unwrap();
    let address = NodeAddress::new("remote", 25520).unwrap();
    manager
        .get_or_create(
            ClusterId::new("test").unwrap(),
            address.clone(),
            NodeIncarnation::new(2).unwrap(),
        )
        .unwrap();
    assert_eq!(
        manager.replace_remote_incarnation(address.clone(), NodeIncarnation::new(3).unwrap()),
        1
    );
    assert!(matches!(
        manager.get_or_create(
            ClusterId::new("test").unwrap(),
            address,
            NodeIncarnation::new(2).unwrap(),
        ),
        Err(AssociationError::OldOrUnreconciledIncarnation)
    ));
}

#[test]
fn an_inbound_generation_takes_over_an_association_without_live_lanes() {
    let manager = manager();
    let stale_id = AssociationId::generate();
    let stale = accept(&manager, stale_id);
    attach_lane_group(&stale);
    assert_eq!(stale.state(), AssociationState::Active);
    for (lane, nonce) in lane_group() {
        stale.detach(lane, nonce);
    }
    assert!(stale.has_activated());

    let fresh_id = AssociationId::generate();
    let fresh = accept(&manager, fresh_id);

    assert_eq!(fresh.id(), fresh_id);
    assert_eq!(manager.len(), 1);
    assert_eq!(manager.get(&key()).unwrap().id(), fresh_id);
    assert_eq!(stale.state(), AssociationState::Closed);
}

#[test]
fn an_inbound_generation_cannot_take_over_an_active_association() {
    let manager = manager();
    let live_id = AssociationId::generate();
    let live = accept(&manager, live_id);
    attach_lane_group(&live);

    assert!(matches!(
        manager.get_or_accept(
            key().cluster_id,
            key().remote_address,
            key().remote_incarnation,
            AssociationId::generate(),
        ),
        Err(AssociationError::IncomingAssociationConflict)
    ));
    assert_eq!(live.state(), AssociationState::Active);
    assert_eq!(manager.get(&key()).unwrap().id(), live_id);
}

#[test]
fn an_inbound_generation_cannot_take_over_a_partially_attached_association() {
    let manager = manager();
    let establishing_id = AssociationId::generate();
    let establishing = accept(&manager, establishing_id);
    establishing
        .attach(LaneAttachment {
            association_id: establishing_id,
            key: key(),
            lane: LaneKind::Control,
            connection_nonce: 1,
        })
        .unwrap();
    assert_eq!(establishing.state(), AssociationState::Establishing);

    assert!(matches!(
        manager.get_or_accept(
            key().cluster_id,
            key().remote_address,
            key().remote_incarnation,
            AssociationId::generate(),
        ),
        Err(AssociationError::IncomingAssociationConflict)
    ));
    assert_eq!(manager.get(&key()).unwrap().id(), establishing_id);
}

/// A frozen or blackholed peer rejoins under its original incarnation, so the association
/// key is identical on both sides and only peer liveness can tell a live entry from a
/// wedged one.
#[test]
fn a_same_incarnation_rejoin_takes_over_an_association_the_peer_went_silent_on() {
    let silence_window = Duration::from_millis(40);
    let manager = manager_with_config(RemotingConfig {
        heartbeat_interval: silence_window,
        heartbeat_miss_limit: 1,
        ..RemotingConfig::default()
    });
    let stale_id = AssociationId::generate();
    let stale = accept(&manager, stale_id);
    attach_lane_group(&stale);
    assert_eq!(stale.state(), AssociationState::Active);
    assert!(matches!(
        take_over(&manager, AssociationId::generate()),
        Err(AssociationError::IncomingAssociationConflict)
    ));

    std::thread::sleep(silence_window * 3);

    // Every lane still claims to be attached; only the peer's silence exposes the entry.
    assert!(stale.has_live_connection());
    let fresh_id = AssociationId::generate();
    let fresh = take_over(&manager, fresh_id).unwrap();

    assert_eq!(fresh.id(), fresh_id);
    assert_eq!(manager.len(), 1);
    assert_eq!(manager.get(&key()).unwrap().id(), fresh_id);
    assert_eq!(stale.state(), AssociationState::Closed);
}

#[test]
fn an_inbound_generation_cannot_take_over_an_association_the_peer_keeps_proving() {
    let silence_window = Duration::from_millis(40);
    let manager = manager_with_config(RemotingConfig {
        heartbeat_interval: silence_window,
        heartbeat_miss_limit: 1,
        ..RemotingConfig::default()
    });
    let live_id = AssociationId::generate();
    let live = accept(&manager, live_id);
    attach_lane_group(&live);

    std::thread::sleep(silence_window * 3);
    // A heartbeat lands on the control lane just before the takeover attempt.
    live.record_peer_activity();

    assert!(matches!(
        take_over(&manager, AssociationId::generate()),
        Err(AssociationError::IncomingAssociationConflict)
    ));
    assert_eq!(live.state(), AssociationState::Active);
    assert_eq!(manager.get(&key()).unwrap().id(), live_id);
}

/// A duplicate connection that loses the lane must not leave an attachment behind: only
/// its own nonce could ever detach it, so the association would stay live forever and
/// fence every later rejoin.
#[test]
fn a_lane_attachment_is_owned_by_the_connection_that_holds_its_receiver() {
    let association = Association::new(key(), RemotingConfig::default()).unwrap();
    let running = association
        .attach_owned_lane(LaneAttachment {
            association_id: association.id(),
            key: key(),
            lane: LaneKind::Control,
            connection_nonce: 20,
        })
        .unwrap();

    assert!(matches!(
        association.attach_owned_lane(LaneAttachment {
            association_id: association.id(),
            key: key(),
            lane: LaneKind::Control,
            connection_nonce: 10,
        }),
        Err(AssociationError::LaneReceiverConflict)
    ));

    association.detach(LaneKind::Control, 20);
    association
        .return_lane_receiver(LaneKind::Control, running)
        .unwrap();
    assert_eq!(association.attached_lane_count(), 0);
    assert!(!association.has_live_connection());
}

#[test]
fn a_rejected_attachment_does_not_leave_the_lane_marked_attached() {
    let association = Association::new(key(), RemotingConfig::default()).unwrap();
    association.begin_close();

    assert!(matches!(
        association.attach_owned_lane(LaneAttachment {
            association_id: association.id(),
            key: key(),
            lane: LaneKind::Control,
            connection_nonce: 1,
        }),
        Err(AssociationError::Closed)
    ));
    assert_eq!(association.attached_lane_count(), 0);
    assert!(association.lane_receiver_available(LaneKind::Control));
}

#[test]
fn an_inbound_takeover_still_fences_unreconciled_peer_incarnations() {
    let manager = manager();
    let address = key().remote_address;
    accept(&manager, AssociationId::generate());
    assert!(matches!(
        manager.get_or_accept(
            key().cluster_id,
            address.clone(),
            NodeIncarnation::new(4).unwrap(),
            AssociationId::generate(),
        ),
        Err(AssociationError::OldOrUnreconciledIncarnation)
    ));
    assert_eq!(
        manager.replace_remote_incarnation(address.clone(), NodeIncarnation::new(3).unwrap()),
        1
    );

    assert!(matches!(
        manager.get_or_accept(
            key().cluster_id,
            address,
            key().remote_incarnation,
            AssociationId::generate(),
        ),
        Err(AssociationError::OldOrUnreconciledIncarnation)
    ));
    assert_eq!(manager.len(), 0);
}

#[test]
fn node_byte_budget_is_shared_across_associations() {
    let config = RemotingConfig {
        max_associations: 2,
        max_outbound_bytes_per_association: 12,
        max_outbound_bytes_per_node: 12,
        ..RemotingConfig::default()
    };
    let manager = AssociationManager::new(
        NodeAddress::new("local", 25519).unwrap(),
        NodeIncarnation::new(1).unwrap(),
        config,
    )
    .unwrap();
    let cluster = ClusterId::new("test").unwrap();
    let first = manager
        .get_or_create(
            cluster.clone(),
            NodeAddress::new("first", 25520).unwrap(),
            NodeIncarnation::new(2).unwrap(),
        )
        .unwrap();
    let second = manager
        .get_or_create(
            cluster,
            NodeAddress::new("second", 25521).unwrap(),
            NodeIncarnation::new(3).unwrap(),
        )
        .unwrap();
    for association in [&first, &second] {
        for (lane, nonce) in [
            (LaneKind::Control, 1),
            (LaneKind::Interactive, 2),
            (LaneKind::Bulk(0), 3),
        ] {
            association
                .attach(LaneAttachment {
                    association_id: association.id(),
                    key: association.key().clone(),
                    lane,
                    connection_nonce: nonce,
                })
                .unwrap();
        }
    }
    first
        .try_admit_interactive(Frame::new(
            FrameKind::Backpressure,
            bytes::Bytes::from_static(b"12345678"),
        ))
        .unwrap();
    assert!(matches!(
        second.try_admit_interactive(Frame::new(
            FrameKind::Backpressure,
            bytes::Bytes::from_static(b"12345678"),
        )),
        Err(AssociationError::NodeByteBudgetExceeded)
    ));
    assert_eq!(second.metrics().node_byte_budget_rejections, 1);
    first.release_queued_bytes(8);
    second
        .try_admit_interactive(Frame::new(
            FrameKind::Backpressure,
            bytes::Bytes::from_static(b"12345678"),
        ))
        .unwrap();
}
