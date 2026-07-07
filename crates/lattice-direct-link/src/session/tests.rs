use super::*;

use std::str::FromStr;

use http::Uri;
use lattice_core::{
    ActorId, DirectLinkMessageDescriptor, DirectLinkMessageId, Epoch, InstanceId, ServiceKind,
    actor_kind, service_kind,
};

#[derive(Clone, PartialEq, prost::Message)]
struct TestPayload {
    #[prost(uint64, tag = "1")]
    value: u64,
}

impl DirectLinkMessage for TestPayload {
    const PROTO_FULL_NAME: &'static str = "game.TestPayload";
}

#[test]
fn open_link_negotiates_unidirectional_session_and_sequence() {
    let manager = DirectLinkSessionManager::new();
    let stream = stream("movement", &[1, 2]);
    manager
        .register_binding(actor_kind!("Battle"), stream.clone())
        .unwrap();
    let link_id = LinkId::new("link-1");

    let ack = manager
        .open_link(OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &stream),
            target_to_source: None,
            options: DirectLinkOptions::default(),
        })
        .unwrap();

    assert_eq!(ack.source_to_target.stream_name, "movement");
    assert_eq!(
        ack.source_to_target.accepted_message_type_ids,
        BTreeSet::from([DirectLinkMessageId(1), DirectLinkMessageId(2)])
    );
    let snapshot = manager.link_snapshot(&link_id).unwrap();
    assert_eq!(snapshot.mode, DirectLinkMode::Unidirectional);
    assert_eq!(
        snapshot.directions,
        BTreeSet::from([LinkDirection::SourceToTarget])
    );
    manager
        .validate_message_frame(
            &link_id,
            LinkDirection::SourceToTarget,
            DirectLinkMessageId(1),
            LinkSequence(1),
        )
        .unwrap();
    assert_eq!(
        manager.validate_message_frame(
            &link_id,
            LinkDirection::SourceToTarget,
            DirectLinkMessageId(1),
            LinkSequence(1),
        ),
        Err(MessageFrameError::InvalidSequence {
            expected: LinkSequence(2),
            actual: LinkSequence(1)
        })
    );
    let metrics = manager.metrics().snapshot();
    assert_eq!(metrics.opened, 1);
    assert_eq!(metrics.received, 1);
    assert_eq!(metrics.protocol_errors, 1);
}

#[test]
fn message_frame_validation_rejects_invalid_frames_before_delivery() {
    let manager = DirectLinkSessionManager::new();
    let stream = stream("movement", &[1]);
    manager
        .register_binding(actor_kind!("Battle"), stream.clone())
        .unwrap();
    let link_id = LinkId::new("link-frames");
    manager
        .open_link(open_request_with_id(&stream, link_id.clone()))
        .unwrap();

    assert_eq!(
        manager.validate_message_frame(
            &LinkId::new("missing"),
            LinkDirection::SourceToTarget,
            DirectLinkMessageId(1),
            LinkSequence(1),
        ),
        Err(MessageFrameError::UnknownLink)
    );
    assert_eq!(
        manager.validate_message_frame(
            &link_id,
            LinkDirection::TargetToSource,
            DirectLinkMessageId(1),
            LinkSequence(1),
        ),
        Err(MessageFrameError::WrongDirection)
    );
    assert_eq!(
        manager.validate_message_frame(
            &link_id,
            LinkDirection::SourceToTarget,
            DirectLinkMessageId(2),
            LinkSequence(1),
        ),
        Err(MessageFrameError::UnsupportedMessageType)
    );
    assert!(matches!(
        manager.validate_and_decode_message::<TestPayload>(
            &link_id,
            LinkDirection::SourceToTarget,
            DirectLinkMessageId(1),
            LinkSequence(1),
            b"not protobuf",
        ),
        Err(MessageFrameError::DecodeError(_))
    ));

    let inactive = DirectLinkSessionManager::new();
    inactive
        .register_binding(actor_kind!("Battle"), stream.clone())
        .unwrap();
    let inactive_id = LinkId::new("link-inactive");
    inactive
        .open_link(open_request_with_id(&stream, inactive_id.clone()))
        .unwrap();
    inactive.register_actor(
        actor_kind!("Battle"),
        DirectLinkActorPolicy {
            activation: DirectLinkActivationPolicy::ExistingOnly,
            active: false,
            owner_epoch: None,
        },
    );
    assert_eq!(
        inactive.validate_message_frame(
            &inactive_id,
            LinkDirection::SourceToTarget,
            DirectLinkMessageId(1),
            LinkSequence(1),
        ),
        Err(MessageFrameError::NonActivatableTarget)
    );
}

#[test]
fn heartbeat_due_tracking_emits_once_per_interval_and_stops_after_close() {
    let manager = DirectLinkSessionManager::new();
    let stream = stream("movement", &[1]);
    manager
        .register_binding(actor_kind!("Battle"), stream.clone())
        .unwrap();
    let link_id = LinkId::new("link-heartbeat-due");
    let mut request = open_request_with_id(&stream, link_id.clone());
    request.options.heartbeat_interval = Duration::from_secs(10);
    manager.open_link(request).unwrap();

    let opened_at = Instant::now();
    assert!(
        manager
            .heartbeat_due_link_ids_at(opened_at + Duration::from_secs(1))
            .is_empty()
    );
    assert_eq!(
        manager.heartbeat_due_link_ids_at(opened_at + Duration::from_secs(10)),
        vec![link_id.clone()]
    );
    assert!(
        manager
            .heartbeat_due_link_ids_at(opened_at + Duration::from_secs(19))
            .is_empty()
    );
    assert_eq!(
        manager.heartbeat_due_link_ids_at(opened_at + Duration::from_secs(20)),
        vec![link_id.clone()]
    );

    manager
        .close_all(&link_id, LinkCloseReason::Done)
        .expect("close link");
    assert!(
        manager
            .heartbeat_due_link_ids_at(opened_at + Duration::from_secs(30))
            .is_empty()
    );
}

#[test]
fn open_link_rejects_unavailable_actor_unsupported_stream_and_message() {
    let manager = DirectLinkSessionManager::new();
    let requested_stream = stream("movement", &[1]);
    let link_id = LinkId::new("link-1");

    let reject = manager
        .open_link(OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &requested_stream),
            target_to_source: None,
            options: DirectLinkOptions::default(),
        })
        .unwrap_err();
    assert_eq!(reject.reason, OpenLinkRejectReason::ActorUnavailable);

    manager.register_actor(actor_kind!("Battle"), DirectLinkActorPolicy::default());
    let reject = manager
        .open_link(OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &requested_stream),
            target_to_source: None,
            options: DirectLinkOptions::default(),
        })
        .unwrap_err();
    assert_eq!(reject.reason, OpenLinkRejectReason::UnsupportedStream);

    manager
        .register_binding(actor_kind!("Battle"), stream("movement", &[1]))
        .unwrap();
    let reject = manager
        .open_link(OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id,
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection {
                link_id: LinkId::new("link-2"),
                stream_name: "movement".to_string(),
                supported_message_type_ids: BTreeSet::from([DirectLinkMessageId(2)]),
            },
            target_to_source: None,
            options: DirectLinkOptions::default(),
        })
        .unwrap_err();
    assert_eq!(reject.reason, OpenLinkRejectReason::UnsupportedMessageType);
}

#[test]
fn open_link_validates_service_auth_epoch_activation_and_backpressure() {
    let stream = stream("movement", &[1]);

    let protocol = configured_manager(&stream);
    let mut request = open_request(&stream);
    request.protocol_version = DIRECT_LINK_PROTOCOL_VERSION + 1;
    let reject = protocol.open_link(request).unwrap_err();
    assert_eq!(reject.reason, OpenLinkRejectReason::ProtocolVersionMismatch);

    let wrong_service = configured_manager(&stream);
    let mut request = open_request(&stream);
    request.target.service_kind = service_kind!("Wrong");
    let reject = wrong_service.open_link(request).unwrap_err();
    assert_eq!(reject.reason, OpenLinkRejectReason::NotOwner);

    let unauthorized = configured_manager(&stream);
    let mut request = open_request(&stream);
    request.source.service_kind = service_kind!("Intruder");
    let reject = unauthorized.open_link(request).unwrap_err();
    assert_eq!(reject.reason, OpenLinkRejectReason::Unauthorized);

    let overloaded = configured_manager(&stream);
    overloaded.set_validation_policy(
        OpenLinkValidationPolicy::hosted(service_kind!("Battle"))
            .authorize_sources([service_kind!("Gateway")])
            .max_pending(4)
            .max_frame_size(128),
    );
    let mut request = open_request(&stream);
    request.options.backpressure = BackpressurePolicy::DropOldest { max_pending: 8 };
    let reject = overloaded.open_link(request).unwrap_err();
    assert_eq!(reject.reason, OpenLinkRejectReason::Overloaded);

    let link_limited = configured_manager(&stream);
    link_limited.update_validation_policy(|policy| policy.max_active_links(1));
    link_limited
        .open_link(open_request_with_id(&stream, LinkId::new("link-active-1")))
        .unwrap();
    let reject = link_limited
        .open_link(open_request_with_id(&stream, LinkId::new("link-active-2")))
        .unwrap_err();
    assert_eq!(reject.reason, OpenLinkRejectReason::Overloaded);

    let open_rate_limited = configured_manager(&stream);
    open_rate_limited
        .update_validation_policy(|policy| policy.open_rate_limit(1, Duration::from_secs(60)));
    open_rate_limited
        .open_link(open_request_with_id(&stream, LinkId::new("link-rate-1")))
        .unwrap();
    let reject = open_rate_limited
        .open_link(open_request_with_id(&stream, LinkId::new("link-rate-2")))
        .unwrap_err();
    assert_eq!(reject.reason, OpenLinkRejectReason::Overloaded);

    let message_rate_limited = configured_manager(&stream);
    message_rate_limited
        .update_validation_policy(|policy| policy.message_rate_limit(1, Duration::from_secs(60)));
    let link_id = LinkId::new("link-message-rate");
    message_rate_limited
        .open_link(open_request_with_id(&stream, link_id.clone()))
        .unwrap();
    message_rate_limited
        .validate_message_frame(
            &link_id,
            LinkDirection::SourceToTarget,
            DirectLinkMessageId(1),
            LinkSequence(1),
        )
        .unwrap();
    assert_eq!(
        message_rate_limited.validate_message_frame(
            &link_id,
            LinkDirection::SourceToTarget,
            DirectLinkMessageId(1),
            LinkSequence(2),
        ),
        Err(MessageFrameError::RateLimited)
    );

    let fenced = configured_manager(&stream);
    fenced.register_actor(
        actor_kind!("Battle"),
        DirectLinkActorPolicy::active(Some(Epoch(2))),
    );
    let mut request = open_request(&stream);
    request.target = actor_ref_with_epoch(
        service_kind!("Battle"),
        actor_kind!("Battle"),
        9,
        Some(Epoch(1)),
    );
    let reject = fenced.open_link(request).unwrap_err();
    assert_eq!(reject.reason, OpenLinkRejectReason::Fenced);

    let inactive = configured_manager(&stream);
    inactive.register_actor(
        actor_kind!("Battle"),
        DirectLinkActorPolicy {
            activation: DirectLinkActivationPolicy::ExistingOnly,
            active: false,
            owner_epoch: None,
        },
    );
    let reject = inactive.open_link(open_request(&stream)).unwrap_err();
    assert_eq!(reject.reason, OpenLinkRejectReason::ActorUnavailable);

    let lazy = configured_manager(&stream);
    lazy.register_actor(actor_kind!("Battle"), DirectLinkActorPolicy::lazy(None));
    assert!(lazy.open_link(open_request(&stream)).is_ok());
}

#[test]
fn open_link_binds_required_peer_identity_to_source_metadata() {
    let stream = stream("movement", &[1]);
    let manager = configured_manager(&stream);
    manager.set_validation_policy(
        OpenLinkValidationPolicy::hosted(service_kind!("Battle"))
            .authorize_sources([service_kind!("Gateway")])
            .require_peer_identity("lattice.test"),
    );

    let reject = manager.open_link(open_request(&stream)).unwrap_err();
    assert_eq!(reject.reason, OpenLinkRejectReason::Unauthorized);

    let wrong_service = DirectLinkPeerIdentity::new(
        service_kind!("Intruder"),
        InstanceId::new("instance-7"),
        "spiffe://lattice.test/svc/intruder/instance/instance-7",
    );
    let reject = manager
        .open_link_from_peer(open_request(&stream), Some(wrong_service))
        .unwrap_err();
    assert_eq!(reject.reason, OpenLinkRejectReason::Unauthorized);

    let wrong_instance = DirectLinkPeerIdentity::new(
        service_kind!("Gateway"),
        InstanceId::new("instance-8"),
        "spiffe://lattice.test/svc/gateway/instance/instance-8",
    );
    let reject = manager
        .open_link_from_peer(open_request(&stream), Some(wrong_instance))
        .unwrap_err();
    assert_eq!(reject.reason, OpenLinkRejectReason::Unauthorized);

    let wrong_trust_domain = DirectLinkPeerIdentity::new(
        service_kind!("Gateway"),
        InstanceId::new("instance-7"),
        "spiffe://other.test/svc/gateway/instance/instance-7",
    );
    let reject = manager
        .open_link_from_peer(open_request(&stream), Some(wrong_trust_domain))
        .unwrap_err();
    assert_eq!(reject.reason, OpenLinkRejectReason::Unauthorized);

    let accepted = DirectLinkPeerIdentity::new(
        service_kind!("Gateway"),
        InstanceId::new("instance-7"),
        "spiffe://lattice.test/svc/gateway/instance/instance-7",
    );
    assert!(
        manager
            .open_link_from_peer(
                open_request_with_id(&stream, LinkId::new("link-peer-ok")),
                Some(accepted)
            )
            .is_ok()
    );
}

#[test]
fn bidirectional_close_keeps_opposite_direction_until_closed() {
    let manager = DirectLinkSessionManager::new();
    let outbound = stream("input", &[10]);
    let inbound = stream("updates", &[20]);
    manager
        .register_binding(actor_kind!("Battle"), outbound.clone())
        .unwrap();
    manager
        .register_binding(actor_kind!("GatewaySession"), inbound.clone())
        .unwrap();
    let link_id = LinkId::new("link-1");
    manager
        .open_link(OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
            mode: DirectLinkMode::Bidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &outbound),
            target_to_source: Some(OpenLinkDirection::from_stream(link_id.clone(), &inbound)),
            options: DirectLinkOptions::bidirectional(),
        })
        .unwrap();

    match manager
        .close_direction(
            &link_id,
            LinkDirection::SourceToTarget,
            LinkCloseReason::Done,
        )
        .unwrap()
    {
        CloseTransition::DirectionClosed(event) => {
            assert_eq!(event.reason, LinkCloseReason::Done);
            assert_eq!(event.direction, LinkDirection::SourceToTarget);
            assert_eq!(event.stream, "input");
        }
        other => panic!("expected direction close, got {other:?}"),
    }
    manager
        .validate_message_frame(
            &link_id,
            LinkDirection::TargetToSource,
            DirectLinkMessageId(20),
            LinkSequence(1),
        )
        .unwrap();
    match manager
        .close_direction(
            &link_id,
            LinkDirection::TargetToSource,
            LinkCloseReason::Done,
        )
        .unwrap()
    {
        CloseTransition::LinkClosed {
            direction_closed,
            link_closed,
        } => {
            assert_eq!(direction_closed.direction, LinkDirection::TargetToSource);
            assert_eq!(direction_closed.stream, "updates");
            assert_eq!(
                link_closed.closed_directions,
                [LinkDirection::SourceToTarget, LinkDirection::TargetToSource]
                    .into_iter()
                    .collect()
            );
        }
        other => panic!("expected link close, got {other:?}"),
    }
    assert_eq!(manager.metrics().snapshot().closed, 1);
}

#[test]
fn observability_hooks_increment_metrics() {
    let descriptor = stream("movement", &[10]);
    let manager = configured_manager(&descriptor);
    let link_id = LinkId::new("link-observed");

    manager
        .open_link(open_request_with_id(&descriptor, link_id.clone()))
        .unwrap();
    manager
        .validate_message_frame(
            &link_id,
            LinkDirection::SourceToTarget,
            DirectLinkMessageId(10),
            LinkSequence(1),
        )
        .unwrap();
    assert!(manager.close(&link_id, LinkCloseReason::Done));

    manager.record_decode_error(Some(&link_id), "bad payload");
    manager.record_backpressure(
        &link_id,
        &BackpressurePolicy::DropOldest { max_pending: 1 },
        1,
    );
    manager.record_drop(&link_id, DirectLinkMessageId(10));
    manager.record_coalesce(&link_id, DirectLinkMessageId(10));

    let metrics = manager.metrics().snapshot();
    assert_eq!(metrics.opened, 1);
    assert_eq!(metrics.received, 1);
    assert_eq!(metrics.closed, 1);
    assert_eq!(metrics.decode_errors, 1);
    assert_eq!(metrics.backpressure_events, 1);
    assert_eq!(metrics.dropped, 1);
    assert_eq!(metrics.coalesced, 1);
}

fn stream(name: &str, ids: &[u64]) -> DirectLinkStreamDescriptor {
    DirectLinkStreamDescriptor {
        stream_name: name.to_string(),
        messages: ids
            .iter()
            .map(|id| DirectLinkMessageDescriptor {
                message_id: DirectLinkMessageId(*id),
                proto_full_name: format!("game.Message{id}"),
                rust_type_name: format!("Message{id}"),
            })
            .collect(),
    }
}

fn configured_manager(stream: &DirectLinkStreamDescriptor) -> DirectLinkSessionManager {
    let manager = DirectLinkSessionManager::new();
    manager.set_validation_policy(
        OpenLinkValidationPolicy::hosted(service_kind!("Battle"))
            .authorize_sources([service_kind!("Gateway")])
            .max_pending(1024)
            .max_frame_size(256 * 1024),
    );
    manager.register_actor(actor_kind!("Battle"), DirectLinkActorPolicy::active(None));
    manager
        .register_binding(actor_kind!("Battle"), stream.clone())
        .unwrap();
    manager
}

fn open_request(stream: &DirectLinkStreamDescriptor) -> OpenLinkRequest {
    open_request_with_id(stream, LinkId::new("link-policy"))
}

fn open_request_with_id(stream: &DirectLinkStreamDescriptor, link_id: LinkId) -> OpenLinkRequest {
    OpenLinkRequest {
        protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
        link_id: link_id.clone(),
        source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
        target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
        mode: DirectLinkMode::Unidirectional,
        source_to_target: OpenLinkDirection::from_stream(link_id, stream),
        target_to_source: None,
        options: DirectLinkOptions::default(),
    }
}

fn actor_ref(service_kind: ServiceKind, actor_kind: ActorKind, id: u64) -> ActorRef {
    actor_ref_with_epoch(service_kind, actor_kind, id, None)
}

fn actor_ref_with_epoch(
    service_kind: ServiceKind,
    actor_kind: ActorKind,
    id: u64,
    owner_epoch: Option<Epoch>,
) -> ActorRef {
    ActorRef::direct(
        service_kind,
        actor_kind,
        ActorId::U64(id),
        InstanceId::new(format!("instance-{id}")),
        Uri::from_str("http://127.0.0.1:10000").unwrap(),
        owner_epoch,
    )
}
