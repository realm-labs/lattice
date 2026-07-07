use super::*;

#[tokio::test]
async fn process_frame_closes_invalid_message_frames_with_protocol_error() {
    for (name, frame) in [
        ("wrong direction", ProtocolErrorFrame::WrongDirection),
        (
            "unsupported message type",
            ProtocolErrorFrame::UnsupportedMessageType,
        ),
        ("decode error", ProtocolErrorFrame::DecodeError),
    ] {
        let link_id = LinkId::new(format!("link-protocol-error-{name}"));
        let (router, descriptor, received, link_closed) =
            protocol_error_test_router(link_id.clone()).await;
        let message_id = descriptor.message_id_for::<InputCommand>().unwrap();
        let frame = match frame {
            ProtocolErrorFrame::WrongDirection => DirectLinkFrame::directed_message(
                link_id.clone(),
                LinkDirection::TargetToSource,
                LinkSequence(1),
                message_id,
                InputCommand { command_id: 11 }.encode_to_vec(),
            ),
            ProtocolErrorFrame::UnsupportedMessageType => DirectLinkFrame::message(
                link_id.clone(),
                LinkSequence(1),
                DirectLinkMessageId(999),
                InputCommand { command_id: 11 }.encode_to_vec(),
            ),
            ProtocolErrorFrame::DecodeError => DirectLinkFrame::message(
                link_id.clone(),
                LinkSequence(1),
                message_id,
                b"not protobuf".to_vec(),
            ),
        };

        assert!(router.process_frame(frame).is_err());
        wait_for_len(&link_closed, 1).await;
        assert!(received.lock().expect("received mutex poisoned").is_empty());
        let event = link_closed.lock().expect("link closed mutex poisoned")[0].clone();
        assert_eq!(event.link_id, link_id);
        assert!(matches!(
            event.reason,
            LinkCloseReason::ProtocolError(ref reason) if reason.contains(name)
        ));
    }
}

#[tokio::test]
async fn process_frame_closes_remote_protocol_error_frame() {
    let link_id = LinkId::new("link-remote-protocol-error");
    let (router, _descriptor, _received, link_closed) =
        protocol_error_test_router(link_id.clone()).await;
    let frame = DirectLinkFrame {
        kind: DirectLinkFrameKind::ProtocolError,
        link_id: link_id.clone(),
        sequence: LinkSequence(0),
        message_id: None,
        flags: LinkMessageFlags::EMPTY,
        header: Vec::new(),
        payload: b"remote invalid sequence".to_vec(),
    };

    router.process_frame(frame).unwrap();
    wait_for_len(&link_closed, 1).await;
    let event = link_closed.lock().expect("link closed mutex poisoned")[0].clone();
    assert_eq!(event.link_id, link_id);
    assert_eq!(
        event.reason,
        LinkCloseReason::ProtocolError("remote invalid sequence".to_string())
    );
}

#[tokio::test]
async fn inbound_backpressure_drop_newest_emits_event_without_mailbox_delivery() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let backpressure = Arc::new(Mutex::new(Vec::new()));
    let direction_closed = Arc::new(Mutex::new(Vec::new()));
    let link_closed = Arc::new(Mutex::new(Vec::new()));
    let handle = ActorRuntime::default()
        .spawn_actor(
            BackpressureActor {
                received: received.clone(),
                backpressure: backpressure.clone(),
                direction_closed,
                link_closed,
            },
            Default::default(),
        )
        .await
        .unwrap();
    let manager = Arc::new(DirectLinkSessionManager::new());
    let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
    let input_descriptor = input_stream.descriptor();
    manager
        .register_binding(actor_kind!("Battle"), input_descriptor.clone())
        .unwrap();
    let link_id = LinkId::new("link-inbound-drop-newest");
    let mut options = DirectLinkOptions::unidirectional();
    options.backpressure = BackpressurePolicy::DropNewest { max_pending: 0 };
    manager
        .open_link(OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &input_descriptor),
            target_to_source: None,
            options,
        })
        .unwrap();
    let router = DirectLinkInboundRouter::builder(manager.clone())
        .bind_actor(
            input_stream.for_actor::<BackpressureActor>(actor_kind!("Battle")),
            move |_| Some(handle.clone()),
        )
        .build();

    router
        .deliver_frame(DirectLinkFrame::message(
            link_id.clone(),
            LinkSequence(1),
            input_descriptor.message_id_for::<InputCommand>().unwrap(),
            InputCommand { command_id: 11 }.encode_to_vec(),
        ))
        .unwrap();

    wait_for_len(&backpressure, 1).await;
    assert!(received.lock().expect("received mutex poisoned").is_empty());
    let events = backpressure.lock().expect("backpressure mutex poisoned");
    assert_eq!(events[0].link_id, link_id);
    assert_eq!(
        events[0].policy,
        BackpressurePolicy::DropNewest { max_pending: 0 }
    );
    assert_eq!(events[0].pending, 0);
    assert_eq!(events[0].dropped, 1);
    assert_eq!(manager.metrics().snapshot().dropped, 1);
    assert_eq!(manager.metrics().snapshot().backpressure_events, 1);
}

#[tokio::test]
async fn inbound_backpressure_disconnect_closes_link_with_event() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let backpressure = Arc::new(Mutex::new(Vec::new()));
    let direction_closed = Arc::new(Mutex::new(Vec::new()));
    let link_closed = Arc::new(Mutex::new(Vec::new()));
    let handle = ActorRuntime::default()
        .spawn_actor(
            BackpressureActor {
                received: received.clone(),
                backpressure: backpressure.clone(),
                direction_closed: direction_closed.clone(),
                link_closed: link_closed.clone(),
            },
            Default::default(),
        )
        .await
        .unwrap();
    let manager = Arc::new(DirectLinkSessionManager::new());
    let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
    let input_descriptor = input_stream.descriptor();
    manager
        .register_binding(actor_kind!("Battle"), input_descriptor.clone())
        .unwrap();
    let link_id = LinkId::new("link-inbound-disconnect");
    let mut options = DirectLinkOptions::unidirectional();
    options.backpressure = BackpressurePolicy::Disconnect { max_pending: 0 };
    manager
        .open_link(OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &input_descriptor),
            target_to_source: None,
            options,
        })
        .unwrap();
    let router = DirectLinkInboundRouter::builder(manager.clone())
        .bind_actor(
            input_stream.for_actor::<BackpressureActor>(actor_kind!("Battle")),
            move |_| Some(handle.clone()),
        )
        .build();

    assert!(matches!(
        router.deliver_frame(DirectLinkFrame::message(
            link_id.clone(),
            LinkSequence(1),
            input_descriptor.message_id_for::<InputCommand>().unwrap(),
            InputCommand { command_id: 11 }.encode_to_vec(),
        )),
        Err(InboundDeliveryError::BackpressureExceeded)
    ));

    wait_for_len(&backpressure, 1).await;
    wait_for_len(&direction_closed, 1).await;
    wait_for_len(&link_closed, 1).await;
    assert!(received.lock().expect("received mutex poisoned").is_empty());
    assert_eq!(
        direction_closed
            .lock()
            .expect("direction closed mutex poisoned")[0]
            .reason,
        LinkCloseReason::BackpressureExceeded
    );
    assert_eq!(
        link_closed.lock().expect("link closed mutex poisoned")[0].reason,
        LinkCloseReason::BackpressureExceeded
    );
    assert_eq!(manager.metrics().snapshot().closed, 1);
    assert_eq!(manager.metrics().snapshot().backpressure_events, 1);
}

#[test]
fn inbound_router_rejects_unbound_actor_kind() {
    let manager = Arc::new(DirectLinkSessionManager::new());
    let stream = DirectLinkStream::new("movement").message::<PositionUpdate>();
    let descriptor = stream.descriptor();
    manager
        .register_binding(actor_kind!("Battle"), descriptor.clone())
        .unwrap();
    let link_id = LinkId::new("link-unbound");
    manager
        .open_link(OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &descriptor),
            target_to_source: None,
            options: DirectLinkOptions::default(),
        })
        .unwrap();
    let router = DirectLinkInboundRouter::builder(manager).build();
    let frame = DirectLinkFrame::message(
        link_id,
        LinkSequence(1),
        descriptor.message_id_for::<PositionUpdate>().unwrap(),
        PositionUpdate { tick: 1 }.encode_to_vec(),
    );

    assert!(matches!(
        router.deliver_frame(frame),
        Err(InboundDeliveryError::UnboundActorKind { .. })
    ));
}

enum ProtocolErrorFrame {
    WrongDirection,
    UnsupportedMessageType,
    DecodeError,
}

async fn protocol_error_test_router(
    link_id: LinkId,
) -> (
    DirectLinkInboundRouter,
    DirectLinkStreamDescriptor,
    Arc<Mutex<Vec<u64>>>,
    Arc<Mutex<Vec<LinkClosed>>>,
) {
    let received = Arc::new(Mutex::new(Vec::new()));
    let backpressure = Arc::new(Mutex::new(Vec::new()));
    let direction_closed = Arc::new(Mutex::new(Vec::new()));
    let link_closed = Arc::new(Mutex::new(Vec::new()));
    let handle = ActorRuntime::default()
        .spawn_actor(
            BackpressureActor {
                received: received.clone(),
                backpressure,
                direction_closed,
                link_closed: link_closed.clone(),
            },
            Default::default(),
        )
        .await
        .unwrap();
    let manager = Arc::new(DirectLinkSessionManager::new());
    let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
    let input_descriptor = input_stream.descriptor();
    manager
        .register_binding(actor_kind!("Battle"), input_descriptor.clone())
        .unwrap();
    manager
        .open_link(OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id, &input_descriptor),
            target_to_source: None,
            options: DirectLinkOptions::unidirectional(),
        })
        .unwrap();
    let router = DirectLinkInboundRouter::builder(manager)
        .bind_actor(
            input_stream.for_actor::<BackpressureActor>(actor_kind!("Battle")),
            move |_| Some(handle.clone()),
        )
        .build();

    (router, input_descriptor, received, link_closed)
}
