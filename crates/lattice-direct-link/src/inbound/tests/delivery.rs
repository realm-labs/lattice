use super::*;

#[tokio::test]
async fn inbound_router_delivers_message_frame_to_target_actor_mailbox() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let handle = ActorRuntime::default()
        .spawn_actor(
            BattleActor {
                received: received.clone(),
            },
            Default::default(),
        )
        .await
        .unwrap();
    let manager = Arc::new(DirectLinkSessionManager::new());
    let stream = DirectLinkStream::new("movement").message::<PositionUpdate>();
    let descriptor = stream.descriptor();
    let binding = stream.for_actor::<BattleActor>(actor_kind!("Battle"));
    manager
        .register_binding(actor_kind!("Battle"), descriptor.clone())
        .unwrap();
    let link_id = LinkId::new("link-inbound");
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
    let router = DirectLinkInboundRouter::builder(manager)
        .bind_actor(binding, move |_| Some(handle.clone()))
        .build();
    let message_id = descriptor.message_id_for::<PositionUpdate>().unwrap();
    let frame = DirectLinkFrame::message(
        link_id,
        LinkSequence(1),
        message_id,
        PositionUpdate { tick: 99 }.encode_to_vec(),
    );

    router.deliver_frame(frame).unwrap();

    timeout(Duration::from_secs(1), async {
        loop {
            if !received.lock().expect("received mutex poisoned").is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(*received.lock().expect("received mutex poisoned"), vec![99]);
}

#[tokio::test]
async fn inbound_router_does_not_advance_sequence_when_mailbox_is_full() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let handle = ActorRuntime::default()
        .spawn_actor(
            BlockingActor {
                received: received.clone(),
                entered: entered.clone(),
                release: release.clone(),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    handle.try_tell(linked_command(100)).unwrap();
    timeout(Duration::from_secs(1), entered.notified())
        .await
        .unwrap();
    handle.try_tell(linked_command(101)).unwrap();

    let manager = Arc::new(DirectLinkSessionManager::new());
    let stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
    let descriptor = stream.descriptor();
    manager
        .register_binding(actor_kind!("Battle"), descriptor.clone())
        .unwrap();
    let link_id = LinkId::new("link-mailbox-full-sequence");
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
    let handle_for_router = handle.clone();
    let router = DirectLinkInboundRouter::builder(manager)
        .bind_actor(
            stream.for_actor::<BlockingActor>(actor_kind!("Battle")),
            move |_| Some(handle_for_router.clone()),
        )
        .build();
    let message_id = descriptor.message_id_for::<InputCommand>().unwrap();
    let frame = || {
        DirectLinkFrame::message(
            link_id.clone(),
            LinkSequence(1),
            message_id,
            InputCommand { command_id: 11 }.encode_to_vec(),
        )
    };

    assert!(matches!(
        router.deliver_frame(frame()),
        Err(InboundDeliveryError::Delivery(
            DirectLinkDeliveryError::Mailbox(ActorTellError::MailboxFull)
        ))
    ));

    release.notify_waiters();
    wait_for_len(&received, 2).await;

    router.deliver_frame(frame()).unwrap();
    wait_for_len(&received, 3).await;
    assert_eq!(
        *received.lock().expect("received mutex poisoned"),
        vec![100, 101, 11]
    );
}

#[tokio::test]
async fn inbound_router_delivers_bidirectional_frames_to_each_direction_actor() {
    let battle_received = Arc::new(Mutex::new(Vec::new()));
    let gateway_received = Arc::new(Mutex::new(Vec::new()));
    let runtime = ActorRuntime::default();
    let battle_handle = runtime
        .spawn_actor(
            BattleActor {
                received: battle_received.clone(),
            },
            Default::default(),
        )
        .await
        .unwrap();
    let gateway_handle = runtime
        .spawn_actor(
            GatewayActor {
                received: gateway_received.clone(),
            },
            Default::default(),
        )
        .await
        .unwrap();
    let manager = Arc::new(DirectLinkSessionManager::new());
    let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
    let update_stream = DirectLinkStream::new("battle-update").message::<PositionUpdate>();
    let input_descriptor = input_stream.descriptor();
    let update_descriptor = update_stream.descriptor();
    manager
        .register_binding(actor_kind!("Battle"), input_descriptor.clone())
        .unwrap();
    manager
        .register_binding(actor_kind!("GatewaySession"), update_descriptor.clone())
        .unwrap();
    let link_id = LinkId::new("link-bidirectional");
    manager
        .open_link(OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
            mode: DirectLinkMode::Bidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &input_descriptor),
            target_to_source: Some(OpenLinkDirection::from_stream(
                link_id.clone(),
                &update_descriptor,
            )),
            options: DirectLinkOptions::bidirectional(),
        })
        .unwrap();
    let router = DirectLinkInboundRouter::builder(manager.clone())
        .bind_actor(
            input_stream.for_actor::<BattleActor>(actor_kind!("Battle")),
            move |_| Some(battle_handle.clone()),
        )
        .bind_actor(
            update_stream.for_actor::<GatewayActor>(actor_kind!("GatewaySession")),
            move |_| Some(gateway_handle.clone()),
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
    router
        .deliver_frame(DirectLinkFrame::directed_message(
            link_id.clone(),
            LinkDirection::TargetToSource,
            LinkSequence(1),
            update_descriptor
                .message_id_for::<PositionUpdate>()
                .unwrap(),
            PositionUpdate { tick: 22 }.encode_to_vec(),
        ))
        .unwrap();

    timeout(Duration::from_secs(1), async {
        loop {
            let battle_done = !battle_received
                .lock()
                .expect("received mutex poisoned")
                .is_empty();
            let gateway_done = !gateway_received
                .lock()
                .expect("received mutex poisoned")
                .is_empty();
            if battle_done && gateway_done {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        *battle_received.lock().expect("received mutex poisoned"),
        vec![11]
    );
    assert_eq!(
        *gateway_received.lock().expect("received mutex poisoned"),
        vec![22]
    );
    assert_eq!(
        manager.link_snapshot(&link_id).unwrap().directions,
        [LinkDirection::SourceToTarget, LinkDirection::TargetToSource]
            .into_iter()
            .collect()
    );
}

#[tokio::test]
async fn inbound_router_delivers_link_opened_and_actor_gets_target_to_source_handle() {
    let opened = Arc::new(Mutex::new(Vec::new()));
    let outbound = Arc::new(Mutex::new(None));
    let runtime = Arc::new(RecordingLinkRuntime::default());
    let mut service = ServiceContext::builder(service_kind!("Battle"), InstanceId::new("battle-1"));
    service
        .insert_extension(DirectLinkRuntimeHandle::new(runtime.clone()))
        .unwrap();
    let link_id = LinkId::new("link-opened");
    let target_ref = actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9);
    let handle = ActorRuntime::default()
        .spawn_actor(
            OpeningBattleActor {
                opened: opened.clone(),
                outbound: outbound.clone(),
            },
            ActorSpawnOptions {
                self_ref: Some(target_ref.clone()),
                service: service.build(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let manager = Arc::new(DirectLinkSessionManager::new());
    let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
    let update_stream = DirectLinkStream::new("battle-update").message::<PositionUpdate>();
    let input_descriptor = input_stream.descriptor();
    let update_descriptor = update_stream.descriptor();
    manager
        .register_binding(actor_kind!("Battle"), input_descriptor.clone())
        .unwrap();
    manager
        .register_binding(actor_kind!("GatewaySession"), update_descriptor.clone())
        .unwrap();
    manager
        .open_link(OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: target_ref,
            mode: DirectLinkMode::Bidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &input_descriptor),
            target_to_source: Some(OpenLinkDirection::from_stream(
                link_id.clone(),
                &update_descriptor,
            )),
            options: DirectLinkOptions::bidirectional(),
        })
        .unwrap();
    let router = DirectLinkInboundRouter::builder(manager)
        .bind_actor(
            input_stream.for_actor::<OpeningBattleActor>(actor_kind!("Battle")),
            move |_| Some(handle.clone()),
        )
        .build();

    router.deliver_link_opened_to_target(&link_id).unwrap();

    timeout(Duration::from_secs(1), async {
        loop {
            if outbound
                .lock()
                .expect("outbound handle mutex poisoned")
                .is_some()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let opened = opened.lock().expect("opened messages mutex poisoned");
    assert_eq!(opened.len(), 1);
    assert_eq!(opened[0].mode, DirectLinkMode::Bidirectional);
    assert_eq!(opened[0].inbound_stream, "gateway-input");
    assert_eq!(opened[0].outbound_stream.as_deref(), Some("battle-update"));
    assert_eq!(
        *outbound.lock().expect("outbound handle mutex poisoned"),
        Some((LinkDirection::TargetToSource, "battle-update".to_string()))
    );
    assert_eq!(
        runtime
            .outbound_requests
            .lock()
            .expect("outbound requests mutex poisoned")
            .as_slice(),
        &[(link_id, BattleUpdateStream::descriptor())]
    );
}

#[tokio::test]
async fn process_open_link_frame_returns_ack_and_delivers_link_opened() {
    let opened = Arc::new(Mutex::new(Vec::new()));
    let outbound = Arc::new(Mutex::new(None));
    let runtime = Arc::new(RecordingLinkRuntime::default());
    let mut service = ServiceContext::builder(service_kind!("Battle"), InstanceId::new("battle-1"));
    service
        .insert_extension(DirectLinkRuntimeHandle::new(runtime.clone()))
        .unwrap();
    let link_id = LinkId::new("link-open-frame");
    let target_ref = actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9);
    let handle = ActorRuntime::default()
        .spawn_actor(
            OpeningBattleActor {
                opened: opened.clone(),
                outbound: outbound.clone(),
            },
            ActorSpawnOptions {
                self_ref: Some(target_ref.clone()),
                service: service.build(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let manager = Arc::new(DirectLinkSessionManager::new());
    let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
    let update_stream = DirectLinkStream::new("battle-update").message::<PositionUpdate>();
    let input_descriptor = input_stream.descriptor();
    let update_descriptor = update_stream.descriptor();
    manager
        .register_binding(actor_kind!("Battle"), input_descriptor.clone())
        .unwrap();
    manager
        .register_binding(actor_kind!("GatewaySession"), update_descriptor.clone())
        .unwrap();
    manager.set_validation_policy(
        OpenLinkValidationPolicy::hosted(service_kind!("Battle"))
            .authorize_sources([service_kind!("Gateway")])
            .require_peer_identity("lattice.test"),
    );
    let router = DirectLinkInboundRouter::builder(manager)
        .bind_actor(
            input_stream.for_actor::<OpeningBattleActor>(actor_kind!("Battle")),
            move |_| Some(handle.clone()),
        )
        .build();

    let response = router
        .process_open_link_frame(
            DirectLinkFrame::open_link_with_peer_identity(
                &OpenLinkRequest {
                    protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                    link_id: link_id.clone(),
                    source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                    target: target_ref,
                    mode: DirectLinkMode::Bidirectional,
                    source_to_target: OpenLinkDirection::from_stream(
                        link_id.clone(),
                        &input_descriptor,
                    ),
                    target_to_source: Some(OpenLinkDirection::from_stream(
                        link_id.clone(),
                        &update_descriptor,
                    )),
                    options: DirectLinkOptions::bidirectional(),
                },
                DirectLinkPeerIdentity::new(
                    service_kind!("Gateway"),
                    InstanceId::new("instance-7"),
                    "spiffe://lattice.test/svc/gateway/instance/instance-7",
                ),
            )
            .unwrap(),
            None,
        )
        .unwrap();

    assert_eq!(response.kind, DirectLinkFrameKind::OpenLinkAck);
    let ack = response.decode_open_link_ack().unwrap();
    assert_eq!(ack.link_id, link_id);
    assert_eq!(ack.source_to_target.stream_name, "gateway-input");
    assert_eq!(
        ack.target_to_source
            .as_ref()
            .expect("target-to-source negotiation")
            .stream_name,
        "battle-update"
    );
    wait_for_len(&opened, 1).await;
    let opened = opened.lock().expect("opened messages mutex poisoned");
    assert_eq!(opened[0].link_id, link_id);
    assert_eq!(opened[0].mode, DirectLinkMode::Bidirectional);
    assert_eq!(opened[0].inbound_stream, "gateway-input");
    assert_eq!(opened[0].outbound_stream.as_deref(), Some("battle-update"));
    assert_eq!(
        *outbound.lock().expect("outbound handle mutex poisoned"),
        Some((LinkDirection::TargetToSource, "battle-update".to_string()))
    );
}

#[tokio::test]
async fn process_open_link_frame_rejects_missing_required_peer_identity() {
    let manager = Arc::new(DirectLinkSessionManager::new());
    let stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
    let descriptor = stream.descriptor();
    manager
        .register_binding(actor_kind!("Battle"), descriptor.clone())
        .unwrap();
    manager.register_actor(actor_kind!("Battle"), DirectLinkActorPolicy::lazy(None));
    manager.set_validation_policy(
        OpenLinkValidationPolicy::hosted(service_kind!("Battle"))
            .authorize_sources([service_kind!("Gateway")])
            .require_peer_identity("lattice.test"),
    );
    let link_id = LinkId::new("link-open-missing-identity");
    let router = DirectLinkInboundRouter::builder(manager).build();

    let response = router
        .process_open_link_frame(
            DirectLinkFrame::open_link(&OpenLinkRequest {
                protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                link_id: link_id.clone(),
                source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
                mode: DirectLinkMode::Unidirectional,
                source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &descriptor),
                target_to_source: None,
                options: DirectLinkOptions::default(),
            })
            .unwrap(),
            None,
        )
        .unwrap();

    assert_eq!(response.kind, DirectLinkFrameKind::OpenLinkReject);
    let reject = response.decode_open_link_reject().unwrap();
    assert_eq!(reject.reason, OpenLinkRejectReason::Unauthorized);
}

#[tokio::test]
async fn process_open_link_frame_rejects_when_link_open_delivery_fails() {
    let manager = Arc::new(DirectLinkSessionManager::new());
    let stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
    let descriptor = stream.descriptor();
    manager
        .register_binding(actor_kind!("Battle"), descriptor.clone())
        .unwrap();
    manager.register_actor(actor_kind!("Battle"), DirectLinkActorPolicy::lazy(None));
    manager.set_validation_policy(
        OpenLinkValidationPolicy::hosted(service_kind!("Battle"))
            .authorize_sources([service_kind!("Gateway")]),
    );
    let link_id = LinkId::new("link-open-delivery-fails");
    let router = DirectLinkInboundRouter::builder(manager)
        .bind_actor(
            stream.for_actor::<BattleActor>(actor_kind!("Battle")),
            |_| None,
        )
        .build();

    let response = router
        .process_open_link_frame(
            DirectLinkFrame::open_link(&OpenLinkRequest {
                protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                link_id: link_id.clone(),
                source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
                mode: DirectLinkMode::Unidirectional,
                source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &descriptor),
                target_to_source: None,
                options: DirectLinkOptions::default(),
            })
            .unwrap(),
            None,
        )
        .unwrap();

    assert_eq!(response.kind, DirectLinkFrameKind::OpenLinkReject);
    let reject = response.decode_open_link_reject().unwrap();
    assert_eq!(reject.link_id, link_id);
    assert_eq!(reject.reason, OpenLinkRejectReason::ActorUnavailable);
}
