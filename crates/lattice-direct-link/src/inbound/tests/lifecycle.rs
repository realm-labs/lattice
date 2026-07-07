use super::*;

#[tokio::test]
async fn inbound_router_emits_direction_and_link_closed_once_per_transition() {
    let target_direction_closed = Arc::new(Mutex::new(Vec::new()));
    let source_direction_closed = Arc::new(Mutex::new(Vec::new()));
    let target_link_closed = Arc::new(Mutex::new(Vec::new()));
    let source_link_closed = Arc::new(Mutex::new(Vec::new()));
    let runtime = ActorRuntime::default();
    let target_handle = runtime
        .spawn_actor(
            ClosingActor {
                direction_closed: target_direction_closed.clone(),
                link_closed: target_link_closed.clone(),
            },
            Default::default(),
        )
        .await
        .unwrap();
    let source_handle = runtime
        .spawn_actor(
            ClosingActor {
                direction_closed: source_direction_closed.clone(),
                link_closed: source_link_closed.clone(),
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
    let link_id = LinkId::new("link-close-events");
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
    let router = DirectLinkInboundRouter::builder(manager)
        .bind_actor(
            input_stream.for_actor::<ClosingActor>(actor_kind!("Battle")),
            move |_| Some(target_handle.clone()),
        )
        .bind_actor(
            update_stream.for_actor::<ClosingActor>(actor_kind!("GatewaySession")),
            move |_| Some(source_handle.clone()),
        )
        .build();

    router
        .close_direction(
            &link_id,
            LinkDirection::SourceToTarget,
            LinkCloseReason::Done,
        )
        .unwrap();
    router
        .close_direction(
            &link_id,
            LinkDirection::SourceToTarget,
            LinkCloseReason::Done,
        )
        .unwrap();
    wait_for_len(&target_direction_closed, 1).await;
    assert_eq!(
        target_direction_closed
            .lock()
            .expect("direction closed mutex poisoned")
            .len(),
        1
    );

    router
        .close_direction(
            &link_id,
            LinkDirection::TargetToSource,
            LinkCloseReason::Done,
        )
        .unwrap();
    router
        .close_direction(
            &link_id,
            LinkDirection::TargetToSource,
            LinkCloseReason::Done,
        )
        .unwrap();
    wait_for_len(&source_direction_closed, 1).await;
    wait_for_len(&target_link_closed, 1).await;
    wait_for_len(&source_link_closed, 1).await;

    assert_eq!(
        source_direction_closed
            .lock()
            .expect("direction closed mutex poisoned")
            .len(),
        1
    );
    assert_eq!(
        target_link_closed
            .lock()
            .expect("link closed mutex poisoned")
            .len(),
        1
    );
    assert_eq!(
        source_link_closed
            .lock()
            .expect("link closed mutex poisoned")
            .len(),
        1
    );
    assert_eq!(
        target_direction_closed
            .lock()
            .expect("direction closed mutex poisoned")[0]
            .stream,
        "gateway-input"
    );
    assert_eq!(
        source_direction_closed
            .lock()
            .expect("direction closed mutex poisoned")[0]
            .stream,
        "battle-update"
    );
}

#[tokio::test]
async fn inbound_router_close_all_emits_structured_reasons_once() {
    for reason in [
        LinkCloseReason::HeartbeatTimeout,
        LinkCloseReason::ProtocolError("invalid sequence".to_string()),
        LinkCloseReason::TargetPassivated,
        LinkCloseReason::TargetMigrating,
        LinkCloseReason::NodeDraining,
        LinkCloseReason::ConnectionLost,
    ] {
        let target_direction_closed = Arc::new(Mutex::new(Vec::new()));
        let source_direction_closed = Arc::new(Mutex::new(Vec::new()));
        let target_link_closed = Arc::new(Mutex::new(Vec::new()));
        let source_link_closed = Arc::new(Mutex::new(Vec::new()));
        let runtime = ActorRuntime::default();
        let target_handle = runtime
            .spawn_actor(
                ClosingActor {
                    direction_closed: target_direction_closed.clone(),
                    link_closed: target_link_closed.clone(),
                },
                Default::default(),
            )
            .await
            .unwrap();
        let source_handle = runtime
            .spawn_actor(
                ClosingActor {
                    direction_closed: source_direction_closed.clone(),
                    link_closed: source_link_closed.clone(),
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
        let link_id = LinkId::new(format!("link-close-all-{reason:?}"));
        manager
            .open_link(OpenLinkRequest {
                protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                link_id: link_id.clone(),
                source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
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
            })
            .unwrap();
        let router = DirectLinkInboundRouter::builder(manager)
            .bind_actor(
                input_stream.for_actor::<ClosingActor>(actor_kind!("Battle")),
                move |_| Some(target_handle.clone()),
            )
            .bind_actor(
                update_stream.for_actor::<ClosingActor>(actor_kind!("GatewaySession")),
                move |_| Some(source_handle.clone()),
            )
            .build();

        router.close_all(&link_id, reason.clone()).unwrap();
        router.close_all(&link_id, reason.clone()).unwrap();

        wait_for_len(&target_direction_closed, 1).await;
        wait_for_len(&source_direction_closed, 1).await;
        wait_for_len(&target_link_closed, 1).await;
        wait_for_len(&source_link_closed, 1).await;
        assert_eq!(
            target_direction_closed
                .lock()
                .expect("direction closed mutex poisoned")
                .len(),
            1
        );
        assert_eq!(
            source_direction_closed
                .lock()
                .expect("direction closed mutex poisoned")
                .len(),
            1
        );
        assert_eq!(
            target_link_closed
                .lock()
                .expect("link closed mutex poisoned")
                .as_slice(),
            &[LinkClosed {
                link_id: link_id.clone(),
                reason: reason.clone(),
                closed_directions: [LinkDirection::SourceToTarget, LinkDirection::TargetToSource]
                    .into_iter()
                    .collect(),
                last_sequence_seen: None,
            }]
        );
        assert_eq!(
            source_link_closed
                .lock()
                .expect("link closed mutex poisoned")
                .as_slice(),
            &[LinkClosed {
                link_id,
                reason,
                closed_directions: [LinkDirection::SourceToTarget, LinkDirection::TargetToSource]
                    .into_iter()
                    .collect(),
                last_sequence_seen: None,
            }]
        );
    }
}

#[tokio::test]
async fn heartbeat_and_ack_refresh_liveness_before_idle_timeout_close() {
    let direction_closed = Arc::new(Mutex::new(Vec::new()));
    let link_closed = Arc::new(Mutex::new(Vec::new()));
    let handle = ActorRuntime::default()
        .spawn_actor(
            ClosingActor {
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
    let link_id = LinkId::new("link-heartbeat");
    let mut options = DirectLinkOptions::unidirectional();
    options.idle_timeout = Duration::from_secs(30);
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
    let router = DirectLinkInboundRouter::builder(manager)
        .bind_actor(
            input_stream.for_actor::<ClosingActor>(actor_kind!("Battle")),
            move |_| Some(handle.clone()),
        )
        .build();
    let heartbeat_at = Instant::now() + Duration::from_secs(10);

    router
        .process_frame_at(DirectLinkFrame::heartbeat(link_id.clone()), heartbeat_at)
        .unwrap();
    assert_eq!(
        router
            .close_idle_links_at(heartbeat_at + Duration::from_secs(29))
            .unwrap(),
        0
    );
    router
        .process_frame_at(
            DirectLinkFrame::heartbeat_ack(link_id.clone()),
            heartbeat_at + Duration::from_secs(29),
        )
        .unwrap();
    assert_eq!(
        router
            .close_idle_links_at(heartbeat_at + Duration::from_secs(58))
            .unwrap(),
        0
    );
    assert_eq!(
        router
            .close_idle_links_at(heartbeat_at + Duration::from_secs(59))
            .unwrap(),
        1
    );

    wait_for_len(&direction_closed, 1).await;
    wait_for_len(&link_closed, 1).await;
    assert_eq!(
        direction_closed
            .lock()
            .expect("direction closed mutex poisoned")[0]
            .reason,
        LinkCloseReason::HeartbeatTimeout
    );
    assert_eq!(
        link_closed.lock().expect("link closed mutex poisoned")[0].reason,
        LinkCloseReason::HeartbeatTimeout
    );
}
