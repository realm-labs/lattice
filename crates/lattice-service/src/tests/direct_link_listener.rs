use super::*;

#[tokio::test]
async fn direct_link_listener_publishes_endpoint_and_stops_with_service() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test-direct-link"));
    let mut watch = store.watch(store.prefix().clone()).await.unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
        .ready_signal(ready_tx)
        .direct_links(DirectLinkConfig::enabled("127.0.0.1:0"))
        .placement_store::<InMemoryPlacementStore, _>(store)
        .register_actor(
            ActorRegistration::builder(actor_kind!("World"))
                .factory(TestFactory)
                .build(),
        )
        .register_sharded_rpc(FakeRpcBinding::<TestActor>::new(
            actor_kind!("World"),
            "WorldRpc",
        ))
        .build()
        .await
        .unwrap();
    let task = tokio::spawn(service.run_until_shutdown_signal(async {
        let _ = shutdown_rx.await;
    }));
    ready_rx.await.unwrap();

    let ready_record = loop {
        let event = timeout(Duration::from_secs(1), watch.next())
            .await
            .unwrap()
            .unwrap();
        if let lattice_placement::store::PlacementWatchEvent::InstanceUpdated { record } = event
            && record.state == InstanceState::Ready
        {
            break record;
        }
    };
    let endpoint = ready_record
        .labels
        .get("direct_link_endpoint")
        .expect("direct-link endpoint label");
    let endpoint: http::Uri = endpoint.parse().unwrap();
    let socket = endpoint.authority().unwrap().as_str();
    let _stream = TcpStream::connect(socket).await.unwrap();

    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn direct_link_listener_routes_message_frames_to_registered_actor() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store =
        InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test-direct-link-route"));
    let mut watch = store.watch(store.prefix().clone()).await.unwrap();
    let received = Arc::new(Mutex::new(Vec::new()));
    let stream = DirectLinkStream::new("movement").message::<DirectLinkTestPayload>();
    let descriptor = stream.descriptor();
    let link_id = LinkId::new("service-link-inbound");
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
        .ready_signal(ready_tx)
        .direct_links(DirectLinkConfig::enabled("127.0.0.1:0"))
        .placement_store::<InMemoryPlacementStore, _>(store)
        .register_actor(
            ActorRegistration::builder(actor_kind!("World"))
                .factory(DirectLinkTestFactory {
                    received: received.clone(),
                })
                .build(),
        )
        .register_direct_link(stream.for_actor::<DirectLinkTestActor>(actor_kind!("World")))
        .register_sharded_rpc(FakeRpcBinding::<DirectLinkTestActor>::new(
            actor_kind!("World"),
            "WorldRpc",
        ))
        .build()
        .await
        .unwrap();
    let direct_link_runtime = service.direct_link_runtime().unwrap();

    let task = tokio::spawn(service.run_until_shutdown_signal(async {
        let _ = shutdown_rx.await;
    }));
    let addr = ready_rx.await.unwrap();
    let mut client = LogicControlClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    client
        .activate_actor(proto::ActivateActorRequest {
            service_kind: "World".to_string(),
            actor_kind: "World".to_string(),
            actor_id: Some(actor_id_to_proto(&ActorId::U64(7))),
            epoch: 1,
        })
        .await
        .unwrap();

    let ready_record = loop {
        let event = timeout(Duration::from_secs(1), watch.next())
            .await
            .unwrap()
            .unwrap();
        if let lattice_placement::store::PlacementWatchEvent::InstanceUpdated { record } = event
            && record.state == InstanceState::Ready
        {
            break record;
        }
    };
    let direct_link_endpoint: http::Uri = ready_record
        .labels
        .get("direct_link_endpoint")
        .expect("direct-link endpoint label")
        .parse()
        .unwrap();
    let target_ref = direct_actor_ref(
        service_kind!("World"),
        actor_kind!("World"),
        ActorId::U64(7),
        direct_link_endpoint.clone(),
    );
    direct_link_runtime
        .session_manager()
        .open_link(OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: direct_actor_ref(
                service_kind!("Gateway"),
                actor_kind!("GatewaySession"),
                ActorId::U64(99),
                "tcp://127.0.0.1:1".parse().unwrap(),
            ),
            target: target_ref,
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &descriptor),
            target_to_source: None,
            options: DirectLinkOptions::default(),
        })
        .unwrap();
    let mut connection = TcpDirectLinkTransport::new()
        .connect_physical(DirectLinkEndpoint::new(direct_link_endpoint), 4096)
        .await
        .unwrap();
    connection
        .write_frame(DirectLinkFrame::message(
            link_id,
            LinkSequence(1),
            descriptor
                .message_id_for::<DirectLinkTestPayload>()
                .unwrap(),
            DirectLinkTestPayload { tick: 42 }.encode_to_vec(),
        ))
        .await
        .unwrap();
    connection.close().await.unwrap();

    timeout(Duration::from_secs(1), async {
        loop {
            if !received
                .lock()
                .expect("received direct-link payloads mutex poisoned")
                .is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        *received
            .lock()
            .expect("received direct-link payloads mutex poisoned"),
        vec![42]
    );

    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn direct_link_listener_demultiplexes_multiple_links_on_one_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store =
        InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test-direct-link-multiplex"));
    let mut watch = store.watch(store.prefix().clone()).await.unwrap();
    let received = Arc::new(Mutex::new(Vec::new()));
    let stream = DirectLinkStream::new("movement").message::<DirectLinkTestPayload>();
    let descriptor = stream.descriptor();
    let link_a = LinkId::new("service-link-mux-a");
    let link_b = LinkId::new("service-link-mux-b");
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
        .ready_signal(ready_tx)
        .direct_links(DirectLinkConfig::enabled("127.0.0.1:0"))
        .placement_store::<InMemoryPlacementStore, _>(store)
        .register_actor(
            ActorRegistration::builder(actor_kind!("World"))
                .factory(DirectLinkTestFactory {
                    received: received.clone(),
                })
                .build(),
        )
        .register_direct_link(stream.for_actor::<DirectLinkTestActor>(actor_kind!("World")))
        .register_sharded_rpc(FakeRpcBinding::<DirectLinkTestActor>::new(
            actor_kind!("World"),
            "WorldRpc",
        ))
        .build()
        .await
        .unwrap();

    let task = tokio::spawn(service.run_until_shutdown_signal(async {
        let _ = shutdown_rx.await;
    }));
    let addr = ready_rx.await.unwrap();
    let mut client = LogicControlClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    client
        .activate_actor(proto::ActivateActorRequest {
            service_kind: "World".to_string(),
            actor_kind: "World".to_string(),
            actor_id: Some(actor_id_to_proto(&ActorId::U64(7))),
            epoch: 1,
        })
        .await
        .unwrap();

    let ready_record = loop {
        let event = timeout(Duration::from_secs(1), watch.next())
            .await
            .unwrap()
            .unwrap();
        if let lattice_placement::store::PlacementWatchEvent::InstanceUpdated { record } = event
            && record.state == InstanceState::Ready
        {
            break record;
        }
    };
    let direct_link_endpoint: http::Uri = ready_record
        .labels
        .get("direct_link_endpoint")
        .expect("direct-link endpoint label")
        .parse()
        .unwrap();
    let target_ref = direct_actor_ref(
        service_kind!("World"),
        actor_kind!("World"),
        ActorId::U64(7),
        direct_link_endpoint.clone(),
    );
    let source_ref = direct_actor_ref(
        service_kind!("Gateway"),
        actor_kind!("GatewaySession"),
        ActorId::U64(99),
        "tcp://127.0.0.1:1".parse().unwrap(),
    );
    let mut connection = TcpDirectLinkTransport::new()
        .connect_physical(DirectLinkEndpoint::new(direct_link_endpoint), 4096)
        .await
        .unwrap();

    for link_id in [&link_a, &link_b] {
        connection
            .write_frame(
                DirectLinkFrame::open_link(&OpenLinkRequest {
                    protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                    link_id: link_id.clone(),
                    source: source_ref.clone(),
                    target: target_ref.clone(),
                    mode: DirectLinkMode::Unidirectional,
                    source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &descriptor),
                    target_to_source: None,
                    options: DirectLinkOptions::default(),
                })
                .unwrap(),
            )
            .await
            .unwrap();
        let ack = timeout(Duration::from_secs(1), connection.read_frame())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ack.kind, DirectLinkFrameKind::OpenLinkAck);
        assert_eq!(ack.decode_open_link_ack().unwrap().link_id, *link_id);
    }

    for (link_id, tick) in [(link_a, 41), (link_b, 42)] {
        connection
            .write_frame(DirectLinkFrame::message(
                link_id,
                LinkSequence(1),
                descriptor
                    .message_id_for::<DirectLinkTestPayload>()
                    .unwrap(),
                DirectLinkTestPayload { tick }.encode_to_vec(),
            ))
            .await
            .unwrap();
    }
    connection.close().await.unwrap();

    timeout(Duration::from_secs(1), async {
        loop {
            if received
                .lock()
                .expect("received direct-link payloads mutex poisoned")
                .len()
                == 2
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut received_ticks = received
        .lock()
        .expect("received direct-link payloads mutex poisoned")
        .clone();
    received_ticks.sort_unstable();
    assert_eq!(received_ticks, vec![41, 42]);

    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn service_context_installs_direct_link_runtime_handle_for_connect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = InMemoryPlacementStore::new(PlacementPrefix::new(
        "/lattice/test-direct-link-runtime-handle",
    ));
    let service = LatticeService::builder(service_kind!("Gateway"))
        .instance_id(InstanceId::new("gateway-1"))
        .listen(listener)
        .direct_links(DirectLinkConfig::enabled("127.0.0.1:0"))
        .placement_store::<InMemoryPlacementStore, _>(store.clone())
        .register_actor(
            ActorRegistration::builder(actor_kind!("GatewaySession"))
                .factory(TestFactory)
                .build(),
        )
        .register_sharded_rpc(FakeRpcBinding::<TestActor>::new(
            actor_kind!("GatewaySession"),
            "GatewayRpc",
        ))
        .build()
        .await
        .unwrap();
    assert!(
        service
            .context()
            .extension::<DirectLinkRuntimeHandle>()
            .is_some()
    );

    let transport = TcpDirectLinkTransport::new();
    let listener = transport
        .bind(DirectLinkListenConfig {
            endpoint: DirectLinkEndpoint::new("tcp://127.0.0.1:0".parse().unwrap()),
            max_frame_size: 4096,
        })
        .await
        .unwrap();
    let target_endpoint = listener.local_endpoint();
    let mut labels = std::collections::BTreeMap::new();
    labels.insert(
        "direct_link_endpoint".to_string(),
        target_endpoint.uri.to_string(),
    );
    store
        .upsert_instance(InstanceRecord {
            service_kind: service_kind!("World"),
            instance_id: InstanceId::new("world-1"),
            lease_id: LeaseId(1),
            advertised_endpoint: "http://127.0.0.1:18080".parse().unwrap(),
            control_endpoint: "http://127.0.0.1:18081".parse().unwrap(),
            version: "test".to_string(),
            state: InstanceState::Ready,
            capacity: lattice_core::instance::InstanceCapacity::default(),
            labels,
        })
        .await
        .unwrap();
    store
        .compare_and_put_actor(
            ActorPlacementKey {
                service_kind: service_kind!("World"),
                actor_kind: actor_kind!("World"),
                actor_id: ActorId::U64(7),
            },
            None,
            ActorPlacementRecord {
                service_kind: service_kind!("World"),
                actor_kind: actor_kind!("World"),
                actor_id: ActorId::U64(7),
                owner: InstanceId::new("world-1"),
                epoch: Epoch(3),
                lease_id: LeaseId(2),
                state: PlacementState::Running,
            },
        )
        .await
        .unwrap();
    let stream = DirectLinkStream::new("movement").message::<DirectLinkTestPayload>();
    let descriptor = stream.descriptor();
    let server = tokio::spawn({
        let descriptor = descriptor.clone();
        async move {
            let mut connection = listener.accept().await.unwrap();
            let open = connection
                .read_frame()
                .await
                .unwrap()
                .decode_open_link()
                .unwrap();
            assert!(matches!(
                open.target.target,
                lattice_core::actor_ref::ActorRefTarget::Direct {
                    owner_epoch: Some(Epoch(3)),
                    ..
                }
            ));
            let ack = OpenLinkAck {
                link_id: open.link_id.clone(),
                source_to_target: NegotiatedDirection {
                    direction: LinkDirection::SourceToTarget,
                    stream_name: open.source_to_target.stream_name,
                    accepted_message_type_ids: descriptor.accepted_message_ids(),
                    next_receive_sequence: LinkSequence(1),
                    backpressure: open.options.backpressure,
                    closed: false,
                },
                target_to_source: None,
            };
            connection
                .write_frame(DirectLinkFrame::open_link_ack(&ack).unwrap())
                .await
                .unwrap();
            connection.read_frame().await.unwrap()
        }
    });

    let source = direct_actor_ref(
        service_kind!("Gateway"),
        actor_kind!("GatewaySession"),
        ActorId::U64(99),
        "http://127.0.0.1:18080".parse().unwrap(),
    );
    let target = ActorRef::routed(
        service_kind!("World"),
        actor_kind!("World"),
        ActorId::U64(7),
    );
    let manager = DirectLinkManager::new(service.context().clone(), Some(source));
    let link = manager
        .connect(target, stream, DirectLinkOptions::default())
        .await
        .unwrap();
    link.tell(DirectLinkTestPayload { tick: 7 }).await.unwrap();

    let frame = timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.kind, DirectLinkFrameKind::Message);
    assert_eq!(
        frame.message_id,
        descriptor.message_id_for::<DirectLinkTestPayload>()
    );
}

#[tokio::test]
async fn direct_link_connection_writes_open_link_reject_frames() {
    let transport = TcpDirectLinkTransport::new();
    let listener = transport
        .bind(DirectLinkListenConfig {
            endpoint: DirectLinkEndpoint::new("tcp://127.0.0.1:0".parse().unwrap()),
            max_frame_size: 4096,
        })
        .await
        .unwrap();
    let endpoint = listener.local_endpoint();
    let manager = Arc::new(DirectLinkSessionManager::new());
    manager.set_validation_policy(
        OpenLinkValidationPolicy::hosted(service_kind!("World"))
            .authorize_sources([service_kind!("Gateway")])
            .require_peer_identity("lattice.test"),
    );
    let router = Arc::new(DirectLinkInboundRouter::builder(manager).build());
    let server = tokio::spawn(async move {
        let connection = listener.accept().await.unwrap();
        crate::service::handle_direct_link_connection(
            connection,
            Some(router),
            Duration::from_secs(1),
        )
        .await;
    });

    let link_id = LinkId::new("service-link-open-reject");
    let mut connection = TcpDirectLinkTransport::new()
        .connect_physical(endpoint, 4096)
        .await
        .unwrap();
    connection
        .write_frame(
            DirectLinkFrame::open_link_with_peer_identity(
                &OpenLinkRequest {
                    protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                    link_id: link_id.clone(),
                    source: direct_actor_ref(
                        service_kind!("Gateway"),
                        actor_kind!("GatewaySession"),
                        ActorId::U64(99),
                        "tcp://127.0.0.1:1".parse().unwrap(),
                    ),
                    target: direct_actor_ref(
                        service_kind!("World"),
                        actor_kind!("World"),
                        ActorId::U64(7),
                        "tcp://127.0.0.1:2".parse().unwrap(),
                    ),
                    mode: DirectLinkMode::Unidirectional,
                    source_to_target: OpenLinkDirection {
                        link_id: link_id.clone(),
                        stream_name: "unregistered".to_string(),
                        supported_message_type_ids: [DirectLinkMessageId(1)].into_iter().collect(),
                    },
                    target_to_source: None,
                    options: DirectLinkOptions::default(),
                },
                DirectLinkPeerIdentity::new(
                    service_kind!("Gateway"),
                    InstanceId::new("direct-link-test"),
                    "spiffe://lattice.test/svc/gateway/instance/direct-link-test",
                ),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let response = timeout(Duration::from_secs(1), connection.read_frame())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.kind, DirectLinkFrameKind::OpenLinkReject);
    let reject = response.decode_open_link_reject().unwrap();
    assert_eq!(reject.link_id, link_id);
    assert_eq!(reject.reason, OpenLinkRejectReason::ActorUnavailable);

    connection.close().await.unwrap();
    timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn direct_link_connection_allows_target_to_source_outbound_session() {
    let transport = TcpDirectLinkTransport::new();
    let listener = transport
        .bind(DirectLinkListenConfig {
            endpoint: DirectLinkEndpoint::new("tcp://127.0.0.1:0".parse().unwrap()),
            max_frame_size: 4096,
        })
        .await
        .unwrap();
    let endpoint = listener.local_endpoint();
    let manager = Arc::new(DirectLinkSessionManager::new());
    let stream = DirectLinkStream::new("movement").message::<DirectLinkTestPayload>();
    let descriptor = stream.descriptor();
    manager
        .register_binding(actor_kind!("World"), descriptor.clone())
        .unwrap();
    manager
        .register_binding(actor_kind!("GatewaySession"), descriptor.clone())
        .unwrap();
    manager.register_actor(actor_kind!("World"), DirectLinkActorPolicy::active(None));
    manager.register_actor(
        actor_kind!("GatewaySession"),
        DirectLinkActorPolicy::active(None),
    );

    let received = Arc::new(Mutex::new(Vec::new()));
    let actor_handle = ActorRuntime::default()
        .spawn_actor(
            DirectLinkTestActor {
                received: received.clone(),
            },
            Default::default(),
        )
        .await
        .unwrap();
    let router = Arc::new(
        DirectLinkInboundRouter::builder(manager)
            .bind_actor(
                stream.for_actor::<DirectLinkTestActor>(actor_kind!("World")),
                move |_| Some(actor_handle.clone()),
            )
            .build(),
    );
    let server = tokio::spawn({
        let router = router.clone();
        async move {
            let connection = listener.accept().await.unwrap();
            crate::service::handle_direct_link_connection(
                connection,
                Some(router),
                Duration::from_secs(60),
            )
            .await;
        }
    });

    let link_id = LinkId::new("service-link-target-to-source");
    let mut connection = TcpDirectLinkTransport::new()
        .connect_physical(endpoint, 4096)
        .await
        .unwrap();
    connection
        .write_frame(
            DirectLinkFrame::open_link(&OpenLinkRequest {
                protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                link_id: link_id.clone(),
                source: direct_actor_ref(
                    service_kind!("Gateway"),
                    actor_kind!("GatewaySession"),
                    ActorId::U64(99),
                    "tcp://127.0.0.1:1".parse().unwrap(),
                ),
                target: direct_actor_ref(
                    service_kind!("World"),
                    actor_kind!("World"),
                    ActorId::U64(7),
                    "tcp://127.0.0.1:2".parse().unwrap(),
                ),
                mode: DirectLinkMode::Bidirectional,
                source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &descriptor),
                target_to_source: Some(OpenLinkDirection::from_stream(
                    link_id.clone(),
                    &descriptor,
                )),
                options: DirectLinkOptions::bidirectional(),
            })
            .unwrap(),
        )
        .await
        .unwrap();

    let ack = timeout(Duration::from_secs(1), connection.read_frame())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ack.kind, DirectLinkFrameKind::OpenLinkAck);

    let session = router
        .outbound_session(link_id.clone(), descriptor.clone())
        .unwrap();
    session
        .sender
        .tell(OutboundDirectLinkMessage {
            link_id: link_id.clone(),
            direction: LinkDirection::TargetToSource,
            message_id: descriptor
                .message_id_for::<DirectLinkTestPayload>()
                .unwrap(),
            proto_full_name: DirectLinkTestPayload::PROTO_FULL_NAME,
            metadata: Vec::new(),
            payload: DirectLinkTestPayload { tick: 55 }.encode_to_vec(),
            flags: LinkMessageFlags::EMPTY,
        })
        .await
        .unwrap();

    let outbound = timeout(Duration::from_secs(1), connection.read_frame())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outbound.kind, DirectLinkFrameKind::Message);
    assert_eq!(outbound.link_id, link_id);
    assert_eq!(outbound.direction(), LinkDirection::TargetToSource);
    let payload = DirectLinkTestPayload::decode(outbound.payload.as_slice()).unwrap();
    assert_eq!(payload.tick, 55);

    connection.close().await.unwrap();
    timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}
