use super::*;

#[tokio::test]
async fn direct_link_listener_enforces_connection_limit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store =
        InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test-direct-link-conn-limit"));
    let mut watch = store.watch(store.prefix().clone()).await.unwrap();
    let received = Arc::new(Mutex::new(Vec::new()));
    let stream = DirectLinkStream::new("movement").message::<DirectLinkTestPayload>();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
        .ready_signal(ready_tx)
        .direct_links(DirectLinkConfig::enabled("127.0.0.1:0").max_connections(1))
        .placement_store::<InMemoryPlacementStore, _>(store)
        .register_actor(
            ActorRegistration::builder(actor_kind!("World"))
                .factory(DirectLinkTestFactory {
                    received: received.clone(),
                })
                .build(),
        )
        .register_direct_link(stream.for_actor::<DirectLinkTestActor>(actor_kind!("World")))
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
    let direct_link_endpoint: http::Uri = ready_record
        .labels
        .get("direct_link_endpoint")
        .expect("direct-link endpoint label")
        .parse()
        .unwrap();
    let transport = TcpDirectLinkTransport::new();
    let mut first = transport
        .connect_physical(DirectLinkEndpoint::new(direct_link_endpoint.clone()), 4096)
        .await
        .unwrap();
    let mut second = transport
        .connect_physical(DirectLinkEndpoint::new(direct_link_endpoint), 4096)
        .await
        .unwrap();

    let rejected = timeout(Duration::from_secs(1), second.read_frame())
        .await
        .unwrap();
    assert!(
        rejected.is_err(),
        "second direct-link connection stayed open"
    );

    first.close().await.unwrap();
    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn direct_link_config_applies_active_link_limit_to_session_manager() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = InMemoryPlacementStore::new(PlacementPrefix::new(
        "/lattice/test-direct-link-active-limit",
    ));
    let received = Arc::new(Mutex::new(Vec::new()));
    let stream = DirectLinkStream::new("movement").message::<DirectLinkTestPayload>();
    let descriptor = stream.descriptor();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
        .direct_links(DirectLinkConfig::enabled("127.0.0.1:0").max_active_links(1))
        .placement_store::<InMemoryPlacementStore, _>(store)
        .register_actor(
            ActorRegistration::builder(actor_kind!("World"))
                .factory(DirectLinkTestFactory {
                    received: received.clone(),
                })
                .build(),
        )
        .register_direct_link(stream.for_actor::<DirectLinkTestActor>(actor_kind!("World")))
        .build()
        .await
        .unwrap();
    let direct_link_runtime = service.direct_link_runtime().unwrap();
    let target_ref = direct_actor_ref(
        service_kind!("World"),
        actor_kind!("World"),
        ActorId::U64(7),
        "tcp://127.0.0.1:2".parse().unwrap(),
    );
    let open_request = |link_id: LinkId| OpenLinkRequest {
        protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
        link_id: link_id.clone(),
        source: direct_actor_ref(
            service_kind!("Gateway"),
            actor_kind!("GatewaySession"),
            ActorId::U64(99),
            "tcp://127.0.0.1:1".parse().unwrap(),
        ),
        target: target_ref.clone(),
        mode: DirectLinkMode::Unidirectional,
        source_to_target: OpenLinkDirection::from_stream(link_id, &descriptor),
        target_to_source: None,
        options: DirectLinkOptions::default(),
    };

    direct_link_runtime
        .session_manager()
        .open_link(open_request(LinkId::new("service-link-active-1")))
        .unwrap();
    let reject = direct_link_runtime
        .session_manager()
        .open_link(open_request(LinkId::new("service-link-active-2")))
        .unwrap_err();
    assert_eq!(reject.reason, OpenLinkRejectReason::Overloaded);
}

#[tokio::test]
async fn direct_link_runtime_rejects_open_links_for_other_service_kind() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store =
        InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test-direct-link-hosted"));
    let stream = DirectLinkStream::new("movement").message::<DirectLinkTestPayload>();
    let descriptor = stream.descriptor();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
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
    let link_id = LinkId::new("service-link-wrong-owner");
    let reject = service
        .direct_link_runtime()
        .unwrap()
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
            target: direct_actor_ref(
                service_kind!("Inventory"),
                actor_kind!("Inventory"),
                ActorId::U64(7),
                "tcp://127.0.0.1:2".parse().unwrap(),
            ),
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id, &descriptor),
            target_to_source: None,
            options: DirectLinkOptions::default(),
        })
        .unwrap_err();

    assert_eq!(reject.reason, OpenLinkRejectReason::NotOwner);
}

#[tokio::test]
async fn direct_link_config_applies_rate_limits_to_session_manager() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store =
        InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test-direct-link-rate-limit"));
    let received = Arc::new(Mutex::new(Vec::new()));
    let stream = DirectLinkStream::new("movement").message::<DirectLinkTestPayload>();
    let descriptor = stream.descriptor();
    let message_id = descriptor
        .message_id_for::<DirectLinkTestPayload>()
        .unwrap();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
        .direct_links(
            DirectLinkConfig::enabled("127.0.0.1:0")
                .max_open_links_per_second(1)
                .max_messages_per_second(1),
        )
        .placement_store::<InMemoryPlacementStore, _>(store)
        .register_actor(
            ActorRegistration::builder(actor_kind!("World"))
                .factory(DirectLinkTestFactory {
                    received: received.clone(),
                })
                .build(),
        )
        .register_direct_link(stream.for_actor::<DirectLinkTestActor>(actor_kind!("World")))
        .build()
        .await
        .unwrap();
    let direct_link_runtime = service.direct_link_runtime().unwrap();
    let target_ref = direct_actor_ref(
        service_kind!("World"),
        actor_kind!("World"),
        ActorId::U64(7),
        "tcp://127.0.0.1:2".parse().unwrap(),
    );
    let open_request = |link_id: LinkId| OpenLinkRequest {
        protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
        link_id: link_id.clone(),
        source: direct_actor_ref(
            service_kind!("Gateway"),
            actor_kind!("GatewaySession"),
            ActorId::U64(99),
            "tcp://127.0.0.1:1".parse().unwrap(),
        ),
        target: target_ref.clone(),
        mode: DirectLinkMode::Unidirectional,
        source_to_target: OpenLinkDirection::from_stream(link_id, &descriptor),
        target_to_source: None,
        options: DirectLinkOptions::default(),
    };
    let session_manager = direct_link_runtime.session_manager();
    let link_id = LinkId::new("service-link-rate-1");
    session_manager
        .open_link(open_request(link_id.clone()))
        .unwrap();
    let reject = session_manager
        .open_link(open_request(LinkId::new("service-link-rate-2")))
        .unwrap_err();
    assert_eq!(reject.reason, OpenLinkRejectReason::Overloaded);

    session_manager
        .validate_message_frame(
            &link_id,
            LinkDirection::SourceToTarget,
            message_id,
            LinkSequence(1),
        )
        .unwrap();
    assert!(
        session_manager
            .validate_message_frame(
                &link_id,
                LinkDirection::SourceToTarget,
                message_id,
                LinkSequence(2),
            )
            .is_err()
    );
}

#[tokio::test]
async fn direct_link_listener_idle_maintenance_closes_stale_links() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test-direct-link-idle"));
    let closed = Arc::new(Mutex::new(Vec::new()));
    let stream = DirectLinkStream::new("movement").message::<DirectLinkTestPayload>();
    let descriptor = stream.descriptor();
    let link_id = LinkId::new("service-link-idle");
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
        .ready_signal(ready_tx)
        .direct_links(
            DirectLinkConfig::enabled("127.0.0.1:0")
                .maintenance_interval(Duration::from_millis(10)),
        )
        .placement_store::<InMemoryPlacementStore, _>(store)
        .register_actor(
            ActorRegistration::builder(actor_kind!("World"))
                .factory(DirectLinkLifecycleFactory {
                    closed: closed.clone(),
                })
                .build(),
        )
        .register_direct_link(stream.for_actor::<DirectLinkLifecycleActor>(actor_kind!("World")))
        .register_sharded_rpc(FakeRpcBinding::<DirectLinkLifecycleActor>::new(
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

    let mut options = DirectLinkOptions::unidirectional();
    options.idle_timeout = Duration::from_millis(5);
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
            target: direct_actor_ref(
                service_kind!("World"),
                actor_kind!("World"),
                ActorId::U64(7),
                "tcp://127.0.0.1:2".parse().unwrap(),
            ),
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id, &descriptor),
            target_to_source: None,
            options,
        })
        .unwrap();

    timeout(Duration::from_secs(1), async {
        loop {
            if closed
                .lock()
                .expect("closed reasons mutex poisoned")
                .contains(&LinkCloseReason::HeartbeatTimeout)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn direct_link_listener_writes_heartbeat_frames_for_open_links() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store =
        InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test-direct-link-heartbeat"));
    let mut watch = store.watch(store.prefix().clone()).await.unwrap();
    let closed = Arc::new(Mutex::new(Vec::new()));
    let stream = DirectLinkStream::new("movement").message::<DirectLinkTestPayload>();
    let descriptor = stream.descriptor();
    let link_id = LinkId::new("service-link-heartbeat");
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
        .ready_signal(ready_tx)
        .direct_links(
            DirectLinkConfig::enabled("127.0.0.1:0").maintenance_interval(Duration::from_millis(5)),
        )
        .placement_store::<InMemoryPlacementStore, _>(store)
        .register_actor(
            ActorRegistration::builder(actor_kind!("World"))
                .factory(DirectLinkLifecycleFactory {
                    closed: closed.clone(),
                })
                .build(),
        )
        .register_direct_link(stream.for_actor::<DirectLinkLifecycleActor>(actor_kind!("World")))
        .register_sharded_rpc(FakeRpcBinding::<DirectLinkLifecycleActor>::new(
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

    let mut options = DirectLinkOptions::unidirectional();
    options.heartbeat_interval = Duration::from_millis(10);
    options.idle_timeout = Duration::from_secs(1);
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
            target: direct_actor_ref(
                service_kind!("World"),
                actor_kind!("World"),
                ActorId::U64(7),
                "tcp://127.0.0.1:2".parse().unwrap(),
            ),
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &descriptor),
            target_to_source: None,
            options,
        })
        .unwrap();

    let mut connection = TcpDirectLinkTransport::new()
        .connect_physical(DirectLinkEndpoint::new(direct_link_endpoint), 4096)
        .await
        .unwrap();
    let frame = timeout(Duration::from_secs(1), connection.read_frame())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.kind, DirectLinkFrameKind::Heartbeat);
    assert_eq!(frame.link_id, link_id);
    connection.close().await.unwrap();

    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn service_shutdown_closes_active_direct_links_with_node_draining() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store =
        InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test-direct-link-drain"));
    let closed = Arc::new(Mutex::new(Vec::new()));
    let stream = DirectLinkStream::new("movement").message::<DirectLinkTestPayload>();
    let descriptor = stream.descriptor();
    let link_id = LinkId::new("service-link-node-drain");
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
                .factory(DirectLinkLifecycleFactory {
                    closed: closed.clone(),
                })
                .build(),
        )
        .register_direct_link(stream.for_actor::<DirectLinkLifecycleActor>(actor_kind!("World")))
        .register_sharded_rpc(FakeRpcBinding::<DirectLinkLifecycleActor>::new(
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

    let mut options = DirectLinkOptions::unidirectional();
    options.idle_timeout = Duration::from_secs(30);
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
            target: direct_actor_ref(
                service_kind!("World"),
                actor_kind!("World"),
                ActorId::U64(7),
                "tcp://127.0.0.1:2".parse().unwrap(),
            ),
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &descriptor),
            target_to_source: None,
            options,
        })
        .unwrap();

    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
    let snapshot = direct_link_runtime
        .session_manager()
        .link_snapshot(&link_id)
        .unwrap();
    assert!(snapshot.closed);
    assert_eq!(snapshot.close_reason, Some(LinkCloseReason::NodeDraining));
}

#[tokio::test]
async fn actor_idle_passivation_closes_active_direct_links_with_target_passivated() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store =
        InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test-direct-link-passivate"));
    let closed = Arc::new(Mutex::new(Vec::new()));
    let stopped = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let stream = DirectLinkStream::new("movement").message::<DirectLinkTestPayload>();
    let descriptor = stream.descriptor();
    let link_id = LinkId::new("service-link-target-passivated");
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
                .factory(AutoPassivatingDirectLinkFactory {
                    closed: closed.clone(),
                    stopped: stopped.clone(),
                })
                .build(),
        )
        .register_direct_link(
            stream.for_actor::<AutoPassivatingDirectLinkActor>(actor_kind!("World")),
        )
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
            target: direct_actor_ref(
                service_kind!("World"),
                actor_kind!("World"),
                ActorId::U64(7),
                "tcp://127.0.0.1:2".parse().unwrap(),
            ),
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &descriptor),
            target_to_source: None,
            options: DirectLinkOptions::unidirectional(),
        })
        .unwrap();

    timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = direct_link_runtime
                .session_manager()
                .link_snapshot(&link_id)
                .unwrap();
            if snapshot.close_reason == Some(LinkCloseReason::TargetPassivated) {
                break;
            }
            let stopped_reasons = stopped.lock().await.clone();
            assert!(
                stopped_reasons.is_empty(),
                "actor stopped without closing direct link: {stopped_reasons:?}, snapshot: {snapshot:?}"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
}
