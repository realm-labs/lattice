use super::*;

#[tokio::test]
async fn shutdown_signal_helper_returns_on_first_trigger() {
    let (trigger_tx, trigger_rx) = tokio::sync::oneshot::channel();
    trigger_tx.send(()).unwrap();

    timeout(
        Duration::from_millis(50),
        crate::runtime::shutdown::first_shutdown_signal(
            async {
                let _ = trigger_rx.await;
            },
            pending::<()>(),
        ),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn service_shutdown_cancels_context_event_subscriptions() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test"));
    let bus = LocalEventBus::new();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
        .ready_signal(ready_tx)
        .placement_store::<InMemoryPlacementStore, _>(store)
        .cluster_event_bus::<LocalEventBus, _>(bus.clone())
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
    let core = RecordingRpcCore::default();
    let calls = core.calls.clone();
    service
        .context()
        .cluster_events()
        .subscribe_actor_mapped(
            EventSubscription::local(SubjectFilter::new("system.shutdown.*")),
            core,
            |_event| SingletonScopeRequest {
                scope: "season-1".to_string(),
            },
        )
        .await
        .unwrap();

    bus.publish(test_event("system.shutdown.before", "BeforeShutdown"))
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let task = tokio::spawn(service.run_until_shutdown_signal(async {
        let _ = shutdown_rx.await;
    }));
    ready_rx.await.unwrap();
    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();

    bus.publish(test_event("system.shutdown.after", "AfterShutdown"))
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn service_context_scheduler_stops_on_shutdown() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test"));
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
        .ready_signal(ready_tx)
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
    let ticks = Arc::new(AtomicUsize::new(0));
    let scheduled_ticks = ticks.clone();
    service
        .context()
        .scheduler()
        .interval(Duration::from_millis(5), move || {
            let scheduled_ticks = scheduled_ticks.clone();
            async move {
                scheduled_ticks.fetch_add(1, Ordering::SeqCst);
            }
        })
        .await;

    let task = tokio::spawn(service.run_until_shutdown_signal(async {
        let _ = shutdown_rx.await;
    }));
    ready_rx.await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(ticks.load(Ordering::SeqCst) > 0);

    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
    let ticks_after_shutdown = ticks.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(30)).await;

    assert_eq!(ticks.load(Ordering::SeqCst), ticks_after_shutdown);
}

#[tokio::test]
async fn service_starts_admin_http_as_managed_listener() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let admin_probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let admin_addr = admin_probe.local_addr().unwrap();
    drop(admin_probe);
    let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test"));
    let store_for_assert = store.clone();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
        .ready_signal(ready_tx)
        .placement_store::<InMemoryPlacementStore, _>(store)
        .admin_http(AdminHttpConfig {
            bind: Some(admin_addr),
            bearer_token: None,
        })
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

    let response = read_admin_http(admin_addr, "/admin/cluster/summary").await;
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("\"instance_count\":1"));

    let response = read_admin_http(admin_addr, "/admin/node/summary").await;
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("\"instance_id\":\"world-1\""));
    assert!(response.contains("\"actor_kinds\":[\"World\"]"));

    let replacement = InstanceRecord {
        service_kind: service_kind!("World"),
        instance_id: InstanceId::new("world-2"),
        lease_id: store_for_assert.grant_instance_lease().await.unwrap(),
        advertised_endpoint: "http://127.0.0.1:19002".parse().unwrap(),
        control_endpoint: "http://127.0.0.1:19002".parse().unwrap(),
        version: "test".to_string(),
        state: InstanceState::Ready,
        capacity: Default::default(),
        labels: Default::default(),
    };
    store_for_assert.upsert_instance(replacement).await.unwrap();
    let actor_key = ActorPlacementKey {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(42),
    };
    store_for_assert
        .compare_and_put_actor(
            actor_key.clone(),
            None,
            ActorPlacementRecord {
                service_kind: service_kind!("World"),
                actor_kind: actor_kind!("World"),
                actor_id: ActorId::U64(42),
                owner: InstanceId::new("world-1"),
                epoch: Epoch(1),
                lease_id: LeaseId(99),
                state: PlacementState::Running,
            },
        )
        .await
        .unwrap();

    let response = write_admin_http(admin_addr, "POST", "/admin/instances/world-1/drain", "").await;
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("\"accepted\":true"));
    let migrated = store_for_assert
        .get_actor(&actor_key)
        .await
        .unwrap()
        .unwrap()
        .1;
    assert_eq!(migrated.owner, InstanceId::new("world-2"));
    assert_eq!(migrated.epoch, Epoch(2));

    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
}

async fn read_admin_http(admin_addr: std::net::SocketAddr, path: &str) -> String {
    write_admin_http(admin_addr, "GET", path, "").await
}

async fn write_admin_http(
    admin_addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    body: &str,
) -> String {
    let mut stream = TcpStream::connect(admin_addr).await.unwrap();
    stream
        .write_all(
            format!(
                "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response
}

#[tokio::test]
async fn service_exposes_tonic_logic_control_activation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let observed_instance = Arc::new(tokio::sync::Mutex::new(None));
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-control"))
        .listen(listener)
        .ready_signal(ready_tx)
        .register_actor(
            ActorRegistration::builder(actor_kind!("World"))
                .factory(ContextRecordingFactory {
                    observed_instance: observed_instance.clone(),
                })
                .build(),
        )
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

    assert_eq!(
        *observed_instance.lock().await,
        Some(InstanceId::new("world-control"))
    );
    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn service_shutdown_drains_runtime_actor_registries() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let reasons = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-control"))
        .listen(listener)
        .ready_signal(ready_tx)
        .register_actor(
            ActorRegistration::builder(actor_kind!("World"))
                .factory(DrainRecordingFactory {
                    reasons: reasons.clone(),
                })
                .build(),
        )
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

    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();

    assert_eq!(
        *reasons.lock().await,
        vec![StopReason::Passivated(PassivationReason::Drain)]
    );
}

#[tokio::test]
async fn service_shutdown_stops_accepting_rpc_before_actor_drain_finishes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-drain-rpc"))
        .listen(listener)
        .ready_signal(ready_tx)
        .register_actor(
            ActorRegistration::builder(actor_kind!("World"))
                .factory(BlockingStopFactory {
                    entered: entered.clone(),
                    release: release.clone(),
                })
                .build(),
        )
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

    shutdown_tx.send(()).unwrap();
    entered.acquire().await.unwrap().forget();

    let mut stopped_accepting = false;
    for _ in 0..50 {
        if TcpStream::connect(addr).await.is_err() {
            stopped_accepting = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(stopped_accepting, "service kept accepting RPC during drain");

    release.add_permits(1);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn logic_control_prepares_virtual_shard_migration_from_registry_policy() {
    let blocked = prepare_virtual_shard_migration_with_policy(
        ShardMigrationPolicy::BlockRunningActors,
        Arc::new(tokio::sync::Mutex::new(Vec::new())),
    )
    .await;
    assert!(!blocked.0.eligible);
    assert_eq!(blocked.0.running_actors, 1);
    assert_eq!(blocked.0.passivated_actors, 0);

    let reasons = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let passivated = prepare_virtual_shard_migration_with_policy(
        ShardMigrationPolicy::PassivateRunningActors,
        reasons.clone(),
    )
    .await;
    assert!(passivated.0.eligible);
    assert_eq!(passivated.0.running_actors, 1);
    assert_eq!(passivated.0.passivated_actors, 1);

    for _ in 0..50 {
        if reasons
            .lock()
            .await
            .contains(&StopReason::Passivated(PassivationReason::Migrate))
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("migration passivation reason was not recorded");
}

#[tokio::test]
async fn logic_control_closes_direct_links_for_migrating_actors() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store =
        InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test-direct-link-migrate"));
    let closed = Arc::new(Mutex::new(Vec::new()));
    let stream = DirectLinkStream::new("movement").message::<DirectLinkTestPayload>();
    let descriptor = stream.descriptor();
    let link_id = LinkId::new("service-link-target-migrating");
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
                .shard_migration(ShardMigrationPolicy::PassivateRunningActors)
                .factory(DirectLinkLifecycleFactory {
                    closed: closed.clone(),
                })
                .build(),
        )
        .register_direct_link(stream.for_actor::<DirectLinkLifecycleActor>(actor_kind!("World")))
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

    let shard_id = VirtualShardMapper::new(8)
        .unwrap()
        .shard_for_route_key(&RouteKey::U64(7));
    let response = client
        .prepare_virtual_shard_migration(proto::PrepareVirtualShardMigrationRequest {
            service_kind: "World".to_string(),
            actor_kind: "World".to_string(),
            shard_id: shard_id.0,
            shard_count: 8,
            owner_epoch: 1,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(response.eligible);
    assert_eq!(response.running_actors, 1);
    assert_eq!(response.passivated_actors, 1);

    let snapshot = direct_link_runtime
        .session_manager()
        .link_snapshot(&link_id)
        .unwrap();
    assert!(snapshot.closed);
    assert_eq!(
        snapshot.close_reason,
        Some(LinkCloseReason::TargetMigrating)
    );

    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn service_shutdown_migrates_owned_placement_records() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test"));
    store
        .upsert_instance(placement_instance("world-2"))
        .await
        .unwrap();
    let actor_key = placement_actor_key(7);
    store
        .compare_and_put_actor(
            actor_key.clone(),
            None,
            placement_actor_record(7, "world-1", 1, 1),
        )
        .await
        .unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
        .ready_signal(ready_tx)
        .placement_store::<InMemoryPlacementStore, _>(store.clone())
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
    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();

    let (_version, migrated) = store.get_actor(&actor_key).await.unwrap().unwrap();
    assert_eq!(migrated.owner, InstanceId::new("world-2"));
    assert_eq!(migrated.epoch, Epoch(2));
}

#[tokio::test]
async fn service_exposes_tonic_logic_control_singleton_activation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let observed_instance = Arc::new(tokio::sync::Mutex::new(None));
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-control"))
        .listen(listener)
        .ready_signal(ready_tx)
        .register_actor(
            ActorRegistration::builder(actor_kind!("SeasonManager"))
                .factory(ContextRecordingFactory {
                    observed_instance: observed_instance.clone(),
                })
                .build(),
        )
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
        .activate_singleton(proto::ActivateSingletonRequest {
            service_kind: "World".to_string(),
            singleton_kind: "SeasonManager".to_string(),
            scope: "global".to_string(),
            epoch: 1,
        })
        .await
        .unwrap();

    assert_eq!(
        *observed_instance.lock().await,
        Some(InstanceId::new("world-control"))
    );
    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn register_client_builds_typed_client_from_context_core() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let service = LatticeService::builder(service_kind!("Player"))
        .instance_id(InstanceId::new("player-1"))
        .listen(listener)
        .extension::<FakeRpcCore, _>(FakeRpcCore)
        .register_client::<FakeRpcClientBinding>()
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

    let client = service.context().extension::<FakeRpcClient>().unwrap();
    assert_eq!(client.service_kind, "World");
    assert_eq!(std::mem::size_of_val(&client.core), 0);
}

#[tokio::test]
async fn register_client_builds_default_placement_core_from_store() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let service = LatticeService::builder(service_kind!("Player"))
        .instance_id(InstanceId::new("player-1"))
        .listen(listener)
        .register_client::<FakePlacementClientBinding>()
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

    let client = service
        .context()
        .extension::<FakePlacementClient>()
        .unwrap();
    assert_eq!(
        std::mem::size_of_val(&client.core),
        std::mem::size_of::<FakePlacementCore>()
    );
    assert_eq!(service.placement_watch_count(), 1);
}

#[tokio::test]
async fn register_client_passes_rpc_client_transport_config() {
    OBSERVED_RPC_CLIENT_STRIPES.store(0, Ordering::SeqCst);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let service = LatticeService::builder(service_kind!("Player"))
        .instance_id(InstanceId::new("player-1"))
        .listen(listener)
        .rpc_client_transport(TonicEndpointChannelPoolConfig::try_new(8).unwrap())
        .register_client::<TransportConfigProbeBinding>()
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

    assert!(
        service
            .context()
            .extension::<TransportConfigProbeClient>()
            .is_some()
    );
    assert_eq!(OBSERVED_RPC_CLIENT_STRIPES.load(Ordering::SeqCst), 8);
}

#[tokio::test]
async fn register_client_builds_default_singleton_core_from_store() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/singleton-client"));
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-a"))
        .listen(listener)
        .ready_signal(ready_tx)
        .instance_lease_keepalive_interval(Duration::from_millis(10))
        .placement_store::<InMemoryPlacementStore, _>(store.clone())
        .register_client::<FakeSingletonClientBinding>()
        .register_actor(
            ActorRegistration::builder(actor_kind!("World"))
                .factory(TestFactory)
                .build(),
        )
        .register_actor(
            ActorRegistration::builder(actor_kind!("SeasonManager"))
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

    let client = service
        .context()
        .extension::<FakePlacementClient>()
        .unwrap()
        .as_ref()
        .clone();
    let task = tokio::spawn(service.run_until_shutdown_signal(async {
        let _ = shutdown_rx.await;
    }));
    ready_rx.await.unwrap();
    let result = client
        .core
        .call(SingletonScopeRequest {
            scope: "global".to_string(),
        })
        .await;

    assert!(matches!(result, Err(RpcError::Business(_))));
    let singleton_key = SingletonKey {
        service_kind: service_kind!("World"),
        singleton_kind: actor_kind!("SeasonManager"),
        scope: "global".to_string(),
    };
    let singleton_lease_id = store
        .get_singleton(&singleton_key)
        .await
        .unwrap()
        .unwrap()
        .1
        .lease_id;
    assert!(
        store
            .get_actor(&ActorPlacementKey {
                service_kind: service_kind!("World"),
                actor_kind: actor_kind!("SeasonManager"),
                actor_id: ActorId::Str("global".to_string()),
            })
            .await
            .unwrap()
            .is_none()
    );
    timeout(Duration::from_secs(1), async {
        loop {
            if store
                .instance_lease_keepalive_count(singleton_lease_id)
                .unwrap_or(0)
                >= 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn register_client_fails_when_core_is_missing() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let result = LatticeService::builder(service_kind!("Player"))
        .instance_id(InstanceId::new("player-1"))
        .listen(listener)
        .register_client::<FakeRpcClientBinding>()
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
        .await;

    assert!(matches!(
        result,
        Err(LatticeServiceError::MissingRpcClientCore { .. })
    ));
}

#[tokio::test]
async fn duplicate_extension_type_fails_at_build() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let result = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
        .extension::<ExampleComponent, _>(ExampleComponent {
            value: "first".to_string(),
        })
        .extension::<ExampleComponent, _>(ExampleComponent {
            value: "second".to_string(),
        })
        .build()
        .await;

    assert!(matches!(
        result,
        Err(LatticeServiceError::DuplicateServiceExtension { .. })
    ));
}

#[tokio::test]
async fn framework_accessors_are_trait_based_even_with_same_concrete_type() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
        .cluster_event_bus::<LocalEventBus, _>(LocalEventBus::default())
        .local_event_bus::<LocalEventBus, _>(LocalEventBus::default())
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

    let _cluster_event_bus = service.context().cluster_event_bus();
    let _local_event_bus = service.context().local_event_bus();
    let _cluster_events = service.context().cluster_events();
    let _local_events = service.context().local_events();
    assert!(service.context().extension::<LocalEventBus>().is_none());
}

#[tokio::test]
async fn service_context_reaches_actor_factory_and_handler() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let observed_instance = Arc::new(tokio::sync::Mutex::new(None));
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-ctx"))
        .listen(listener)
        .register_actor(
            ActorRegistration::builder(actor_kind!("World"))
                .factory(ContextRecordingFactory {
                    observed_instance: observed_instance.clone(),
                })
                .build(),
        )
        .register_sharded_rpc(FakeRpcBinding::<TestActor>::new(
            actor_kind!("World"),
            "WorldRpc",
        ))
        .build()
        .await
        .unwrap();

    let mut build_context = ServiceBuildContext::new(service.context().clone());
    Box::new(
        ActorRegistration::builder(actor_kind!("World"))
            .factory(ContextRecordingFactory {
                observed_instance: observed_instance.clone(),
            })
            .build(),
    )
    .register(&mut build_context)
    .unwrap();
    let registered = build_context
        .actor::<TestActor>(&actor_kind!("World"))
        .unwrap();
    let handle = registered
        .registry()
        .get_or_load(ActorId::U64(7), registered.loader())
        .await
        .unwrap();

    let reply = handle.call(ReadServiceContext).await.unwrap();

    assert_eq!(reply, InstanceId::new("world-ctx"));
    assert_eq!(
        *observed_instance.lock().await,
        Some(InstanceId::new("world-ctx"))
    );
}

#[tokio::test]
async fn service_build_starts_registered_placement_watch_for_route_cache_refresh() {
    let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/watch"));
    store
        .upsert_instance(placement_instance("world-a"))
        .await
        .unwrap();
    store
        .upsert_instance(placement_instance("world-b"))
        .await
        .unwrap();
    let key = placement_actor_key(7);
    let first_record = placement_actor_record(7, "world-a", 1, 1);
    let version = store
        .compare_and_put_actor(key.clone(), None, first_record)
        .await
        .unwrap();
    let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);
    let resolver = ExplicitRouteResolver::new(
        service_kind!("World"),
        store.clone(),
        coordinator,
        RouteCacheConfig::default(),
    );
    let request = ResolveRequest {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        route_key: RouteKey::U64(7),
    };
    let cached = resolver.resolve(request.clone()).await.unwrap();
    assert_eq!(cached.instance_id, InstanceId::new("world-a"));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let _service = LatticeService::builder(service_kind!("Player"))
        .instance_id(InstanceId::new("player-1"))
        .listen(listener)
        .placement_watch(resolver.clone())
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

    store
        .compare_and_put_actor(
            key,
            Some(version),
            placement_actor_record(7, "world-b", 2, 2),
        )
        .await
        .unwrap();

    for _ in 0..50 {
        let refreshed = resolver.resolve(request.clone()).await.unwrap();
        if refreshed.instance_id == InstanceId::new("world-b") {
            assert_eq!(refreshed.owner_epoch, Some(Epoch(2)));
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    panic!("service-owned placement watch did not refresh route cache");
}

fn placement_instance(instance_id: &str) -> InstanceRecord {
    InstanceRecord {
        service_kind: service_kind!("World"),
        instance_id: InstanceId::new(instance_id),
        lease_id: LeaseId(1),
        advertised_endpoint: format!("http://{instance_id}.world:18080").parse().unwrap(),
        control_endpoint: format!("http://{instance_id}.world:18081").parse().unwrap(),
        version: "test".to_string(),
        state: InstanceState::Ready,
        capacity: Default::default(),
        labels: Default::default(),
    }
}

async fn prepare_virtual_shard_migration_with_policy(
    policy: ShardMigrationPolicy,
    reasons: Arc<tokio::sync::Mutex<Vec<StopReason>>>,
) -> (proto::PrepareVirtualShardMigrationReply,) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-migration"))
        .listen(listener)
        .ready_signal(ready_tx)
        .register_actor(
            ActorRegistration::builder(actor_kind!("World"))
                .shard_migration(policy)
                .factory(DrainRecordingFactory {
                    reasons: reasons.clone(),
                })
                .build(),
        )
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

    let shard_id = VirtualShardMapper::new(8)
        .unwrap()
        .shard_for_route_key(&RouteKey::U64(7));
    let response = client
        .prepare_virtual_shard_migration(proto::PrepareVirtualShardMigrationRequest {
            service_kind: "World".to_string(),
            actor_kind: "World".to_string(),
            shard_id: shard_id.0,
            shard_count: 8,
            owner_epoch: 1,
        })
        .await
        .unwrap()
        .into_inner();

    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
    (response,)
}

fn placement_actor_key(actor_id: u64) -> ActorPlacementKey {
    ActorPlacementKey {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(actor_id),
    }
}

fn placement_actor_record(
    actor_id: u64,
    owner: &str,
    epoch: u64,
    lease_id: u64,
) -> ActorPlacementRecord {
    ActorPlacementRecord {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(actor_id),
        owner: InstanceId::new(owner),
        epoch: Epoch(epoch),
        lease_id: LeaseId(lease_id),
        state: PlacementState::Running,
    }
}
