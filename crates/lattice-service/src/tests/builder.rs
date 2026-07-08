use super::*;

#[tokio::test]
async fn build_requires_listener() {
    let result = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .build()
        .await;

    assert!(matches!(result, Err(LatticeServiceError::MissingListener)));
}

#[tokio::test]
async fn duplicate_actor_registration_fails() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let registration = || {
        ActorRegistration::builder(actor_kind!("World"))
            .factory(TestFactory)
            .build()
    };

    let result = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
        .register_actor(registration())
        .register_actor(registration())
        .build()
        .await;

    assert!(matches!(
        result,
        Err(LatticeServiceError::DuplicateActorRegistration { .. })
    ));
}

#[tokio::test]
async fn rpc_without_matching_actor_registration_fails() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let result = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
        .register_sharded_rpc(FakeRpcBinding::<TestActor>::new(
            actor_kind!("World"),
            "WorldRpc",
        ))
        .build()
        .await;

    assert!(matches!(
        result,
        Err(LatticeServiceError::MissingActorRegistration { .. })
    ));
}

#[tokio::test]
async fn actor_type_mismatch_fails() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let result = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
        .register_actor(
            ActorRegistration::builder(actor_kind!("World"))
                .factory(TestFactory)
                .build(),
        )
        .register_sharded_rpc(FakeRpcBinding::<OtherActor>::new(
            actor_kind!("World"),
            "WorldRpc",
        ))
        .build()
        .await;

    assert!(matches!(
        result,
        Err(LatticeServiceError::ActorTypeMismatch { .. })
    ));
}

#[tokio::test]
async fn duplicate_rpc_service_registration_fails() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let result = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
        .register_actor(
            ActorRegistration::builder(actor_kind!("World"))
                .factory(TestFactory)
                .build(),
        )
        .register_sharded_rpc(FakeRpcBinding::<TestActor>::new(
            actor_kind!("World"),
            "WorldRpc",
        ))
        .register_sharded_rpc(FakeRpcBinding::<TestActor>::new(
            actor_kind!("World"),
            "WorldRpc",
        ))
        .build()
        .await;

    assert!(matches!(
        result,
        Err(LatticeServiceError::DuplicateRpcService { .. })
    ));
}

#[tokio::test]
async fn builder_propagates_rpc_security_to_service_bindings() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let _service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
        .rpc_security(
            RpcSecurityPolicy::require_service_identity(test_service_identity_config())
                .allow_service(service_kind!("Player"))
                .require_authorization(),
        )
        .register_sharded_rpc(SecurityProbeBinding)
        .register_client::<SecurityClientProbeBinding>()
        .build()
        .await
        .unwrap();
}

#[tokio::test]
async fn registered_factory_activates_actor_once_and_can_retry_failures() {
    let registration = ActorRegistration::builder(actor_kind!("World"))
        .factory(TestFactory)
        .build();
    let context_service = lattice_core::service_context::ServiceContext::new(
        service_kind!("World"),
        InstanceId::new("world-1"),
    );
    let mut context = ServiceBuildContext::new(context_service);
    Box::new(registration).register(&mut context).unwrap();
    let registered = context.actor::<TestActor>(&actor_kind!("World")).unwrap();

    let handle = registered
        .registry()
        .get_or_load(ActorId::U64(1), registered.loader())
        .await
        .unwrap();
    handle.call(TestMessage).await.unwrap();
}

#[tokio::test]
async fn factory_activation_failure_does_not_leave_zombie_actor() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let registration = ActorRegistration::builder(actor_kind!("World"))
        .factory(FailOnceFactory {
            attempts: attempts.clone(),
        })
        .build();
    let context_service = lattice_core::service_context::ServiceContext::new(
        service_kind!("World"),
        InstanceId::new("world-1"),
    );
    let mut context = ServiceBuildContext::new(context_service);
    Box::new(registration).register(&mut context).unwrap();
    let registered = context.actor::<TestActor>(&actor_kind!("World")).unwrap();
    let actor_id = ActorId::U64(1);

    let first = registered
        .registry()
        .get_or_load(actor_id.clone(), registered.loader())
        .await;
    assert!(first.is_err());
    assert!(registered.registry().get(&actor_id).await.is_none());

    let second = registered
        .registry()
        .get_or_load(actor_id, registered.loader())
        .await;
    assert!(second.is_ok());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn build_loads_config_and_stores_components_in_service_context() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
        .config(ConfigSource::inline(
            r#"{ "example": { "value": "from-config" } }"#,
            ConfigFormat::Json,
        ))
        .extension(ConfiguredComponent::from_section(
            "example",
            |options: ExampleOptions| async move {
                Ok::<_, ActorError>(ExampleComponent {
                    value: options.value,
                })
            },
        ))
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

    let component = service.context().extension::<ExampleComponent>().unwrap();
    assert_eq!(component.value, "from-config");
    let _placement_store = service.context().placement_store();
    let _cluster_event_bus = service.context().cluster_event_bus();
    let _local_event_bus = service.context().local_event_bus();
    let _cluster_events = service.context().cluster_events();
    let _local_events = service.context().local_events();
    let _scheduler = service.context().scheduler();
    let _config_store = service.context().config_store();
    assert!(service.context().extension::<LocalEventBus>().is_none());
}

#[tokio::test]
async fn service_lifecycle_writes_starting_ready_draining_stopping() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test"));
    let mut watch = store.watch(store.prefix().clone()).await.unwrap();
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

    let task = tokio::spawn(service.run_until_shutdown_signal(async {
        let _ = shutdown_rx.await;
    }));
    ready_rx.await.unwrap();
    shutdown_tx.send(()).unwrap();

    let mut states = Vec::new();
    while !states.contains(&InstanceState::Stopping) {
        let event = timeout(Duration::from_secs(1), watch.next())
            .await
            .unwrap()
            .unwrap();
        if let lattice_placement::storage::PlacementWatchEvent::InstanceUpdated { record } = event
            && states.last() != Some(&record.state)
        {
            states.push(record.state);
        }
    }
    task.await.unwrap().unwrap();

    assert_eq!(
        states,
        vec![
            InstanceState::Starting,
            InstanceState::Ready,
            InstanceState::Draining,
            InstanceState::Stopping,
        ]
    );
}
