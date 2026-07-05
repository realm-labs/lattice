use std::convert::Infallible;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use http::{Request, Response};
use lattice_actor::registry::ActorCreateContext;
use lattice_actor::{Actor, ActorContext, ActorError, ActorFactory, Handler, Message};
use lattice_config::{ConfigFormat, ConfigSource};
use lattice_core::{
    ActorId, ActorKind, ConfiguredComponent, Epoch, InstanceId, RouteKey, actor_kind, service_kind,
};
use lattice_eventbus::LocalEventBus;
use lattice_placement::cache::RouteCacheConfig;
use lattice_placement::control::{LogicControlClient, actor_id_to_proto, proto};
use lattice_placement::coordinator::{
    ExplicitRouteResolver, NoopLogicControl, PlacementCoordinator,
};
use lattice_placement::instance::{InstanceRecord, InstanceState};
use lattice_placement::store::{
    ActorPlacementKey, ActorPlacementRecord, InMemoryPlacementStore, LeaseId, PlacementPrefix,
    PlacementState, PlacementStore,
};
use lattice_placement::{
    BoxRouteResolver, EndpointLease, EndpointPool, EndpointRpcTransport, ResolveRequest,
    ResolvingRpcCore, RouteResolver,
};
use lattice_rpc::{RoutedRequest, RpcClientContextFactory, RpcError, RpcRequest, ShardedRpcCore};
use tokio::net::TcpListener;
use tokio::time::timeout;
use tonic::body::Body;
use tonic::codegen::Service;
use tonic::server::NamedService;

use crate::actor::ErasedActorRegistration;
use crate::context::ServiceBuildContext;
use crate::{
    ActorRegistration, LatticeService, LatticeServiceError, RpcClientBinding, RpcServiceBinding,
    ServiceContextExt,
};

#[derive(Clone)]
struct TestFactory;

struct TestActor;

#[async_trait]
impl Actor for TestActor {
    type Error = ActorError;
}

struct OtherActor;

#[async_trait]
impl Actor for OtherActor {
    type Error = ActorError;
}

#[async_trait]
impl ActorFactory<TestActor> for TestFactory {
    async fn create(&self, _ctx: ActorCreateContext) -> Result<TestActor, ActorError> {
        Ok(TestActor)
    }
}

#[derive(Clone)]
struct FailOnceFactory {
    attempts: Arc<AtomicUsize>,
}

#[async_trait]
impl ActorFactory<TestActor> for FailOnceFactory {
    async fn create(&self, _ctx: ActorCreateContext) -> Result<TestActor, ActorError> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(ActorError::new("first activation fails"));
        }
        Ok(TestActor)
    }
}

#[derive(Debug)]
struct TestMessage;

impl Message for TestMessage {
    type Reply = ();
}

#[async_trait]
impl Handler<TestMessage> for TestActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: TestMessage,
    ) -> Result<(), ActorError> {
        Ok(())
    }
}

struct FakeRpcBinding<A> {
    actor_kind: ActorKind,
    service_name: &'static str,
    _actor: PhantomData<fn() -> A>,
}

#[derive(Clone)]
struct EmptyRpcService;

impl NamedService for EmptyRpcService {
    const NAME: &'static str = "test.EmptyRpc";
}

impl Service<Request<Body>> for EmptyRpcService {
    type Response = Response<Body>;
    type Error = Infallible;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: Request<Body>) -> Self::Future {
        std::future::ready(Ok(Response::new(Body::empty())))
    }
}

impl<A> FakeRpcBinding<A> {
    fn new(actor_kind: ActorKind, service_name: &'static str) -> Self {
        Self {
            actor_kind,
            service_name,
            _actor: PhantomData,
        }
    }
}

impl<A> RpcServiceBinding for FakeRpcBinding<A>
where
    A: Actor + Sync,
{
    fn service_name(&self) -> &'static str {
        self.service_name
    }

    fn register(
        self: Box<Self>,
        context: &mut ServiceBuildContext,
    ) -> Result<(), LatticeServiceError> {
        let _ = context.actor::<A>(&self.actor_kind)?;
        context.add_rpc_service(EmptyRpcService);
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct FakeRpcCore;

#[async_trait]
impl ShardedRpcCore for FakeRpcCore {
    async fn call<Req>(&self, _req: Req) -> Result<Req::Reply, RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        Err(RpcError::Business(
            "fake core is only used for client construction".to_string(),
        ))
    }
}

#[derive(Debug, Clone)]
struct FakeRpcClient {
    service_kind: &'static str,
    core: FakeRpcCore,
}

struct FakeRpcClientBinding;

impl RpcClientBinding for FakeRpcClientBinding {
    type Core = FakeRpcCore;
    type Client = FakeRpcClient;

    const SERVICE_KIND: &'static str = "World";

    fn build_client(core: Self::Core) -> Self::Client {
        FakeRpcClient {
            service_kind: Self::SERVICE_KIND,
            core,
        }
    }
}

#[derive(Debug, Clone)]
struct FakeEndpointTransport;

#[async_trait]
impl EndpointRpcTransport for FakeEndpointTransport {
    async fn unary<Req>(
        &self,
        _endpoint: EndpointLease,
        _target: lattice_rpc::RouteTarget,
        _metadata: tonic::metadata::MetadataMap,
        _request: &Req,
    ) -> Result<tonic::Response<Req::Reply>, RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        Err(RpcError::Business(
            "fake endpoint transport is not used by service build tests".to_string(),
        ))
    }
}

type FakePlacementCore = ResolvingRpcCore<BoxRouteResolver, FakeEndpointTransport>;

#[derive(Debug, Clone)]
struct FakePlacementClient {
    core: FakePlacementCore,
}

struct FakePlacementClientBinding;

impl RpcClientBinding for FakePlacementClientBinding {
    type Core = FakePlacementCore;
    type Client = FakePlacementClient;

    const SERVICE_KIND: &'static str = "World";

    fn build_client(core: Self::Core) -> Self::Client {
        FakePlacementClient { core }
    }

    fn build_default_core(
        resolver: BoxRouteResolver,
        context_factory: RpcClientContextFactory,
    ) -> Option<Self::Core> {
        Some(ResolvingRpcCore::new(
            service_kind!("World"),
            resolver,
            EndpointPool::new(),
            context_factory,
            FakeEndpointTransport,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
struct ExampleOptions {
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExampleComponent {
    value: String,
}

#[derive(Clone)]
struct ContextRecordingFactory {
    observed_instance: Arc<tokio::sync::Mutex<Option<InstanceId>>>,
}

#[async_trait]
impl ActorFactory<TestActor> for ContextRecordingFactory {
    async fn create(&self, ctx: ActorCreateContext) -> Result<TestActor, ActorError> {
        *self.observed_instance.lock().await = Some(ctx.service.instance_id().clone());
        Ok(TestActor)
    }
}

#[derive(Debug)]
struct ReadServiceContext;

impl Message for ReadServiceContext {
    type Reply = InstanceId;
}

#[async_trait]
impl Handler<ReadServiceContext> for TestActor {
    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        _msg: ReadServiceContext,
    ) -> Result<InstanceId, ActorError> {
        Ok(ctx.service().instance_id().clone())
    }
}

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
async fn registered_factory_activates_actor_once_and_can_retry_failures() {
    let registration = ActorRegistration::builder(actor_kind!("World"))
        .factory(TestFactory)
        .build();
    let context_service =
        lattice_core::ServiceContext::new(service_kind!("World"), InstanceId::new("world-1"));
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
    let context_service =
        lattice_core::ServiceContext::new(service_kind!("World"), InstanceId::new("world-1"));
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
    while states.len() < 4 {
        let event = timeout(Duration::from_secs(1), watch.next())
            .await
            .unwrap()
            .unwrap();
        if let lattice_placement::store::PlacementWatchEvent::InstanceUpdated { record } = event {
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

#[tokio::test]
async fn service_keeps_instance_lease_alive_while_running() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test"));
    let store_for_service = store.clone();
    let mut watch = store.watch(store.prefix().clone()).await.unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
        .ready_signal(ready_tx)
        .instance_lease_keepalive_interval(Duration::from_millis(10))
        .placement_store::<InMemoryPlacementStore, _>(store_for_service)
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

    let lease_id = loop {
        let event = timeout(Duration::from_secs(1), watch.next())
            .await
            .unwrap()
            .unwrap();
        if let lattice_placement::store::PlacementWatchEvent::InstanceUpdated { record } = event
            && record.state == InstanceState::Ready
        {
            break record.lease_id;
        }
    };
    timeout(Duration::from_secs(1), async {
        loop {
            if store.instance_lease_keepalive_count(lease_id).unwrap_or(0) >= 2 {
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

fn placement_actor_key(actor_id: u64) -> ActorPlacementKey {
    ActorPlacementKey {
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
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(actor_id),
        owner: InstanceId::new(owner),
        epoch: Epoch(epoch),
        lease_id: LeaseId(lease_id),
        state: PlacementState::Running,
    }
}
