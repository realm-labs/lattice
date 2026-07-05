use std::convert::Infallible;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use async_trait::async_trait;
use http::{Request, Response};
use lattice_actor::registry::ActorCreateContext;
use lattice_actor::{Actor, ActorContext, ActorError, ActorFactory, Handler, Message};
use lattice_config::{ConfigFormat, ConfigSource};
use lattice_core::{ActorId, ActorKind, ConfiguredComponent, InstanceId, actor_kind, service_kind};
use lattice_eventbus::LocalEventBus;
use tokio::net::TcpListener;
use tonic::body::Body;
use tonic::codegen::Service;
use tonic::server::NamedService;

use crate::actor::ErasedActorRegistration;
use crate::context::ServiceBuildContext;
use crate::{
    ActorRegistration, LatticeService, LatticeServiceError, RpcServiceBinding, ServiceContextExt,
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
