//! Consolidated service test module.
//!
//! These tests intentionally share one crate-private module because they assert
//! service builder, lifecycle, readiness, lease, admin, RPC binding, and shutdown
//! behavior through internal seams that are not stable public test fixtures.
//! Split this module when those fixtures become public or when a subdomain can
//! move to integration tests without weakening coverage of service internals.

use std::convert::Infallible;
use std::future::pending;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use http::{Request, Response};
use lattice_actor::registry::ActorCreateContext;
use lattice_actor::{
    Actor, ActorContext, ActorError, ActorFactory, ActorStopError, Handler, Message,
    PassivationReason, ShardMigrationPolicy, StopReason,
};
use lattice_config::{ConfigFormat, ConfigSource};
use lattice_core::{
    ActorId, ActorKind, ActorRef, ConfiguredComponent, DirectLinkEndpoint, DirectLinkMessage,
    DirectLinkMode, DirectLinkOptions, Epoch, InstanceId, LinkBackpressure, LinkCloseReason,
    LinkClosed, LinkDirectionClosed, LinkId, LinkOpened, LinkSequence, Linked, RequestId, RouteKey,
    TraceContext, actor_kind, service_kind,
};
use lattice_direct_link::{
    DIRECT_LINK_PROTOCOL_VERSION, DirectLinkConnection, DirectLinkFrame, DirectLinkFrameKind,
    DirectLinkInboundRouter, DirectLinkListenConfig, DirectLinkPeerIdentity,
    DirectLinkSessionManager, DirectLinkStream, DirectLinkTransport, OpenLinkDirection,
    OpenLinkRejectReason, OpenLinkRequest, OpenLinkValidationPolicy, TcpDirectLinkTransport,
};
use lattice_eventbus::{
    EventBus, EventEnvelope, EventId, EventSubscription, LocalEventBus, Subject, SubjectFilter,
};
use lattice_placement::cache::RouteCacheConfig;
use lattice_placement::control::{LogicControlClient, actor_id_to_proto, proto};
use lattice_placement::coordinator::{
    ExplicitRouteResolver, NoopLogicControl, PlacementCoordinator,
};
use lattice_placement::instance::{InstanceRecord, InstanceState};
use lattice_placement::store::{
    ActorPlacementKey, ActorPlacementRecord, InMemoryPlacementStore, LeaseId, PlacementPrefix,
    PlacementState, PlacementStore, SingletonKey,
};
use lattice_placement::vshard::VirtualShardMapper;
use lattice_placement::{
    BoxRouteResolver, EndpointLease, EndpointPool, EndpointRpcTransport, ResolveRequest,
    ResolvingRpcCore, RouteResolver,
};
use lattice_rpc::{
    AuthContext, RoutedRequest, RpcClientContextFactory, RpcContext, RpcError, RpcRequest,
    RpcSecurityError, RpcSecurityPolicy, ServiceIdentityConfig, ShardedRpcCore,
    TonicEndpointChannelPoolConfig,
};
use prost::Message as ProstMessage;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tonic::body::Body;
use tonic::codegen::Service;
use tonic::server::NamedService;

use crate::actor::ErasedActorRegistration;
use crate::context::ServiceBuildContext;
use crate::{
    ActorRegistration, AdminHttpConfig, DirectLinkConfig, LatticeService, LatticeServiceError,
    RpcClientBinding, RpcClientPlacement, RpcServiceBinding, ServiceContextExt,
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

#[derive(Clone, PartialEq, prost::Message)]
struct SingletonScopeRequest {
    #[prost(string, tag = "1")]
    scope: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct SingletonScopeReply {
    #[prost(bool, tag = "1")]
    ok: bool,
}

impl RoutedRequest for SingletonScopeRequest {
    fn actor_kind(&self) -> ActorKind {
        actor_kind!("SeasonManager")
    }

    fn route_key(&self) -> RouteKey {
        RouteKey::Str(self.scope.clone())
    }
}

impl RpcRequest for SingletonScopeRequest {
    type Reply = SingletonScopeReply;
    const METHOD: &'static str = "test.SeasonRpc/Tick";
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

#[derive(Clone, PartialEq, prost::Message)]
struct DirectLinkTestPayload {
    #[prost(uint64, tag = "1")]
    tick: u64,
}

impl DirectLinkMessage for DirectLinkTestPayload {
    const PROTO_FULL_NAME: &'static str = "test.DirectLinkPayload";
}

#[derive(Clone)]
struct DirectLinkTestFactory {
    received: Arc<Mutex<Vec<u64>>>,
}

struct DirectLinkTestActor {
    received: Arc<Mutex<Vec<u64>>>,
}

#[async_trait]
impl Actor for DirectLinkTestActor {
    type Error = ActorError;
}

#[async_trait]
impl ActorFactory<DirectLinkTestActor> for DirectLinkTestFactory {
    async fn create(&self, _ctx: ActorCreateContext) -> Result<DirectLinkTestActor, ActorError> {
        Ok(DirectLinkTestActor {
            received: self.received.clone(),
        })
    }
}

#[async_trait]
impl Handler<Linked<DirectLinkTestPayload>> for DirectLinkTestActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: Linked<DirectLinkTestPayload>,
    ) -> Result<(), ActorError> {
        self.received
            .lock()
            .expect("received direct-link payloads mutex poisoned")
            .push(msg.payload.tick);
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkOpened> for DirectLinkTestActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkOpened,
    ) -> Result<(), ActorError> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkDirectionClosed> for DirectLinkTestActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkDirectionClosed,
    ) -> Result<(), ActorError> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkClosed> for DirectLinkTestActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkClosed,
    ) -> Result<(), ActorError> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkBackpressure> for DirectLinkTestActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkBackpressure,
    ) -> Result<(), ActorError> {
        Ok(())
    }
}

#[derive(Clone)]
struct DirectLinkLifecycleFactory {
    closed: Arc<Mutex<Vec<LinkCloseReason>>>,
}

struct DirectLinkLifecycleActor {
    closed: Arc<Mutex<Vec<LinkCloseReason>>>,
}

#[derive(Clone)]
struct AutoPassivatingDirectLinkFactory {
    closed: Arc<Mutex<Vec<LinkCloseReason>>>,
    stopped: Arc<tokio::sync::Mutex<Vec<StopReason>>>,
}

struct AutoPassivatingDirectLinkActor {
    closed: Arc<Mutex<Vec<LinkCloseReason>>>,
    stopped: Arc<tokio::sync::Mutex<Vec<StopReason>>>,
}

struct PassivateSelf;

impl Message for PassivateSelf {
    type Reply = ();
}

#[async_trait]
impl Actor for DirectLinkLifecycleActor {
    type Error = ActorError;
}

#[async_trait]
impl Actor for AutoPassivatingDirectLinkActor {
    type Error = ActorError;

    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        ctx.notify_after(Duration::from_millis(250), PassivateSelf);
        Ok(())
    }

    async fn stopping(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        reason: StopReason,
    ) -> Result<(), ActorStopError> {
        self.stopped.lock().await.push(reason);
        Ok(())
    }
}

#[async_trait]
impl ActorFactory<DirectLinkLifecycleActor> for DirectLinkLifecycleFactory {
    async fn create(
        &self,
        _ctx: ActorCreateContext,
    ) -> Result<DirectLinkLifecycleActor, ActorError> {
        Ok(DirectLinkLifecycleActor {
            closed: self.closed.clone(),
        })
    }
}

#[async_trait]
impl ActorFactory<AutoPassivatingDirectLinkActor> for AutoPassivatingDirectLinkFactory {
    async fn create(
        &self,
        _ctx: ActorCreateContext,
    ) -> Result<AutoPassivatingDirectLinkActor, ActorError> {
        Ok(AutoPassivatingDirectLinkActor {
            closed: self.closed.clone(),
            stopped: self.stopped.clone(),
        })
    }
}

#[async_trait]
impl Handler<Linked<DirectLinkTestPayload>> for DirectLinkLifecycleActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: Linked<DirectLinkTestPayload>,
    ) -> Result<(), ActorError> {
        Ok(())
    }
}

#[async_trait]
impl Handler<Linked<DirectLinkTestPayload>> for AutoPassivatingDirectLinkActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: Linked<DirectLinkTestPayload>,
    ) -> Result<(), ActorError> {
        Ok(())
    }
}

#[async_trait]
impl Handler<PassivateSelf> for AutoPassivatingDirectLinkActor {
    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        _msg: PassivateSelf,
    ) -> Result<(), ActorError> {
        ctx.request_passivation(PassivationReason::BusinessIdle)?;
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkOpened> for DirectLinkLifecycleActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkOpened,
    ) -> Result<(), ActorError> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkOpened> for AutoPassivatingDirectLinkActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkOpened,
    ) -> Result<(), ActorError> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkDirectionClosed> for DirectLinkLifecycleActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkDirectionClosed,
    ) -> Result<(), ActorError> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkDirectionClosed> for AutoPassivatingDirectLinkActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkDirectionClosed,
    ) -> Result<(), ActorError> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkClosed> for DirectLinkLifecycleActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: LinkClosed,
    ) -> Result<(), ActorError> {
        self.closed
            .lock()
            .expect("closed reasons mutex poisoned")
            .push(msg.reason);
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkClosed> for AutoPassivatingDirectLinkActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: LinkClosed,
    ) -> Result<(), ActorError> {
        self.closed
            .lock()
            .expect("closed reasons mutex poisoned")
            .push(msg.reason);
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkBackpressure> for DirectLinkLifecycleActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkBackpressure,
    ) -> Result<(), ActorError> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkBackpressure> for AutoPassivatingDirectLinkActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkBackpressure,
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

#[derive(Debug)]
struct SecurityProbeBinding;

impl RpcServiceBinding for SecurityProbeBinding {
    fn service_name(&self) -> &'static str {
        "SecurityProbeRpc"
    }

    fn register(
        self: Box<Self>,
        context: &mut ServiceBuildContext,
    ) -> Result<(), LatticeServiceError> {
        let rpc_context = RpcContext {
            request_id: RequestId::new("req-1"),
            route_epoch: None,
            source_service: service_kind!("Player"),
            source_instance: InstanceId::new("player-1"),
            trace: TraceContext::default(),
            auth: Some(AuthContext {
                authorization: "Bearer internal".to_string(),
            }),
            peer_identity: None,
        };
        let result = context.rpc_security().policy().validate(&rpc_context, None);

        assert_eq!(result, Err(RpcSecurityError::MissingPeerIdentity));

        context.add_rpc_service(EmptyRpcService);
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct SecurityClientProbeCore;

#[async_trait]
impl ShardedRpcCore for SecurityClientProbeCore {
    async fn call<Req>(&self, _req: Req) -> Result<Req::Reply, RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        Err(RpcError::Business(
            "security probe core is not called".to_string(),
        ))
    }
}

#[derive(Debug, Clone)]
struct SecurityClientProbe;

struct SecurityClientProbeBinding;

impl RpcClientBinding for SecurityClientProbeBinding {
    type Core = SecurityClientProbeCore;
    type Client = SecurityClientProbe;

    const SERVICE_KIND: &'static str = "World";

    fn build_client(_core: Self::Core) -> Self::Client {
        SecurityClientProbe
    }

    fn build_default_core(
        _resolver: BoxRouteResolver,
        context_factory: RpcClientContextFactory,
        _retry_policy: lattice_placement::RpcRetryPolicy,
        _transport_security: lattice_rpc::RpcTransportSecurity,
        _transport_config: lattice_rpc::TonicEndpointChannelPoolConfig,
    ) -> Option<Self::Core> {
        let ctx = context_factory.next_context(None);
        assert!(ctx.auth.is_some());
        let peer = ctx.peer_identity.as_ref().expect("peer identity");
        assert_eq!(peer.service_kind, service_kind!("World"));
        assert_eq!(peer.instance_id, InstanceId::new("world-1"));
        assert!(peer.spiffe_id.starts_with("spiffe://lattice.test/"));
        Some(SecurityClientProbeCore)
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

#[derive(Debug, Clone, Default)]
struct RecordingRpcCore {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl ShardedRpcCore for RecordingRpcCore {
    async fn call<Req>(&self, _req: Req) -> Result<Req::Reply, RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Req::Reply::default())
    }
}

#[derive(Debug, Clone)]
struct FakeRpcClient {
    service_kind: &'static str,
    core: FakeRpcCore,
}

fn test_event(subject: &str, event_type: &str) -> EventEnvelope {
    EventEnvelope {
        event_id: EventId::new("event-1"),
        subject: Subject::new(subject),
        event_type: event_type.to_string(),
        source_service: service_kind!("World"),
        source_instance: InstanceId::new("world-1"),
        actor_kind: None,
        actor_id: None,
        request_id: None,
        trace: TraceContext::default(),
        occurred_unix_ms: 1,
        payload: Vec::new(),
    }
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

static OBSERVED_RPC_CLIENT_STRIPES: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone)]
struct TransportConfigProbeCore;

#[async_trait]
impl ShardedRpcCore for TransportConfigProbeCore {
    async fn call<Req>(&self, _req: Req) -> Result<Req::Reply, RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        Err(RpcError::Business(
            "transport config probe core is not called".to_string(),
        ))
    }
}

#[derive(Debug, Clone)]
struct TransportConfigProbeClient;

struct TransportConfigProbeBinding;

impl RpcClientBinding for TransportConfigProbeBinding {
    type Core = TransportConfigProbeCore;
    type Client = TransportConfigProbeClient;

    const SERVICE_KIND: &'static str = "World";

    fn build_client(_core: Self::Core) -> Self::Client {
        TransportConfigProbeClient
    }

    fn build_default_core(
        _resolver: BoxRouteResolver,
        _context_factory: RpcClientContextFactory,
        _retry_policy: lattice_placement::RpcRetryPolicy,
        _transport_security: lattice_rpc::RpcTransportSecurity,
        transport_config: lattice_rpc::TonicEndpointChannelPoolConfig,
    ) -> Option<Self::Core> {
        OBSERVED_RPC_CLIENT_STRIPES.store(
            transport_config.channels_per_endpoint().get(),
            Ordering::SeqCst,
        );
        Some(TransportConfigProbeCore)
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
        _request: Req,
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
        retry_policy: lattice_placement::RpcRetryPolicy,
        _transport_security: lattice_rpc::RpcTransportSecurity,
        _transport_config: lattice_rpc::TonicEndpointChannelPoolConfig,
    ) -> Option<Self::Core> {
        Some(
            ResolvingRpcCore::new(
                service_kind!("World"),
                resolver,
                EndpointPool::new(),
                context_factory,
                FakeEndpointTransport,
            )
            .with_retry_policy(retry_policy),
        )
    }
}

struct FakeSingletonClientBinding;

impl RpcClientBinding for FakeSingletonClientBinding {
    type Core = FakePlacementCore;
    type Client = FakePlacementClient;

    const SERVICE_KIND: &'static str = "World";

    fn placement() -> RpcClientPlacement {
        RpcClientPlacement::Singleton
    }

    fn build_client(core: Self::Core) -> Self::Client {
        FakePlacementClient { core }
    }

    fn build_default_core(
        resolver: BoxRouteResolver,
        context_factory: RpcClientContextFactory,
        retry_policy: lattice_placement::RpcRetryPolicy,
        _transport_security: lattice_rpc::RpcTransportSecurity,
        _transport_config: lattice_rpc::TonicEndpointChannelPoolConfig,
    ) -> Option<Self::Core> {
        Some(
            ResolvingRpcCore::new(
                service_kind!("World"),
                resolver,
                EndpointPool::new(),
                context_factory,
                FakeEndpointTransport,
            )
            .with_retry_policy(retry_policy),
        )
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

struct DrainRecordingActor {
    reasons: Arc<tokio::sync::Mutex<Vec<StopReason>>>,
}

#[async_trait]
impl Actor for DrainRecordingActor {
    type Error = ActorError;

    async fn stopping(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        reason: StopReason,
    ) -> Result<(), ActorStopError> {
        self.reasons.lock().await.push(reason);
        Ok(())
    }
}

#[derive(Clone)]
struct DrainRecordingFactory {
    reasons: Arc<tokio::sync::Mutex<Vec<StopReason>>>,
}

#[async_trait]
impl ActorFactory<DrainRecordingActor> for DrainRecordingFactory {
    async fn create(&self, _ctx: ActorCreateContext) -> Result<DrainRecordingActor, ActorError> {
        Ok(DrainRecordingActor {
            reasons: self.reasons.clone(),
        })
    }
}

struct BlockingStopActor {
    entered: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
}

#[async_trait]
impl Actor for BlockingStopActor {
    type Error = ActorError;

    async fn stopping(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _reason: StopReason,
    ) -> Result<(), ActorStopError> {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        Ok(())
    }
}

#[derive(Clone)]
struct BlockingStopFactory {
    entered: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
}

#[async_trait]
impl ActorFactory<BlockingStopActor> for BlockingStopFactory {
    async fn create(&self, _ctx: ActorCreateContext) -> Result<BlockingStopActor, ActorError> {
        Ok(BlockingStopActor {
            entered: self.entered.clone(),
            release: self.release.clone(),
        })
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
        if let lattice_placement::store::PlacementWatchEvent::InstanceUpdated { record } = event
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
        .connect(DirectLinkEndpoint::new(direct_link_endpoint))
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
        .connect(endpoint)
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
                        supported_message_type_ids: [lattice_core::DirectLinkMessageId(1)]
                            .into_iter()
                            .collect(),
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
        .connect(DirectLinkEndpoint::new(direct_link_endpoint.clone()))
        .await
        .unwrap();
    let mut second = transport
        .connect(DirectLinkEndpoint::new(direct_link_endpoint))
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
            lattice_core::LinkDirection::SourceToTarget,
            message_id,
            LinkSequence(1),
        )
        .unwrap();
    assert!(
        session_manager
            .validate_message_frame(
                &link_id,
                lattice_core::LinkDirection::SourceToTarget,
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
        .connect(DirectLinkEndpoint::new(direct_link_endpoint))
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

fn direct_actor_ref(
    service_kind: lattice_core::ServiceKind,
    actor_kind: ActorKind,
    actor_id: ActorId,
    endpoint: http::Uri,
) -> ActorRef {
    ActorRef::direct(
        service_kind,
        actor_kind,
        actor_id,
        InstanceId::new("direct-link-test"),
        endpoint,
        None,
    )
}

#[tokio::test]
async fn shutdown_signal_helper_returns_on_first_trigger() {
    let (trigger_tx, trigger_rx) = tokio::sync::oneshot::channel();
    trigger_tx.send(()).unwrap();

    timeout(
        Duration::from_millis(50),
        crate::service::first_shutdown_signal(
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
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(42),
    };
    store_for_assert
        .compare_and_put_actor(
            actor_key.clone(),
            None,
            ActorPlacementRecord {
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

fn test_service_identity_config() -> ServiceIdentityConfig {
    ServiceIdentityConfig {
        trust_domain: "lattice.test".to_string(),
    }
}
