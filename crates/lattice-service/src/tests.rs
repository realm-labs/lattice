// Consolidated service test module.
//
// These tests intentionally share one crate-private module because they assert
// service builder, lifecycle, readiness, lease, admin, RPC binding, and shutdown
// behavior through internal seams that are not stable public test fixtures.
// Split this module when those fixtures become public or when a subdomain can
// move to integration tests without weakening coverage of service internals.

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
use lattice_actor::context::ActorContext;
use lattice_actor::error::{ActorError, ActorStopError};
use lattice_actor::registry::{ActorCreateContext, ActorFactory};
use lattice_actor::runtime::{ActorRuntime, ShardMigrationPolicy};
use lattice_actor::traits::{Actor, Handler, Message, PassivationReason, StopReason};
use lattice_config::format::ConfigFormat;
use lattice_config::source::ConfigSource;
use lattice_core::actor_ref::{ActorRef, Epoch, RequestId};
use lattice_core::direct_link::ids::{DirectLinkMessageId, LinkId, LinkSequence};
use lattice_core::direct_link::messages::{
    LinkBackpressure, LinkClosed, LinkDirectionClosed, LinkMessageFlags, LinkOpened, Linked,
};
use lattice_core::direct_link::options::{
    DirectLinkMode, DirectLinkOptions, LinkCloseReason, LinkDirection,
};
use lattice_core::direct_link::runtime::{
    DirectLinkManager, DirectLinkRuntimeHandle, OutboundDirectLinkMessage,
};
use lattice_core::direct_link::stream::DirectLinkMessage;
use lattice_core::direct_link::target::DirectLinkEndpoint;
use lattice_core::id::{ActorId, RouteKey};
use lattice_core::instance::{InstanceId, InstanceIncarnation};
use lattice_core::kind::{ActorKind, ServiceKind};
use lattice_core::service_context::ConfiguredComponent;
use lattice_core::trace::TraceContext;
use lattice_core::{actor_kind, service_kind};
use lattice_direct_link::inbound::DirectLinkInboundRouter;
use lattice_direct_link::protocol::{DirectLinkFrame, DirectLinkFrameKind};
use lattice_direct_link::session::{
    DIRECT_LINK_PROTOCOL_VERSION, DirectLinkActorPolicy, DirectLinkPeerIdentity,
    DirectLinkSessionManager, NegotiatedDirection, OpenLinkAck, OpenLinkDirection,
    OpenLinkRejectReason, OpenLinkRequest, OpenLinkValidationPolicy,
};
use lattice_direct_link::stream::DirectLinkStream;
use lattice_direct_link::transport::{
    DirectLinkConnection, DirectLinkListenConfig, DirectLinkTransport, TcpDirectLinkTransport,
};
use lattice_eventbus::local::{EventBus, LocalEventBus};
use lattice_eventbus::types::{EventEnvelope, EventId, EventSubscription, Subject, SubjectFilter};
use lattice_ops::ops_config::AdminHttpConfig;
use lattice_placement::authority::{DevelopmentInProcessPlacementAuthority, PlacementAuthority};
use lattice_placement::control::proto::logic_control_client::LogicControlClient;
use lattice_placement::control::{TonicLogicControl, actor_id_to_proto, proto};
use lattice_placement::coordination::logic::NoopLogicControl;
use lattice_placement::endpoint::{EndpointLease, EndpointPool};
use lattice_placement::error::PlacementError;
use lattice_placement::registry::{InstanceRecord, InstanceState};
use lattice_placement::routing::cache::RouteCacheConfig;
use lattice_placement::routing::placement::{ExplicitRouteResolver, PlacementRoutingStore};
use lattice_placement::routing::resolver::{BoxRouteResolver, ResolveRequest, RouteResolver};
use lattice_placement::routing::rpc::{EndpointRpcTransport, ResolvingRpcCore};
use lattice_placement::sharding::VirtualShardMapper;
use lattice_placement::storage::memory::InMemoryPlacementStore;
use lattice_placement::storage::{
    ActorPlacementKey, ActorPlacementRecord, LeaseId, PlacementPrefix, PlacementState,
    PlacementStore, PlacementVersion, PlacementWatch, ReadOnlyPlacementStore, SingletonKey,
    SingletonPlacementRecord,
};
use lattice_rpc::client::TonicEndpointChannelPoolConfig;
use lattice_rpc::error::RpcError;
use lattice_rpc::metadata::{AuthContext, RpcClientContextFactory, RpcContext};
use lattice_rpc::security::{RpcSecurityError, RpcSecurityPolicy, ServiceIdentityConfig};
use lattice_rpc::traits::{RoutedRequest, RpcRequest, ShardedRpcCore};
use lattice_rpc::types::RouteTarget;
use prost::Message as ProstMessage;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tonic::body::Body;
use tonic::codegen::Service;
use tonic::server::NamedService;

use crate::actors::registration::ActorRegistration;
use crate::actors::registration::ErasedActorRegistration;
use crate::clients::{RpcClientBinding, RpcClientPlacement, RpcServiceBinding};
use crate::config::DirectLinkConfig;
use crate::context::ServiceBuildContext;
use crate::error::LatticeServiceError;
use crate::framework::context::ServiceContextExt;
use crate::runtime::service::LatticeService;

fn development_service_builder(
    service_kind: ServiceKind,
) -> crate::assembly::builder::LatticeServiceBuilder {
    let store = InMemoryPlacementStore::new(PlacementPrefix::new(format!(
        "/lattice/{}/service-test",
        service_kind.as_str()
    )));
    LatticeService::builder(service_kind)
        .dangerously_use_in_process_placement(store, TonicLogicControl)
}

#[derive(Clone)]
struct CountingRoutingStore {
    inner: InMemoryPlacementStore,
    watch_starts: Arc<AtomicUsize>,
}

#[async_trait]
impl PlacementRoutingStore for CountingRoutingStore {
    async fn get_routing_instance(
        &self,
        instance_id: &InstanceId,
    ) -> Result<Option<InstanceRecord>, PlacementError> {
        PlacementStore::get_instance(&self.inner, instance_id).await
    }

    async fn get_routing_actor(
        &self,
        key: &ActorPlacementKey,
    ) -> Result<Option<(PlacementVersion, ActorPlacementRecord)>, PlacementError> {
        PlacementStore::get_actor(&self.inner, key).await
    }

    async fn get_routing_singleton(
        &self,
        key: &SingletonKey,
    ) -> Result<Option<(PlacementVersion, SingletonPlacementRecord)>, PlacementError> {
        PlacementStore::get_singleton(&self.inner, key).await
    }

    async fn watch_routing(
        &self,
        _service_kind: &ServiceKind,
    ) -> Result<PlacementWatch, PlacementError> {
        self.watch_starts.fetch_add(1, Ordering::SeqCst);
        PlacementStore::watch(&self.inner, PlacementStore::prefix(&self.inner).clone()).await
    }
}

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

    fn ingress_placement(&self) -> crate::clients::RpcServicePlacement {
        crate::clients::RpcServicePlacement::StaticLocalUnfenced
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

    fn ingress_placement(&self) -> crate::clients::RpcServicePlacement {
        crate::clients::RpcServicePlacement::StaticLocalUnfenced
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

        assert_eq!(result, Err(RpcSecurityError::InvalidAuthorization));

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
        _retry_policy: lattice_placement::routing::rpc::RpcRetryPolicy,
        _transport_security: lattice_rpc::security::RpcTransportSecurity,
        _transport_config: lattice_rpc::client::TonicEndpointChannelPoolConfig,
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
        _retry_policy: lattice_placement::routing::rpc::RpcRetryPolicy,
        _transport_security: lattice_rpc::security::RpcTransportSecurity,
        transport_config: lattice_rpc::client::TonicEndpointChannelPoolConfig,
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
        _target: RouteTarget,
        _route_key: &RouteKey,
        _metadata: tonic::metadata::MetadataMap,
        _request: Req,
    ) -> Result<tonic::Response<Req::Reply>, RpcError>
    where
        Req: RpcRequest,
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
        retry_policy: lattice_placement::routing::rpc::RpcRetryPolicy,
        _transport_security: lattice_rpc::security::RpcTransportSecurity,
        _transport_config: lattice_rpc::client::TonicEndpointChannelPoolConfig,
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
        retry_policy: lattice_placement::routing::rpc::RpcRetryPolicy,
        _transport_security: lattice_rpc::security::RpcTransportSecurity,
        _transport_config: lattice_rpc::client::TonicEndpointChannelPoolConfig,
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

fn direct_actor_ref(
    service_kind: lattice_core::kind::ServiceKind,
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

fn test_service_identity_config() -> ServiceIdentityConfig {
    ServiceIdentityConfig {
        trust_domain: "lattice.test".to_string(),
    }
}

mod builder;
mod direct_link_limits;
mod direct_link_listener;
mod lifecycle_and_clients;
mod production_hardening;
