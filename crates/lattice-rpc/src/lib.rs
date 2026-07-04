use async_trait::async_trait;
use http::Uri;
use lattice_actor::{Actor, ActorCallError, ActorHandle, Handler, Message};
use lattice_core::{ActorKind, Epoch, InstanceId, RequestId, RouteKey, ServiceKind, TraceContext};
use prost::Message as ProstMessage;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tonic::metadata::{Ascii, MetadataMap, MetadataValue};
use tonic::{Request, Response, Status};
use tracing::Instrument;

const REQUEST_ID: &str = "lattice-request-id";
const ROUTE_EPOCH: &str = "lattice-route-epoch";
const SOURCE_SERVICE: &str = "lattice-source-service";
const SOURCE_INSTANCE: &str = "lattice-source-instance";
const TRACEPARENT: &str = "traceparent";
const TRACESTATE: &str = "tracestate";
const AUTHORIZATION: &str = "authorization";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MtlsConfig {
    pub trust_domain: String,
    pub ca_cert_path: String,
    pub cert_chain_path: String,
    pub private_key_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerIdentity {
    pub service_kind: ServiceKind,
    pub instance_id: InstanceId,
    pub spiffe_id: String,
}

impl PeerIdentity {
    pub fn new(
        service_kind: ServiceKind,
        instance_id: InstanceId,
        spiffe_id: impl Into<String>,
    ) -> Self {
        Self {
            service_kind,
            instance_id,
            spiffe_id: spiffe_id.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcSecurityPolicy {
    mtls: Option<MtlsConfig>,
    allowed_services: Vec<ServiceKind>,
    require_authorization: bool,
}

impl RpcSecurityPolicy {
    pub fn disabled() -> Self {
        Self {
            mtls: None,
            allowed_services: Vec::new(),
            require_authorization: false,
        }
    }

    pub fn require_mtls(config: MtlsConfig) -> Self {
        Self {
            mtls: Some(config),
            allowed_services: Vec::new(),
            require_authorization: false,
        }
    }

    pub fn allow_service(mut self, service_kind: ServiceKind) -> Self {
        if !self.allowed_services.contains(&service_kind) {
            self.allowed_services.push(service_kind);
        }
        self
    }

    pub fn require_authorization(mut self) -> Self {
        self.require_authorization = true;
        self
    }

    pub fn validate(
        &self,
        ctx: &RpcContext,
        peer: Option<&PeerIdentity>,
    ) -> Result<(), RpcSecurityError> {
        if self.require_authorization && ctx.auth.is_none() {
            return Err(RpcSecurityError::MissingAuthorization);
        }
        if !self.allowed_services.is_empty()
            && !self
                .allowed_services
                .iter()
                .any(|allowed| allowed == &ctx.source_service)
        {
            return Err(RpcSecurityError::ServiceNotAllowed {
                service_kind: ctx.source_service.clone(),
            });
        }

        let Some(mtls) = &self.mtls else {
            return Ok(());
        };
        let peer = peer.ok_or(RpcSecurityError::MissingPeerIdentity)?;
        if peer.service_kind != ctx.source_service {
            return Err(RpcSecurityError::SourceServiceMismatch {
                metadata: ctx.source_service.clone(),
                peer: peer.service_kind.clone(),
            });
        }
        if peer.instance_id != ctx.source_instance {
            return Err(RpcSecurityError::SourceInstanceMismatch {
                metadata: ctx.source_instance.clone(),
                peer: peer.instance_id.clone(),
            });
        }
        let expected_prefix = format!("spiffe://{}/", mtls.trust_domain);
        if !peer.spiffe_id.starts_with(&expected_prefix) {
            return Err(RpcSecurityError::TrustDomainMismatch {
                expected: mtls.trust_domain.clone(),
                spiffe_id: peer.spiffe_id.clone(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rpc<T> {
    pub req: T,
    pub ctx: RpcContext,
}

impl<T> Message for Rpc<T>
where
    T: RpcRequest,
{
    type Reply = T::Reply;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcContext {
    pub request_id: RequestId,
    pub route_epoch: Option<Epoch>,
    pub source_service: ServiceKind,
    pub source_instance: InstanceId,
    pub trace: TraceContext,
    pub auth: Option<AuthContext>,
}

impl RpcContext {
    pub fn inject_metadata(&self, metadata: &mut MetadataMap) -> Result<(), RpcMetadataError> {
        insert_ascii(metadata, REQUEST_ID, self.request_id.as_str())?;
        if let Some(epoch) = self.route_epoch {
            insert_ascii(metadata, ROUTE_EPOCH, &epoch.0.to_string())?;
        }
        insert_ascii(metadata, SOURCE_SERVICE, self.source_service.as_str())?;
        insert_ascii(metadata, SOURCE_INSTANCE, self.source_instance.as_str())?;
        if let Some(traceparent) = &self.trace.traceparent {
            insert_ascii(metadata, TRACEPARENT, traceparent)?;
        }
        if let Some(tracestate) = &self.trace.tracestate {
            insert_ascii(metadata, TRACESTATE, tracestate)?;
        }
        if let Some(auth) = &self.auth {
            insert_ascii(metadata, AUTHORIZATION, &auth.authorization)?;
        }
        Ok(())
    }

    pub fn from_metadata(metadata: &MetadataMap) -> Result<Self, RpcMetadataError> {
        Ok(Self {
            request_id: RequestId::new(required_ascii(metadata, REQUEST_ID)?),
            route_epoch: optional_ascii(metadata, ROUTE_EPOCH)?
                .map(|value| {
                    value
                        .parse::<u64>()
                        .map(Epoch)
                        .map_err(|_| RpcMetadataError::InvalidU64 {
                            key: ROUTE_EPOCH,
                            value,
                        })
                })
                .transpose()?,
            source_service: ServiceKind::new(required_ascii(metadata, SOURCE_SERVICE)?),
            source_instance: InstanceId::new(required_ascii(metadata, SOURCE_INSTANCE)?),
            trace: TraceContext {
                traceparent: optional_ascii(metadata, TRACEPARENT)?,
                tracestate: optional_ascii(metadata, TRACESTATE)?,
            },
            auth: optional_ascii(metadata, AUTHORIZATION)?
                .map(|authorization| AuthContext { authorization }),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthContext {
    pub authorization: String,
}

#[derive(Debug, Clone)]
pub struct RpcClientContextFactory {
    source_service: ServiceKind,
    source_instance: InstanceId,
    trace: TraceContext,
    auth: Option<AuthContext>,
    request_seq: Arc<AtomicU64>,
}

impl RpcClientContextFactory {
    pub fn new(source_service: ServiceKind, source_instance: InstanceId) -> Self {
        Self {
            source_service,
            source_instance,
            trace: TraceContext::default(),
            auth: None,
            request_seq: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn with_trace(mut self, trace: TraceContext) -> Self {
        self.trace = trace;
        self
    }

    pub fn with_auth(mut self, auth: AuthContext) -> Self {
        self.auth = Some(auth);
        self
    }

    pub fn next_context(&self, route_epoch: Option<Epoch>) -> RpcContext {
        let seq = self.request_seq.fetch_add(1, Ordering::Relaxed);
        RpcContext {
            request_id: RequestId::new(format!(
                "{}:{}:{seq}",
                self.source_service.as_str(),
                self.source_instance.as_str()
            )),
            route_epoch,
            source_service: self.source_service.clone(),
            source_instance: self.source_instance.clone(),
            trace: self.trace.clone(),
            auth: self.auth.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteTarget {
    pub service_kind: ServiceKind,
    pub instance_id: InstanceId,
    pub advertised_endpoint: Uri,
    pub owner_epoch: Option<Epoch>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredRpcService {
    pub name: String,
    pub target: RouteTarget,
}

#[derive(Debug, Default)]
pub struct RpcServerBuilder {
    services: Vec<RegisteredRpcService>,
}

impl RpcServerBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_service(
        &mut self,
        name: impl Into<String>,
        target: RouteTarget,
    ) -> Result<(), RpcServerBuildError> {
        let name = name.into();
        if self.services.iter().any(|service| service.name == name) {
            return Err(RpcServerBuildError::DuplicateService { name });
        }
        self.services.push(RegisteredRpcService { name, target });
        Ok(())
    }

    pub fn services(&self) -> &[RegisteredRpcService] {
        &self.services
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RpcServerBuildError {
    #[error("duplicate rpc service registration {name}")]
    DuplicateService { name: String },
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RpcSecurityError {
    #[error("missing internal rpc peer identity")]
    MissingPeerIdentity,
    #[error("missing authorization context")]
    MissingAuthorization,
    #[error("source service {service_kind} is not allowed by rpc security policy")]
    ServiceNotAllowed { service_kind: ServiceKind },
    #[error("source service metadata {metadata} does not match peer identity {peer}")]
    SourceServiceMismatch {
        metadata: ServiceKind,
        peer: ServiceKind,
    },
    #[error("source instance metadata {metadata} does not match peer identity {peer}")]
    SourceInstanceMismatch {
        metadata: InstanceId,
        peer: InstanceId,
    },
    #[error("peer spiffe id {spiffe_id} is outside trust domain {expected}")]
    TrustDomainMismatch { expected: String, spiffe_id: String },
}

pub struct ActorRpcAdapter<A: Actor> {
    handle: ActorHandle<A>,
    owner_epoch: Option<Epoch>,
}

impl<A: Actor> Clone for ActorRpcAdapter<A> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            owner_epoch: self.owner_epoch,
        }
    }
}

impl<A: Actor> ActorRpcAdapter<A> {
    pub fn new(handle: ActorHandle<A>) -> Self {
        Self {
            handle,
            owner_epoch: None,
        }
    }

    pub fn with_owner_epoch(mut self, owner_epoch: Epoch) -> Self {
        self.owner_epoch = Some(owner_epoch);
        self
    }

    pub async fn unary<Req>(&self, request: Request<Req>) -> Result<Response<Req::Reply>, Status>
    where
        A: Handler<Rpc<Req>>,
        Req: RoutedRequest + RpcRequest,
    {
        let ctx = RpcContext::from_metadata(request.metadata()).map_err(metadata_status)?;
        self.validate_owner_epoch(&ctx)?;

        let req = request.into_inner();
        self.dispatch(req, ctx).await
    }

    pub async fn unary_secure<Req>(
        &self,
        request: Request<Req>,
        policy: &RpcSecurityPolicy,
        peer: Option<&PeerIdentity>,
    ) -> Result<Response<Req::Reply>, Status>
    where
        A: Handler<Rpc<Req>>,
        Req: RoutedRequest + RpcRequest,
    {
        let ctx = RpcContext::from_metadata(request.metadata()).map_err(metadata_status)?;
        self.validate_owner_epoch(&ctx)?;
        policy.validate(&ctx, peer).map_err(security_status)?;

        let req = request.into_inner();
        self.dispatch(req, ctx).await
    }

    async fn dispatch<Req>(&self, req: Req, ctx: RpcContext) -> Result<Response<Req::Reply>, Status>
    where
        A: Handler<Rpc<Req>>,
        Req: RoutedRequest + RpcRequest,
    {
        let actor_kind = req.actor_kind();
        let route_key = req.route_key();
        let span = tracing::info_span!(
            "rpc.server",
            otel.kind = "server",
            rpc.method = Req::METHOD,
            actor.kind = actor_kind.as_str(),
            route.key = ?route_key,
            request.id = ctx.request_id.as_str(),
            source.service = ctx.source_service.as_str(),
            source.instance = ctx.source_instance.as_str()
        );
        async {
            let reply = self
                .handle
                .call(Rpc { req, ctx })
                .await
                .map_err(actor_call_status)?;
            Ok(Response::new(reply))
        }
        .instrument(span)
        .await
    }

    pub async fn unary_dedup<Req>(
        &self,
        request: Request<Req>,
        deduplicator: &RequestDeduplicator,
    ) -> Result<Response<Req::Reply>, Status>
    where
        A: Handler<Rpc<Req>>,
        Req: RoutedRequest + RpcRequest,
    {
        let ctx = RpcContext::from_metadata(request.metadata()).map_err(metadata_status)?;
        self.validate_owner_epoch(&ctx)?;
        let key = RequestDedupKey::new(Req::METHOD, &ctx.request_id);
        if let Some(reply) = deduplicator.get::<Req>(&key)? {
            return Ok(Response::new(reply));
        }

        let response = self.unary(request).await?;
        deduplicator.record(&key, response.get_ref())?;
        Ok(response)
    }

    fn validate_owner_epoch(&self, ctx: &RpcContext) -> Result<(), Status> {
        if let (Some(expected), Some(actual)) = (ctx.route_epoch, self.owner_epoch)
            && expected != actual
        {
            return Err(Status::failed_precondition("route epoch mismatch"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequestDedupKey {
    method: &'static str,
    request_id: RequestId,
}

impl RequestDedupKey {
    pub fn new(method: &'static str, request_id: &RequestId) -> Self {
        Self {
            method,
            request_id: request_id.clone(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct RequestDeduplicator {
    replies: Arc<Mutex<HashMap<RequestDedupKey, Vec<u8>>>>,
}

impl RequestDeduplicator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get<Req>(&self, key: &RequestDedupKey) -> Result<Option<Req::Reply>, Status>
    where
        Req: RpcRequest,
    {
        self.replies
            .lock()
            .expect("request deduplicator mutex poisoned")
            .get(key)
            .map(|bytes| {
                Req::Reply::decode(bytes.as_slice())
                    .map_err(|error| Status::internal(error.to_string()))
            })
            .transpose()
    }

    pub fn record<Reply>(&self, key: &RequestDedupKey, reply: &Reply) -> Result<(), Status>
    where
        Reply: prost::Message,
    {
        self.replies
            .lock()
            .expect("request deduplicator mutex poisoned")
            .insert(key.clone(), reply.encode_to_vec());
        Ok(())
    }
}

pub trait RoutedRequest {
    fn actor_kind(&self) -> ActorKind;
    fn route_key(&self) -> RouteKey;
}

pub trait RpcRequest: prost::Message + Default + Send + Sync + 'static {
    type Reply: prost::Message + Default + Send + Sync + 'static;
    const METHOD: &'static str;
}

#[async_trait]
pub trait ShardedRpcCore: Clone + Send + Sync + 'static {
    async fn call<Req>(&self, req: Req) -> Result<Req::Reply, RpcError>
    where
        Req: RoutedRequest + RpcRequest;
}

#[async_trait]
pub trait UnaryRpcTransport: Clone + Send + Sync + 'static {
    async fn unary<Req>(&self, request: Request<Req>) -> Result<Response<Req::Reply>, RpcError>
    where
        Req: RoutedRequest + RpcRequest;
}

#[derive(Debug, Clone)]
pub struct MetadataInjectingRpcCore<T> {
    transport: T,
    context_factory: RpcClientContextFactory,
    route_epoch: Option<Epoch>,
}

impl<T> MetadataInjectingRpcCore<T> {
    pub fn new(transport: T, context_factory: RpcClientContextFactory) -> Self {
        Self {
            transport,
            context_factory,
            route_epoch: None,
        }
    }

    pub fn with_route_epoch(mut self, route_epoch: Epoch) -> Self {
        self.route_epoch = Some(route_epoch);
        self
    }
}

#[async_trait]
impl<T> ShardedRpcCore for MetadataInjectingRpcCore<T>
where
    T: UnaryRpcTransport,
{
    async fn call<Req>(&self, req: Req) -> Result<Req::Reply, RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        let mut request = Request::new(req);
        self.context_factory
            .next_context(self.route_epoch)
            .inject_metadata(request.metadata_mut())
            .map_err(|error| RpcError::Business(error.to_string()))?;
        self.transport
            .unary(request)
            .await
            .map(Response::into_inner)
    }
}

#[derive(Debug, Clone)]
pub struct TypedRpcClient<C> {
    core: C,
}

impl<C> TypedRpcClient<C> {
    pub fn new(core: C) -> Self {
        Self { core }
    }

    pub fn core(&self) -> &C {
        &self.core
    }
}

impl<C> TypedRpcClient<C>
where
    C: ShardedRpcCore,
{
    pub async fn call<Req>(&self, req: Req) -> Result<Req::Reply, RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        self.core.call(req).await
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RpcError {
    #[error("target owner not found")]
    NoOwner,
    #[error("route target is not owner")]
    NotOwner { expected_epoch: Option<Epoch> },
    #[error("request was fenced by newer owner")]
    Fenced { current_epoch: Epoch },
    #[error("actor is unavailable")]
    ActorUnavailable,
    #[error("mailbox is full")]
    MailboxFull,
    #[error("rpc timed out; result may be unknown")]
    TimeoutUnknown,
    #[error("business error: {0}")]
    Business(String),
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RpcMetadataError {
    #[error("missing rpc metadata key {key}")]
    Missing { key: &'static str },
    #[error("invalid rpc metadata key {key}")]
    InvalidAscii { key: &'static str },
    #[error("invalid unsigned integer in rpc metadata key {key}: {value}")]
    InvalidU64 { key: &'static str, value: String },
}

fn insert_ascii(
    metadata: &mut MetadataMap,
    key: &'static str,
    value: &str,
) -> Result<(), RpcMetadataError> {
    let value = MetadataValue::<Ascii>::try_from(value)
        .map_err(|_| RpcMetadataError::InvalidAscii { key })?;
    metadata.insert(key, value);
    Ok(())
}

fn required_ascii(metadata: &MetadataMap, key: &'static str) -> Result<String, RpcMetadataError> {
    optional_ascii(metadata, key)?.ok_or(RpcMetadataError::Missing { key })
}

fn optional_ascii(
    metadata: &MetadataMap,
    key: &'static str,
) -> Result<Option<String>, RpcMetadataError> {
    metadata
        .get(key)
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| RpcMetadataError::InvalidAscii { key })
        })
        .transpose()
}

fn metadata_status(error: RpcMetadataError) -> Status {
    Status::invalid_argument(error.to_string())
}

fn security_status(error: RpcSecurityError) -> Status {
    match error {
        RpcSecurityError::MissingPeerIdentity | RpcSecurityError::MissingAuthorization => {
            Status::unauthenticated(error.to_string())
        }
        RpcSecurityError::ServiceNotAllowed { .. }
        | RpcSecurityError::SourceServiceMismatch { .. }
        | RpcSecurityError::SourceInstanceMismatch { .. }
        | RpcSecurityError::TrustDomainMismatch { .. } => {
            Status::permission_denied(error.to_string())
        }
    }
}

fn actor_call_status(error: ActorCallError) -> Status {
    match error {
        ActorCallError::MailboxFull => Status::resource_exhausted(error.to_string()),
        ActorCallError::MailboxClosed | ActorCallError::ResponseDropped => {
            Status::unavailable(error.to_string())
        }
        ActorCallError::Handler(_) => Status::internal(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use lattice_actor::{ActorContext, ActorError, ActorRuntime, ActorSpawnOptions};
    use lattice_core::{actor_kind, service_kind};

    use super::*;

    #[derive(Clone, PartialEq, prost::Message)]
    struct EnterWorldRequest {
        #[prost(uint64, tag = "1")]
        world_id: u64,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct EnterWorldReply {
        #[prost(bool, tag = "1")]
        ok: bool,
    }

    impl RoutedRequest for EnterWorldRequest {
        fn actor_kind(&self) -> ActorKind {
            actor_kind!("World")
        }

        fn route_key(&self) -> RouteKey {
            RouteKey::U64(self.world_id)
        }
    }

    impl RpcRequest for EnterWorldRequest {
        type Reply = EnterWorldReply;
        const METHOD: &'static str = "world.WorldRpc/EnterWorld";
    }

    struct WorldActor;

    #[async_trait]
    impl Actor for WorldActor {}

    #[async_trait]
    impl Handler<Rpc<EnterWorldRequest>> for WorldActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            msg: Rpc<EnterWorldRequest>,
        ) -> Result<EnterWorldReply, ActorError> {
            assert_eq!(msg.ctx.request_id.as_str(), "req-1");
            Ok(EnterWorldReply {
                ok: msg.req.world_id == 9,
            })
        }
    }

    struct CountingWorldActor {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Actor for CountingWorldActor {}

    #[async_trait]
    impl Handler<Rpc<EnterWorldRequest>> for CountingWorldActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            msg: Rpc<EnterWorldRequest>,
        ) -> Result<EnterWorldReply, ActorError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(EnterWorldReply {
                ok: msg.req.world_id == 9,
            })
        }
    }

    #[test]
    fn rpc_context_injects_and_extracts_grpc_metadata() {
        let ctx = RpcContext {
            request_id: RequestId::new("req-1"),
            route_epoch: Some(Epoch(42)),
            source_service: service_kind!("World"),
            source_instance: InstanceId::new("world-0"),
            trace: TraceContext {
                traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00".into()),
                tracestate: Some("rojo=00f067aa0ba902b7".into()),
            },
            auth: Some(AuthContext {
                authorization: "Bearer test".into(),
            }),
        };
        let mut metadata = MetadataMap::new();

        ctx.inject_metadata(&mut metadata).unwrap();
        let extracted = RpcContext::from_metadata(&metadata).unwrap();

        assert_eq!(extracted, ctx);
    }

    #[test]
    fn rpc_context_requires_framework_metadata() {
        let error = RpcContext::from_metadata(&MetadataMap::new()).unwrap_err();

        assert_eq!(error, RpcMetadataError::Missing { key: REQUEST_ID });
    }

    #[test]
    fn rpc_security_policy_validates_mtls_peer_identity_against_metadata() {
        let ctx = RpcContext {
            request_id: RequestId::new("req-1"),
            route_epoch: None,
            source_service: service_kind!("Player"),
            source_instance: InstanceId::new("player-0"),
            trace: TraceContext::default(),
            auth: Some(AuthContext {
                authorization: "Bearer internal".to_string(),
            }),
        };
        let mut metadata = MetadataMap::new();
        ctx.inject_metadata(&mut metadata).unwrap();
        let extracted = RpcContext::from_metadata(&metadata).unwrap();
        let policy = RpcSecurityPolicy::require_mtls(mtls_config())
            .allow_service(service_kind!("Player"))
            .require_authorization();
        let peer = PeerIdentity::new(
            service_kind!("Player"),
            InstanceId::new("player-0"),
            "spiffe://lattice.test/ns/default/sa/player",
        );

        assert_eq!(policy.validate(&extracted, Some(&peer)), Ok(()));
        assert_eq!(
            policy.validate(&extracted, None),
            Err(RpcSecurityError::MissingPeerIdentity)
        );
        assert_eq!(
            policy.validate(
                &extracted,
                Some(&PeerIdentity::new(
                    service_kind!("Gateway"),
                    InstanceId::new("player-0"),
                    "spiffe://lattice.test/ns/default/sa/gateway",
                )),
            ),
            Err(RpcSecurityError::SourceServiceMismatch {
                metadata: service_kind!("Player"),
                peer: service_kind!("Gateway"),
            })
        );
    }

    #[test]
    fn routed_request_exposes_actor_kind_and_route_key() {
        let request = EnterWorldRequest { world_id: 9 };

        assert_eq!(request.actor_kind(), actor_kind!("World"));
        assert_eq!(request.route_key(), RouteKey::U64(9));
        assert_eq!(EnterWorldRequest::METHOD, "world.WorldRpc/EnterWorld");
    }

    fn assert_actor_message<M: Message>() {}

    #[test]
    fn rpc_wrapper_is_actor_message_for_rpc_request() {
        assert_actor_message::<Rpc<EnterWorldRequest>>();
    }

    #[test]
    fn client_context_factory_generates_metadata_contexts() {
        let factory = RpcClientContextFactory::new(service_kind!("Player"), InstanceId::new("p0"))
            .with_trace(TraceContext {
                traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00".into()),
                tracestate: None,
            });

        let first = factory.next_context(Some(Epoch(1)));
        let second = factory.next_context(Some(Epoch(1)));

        assert_eq!(first.source_service, service_kind!("Player"));
        assert_eq!(first.source_instance, InstanceId::new("p0"));
        assert_eq!(first.route_epoch, Some(Epoch(1)));
        assert_ne!(first.request_id, second.request_id);
        assert!(first.trace.traceparent.is_some());
    }

    #[derive(Clone, Default)]
    struct FakeRpcCore {
        methods: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait]
    impl ShardedRpcCore for FakeRpcCore {
        async fn call<Req>(&self, _req: Req) -> Result<Req::Reply, RpcError>
        where
            Req: RoutedRequest + RpcRequest,
        {
            self.methods.lock().unwrap().push(Req::METHOD);
            Ok(Req::Reply::default())
        }
    }

    struct WorldClient<C> {
        inner: TypedRpcClient<C>,
    }

    impl<C> WorldClient<C>
    where
        C: ShardedRpcCore,
    {
        fn new(core: C) -> Self {
            Self {
                inner: TypedRpcClient::new(core),
            }
        }

        async fn enter_world(&self, world_id: u64) -> Result<EnterWorldReply, RpcError> {
            self.inner.call(EnterWorldRequest { world_id }).await
        }
    }

    #[tokio::test]
    async fn generated_typed_client_wrapper_delegates_to_rpc_core() {
        let core = FakeRpcCore::default();
        let observed = core.methods.clone();
        let client = WorldClient::new(core);

        let reply = client.enter_world(5).await.unwrap();

        assert!(!reply.ok);
        assert_eq!(*observed.lock().unwrap(), vec!["world.WorldRpc/EnterWorld"]);
    }

    #[tokio::test]
    async fn actor_rpc_adapter_converts_tonic_request_into_actor_call() {
        let runtime = ActorRuntime::default();
        let handle = runtime
            .spawn_actor(WorldActor, ActorSpawnOptions::default())
            .await
            .unwrap();
        let adapter = ActorRpcAdapter::new(handle).with_owner_epoch(Epoch(7));
        let mut request = Request::new(EnterWorldRequest { world_id: 9 });
        test_context(Some(Epoch(7)))
            .inject_metadata(request.metadata_mut())
            .unwrap();

        let response = adapter.unary(request).await.unwrap().into_inner();

        assert!(response.ok);
    }

    #[tokio::test]
    async fn actor_rpc_adapter_secure_unary_rejects_mismatched_peer_identity() {
        let runtime = ActorRuntime::default();
        let handle = runtime
            .spawn_actor(WorldActor, ActorSpawnOptions::default())
            .await
            .unwrap();
        let adapter = ActorRpcAdapter::new(handle).with_owner_epoch(Epoch(7));
        let policy =
            RpcSecurityPolicy::require_mtls(mtls_config()).allow_service(service_kind!("World"));
        let peer = PeerIdentity::new(
            service_kind!("Gateway"),
            InstanceId::new("world-0"),
            "spiffe://lattice.test/ns/default/sa/gateway",
        );
        let mut request = Request::new(EnterWorldRequest { world_id: 9 });
        test_context(Some(Epoch(7)))
            .inject_metadata(request.metadata_mut())
            .unwrap();

        let status = adapter
            .unary_secure(request, &policy, Some(&peer))
            .await
            .unwrap_err();

        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn actor_rpc_adapter_rejects_stale_route_epoch_before_handler() {
        let runtime = ActorRuntime::default();
        let handle = runtime
            .spawn_actor(WorldActor, ActorSpawnOptions::default())
            .await
            .unwrap();
        let adapter = ActorRpcAdapter::new(handle).with_owner_epoch(Epoch(8));
        let mut request = Request::new(EnterWorldRequest { world_id: 9 });
        test_context(Some(Epoch(7)))
            .inject_metadata(request.metadata_mut())
            .unwrap();

        let status = adapter.unary(request).await.unwrap_err();

        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn actor_rpc_adapter_replays_duplicate_request_id_without_reentering_handler() {
        let runtime = ActorRuntime::default();
        let calls = Arc::new(AtomicUsize::new(0));
        let handle = runtime
            .spawn_actor(
                CountingWorldActor {
                    calls: calls.clone(),
                },
                ActorSpawnOptions::default(),
            )
            .await
            .unwrap();
        let adapter = ActorRpcAdapter::new(handle).with_owner_epoch(Epoch(7));
        let deduplicator = RequestDeduplicator::new();
        let mut first = Request::new(EnterWorldRequest { world_id: 9 });
        test_context(Some(Epoch(7)))
            .inject_metadata(first.metadata_mut())
            .unwrap();
        let mut duplicate = Request::new(EnterWorldRequest { world_id: 1 });
        test_context(Some(Epoch(7)))
            .inject_metadata(duplicate.metadata_mut())
            .unwrap();

        let first_reply = adapter
            .unary_dedup(first, &deduplicator)
            .await
            .unwrap()
            .into_inner();
        let duplicate_reply = adapter
            .unary_dedup(duplicate, &deduplicator)
            .await
            .unwrap()
            .into_inner();

        assert!(first_reply.ok);
        assert!(duplicate_reply.ok);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    fn test_context(route_epoch: Option<Epoch>) -> RpcContext {
        RpcContext {
            request_id: RequestId::new("req-1"),
            route_epoch,
            source_service: service_kind!("World"),
            source_instance: InstanceId::new("world-0"),
            trace: TraceContext::default(),
            auth: None,
        }
    }

    fn mtls_config() -> MtlsConfig {
        MtlsConfig {
            trust_domain: "lattice.test".to_string(),
            ca_cert_path: "/etc/lattice/ca.pem".to_string(),
            cert_chain_path: "/etc/lattice/tls.crt".to_string(),
            private_key_path: "/etc/lattice/tls.key".to_string(),
        }
    }

    #[test]
    fn rpc_server_builder_allows_multiple_services_on_one_endpoint() {
        let endpoint: Uri = "http://world-0.world:18080".parse().unwrap();
        let target = RouteTarget {
            service_kind: service_kind!("World"),
            instance_id: InstanceId::new("world-0"),
            advertised_endpoint: endpoint.clone(),
            owner_epoch: Some(Epoch(1)),
        };
        let mut builder = RpcServerBuilder::new();

        builder.add_service("WorldRpc", target.clone()).unwrap();
        builder.add_service("RoomRpc", target).unwrap();

        assert_eq!(builder.services().len(), 2);
        assert!(
            builder
                .services()
                .iter()
                .all(|service| service.target.advertised_endpoint == endpoint)
        );
    }

    #[test]
    fn rpc_server_builder_rejects_duplicate_service_names() {
        let target = RouteTarget {
            service_kind: service_kind!("World"),
            instance_id: InstanceId::new("world-0"),
            advertised_endpoint: "http://world-0.world:18080".parse().unwrap(),
            owner_epoch: None,
        };
        let mut builder = RpcServerBuilder::new();

        builder.add_service("WorldRpc", target.clone()).unwrap();
        let duplicate = builder.add_service("WorldRpc", target);

        assert_eq!(
            duplicate,
            Err(RpcServerBuildError::DuplicateService {
                name: "WorldRpc".to_string()
            })
        );
    }
}
