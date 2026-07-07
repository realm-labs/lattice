use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use http::Uri;
use lattice_actor::context::ActorContext;
use lattice_actor::error::ActorError;
use lattice_actor::runtime::{ActorRuntime, ActorSpawnOptions};
use lattice_actor::traits::{Actor, Handler, Message};
use lattice_core::actor_ref::{Epoch, RequestId};
use lattice_core::id::RouteKey;
use lattice_core::instance::InstanceId;
use lattice_core::kind::ActorKind;
use lattice_core::trace::TraceContext;
use lattice_core::{actor_kind, service_kind};
use tonic::Request;
use tonic::metadata::MetadataMap;

use crate::adapter::ActorRpcAdapter;
use crate::client::{
    self, TonicEndpointChannelPool, TonicEndpointChannelPoolConfig, TypedRpcClient,
};
use crate::dedup::{RequestDedupKey, RequestDeduplicator};
use crate::error::RpcError;
use crate::metadata::{
    AuthContext, REQUEST_ID, RpcClientContextFactory, RpcContext, RpcMetadataError,
};
use crate::security::{
    PeerIdentity, RpcSecurityError, RpcSecurityPolicy, RpcServerSecurity, RpcTlsConfig,
    RpcTransportSecurity, ServiceIdentityConfig,
};
use crate::server::{RpcServerBuildError, RpcServerBuilder};
use crate::traits::{RoutedRequest, RpcRequest, ShardedRpcCore};
use crate::types::{RouteTarget, Rpc};

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
impl Actor for WorldActor {
    type Error = ActorError;
}

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
impl Actor for CountingWorldActor {
    type Error = ActorError;
}

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
        peer_identity: None,
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
fn rpc_security_policy_validates_service_identity_peer_against_metadata() {
    let ctx = RpcContext {
        request_id: RequestId::new("req-1"),
        route_epoch: None,
        source_service: service_kind!("Player"),
        source_instance: InstanceId::new("player-0"),
        trace: TraceContext::default(),
        auth: Some(AuthContext {
            authorization: "Bearer lattice-internal".to_string(),
        }),
        peer_identity: None,
    };
    let mut metadata = MetadataMap::new();
    ctx.inject_metadata(&mut metadata).unwrap();
    let extracted = RpcContext::from_metadata(&metadata).unwrap();
    let policy = RpcSecurityPolicy::require_service_identity(service_identity_config())
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
fn rpc_server_security_reads_peer_identity_from_request_extensions() {
    let security = RpcServerSecurity::new(RpcSecurityPolicy::require_service_identity(
        service_identity_config(),
    ));
    let peer = PeerIdentity::new(
        service_kind!("Player"),
        InstanceId::new("player-0"),
        "spiffe://lattice.test/ns/default/sa/player",
    );
    let mut request = Request::new(EnterWorldRequest { world_id: 9 });

    assert_eq!(security.peer_identity(&request), None);

    request.extensions_mut().insert(peer.clone());

    assert_eq!(security.peer_identity(&request), Some(peer));
}

#[test]
fn rpc_server_security_ignores_peer_identity_from_metadata() {
    let security = RpcServerSecurity::new(RpcSecurityPolicy::require_service_identity(
        service_identity_config(),
    ));
    let peer = PeerIdentity::new(
        service_kind!("Player"),
        InstanceId::new("player-0"),
        "spiffe://lattice.test/ns/default/sa/player",
    );
    let ctx = RpcClientContextFactory::new(service_kind!("Player"), InstanceId::new("player-0"))
        .with_peer_identity(peer.clone())
        .next_context(None);
    let mut request = Request::new(EnterWorldRequest { world_id: 9 });
    ctx.inject_metadata(request.metadata_mut()).unwrap();

    assert_eq!(security.peer_identity(&request), None);
}

#[test]
fn rpc_security_policy_builds_default_client_context() {
    let security = RpcServerSecurity::new(
        RpcSecurityPolicy::require_service_identity(service_identity_config())
            .require_authorization(),
    );
    let ctx = security
        .client_context_factory(service_kind!("Player"), InstanceId::new("player-0"))
        .next_context(None);
    let mut metadata = MetadataMap::new();
    ctx.inject_metadata(&mut metadata).unwrap();
    let extracted = RpcContext::from_metadata(&metadata).unwrap();
    let peer = extracted.peer_identity.as_ref().unwrap();

    assert!(extracted.auth.is_some());
    assert_eq!(peer.service_kind, service_kind!("Player"));
    assert_eq!(peer.instance_id, InstanceId::new("player-0"));
    assert!(peer.spiffe_id.starts_with("spiffe://lattice.test/"));
    assert_eq!(security.policy().validate(&extracted, Some(peer)), Ok(()));
}

#[test]
fn rpc_transport_security_plaintext_does_not_configure_tls() {
    let endpoint: Uri = "http://world-0.world:18080".parse().unwrap();

    assert!(
        RpcTransportSecurity::plaintext()
            .client_tls_config(&endpoint)
            .unwrap()
            .is_none()
    );
    assert!(
        RpcTransportSecurity::plaintext()
            .server_tls_config()
            .unwrap()
            .is_none()
    );
}

#[test]
fn rpc_transport_security_builds_client_tls_from_endpoint_host() {
    let endpoint: Uri = "https://world-0.world:18080".parse().unwrap();
    let security = RpcTransportSecurity::tls(RpcTlsConfig::new());

    assert!(security.client_tls_config(&endpoint).unwrap().is_some());
}

#[test]
fn rpc_transport_security_server_tls_requires_identity() {
    let security = RpcTransportSecurity::tls(RpcTlsConfig::new());

    assert_eq!(
        security.server_tls_config().unwrap_err(),
        "server TLS requires certificate/key identity"
    );
}

#[test]
fn tonic_channel_pool_config_defaults_to_four_stripes() {
    assert_eq!(
        TonicEndpointChannelPoolConfig::default()
            .channels_per_endpoint()
            .get(),
        4
    );
}

#[test]
fn tonic_channel_pool_config_rejects_zero_stripes() {
    assert!(TonicEndpointChannelPoolConfig::try_new(0).is_none());
}

#[test]
fn tonic_channel_pool_uses_stable_route_key_stripes() {
    let pool = TonicEndpointChannelPool::with_transport_config(
        RpcTransportSecurity::plaintext(),
        TonicEndpointChannelPoolConfig::try_new(8).unwrap(),
    );
    let route_key = RouteKey::U64(42);

    assert_eq!(
        pool.stripe_index_for(&route_key),
        pool.stripe_index_for(&route_key)
    );
}

#[test]
fn tonic_channel_pool_distributes_route_keys_over_stripes() {
    let pool = TonicEndpointChannelPool::with_transport_config(
        RpcTransportSecurity::plaintext(),
        TonicEndpointChannelPoolConfig::try_new(8).unwrap(),
    );
    let mut stripes = std::collections::BTreeSet::new();

    for actor_id in 1..=64 {
        stripes.insert(pool.stripe_index_for(&RouteKey::U64(actor_id)));
    }

    assert!(stripes.len() > 1);
}

#[test]
fn tonic_channel_pool_can_select_stripe_from_request_id() {
    let pool = TonicEndpointChannelPool::with_transport_config(
        RpcTransportSecurity::plaintext(),
        TonicEndpointChannelPoolConfig::try_new(4).unwrap(),
    );

    assert!(pool.stripe_index_for(&RequestId::new("req-1")) < 4);
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

#[test]
fn tonic_failed_precondition_owner_epoch_maps_to_fenced_retry() {
    let status = tonic::Status::failed_precondition("singleton route epoch mismatch");

    let error = client::tonic_status_to_rpc_error_for_request(
        status,
        EnterWorldRequest::METHOD,
        RequestId::new("req-1"),
    );

    assert_eq!(
        error,
        RpcError::Fenced {
            current_epoch: Epoch(0)
        }
    );
}

#[test]
fn tonic_unavailable_maps_to_unknown_result_with_request_id() {
    let status = tonic::Status::unavailable("connection closed before response");

    let error = client::tonic_status_to_rpc_error_for_request(
        status,
        EnterWorldRequest::METHOD,
        RequestId::new("req-2"),
    );

    match error {
        RpcError::UnknownResult {
            method,
            request_id,
            message,
        } => {
            assert_eq!(method, EnterWorldRequest::METHOD);
            assert_eq!(request_id, RequestId::new("req-2"));
            assert!(message.contains("connection closed before response"));
        }
        other => panic!("expected unknown result, got {other:?}"),
    }
}

#[test]
fn tonic_deadline_exceeded_maps_to_unknown_result() {
    let status = tonic::Status::deadline_exceeded("deadline elapsed");

    let error = client::tonic_status_to_rpc_error_for_request(
        status,
        EnterWorldRequest::METHOD,
        RequestId::new("req-3"),
    );

    assert!(matches!(
        error,
        RpcError::UnknownResult {
            method: EnterWorldRequest::METHOD,
            request_id,
            ..
        } if request_id == RequestId::new("req-3")
    ));
}

#[test]
fn duplicate_request_id_status_maps_to_unknown_result() {
    let status = tonic::Status::already_exists("duplicate request id");

    let error = client::tonic_status_to_rpc_error_for_request(
        status,
        EnterWorldRequest::METHOD,
        RequestId::new("req-4"),
    );

    assert!(matches!(
        error,
        RpcError::UnknownResult {
            method: EnterWorldRequest::METHOD,
            request_id,
            ..
        } if request_id == RequestId::new("req-4")
    ));
}

#[test]
fn request_deduplicator_rejects_duplicate_key() {
    let deduplicator = RequestDeduplicator::new();
    let key = RequestDedupKey::new(EnterWorldRequest::METHOD, &RequestId::new("req-dedup"));

    assert!(deduplicator.begin(&key));
    assert!(!deduplicator.begin(&key));
    assert!(deduplicator.contains(&key));
}

#[test]
fn request_deduplicator_ttl_expiry_allows_reuse_without_sleep() {
    let deduplicator = RequestDeduplicator::with_ttl(Duration::ZERO);
    let key = RequestDedupKey::new(EnterWorldRequest::METHOD, &RequestId::new("req-expired"));

    assert!(deduplicator.begin(&key));
    assert!(!deduplicator.contains(&key));
    assert!(deduplicator.begin(&key));
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
    let policy = RpcSecurityPolicy::require_service_identity(service_identity_config())
        .allow_service(service_kind!("World"));
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
async fn actor_rpc_adapter_rejects_duplicate_request_id_without_reentering_handler() {
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
    let duplicate_status = adapter
        .unary_dedup(duplicate, &deduplicator)
        .await
        .unwrap_err();

    assert!(first_reply.ok);
    assert_eq!(duplicate_status.code(), tonic::Code::AlreadyExists);
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
        peer_identity: None,
    }
}

fn service_identity_config() -> ServiceIdentityConfig {
    ServiceIdentityConfig {
        trust_domain: "lattice.test".to_string(),
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
