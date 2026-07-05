use crate::dedup::RequestDeduplicator;
use crate::metadata::RpcMetadataError;
use crate::security::{MtlsConfig, PeerIdentity, RpcSecurityError, RpcSecurityPolicy};
use crate::server::{RpcServerBuildError, RpcServerBuilder};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use http::Uri;
use lattice_actor::{
    Actor, ActorContext, ActorError, ActorRuntime, ActorSpawnOptions, Handler, Message,
};
use lattice_core::{
    ActorKind, Epoch, InstanceId, RequestId, RouteKey, TraceContext, actor_kind, service_kind,
};
use tonic::Request;
use tonic::metadata::MetadataMap;

use crate::metadata::REQUEST_ID;
use crate::*;

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
