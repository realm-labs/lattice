use async_trait::async_trait;
use lattice_actor::{Actor, ActorContext, ActorError, ActorRuntime, ActorSpawnOptions, Handler};
use lattice_core::{
    ActorKind, Epoch, InstanceId, RouteKey, TraceContext, actor_kind, service_kind,
};
use lattice_rpc::client::MetadataInjectingRpcCore;
use lattice_rpc::traits::UnaryRpcTransport;
use lattice_rpc::{
    ActorRpcAdapter, RoutedRequest, Rpc, RpcClientContextFactory, RpcContext, RpcError, RpcRequest,
    ShardedRpcCore, TypedRpcClient,
};
use prost::Message as ProstMessage;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response};

#[derive(Clone, PartialEq, prost::Message)]
struct EnterWorldRequest {
    #[prost(uint64, tag = "1")]
    world_id: u64,
    #[prost(uint64, tag = "2")]
    player_id: u64,
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

struct WorldActor {
    seen_request_ids: Arc<Mutex<Vec<String>>>,
}

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
        self.seen_request_ids
            .lock()
            .unwrap()
            .push(msg.ctx.request_id.as_str().to_string());
        Ok(EnterWorldReply {
            ok: msg.req.world_id == 1 && msg.req.player_id == 1001,
        })
    }
}

#[derive(Clone)]
struct FakeTonicTransport {
    adapter: ActorRpcAdapter<WorldActor>,
    seen_metadata: Arc<Mutex<Vec<RpcContext>>>,
}

#[async_trait]
impl UnaryRpcTransport for FakeTonicTransport {
    async fn unary<Req>(&self, request: Request<Req>) -> Result<Response<Req::Reply>, RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        let metadata = request.metadata().clone();
        let injected = RpcContext::from_metadata(&metadata)
            .map_err(|error| RpcError::Business(error.to_string()))?;
        self.seen_metadata.lock().unwrap().push(injected);

        let req = request.into_inner();
        let request_bytes = req.encode_to_vec();
        let mut request = Request::new(
            EnterWorldRequest::decode(request_bytes.as_slice())
                .map_err(|error| RpcError::Business(error.to_string()))?,
        );
        *request.metadata_mut() = metadata;

        let reply = self
            .adapter
            .unary(request)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| RpcError::Business(status.to_string()))?;
        Req::Reply::decode(reply.encode_to_vec().as_slice())
            .map(Response::new)
            .map_err(|error| RpcError::Business(error.to_string()))
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

    async fn enter_world(
        &self,
        world_id: u64,
        player_id: u64,
    ) -> Result<EnterWorldReply, RpcError> {
        self.inner
            .call(EnterWorldRequest {
                world_id,
                player_id,
            })
            .await
    }
}

#[tokio::test]
async fn generated_client_round_trips_through_fake_tonic_transport() {
    let runtime = ActorRuntime::default();
    let seen_request_ids = Arc::new(Mutex::new(Vec::new()));
    let seen_metadata = Arc::new(Mutex::new(Vec::new()));
    let world = runtime
        .spawn_actor(
            WorldActor {
                seen_request_ids: seen_request_ids.clone(),
            },
            ActorSpawnOptions::default(),
        )
        .await
        .unwrap();
    let transport = FakeTonicTransport {
        adapter: ActorRpcAdapter::new(world).with_owner_epoch(Epoch(3)),
        seen_metadata: seen_metadata.clone(),
    };
    let context_factory =
        RpcClientContextFactory::new(service_kind!("Player"), InstanceId::new("player-0"))
            .with_trace(TraceContext {
                traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00".into()),
                tracestate: None,
            });
    let core = MetadataInjectingRpcCore::new(transport, context_factory).with_route_epoch(Epoch(3));
    let client = WorldClient::new(core);

    let reply = client.enter_world(1, 1001).await.unwrap();

    assert!(reply.ok);
    let metadata = seen_metadata.lock().unwrap();
    assert_eq!(metadata.len(), 1);
    assert_eq!(metadata[0].route_epoch, Some(Epoch(3)));
    assert_eq!(metadata[0].source_service, service_kind!("Player"));
    assert_eq!(metadata[0].source_instance, InstanceId::new("player-0"));
    assert!(metadata[0].trace.traceparent.is_some());
    assert_eq!(
        *seen_request_ids.lock().unwrap(),
        vec![metadata[0].request_id.as_str().to_string()]
    );
}
