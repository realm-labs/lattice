use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use lattice_actor::{
    Actor, ActorContext, ActorError, ActorRuntime, ActorSpawnOptions, Handler, MailboxConfig,
    Message,
};
use lattice_config::ConfigSource;
use lattice_core::{
    ActorId, ActorKey, ActorKeyDecodeError, ActorKind, Epoch, InstanceId, RouteKey, ServiceKind,
    TraceContext, actor_kind, service_kind,
};
use lattice_gateway::{
    BinaryClientCodec, ClientCodec, ClientFrame, GatewayError, GatewayRouteTable,
    ProstClientMessageBinding,
};
use lattice_rpc::{
    ActorRpcAdapter, MetadataInjectingRpcCore, RouteTarget, RoutedRequest, Rpc,
    RpcClientContextFactory, RpcError, RpcRequest, RpcServerBuilder, ShardedRpcCore,
    TypedRpcClient, UnaryRpcTransport,
};
use prost::Message as ProstMessage;
use serde::Deserialize;
use tonic::{Request, Response};

pub const WORLD_SERVICE: ServiceKind = service_kind!("World");
pub const WORLD_ACTOR: ActorKind = actor_kind!("World");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WorldId(pub u64);

impl ActorKey for WorldId {
    fn to_route_key(&self) -> RouteKey {
        RouteKey::U64(self.0)
    }

    fn to_actor_id(&self) -> ActorId {
        ActorId::U64(self.0)
    }

    fn try_from_actor_id(actor_id: &ActorId) -> Result<Self, ActorKeyDecodeError> {
        match actor_id {
            ActorId::U64(value) => Ok(Self(*value)),
            _ => Err(ActorKeyDecodeError {
                reason: "expected u64 actor id for WorldId".to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PlayerId(pub u64);

#[derive(Debug, Default)]
pub struct PlayerRuntimeState {
    ticks_seen: u64,
}

pub struct WorldActor {
    pub world_id: WorldId,
    pub tick_ms: u64,
    pub players: HashMap<PlayerId, PlayerRuntimeState>,
    pub last_rpc_request_id: Option<String>,
}

#[async_trait]
impl Actor for WorldActor {
    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        let tick_ms = self.tick_ms;
        ctx.notify_interval(Duration::from_millis(tick_ms), move || WorldTick {
            delta_ms: tick_ms,
        });
        Ok(())
    }
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct EnterWorldRequest {
    #[prost(uint64, tag = "1")]
    pub world_id: u64,
    #[prost(uint64, tag = "2")]
    pub player_id: u64,
}

impl RoutedRequest for EnterWorldRequest {
    fn actor_kind(&self) -> ActorKind {
        WORLD_ACTOR
    }

    fn route_key(&self) -> RouteKey {
        RouteKey::U64(self.world_id)
    }
}

impl RpcRequest for EnterWorldRequest {
    type Reply = EnterWorldReply;
    const METHOD: &'static str = "WorldRpc/EnterWorld";
}

#[derive(Debug)]
pub struct EnterWorld {
    pub player_id: u64,
}

impl Message for EnterWorld {
    type Reply = EnterWorldReply;
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct EnterWorldReply {
    #[prost(bool, tag = "1")]
    pub ok: bool,
    #[prost(uint64, tag = "2")]
    pub player_count: u64,
}

#[derive(Debug)]
pub struct WorldTick {
    pub delta_ms: u64,
}

impl Message for WorldTick {
    type Reply = ();
}

#[derive(Debug)]
pub struct InspectWorld;

impl Message for InspectWorld {
    type Reply = WorldSnapshot;
}

#[derive(Debug, PartialEq, Eq)]
pub struct WorldSnapshot {
    pub world_id: WorldId,
    pub player_count: usize,
    pub total_ticks: u64,
    pub last_rpc_request_id: Option<String>,
}

#[async_trait]
impl Handler<EnterWorld> for WorldActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: EnterWorld,
    ) -> Result<EnterWorldReply, ActorError> {
        let player_id = PlayerId(msg.player_id);
        self.players.entry(player_id).or_default();
        Ok(EnterWorldReply {
            ok: true,
            player_count: self.players.len() as u64,
        })
    }
}

#[async_trait]
impl Handler<Rpc<EnterWorldRequest>> for WorldActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: Rpc<EnterWorldRequest>,
    ) -> Result<EnterWorldReply, ActorError> {
        self.last_rpc_request_id = Some(msg.ctx.request_id.as_str().to_string());
        if msg.req.world_id != self.world_id.0 {
            return Ok(EnterWorldReply {
                ok: false,
                player_count: self.players.len() as u64,
            });
        }

        let player_id = PlayerId(msg.req.player_id);
        self.players.entry(player_id).or_default();
        Ok(EnterWorldReply {
            ok: true,
            player_count: self.players.len() as u64,
        })
    }
}

#[async_trait]
impl Handler<WorldTick> for WorldActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: WorldTick,
    ) -> Result<(), ActorError> {
        assert_eq!(msg.delta_ms, self.tick_ms);
        for state in self.players.values_mut() {
            state.ticks_seen += 1;
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<InspectWorld> for WorldActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: InspectWorld,
    ) -> Result<WorldSnapshot, ActorError> {
        Ok(WorldSnapshot {
            world_id: self.world_id,
            player_count: self.players.len(),
            total_ticks: self.players.values().map(|state| state.ticks_seen).sum(),
            last_rpc_request_id: self.last_rpc_request_id.clone(),
        })
    }
}

#[derive(Clone)]
struct LocalWorldTransport {
    adapter: ActorRpcAdapter<WorldActor>,
}

#[async_trait]
impl UnaryRpcTransport for LocalWorldTransport {
    async fn unary<Req>(&self, request: Request<Req>) -> Result<Response<Req::Reply>, RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        let metadata = request.metadata().clone();
        let request_bytes = request.into_inner().encode_to_vec();
        let mut actor_request = Request::new(
            EnterWorldRequest::decode(request_bytes.as_slice())
                .map_err(|error| RpcError::Business(error.to_string()))?,
        );
        *actor_request.metadata_mut() = metadata;

        let reply = self
            .adapter
            .unary(actor_request)
            .await
            .map(Response::into_inner)
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

struct WorldRpcEnterWorldGatewayBinding;

impl WorldRpcEnterWorldGatewayBinding {
    fn binding() -> ProstClientMessageBinding<EnterWorldRequest> {
        ProstClientMessageBinding::new(100)
    }
}

fn register_gateway_routes(table: &mut GatewayRouteTable) -> Result<(), GatewayError> {
    table.register(WorldRpcEnterWorldGatewayBinding::binding().route_spec())?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct WorldConfig {
    tick_ms: u64,
    mailbox_capacity: usize,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config: WorldConfig =
        ConfigSource::file("examples/minimal-world/config/world-service.toml")
            .load()?
            .section("world")?;
    let runtime = ActorRuntime::default();
    let world = runtime
        .spawn_actor(
            WorldActor {
                world_id: WorldId(1),
                tick_ms: config.tick_ms,
                players: HashMap::new(),
                last_rpc_request_id: None,
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(config.mailbox_capacity),
                ..ActorSpawnOptions::default()
            },
        )
        .await?;

    let endpoint = "http://world-0.world:18080".parse()?;
    let target = RouteTarget {
        service_kind: WORLD_SERVICE,
        instance_id: InstanceId::new("world-0"),
        advertised_endpoint: endpoint,
        owner_epoch: Some(Epoch(1)),
    };
    let mut rpc_server = RpcServerBuilder::new();
    rpc_server.add_service("WorldRpc", target.clone())?;
    rpc_server.add_service("WorldAdminRpc", target)?;

    let transport = LocalWorldTransport {
        adapter: ActorRpcAdapter::new(world.clone()).with_owner_epoch(Epoch(1)),
    };
    let context_factory =
        RpcClientContextFactory::new(service_kind!("Player"), InstanceId::new("player-0"))
            .with_trace(TraceContext {
                traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00".into()),
                tracestate: None,
            });
    let core = MetadataInjectingRpcCore::new(transport, context_factory).with_route_epoch(Epoch(1));
    let client = WorldClient::new(core.clone());

    let direct_reply = world.call(EnterWorld { player_id: 1000 }).await?;
    let rpc_reply = client.enter_world(1, 1001).await?;

    let mut route_table = GatewayRouteTable::new();
    register_gateway_routes(&mut route_table)?;
    let codec = BinaryClientCodec;
    let gateway_request = EnterWorldRequest {
        world_id: 1,
        player_id: 1002,
    };
    let encoded = codec.encode(ClientFrame {
        msg_id: 100,
        payload: gateway_request.encode_to_vec(),
    })?;
    let decoded = codec.decode(&encoded)?;
    let gateway_reply_frame = WorldRpcEnterWorldGatewayBinding::binding()
        .decode_and_forward(decoded, core)
        .await?;
    let gateway_reply = EnterWorldReply::decode(gateway_reply_frame.payload.as_slice())?;

    tokio::time::sleep(Duration::from_millis(config.tick_ms * 2)).await;
    let snapshot = world.call(InspectWorld).await?;

    println!(
        "{}:{} direct_ok={} rpc_ok={} gateway_ok={} services={} routes={} players={} ticks={} last_rpc_request_id={}",
        WORLD_SERVICE.as_str(),
        WORLD_ACTOR.as_str(),
        direct_reply.ok,
        rpc_reply.ok,
        gateway_reply.ok,
        rpc_server.services().len(),
        usize::from(route_table.get(100).is_some()),
        snapshot.player_count,
        snapshot.total_ticks,
        snapshot
            .last_rpc_request_id
            .as_deref()
            .unwrap_or("<missing>")
    );
    Ok(())
}
