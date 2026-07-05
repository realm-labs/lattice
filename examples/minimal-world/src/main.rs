use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use lattice_actor::{
    Actor, ActorContext, ActorError, ActorRuntime, ActorSpawnOptions, Handler, MailboxConfig,
    Message,
};
use lattice_config::{ConfigSource, ConfigStore, LocalConfigStore};
use lattice_core::{
    ActorId, ActorKey, ActorKeyDecodeError, ActorKind, Epoch, InstanceId, RouteKey, ServiceKind,
    TraceContext, actor_kind, service_kind,
};
use lattice_eventbus::{
    EventBus, EventEnvelope, EventPublisher, EventSubscription, LocalEventBus, Subject,
    SubjectFilter,
};
use lattice_gateway::{BinaryClientCodec, ClientCodec, ClientFrame, GatewayRouteTable};
use lattice_ops::ServiceScheduler;
use lattice_placement::cache::RouteCacheConfig;
use lattice_placement::static_resolver::{
    StaticPlacementConfig, StaticRouteRange, StaticRouteResolver,
};
use lattice_placement::{EndpointLease, EndpointPool, EndpointRpcTransport, ResolvingRpcCore};
use lattice_rpc::server::RpcServerBuilder;
use lattice_rpc::{
    ActorRpcAdapter, RouteTarget, RoutedRequest, Rpc, RpcClientContextFactory, RpcError, RpcRequest,
};
use prost::Message as ProstMessage;
use serde::Deserialize;
use serde_json::json;
use tonic::{Request, Response};

pub mod world {
    tonic::include_proto!("world");
}

pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/lattice.generated.rs"));
}

use generated::{register_gateway_routes, world_rpc::Client as WorldClient};
use world::{EnterWorldReply, EnterWorldRequest};

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

#[derive(Debug)]
pub struct EnterWorld {
    pub player_id: u64,
}

impl Message for EnterWorld {
    type Reply = EnterWorldReply;
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
    adapters: HashMap<InstanceId, ActorRpcAdapter<WorldActor>>,
}

#[async_trait]
impl EndpointRpcTransport for LocalWorldTransport {
    async fn unary<Req>(
        &self,
        _endpoint: EndpointLease,
        target: RouteTarget,
        request: Request<Req>,
    ) -> Result<Response<Req::Reply>, RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        let adapter = self
            .adapters
            .get(&target.instance_id)
            .ok_or_else(|| RpcError::Business("missing local adapter".to_string()))?;
        let metadata = request.metadata().clone();
        let request_bytes = request.into_inner().encode_to_vec();
        let mut actor_request = Request::new(
            EnterWorldRequest::decode(request_bytes.as_slice())
                .map_err(|error| RpcError::Business(error.to_string()))?,
        );
        *actor_request.metadata_mut() = metadata;

        let reply = adapter
            .unary(actor_request)
            .await
            .map(Response::into_inner)
            .map_err(|status| RpcError::Business(status.to_string()))?;
        Req::Reply::decode(reply.encode_to_vec().as_slice())
            .map(Response::new)
            .map_err(|error| RpcError::Business(error.to_string()))
    }
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
    let world_a = runtime
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
    let world_b = runtime
        .spawn_actor(
            WorldActor {
                world_id: WorldId(75),
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

    let target_a = RouteTarget {
        service_kind: WORLD_SERVICE,
        instance_id: InstanceId::new("world-a"),
        advertised_endpoint: "http://world-a.world:18080".parse()?,
        owner_epoch: Some(Epoch(1)),
    };
    let target_b = RouteTarget {
        service_kind: WORLD_SERVICE,
        instance_id: InstanceId::new("world-b"),
        advertised_endpoint: "http://world-b.world:18080".parse()?,
        owner_epoch: Some(Epoch(1)),
    };
    let mut rpc_server = RpcServerBuilder::new();
    rpc_server.add_service("WorldRpc", target_a.clone())?;
    rpc_server.add_service("WorldAdminRpc", target_a.clone())?;

    let resolver = StaticRouteResolver::new(
        StaticPlacementConfig {
            ranges: vec![
                StaticRouteRange {
                    service_kind: WORLD_SERVICE,
                    actor_kind: WORLD_ACTOR,
                    start_inclusive: 0,
                    end_exclusive: 50,
                    target: target_a,
                },
                StaticRouteRange {
                    service_kind: WORLD_SERVICE,
                    actor_kind: WORLD_ACTOR,
                    start_inclusive: 50,
                    end_exclusive: 100,
                    target: target_b,
                },
            ],
        },
        RouteCacheConfig::default(),
    );
    let transport = LocalWorldTransport {
        adapters: HashMap::from([
            (
                InstanceId::new("world-a"),
                ActorRpcAdapter::new(world_a.clone()).with_owner_epoch(Epoch(1)),
            ),
            (
                InstanceId::new("world-b"),
                ActorRpcAdapter::new(world_b.clone()).with_owner_epoch(Epoch(1)),
            ),
        ]),
    };
    let context_factory =
        RpcClientContextFactory::new(service_kind!("Player"), InstanceId::new("player-0"))
            .with_trace(TraceContext {
                traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00".into()),
                tracestate: None,
            });
    let core = ResolvingRpcCore::new(
        WORLD_SERVICE,
        resolver.clone(),
        EndpointPool::new(),
        context_factory,
        transport,
    );
    let client = WorldClient::new(core.clone());

    let trace = TraceContext {
        traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00".into()),
        tracestate: None,
    };
    let events = LocalEventBus::new();
    let event_count = Arc::new(AtomicUsize::new(0));
    let event_count_clone = event_count.clone();
    events
        .subscribe(
            EventSubscription::local(SubjectFilter::new("game.world.*")),
            move |_event: EventEnvelope| {
                let event_count = event_count_clone.clone();
                async move {
                    event_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        )
        .await?;
    let publisher = EventPublisher::new(events, WORLD_SERVICE, InstanceId::new("world-a"));

    let scheduler = ServiceScheduler::new();
    let scheduled_ticks = Arc::new(AtomicUsize::new(0));
    let scheduled_ticks_clone = scheduled_ticks.clone();
    scheduler
        .interval(Duration::from_millis(config.tick_ms), move || {
            let scheduled_ticks = scheduled_ticks_clone.clone();
            async move {
                scheduled_ticks.fetch_add(1, Ordering::SeqCst);
            }
        })
        .await;

    let config_store = LocalConfigStore::default();
    config_store
        .put("world.tick_ms".to_string(), json!(config.tick_ms))
        .await?;

    let direct_reply = world_a.call(EnterWorld { player_id: 1000 }).await?;
    let rpc_reply = client
        .enter_world(EnterWorldRequest {
            world_id: 1,
            player_id: 1001,
        })
        .await?;
    let range_reply = client
        .enter_world(EnterWorldRequest {
            world_id: 75,
            player_id: 2001,
        })
        .await?;

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
    let dispatcher = generated::GatewayDispatcher::new(core);
    let gateway_reply_frame = dispatcher.dispatch(decoded).await?;
    let gateway_reply = EnterWorldReply::decode(gateway_reply_frame.payload.as_slice())?;
    let event_id = publisher
        .publish_bytes(
            Subject::new("game.world.player_entered"),
            "PlayerEntered",
            vec![1, 100],
            trace.clone(),
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(config.tick_ms * 2)).await;
    scheduler.shutdown().await;
    let snapshot_a = world_a.call(InspectWorld).await?;
    let snapshot_b = world_b.call(InspectWorld).await?;
    let configured_tick = config_store.get("world.tick_ms").await?;

    println!(
        "{}:{} direct_ok={} rpc_ok={} range_ok={} gateway_ok={} services={} routes={} placement_lookups={} players_a={} players_b={} ticks_a={} ticks_b={} events={} scheduled={} config_tick={} trace={} event_id={} last_rpc_request_id={}",
        WORLD_SERVICE.as_str(),
        WORLD_ACTOR.as_str(),
        direct_reply.ok,
        rpc_reply.ok,
        range_reply.ok,
        gateway_reply.ok,
        rpc_server.services().len(),
        usize::from(route_table.get(100).is_some()),
        resolver.placement_lookups(),
        snapshot_a.player_count,
        snapshot_b.player_count,
        snapshot_a.total_ticks,
        snapshot_b.total_ticks,
        event_count.load(Ordering::SeqCst),
        scheduled_ticks.load(Ordering::SeqCst),
        configured_tick.unwrap_or_default(),
        trace.traceparent.as_deref().unwrap_or("<missing>"),
        event_id.as_str(),
        snapshot_a
            .last_rpc_request_id
            .as_deref()
            .unwrap_or("<missing>")
    );
    Ok(())
}
