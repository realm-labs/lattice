use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use lattice_actor::context::ActorContext;
use lattice_actor::error::ActorError;
use lattice_actor::mailbox::MailboxConfig;
use lattice_actor::registry::{ActorCreateContext, ActorFactory};
use lattice_actor::traits::{Actor, Handler, Message};
use lattice_config::source::ConfigSource;
use lattice_config::store::{ConfigStore, LocalConfigStore};
use lattice_core::id::{ActorId, ActorKey, ActorKeyDecodeError, RouteKey};
use lattice_core::instance::InstanceId;
use lattice_core::kind::{ActorKind, ServiceKind};
use lattice_core::trace::TraceContext;
use lattice_core::{actor_kind, service_kind};
use lattice_eventbus::local::{EventBus, LocalEventBus};
use lattice_eventbus::publisher::EventPublisher;
use lattice_eventbus::types::{EventEnvelope, EventSubscription, Subject, SubjectFilter};
use lattice_gateway::frame::{BinaryClientCodec, ClientCodec, ClientFrame};
use lattice_ops::admin::{AdminSnapshot, ClusterSummary, NodeSummary};
use lattice_ops::scheduler::ServiceScheduler;
use lattice_ops::telemetry::{
    InMemoryTelemetryExporter, MetricSample, OpenTelemetryPipeline, TelemetryRecorder,
    TelemetryResource, TraceSpan, TraceSpanKind,
};
use lattice_placement::store::{InMemoryPlacementStore, PlacementPrefix};
use lattice_rpc::types::Rpc;
use lattice_service::actor::ActorRegistration;
use lattice_service::service::LatticeService;
use prost::Message as ProstMessage;
use serde::Deserialize;
use serde_json::json;
use tokio::net::TcpListener;

pub mod world {
    tonic::include_proto!("world");
}

pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/lattice.generated.rs"));
}

use generated::world_rpc::Client as WorldClient;
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

#[derive(Debug)]
pub struct WorldActor {
    pub world_id: WorldId,
    pub tick_ms: u64,
    pub players: HashMap<PlayerId, PlayerRuntimeState>,
    pub last_rpc_request_id: Option<String>,
}

#[async_trait]
impl Actor for WorldActor {
    type Error = ActorError;
    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        let tick_ms = self.tick_ms;
        ctx.notify_interval(Duration::from_millis(tick_ms), move || WorldTick {
            delta_ms: tick_ms,
        });
        Ok(())
    }
}

#[derive(Debug)]
pub struct WorldTick {
    pub delta_ms: u64,
}

impl Message for WorldTick {
    type Reply = ();
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

type GeneratedWorldClient = WorldClient<generated::world_rpc::DefaultClientCore>;

#[derive(Clone)]
struct WorldActorFactory {
    tick_ms: u64,
}

#[async_trait]
impl ActorFactory<WorldActor> for WorldActorFactory {
    async fn create(&self, ctx: ActorCreateContext) -> Result<WorldActor, ActorError> {
        let world_id = match ctx.actor_id {
            ActorId::U64(value) => WorldId(value),
            other => {
                return Err(ActorError::new(format!(
                    "expected u64 world actor id, got {other:?}"
                )));
            }
        };

        Ok(WorldActor {
            world_id,
            tick_ms: self.tick_ms,
            players: HashMap::new(),
            last_rpc_request_id: None,
        })
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
    let placement_store =
        InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/minimal-world"));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let service = LatticeService::builder(WORLD_SERVICE)
        .instance_id(InstanceId::new("world-a"))
        .listen(listener)
        .ready_signal(ready_tx)
        .placement_store::<InMemoryPlacementStore, _>(placement_store)
        .register_client::<generated::world_rpc::Binding>()
        .register_actor(
            ActorRegistration::builder(WORLD_ACTOR)
                .factory(WorldActorFactory {
                    tick_ms: config.tick_ms,
                })
                .mailbox(MailboxConfig::bounded(config.mailbox_capacity))
                .build(),
        )
        .register_sharded_rpc(generated::world_rpc::Binding::for_actor::<WorldActor>(
            WORLD_ACTOR,
        ))
        .build()
        .await?;
    let service_context = service.context().clone();
    let service_task = tokio::spawn(service.run_until_shutdown_signal(async {
        let _ = shutdown_rx.await;
    }));
    let _service_addr = ready_rx.await?;
    let client = service_context
        .extension::<GeneratedWorldClient>()
        .ok_or_else(|| {
            std::io::Error::other("generated World client was not registered in ServiceContext")
        })?;

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
    let mut admin_snapshot = AdminSnapshot::new(
        ClusterSummary {
            instance_count: 1,
            actor_owner_count: 0,
        },
        Vec::new(),
    );
    admin_snapshot.node_summary = Some(NodeSummary {
        instance_id: InstanceId::new("world-a"),
        service_kind: WORLD_SERVICE,
        actor_kinds: vec![WORLD_ACTOR],
    });

    let telemetry = TelemetryRecorder::default();
    telemetry
        .record_span(TraceSpan {
            name: "minimal_world.enter_world".to_string(),
            kind: TraceSpanKind::Rpc,
            context: trace.clone(),
            links: Vec::new(),
        })
        .await;

    let rpc_reply = client
        .enter_world(EnterWorldRequest {
            world_id: 1,
            player_id: 1001,
        })
        .await?;
    let second_actor_reply = client
        .enter_world(EnterWorldRequest {
            world_id: 75,
            player_id: 2001,
        })
        .await?;

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
    let gateway_decoded = EnterWorldRequest::decode(decoded.payload.as_slice())?;
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
    let configured_tick = config_store.get("world.tick_ms").await?;
    telemetry
        .record_metric(MetricSample {
            name: "minimal_world.events".to_string(),
            value: event_count.load(Ordering::SeqCst) as u64,
            labels: HashMap::from([("service".to_string(), WORLD_SERVICE.to_string())]),
        })
        .await?;
    let telemetry_exporter = InMemoryTelemetryExporter::default();
    let telemetry_pipeline = OpenTelemetryPipeline::new(
        TelemetryResource {
            service_kind: WORLD_SERVICE,
            instance_id: InstanceId::new("world-a"),
            service_version: "minimal-world".to_string(),
        },
        telemetry_exporter.clone(),
    );
    telemetry_pipeline.export_from(&telemetry).await?;
    let telemetry_batches = telemetry_exporter.batches().await;
    let telemetry_batch = telemetry_batches.first().ok_or_else(|| {
        std::io::Error::other("minimal world telemetry exporter produced no batches")
    })?;
    let ops_actor_kinds = admin_snapshot
        .node_summary
        .as_ref()
        .map(|summary| summary.actor_kinds.len())
        .unwrap_or_default();
    shutdown_tx.send(()).map_err(|_| {
        std::io::Error::other("minimal world service shutdown receiver was dropped")
    })?;
    service_task.await??;

    println!(
        "{}:{} rpc_ok={} second_actor_ok={} client_msg_id={} gateway_world_id={} events={} scheduled={} config_tick={} trace={} event_id={} ops_actor_kinds={} telemetry_spans={} telemetry_metrics={}",
        WORLD_SERVICE.as_str(),
        WORLD_ACTOR.as_str(),
        rpc_reply.ok,
        second_actor_reply.ok,
        decoded.msg_id,
        gateway_decoded.world_id,
        event_count.load(Ordering::SeqCst),
        scheduled_ticks.load(Ordering::SeqCst),
        configured_tick.unwrap_or_default(),
        trace.traceparent.as_deref().unwrap_or("<missing>"),
        event_id.as_str(),
        ops_actor_kinds,
        telemetry_batch.spans.len(),
        telemetry_batch.metrics.len()
    );
    Ok(())
}
