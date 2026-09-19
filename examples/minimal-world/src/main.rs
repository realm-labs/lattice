#![cfg_attr(not(test), deny(clippy::wildcard_imports))]
use lattice_actor::context::HandlerContext;

use std::{
    collections::{BTreeSet, HashSet},
    error::Error as StdError,
    io::Error as IoError,
    net::TcpListener as StdTcpListener,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use lattice_actor_distributed::{
    actor_protocol,
    error::ActorError,
    mailbox::MailboxConfig,
    protocol::ProstCodec,
    registry::{ActorCreateContext, ActorLoader},
    reply::ReplyTo,
    traits::{Actor, Responder},
};
use lattice_config::source::ConfigSource;
use lattice_core::{
    actor_address::{
        ClusterId, EntityAddress, EntityId, EntityType, NodeAddress, NodeIncarnation,
        PlacementDomainId, ProtocolId, RecipientAddress, SingletonAddress, SingletonKind,
    },
    actor_kind,
    instance::InstanceId,
    service_kind,
    trace::{TelemetryResource, TraceContext},
};
use lattice_eventbus::{
    local::{EventBus, LocalEventBus},
    types::{EventEnvelope, EventId, EventSubscription, Subject, SubjectFilter},
};
use lattice_ops::{
    admin::{AdminAuth, AdminHttpAdapter, AdminSnapshot, CoordinatorAdminHandler},
    scheduler::ServiceScheduler,
    telemetry::{
        InMemoryTelemetryExporter, OpenTelemetryPipeline, PlacementDomainTelemetry,
        TelemetryRecorder,
    },
};
use lattice_placement::storage::InMemoryPlacementStore;
use lattice_remoting::config::RemotingConfig;
use lattice_service::{
    builder::LatticeService,
    config::{ClusterJoinConfig, NodeConfig},
    deployment::EmbeddedCoordinatorConfig,
    registration::{EntityOptions, SingletonOptions},
};
use serde::Deserialize;
use tokio::{sync::Mutex, time::Instant};

pub mod world {
    include!(concat!(env!("OUT_DIR"), "/world.rs"));
}

use world::{EnterWorldReply, EnterWorldRequest, GetClockReply, GetClockRequest};

const WORLD_PROTOCOL_ID: u64 = 0x776f_726c_6400_0001;
const CLOCK_PROTOCOL_ID: u64 = 0x776f_726c_6400_0002;

#[derive(Debug)]
struct WorldActor {
    world_id: u64,
    players: HashSet<u64>,
}

impl Actor for WorldActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;
}

impl Responder<EnterWorldRequest> for WorldActor {
    async fn respond(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        request: EnterWorldRequest,
        reply_to: ReplyTo<EnterWorldReply>,
    ) -> Result<(), ActorError> {
        let ok = request.world_id == self.world_id;
        if ok {
            self.players.insert(request.player_id);
        }
        let _ = reply_to.send(EnterWorldReply {
            ok,
            player_count: self.players.len() as u64,
        });
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct WorldLoader;

#[async_trait]
impl ActorLoader<WorldActor> for WorldLoader {
    async fn load(&self, _ctx: ActorCreateContext) -> Result<WorldActor, ActorError> {
        Ok(WorldActor {
            world_id: 1,
            players: HashSet::new(),
        })
    }
}

actor_protocol! {
    pub WorldProtocol {
        protocol_id: WORLD_PROTOCOL_ID;
        name: "minimal-world/world/v1";
        ask 1 => EnterWorldRequest {
            request_schema_version: 1,
            response_schema_version: 1,
            request_codec: ProstCodec,
            response_codec: ProstCodec,
        }
    }
}

#[derive(Debug, Default)]
struct ClockActor {
    tick: u64,
}

impl Actor for ClockActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;
}

impl Responder<GetClockRequest> for ClockActor {
    async fn respond(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        _request: GetClockRequest,
        reply_to: ReplyTo<GetClockReply>,
    ) -> Result<(), ActorError> {
        self.tick += 1;
        let _ = reply_to.send(GetClockReply { tick: self.tick });
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct ClockLoader;

#[async_trait]
impl ActorLoader<ClockActor> for ClockLoader {
    async fn load(&self, _ctx: ActorCreateContext) -> Result<ClockActor, ActorError> {
        Ok(ClockActor::default())
    }
}

actor_protocol! {
    pub ClockProtocol {
        protocol_id: CLOCK_PROTOCOL_ID;
        name: "minimal-world/clock/v1";
        ask 1 => GetClockRequest {
            request_schema_version: 1,
            response_schema_version: 1,
            request_codec: ProstCodec,
            response_codec: ProstCodec,
        }
    }
}

#[derive(Debug, Deserialize)]
struct WorldConfig {
    mailbox_capacity: usize,
    placement_domain: String,
    shard_count: u32,
    capacity_units: u64,
}

fn reserve_address() -> Result<NodeAddress, Box<dyn StdError>> {
    let listener = StdTcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(NodeAddress::new("127.0.0.1", port)?)
}

fn node_config(
    cluster_id: ClusterId,
    node_id: &str,
    address: NodeAddress,
    incarnation: NodeIncarnation,
) -> NodeConfig {
    NodeConfig {
        release: lattice_core::release::ReleaseManifest::development(1),
        cluster_id,
        node_id: node_id.to_owned(),
        address,
        incarnation,
        roles: BTreeSet::from(["world".to_owned()]),
        remoting: RemotingConfig {
            heartbeat_interval: Duration::from_millis(100),
            shutdown_timeout: Duration::from_secs(2),
            ..RemotingConfig::default()
        },
        maximum_actor_protocols: 16,
        maximum_watches: 128,
        maximum_supervised_tasks: 128,
        shutdown_timeout: Duration::from_secs(3),
    }
}

async fn eventually_enter(
    service: &LatticeService,
    target: EntityAddress<WorldProtocol>,
    player_id: u64,
) -> Result<EnterWorldReply, Box<dyn StdError>> {
    let target = service.bind_entity(target)?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match target
            .ask(
                EnterWorldRequest {
                    world_id: 1,
                    player_id,
                },
                Duration::from_secs(1),
            )
            .await
        {
            Ok(reply) => return Ok(reply),
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => return Err(Box::new(error)),
        }
    }
}

async fn eventually_tick(
    service: &LatticeService,
    target: SingletonAddress<ClockProtocol>,
) -> Result<GetClockReply, Box<dyn StdError>> {
    let target = service.bind_singleton(target)?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match target.ask(GetClockRequest {}, Duration::from_secs(1)).await {
            Ok(reply) => return Ok(reply),
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => return Err(Box::new(error)),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    let config: WorldConfig =
        ConfigSource::file("examples/minimal-world/config/world-service.toml")
            .load()?
            .section("world")?;
    let cluster_id = ClusterId::new("minimal-world")?;
    let domain = PlacementDomainId::new(config.placement_domain)?;
    let coordinator_address = reserve_address()?;
    let logic_address = reserve_address()?;
    let store = Arc::new(InMemoryPlacementStore::new(1024, 128)?);

    let logic_incarnation = NodeIncarnation::generate();
    let entity_options = EntityOptions::new(
        domain.clone(),
        EntityType::new("world")?,
        config.shard_count,
    )
    .actor_kind(actor_kind!("World"))
    .mailbox(MailboxConfig::bounded(config.mailbox_capacity));
    let singleton_options =
        SingletonOptions::new(domain.clone(), SingletonKind::new("world-clock")?)
            .actor_kind(actor_kind!("WorldClock"))
            .mailbox(MailboxConfig::bounded(config.mailbox_capacity));
    let entity_config = entity_options.build(ProtocolId::new(WORLD_PROTOCOL_ID)?)?;
    let singleton_config = singleton_options.build(ProtocolId::new(CLOCK_PROTOCOL_ID)?);
    let world_ref = entity_config
        .entity_ref::<WorldProtocol>(cluster_id.clone(), EntityId::new(b"world-1".to_vec())?)?;
    let clock_ref: SingletonAddress<ClockProtocol> = SingletonAddress::new(
        cluster_id.clone(),
        domain.clone(),
        singleton_config.kind.clone(),
        singleton_config.protocol_id,
        singleton_config.fingerprint(),
    )?
    .try_typed()?;

    let application = LatticeService::builder(node_config(
        cluster_id.clone(),
        "world-a",
        logic_address,
        logic_incarnation,
    ))?
    .host_entity::<WorldActor, WorldProtocol, _>(entity_options, WorldLoader)?
    .host_singleton::<ClockActor, ClockProtocol, _>(singleton_options, ClockLoader)?
    .domain_capacity(domain.clone(), config.capacity_units)?
    .join_config(ClusterJoinConfig {
        retry_initial: Duration::from_millis(10),
        retry_max: Duration::from_millis(100),
        join_timeout: Some(Duration::from_secs(10)),
        leave_timeout: Duration::from_secs(5),
        shutdown_timeout: Duration::from_secs(5),
        ..ClusterJoinConfig::default()
    })
    .build_embedded(
        store,
        EmbeddedCoordinatorConfig::new(node_config(
            cluster_id.clone(),
            "coordinator",
            coordinator_address,
            NodeIncarnation::generate(),
        )),
    )
    .await?;
    application.start().await?;
    application.wait_ready(Duration::from_secs(10)).await?;
    let logic = application
        .logic()
        .ok_or_else(|| IoError::other("logic service is unavailable"))?
        .clone();

    let direct_reply = eventually_enter(&logic, world_ref.clone(), 1001).await?;
    let clock_reply = eventually_tick(&logic, clock_ref).await?;

    let bus = LocalEventBus::new();
    let (event_tx, event_rx) = tokio::sync::oneshot::channel();
    let event_tx = Arc::new(Mutex::new(Some(event_tx)));
    bus.subscribe(
        EventSubscription::local(SubjectFilter::new("world.*")),
        move |event: EventEnvelope| {
            let event_tx = event_tx.clone();
            async move {
                if let Some(sender) = event_tx.lock().await.take() {
                    let _ = sender.send(event.event_type);
                }
                Ok(())
            }
        },
    )
    .await?;
    bus.publish(EventEnvelope {
        event_id: EventId::new("entered-1001"),
        subject: Subject::new("world.entered"),
        event_type: "player-entered".to_owned(),
        source_service: service_kind!("World"),
        source_instance: InstanceId::new("world-a"),
        recipient: Some(RecipientAddress::from(&world_ref).erase()),
        correlation_id: Some("minimal-world-run".to_owned()),
        trace: TraceContext::default(),
        occurred_unix_ms: 1,
        payload: Vec::new(),
    })
    .await?;
    let event_type = event_rx.await?;

    let scheduler = ServiceScheduler::new();
    let (scheduled_tx, scheduled_rx) = tokio::sync::oneshot::channel();
    scheduler
        .after(Duration::from_millis(1), async move {
            let _ = scheduled_tx.send("scheduled");
        })
        .await;
    let scheduled = scheduled_rx.await?;

    let coordinator_handle = application
        .coordinator_service()
        .ok_or_else(|| IoError::other("Coordinator service is unavailable"))?
        .coordinator(&domain)
        .ok_or_else(|| IoError::other("domain Coordinator handle is unavailable"))?;
    let _admin_router = AdminHttpAdapter::new(
        AdminAuth::disabled(),
        AdminSnapshot::default,
        CoordinatorAdminHandler::new(coordinator_handle),
    )
    .router();

    let telemetry = TelemetryRecorder::default();
    telemetry
        .record_placement_domain(&PlacementDomainTelemetry {
            cluster: cluster_id.as_str().to_owned(),
            domain: domain.clone(),
            candidate_state: "active".to_owned(),
            leader_term: 1,
            session_ready: true,
            route_available: true,
            unresolved_requests: 0,
            members: 1,
            capacity_units: config.capacity_units,
            load_units: 1,
            slots: u64::from(config.shard_count) + 1,
            claims: 2,
            plans: 0,
            reconciliation_backlog: 0,
            oldest_reconciliation_millis: 0,
        })
        .await?;
    let exporter = InMemoryTelemetryExporter::default();
    OpenTelemetryPipeline::new(
        TelemetryResource {
            service_kind: service_kind!("World"),
            instance_id: InstanceId::new("world-a"),
            service_version: env!("CARGO_PKG_VERSION").to_owned(),
        },
        exporter.clone(),
    )
    .export_from(&telemetry)
    .await?;
    let metric_count = exporter
        .batches()
        .await
        .first()
        .map_or(0, |batch| batch.metrics.len());

    scheduler.shutdown().await;
    application.shutdown().await?;

    println!(
        "domain={} players={} singleton_tick={} event={} task={} metrics={}",
        domain, direct_reply.player_count, clock_reply.tick, event_type, scheduled, metric_count,
    );
    Ok(())
}
