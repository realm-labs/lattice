#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use lattice_actor::actor_protocol;
use lattice_actor::context::ActorContext;
use lattice_actor::error::ActorError;
use lattice_actor::mailbox::MailboxConfig;
use lattice_actor::protocol::ProstCodec;
use lattice_actor::registry::{ActorCreateContext, ActorLoader};
use lattice_actor::reply::ReplyTo;
use lattice_actor::traits::{Actor, Responder};
use lattice_config::source::ConfigSource;
use lattice_core::actor_ref::{
    ClusterId, EntityId, EntityType, NodeAddress, NodeIncarnation, PlacementDomainId, ProtocolId,
    SingletonKind, SingletonRef,
};
use lattice_core::instance::InstanceId;
use lattice_core::trace::TraceContext;
use lattice_core::{actor_kind, service_kind};
use lattice_eventbus::local::{EventBus, LocalEventBus};
use lattice_eventbus::types::{EventEnvelope, EventId, EventSubscription, Subject, SubjectFilter};
use lattice_gateway::error::GatewayError;
use lattice_gateway::frame::ClientFrame;
use lattice_gateway::server::{GatewayTcpServer, read_client_frame, write_client_frame};
use lattice_ops::admin::{AdminAuth, AdminHttpAdapter, AdminSnapshot, CoordinatorAdminHandler};
use lattice_ops::scheduler::ServiceScheduler;
use lattice_ops::telemetry::{
    InMemoryTelemetryExporter, OpenTelemetryPipeline, PlacementDomainTelemetry, TelemetryRecorder,
    TelemetryResource,
};
use lattice_placement::storage::InMemoryPlacementStore;
use lattice_remoting::config::RemotingConfig;
use lattice_service::builder::LatticeService;
use lattice_service::config::{ClusterJoinConfig, NodeConfig};
use lattice_service::deployment::EmbeddedCoordinatorConfig;
use lattice_service::registration::{EntityOptions, SingletonOptions};
use prost::Message;
use serde::Deserialize;

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

#[async_trait]
impl Actor for WorldActor {
    type Error = ActorError;
}

#[async_trait]
impl Responder<EnterWorldRequest> for WorldActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
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

#[async_trait]
impl Actor for ClockActor {
    type Error = ActorError;
}

#[async_trait]
impl Responder<GetClockRequest> for ClockActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
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

fn reserve_address() -> Result<NodeAddress, Box<dyn std::error::Error>> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
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
    target: lattice_core::actor_ref::EntityRef<WorldProtocol>,
    player_id: u64,
) -> Result<EnterWorldReply, Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match service
            .ask(
                &target,
                EnterWorldRequest {
                    world_id: 1,
                    player_id,
                },
                Duration::from_secs(1),
            )
            .await
        {
            Ok(reply) => return Ok(reply),
            Err(error) if tokio::time::Instant::now() < deadline => {
                let _ = error;
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => return Err(Box::new(error)),
        }
    }
}

async fn eventually_tick(
    service: &LatticeService,
    target: SingletonRef<ClockProtocol>,
) -> Result<GetClockReply, Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match service
            .ask(&target, GetClockRequest {}, Duration::from_secs(1))
            .await
        {
            Ok(reply) => return Ok(reply),
            Err(error) if tokio::time::Instant::now() < deadline => {
                let _ = error;
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => return Err(Box::new(error)),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
    let clock_ref: SingletonRef<ClockProtocol> = SingletonRef::new(
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
        .ok_or_else(|| std::io::Error::other("logic service is unavailable"))?
        .clone();

    let direct_reply = eventually_enter(&logic, world_ref.clone(), 1001).await?;
    let clock_reply = eventually_tick(&logic, clock_ref).await?;

    let bus = LocalEventBus::new();
    let (event_tx, event_rx) = tokio::sync::oneshot::channel();
    let event_tx = Arc::new(tokio::sync::Mutex::new(Some(event_tx)));
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
        recipient: Some(lattice_core::actor_ref::RecipientRef::from(&world_ref).erase()),
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

    let gateway_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let gateway_address = gateway_listener.local_addr()?;
    let gateway_logic = logic.clone();
    let gateway_world = world_ref.clone();
    let (gateway_stop_tx, gateway_stop_rx) = tokio::sync::oneshot::channel();
    let gateway =
        GatewayTcpServer::new(gateway_listener, move |frame: ClientFrame| {
            let logic = gateway_logic.clone();
            let target = gateway_world.clone();
            async move {
                if frame.msg_id != 1 || frame.payload.len() != 8 {
                    return Err(GatewayError::DecodePayload(
                        "msg 1 requires an eight-byte player ID".to_owned(),
                    ));
                }
                let player_id =
                    u64::from_be_bytes(frame.payload.as_slice().try_into().map_err(|_| {
                        GatewayError::DecodePayload("invalid player ID".to_owned())
                    })?);
                let reply = eventually_enter(&logic, target, player_id)
                    .await
                    .map_err(|error| GatewayError::Recipient(error.to_string()))?;
                Ok(Some(ClientFrame {
                    msg_id: 2,
                    payload: reply.encode_to_vec(),
                }))
            }
        });
    let gateway_task = tokio::spawn(async move {
        gateway
            .run_until_shutdown_signal(async {
                let _ = gateway_stop_rx.await;
            })
            .await
    });
    let mut gateway_client = tokio::net::TcpStream::connect(gateway_address).await?;
    write_client_frame(
        &mut gateway_client,
        ClientFrame {
            msg_id: 1,
            payload: 1002_u64.to_be_bytes().to_vec(),
        },
    )
    .await?;
    let gateway_reply = EnterWorldReply::decode(
        read_client_frame(&mut gateway_client)
            .await?
            .payload
            .as_slice(),
    )?;
    drop(gateway_client);
    let _ = gateway_stop_tx.send(());
    gateway_task.await??;

    let coordinator_handle = application
        .coordinator_service()
        .ok_or_else(|| std::io::Error::other("Coordinator service is unavailable"))?
        .coordinator(&domain)
        .ok_or_else(|| std::io::Error::other("domain Coordinator handle is unavailable"))?;
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
        "domain={} direct_players={} gateway_players={} singleton_tick={} event={} task={} metrics={}",
        domain,
        direct_reply.player_count,
        gateway_reply.player_count,
        clock_reply.tick,
        event_type,
        scheduled,
        metric_count,
    );
    Ok(())
}
