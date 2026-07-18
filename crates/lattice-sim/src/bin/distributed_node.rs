#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::BytesMut;
use clap::{Parser, ValueEnum};
use lattice_actor::actor_protocol;
use lattice_actor::context::ActorContext;
use lattice_actor::directory::ActivationDirectory;
use lattice_actor::error::ActorError;
use lattice_actor::protocol::CodecDescriptor;
use lattice_actor::protocol::DecodeError;
use lattice_actor::protocol::EncodeError;
use lattice_actor::protocol::WireCodec;
use lattice_actor::registry::{ActorCreateContext, ActorLoader};
use lattice_actor::registry::{ActorRefConfig, ActorRegistry, ActorRegistryConfig};
use lattice_actor::reply::ReplyTo;
use lattice_actor::traits::{Actor, Handler, Responder};
use lattice_config::store::ConfigStore;
use lattice_config_etcd::config::EtcdConfigStoreConfig;
use lattice_config_etcd::store::EtcdConfigStore;
use lattice_core::actor_kind;
use lattice_core::actor_ref::{
    ActorRef, ClusterId, EntityId, EntityRef, EntityType, NodeAddress, NodeIncarnation,
    PlacementDomainId, ProtocolId,
};
use lattice_core::coordinator::CoordinatorScope;
use lattice_core::id::ActorId;
use lattice_core::instance::InstanceId;
use lattice_core::kind::ServiceKind;
use lattice_core::service_context::ServiceContext;
use lattice_discovery::config_store::ConfigStoreDiscovery;
use lattice_discovery::provider::CoordinatorDiscovery;
use lattice_discovery::static_provider::{StaticDiscovery, StaticEndpoint};
use lattice_placement::control::{
    DEFAULT_MAX_CONTROL_PAYLOAD, PlacementControlCommand, PlacementControlRouter,
    encode_control_command,
};
use lattice_placement::coordinator::SnapshotLimits;
use lattice_placement::coordinator::SnapshotRecord;
use lattice_placement::coordinator::SnapshotVersion;
use lattice_placement::coordinator::build_snapshot;
use lattice_placement::coordinator::{MemberHello, PlacementDomainHello};
use lattice_placement::coordinator::{MemberRecord, MemberStatus};
use lattice_placement::region::EntityConfig;
use lattice_placement::runtime::CoordinatorRuntimeError;
use lattice_placement::runtime::host::{CoordinatorHost, CoordinatorHostConfig};
use lattice_placement::runtime::{PlacementDomainLeader, PlacementDomainLeaderConfig};
use lattice_placement::session::LogicCoordinatorConfig;
use lattice_placement::session::PlacementDomainSession;
use lattice_placement::storage::InMemoryPlacementStore;
use lattice_placement::storage::ScopedElectionStore;
use lattice_placement::storage::domain::DurableStorageLimits;
use lattice_placement::storage::etcd::{EtcdPlacementConfig, EtcdPlacementStore};
use lattice_placement::types::AssignmentGeneration;
use lattice_placement::types::ClaimGrant;
use lattice_placement::types::CoordinatorTerm;
use lattice_placement::types::GrantSequence;
use lattice_placement::types::MembershipVersion;
use lattice_placement::types::NodeKey;
use lattice_placement::types::PlacementSlot;
use lattice_placement::types::PlacementSlotKey;
use lattice_placement::types::PlacementSlotState;
use lattice_placement::types::PlacementVersion;
use lattice_placement::types::Revision;
use lattice_remoting::association::AssociationKey;
use lattice_remoting::association::AssociationManager;
use lattice_remoting::association::LaneAttachment;
use lattice_remoting::association::LaneKind;
use lattice_remoting::config::RemotingConfig;
use lattice_remoting::control::CommandId;
use lattice_remoting::control::ControlDispatch;
use lattice_remoting::handshake::NodeIdentity;
use lattice_remoting::watch::WatchStatus;
use lattice_service::builder::LatticeService;
use lattice_service::builder::LatticeServiceBuilder;
use lattice_service::cluster::{DomainLogicalRouter, LogicalBufferConfig};
use lattice_service::config::ClusterJoinConfig;
use lattice_service::config::NodeConfig;
use lattice_service::lifecycle::NodeLifecycleState;
use serde::{Deserialize, Serialize};

const PROTOCOL_ID: u64 = 0x7369_6d00_0000_0001;

#[derive(Parser)]
struct Cli {
    #[arg(value_enum)]
    role: Role,
    #[arg(long, default_value = "/artifacts/server-ref.json")]
    reference: PathBuf,
    #[arg(long)]
    expect_failure: bool,
    #[arg(long, default_value = "coordinator-a")]
    node_id: String,
    #[arg(long, default_value_t = 29101)]
    port: u16,
    #[arg(long, default_value = "")]
    domains: String,
}

#[derive(Clone, Copy, ValueEnum)]
enum Role {
    Server,
    Client,
    Monitor,
    EntityOwner,
    Gateway,
    Coordinator,
    DiscoveryCoordinator,
    StaticMember,
    ConfigMember,
    DomainHost,
    DomainLogic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScopedLeadershipArtifact {
    node_id: String,
    term: u64,
    incarnation: u128,
}

#[derive(Debug, Serialize)]
struct DiscoveryLifecycleArtifact {
    node_id: String,
    incarnation: u128,
    provider: String,
    lifecycle: String,
    authoritative_up_members: Vec<(String, u128)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MultiDomainHostArtifact {
    node_id: String,
    incarnation: u128,
    scopes: BTreeMap<String, ScopedLeadershipArtifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MultiDomainLogicArtifact {
    node_id: String,
    lifecycle: String,
    domains: BTreeMap<String, String>,
}

#[derive(Debug, Clone, lattice_actor::Request)]
#[request(response = Pong)]
struct Ping(u64);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Pong(u64);

#[derive(Debug, Clone, lattice_actor::Message)]
struct StopPing;

#[derive(Clone, Copy)]
struct PingCodec;

impl WireCodec<Ping> for PingCodec {
    const DESCRIPTOR: CodecDescriptor = CodecDescriptor::new(1, 1);

    fn encode(&self, value: &Ping, output: &mut BytesMut) -> Result<(), EncodeError> {
        output.extend_from_slice(&value.0.to_be_bytes());
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<Ping, DecodeError> {
        Ok(Ping(u64::from_be_bytes(input.try_into().map_err(
            |_| DecodeError::new("Ping requires eight bytes"),
        )?)))
    }
}

#[derive(Clone, Copy)]
struct PongCodec;

#[derive(Clone, Copy)]
struct EmptyCodec;

impl WireCodec<StopPing> for EmptyCodec {
    const DESCRIPTOR: CodecDescriptor = CodecDescriptor::new(1, 1);

    fn encode(&self, _value: &StopPing, _output: &mut BytesMut) -> Result<(), EncodeError> {
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<StopPing, DecodeError> {
        if input.is_empty() {
            Ok(StopPing)
        } else {
            Err(DecodeError::new("StopPing requires an empty payload"))
        }
    }
}

impl WireCodec<Pong> for PongCodec {
    const DESCRIPTOR: CodecDescriptor = CodecDescriptor::new(1, 1);

    fn encode(&self, value: &Pong, output: &mut BytesMut) -> Result<(), EncodeError> {
        output.extend_from_slice(&value.0.to_be_bytes());
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<Pong, DecodeError> {
        Ok(Pong(u64::from_be_bytes(input.try_into().map_err(
            |_| DecodeError::new("Pong requires eight bytes"),
        )?)))
    }
}

struct PingActor {
    child_reference: Option<PathBuf>,
}

#[derive(Clone)]
struct PingLoader;

#[async_trait]
impl ActorLoader<PingActor> for PingLoader {
    async fn load(&self, _context: ActorCreateContext) -> Result<PingActor, ActorError> {
        Ok(PingActor {
            child_reference: None,
        })
    }
}

#[derive(Serialize, Deserialize)]
struct EntityFixture {
    owner_node_id: String,
    owner_address: NodeAddress,
    owner_incarnation: String,
    reference: EntityRef<FixtureProtocol>,
}

struct EntityServiceFixture {
    service: LatticeService,
    control: Arc<PlacementControlRouter>,
    coordinator: AssociationKey,
    member: MemberRecord,
}

#[derive(Serialize, Deserialize)]
struct MonitorCommand {
    sequence: u64,
    stop: bool,
}

impl EntityFixture {
    fn owner(&self) -> Result<NodeKey, Box<dyn std::error::Error>> {
        Ok(NodeKey {
            node_id: self.owner_node_id.clone(),
            address: self.owner_address.clone(),
            incarnation: NodeIncarnation::new(self.owner_incarnation.parse()?)?,
        })
    }
}

#[async_trait]
impl Actor for PingActor {
    type Error = ActorError;

    async fn started(&mut self, context: &mut ActorContext<Self>) -> Result<(), Self::Error> {
        let Some(reference) = self.child_reference.take() else {
            return Ok(());
        };
        let child = context.spawn_child(
            lattice_actor::traits::ChildActorKey::new("remote-child"),
            PingActor {
                child_reference: None,
            },
            lattice_actor::traits::ChildActorOptions {
                protocol_id: Some(
                    ProtocolId::new(PROTOCOL_ID)
                        .map_err(|error| ActorError::new(error.to_string()))?,
                ),
                ..lattice_actor::traits::ChildActorOptions::default()
            },
        )?;
        let child_ref = child
            .actor_ref()
            .ok_or_else(|| ActorError::new("missing child ref"))?;
        std::fs::write(
            reference,
            serde_json::to_vec(child_ref).map_err(ActorError::from_error)?,
        )
        .map_err(ActorError::from_error)
    }
}

#[async_trait]
impl Responder<Ping> for PingActor {
    async fn respond(
        &mut self,
        _context: &mut ActorContext<Self>,
        request: Ping,
        reply_to: ReplyTo<Pong>,
    ) -> Result<(), ActorError> {
        let _ = reply_to.send(Pong(request.0 + 1));
        Ok(())
    }
}

#[async_trait]
impl Handler<StopPing> for PingActor {
    async fn handle(
        &mut self,
        context: &mut ActorContext<Self>,
        _message: StopPing,
    ) -> Result<(), ActorError> {
        context.request_stop();
        Ok(())
    }
}

actor_protocol! {
    FixtureProtocol {
        protocol_id: PROTOCOL_ID;
        name: "distributed-fixture/ping/v1";
        ask 1 => Ping {
            request_schema_version: 1,
            response_schema_version: 1,
            request_codec: PingCodec,
            response_codec: PongCodec,
        }
        tell 2 => StopPing {
            schema_version: 1,
            codec: EmptyCodec,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.role {
        Role::Server => server(cli.reference).await,
        Role::Client => client(cli.reference, cli.expect_failure).await,
        Role::Monitor => monitor(cli.reference).await,
        Role::EntityOwner => entity_owner(cli.reference).await,
        Role::Gateway => gateway(cli.reference).await,
        Role::Coordinator => coordinator(cli.reference, cli.node_id, cli.port).await,
        Role::DiscoveryCoordinator => {
            discovery_coordinator(cli.reference, cli.node_id, cli.port).await
        }
        Role::StaticMember => discovery_member(cli.reference, cli.node_id, cli.port, false).await,
        Role::ConfigMember => discovery_member(cli.reference, cli.node_id, cli.port, true).await,
        Role::DomainHost => domain_host(cli.reference, cli.node_id, cli.port, cli.domains).await,
        Role::DomainLogic => domain_logic(cli.reference, cli.node_id, cli.port).await,
    }
}

async fn discovery_coordinator(
    artifact: PathBuf,
    node_id: String,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let cluster = ClusterId::new("docker-discovery")?;
    let address = NodeAddress::new(node_id.clone(), port)?;
    let incarnation = NodeIncarnation::generate();
    let builder =
        LatticeService::builder(node_config(cluster, &node_id, address.clone(), incarnation))?;
    let store = Arc::new(InMemoryPlacementStore::new(64, 64)?);
    let host = CoordinatorHost::elect(
        store,
        builder.association_manager(),
        NodeKey {
            node_id: node_id.clone(),
            address,
            incarnation,
        },
        BTreeSet::from([placement_domain()]),
        CoordinatorHostConfig {
            placement: PlacementDomainLeaderConfig {
                renewal_interval: Duration::from_millis(100),
                ..PlacementDomainLeaderConfig::default()
            },
            ..CoordinatorHostConfig::default()
        },
    )
    .await?;
    let (control, controls) = PlacementControlRouter::bounded(64, DEFAULT_MAX_CONTROL_PAYLOAD)?;
    let service = builder
        .coordinator_host(Arc::new(control), host, controls)
        .build()?;
    service.start().await?;
    write_atomic(
        artifact,
        &serde_json::to_vec_pretty(&DiscoveryLifecycleArtifact {
            node_id,
            incarnation: incarnation.get(),
            provider: "coordinator".to_owned(),
            lifecycle: format!("{:?}", service.node_lifecycle_state()),
            authoritative_up_members: Vec::new(),
        })?,
    )?;
    tokio::signal::ctrl_c().await?;
    service.shutdown().await?;
    Ok(())
}

async fn discovery_member(
    artifact: PathBuf,
    node_id: String,
    port: u16,
    config_store: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let coordinator = NodeAddress::new("discovery-coordinator", 29200)?;
    let scope = CoordinatorScope::Membership;
    let discovery: Arc<dyn CoordinatorDiscovery> = if config_store {
        let run_id = std::env::var("LATTICE_RUN_ID")?;
        let endpoints = std::env::var("LATTICE_ETCD_ENDPOINTS")?
            .split(',')
            .map(str::to_owned)
            .collect();
        let store = EtcdConfigStore::connect(EtcdConfigStoreConfig {
            key_prefix: format!("/lattice-discovery/{run_id}"),
            endpoints,
        })
        .await?;
        store
            .put(
                "/discovery/endpoints".to_owned(),
                serde_json::json!({
                    "schema_version": 1,
                    "generation": 1,
                    "endpoints": [{
                        "host": coordinator.host(),
                        "port": coordinator.port(),
                        "node_id": "discovery-coordinator",
                        "priority": 10
                    }]
                }),
            )
            .await?;
        Arc::new(ConfigStoreDiscovery::new(
            scope.clone(),
            store,
            "/discovery/endpoints",
        )?)
    } else {
        Arc::new(StaticDiscovery::new(
            scope,
            "docker-static",
            vec![StaticEndpoint {
                address: coordinator,
                expected_node_id: Some("discovery-coordinator".to_owned()),
                priority: 10,
            }],
        )?)
    };
    let incarnation = NodeIncarnation::generate();
    let advertised_host = if config_store {
        "discovery-config-member"
    } else {
        "discovery-static-member"
    };
    let address = NodeAddress::new(advertised_host, port)?;
    let join_config = ClusterJoinConfig {
        retry_initial: Duration::from_millis(25),
        retry_max: Duration::from_millis(250),
        join_timeout: Some(Duration::from_secs(30)),
        leave_timeout: Duration::from_secs(5),
        shutdown_timeout: Duration::from_secs(8),
        ..ClusterJoinConfig::default()
    };
    let service = LatticeService::builder(node_config(
        ClusterId::new("docker-discovery")?,
        &node_id,
        address,
        incarnation,
    ))?
    .coordinator_discovery(discovery)?
    .join_config(join_config)
    .member_event_capacity(64)
    .build()?;
    service.start().await?;
    let mut lifecycle = service.subscribe_node_lifecycle();
    tokio::time::timeout(Duration::from_secs(30), async {
        while *lifecycle.borrow() != NodeLifecycleState::Ready {
            lifecycle.changed().await.map_err(|_| "lifecycle closed")?;
        }
        Ok::<(), &'static str>(())
    })
    .await??;
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if service.member_snapshot().members.iter().any(|record| {
                record.node.node_id == node_id
                    && record.node.incarnation == incarnation
                    && record.status == MemberStatus::Up
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await?;
    write_discovery_artifact(
        &artifact,
        &service,
        &node_id,
        incarnation,
        if config_store {
            "config-store"
        } else {
            "static"
        },
    )?;
    let leave_marker = artifact.with_extension("leave");
    tokio::time::timeout(Duration::from_secs(300), async {
        while !leave_marker.exists() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await?;
    service
        .leave(tokio::time::Instant::now() + Duration::from_secs(5))
        .await?;
    write_discovery_artifact(
        &artifact,
        &service,
        &node_id,
        incarnation,
        if config_store {
            "config-store"
        } else {
            "static"
        },
    )?;
    Ok(())
}

fn write_discovery_artifact(
    artifact: &std::path::Path,
    service: &LatticeService,
    node_id: &str,
    incarnation: NodeIncarnation,
    provider: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let members = service
        .member_snapshot()
        .members
        .into_iter()
        .filter(|record| record.status == MemberStatus::Up)
        .map(|record| (record.node.node_id, record.node.incarnation.get()))
        .collect();
    write_atomic(
        artifact.to_path_buf(),
        &serde_json::to_vec_pretty(&DiscoveryLifecycleArtifact {
            node_id: node_id.to_owned(),
            incarnation: incarnation.get(),
            provider: provider.to_owned(),
            lifecycle: format!("{:?}", service.node_lifecycle_state()),
            authoritative_up_members: members,
        })?,
    )?;
    Ok(())
}

async fn coordinator(
    artifact: PathBuf,
    node_id: String,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let endpoints = std::env::var("LATTICE_ETCD_ENDPOINTS")?
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let run_id = std::env::var("LATTICE_RUN_ID")?;
    let store = Arc::new(
        EtcdPlacementStore::connect(EtcdPlacementConfig {
            endpoints,
            cluster_prefix: format!("/lattice-ha/{run_id}"),
            list_page_size: 64,
            limits: DurableStorageLimits {
                maximum_slots: 65_536,
                maximum_plans: 4_096,
                maximum_members: 1_024,
                maximum_admin_operations: 4_096,
                maximum_entity_configs: 1_024,
                maximum_singleton_configs: 1_024,
            },
            connect_options: None,
        })
        .await?,
    );
    let incarnation = NodeIncarnation::generate();
    let address = NodeAddress::new(node_id.clone(), port)?;
    let associations = Arc::new(AssociationManager::new(
        address.clone(),
        incarnation,
        RemotingConfig::default(),
    )?);
    let node = NodeKey {
        node_id: node_id.clone(),
        address,
        incarnation,
    };
    let config = PlacementDomainLeaderConfig {
        leader_lease_ttl: Duration::from_secs(10),
        renewal_interval: Duration::from_secs(1),
        ..PlacementDomainLeaderConfig::default()
    };
    let mut next_term = 1_u64;
    let scope = CoordinatorScope::Placement(placement_domain());
    loop {
        match store.get_leader(&scope).await {
            Ok(Some(current)) => {
                next_term = next_term.max(current.term.get().saturating_add(1));
            }
            Ok(None) => {
                let term = CoordinatorTerm::new(next_term)?;
                match PlacementDomainLeader::elect(
                    store.clone(),
                    associations.clone(),
                    node.clone(),
                    scope.clone(),
                    term,
                    config.clone(),
                )
                .await
                {
                    Ok(leader) => {
                        write_coordinator_artifact(
                            &artifact,
                            &ScopedLeadershipArtifact {
                                node_id: node_id.clone(),
                                term: leader.leader().term.get(),
                                incarnation: incarnation.get(),
                            },
                        )?;
                        let (_router, controls) =
                            PlacementControlRouter::bounded(64, DEFAULT_MAX_CONTROL_PAYLOAD)?;
                        let (_shutdown, shutdown) = tokio::sync::watch::channel(false);
                        let _ = leader.run(controls, shutdown).await;
                        next_term = next_term.saturating_add(1);
                    }
                    Err(CoordinatorRuntimeError::NotLeader)
                    | Err(CoordinatorRuntimeError::Storage(
                        lattice_placement::storage::StorageError::CompareFailed,
                    )) => {}
                    Err(error) => return Err(error.into()),
                }
            }
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn write_coordinator_artifact(
    path: &std::path::Path,
    artifact: &ScopedLeadershipArtifact,
) -> Result<(), Box<dyn std::error::Error>> {
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, serde_json::to_vec_pretty(artifact)?)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

async fn server(reference: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let cluster = ClusterId::new("docker-e2e")?;
    let address = NodeAddress::new("fixture-server", 25520)?;
    let incarnation = NodeIncarnation::generate();
    let protocol = Arc::new(FixtureProtocol::bind::<PingActor>()?);
    let mut service_context = ServiceContext::builder(
        ServiceKind::from_static("distributed-fixture"),
        InstanceId::new("distributed-fixture"),
    );
    service_context.insert_extension(ActivationDirectory::new(64)?)?;
    let registry = Arc::new(ActorRegistry::new_bound(
        actor_kind!("DistributedFixture"),
        ActorRegistryConfig {
            actor_ref: Some(ActorRefConfig {
                cluster_id: cluster.clone(),
                node_address: address.clone(),
                node_incarnation: incarnation,
            }),
            service: service_context.build(),
            ..ActorRegistryConfig::default()
        },
        protocol.as_ref(),
    ));
    let child_reference = reference.with_file_name("child-ref.json");
    let handle = registry
        .start(
            ActorId::U64(1),
            PingActor {
                child_reference: Some(child_reference),
            },
        )
        .await?;
    let target: ActorRef<FixtureProtocol> = handle.typed_actor_ref()?.ok_or("missing actor ref")?;
    let service =
        LatticeService::builder(node_config(cluster, "fixture-server", address, incarnation))?
            .register_actor(registry, protocol)?
            .build()?;
    service.start().await?;
    std::fs::write(reference, serde_json::to_vec(&target)?)?;
    tokio::signal::ctrl_c().await?;
    service.shutdown().await?;
    Ok(())
}

async fn client(
    reference: PathBuf,
    expect_failure: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(60);
    let encoded = loop {
        match std::fs::read(&reference) {
            Ok(encoded) => break encoded,
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound && Instant::now() < deadline =>
            {
                tokio::task::yield_now().await;
            }
            Err(error) => return Err(Box::new(error)),
        }
    };
    let target: ActorRef<FixtureProtocol> = serde_json::from_slice(&encoded)?;
    let cluster = ClusterId::new("docker-e2e")?;
    let client_address = NodeAddress::new("aaa-client", 25521)?;
    let client_incarnation = NodeIncarnation::new(200)?;
    let service = LatticeService::builder(node_config(
        cluster.clone(),
        "aaa-client",
        client_address,
        client_incarnation,
    ))?
    .use_protocol::<FixtureProtocol>()?
    .build()?;
    service.start().await?;
    let connected = tokio::time::timeout(
        Duration::from_secs(10),
        service.connect_peer(NodeIdentity {
            cluster_id: cluster,
            node_id: "fixture-server".to_owned(),
            address: target.node_address().clone(),
            incarnation: target.node_incarnation(),
        }),
    )
    .await;
    if expect_failure && !matches!(connected, Ok(Ok(_))) {
        service.shutdown().await?;
        return Ok(());
    }
    connected??;
    let reply = service
        .ask(&target, Ping(41), Duration::from_secs(10))
        .await;
    if expect_failure {
        service.shutdown().await?;
        return if reply.is_err() {
            Ok(())
        } else {
            Err("stale or partitioned reference unexpectedly succeeded".into())
        };
    }
    let reply = reply?;
    if reply != Pong(42) {
        return Err("unexpected distributed reply".into());
    }
    let child_encoded = std::fs::read(reference.with_file_name("child-ref.json"))?;
    let child: ActorRef<FixtureProtocol> = serde_json::from_slice(&child_encoded)?;
    if service
        .ask(&child, Ping(99), Duration::from_secs(10))
        .await?
        != Pong(100)
    {
        return Err("unexpected distributed child reply".into());
    }
    let watch_id = service.watch(&child).await?;
    service.tell(&child, StopPing).await?;
    let watch_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if service.watch_status(watch_id) == WatchStatus::Terminated {
            break;
        }
        if Instant::now() >= watch_deadline {
            return Err("remote child watch did not terminate".into());
        }
        tokio::task::yield_now().await;
    }
    std::fs::write("/artifacts/multiprocess.json", b"{\"reply\":42}\n")?;
    std::fs::write(
        "/artifacts/child-multiprocess.json",
        b"{\"reply\":100,\"terminated\":true}\n",
    )?;
    service.shutdown().await?;
    Ok(())
}

async fn monitor(reference: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let encoded = wait_for_file(&reference).await?;
    let target: ActorRef<FixtureProtocol> = serde_json::from_slice(&encoded)?;
    let cluster = ClusterId::new("docker-e2e")?;
    let address = NodeAddress::new("aaa-monitor", 25522)?;
    let incarnation = NodeIncarnation::generate();
    let service = LatticeService::builder(node_config(
        cluster.clone(),
        "chaos-monitor",
        address,
        incarnation,
    ))?
    .use_protocol::<FixtureProtocol>()?
    .build()?;
    service.start().await?;
    service
        .connect_peer(NodeIdentity {
            cluster_id: cluster,
            node_id: "fixture-server".to_owned(),
            address: target.node_address().clone(),
            incarnation: target.node_incarnation(),
        })
        .await?;
    if service
        .ask(&target, Ping(1), Duration::from_secs(5))
        .await?
        != Pong(2)
    {
        return Err("chaos monitor initial probe failed".into());
    }
    write_atomic(
        PathBuf::from("/artifacts/monitor-ready.json"),
        b"{\"ready\":true}\n",
    )?;
    let command_path = PathBuf::from("/artifacts/monitor-command.json");
    let mut applied = 0;
    loop {
        let command = match std::fs::read(&command_path) {
            Ok(encoded) => serde_json::from_slice::<MonitorCommand>(&encoded)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                tokio::task::yield_now().await;
                continue;
            }
            Err(error) => return Err(Box::new(error)),
        };
        if command.sequence <= applied {
            tokio::task::yield_now().await;
            continue;
        }
        applied = command.sequence;
        if command.stop {
            break;
        }
        let result = service
            .ask(&target, Ping(applied), Duration::from_secs(3))
            .await;
        write_atomic(
            PathBuf::from(format!("/artifacts/monitor-result-{applied}.json")),
            &serde_json::to_vec(&serde_json::json!({
                "sequence": applied,
                "success": result.is_ok(),
                "outcome": if result.is_ok() { "reply" } else { "bounded-failure" },
            }))?,
        )?;
    }
    service.shutdown().await?;
    Ok(())
}

async fn entity_owner(reference: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let cluster = ClusterId::new("docker-e2e")?;
    let incarnation = NodeIncarnation::generate();
    let owner = NodeKey {
        node_id: "entity-owner".to_owned(),
        address: NodeAddress::new("entity-owner", 25530)?,
        incarnation,
    };
    let entity_config = fixture_entity_config()?;
    let entity_id = EntityId::new(b"gateway-account-42".to_vec())?;
    let slot = fixture_entity_slot(&entity_config, &entity_id, owner.clone())?;
    let EntityServiceFixture {
        service,
        control,
        coordinator,
        member,
    } = entity_service(
        cluster.clone(),
        owner.clone(),
        entity_config.clone(),
        &slot,
        true,
    )?;
    service.start().await?;
    install_fixture_snapshot(&control, &coordinator, &slot, member, true).await?;
    wait_for_node_ready(&service).await?;
    std::fs::write(
        "/artifacts/coordinator-placement-snapshot.json",
        serde_json::to_vec_pretty(&serde_json::json!({
            "redacted": true,
            "term": slot.version.term.get(),
            "revision": slot.version.revision.get(),
            "slot": {
                "entity_type": entity_config.entity_type.as_str(),
                "shard_id": entity_config.shard_for(&entity_id).get(),
                "owner_node_id": owner.node_id.clone(),
                "owner_address": owner.address.to_string(),
                "owner_incarnation": owner.incarnation.get().to_string(),
                "assignment_generation": slot.assignment_generation.get(),
                "state": "Running",
            },
            "claim_ttl_seconds": 300,
        }))?,
    )?;
    std::fs::write(
        reference,
        serde_json::to_vec(&EntityFixture {
            owner_node_id: owner.node_id,
            owner_address: owner.address,
            owner_incarnation: owner.incarnation.get().to_string(),
            reference: entity_config.entity_ref(cluster, entity_id)?,
        })?,
    )?;
    tokio::signal::ctrl_c().await?;
    service.force_shutdown().await?;
    Ok(())
}

async fn gateway(reference: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let encoded = wait_for_file(&reference).await?;
    let fixture: EntityFixture = serde_json::from_slice(&encoded)?;
    let owner = fixture.owner()?;
    let cluster = ClusterId::new("docker-e2e")?;
    let local = NodeKey {
        node_id: "gateway".to_owned(),
        address: NodeAddress::new("aaa-client", 25531)?,
        incarnation: NodeIncarnation::generate(),
    };
    let entity_config = fixture_entity_config()?;
    if entity_config.fingerprint() != fixture.reference.config_fingerprint() {
        return Err("entity fixture configuration mismatch".into());
    }
    let slot = fixture_entity_slot(&entity_config, fixture.reference.entity_id(), owner.clone())?;
    let EntityServiceFixture {
        service,
        control,
        coordinator,
        member,
    } = entity_service(cluster.clone(), local, entity_config, &slot, false)?;
    service.start().await?;
    install_fixture_snapshot(&control, &coordinator, &slot, member, false).await?;
    wait_for_node_ready(&service).await?;
    let owner_identity = NodeIdentity {
        cluster_id: cluster,
        node_id: owner.node_id.clone(),
        address: owner.address.clone(),
        incarnation: owner.incarnation,
    };
    if !service
        .associations()
        .should_dial(&owner_identity.address, owner_identity.incarnation)
    {
        return Err("gateway identity must be the stable association dialer".into());
    }
    service.connect_peer(owner_identity).await?;
    let entity_type = fixture.reference.entity_type().as_str().to_owned();
    let reply = service
        .ask(&fixture.reference, Ping(41), Duration::from_secs(10))
        .await?;
    if reply != Pong(42) {
        return Err("unexpected gateway EntityRef reply".into());
    }
    std::fs::write(
        "/artifacts/admin-snapshot.json",
        serde_json::to_vec_pretty(&serde_json::json!({
            "partial": false,
            "node_lifecycle": format!("{:?}", service.node_lifecycle_state()),
            "associations": service.associations().len(),
            "entity_type": entity_type,
            "authorized_owner_incarnation": owner.incarnation.get().to_string(),
        }))?,
    )?;
    std::fs::write(
        "/artifacts/gateway-entity-multiprocess.json",
        b"{\"reply\":42,\"authorized_owner\":true}\n",
    )?;
    service.force_shutdown().await?;
    Ok(())
}

fn entity_service(
    cluster: ClusterId,
    node: NodeKey,
    entity_config: EntityConfig,
    slot: &PlacementSlot,
    owns_slot: bool,
) -> Result<EntityServiceFixture, Box<dyn std::error::Error>> {
    let mut context = ServiceContext::builder(
        ServiceKind::from_static("distributed-entity-fixture"),
        InstanceId::new(node.node_id.clone()),
    );
    context.insert_extension(ActivationDirectory::new(64)?)?;
    let protocol = Arc::new(FixtureProtocol::bind::<PingActor>()?);
    let registry = Arc::new(ActorRegistry::new_bound(
        actor_kind!("DistributedEntityFixture"),
        ActorRegistryConfig {
            actor_ref: Some(ActorRefConfig {
                cluster_id: cluster.clone(),
                node_address: node.address.clone(),
                node_incarnation: node.incarnation,
            }),
            service: context.build(),
            ..ActorRegistryConfig::default()
        },
        protocol.as_ref(),
    ));
    let mut builder = LatticeServiceBuilder::new(node_config(
        cluster.clone(),
        &node.node_id,
        node.address.clone(),
        node.incarnation,
    ))?;
    let associations = builder.association_manager();
    let messaging = builder.outbound_messaging();
    let coordinator_incarnation = NodeIncarnation::new(999)?;
    let coordinator_address = NodeAddress::new("coordinator-fixture", 25999)?;
    let coordinator_association = associations.get_or_create(
        cluster.clone(),
        coordinator_address.clone(),
        coordinator_incarnation,
    )?;
    let coordinator = AssociationKey {
        cluster_id: cluster,
        local_incarnation: node.incarnation,
        remote_address: coordinator_address,
        remote_incarnation: coordinator_incarnation,
    };
    for (lane, nonce) in [
        (LaneKind::Control, 1),
        (LaneKind::Interactive, 2),
        (LaneKind::Bulk(0), 3),
    ] {
        coordinator_association.attach(LaneAttachment {
            association_id: coordinator_association.id(),
            key: coordinator.clone(),
            lane,
            connection_nonce: nonce,
        })?;
    }
    let member_hello = MemberHello {
        node: node.clone(),
        roles: BTreeSet::from([if owns_slot { "entity" } else { "gateway" }.to_owned()]),
        failure_domains: BTreeMap::new(),
        protocols: Vec::new(),
        remoting_capabilities: BTreeSet::new(),
    };
    let member = MemberRecord {
        node: node.clone(),
        hello: member_hello,
        status: MemberStatus::Up,
        version: MembershipVersion::new(slot.version.term, slot.version.revision),
        lease_id: 1,
    };
    let domain_hello = PlacementDomainHello::new(
        node.clone(),
        placement_domain(),
        1,
        if owns_slot {
            BTreeSet::from([entity_config.entity_type.clone()])
        } else {
            BTreeSet::new()
        },
        if owns_slot {
            BTreeSet::new()
        } else {
            BTreeSet::from([entity_config.entity_type.clone()])
        },
        BTreeSet::new(),
        BTreeSet::new(),
        Vec::new(),
        Vec::new(),
        BTreeMap::new(),
    );
    let (logic, effects) = PlacementDomainSession::new(
        domain_hello,
        coordinator.clone(),
        associations.clone(),
        LogicCoordinatorConfig::default(),
        64,
    )?;
    if owns_slot {
        logic.register_authority(slot.key.clone(), Duration::from_secs(2))?;
    }
    let state = logic.state();
    let (control, controls) = PlacementControlRouter::bounded(64, DEFAULT_MAX_CONTROL_PAYLOAD)?;
    let control = Arc::new(control);
    let mut router = DomainLogicalRouter::new(
        node,
        state,
        associations,
        messaging,
        coordinator.clone(),
        LogicalBufferConfig::default(),
        8,
    )?;
    router.register_entity(
        entity_config,
        registry.clone(),
        protocol.clone(),
        PingLoader,
    )?;
    let router = Arc::new(router);
    builder = builder
        .register_actor(registry, protocol)?
        .cluster_logic_runtime(router, control.clone(), logic, controls, effects);
    Ok(EntityServiceFixture {
        service: builder.build()?,
        control,
        coordinator,
        member,
    })
}

async fn install_fixture_snapshot(
    control: &PlacementControlRouter,
    coordinator: &AssociationKey,
    slot: &PlacementSlot,
    member: MemberRecord,
    owns_slot: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let limits = SnapshotLimits::default();
    let record = SnapshotRecord {
        key: match &slot.key {
            PlacementSlotKey::Shard {
                domain,
                entity_type,
                shard_id,
            } => format!(
                "domain/{}/shard/{}/{}",
                domain.as_str(),
                entity_type.as_str(),
                shard_id.get()
            ),
            PlacementSlotKey::Singleton { domain, kind } => {
                format!("domain/{}/singleton/{}", domain.as_str(), kind.as_str())
            }
        },
        value: serde_json::to_vec(slot)?.into(),
    };
    let (begin, chunks, end) = build_snapshot(
        SnapshotVersion::Placement(slot.version.clone()),
        vec![record],
        &limits,
    )?;
    let mut commands = vec![PlacementControlCommand::SnapshotBegin(begin)];
    commands.extend(
        chunks
            .into_iter()
            .map(PlacementControlCommand::SnapshotChunk),
    );
    commands.push(PlacementControlCommand::SnapshotEnd(end));
    commands.push(PlacementControlCommand::MemberUp(member));
    if owns_slot {
        commands.push(PlacementControlCommand::ClaimGranted(ClaimGrant {
            domain: slot.key.domain().clone(),
            slot: slot.key.clone(),
            owner: slot.owner.clone().ok_or("fixture slot has no owner")?,
            coordinator_term: slot.version.term,
            assignment_generation: slot.assignment_generation,
            grant_sequence: GrantSequence::new(1)?,
            ttl: Duration::from_secs(300),
        }));
    }
    for command in commands {
        control
            .apply(
                coordinator.clone(),
                CommandId::generate(),
                encode_control_command(
                    &CoordinatorScope::Placement(slot.key.domain().clone()),
                    &command,
                    DEFAULT_MAX_CONTROL_PAYLOAD,
                )?,
            )
            .await?;
    }
    Ok(())
}

async fn wait_for_node_ready(service: &LatticeService) -> Result<(), Box<dyn std::error::Error>> {
    let mut lifecycle = service.subscribe_node_lifecycle();
    tokio::time::timeout(Duration::from_secs(10), async {
        while *lifecycle.borrow() != NodeLifecycleState::Ready {
            lifecycle.changed().await.map_err(|_| "lifecycle closed")?;
        }
        Ok::<(), &'static str>(())
    })
    .await??;
    Ok(())
}

include!("distributed_node/helpers.rs");
include!("distributed_node/domain_cluster.rs");
