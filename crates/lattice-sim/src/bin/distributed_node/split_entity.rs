use std::sync::atomic::{AtomicU64, Ordering};

use lattice_actor::{error::ActorStopError, recipient::RecipientError, traits::StopReason};

const SPLIT_PROTOCOL_ID: u64 = 0x7369_6d00_0000_0002;
const SPLIT_ENTITY_ID: &[u8] = b"split-brain-entity";
/// Where a scenario writes the release this host must be running. The file is rewritten while the
/// fixture is live, which is how one binary presents two releases to the cluster without a second
/// image: the admission guard compares release manifests, never binaries.
const RELEASE_FILE_ENV: &str = "LATTICE_RELEASE_FILE";
/// The release to run when no file has been written. Zero means "stay out of the cluster", which is
/// what keeps a spare host inert until a scenario asks it to join.
const RELEASE_ID_ENV: &str = "LATTICE_RELEASE_ID";
const RELEASE_POLL_INTERVAL: Duration = Duration::from_millis(200);
/// How long a host that could not start waits before trying the same release again. A release the
/// framework refuses stays refused, but a port that has not been released yet will not.
const STARTUP_RETRY_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Serialize, Deserialize, lattice_actor::Request)]
#[request(response = SplitProbeReply)]
struct SplitProbe {
    sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SplitProbeReply {
    sequence: u64,
    activation: ActivationIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct ActivationIdentity {
    node_id: String,
    incarnation: String,
    activation: u64,
}

/// The release identity a split host boots with.
///
/// A code-only rolling upgrade is exactly a change of `release_id`: the compatibility contract is
/// what the cluster guard compares between members, so the remaining fields exist only to build the
/// releases the guard has to refuse.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReleaseIdentity {
    release_id: u64,
    /// Filler byte for the actor protocol fingerprint. Any value changes the compatibility
    /// contract, which is a break that demands a full deployment restart rather than a rolling one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    protocol_fingerprint: Option<u8>,
    /// The control plane generation this release claims. A claim that does not match the framework
    /// the node is linked against must be refused by the node itself, before any Coordinator sees
    /// it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    control_generation: Option<u64>,
}

impl ReleaseIdentity {
    fn manifest(&self) -> Result<lattice_core::release::ReleaseManifest, String> {
        let mut compatibility = lattice_core::release::ReleaseCompatibility::development();
        if let Some(byte) = self.protocol_fingerprint {
            compatibility.actor_protocol_fingerprint = [byte; 32];
        }
        if let Some(generation) = self.control_generation {
            compatibility.control_generation = generation;
        }
        let release_id = lattice_core::release::ReleaseId::new(self.release_id)
            .ok_or_else(|| "release id must be nonzero".to_owned())?;
        lattice_core::release::ReleaseManifest::new(release_id, compatibility)
            .map_err(|error| error.to_string())
    }
}

/// Resolves the release this host should be running now.
///
/// The file wins over the environment so a scenario can move a running host onto another release,
/// and a file that cannot be read leaves the running identity in place: a torn read must never
/// invent a release nobody asked for.
fn resolve_release_identity(
    source: Option<&Path>,
    running: Option<&ReleaseIdentity>,
) -> Option<ReleaseIdentity> {
    if let Some(path) = source {
        match std::fs::read(path) {
            Ok(encoded) => {
                return serde_json::from_slice(&encoded)
                    .ok()
                    .or_else(|| running.cloned());
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(_) => return running.cloned(),
        }
    }
    let release_id = std::env::var(RELEASE_ID_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(1);
    (release_id != 0).then_some(ReleaseIdentity {
        release_id,
        protocol_fingerprint: None,
        control_generation: None,
    })
}

#[derive(Debug, Serialize)]
struct ActivationEvent<'a> {
    event: &'a str,
    unix_millis: u128,
    activation: &'a ActivationIdentity,
}

#[derive(Debug, Serialize)]
struct ProbeEvent<'a> {
    sequence: u64,
    requested_unix_millis: u128,
    unix_millis: u128,
    outcome: &'a str,
    served_by: Option<&'a ActivationIdentity>,
    error: Option<String>,
}

/// The release composition this host currently observes across the live, lease-backed members. It
/// is derived from the same member manifests the Coordinator admits against, so a state that has no
/// legal reading is recorded rather than hidden: only a broken guard can produce one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum ClusterReleaseArtifact {
    /// This host is not in the cluster, so it observes nothing about the cluster's releases.
    Absent,
    Empty,
    Stable {
        release_id: u64,
    },
    Rolling {
        from: u64,
        to: u64,
    },
    Invalid {
        error: String,
    },
}

impl ClusterReleaseArtifact {
    fn observe(service: &LatticeService) -> Self {
        match service.cluster_release_state() {
            Ok(lattice_core::release::ClusterReleaseState::Empty) => Self::Empty,
            Ok(lattice_core::release::ClusterReleaseState::Stable { release }) => Self::Stable {
                release_id: release.release_id.get(),
            },
            Ok(lattice_core::release::ClusterReleaseState::Rolling { from, to }) => Self::Rolling {
                from: from.release_id.get(),
                to: to.release_id.get(),
            },
            Err(error) => Self::Invalid {
                error: error.to_string(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
struct RolloutMemberArtifact {
    node_id: String,
    status: String,
    release_id: u64,
}

/// Why a host is not running a release. `kind` is what a scenario asserts on; `detail` keeps the
/// framework's own rendering of the refusal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct StartupErrorArtifact {
    kind: &'static str,
    detail: String,
}

#[derive(Debug, Serialize)]
struct SplitHostArtifact {
    node_id: String,
    #[serde(with = "lattice_sim::serde_u128")]
    incarnation: u128,
    unix_millis: u128,
    lifecycle: String,
    domain: String,
    domain_state: String,
    probes: u64,
    served: u64,
    rejected: u64,
    last_outcome: String,
    last_served_by: Option<ActivationIdentity>,
    release: Option<ReleaseIdentity>,
    cluster_release: ClusterReleaseArtifact,
    rollout_members: Vec<RolloutMemberArtifact>,
    #[serde(skip_serializing_if = "Option::is_none")]
    startup_error: Option<StartupErrorArtifact>,
}

#[derive(Clone, Copy)]
struct SplitProbeCodec;

#[derive(Clone, Copy)]
struct SplitProbeReplyCodec;

impl WireCodec<SplitProbe> for SplitProbeCodec {
    const DESCRIPTOR: CodecDescriptor = CodecDescriptor::new(2, 1);

    fn encode(&self, value: &SplitProbe, output: &mut BytesMut) -> Result<(), EncodeError> {
        output.extend_from_slice(
            &serde_json::to_vec(value).map_err(|error| EncodeError::new(error.to_string()))?,
        );
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<SplitProbe, DecodeError> {
        serde_json::from_slice(input).map_err(|error| DecodeError::new(error.to_string()))
    }
}

impl WireCodec<SplitProbeReply> for SplitProbeReplyCodec {
    const DESCRIPTOR: CodecDescriptor = CodecDescriptor::new(2, 1);

    fn encode(&self, value: &SplitProbeReply, output: &mut BytesMut) -> Result<(), EncodeError> {
        output.extend_from_slice(
            &serde_json::to_vec(value).map_err(|error| EncodeError::new(error.to_string()))?,
        );
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<SplitProbeReply, DecodeError> {
        serde_json::from_slice(input).map_err(|error| DecodeError::new(error.to_string()))
    }
}

actor_protocol! {
    SplitProtocol {
        protocol_id: SPLIT_PROTOCOL_ID;
        name: "distributed-fixture/split-brain/v1";
        ask 1 => SplitProbe {
            request_schema_version: 1,
            response_schema_version: 1,
            request_codec: SplitProbeCodec,
            response_codec: SplitProbeReplyCodec,
        }
    }
}

struct SplitEntityActor {
    identity: ActivationIdentity,
    journal: PathBuf,
}

#[derive(Clone)]
struct SplitEntityLoader {
    node_id: String,
    incarnation: NodeIncarnation,
    activations: Arc<AtomicU64>,
    journal: PathBuf,
}

#[async_trait]
impl ActorLoader<SplitEntityActor> for SplitEntityLoader {
    async fn load(&self, _context: ActorCreateContext) -> Result<SplitEntityActor, ActorError> {
        Ok(SplitEntityActor {
            identity: ActivationIdentity {
                node_id: self.node_id.clone(),
                incarnation: self.incarnation.get().to_string(),
                activation: self.activations.fetch_add(1, Ordering::SeqCst) + 1,
            },
            journal: self.journal.clone(),
        })
    }
}

impl Actor for SplitEntityActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;

    async fn started(&mut self, _context: &mut ActorContext<Self>) -> Result<(), Self::Error> {
        append_activation_event(&self.journal, "activated", &self.identity)
            .map_err(ActorError::from_error)
    }

    async fn stopping(
        &mut self,
        _context: &mut ActorContext<Self>,
        _reason: StopReason,
    ) -> Result<(), ActorStopError> {
        let _ = append_activation_event(&self.journal, "deactivated", &self.identity);
        Ok(())
    }
}

impl Responder<SplitProbe> for SplitEntityActor {
    async fn respond(
        &mut self,
        _context: &mut HandlerContext<'_, Self>,
        request: SplitProbe,
        reply_to: ReplyTo<SplitProbeReply>,
    ) -> Result<(), ActorError> {
        let _ = reply_to.send(SplitProbeReply {
            sequence: request.sequence,
            activation: self.identity.clone(),
        });
        Ok(())
    }
}

fn unix_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .unwrap_or_default()
}

fn append_line(path: &Path, line: &[u8]) -> Result<(), IoError> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    let mut record = line.to_vec();
    record.push(b'\n');
    std::io::Write::write_all(&mut file, &record)
}

fn append_activation_event(
    journal: &Path,
    event: &str,
    activation: &ActivationIdentity,
) -> Result<(), IoError> {
    append_line(
        journal,
        &serde_json::to_vec(&ActivationEvent {
            event,
            unix_millis: unix_millis(),
            activation,
        })
        .map_err(IoError::other)?,
    )
}

fn split_entity_config(domain: &str) -> Result<EntityConfig, Box<dyn Error>> {
    Ok(EntityConfig::new(
        distributed_domain(domain)?,
        EntityType::new("split-brain-entity")?,
        ProtocolId::new(SPLIT_PROTOCOL_ID)?,
        1,
        "weighted-least-load",
        1,
        Vec::new(),
    )?)
}

/// Member associations are dialled explicitly, so a proxying node can only reach the current owner
/// once it has one. Reconciling before every probe keeps a healed node reachable again without
/// waiting for traffic to fail first.
async fn connect_split_peers(service: &LatticeService, node_id: &str) {
    let peers = service
        .member_snapshot()
        .members
        .into_iter()
        .filter(|member| {
            member.status == MemberStatus::Up
                && member.node.node_id != node_id
                && member
                    .hello
                    .protocols
                    .iter()
                    .any(|descriptor| descriptor.protocol_id.get() == SPLIT_PROTOCOL_ID)
        })
        .collect::<Vec<_>>();
    for peer in peers {
        let _ = tokio::time::timeout(
            Duration::from_millis(250),
            service.connect_member(&peer.node),
        )
        .await;
    }
}

fn rollout_members(service: &LatticeService) -> Vec<RolloutMemberArtifact> {
    let mut members = service
        .member_snapshot()
        .members
        .into_iter()
        .filter(|member| member.hello.rollout_participant)
        .map(|member| RolloutMemberArtifact {
            node_id: member.node.node_id.clone(),
            status: format!("{:?}", member.status),
            release_id: member.hello.release.release_id.get(),
        })
        .collect::<Vec<_>>();
    members.sort();
    members
}

/// What ended a host generation.
enum HostGeneration {
    /// The fixture was asked to leave for good.
    Stopped,
    /// The scenario moved this host onto another release, which is the upgrade itself: the host
    /// hands its slots back, leaves, and rejoins under the manifest it was given.
    ReleaseChanged,
}

/// Everything one split host keeps across the releases it runs. Probe sequence numbers and the
/// journals survive an upgrade on purpose: a rolling upgrade that loses a request must be visible
/// as a gap rather than as a fresh count.
struct SplitHost {
    artifact: PathBuf,
    node_id: String,
    port: u16,
    domain: String,
    journal: PathBuf,
    probes: PathBuf,
    release_source: Option<PathBuf>,
}

/// Hosts one placement-managed entity and continuously probes it through the cluster router so
/// every side of a partition keeps an independent, timestamped record of which activation served
/// it. The probe outcome is the single-activation oracle: a rejected probe proves the local
/// authority is fenced, and a served probe names the activation that answered.
///
/// The host runs one release at a time and swaps to another when a scenario rewrites its release
/// file, tearing the service down and rejoining under the new manifest. The process itself never
/// exits for a release change: an exited container aborts the whole compose run, and with it every
/// scenario that had not run yet.
async fn split_entity_host(
    artifact: PathBuf,
    node_id: String,
    port: u16,
    domain: String,
) -> Result<(), Box<dyn Error>> {
    let directory = artifact
        .parent()
        .ok_or_else(|| IoError::other("split host artifact has no parent directory"))?
        .to_path_buf();
    std::fs::create_dir_all(&directory)?;
    let host = SplitHost {
        journal: directory.join(format!("{node_id}-activations.jsonl")),
        probes: directory.join(format!("{node_id}-probes.jsonl")),
        release_source: std::env::var_os(RELEASE_FILE_ENV).map(PathBuf::from),
        artifact,
        node_id,
        port,
        domain,
    };
    // The interrupt is subscribed to once and latched into a flag every loop polls. A listener
    // built per generation would register a fresh one each time, and an interrupt that arrives
    // while a generation is being rebuilt would be delivered to nobody and never replayed: the host
    // would keep serving as if it had never been asked to leave.
    let stopping = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let latch = stopping.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            latch.store(true, Ordering::SeqCst);
        }
    });
    let mut counters = SplitProbeCounters::default();
    let mut running: Option<ReleaseIdentity> = None;
    loop {
        let desired = resolve_release_identity(host.release_source.as_deref(), running.as_ref());
        running = desired.clone();
        let Some(identity) = desired else {
            if host.idle(None, &counters, &stopping).await? {
                break;
            }
            continue;
        };
        match host.run_release(&identity, &mut counters, &stopping).await {
            Ok(HostGeneration::Stopped) => break,
            Ok(HostGeneration::ReleaseChanged) => {}
            Err(failure) => {
                if host
                    .idle(Some((&identity, &failure)), &counters, &stopping)
                    .await?
                {
                    break;
                }
            }
        }
    }
    Ok(())
}

impl SplitHost {
    /// Publishes what this host is doing while it is deliberately not in the cluster, and returns
    /// once the fixture has been asked to stop (`true`) or the scenario has asked for a different
    /// release (`false`). A refused release is republished the whole time it stands, so a scenario
    /// reads the refusal from an artifact rather than from a log line.
    async fn idle(
        &self,
        failed: Option<(&ReleaseIdentity, &StartupErrorArtifact)>,
        counters: &SplitProbeCounters,
        stopping: &std::sync::atomic::AtomicBool,
    ) -> Result<bool, Box<dyn Error>> {
        let running = failed.map(|(identity, _)| identity);
        let deadline = Instant::now() + STARTUP_RETRY_INTERVAL;
        loop {
            if stopping.load(Ordering::SeqCst) {
                return Ok(true);
            }
            write_atomic(
                self.artifact.clone(),
                &serde_json::to_vec_pretty(&SplitHostArtifact {
                    node_id: self.node_id.clone(),
                    incarnation: 0,
                    unix_millis: unix_millis(),
                    lifecycle: match failed {
                        Some(_) => "StartupFailed".to_owned(),
                        None => "Absent".to_owned(),
                    },
                    domain: self.domain.clone(),
                    domain_state: "Absent".to_owned(),
                    probes: counters.probes,
                    served: counters.served,
                    rejected: counters.rejected,
                    last_outcome: counters.last_outcome.clone(),
                    last_served_by: counters.last_served_by.clone(),
                    release: running.cloned(),
                    cluster_release: ClusterReleaseArtifact::Absent,
                    rollout_members: Vec::new(),
                    startup_error: failed.map(|(_, error)| error.clone()),
                })?,
            )?;
            if resolve_release_identity(self.release_source.as_deref(), running).as_ref() != running
            {
                return Ok(false);
            }
            // A release the framework refuses stays refused however long it is retried, but a
            // resource the previous generation had not released yet will not, so a failed start is
            // retried rather than parked until the scenario intervenes.
            if failed.is_some() && Instant::now() >= deadline {
                return Ok(false);
            }
            tokio::time::sleep(RELEASE_POLL_INTERVAL).await;
        }
    }

    /// Runs one release: joins the cluster under `identity`, keeps the entity probed, and hands
    /// everything back when the scenario names another release.
    async fn run_release(
        &self,
        identity: &ReleaseIdentity,
        counters: &mut SplitProbeCounters,
        stopping: &std::sync::atomic::AtomicBool,
    ) -> Result<HostGeneration, StartupErrorArtifact> {
        let mut config = node_config(
            ClusterId::new("docker-domain-e2e").map_err(StartupErrorArtifact::from_error)?,
            &self.node_id,
            NodeAddress::new(self.node_id.clone(), self.port)
                .map_err(StartupErrorArtifact::from_error)?,
            NodeIncarnation::generate(),
        );
        config.release = identity.manifest().map_err(StartupErrorArtifact::release)?;
        // A node must refuse a release whose framework generations are not the ones it is linked
        // against, and it must do so before a Coordinator is ever involved.
        if let Err(error) = config.validate() {
            return Err(match error {
                lattice_service::config::NodeConfigError::InvalidRelease
                | lattice_service::config::NodeConfigError::ReleaseGenerationMismatch => {
                    StartupErrorArtifact::release(error.to_string())
                }
                other => StartupErrorArtifact::from_error(other),
            });
        }
        self.serve_release(config, identity, counters, stopping)
            .await
            .map_err(StartupErrorArtifact::from_error)
    }

    async fn serve_release(
        &self,
        config: NodeConfig,
        identity: &ReleaseIdentity,
        counters: &mut SplitProbeCounters,
        stopping: &std::sync::atomic::AtomicBool,
    ) -> Result<HostGeneration, Box<dyn Error>> {
        let cluster = config.cluster_id.clone();
        let incarnation = config.incarnation;
        let entity = split_entity_config(&self.domain)?;
        let placement_domain = entity.domain.clone();
        let protocol = Arc::new(SplitProtocol::bind::<SplitEntityActor>()?);
        let mut context = ServiceContext::builder(
            ServiceKind::from_static("distributed-split-fixture"),
            InstanceId::new(self.node_id.clone()),
        );
        context.insert_extension(ActivationDirectory::new(8)?)?;
        let registry = Arc::new(ActorRegistry::new_bound(
            actor_kind!("DistributedSplitFixture"),
            ActorRegistryConfig {
                actor_ref: Some(ActorRefConfig {
                    cluster_id: cluster.clone(),
                    node_address: config.address.clone(),
                    node_incarnation: incarnation,
                }),
                service: context.build(),
                ..ActorRegistryConfig::default()
            },
            protocol.as_ref(),
        ));
        let service = LatticeService::builder(config)?
            .host_entity_with_registry(
                entity.clone(),
                registry,
                protocol,
                SplitEntityLoader {
                    node_id: self.node_id.clone(),
                    incarnation,
                    activations: Arc::new(AtomicU64::new(0)),
                    journal: self.journal.clone(),
                },
            )?
            .domain_capacity(placement_domain.clone(), 1)?
            .coordinator_discovery(domain_static_discovery(
                CoordinatorScope::Membership,
                "membership",
                &[
                    ("domain-membership", 29300),
                    ("domain-alpha", 29301),
                    ("domain-beta", 29302),
                    ("domain-gamma", 29303),
                    ("domain-standby", 29304),
                ],
            )?)?
            .coordinator_discovery(domain_static_discovery(
                CoordinatorScope::Placement(placement_domain.clone()),
                "split",
                &[("domain-standby", 29304)],
            )?)?
            .join_config(ClusterJoinConfig {
                retry_initial: Duration::from_millis(25),
                retry_max: Duration::from_millis(250),
                leadership_refresh_interval: Duration::from_secs(1),
                discovery_stale_grace: Duration::from_secs(5),
                join_timeout: Some(Duration::from_secs(240)),
                ..ClusterJoinConfig::default()
            })
            .build()?;
        service.start().await?;
        let reference =
            entity.entity_ref::<SplitProtocol>(cluster, EntityId::new(SPLIT_ENTITY_ID.to_vec())?)?;
        let health = service.subscribe_health();
        let mut ticker = tokio::time::interval(RELEASE_POLL_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let outcome = loop {
            ticker.tick().await;
            if stopping.load(Ordering::SeqCst) {
                break HostGeneration::Stopped;
            }
            if resolve_release_identity(self.release_source.as_deref(), Some(identity)).as_ref()
                != Some(identity)
            {
                break HostGeneration::ReleaseChanged;
            }
            connect_split_peers(&service, &self.node_id).await;
            let requested = unix_millis();
            let sequence = counters.next_sequence();
            let reply = service
                .ask(
                    &reference,
                    SplitProbe { sequence },
                    Duration::from_millis(1_500),
                )
                .await;
            counters.record(&self.probes, sequence, requested, reply)?;
            write_atomic(
                self.artifact.clone(),
                &serde_json::to_vec_pretty(&SplitHostArtifact {
                    node_id: self.node_id.clone(),
                    incarnation: incarnation.get(),
                    unix_millis: unix_millis(),
                    lifecycle: format!("{:?}", service.node_lifecycle_state()),
                    domain: placement_domain.as_str().to_owned(),
                    domain_state: health
                        .borrow()
                        .domains
                        .get(&placement_domain)
                        .map_or_else(|| "Absent".to_owned(), |state| format!("{state:?}")),
                    probes: counters.probes,
                    served: counters.served,
                    rejected: counters.rejected,
                    last_outcome: counters.last_outcome.clone(),
                    last_served_by: counters.last_served_by.clone(),
                    release: Some(identity.clone()),
                    cluster_release: ClusterReleaseArtifact::observe(&service),
                    rollout_members: rollout_members(&service),
                    startup_error: None,
                })?,
            )?;
        };
        // A host that never joined has no slots to hand over and cannot complete a drain, but it
        // still holds the port the next release needs, so it is stopped either way.
        if service.shutdown().await.is_err() {
            service.terminal_shutdown().await?;
        }
        Ok(outcome)
    }
}

impl StartupErrorArtifact {
    /// The node refused the release it was given, on its own, before joining anything.
    fn release(detail: impl Into<String>) -> Self {
        Self {
            kind: "release",
            detail: detail.into(),
        }
    }

    fn from_error(error: impl std::fmt::Display) -> Self {
        Self {
            kind: "startup",
            detail: error.to_string(),
        }
    }
}

#[derive(Default)]
struct SplitProbeCounters {
    sequence: u64,
    probes: u64,
    served: u64,
    rejected: u64,
    last_outcome: String,
    last_served_by: Option<ActivationIdentity>,
}

impl SplitProbeCounters {
    /// Sequence numbers continue across a release change, so a probe lost while a host swaps
    /// releases shows up as a hole in the journal rather than as a second run of numbers.
    fn next_sequence(&mut self) -> u64 {
        self.sequence = self.sequence.saturating_add(1);
        self.sequence
    }

    /// `requested` is taken before the request is issued. A frozen or descheduled process can only
    /// be told apart from one that answered late by knowing when the request was admitted, so both
    /// ends of the round trip are recorded.
    fn record(
        &mut self,
        journal: &Path,
        sequence: u64,
        requested: u128,
        reply: Result<SplitProbeReply, RecipientError>,
    ) -> Result<(), Box<dyn Error>> {
        self.probes = self.probes.saturating_add(1);
        let event = match &reply {
            Ok(reply) => {
                self.served = self.served.saturating_add(1);
                self.last_outcome = "served".to_owned();
                self.last_served_by = Some(reply.activation.clone());
                ProbeEvent {
                    sequence,
                    requested_unix_millis: requested,
                    unix_millis: unix_millis(),
                    outcome: "served",
                    served_by: Some(&reply.activation),
                    error: None,
                }
            }
            Err(error) => {
                self.rejected = self.rejected.saturating_add(1);
                self.last_outcome = "rejected".to_owned();
                ProbeEvent {
                    sequence,
                    requested_unix_millis: requested,
                    unix_millis: unix_millis(),
                    outcome: "rejected",
                    served_by: None,
                    error: Some(error.to_string()),
                }
            }
        };
        append_line(journal, &serde_json::to_vec(&event)?)?;
        Ok(())
    }
}
