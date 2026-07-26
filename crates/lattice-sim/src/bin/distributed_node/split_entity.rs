use std::sync::atomic::{AtomicU64, Ordering};

use lattice_actor::{error::ActorStopError, recipient::RecipientError, traits::StopReason};

const SPLIT_PROTOCOL_ID: u64 = 0x7369_6d00_0000_0002;
const SPLIT_ENTITY_ID: &[u8] = b"split-brain-entity";

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

#[derive(Debug, Serialize)]
struct ActivationEvent<'a> {
    event: &'a str,
    unix_millis: u128,
    activation: &'a ActivationIdentity,
}

#[derive(Debug, Serialize)]
struct ProbeEvent<'a> {
    sequence: u64,
    unix_millis: u128,
    outcome: &'a str,
    served_by: Option<&'a ActivationIdentity>,
    error: Option<String>,
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
                && member.hello.protocols.iter().any(|descriptor| {
                    descriptor.protocol_id.get() == SPLIT_PROTOCOL_ID
                })
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

/// Hosts one placement-managed entity and continuously probes it through the cluster router so
/// every side of a partition keeps an independent, timestamped record of which activation served
/// it. The probe outcome is the single-activation oracle: a rejected probe proves the local
/// authority is fenced, and a served probe names the activation that answered.
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
    let journal = directory.join(format!("{node_id}-activations.jsonl"));
    let probes = directory.join(format!("{node_id}-probes.jsonl"));
    let cluster = ClusterId::new("docker-domain-e2e")?;
    let incarnation = NodeIncarnation::generate();
    let address = NodeAddress::new(node_id.clone(), port)?;
    let config = split_entity_config(&domain)?;
    let placement_domain = config.domain.clone();
    let protocol = Arc::new(SplitProtocol::bind::<SplitEntityActor>()?);
    let mut context = ServiceContext::builder(
        ServiceKind::from_static("distributed-split-fixture"),
        InstanceId::new(node_id.clone()),
    );
    context.insert_extension(ActivationDirectory::new(8)?)?;
    let registry = Arc::new(ActorRegistry::new_bound(
        actor_kind!("DistributedSplitFixture"),
        ActorRegistryConfig {
            actor_ref: Some(ActorRefConfig {
                cluster_id: cluster.clone(),
                node_address: address.clone(),
                node_incarnation: incarnation,
            }),
            service: context.build(),
            ..ActorRegistryConfig::default()
        },
        protocol.as_ref(),
    ));
    let service =
        LatticeService::builder(node_config(cluster.clone(), &node_id, address, incarnation))?
            .host_entity_with_registry(
                config.clone(),
                registry,
                protocol,
                SplitEntityLoader {
                    node_id: node_id.clone(),
                    incarnation,
                    activations: Arc::new(AtomicU64::new(0)),
                    journal: journal.clone(),
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
        config.entity_ref::<SplitProtocol>(cluster, EntityId::new(SPLIT_ENTITY_ID.to_vec())?)?;
    let health = service.subscribe_health();
    let mut counters = SplitProbeCounters::default();
    let mut sequence = 0_u64;
    let mut ticker = tokio::time::interval(Duration::from_millis(200));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal?;
                break;
            }
            _ = ticker.tick() => {
                connect_split_peers(&service, &node_id).await;
                sequence = sequence.saturating_add(1);
                let reply = service
                    .ask(&reference, SplitProbe { sequence }, Duration::from_millis(1_500))
                    .await;
                counters.record(&probes, sequence, reply)?;
                write_atomic(
                    artifact.clone(),
                    &serde_json::to_vec_pretty(&SplitHostArtifact {
                        node_id: node_id.clone(),
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
                    })?,
                )?;
            }
        }
    }
    service.shutdown().await?;
    Ok(())
}

#[derive(Default)]
struct SplitProbeCounters {
    probes: u64,
    served: u64,
    rejected: u64,
    last_outcome: String,
    last_served_by: Option<ActivationIdentity>,
}

impl SplitProbeCounters {
    fn record(
        &mut self,
        journal: &Path,
        sequence: u64,
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
