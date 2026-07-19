use lattice_placement::{
    coordinator::LeaderRecord, runtime::membership_plane::MembershipLeaderConfig,
};
use lattice_service::lifecycle::{PlacementDomainState, ServiceHealthSnapshot};
fn distributed_domain(name: &str) -> Result<PlacementDomainId, Box<dyn Error>> {
    Ok(PlacementDomainId::new(format!("domain-{name}"))?)
}

fn parse_distributed_domains(value: &str) -> Result<BTreeSet<PlacementDomainId>, Box<dyn Error>> {
    value
        .split(',')
        .filter(|value| !value.is_empty())
        .map(distributed_domain)
        .collect()
}

async fn domain_host(
    artifact: PathBuf,
    node_id: String,
    port: u16,
    domains: String,
) -> Result<(), Box<dyn Error>> {
    let endpoints = std::env::var("LATTICE_ETCD_ENDPOINTS")?
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let run_id = std::env::var("LATTICE_RUN_ID")?;
    let store = Arc::new(
        EtcdPlacementStore::connect(EtcdPlacementConfig {
            endpoints,
            cluster_prefix: format!("/lattice-domain-e2e/{run_id}"),
            list_page_size: 64,
            limits: DurableStorageLimits {
                maximum_slots: 1_024,
                maximum_plans: 256,
                maximum_members: 128,
                maximum_admin_operations: 256,
                maximum_entity_configs: 128,
                maximum_singleton_configs: 128,
            },
            connect_options: None,
        })
        .await?,
    );
    let cluster = ClusterId::new("docker-domain-e2e")?;
    let incarnation = NodeIncarnation::generate();
    let address = NodeAddress::new(node_id.clone(), port)?;
    let builder =
        LatticeService::builder(node_config(cluster, &node_id, address.clone(), incarnation))?;
    let host = CoordinatorHost::elect(
        store,
        builder.association_manager(),
        NodeKey {
            node_id: node_id.clone(),
            address,
            incarnation,
        },
        parse_distributed_domains(&domains)?,
        CoordinatorHostConfig {
            membership: MembershipLeaderConfig {
                leader_lease_ttl: Duration::from_secs(10),
                renewal_interval: Duration::from_secs(1),
                ..MembershipLeaderConfig::default()
            },
            placement: PlacementDomainLeaderConfig {
                leader_lease_ttl: Duration::from_secs(10),
                renewal_interval: Duration::from_secs(1),
                ..PlacementDomainLeaderConfig::default()
            },
            renewal_interval: Duration::from_millis(500),
            maximum_candidate_jitter: Duration::from_millis(50),
            ..CoordinatorHostConfig::default()
        },
    )
    .await?;
    let mut directory = host.subscribe_directory();
    let (control, controls) = PlacementControlRouter::bounded(256, DEFAULT_MAX_CONTROL_PAYLOAD)?;
    let service = builder
        .coordinator_host(Arc::new(control), host, controls)
        .build()?;
    service.start().await?;
    write_domain_host_artifact(
        &artifact,
        &node_id,
        incarnation,
        &directory.borrow().clone(),
    )?;
    let writer_artifact = artifact.clone();
    let writer_node = node_id.clone();
    let writer = tokio::spawn(async move {
        loop {
            if directory.changed().await.is_err() {
                break;
            }
            let snapshot = directory.borrow().clone();
            if write_domain_host_artifact(&writer_artifact, &writer_node, incarnation, &snapshot)
                .is_err()
            {
                break;
            }
        }
    });
    tokio::signal::ctrl_c().await?;
    service.shutdown().await?;
    writer.abort();
    Ok(())
}

fn write_domain_host_artifact(
    artifact: &Path,
    node_id: &str,
    incarnation: NodeIncarnation,
    directory: &BTreeMap<CoordinatorScope, LeaderRecord>,
) -> Result<(), Box<dyn Error>> {
    let scopes = directory
        .iter()
        .map(|(scope, leader)| {
            let name = match scope {
                CoordinatorScope::Membership => "membership".to_owned(),
                CoordinatorScope::Placement(domain) => {
                    format!("placement:{}", domain.as_str())
                }
            };
            (
                name,
                ScopedLeadershipArtifact {
                    node_id: leader.node.node_id.clone(),
                    term: leader.term.get(),
                    incarnation: leader.node.incarnation.get(),
                },
            )
        })
        .collect();
    write_atomic(
        artifact.to_path_buf(),
        &serde_json::to_vec_pretty(&MultiDomainHostArtifact {
            node_id: node_id.to_owned(),
            incarnation: incarnation.get(),
            scopes,
        })?,
    )?;
    Ok(())
}

async fn domain_logic(
    artifact: PathBuf,
    node_id: String,
    address_host: String,
    port: u16,
    expected_members: Option<usize>,
    membership_only: bool,
) -> Result<(), Box<dyn Error>> {
    let started = Instant::now();
    if let Some(parent) = artifact.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let cluster = ClusterId::new("docker-domain-e2e")?;
    let incarnation = NodeIncarnation::generate();
    let address = NodeAddress::new(address_host, port)?;
    let mut config = node_config(
        cluster.clone(),
        &node_id,
        address.clone(),
        incarnation,
    );
    if membership_only {
        config.remoting.heartbeat_interval = Duration::from_secs(2);
        config.remoting.idle_data_connection_timeout = Duration::from_secs(2);
    }
    let mut builder = LatticeService::builder(config)?;
    if !membership_only {
        for (name, entity) in [
            ("alpha", "distributed-alpha"),
            ("beta", "distributed-beta"),
            ("gamma", "distributed-gamma"),
            ("delta", "distributed-delta"),
        ] {
            let domain = distributed_domain(name)?;
            builder = builder
                .proxy_entity_config::<FixtureProtocol>(EntityConfig::new(
                    domain.clone(),
                    EntityType::new(entity)?,
                    ProtocolId::new(PROTOCOL_ID)?,
                    16,
                    "weighted-least-load",
                    1,
                    Vec::new(),
                )?)?
                .domain_capacity(domain, 1)?;
        }
    }
    let membership_candidates = if membership_only {
        vec![("domain-membership", 29300)]
    } else {
        vec![
            ("domain-membership", 29300),
            ("domain-alpha", 29301),
            ("domain-beta", 29302),
            ("domain-gamma", 29303),
            ("domain-standby", 29304),
        ]
    };
    builder = builder.coordinator_discovery(domain_static_discovery(
        CoordinatorScope::Membership,
        "membership",
        &membership_candidates,
    )?)?;
    if !membership_only {
        builder = builder
            .coordinator_discovery(domain_static_discovery(
            CoordinatorScope::Placement(distributed_domain("alpha")?),
            "alpha",
            &[("domain-alpha", 29301), ("domain-standby", 29304)],
        )?)?
        .coordinator_discovery(domain_static_discovery(
            CoordinatorScope::Placement(distributed_domain("beta")?),
            "beta",
            &[("domain-beta", 29302), ("domain-standby", 29304)],
        )?)?
        .coordinator_discovery(domain_static_discovery(
            CoordinatorScope::Placement(distributed_domain("gamma")?),
            "gamma",
            &[("domain-gamma", 29303), ("domain-standby", 29304)],
        )?)?
        .coordinator_discovery(domain_static_discovery(
            CoordinatorScope::Placement(distributed_domain("delta")?),
            "delta",
            &[("domain-alpha", 29301), ("domain-standby", 29304)],
        )?)?;
    }
    builder = builder.join_config(ClusterJoinConfig {
        retry_initial: Duration::from_millis(25),
        retry_max: Duration::from_millis(250),
        join_timeout: Some(Duration::from_secs(240)),
        ..ClusterJoinConfig::default()
    });
    let mut scale_actor = None;
    if membership_only {
        let protocol = Arc::new(FixtureProtocol::bind::<PingActor>()?);
        let mut context = ServiceContext::builder(
            ServiceKind::from_static("distributed-scale-fixture"),
            InstanceId::new(node_id.clone()),
        );
        context.insert_extension(ActivationDirectory::new(8)?)?;
        let registry = Arc::new(ActorRegistry::new_bound(
            actor_kind!("DistributedScaleFixture"),
            ActorRegistryConfig {
                actor_ref: Some(ActorRefConfig {
                    cluster_id: cluster.clone(),
                    node_address: address,
                    node_incarnation: incarnation,
                }),
                service: context.build(),
                ..ActorRegistryConfig::default()
            },
            protocol.as_ref(),
        ));
        let handle = registry
            .start(
                ActorId::U64(1),
                PingActor {
                    child_reference: None,
                },
            )
            .await?;
        let reference: ActorRef<FixtureProtocol> = handle
            .typed_actor_ref()?
            .ok_or("scale actor is missing its ActorRef")?;
        scale_actor = Some(reference);
        builder = builder.register_actor(registry, protocol)?;
    }
    let service = builder.build()?;
    service.start().await?;
    if let Some(reference) = &scale_actor {
        write_scale_actor_artifact(&artifact, &node_id, reference)?;
    }
    let mut health = service.subscribe_health();
    let mut evidence = LogicEvidence::default();
    let mut artifact_tick = tokio::time::interval(Duration::from_millis(250));
    artifact_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let ready = tokio::time::timeout(Duration::from_secs(300), async {
        loop {
            let snapshot = health.borrow().clone();
            let members = service.member_snapshot();
            write_domain_logic_artifact(
                &artifact,
                &node_id,
                incarnation,
                &snapshot,
                &members,
                &evidence,
                &service,
            )?;
            if domain_logic_ready(&snapshot, membership_only)
                && expected_members.is_none_or(|expected| {
                    members.members.len() == expected
                        && members
                            .members
                            .iter()
                            .all(|member| member.status == MemberStatus::Up)
                })
            {
                break;
            }
            tokio::select! {
                changed = health.changed() => {
                    changed?;
                }
                _ = artifact_tick.tick() => {}
            }
        }
        Ok::<(), Box<dyn Error>>(())
    })
    .await;
    match ready {
        Ok(result) => result?,
        Err(_) => {
            let snapshot = health.borrow().clone();
            write_domain_logic_artifact(
                &artifact,
                &node_id,
                incarnation,
                &snapshot,
                &service.member_snapshot(),
                &evidence,
                &service,
            )?;
            return Err(IoError::other(format!(
                "domain logic {node_id} did not reach Ready with {expected_members:?} expected members within 300s; last health snapshot: {snapshot:?}"
            ))
            .into());
        }
    }
    evidence.join_millis = Some(started.elapsed().as_millis());
    if let (true, Some(reference)) = (membership_only, scale_actor.as_ref()) {
        evidence.ring = Some(
            run_scale_ring(&artifact, &node_id, reference, &service, &service.member_snapshot())
                .await?,
        );
    }
    write_domain_logic_artifact(
        &artifact,
        &node_id,
        incarnation,
        &health.borrow().clone(),
        &service.member_snapshot(),
        &evidence,
        &service,
    )?;
    let mut artifact_tick = tokio::time::interval(Duration::from_secs(1));
    artifact_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            changed = health.changed() => {
                if changed.is_err() {
                    break;
                }
                write_domain_logic_artifact(
                    &artifact,
                    &node_id,
                    incarnation,
                    &health.borrow().clone(),
                    &service.member_snapshot(),
                    &evidence,
                    &service,
                )?;
            }
            _ = artifact_tick.tick() => write_domain_logic_artifact(
                &artifact,
                &node_id,
                incarnation,
                &health.borrow().clone(),
                &service.member_snapshot(),
                &evidence,
                &service,
            )?,
            signal = tokio::signal::ctrl_c() => {
                signal?;
                break;
            }
        }
    }
    service.shutdown().await?;
    Ok(())
}

fn write_scale_actor_artifact(
    artifact: &Path,
    node_id: &str,
    reference: &ActorRef<FixtureProtocol>,
) -> Result<(), Box<dyn Error>> {
    let directory = artifact
        .parent()
        .ok_or_else(|| IoError::other("scale artifact has no parent directory"))?
        .join("refs");
    std::fs::create_dir_all(&directory)?;
    write_atomic(
        directory.join(format!("{node_id}.json")),
        &serde_json::to_vec(&ScaleActorArtifact {
            node_id: node_id.to_owned(),
            reference: reference.clone(),
        })?,
    )?;
    Ok(())
}

async fn run_scale_ring(
    artifact: &Path,
    node_id: &str,
    local_reference: &ActorRef<FixtureProtocol>,
    service: &LatticeService,
    membership: &MemberSnapshot,
) -> Result<RingArtifact, Box<dyn Error>> {
    let mut members = membership
        .members
        .iter()
        .filter(|member| member.status == MemberStatus::Up)
        .collect::<Vec<_>>();
    members.sort_by(|left, right| {
        (&left.node.node_id, left.node.incarnation.get())
            .cmp(&(&right.node.node_id, right.node.incarnation.get()))
    });
    let index = members
        .iter()
        .position(|member| {
            member.node.node_id == node_id
                && member.node.incarnation == local_reference.node_incarnation()
        })
        .ok_or_else(|| IoError::other("scale node is absent from its membership directory"))?;
    let peer = members[(index + 1) % members.len()];
    let target = if peer.node.node_id == node_id
        && peer.node.incarnation == local_reference.node_incarnation()
    {
        local_reference.clone()
    } else {
        let directory = artifact
            .parent()
            .ok_or_else(|| IoError::other("scale artifact has no parent directory"))?
            .join("refs");
        let path = directory.join(format!("{}.json", peer.node.node_id));
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match std::fs::read(&path)
                .ok()
                .and_then(|encoded| serde_json::from_slice::<ScaleActorArtifact>(&encoded).ok())
            {
                Some(found)
                    if found.node_id == peer.node.node_id
                        && found.reference.node_address() == &peer.node.address
                        && found.reference.node_incarnation() == peer.node.incarnation =>
                {
                    break found.reference;
                }
                _ if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                _ => {
                    return Err(IoError::other(format!(
                        "scale peer ActorRef {} did not appear within 30s",
                        peer.node.node_id
                    ))
                    .into());
                }
            }
        }
    };
    if peer.node.node_id != node_id || peer.node.incarnation != local_reference.node_incarnation() {
        service.connect_member(&peer.node).await?;
    }
    let request = u64::try_from(index)?;
    let started = Instant::now();
    let reply = service.ask(&target, Ping(request), Duration::from_secs(10)).await?;
    if reply != Pong(request + 1) {
        return Err(IoError::other("scale ring returned an unexpected reply").into());
    }
    let data_lanes_slept = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let associations = service.associations();
            if associations.attached_lane_count() == associations.len() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok();
    Ok(RingArtifact {
        peer_node_id: peer.node.node_id.clone(),
        request,
        reply: reply.0,
        elapsed_millis: started.elapsed().as_millis(),
        data_lanes_slept,
    })
}

fn process_resources() -> ProcessResourceArtifact {
    let status = std::fs::read_to_string("/proc/self/status").ok();
    ProcessResourceArtifact {
        resident_memory_kib: process_status_value(&status, "VmRSS:"),
        threads: process_status_value(&status, "Threads:"),
        open_file_descriptors: std::fs::read_dir("/proc/self/fd")
            .ok()
            .map(|entries| entries.count()),
    }
}

fn process_status_value(status: &Option<String>, key: &str) -> Option<u64> {
    status.as_deref()?.lines().find_map(|line| {
        line.strip_prefix(key)?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    })
}

fn domain_logic_ready(health: &ServiceHealthSnapshot, membership_only: bool) -> bool {
    health.node == NodeLifecycleState::Ready
        && (membership_only
            || ["alpha", "beta", "gamma", "delta"]
            .into_iter()
            .all(|name| {
                health.domains.get(
                    &distributed_domain(name).expect("static distributed domain must be valid"),
                ) == Some(&PlacementDomainState::Ready)
            }))
}

fn domain_static_discovery(
    scope: CoordinatorScope,
    name: &'static str,
    candidates: &[(&str, u16)],
) -> Result<Arc<dyn CoordinatorDiscovery>, Box<dyn Error>> {
    let endpoints = candidates
        .iter()
        .enumerate()
        .map(|(index, (node_id, port))| {
            Ok(StaticEndpoint {
                address: NodeAddress::new(*node_id, *port)?,
                expected_node_id: Some((*node_id).to_owned()),
                priority: u16::try_from(index + 1)?,
            })
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    Ok(Arc::new(StaticDiscovery::new(scope, name, endpoints)?))
}

fn write_domain_logic_artifact(
    artifact: &Path,
    node_id: &str,
    incarnation: NodeIncarnation,
    health: &ServiceHealthSnapshot,
    membership: &MemberSnapshot,
    evidence: &LogicEvidence,
    service: &LatticeService,
) -> Result<(), Box<dyn Error>> {
    let mut members = membership
        .members
        .iter()
        .map(|member| MemberArtifact {
            node_id: member.node.node_id.clone(),
            incarnation: member.node.incarnation.get(),
            status: format!("{:?}", member.status),
        })
        .collect::<Vec<_>>();
    members.sort_by(|left, right| {
        (&left.node_id, left.incarnation).cmp(&(&right.node_id, right.incarnation))
    });
    write_atomic(
        artifact.to_path_buf(),
        &serde_json::to_vec_pretty(&MultiDomainLogicArtifact {
            node_id: node_id.to_owned(),
            incarnation: incarnation.get(),
            lifecycle: format!("{:?}", health.node),
            domains: health
                .domains
                .iter()
                .map(|(domain, state)| (domain.as_str().to_owned(), format!("{state:?}")))
                .collect(),
            membership_version: membership.version.map(|version| MembershipVersionArtifact {
                term: version.term.get(),
                revision: version.revision.get(),
            }),
            members,
            join_millis: evidence.join_millis,
            ring: evidence.ring.clone(),
            resources: process_resources(),
            associations: service.associations().len(),
            attached_lanes: service.associations().attached_lane_count(),
        })?,
    )?;
    Ok(())
}
