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
    if let Some(parent) = artifact.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let cluster = ClusterId::new("docker-domain-e2e")?;
    let incarnation = NodeIncarnation::generate();
    let mut config = node_config(
        cluster,
        &node_id,
        NodeAddress::new(address_host, port)?,
        incarnation,
    );
    if membership_only {
        config.remoting.heartbeat_interval = Duration::from_secs(2);
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
    let service = builder.build()?;
    service.start().await?;
    let mut health = service.subscribe_health();
    let mut artifact_tick = tokio::time::interval(Duration::from_millis(250));
    artifact_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let ready = tokio::time::timeout(Duration::from_secs(300), async {
        loop {
            let snapshot = health.borrow().clone();
            let members = service.member_snapshot();
            write_domain_logic_artifact(&artifact, &node_id, incarnation, &snapshot, &members)?;
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
            )?;
            return Err(IoError::other(format!(
                "domain logic {node_id} did not reach Ready with {expected_members:?} expected members within 300s; last health snapshot: {snapshot:?}"
            ))
            .into());
        }
    }
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
                )?;
            }
            _ = artifact_tick.tick() => write_domain_logic_artifact(
                &artifact,
                &node_id,
                incarnation,
                &health.borrow().clone(),
                &service.member_snapshot(),
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
        })?,
    )?;
    Ok(())
}
