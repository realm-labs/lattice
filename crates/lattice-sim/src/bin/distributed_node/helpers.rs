fn fixture_entity_config() -> Result<EntityConfig, Box<dyn Error>> {
    Ok(EntityConfig::new(
        placement_domain(),
        EntityType::new("distributed-entity")?,
        ProtocolId::new(PROTOCOL_ID)?,
        16,
        "weighted-least-load",
        1,
        Vec::new(),
    )?)
}

fn fixture_entity_slot(
    config: &EntityConfig,
    entity_id: &EntityId,
    owner: NodeKey,
) -> Result<PlacementSlot, Box<dyn Error>> {
    Ok(PlacementSlot {
        key: PlacementSlotKey::Shard {
            domain: config.domain.clone(),
            entity_type: config.entity_type.clone(),
            shard_id: config.shard_for(entity_id),
        },
        config_fingerprint: config.fingerprint(),
        owner: Some(owner),
        target: None,
        assignment_generation: AssignmentGeneration::new(1)?,
        version: PlacementVersion::new(
            config.domain.clone(),
            CoordinatorTerm::new(1)?,
            Revision::new(1)?,
        ),
        state: PlacementSlotState::Running,
        active_move: None,
        barrier_sessions: BTreeSet::new(),
    })
}

fn placement_domain() -> PlacementDomainId {
    PlacementDomainId::new("distributed-simulation").expect("static placement domain is valid")
}

async fn wait_for_file(path: &PathBuf) -> Result<Vec<u8>, Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match std::fs::read(path) {
            Ok(encoded) => return Ok(encoded),
            Err(error) if error.kind() == ErrorKind::NotFound && Instant::now() < deadline => {
                tokio::task::yield_now().await;
            }
            Err(error) => return Err(Box::new(error)),
        }
    }
}

fn write_atomic(path: PathBuf, contents: &[u8]) -> Result<(), IoError> {
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    std::fs::write(&temporary, contents)?;
    std::fs::rename(temporary, path)
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
        roles: BTreeSet::new(),
        remoting: RemotingConfig {
            heartbeat_interval: Duration::from_millis(100),
            shutdown_timeout: Duration::from_secs(2),
            ..RemotingConfig::default()
        },
        maximum_actor_protocols: 8,
        maximum_watches: 32,
        maximum_supervised_tasks: 32,
        shutdown_timeout: Duration::from_secs(2),
    }
}
