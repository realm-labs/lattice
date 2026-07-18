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
                        StorageError::CompareFailed
                        | StorageError::LeadershipLost
                        | StorageError::Unavailable
                        | StorageError::Deadline
                        | StorageError::OutcomeUnknown
                        | StorageError::IncarnationConflict,
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
