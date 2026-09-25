use lattice_coordination::{
    candidates::CandidateChange,
    coordinator::{ClusterLeaderGuard, MemberHello},
    runtime::cluster::{ClusterCoordinator, ClusterCoordinatorConfig},
    shutdown::{ClusterShutdownStore, NodeStopOutcome, ShutdownReport},
    storage::{
        InMemoryCoordinationStore, MembershipStore, ScopedElectionStore, StorageError,
        candidates::CandidateStore,
        etcd::{EtcdCoordinationConfig, EtcdCoordinationStore},
        records::DurableStorageLimits,
    },
    types::{CoordinatorTerm, NodeKey},
};
use lattice_model::{
    cluster::{CoordinatorScope, NodeEndpoint, NodeIncarnation},
    run::{ControlOperationId, RunCompletion, RunPhase},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

fn node(id: &str, incarnation: u128) -> NodeKey {
    NodeKey {
        node_id: id.to_owned(),
        address: NodeEndpoint::new("127.0.0.1", 32321).unwrap(),
        incarnation: NodeIncarnation::new(incarnation).unwrap(),
    }
}

async fn contract<S>(store: Arc<S>)
where
    S: ClusterShutdownStore + MembershipStore + ScopedElectionStore + CandidateStore,
{
    let epoch = store.ensure_framework().await.unwrap();
    store
        .change_candidate(CandidateChange {
            epoch,
            scope: CoordinatorScope::Cluster,
            expected_revision: 0,
            node_id: "coordinator".to_owned(),
            enabled: true,
            operation: ControlOperationId::new("enable").unwrap(),
        })
        .await
        .unwrap();
    store
        .change_candidate(CandidateChange {
            epoch,
            scope: CoordinatorScope::Cluster,
            expected_revision: 1,
            node_id: "successor".to_owned(),
            enabled: true,
            operation: ControlOperationId::new("enable-successor").unwrap(),
        })
        .await
        .unwrap();
    let mut coordinator = ClusterCoordinator::elect(
        store.clone(),
        node("coordinator", 1),
        CoordinatorTerm::new(1).unwrap(),
        ClusterCoordinatorConfig::default(),
    )
    .await
    .unwrap();
    let mut guard = ClusterLeaderGuard::new(coordinator.leader().clone()).unwrap();
    let participant = node("actor-host", 2);
    let member = coordinator
        .join(MemberHello {
            node: participant.clone(),
            roles: BTreeSet::new(),
            failure_domains: BTreeMap::new(),
            protocols: Vec::new(),
            remoting_capabilities: BTreeSet::new(),
        })
        .await
        .unwrap();
    let mut other_participants = Vec::new();
    for index in 0..32 {
        let other = node(&format!("other-{index:02}"), 100 + index);
        coordinator
            .join(MemberHello {
                node: other.clone(),
                roles: BTreeSet::new(),
                failure_domains: BTreeMap::new(),
                protocols: Vec::new(),
                remoting_capabilities: BTreeSet::new(),
            })
            .await
            .unwrap();
        other_participants.push(other);
    }
    // Expiry removes live membership, not the old process's stop obligation.
    store.revoke_lease(member.lease_id).await.unwrap();
    let operation = ControlOperationId::new("shutdown-1").unwrap();
    store
        .begin_shutdown(&guard, operation.clone())
        .await
        .unwrap();
    let mut manifest = store
        .build_shutdown_manifest(&guard, &operation)
        .await
        .unwrap();
    assert!(!manifest.sealed);
    manifest = store
        .build_shutdown_manifest(&guard, &operation)
        .await
        .unwrap();
    assert!(manifest.sealed);
    assert_eq!(manifest.participant_count, 33);
    assert_eq!(
        store
            .shutdown_participants(&operation, None, 32)
            .await
            .unwrap()[0]
            .1,
        participant
    );
    assert!(matches!(
        store.begin_shutdown_cleanup(&guard, &operation).await,
        Err(StorageError::ShutdownBlocked)
    ));
    let mut report = ShutdownReport {
        epoch,
        operation: operation.clone(),
        node: participant.clone(),
        outcome: NodeStopOutcome::Blocked {
            reason: "stop hook failed".into(),
        },
    };
    store
        .record_shutdown_report(&guard, report.clone())
        .await
        .unwrap();
    assert!(matches!(
        store.begin_shutdown_cleanup(&guard, &operation).await,
        Err(StorageError::ShutdownBlocked)
    ));
    report.node.incarnation = NodeIncarnation::new(3).unwrap();
    report.outcome = NodeStopOutcome::Stopped;
    assert!(
        store
            .record_shutdown_report(&guard, report.clone())
            .await
            .is_err()
    );
    report.node = participant;
    store
        .record_shutdown_report(&guard, report.clone())
        .await
        .unwrap();
    store
        .record_shutdown_report(&guard, report.clone())
        .await
        .unwrap();
    report.outcome = NodeStopOutcome::Blocked {
        reason: "late stale reply".into(),
    };
    assert!(store.record_shutdown_report(&guard, report).await.is_err());
    for node in other_participants {
        store
            .record_shutdown_report(
                &guard,
                ShutdownReport {
                    epoch,
                    operation: operation.clone(),
                    node,
                    outcome: NodeStopOutcome::Stopped,
                },
            )
            .await
            .unwrap();
    }
    loop {
        match store.begin_shutdown_cleanup(&guard, &operation).await {
            Ok(()) => break,
            Err(StorageError::ShutdownBlocked) => continue,
            Err(error) => panic!("unexpected cleanup failure: {error}"),
        }
    }
    // Crash after Cleaning has begun: the successor must resume bounded cleanup,
    // without reconstructing expired members or repeating application stop hooks.
    assert!(
        !store
            .shutdown_cleanup_batch(&guard, &operation, 1)
            .await
            .unwrap()
            .complete
    );
    store
        .revoke_lease(guard.record().candidate_lease_id)
        .await
        .unwrap();
    let successor = ClusterCoordinator::elect(
        store.clone(),
        node("successor", 7),
        CoordinatorTerm::new(2).unwrap(),
        ClusterCoordinatorConfig::default(),
    )
    .await
    .unwrap();
    let old_guard = guard;
    guard = ClusterLeaderGuard::new(successor.leader().clone()).unwrap();
    assert!(
        store
            .shutdown_cleanup_batch(&old_guard, &operation, 1)
            .await
            .is_err()
    );
    for _ in 0..256 {
        if store
            .shutdown_cleanup_batch(&guard, &operation, 1)
            .await
            .unwrap()
            .complete
        {
            break;
        }
    }
    let run = store.lifecycle().await.unwrap();
    assert!(matches!(
        run.phase,
        RunPhase::Closed {
            completion: RunCompletion::Graceful,
            ..
        }
    ));
    assert!(
        store
            .shutdown_cleanup_batch(&guard, &operation, 1)
            .await
            .unwrap()
            .complete
    );
    assert!(matches!(
        store.ensure_framework().await,
        Err(StorageError::RunNotRunning)
    ));
}

#[tokio::test]
async fn memory_shutdown_requires_positive_exact_incarnation_evidence() {
    contract(Arc::new(InMemoryCoordinationStore::new(64, 64).unwrap())).await;
}

#[tokio::test]
#[ignore = "requires isolated etcd via LATTICE_ETCD_ENDPOINTS"]
async fn etcd_shutdown_requires_evidence_and_reclaims_runtime_only() {
    let endpoints = std::env::var("LATTICE_ETCD_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let prefix = format!("/test/shutdown/{}", uuid::Uuid::new_v4());
    let store = EtcdCoordinationStore::connect(EtcdCoordinationConfig {
        endpoints: endpoints.clone(),
        cluster_prefix: prefix.clone(),
        list_page_size: 32,
        limits: DurableStorageLimits {
            maximum_slots: 8,
            maximum_plans: 8,
            maximum_members: 64,
            maximum_admin_operations: 8,
            maximum_entity_configs: 8,
            maximum_singleton_configs: 8,
        },
        connect_options: None,
    })
    .await
    .unwrap();
    contract(Arc::new(store)).await;
    // Cold read-only discovery can recover completion after every run-scoped
    // candidate/leader key has been deleted; no endpoint cache is required.
    use lattice_coordination::discovery::EtcdCoordinatorDiscovery;
    use lattice_discovery::provider::CoordinatorDiscovery;
    use lattice_model::run::RunEpoch;
    let discovery =
        EtcdCoordinatorDiscovery::connect(&endpoints, None, prefix, CoordinatorScope::Cluster)
            .await
            .unwrap();
    let completion = discovery
        .terminal_lifecycle(RunEpoch::INITIAL)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        completion.phase,
        RunPhase::Closed {
            completion: RunCompletion::Graceful,
            ..
        }
    ));
    assert!(
        discovery
            .terminal_lifecycle(RunEpoch::INITIAL.next().unwrap())
            .await
            .unwrap()
            .is_none()
    );
}
