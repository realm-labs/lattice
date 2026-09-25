mod candidate_fixture;
use candidate_fixture::prepare_leader;
use lattice_coordination::candidates::CandidateGeneration;
use std::time::Duration;

use etcd_client::Client;
use lattice_coordination::{
    coordinator::{ClusterLeaderGuard, GroupLeaderGuard, LeaderRecord, SingletonConfig},
    region::EntityConfig,
    storage::{
        ActorGroupStore, ClusterLifecycleStore, CoordinatorLeaseStore, InMemoryCoordinationStore,
        ScopedElectionStore, StorageError,
        etcd::{EtcdCoordinationConfig, EtcdCoordinationStore},
        records::{DurableStorageLimits, PutEntityConfig, PutSingletonConfig},
    },
    types::{CoordinatorTerm, NodeKey},
};
use lattice_model::{
    actor::ProtocolId,
    cluster::{
        ActorGroupId, CoordinatorScope, EntityType, NodeEndpoint, NodeIncarnation, SingletonKind,
    },
    run::{ClusterLifecycle, ControlOperationId, RunEpoch, RunPhase},
};

fn leader(scope: CoordinatorScope, epoch: RunEpoch, term: u64) -> LeaderRecord {
    LeaderRecord {
        candidate_generation: CandidateGeneration::new(1).unwrap(),
        candidate_lease_id: 1,
        epoch,
        scope,
        term: CoordinatorTerm::new(term).unwrap(),
        node: NodeKey {
            node_id: "candidate".to_owned(),
            address: NodeEndpoint::new("127.0.0.1", 32100).unwrap(),
            incarnation: NodeIncarnation::new(1).unwrap(),
        },
    }
}

async fn closing_contract<S>(store: &S)
where
    S: ClusterLifecycleStore + ScopedElectionStore + ActorGroupStore,
{
    let epoch = store.ensure_framework().await.unwrap();
    let lease = store.grant_lease(Duration::from_secs(30)).await.unwrap();
    let cluster = leader(CoordinatorScope::Cluster, epoch, 1);
    let group = ActorGroupId::new("lifecycle-test").unwrap();
    let group_record = leader(CoordinatorScope::Group(group.clone()), epoch, 1);
    let cluster = prepare_leader(store, cluster, lease).await.unwrap();
    let group_record = prepare_leader(store, group_record, lease).await.unwrap();
    assert!(store.campaign_leader(&cluster, lease).await.unwrap());
    assert!(store.campaign_leader(&group_record, lease).await.unwrap());
    let cluster_guard = ClusterLeaderGuard::new(cluster).unwrap();
    let group_guard = GroupLeaderGuard::new(group_record).unwrap();
    let config = SingletonConfig::new(
        group.clone(),
        SingletonKind::new("scheduler").unwrap(),
        ProtocolId::new(1).unwrap(),
    );
    store
        .put_singleton_config(
            &group_guard,
            PutSingletonConfig {
                expected: None,
                config: config.clone(),
            },
        )
        .await
        .unwrap();

    let oversized = EntityConfig::new(
        group.clone(),
        EntityType::new("large-definition").unwrap(),
        ProtocolId::new(2).unwrap(),
        1,
        "weighted-least-load",
        1,
        (0..64)
            .map(|index| format!("{index:03}{}", "x".repeat(240)))
            .collect(),
    )
    .unwrap();
    assert!(matches!(
        store
            .put_entity_config(
                &group_guard,
                PutEntityConfig {
                    expected: None,
                    config: oversized
                }
            )
            .await,
        Err(StorageError::RecordTooLarge { .. })
    ));
    assert!(
        store
            .get_entity_config(&group, &EntityType::new("large-definition").unwrap())
            .await
            .unwrap()
            .is_none()
    );

    let first = store
        .begin_shutdown(&cluster_guard, ControlOperationId::new("close-1").unwrap())
        .await
        .unwrap();
    assert!(matches!(first.phase, RunPhase::Closing { .. }));
    let retry = store
        .begin_shutdown(&cluster_guard, ControlOperationId::new("close-2").unwrap())
        .await
        .unwrap();
    assert_eq!(
        first, retry,
        "a retry must not replace the canonical operation"
    );
    assert!(matches!(
        store
            .put_singleton_config(
                &group_guard,
                PutSingletonConfig {
                    expected: Some(config.clone()),
                    config,
                }
            )
            .await,
        Err(StorageError::RunNotRunning)
    ));

    // Closing still permits a leader replacement to drive shutdown, never ordinary writes.
    store.revoke_lease(lease).await.unwrap();
    assert!(matches!(
        store
            .begin_shutdown(&cluster_guard, ControlOperationId::new("stale").unwrap())
            .await,
        Err(StorageError::LeadershipLost
            | StorageError::CompareFailed
            | StorageError::CandidateNotEligible)
    ));
    let lease = store.grant_lease(Duration::from_secs(30)).await.unwrap();
    let replacement = leader(CoordinatorScope::Cluster, epoch, 2);
    let replacement = prepare_leader(store, replacement, lease).await.unwrap();
    assert!(store.campaign_leader(&replacement, lease).await.unwrap());
    let replacement_guard = ClusterLeaderGuard::new(replacement).unwrap();
    assert_eq!(
        store
            .begin_shutdown(
                &replacement_guard,
                ControlOperationId::new("retry").unwrap()
            )
            .await
            .unwrap(),
        first
    );
    assert!(matches!(
        store
            .campaign_leader(
                &leader(CoordinatorScope::Cluster, epoch.next().unwrap(), 3),
                lease
            )
            .await,
        Err(StorageError::RunMismatch)
    ));
    store.revoke_lease(lease).await.unwrap();
}

#[tokio::test]
async fn memory_closing_fences_every_scope_and_allows_shutdown_takeover() {
    closing_contract(&InMemoryCoordinationStore::new(8, 8).unwrap()).await;
}

#[tokio::test]
#[ignore = "requires isolated etcd via LATTICE_ETCD_ENDPOINTS"]
async fn etcd_closing_and_run_binding_contract() {
    let endpoints = std::env::var("LATTICE_ETCD_ENDPOINTS")
        .expect("isolated test etcd")
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let prefix = format!("/lattice-run-test/{}", uuid::Uuid::new_v4().simple());
    let store = EtcdCoordinationStore::connect(EtcdCoordinationConfig {
        endpoints: endpoints.clone(),
        cluster_prefix: prefix.clone(),
        list_page_size: 4,
        limits: DurableStorageLimits {
            maximum_slots: 8,
            maximum_plans: 8,
            maximum_members: 8,
            maximum_admin_operations: 8,
            maximum_entity_configs: 8,
            maximum_singleton_configs: 8,
        },
        connect_options: None,
    })
    .await
    .unwrap();
    assert!(matches!(
        store.get_leader(&CoordinatorScope::Cluster).await,
        Err(StorageError::RunNotInitialized)
    ));
    closing_contract(&store).await;

    let mut raw = Client::connect(endpoints, None).await.unwrap();
    for suffix in [
        "meta/lifecycle",
        "meta/terms/cluster",
        "meta/terms/groups/lifecycle-test",
        "runs/1/membership/state_revision",
        "definitions/groups/lifecycle-test/singleton_types/scheduler",
        "definitions/groups/lifecycle-test/counters/singleton_configs",
    ] {
        assert_eq!(
            raw.get(format!("{prefix}/{suffix}"), None)
                .await
                .unwrap()
                .kvs()
                .len(),
            1,
            "{suffix}"
        );
    }
    assert!(
        raw.get(format!("{prefix}/membership/state_revision"), None)
            .await
            .unwrap()
            .kvs()
            .is_empty()
    );

    // Simulate a privileged, already-completed run transition. An existing handle
    // must neither rebind nor campaign into the new epoch using the old namespace.
    let epoch = RunEpoch::INITIAL.next().unwrap();
    raw.put(
        format!("{prefix}/meta/lifecycle"),
        serde_json::to_vec(&ClusterLifecycle {
            epoch,
            phase: RunPhase::Running,
        })
        .unwrap(),
        None,
    )
    .await
    .unwrap();
    for (suffix, value) in [("state_revision", "1"), ("counters/members", "0")] {
        raw.put(format!("{prefix}/runs/2/membership/{suffix}"), value, None)
            .await
            .unwrap();
    }
    assert!(matches!(
        store.ensure_framework().await,
        Err(StorageError::RunMismatch)
    ));
    let lease = store.grant_lease(Duration::from_secs(30)).await.unwrap();
    assert!(matches!(
        store
            .campaign_leader(&leader(CoordinatorScope::Cluster, epoch, 3), lease)
            .await,
        Err(StorageError::RunMismatch)
    ));
    assert!(matches!(
        store
            .campaign_leader(
                &leader(CoordinatorScope::Cluster, RunEpoch::INITIAL, 3),
                lease
            )
            .await,
        Err(StorageError::RunMismatch)
    ));
    store.revoke_lease(lease).await.unwrap();
}
