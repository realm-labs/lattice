mod candidate_fixture;
use candidate_fixture::prepare_leader;
use lattice_coordination::candidates::CandidateGeneration;
use std::time::Duration;

use etcd_client::{Client, GetOptions};
use lattice_coordination::{
    coordinator::LeaderRecord,
    storage::{
        ClusterLifecycleStore, CoordinatorLeaseStore, ScopedElectionStore, StorageError,
        etcd::{EtcdCoordinationConfig, EtcdCoordinationStore},
        maintenance::{ClusterResetStore, StoppedDeployment},
        records::DurableStorageLimits,
    },
    types::{CoordinatorTerm, NodeKey},
};
use lattice_model::{
    cluster::{CoordinatorScope, NodeEndpoint, NodeIncarnation},
    run::{ControlOperationId, RunCompletion, RunEpoch, RunPhase},
};

#[tokio::test]
#[ignore = "requires isolated etcd via LATTICE_ETCD_ENDPOINTS"]
async fn reset_is_fenced_resumable_bounded_and_preserves_persistent_data() {
    let endpoints = std::env::var("LATTICE_ETCD_ENDPOINTS")
        .expect("isolated etcd")
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let prefix = format!("/lattice-reset-test/{}", uuid::Uuid::new_v4().simple());
    let config = EtcdCoordinationConfig {
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
    };
    let store = EtcdCoordinationStore::connect(config.clone())
        .await
        .unwrap();
    let epoch = store.ensure_framework().await.unwrap();
    let lease = store.grant_lease(Duration::from_secs(30)).await.unwrap();
    let leader = LeaderRecord {
        candidate_generation: CandidateGeneration::new(1).unwrap(),
        candidate_lease_id: 1,
        epoch,
        scope: CoordinatorScope::Cluster,
        term: CoordinatorTerm::new(1).unwrap(),
        node: NodeKey {
            node_id: "candidate".to_owned(),
            address: NodeEndpoint::new("127.0.0.1", 32101).unwrap(),
            incarnation: NodeIncarnation::new(1).unwrap(),
        },
    };
    let mut leader = prepare_leader(&store, leader, lease).await.unwrap();
    assert!(store.campaign_leader(&leader, lease).await.unwrap());
    let initial = store.lifecycle().await.unwrap();
    let operation = ControlOperationId::new("reset-1").unwrap();
    let wrong = StoppedDeployment {
        namespace: format!("{prefix}/wrong"),
        epoch,
    };
    assert!(matches!(
        store.begin_reset(&initial, operation.clone(), &wrong).await,
        Err(StorageError::ResetConfirmationRequired)
    ));
    assert_eq!(store.lifecycle().await.unwrap(), initial);
    let confirmation = StoppedDeployment {
        namespace: prefix.clone(),
        epoch,
    };
    let resetting = store
        .begin_reset(&initial, operation.clone(), &confirmation)
        .await
        .unwrap();
    assert!(matches!(resetting.phase, RunPhase::Resetting { .. }));
    assert_eq!(
        store
            .begin_reset(&initial, operation.clone(), &confirmation)
            .await
            .unwrap(),
        resetting
    );
    assert!(matches!(
        store.ensure_framework().await,
        Err(StorageError::RunNotRunning)
    ));
    assert!(matches!(
        store.reset_batch(epoch, &operation, 32).await,
        Err(StorageError::ResetLiveLease)
    ));
    assert!(matches!(
        store.campaign_leader(&leader, lease).await,
        Err(StorageError::RunNotRunning)
    ));
    store.revoke_lease(lease).await.unwrap();

    let mut raw = Client::connect(endpoints, None).await.unwrap();
    for suffix in [
        "definitions/groups/game/entity_types/player",
        "worker-ids/history/7",
    ] {
        raw.put(format!("{prefix}/{suffix}"), "persistent", None)
            .await
            .unwrap();
    }
    // Offline reset does not need to decode potentially damaged runtime payloads.
    for i in 0..9 {
        raw.put(
            format!("{prefix}/runs/1/groups/game/shards/player/{i}"),
            "damaged",
            None,
        )
        .await
        .unwrap();
    }
    let unknown = format!("{prefix}/runs/1/unknown/application-data");
    raw.put(unknown.clone(), "must-inspect", None)
        .await
        .unwrap();
    let mut observed_unknown = false;
    for _ in 0..16 {
        match store.reset_batch(epoch, &operation, 2).await {
            Ok(progress) => {
                assert!(progress.deleted_keys <= 2);
                assert!(!progress.complete);
            }
            Err(StorageError::UnrecognizedRuntimeKey) => {
                observed_unknown = true;
                break;
            }
            result => panic!("unexpected cleanup result: {result:?}"),
        }
    }
    assert!(observed_unknown);
    assert_eq!(
        raw.get(unknown.clone(), None).await.unwrap().kvs()[0].value(),
        b"must-inspect"
    );
    // The operator explicitly removes the test's unexpected key after inspection.
    raw.delete(unknown, None).await.unwrap();

    let resumed = EtcdCoordinationStore::connect(config.clone())
        .await
        .unwrap();
    let mut completed = false;
    for _ in 0..16 {
        let progress = resumed.reset_batch(epoch, &operation, 2).await.unwrap();
        assert!(progress.deleted_keys <= 2);
        if progress.complete {
            completed = true;
            break;
        }
    }
    assert!(completed);
    assert!(
        resumed
            .reset_batch(epoch, &operation, 2)
            .await
            .unwrap()
            .complete
    );
    let closed = resumed.lifecycle().await.unwrap();
    assert!(matches!(
        closed.phase,
        RunPhase::Closed {
            completion: RunCompletion::Reset,
            ..
        }
    ));
    assert!(
        raw.get(
            format!("{prefix}/runs/1/"),
            Some(GetOptions::new().with_prefix())
        )
        .await
        .unwrap()
        .kvs()
        .is_empty()
    );
    for suffix in [
        "definitions/groups/game/entity_types/player",
        "worker-ids/history/7",
    ] {
        assert_eq!(
            raw.get(format!("{prefix}/{suffix}"), None)
                .await
                .unwrap()
                .kvs()[0]
                .value(),
            b"persistent"
        );
    }

    let new_epoch = resumed.start_new_run(&closed).await.unwrap();
    assert_eq!(new_epoch, RunEpoch::INITIAL.next().unwrap());
    assert_eq!(resumed.start_new_run(&closed).await.unwrap(), new_epoch);
    assert!(matches!(
        store.ensure_framework().await,
        Err(StorageError::RunMismatch)
    ));
    let fresh = EtcdCoordinationStore::connect(config).await.unwrap();
    assert_eq!(fresh.ensure_framework().await.unwrap(), new_epoch);
    assert_eq!(
        fresh
            .get_leader_term(&CoordinatorScope::Cluster)
            .await
            .unwrap(),
        1
    );
    leader.epoch = new_epoch;
    leader.term = CoordinatorTerm::new(2).unwrap();
    let lease = fresh.grant_lease(Duration::from_secs(30)).await.unwrap();
    let leader = prepare_leader(&fresh, leader, lease).await.unwrap();
    assert!(fresh.campaign_leader(&leader, lease).await.unwrap());
    assert!(matches!(
        resumed.reset_batch(epoch, &operation, 2).await,
        Err(StorageError::RunMismatch)
    ));
    fresh.revoke_lease(lease).await.unwrap();
}
