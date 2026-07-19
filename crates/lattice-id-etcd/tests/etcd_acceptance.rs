use std::{collections::BTreeSet, sync::Arc, time::Duration};

use lattice_core::actor_ref::{ClusterId, NodeIncarnation};
use lattice_id::{
    service::{DistributedIdConfig, DistributedIdService, DistributedIdState},
    worker::{
        WorkerIdAcquisition, WorkerIdLeaseStore, WorkerIdOwner, WorkerIdRange, WorkerIdStoreError,
    },
};
use lattice_id_etcd::{config::EtcdWorkerIdStoreConfig, store::EtcdWorkerIdLeaseStore};
use tokio::task::JoinSet;

fn endpoints() -> Option<Vec<String>> {
    std::env::var("LATTICE_ETCD_ENDPOINTS")
        .ok()
        .map(|value| value.split(',').map(str::to_owned).collect())
}

fn owner(cluster: &str, node: usize) -> WorkerIdOwner {
    WorkerIdOwner::for_node(
        ClusterId::new(cluster).unwrap(),
        format!("node-{node}"),
        NodeIncarnation::new(node as u128 + 1).unwrap(),
    )
    .unwrap()
}

fn service_config(range: WorkerIdRange) -> DistributedIdConfig {
    DistributedIdConfig {
        worker_range: range,
        lease_ttl: Duration::from_secs(5),
        renew_interval: Duration::from_secs(1),
        lease_safety_margin: Duration::from_secs(1),
        maximum_clock_skew: Duration::from_millis(1),
        reacquire_backoff_initial: Duration::from_millis(10),
        reacquire_backoff_max: Duration::from_millis(100),
        ..DistributedIdConfig::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_etcd_fences_and_reuses_worker_ids_across_64_services() {
    let Some(endpoints) = endpoints() else {
        eprintln!("LATTICE_ETCD_ENDPOINTS is absent; Docker acceptance owns this test");
        return;
    };
    let key_prefix = format!("/lattice-worker-id-tests/{}", uuid::Uuid::new_v4().simple());
    let store = Arc::new(
        EtcdWorkerIdLeaseStore::connect(EtcdWorkerIdStoreConfig {
            endpoints,
            key_prefix,
        })
        .await
        .unwrap(),
    );
    let range = WorkerIdRange::new(0, 63).unwrap();
    let mut starters = JoinSet::new();
    for node in 0..64 {
        let store = store.clone();
        starters.spawn(async move {
            DistributedIdService::start(
                store,
                owner("service-cluster", node),
                service_config(range),
            )
            .await
            .unwrap()
        });
    }
    let mut services = Vec::new();
    while let Some(result) = starters.join_next().await {
        services.push(result.unwrap());
    }

    let mut workers = BTreeSet::new();
    let mut ids = BTreeSet::new();
    for service in &services {
        let DistributedIdState::Active { worker_id, .. } = service.state() else {
            panic!("new Etcd worker ID must be active");
        };
        workers.insert(worker_id);
        ids.insert(service.generator().try_next_id().unwrap());
    }
    assert_eq!(workers.len(), 64);
    assert_eq!(ids.len(), 64);
    assert!(matches!(
        DistributedIdService::start(
            store.clone(),
            owner("service-cluster", 100),
            service_config(range),
        )
        .await,
        Err(lattice_id::service::DistributedIdError::LeaseStore(
            WorkerIdStoreError::Unavailable { .. }
        ))
    ));

    let released = services.pop().unwrap();
    let released_worker = match released.state() {
        DistributedIdState::Active { worker_id, .. } => worker_id,
        state => panic!("unexpected state: {state:?}"),
    };
    assert!(released.shutdown().await.unwrap());
    let replacement = DistributedIdService::start(
        store.clone(),
        owner("service-cluster", 101),
        service_config(WorkerIdRange::new(released_worker.get(), released_worker.get()).unwrap()),
    )
    .await
    .unwrap();
    assert!(matches!(
        replacement.state(),
        DistributedIdState::CoolingDown { .. }
    ));
    assert_eq!(
        replacement.wait_until_active().await.unwrap(),
        released_worker
    );
    replacement.shutdown().await.unwrap();
    for service in services {
        service.shutdown().await.unwrap();
    }

    let stale = store
        .acquire(
            &owner("fencing-cluster", 1),
            WorkerIdRange::new(7, 7).unwrap(),
            Duration::from_secs(5),
        )
        .await
        .unwrap()
        .into_lease();
    assert!(store.release(&stale).await.unwrap());
    let current = store
        .acquire(
            &owner("fencing-cluster", 2),
            WorkerIdRange::new(7, 7).unwrap(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert!(matches!(current, WorkerIdAcquisition::Reused(_)));
    let renewed = store
        .renew(current.lease(), Duration::from_secs(5))
        .await
        .unwrap()
        .expect("the current fenced lease must renew");
    assert!(
        store
            .renew(&stale, Duration::from_secs(5))
            .await
            .unwrap()
            .is_none()
    );
    assert!(!store.release(&stale).await.unwrap());
    assert!(store.release(&renewed).await.unwrap());

    let expiring = store
        .acquire(
            &owner("expiry-cluster", 1),
            WorkerIdRange::new(9, 9).unwrap(),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert!(matches!(expiring, WorkerIdAcquisition::FirstUse(_)));
    let expiry_deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    let after_expiry = loop {
        match store
            .acquire(
                &owner("expiry-cluster", 2),
                WorkerIdRange::new(9, 9).unwrap(),
                Duration::from_secs(5),
            )
            .await
        {
            Ok(acquisition) => break acquisition,
            Err(WorkerIdStoreError::Unavailable { .. })
                if tokio::time::Instant::now() < expiry_deadline =>
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => panic!("expired Etcd lease was not reclaimed: {error}"),
        }
    };
    assert!(matches!(after_expiry, WorkerIdAcquisition::Reused(_)));
    assert!(store.release(after_expiry.lease()).await.unwrap());

    let cluster_a = store
        .acquire(
            &owner("cluster-a", 1),
            WorkerIdRange::new(11, 11).unwrap(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    let cluster_b = store
        .acquire(
            &owner("cluster-b", 1),
            WorkerIdRange::new(11, 11).unwrap(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(cluster_a.lease().id(), cluster_b.lease().id());
    assert!(store.release(cluster_a.lease()).await.unwrap());
    assert!(store.release(cluster_b.lease()).await.unwrap());
}
