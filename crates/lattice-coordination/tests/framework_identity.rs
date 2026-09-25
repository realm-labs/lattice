use std::sync::Arc;

use etcd_client::Client;
use lattice_coordination::storage::{
    CoordinatorLeaseStore, StorageError,
    etcd::{EtcdCoordinationConfig, EtcdCoordinationStore},
    records::DurableStorageLimits,
};
use lattice_coordination::{
    runtime::{
        CoordinatorRuntimeError,
        cluster::{ClusterCoordinator, ClusterCoordinatorConfig},
    },
    types::{CoordinatorTerm, NodeKey},
};
use lattice_model::{
    cluster::{NodeEndpoint, NodeIncarnation},
    framework::LatticeVersion,
};

/// Run explicitly against an isolated test etcd. A skipped backend is not validation evidence.
#[tokio::test]
#[ignore = "requires isolated etcd via LATTICE_ETCD_ENDPOINTS"]
async fn framework_marker_is_initialized_once_and_never_overwritten() {
    let endpoints = std::env::var("LATTICE_ETCD_ENDPOINTS")
        .expect("isolated test etcd endpoints")
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let prefix = format!("/lattice-framework-test/{}", uuid::Uuid::new_v4().simple());
    let mut raw = Client::connect(endpoints.clone(), None).await.unwrap();
    let store = EtcdCoordinationStore::connect(EtcdCoordinationConfig {
        endpoints,
        cluster_prefix: prefix.clone(),
        list_page_size: 8,
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
    let (first, second) = tokio::join!(store.ensure_framework(), store.ensure_framework());
    first.unwrap();
    second.unwrap();
    assert!(
        raw.get(format!("{prefix}/schema_generation"), None)
            .await
            .unwrap()
            .kvs()
            .is_empty()
    );
    let key = format!("{prefix}/meta/framework");
    assert_eq!(
        raw.get(key.clone(), None).await.unwrap().kvs()[0].value(),
        LatticeVersion::CURRENT.as_bytes()
    );

    raw.put(key.clone(), "foreign-framework", None)
        .await
        .unwrap();
    // A candidate must fail before decoding state, granting a lease, or campaigning.
    raw.put(
        format!("{prefix}/runs/1/membership/members/poison"),
        "not-json",
        None,
    )
    .await
    .unwrap();
    let result = ClusterCoordinator::elect(
        Arc::new(store.clone()),
        NodeKey {
            node_id: "candidate".to_owned(),
            address: NodeEndpoint::new("127.0.0.1", 29999).unwrap(),
            incarnation: NodeIncarnation::new(1).unwrap(),
        },
        CoordinatorTerm::new(1).unwrap(),
        ClusterCoordinatorConfig::default(),
    )
    .await;
    assert!(matches!(
        result,
        Err(CoordinatorRuntimeError::Storage(
            StorageError::FrameworkMismatch
        ))
    ));
    assert!(
        raw.get(format!("{prefix}/runs/1/membership/leader"), None)
            .await
            .unwrap()
            .kvs()
            .is_empty()
    );
    assert!(
        raw.get(format!("{prefix}/meta/terms/cluster"), None)
            .await
            .unwrap()
            .kvs()
            .is_empty()
    );
    assert!(matches!(
        store.ensure_framework().await,
        Err(StorageError::FrameworkMismatch)
    ));
    assert_eq!(
        raw.get(key.clone(), None).await.unwrap().kvs()[0].value(),
        b"foreign-framework"
    );

    raw.delete(key.clone(), None).await.unwrap();
    assert!(matches!(
        store.ensure_framework().await,
        Err(StorageError::FrameworkMismatch)
    ));
    assert!(raw.get(key.clone(), None).await.unwrap().kvs().is_empty());

    // Manually stamping the current version on a legacy schema is not an upgrade.
    raw.put(key, LatticeVersion::CURRENT, None).await.unwrap();
    raw.put(format!("{prefix}/schema_generation"), "7", None)
        .await
        .unwrap();
    assert!(matches!(
        store.ensure_framework().await,
        Err(StorageError::FrameworkMismatch)
    ));
}
