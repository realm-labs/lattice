use std::time::Duration;

use etcd_client::{Client, GetOptions};
use futures_util::StreamExt;
use lattice_coordination::{
    coordinator::LeaderRecord,
    discovery::EtcdCoordinatorDiscovery,
    storage::{
        CoordinatorLeaseStore, ScopedElectionStore,
        candidates::{CandidateStore, provision_candidate},
        etcd::EtcdCoordinationStore,
        records::DurableStorageLimits,
    },
    types::{CoordinatorTerm, NodeKey},
};
use lattice_discovery::provider::{CoordinatorDiscovery, DiscoveryOrigin};
use lattice_model::cluster::{CoordinatorScope, NodeEndpoint, NodeIncarnation};

#[tokio::test]
#[ignore = "requires isolated etcd via LATTICE_ETCD_ENDPOINTS"]
async fn discovery_is_read_only_and_tracks_leased_candidates() {
    let endpoints = std::env::var("LATTICE_ETCD_ENDPOINTS")
        .expect("isolated etcd")
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let prefix = format!("/lattice-discovery-test/{}", uuid::Uuid::new_v4().simple());
    let mut raw = Client::connect(endpoints, None).await.unwrap();
    let discovery = EtcdCoordinatorDiscovery::from_client(
        raw.clone(),
        prefix.clone(),
        CoordinatorScope::Cluster,
    )
    .unwrap();
    let mut snapshots = discovery.snapshots();
    assert!(snapshots.next().await.unwrap().is_err());
    assert!(
        raw.get(prefix.clone(), Some(GetOptions::new().with_prefix()))
            .await
            .unwrap()
            .kvs()
            .is_empty()
    );
    let store = EtcdCoordinationStore::from_client(
        raw.clone(),
        prefix.clone(),
        8,
        DurableStorageLimits {
            maximum_slots: 8,
            maximum_plans: 8,
            maximum_members: 8,
            maximum_admin_operations: 8,
            maximum_entity_configs: 8,
            maximum_singleton_configs: 8,
        },
    )
    .unwrap();
    let epoch = store.ensure_framework().await.unwrap();
    let authorization = provision_candidate(&store, CoordinatorScope::Cluster, "candidate".into())
        .await
        .unwrap();
    let lease = store.grant_lease(Duration::from_secs(30)).await.unwrap();
    let leader = LeaderRecord {
        candidate_generation: authorization.generation,
        candidate_lease_id: lease,
        epoch,
        scope: CoordinatorScope::Cluster,
        node: NodeKey {
            node_id: "candidate".into(),
            address: NodeEndpoint::new("127.0.0.1", 31021).unwrap(),
            incarnation: NodeIncarnation::new(1).unwrap(),
        },
        term: CoordinatorTerm::new(1).unwrap(),
    };
    store
        .register_candidate(&leader.candidate_registration())
        .await
        .unwrap();
    assert!(store.campaign_leader(&leader, lease).await.unwrap());
    let before = raw
        .get(prefix.clone(), Some(GetOptions::new().with_prefix()))
        .await
        .unwrap();
    let snapshot = tokio::time::timeout(Duration::from_secs(5), snapshots.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.targets.len(), 1);
    assert_eq!(snapshot.leader_hint.unwrap().address, leader.node.address);
    assert!(snapshot.targets[0].source.origins().any(
        |origin| matches!(origin, DiscoveryOrigin::Etcd { run_epoch, .. } if *run_epoch == epoch)
    ));
    let after = raw
        .get(prefix.clone(), Some(GetOptions::new().with_prefix()))
        .await
        .unwrap();
    assert_eq!(
        before
            .kvs()
            .iter()
            .map(|kv| (kv.key(), kv.value(), kv.mod_revision()))
            .collect::<Vec<_>>(),
        after
            .kvs()
            .iter()
            .map(|kv| (kv.key(), kv.value(), kv.mod_revision()))
            .collect::<Vec<_>>(),
        "discovery must never mutate cluster state"
    );
    store
        .unregister_candidate(&leader.candidate_registration())
        .await
        .unwrap();
    let snapshot = tokio::time::timeout(Duration::from_secs(5), snapshots.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(snapshot.targets.is_empty());
    assert!(
        snapshot.leader_hint.is_none(),
        "a revoked registration must suppress a still-present leader hint"
    );
    store.revoke_lease(lease).await.unwrap();
}
