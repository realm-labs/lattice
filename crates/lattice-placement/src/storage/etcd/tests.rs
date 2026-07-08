use std::collections::BTreeMap;

use lattice_core::actor_ref::Epoch;
use lattice_core::id::ActorId;
use lattice_core::instance::InstanceCapacity;
use lattice_core::{actor_kind, service_kind};

use super::*;
use crate::registry::InstanceState;
use crate::storage::PlacementState;
use crate::storage::etcd::codec::{decode_etcd_value, encode_etcd_value, put_options_for};

#[tokio::test]
async fn etcd_store_writes_under_cluster_prefix_and_isolates_reads() {
    let client = InMemoryEtcdClient::new();
    let first = EtcdPlacementStore::new(PlacementPrefix::new("/lattice/cluster-a"), client.clone());
    let second =
        EtcdPlacementStore::new(PlacementPrefix::new("/lattice/cluster-b"), client.clone());
    let key = actor_key_for(7);

    first
        .upsert_instance(instance_record("world-a", InstanceState::Ready))
        .await
        .unwrap();
    first
        .compare_and_put_actor(key.clone(), None, actor_record(7, "world-a", 1, LeaseId(1)))
        .await
        .unwrap();
    first
        .compare_and_put_virtual_shard(vshard_key_for(3), None, vshard_record(3, "world-a", 1))
        .await
        .unwrap();
    first
        .compare_and_put_singleton(
            singleton_key_for("global"),
            None,
            singleton_record("global", "world-a", 1, LeaseId(9)),
        )
        .await
        .unwrap();

    assert_eq!(
        client.keys(),
        vec![
            "/lattice/cluster-a/logic/actors/World/World/u64:7".to_string(),
            "/lattice/cluster-a/logic/instances/World/world-a".to_string(),
            "/lattice/cluster-a/logic/singletons/World/SeasonManager/676c6f62616c".to_string(),
            "/lattice/cluster-a/logic/vshards/World/World/3".to_string(),
        ]
    );
    assert!(second.get_actor(&key).await.unwrap().is_none());
    assert!(
        second
            .get_virtual_shard(&vshard_key_for(3))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        second
            .list_instances(&service_kind!("World"))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        second
            .get_singleton(&singleton_key_for("global"))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn etcd_store_compare_and_put_uses_versions() {
    let store = EtcdPlacementStore::new(
        PlacementPrefix::new("/lattice/test"),
        InMemoryEtcdClient::new(),
    );
    let key = actor_key_for(7);
    let record = actor_record(7, "world-a", 1, LeaseId(1));

    let version = store
        .compare_and_put_actor(key.clone(), None, record.clone())
        .await
        .unwrap();
    let stale = store
        .compare_and_put_actor(key.clone(), None, record.clone())
        .await;
    let updated = ActorPlacementRecord {
        epoch: Epoch(2),
        ..record
    };
    let next = store
        .compare_and_put_actor(key.clone(), Some(version), updated.clone())
        .await
        .unwrap();

    assert_eq!(version, PlacementVersion(1));
    assert_eq!(stale, Err(PlacementError::CompareAndPutFailed));
    assert_eq!(next, PlacementVersion(2));
    assert_eq!(store.get_actor(&key).await.unwrap().unwrap().1, updated);
}

#[tokio::test]
async fn etcd_store_persists_virtual_shards_with_versions() {
    let store = EtcdPlacementStore::new(
        PlacementPrefix::new("/lattice/test"),
        InMemoryEtcdClient::new(),
    );
    let key = vshard_key_for(9);
    let record = vshard_record(9, "world-a", 1);

    let version = store
        .compare_and_put_virtual_shard(key.clone(), None, record.clone())
        .await
        .unwrap();
    let stale = store
        .compare_and_put_virtual_shard(key.clone(), None, record.clone())
        .await;
    let updated = VirtualShardPlacementRecord {
        owner: InstanceId::new("world-b"),
        epoch: Epoch(2),
        ..record
    };
    let next = store
        .compare_and_put_virtual_shard(key.clone(), Some(version), updated.clone())
        .await
        .unwrap();

    assert_eq!(version, PlacementVersion(1));
    assert_eq!(stale, Err(PlacementError::CompareAndPutFailed));
    assert_eq!(next, PlacementVersion(2));
    assert_eq!(
        store.get_virtual_shard(&key).await.unwrap().unwrap().1,
        updated
    );
    assert_eq!(
        store
            .list_virtual_shards(&service_kind!("World"), &actor_kind!("World"))
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn etcd_store_persists_singletons_with_versions() {
    let store = EtcdPlacementStore::new(
        PlacementPrefix::new("/lattice/test"),
        InMemoryEtcdClient::new(),
    );
    let key = singleton_key_for("global");
    let record = singleton_record("global", "world-a", 1, LeaseId(7));

    let version = store
        .compare_and_put_singleton(key.clone(), None, record.clone())
        .await
        .unwrap();
    let stale = store
        .compare_and_put_singleton(key.clone(), None, record.clone())
        .await;
    let updated = SingletonPlacementRecord {
        owner: InstanceId::new("world-b"),
        epoch: Epoch(2),
        lease_id: LeaseId(8),
        ..record
    };
    let next = store
        .compare_and_put_singleton(key.clone(), Some(version), updated.clone())
        .await
        .unwrap();

    assert_eq!(version, PlacementVersion(1));
    assert_eq!(stale, Err(PlacementError::CompareAndPutFailed));
    assert_eq!(next, PlacementVersion(2));
    assert_eq!(store.get_singleton(&key).await.unwrap().unwrap().1, updated);
    assert_eq!(store.list_singletons().await.unwrap().len(), 1);
}

#[tokio::test]
async fn etcd_watch_reports_instance_updates() {
    let store = EtcdPlacementStore::new(
        PlacementPrefix::new("/lattice/test"),
        InMemoryEtcdClient::new(),
    );
    let mut watch = store.watch(store.prefix().clone()).await.unwrap();
    let record = instance_record("world-a", InstanceState::Ready);

    store.upsert_instance(record.clone()).await.unwrap();

    let event = watch.next().await.unwrap();
    assert_eq!(event, PlacementWatchEvent::InstanceUpdated { record });
}

#[tokio::test]
async fn etcd_watch_reports_virtual_shard_updates() {
    let store = EtcdPlacementStore::new(
        PlacementPrefix::new("/lattice/test"),
        InMemoryEtcdClient::new(),
    );
    let mut watch = store.watch(store.prefix().clone()).await.unwrap();
    let key = vshard_key_for(5);
    let record = vshard_record(5, "world-a", 1);
    let version = store
        .compare_and_put_virtual_shard(key.clone(), None, record.clone())
        .await
        .unwrap();

    let event = watch.next().await.unwrap();
    assert_eq!(
        event,
        PlacementWatchEvent::VirtualShardUpdated {
            key,
            version,
            record,
        }
    );
}

#[tokio::test]
async fn etcd_watch_reports_singleton_updates() {
    let store = EtcdPlacementStore::new(
        PlacementPrefix::new("/lattice/test"),
        InMemoryEtcdClient::new(),
    );
    let mut watch = store.watch(store.prefix().clone()).await.unwrap();
    let key = singleton_key_for("global");
    let record = singleton_record("global", "world-a", 1, LeaseId(7));
    let version = store
        .compare_and_put_singleton(key.clone(), None, record.clone())
        .await
        .unwrap();

    let event = watch.next().await.unwrap();
    assert_eq!(
        event,
        PlacementWatchEvent::SingletonUpdated {
            key,
            version,
            record,
        }
    );
}

#[tokio::test]
async fn etcd_store_grants_and_keeps_instance_leases_alive() {
    let store = EtcdPlacementStore::new(
        PlacementPrefix::new("/lattice/test"),
        InMemoryEtcdClient::new(),
    );

    let lease_id = store.grant_instance_lease().await.unwrap();
    store.keepalive_instance_lease(lease_id).await.unwrap();
    let missing = store.keepalive_instance_lease(LeaseId(999)).await;

    assert_eq!(lease_id, LeaseId(1));
    assert_eq!(
        missing,
        Err(PlacementError::InstanceLeaseNotFound {
            lease_id: LeaseId(999)
        })
    );
}

#[tokio::test]
async fn etcd_store_elects_one_coordinator_leader_until_resign() {
    let store = EtcdPlacementStore::new(
        PlacementPrefix::new("/lattice/test"),
        InMemoryEtcdClient::new(),
    );

    let first = store
        .campaign_coordinator_leader(InstanceId::new("coordinator-a"))
        .await
        .unwrap()
        .unwrap();
    let second = store
        .campaign_coordinator_leader(InstanceId::new("coordinator-b"))
        .await
        .unwrap();
    store.keepalive_coordinator_leader(&first).await.unwrap();
    store.resign_coordinator_leader(&first).await.unwrap();
    let third = store
        .campaign_coordinator_leader(InstanceId::new("coordinator-b"))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(first.candidate_id, InstanceId::new("coordinator-a"));
    assert_eq!(second, None);
    assert_eq!(third.candidate_id, InstanceId::new("coordinator-b"));
}

#[tokio::test]
async fn etcd_store_activation_lock_is_exclusive_until_release() {
    let store = EtcdPlacementStore::new(
        PlacementPrefix::new("/lattice/test"),
        InMemoryEtcdClient::new(),
    );
    let key = actor_key_for(7);

    let first = store.acquire_activation_lock(key.clone()).await.unwrap();
    let second = store.acquire_activation_lock(key.clone()).await;
    store.release_activation_lock(&key, first).await.unwrap();
    let third = store.acquire_activation_lock(key).await.unwrap();

    assert_eq!(first, LeaseId(1));
    assert_eq!(second, Err(PlacementError::ActivationLockHeld));
    assert_eq!(third, LeaseId(3));
}

#[tokio::test]
async fn etcd_store_singleton_lock_is_exclusive_until_release() {
    let store = EtcdPlacementStore::new(
        PlacementPrefix::new("/lattice/test"),
        InMemoryEtcdClient::new(),
    );
    let key = singleton_key_for("global");

    let first = store.acquire_singleton_lock(key.clone()).await.unwrap();
    let second = store.acquire_singleton_lock(key.clone()).await;
    store.release_singleton_lock(&key, first).await.unwrap();
    let third = store.acquire_singleton_lock(key).await.unwrap();

    assert_eq!(first, LeaseId(1));
    assert_eq!(second, Err(PlacementError::SingletonLockHeld));
    assert_eq!(third, LeaseId(3));
}

#[test]
fn etcd_store_builds_from_config() {
    let store = EtcdPlacementStore::in_memory_from_config(EtcdPlacementStoreConfig {
        key_prefix: "/lattice/test".to_string(),
        endpoints: vec!["http://127.0.0.1:2379".to_string()],
        instance_lease_ttl_secs: 30,
        activation_lock_ttl_secs: 30,
    });

    assert_eq!(store.prefix().as_str(), "/lattice/test");
    assert_eq!(
        EtcdPlacementStore::from_config().section(),
        "placement_store"
    );
}

#[test]
fn etcd_value_codec_round_trips_placement_metadata() {
    let instance = EtcdValue::Instance(Box::new(instance_record("world-a", InstanceState::Ready)));
    let actor = EtcdValue::Actor(Box::new(actor_record(7, "world-a", 3, LeaseId(5))));
    let singleton = EtcdValue::Singleton(Box::new(singleton_record(
        "global",
        "world-a",
        3,
        LeaseId(8),
    )));
    let leader = EtcdValue::CoordinatorLeader(Box::new(CoordinatorLeadership {
        candidate_id: InstanceId::new("coordinator-a"),
        lease_id: LeaseId(99),
    }));
    let lock = EtcdValue::ActivationLock(LeaseId(42));
    let singleton_lock = EtcdValue::SingletonLock(LeaseId(43));

    for value in [instance, actor, singleton, leader, lock, singleton_lock] {
        let encoded = encode_etcd_value(&value).unwrap();
        let decoded = decode_etcd_value(&encoded).unwrap();
        assert_eq!(decoded, value);
    }
}

#[test]
fn etcd_instance_records_are_written_with_their_instance_lease() {
    let instance = EtcdValue::Instance(Box::new(instance_record("world-a", InstanceState::Ready)));

    let options = put_options_for(&instance).unwrap();

    assert!(options.is_some());
}

#[test]
fn etcd_singleton_records_are_written_with_their_owner_lease() {
    let singleton = EtcdValue::Singleton(Box::new(singleton_record(
        "global",
        "world-a",
        1,
        LeaseId(7),
    )));

    let options = put_options_for(&singleton).unwrap();

    assert!(options.is_some());
}

fn actor_key_for(actor_id: u64) -> ActorPlacementKey {
    ActorPlacementKey {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(actor_id),
    }
}

fn vshard_key_for(shard_id: u32) -> VirtualShardPlacementKey {
    VirtualShardPlacementKey {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        shard_id: crate::sharding::VirtualShardId(shard_id),
    }
}

fn singleton_key_for(scope: &str) -> SingletonKey {
    SingletonKey {
        service_kind: service_kind!("World"),
        singleton_kind: actor_kind!("SeasonManager"),
        scope: scope.to_string(),
    }
}

fn instance_record(instance_id: &str, state: InstanceState) -> InstanceRecord {
    InstanceRecord {
        service_kind: service_kind!("World"),
        instance_id: InstanceId::new(instance_id),
        lease_id: LeaseId(1),
        advertised_endpoint: format!("http://{instance_id}.world:18080").parse().unwrap(),
        control_endpoint: format!("http://{instance_id}.world:18081").parse().unwrap(),
        version: "test".to_string(),
        state,
        capacity: InstanceCapacity::default(),
        labels: BTreeMap::new(),
    }
}

fn actor_record(actor_id: u64, owner: &str, epoch: u64, lease_id: LeaseId) -> ActorPlacementRecord {
    ActorPlacementRecord {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(actor_id),
        owner: InstanceId::new(owner),
        epoch: Epoch(epoch),
        lease_id,
        state: PlacementState::Running,
    }
}

fn vshard_record(shard_id: u32, owner: &str, epoch: u64) -> VirtualShardPlacementRecord {
    VirtualShardPlacementRecord {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        shard_id: crate::sharding::VirtualShardId(shard_id),
        owner: InstanceId::new(owner),
        epoch: Epoch(epoch),
    }
}

fn singleton_record(
    scope: &str,
    owner: &str,
    epoch: u64,
    lease_id: LeaseId,
) -> SingletonPlacementRecord {
    SingletonPlacementRecord {
        service_kind: service_kind!("World"),
        singleton_kind: actor_kind!("SeasonManager"),
        scope: scope.to_string(),
        owner: InstanceId::new(owner),
        epoch: Epoch(epoch),
        lease_id,
        state: PlacementState::Running,
    }
}
