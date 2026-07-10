use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use lattice_core::actor_ref::Epoch;
use lattice_core::id::ActorId;
use lattice_core::instance::InstanceCapacity;
use lattice_core::{actor_kind, service_kind};

use super::*;
use crate::registry::InstanceState;
use crate::storage::etcd::client::InMemoryEtcdMutation;
use crate::storage::etcd::codec::{decode_etcd_value, encode_etcd_value, put_options_for};
use crate::storage::{
    OwnershipViewError, OwnershipViewRecord, OwnershipWatchError, OwnershipWatchEvent,
    OwnershipWatchUpdate, PlacementRevision, PlacementState,
};

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
async fn etcd_ownership_view_uses_mod_revisions_and_subscribes_to_later_changes() {
    let client = InMemoryEtcdClient::new();
    let store = EtcdPlacementStore::new(PlacementPrefix::new("/lattice/test"), client);
    store
        .upsert_instance(instance_record("world-a", InstanceState::Ready))
        .await
        .unwrap();
    let key = actor_key_for(7);
    let first = actor_record(7, "world-a", 1, LeaseId(1));
    let first_version = store
        .compare_and_put_actor(key.clone(), None, first)
        .await
        .unwrap();
    let current = actor_record(7, "world-a", 2, LeaseId(1));
    let current_version = store
        .compare_and_put_actor(key.clone(), Some(first_version), current.clone())
        .await
        .unwrap();
    store
        .compare_and_put_actor(
            actor_key_for(8),
            None,
            actor_record(8, "world-b", 1, LeaseId(2)),
        )
        .await
        .unwrap();
    store
        .compare_and_put_virtual_shard(vshard_key_for(3), None, vshard_record(3, "world-a", 1))
        .await
        .unwrap();

    let mut view = store
        .open_ownership_view(
            &service_kind!("World"),
            &InstanceId::new("world-a"),
            NonZeroUsize::new(8).unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(view.snapshot.revision, PlacementRevision(5));
    assert_eq!(
        view.snapshot
            .local_instance
            .as_ref()
            .map(|record| &record.instance_id),
        Some(&InstanceId::new("world-a"))
    );
    let actor_revision = view
        .snapshot
        .records
        .iter()
        .find_map(|record| match record {
            OwnershipViewRecord::Actor { revision, record }
                if record.actor_id == ActorId::U64(7) =>
            {
                Some(*revision)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(current_version, PlacementVersion(2));
    assert_eq!(actor_revision, PlacementRevision(3));
    assert_ne!(actor_revision.0, current_version.0);
    assert_eq!(view.snapshot.records.len(), 2);

    let moved = actor_record(7, "world-b", 3, LeaseId(2));
    store
        .compare_and_put_actor(key.clone(), Some(current_version), moved.clone())
        .await
        .unwrap();
    let batch = view.watch.next().await.unwrap();
    assert_eq!(batch.revision, PlacementRevision(6));
    assert_eq!(
        batch.events,
        vec![OwnershipWatchEvent::ActorUpserted { key, record: moved }]
    );
}

#[tokio::test]
async fn etcd_ownership_watch_preserves_same_revision_batches() {
    let client = InMemoryEtcdClient::new();
    let store = EtcdPlacementStore::new(PlacementPrefix::new("/lattice/test"), client.clone());
    let mut view = store
        .open_ownership_view(
            &service_kind!("World"),
            &InstanceId::new("world-a"),
            NonZeroUsize::new(8).unwrap(),
        )
        .await
        .unwrap();
    let actor_placement_key = actor_key_for(7);
    let shard_key = vshard_key_for(3);
    let revision = client
        .put_same_revision_for_test(vec![
            (
                actor_key(&store.prefix, &actor_placement_key),
                EtcdValue::Actor(Box::new(actor_record(7, "world-a", 1, LeaseId(1)))),
            ),
            (
                vshard_key(&store.prefix, &shard_key),
                EtcdValue::VirtualShard(Box::new(vshard_record(3, "world-a", 1))),
            ),
        ])
        .unwrap();

    let batch = view.watch.next().await.unwrap();
    assert_eq!(batch.revision, revision);
    assert_eq!(batch.events.len(), 2);
    assert!(batch.events.iter().any(|event| matches!(
        event,
        OwnershipWatchEvent::ActorUpserted { key, .. } if key == &actor_placement_key
    )));
    assert!(batch.events.iter().any(|event| matches!(
        event,
        OwnershipWatchEvent::VirtualShardUpserted { key, .. } if key == &shard_key
    )));
}

#[tokio::test]
async fn etcd_ownership_watch_preserves_mixed_put_delete_revision_batches() {
    let client = InMemoryEtcdClient::new();
    let store = EtcdPlacementStore::new(PlacementPrefix::new("/lattice/test"), client.clone());
    let actor = actor_key_for(7);
    store
        .compare_and_put_actor(
            actor.clone(),
            None,
            actor_record(7, "world-a", 1, LeaseId(1)),
        )
        .await
        .unwrap();
    let mut view = store
        .open_ownership_view(
            &service_kind!("World"),
            &InstanceId::new("world-a"),
            NonZeroUsize::new(8).unwrap(),
        )
        .await
        .unwrap();
    let shard = vshard_key_for(3);
    let revision = client
        .mutate_same_revision_for_test(vec![
            InMemoryEtcdMutation::Delete {
                key: actor_key(&store.prefix, &actor),
            },
            InMemoryEtcdMutation::Put {
                key: vshard_key(&store.prefix, &shard),
                value: EtcdValue::VirtualShard(Box::new(vshard_record(3, "world-a", 1))),
            },
        ])
        .unwrap();

    let batch = view.watch.next().await.unwrap();
    assert_eq!(batch.revision, revision);
    assert_eq!(
        batch.events,
        vec![
            OwnershipWatchEvent::ActorDeleted { key: actor },
            OwnershipWatchEvent::VirtualShardUpserted {
                key: shard,
                record: vshard_record(3, "world-a", 1),
            },
        ]
    );
}

#[tokio::test]
async fn etcd_ownership_watch_reports_record_backed_deletes() {
    let client = InMemoryEtcdClient::new();
    let store = EtcdPlacementStore::new(PlacementPrefix::new("/lattice/test"), client.clone());
    let instance = instance_record("world-a", InstanceState::Ready);
    store.upsert_instance(instance.clone()).await.unwrap();
    let key = actor_key_for(7);
    store
        .compare_and_put_actor(key.clone(), None, actor_record(7, "world-a", 1, LeaseId(1)))
        .await
        .unwrap();
    let mut view = store
        .open_ownership_view(
            &service_kind!("World"),
            &InstanceId::new("world-a"),
            NonZeroUsize::new(8).unwrap(),
        )
        .await
        .unwrap();

    client
        .delete(&actor_key(&store.prefix, &key))
        .await
        .unwrap();
    assert_eq!(
        view.watch.next().await.unwrap().events,
        vec![OwnershipWatchEvent::ActorDeleted { key: key.clone() }]
    );

    client
        .delete(&instance_key(
            &store.prefix,
            &instance.service_kind,
            &instance.instance_id,
        ))
        .await
        .unwrap();
    assert_eq!(
        view.watch.next().await.unwrap().events,
        vec![OwnershipWatchEvent::InstanceDeleted { record: instance }]
    );
}

#[tokio::test]
async fn in_memory_etcd_failed_cas_does_not_advance_and_recreate_resets_only_key_version() {
    let client = InMemoryEtcdClient::new();
    let store = EtcdPlacementStore::new(PlacementPrefix::new("/lattice/test"), client.clone());
    let key = actor_key_for(7);
    let record = actor_record(7, "world-a", 1, LeaseId(1));
    let first_version = store
        .compare_and_put_actor(key.clone(), None, record.clone())
        .await
        .unwrap();
    let mut view = store
        .open_ownership_view(
            &service_kind!("World"),
            &InstanceId::new("world-a"),
            NonZeroUsize::new(8).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .compare_and_put_actor(key.clone(), None, record.clone())
            .await,
        Err(PlacementError::CompareAndPutFailed)
    );
    store
        .compare_and_put_virtual_shard(vshard_key_for(3), None, vshard_record(3, "world-a", 1))
        .await
        .unwrap();
    assert_eq!(first_version, PlacementVersion(1));
    assert_eq!(view.snapshot.revision, PlacementRevision(1));
    let next = view.watch.next().await.unwrap();
    assert_eq!(next.revision, PlacementRevision(2));
    assert!(matches!(
        next.events.as_slice(),
        [OwnershipWatchEvent::VirtualShardUpserted { .. }]
    ));

    client
        .delete(&actor_key(&store.prefix, &key))
        .await
        .unwrap();
    assert_eq!(
        view.watch.next().await.unwrap().revision,
        PlacementRevision(3)
    );
    let recreated_version = store
        .compare_and_put_actor(key, None, record)
        .await
        .unwrap();
    assert_eq!(recreated_version, PlacementVersion(1));
    assert_eq!(
        view.watch.next().await.unwrap().revision,
        PlacementRevision(4)
    );
}

#[tokio::test]
async fn etcd_ownership_view_bounds_scanned_service_records() {
    let store = EtcdPlacementStore::new(
        PlacementPrefix::new("/lattice/test"),
        InMemoryEtcdClient::new(),
    );
    for actor_id in 1..=2 {
        store
            .compare_and_put_actor(
                actor_key_for(actor_id),
                None,
                actor_record(actor_id, "world-a", 1, LeaseId(1)),
            )
            .await
            .unwrap();
    }

    let error = store
        .open_ownership_view(
            &service_kind!("World"),
            &InstanceId::new("world-a"),
            NonZeroUsize::new(1).unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error,
        OwnershipViewError::CapacityExceeded { max_entries: 1 }
    );
}

#[tokio::test]
async fn etcd_ownership_watch_bounds_each_revision_batch() {
    let client = InMemoryEtcdClient::new();
    let store = EtcdPlacementStore::new(PlacementPrefix::new("/lattice/test"), client.clone());
    let mut view = store
        .open_ownership_view(
            &service_kind!("World"),
            &InstanceId::new("world-a"),
            NonZeroUsize::new(1).unwrap(),
        )
        .await
        .unwrap();
    client
        .put_same_revision_for_test(vec![
            (
                actor_key(&store.prefix, &actor_key_for(1)),
                EtcdValue::Actor(Box::new(actor_record(1, "world-a", 1, LeaseId(1)))),
            ),
            (
                actor_key(&store.prefix, &actor_key_for(2)),
                EtcdValue::Actor(Box::new(actor_record(2, "world-a", 1, LeaseId(1)))),
            ),
        ])
        .unwrap();

    assert_eq!(
        view.watch.next_update().await,
        Err(OwnershipWatchError::CapacityExceeded { max_entries: 1 })
    );
}

#[tokio::test]
async fn etcd_ownership_watch_surfaces_progress_and_exact_terminal_failures() {
    let client = InMemoryEtcdClient::new();
    let store = EtcdPlacementStore::new(PlacementPrefix::new("/lattice/test"), client.clone());
    let mut view = store
        .open_ownership_view(
            &service_kind!("World"),
            &InstanceId::new("world-a"),
            NonZeroUsize::new(8).unwrap(),
        )
        .await
        .unwrap();
    client.progress_ownership_watches_for_test(PlacementRevision(7));
    assert_eq!(
        view.watch.next_update().await.unwrap(),
        OwnershipWatchUpdate::Progress {
            revision: PlacementRevision(7)
        }
    );

    drop(view);

    for failure in [
        OwnershipWatchError::Backend {
            message: "transport lost".to_string(),
        },
        OwnershipWatchError::Canceled {
            reason: "permission revoked".to_string(),
        },
        OwnershipWatchError::Protocol {
            message: "missing prev_kv".to_string(),
        },
        OwnershipWatchError::Compacted {
            requested_revision: PlacementRevision(3),
            compact_revision: PlacementRevision(5),
        },
    ] {
        let client = InMemoryEtcdClient::new();
        let store = EtcdPlacementStore::new(PlacementPrefix::new("/lattice/test"), client.clone());
        let mut view = store
            .open_ownership_view(
                &service_kind!("World"),
                &InstanceId::new("world-a"),
                NonZeroUsize::new(8).unwrap(),
            )
            .await
            .unwrap();
        client.fail_ownership_watches_for_test(failure.clone());
        assert_eq!(view.watch.next_update().await, Err(failure));
    }
}

#[tokio::test]
async fn etcd_ownership_watch_treats_valid_lock_mutations_as_progress() {
    let store = EtcdPlacementStore::new(
        PlacementPrefix::new("/lattice/test"),
        InMemoryEtcdClient::new(),
    );
    let mut view = store
        .open_ownership_view(
            &service_kind!("World"),
            &InstanceId::new("world-a"),
            NonZeroUsize::new(8).unwrap(),
        )
        .await
        .unwrap();
    store
        .acquire_activation_lock(actor_key_for(7))
        .await
        .unwrap();

    assert_eq!(
        view.watch.next_update().await.unwrap(),
        OwnershipWatchUpdate::Progress {
            revision: PlacementRevision(1)
        }
    );
}

#[tokio::test]
async fn etcd_ownership_watch_rejects_batches_behind_a_progress_barrier() {
    let client = InMemoryEtcdClient::new();
    let store = EtcdPlacementStore::new(PlacementPrefix::new("/lattice/test"), client.clone());
    let mut view = store
        .open_ownership_view(
            &service_kind!("World"),
            &InstanceId::new("world-a"),
            NonZeroUsize::new(8).unwrap(),
        )
        .await
        .unwrap();
    client.progress_ownership_watches_for_test(PlacementRevision(7));
    assert_eq!(
        view.watch.next_update().await.unwrap(),
        OwnershipWatchUpdate::Progress {
            revision: PlacementRevision(7)
        }
    );
    store
        .compare_and_put_actor(
            actor_key_for(7),
            None,
            actor_record(7, "world-a", 1, LeaseId(1)),
        )
        .await
        .unwrap();

    let error = view.watch.next_update().await.unwrap_err();
    assert!(
        matches!(error, OwnershipWatchError::Protocol { message } if message.contains("did not advance"))
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
fn etcd_actor_records_remain_durable_for_epoch_preserving_failover() {
    let actor = EtcdValue::Actor(Box::new(actor_record(7, "world-a", 3, LeaseId(5))));

    let options = put_options_for(&actor).unwrap();

    assert!(options.is_none());
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
