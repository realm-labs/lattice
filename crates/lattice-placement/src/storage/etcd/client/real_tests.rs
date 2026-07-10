use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use etcd_client::{Client, CompactionOptions, DeleteOptions, Txn, TxnOp};
use lattice_core::actor_ref::Epoch;
use lattice_core::id::ActorId;
use lattice_core::instance::InstanceId;
use lattice_core::{actor_kind, service_kind};
use tokio::sync::Barrier;
use tokio::time::timeout;

use super::*;
use crate::sharding::VirtualShardId;
use crate::storage::etcd::EtcdPlacementStore;
use crate::storage::etcd::codec::{
    actor_key, encode_etcd_value, epoch_floor_key, singleton_key, vshard_key,
};
use crate::storage::{
    ActorPlacementKey, ActorPlacementRecord, EpochFloorRecord, OwnershipWatch, OwnershipWatchBatch,
    OwnershipWatchEvent, OwnershipWatchUpdate, PlacementEpochKey, PlacementEpochReservation,
    PlacementPrefix, PlacementState, PlacementStore, SingletonKey, SingletonPlacementRecord,
    VirtualShardPlacementKey, VirtualShardPlacementRecord,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(10);
const TEST_ETCD_ENDPOINT: &str = "LATTICE_TEST_ETCD_ENDPOINT";

#[test]
fn real_watch_startup_buffer_reports_its_own_bounded_backlog_error() {
    let mut updates = Vec::new();
    for revision in 0..WATCH_CAPACITY {
        push_startup_update(
            &mut updates,
            EtcdOwnershipWatchUpdate::Progress {
                revision: PlacementRevision(revision as u64),
            },
        )
        .expect("startup buffer should accept its advertised capacity");
    }
    assert_eq!(
        push_startup_update(
            &mut updates,
            EtcdOwnershipWatchUpdate::Progress {
                revision: PlacementRevision(WATCH_CAPACITY as u64),
            },
        ),
        Err(OwnershipWatchError::StartupBacklogExceeded {
            max_updates: WATCH_CAPACITY,
        })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a real etcd endpoint in LATTICE_TEST_ETCD_ENDPOINT"]
async fn real_etcd_ownership_view_covers_gap_progress_deletes_batches_and_compaction() {
    let endpoint = std::env::var(TEST_ETCD_ENDPOINT)
        .unwrap_or_else(|_| panic!("set {TEST_ETCD_ENDPOINT} to a real etcd endpoint"));
    assert!(
        !endpoint.trim().is_empty(),
        "{TEST_ETCD_ENDPOINT} must not be blank"
    );
    let endpoints = vec![endpoint];
    let mut raw = Client::connect(endpoints.clone(), None)
        .await
        .expect("connect raw real-etcd test client");

    let namespace = unique_namespace("ownership");
    delete_namespace(&mut raw, &namespace).await;
    let prefix = PlacementPrefix::new(namespace.clone());
    let service = service_kind!("World");
    let owner = InstanceId::new("world-a");
    let actor = ActorPlacementKey {
        service_kind: service.clone(),
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(7),
    };
    let actor_path = actor_key(&prefix, &actor);
    let initial = actor_record(7, "world-a", 1);
    let initial_revision = put_value(
        &mut raw,
        actor_path.clone(),
        EtcdValue::Actor(Box::new(initial.clone())),
    )
    .await;

    let gap = Arc::new(Barrier::new(2));
    let mut real = RealEtcdClient::connect(
        endpoints.clone(),
        InstanceLeaseTtl::new(30),
        ActivationLockTtl::new(30),
    )
    .await
    .expect("connect ownership-view client");
    real.ownership_view_gap = Some(gap.clone());
    let store = EtcdPlacementStore::new(prefix.clone(), real);
    let open = tokio::spawn({
        let service = service.clone();
        let owner = owner.clone();
        async move {
            store
                .open_ownership_view(&service, &owner, NonZeroUsize::new(16).unwrap())
                .await
        }
    });

    timeout(TEST_TIMEOUT, gap.wait())
        .await
        .expect("snapshot transaction did not reach the deterministic gap");
    let changed = actor_record(7, "world-a", 2);
    let gap_revision = put_value(
        &mut raw,
        actor_path.clone(),
        EtcdValue::Actor(Box::new(changed.clone())),
    )
    .await;
    assert!(gap_revision > initial_revision);
    timeout(TEST_TIMEOUT, gap.wait())
        .await
        .expect("failed to release the deterministic snapshot/watch gap");

    // open_ownership_view only completes after etcd has acknowledged Created.
    // The mutation committed in the gap must then be replayed by its R+1 watch.
    let mut view = timeout(TEST_TIMEOUT, open)
        .await
        .expect("ownership view did not complete its Created handshake")
        .expect("ownership-view task panicked")
        .expect("open real-etcd ownership view");
    assert_eq!(view.snapshot.revision, PlacementRevision(initial_revision));
    assert!(view.snapshot.records.iter().any(|record| matches!(
        record,
        crate::storage::OwnershipViewRecord::Actor { revision, record }
            if *revision == PlacementRevision(initial_revision) && record == &initial
    )));

    let (historical, progress) = next_batch_and_progress(&mut view.watch).await;
    assert_eq!(historical.revision, PlacementRevision(gap_revision));
    assert_eq!(
        historical.events,
        vec![OwnershipWatchEvent::ActorUpserted {
            key: actor.clone(),
            record: changed.clone(),
        }]
    );

    // The real client explicitly requests this event-free progress barrier
    // after Created; etcd may deliver it immediately before or after historical
    // replay, so collect both without imposing an ordering not promised by etcd.
    assert!(progress >= PlacementRevision(gap_revision));

    let delete = raw
        .delete(actor_path, None)
        .await
        .expect("delete watched actor");
    let delete_revision = response_revision(delete.header(), "delete");
    let deleted = next_batch(&mut view.watch).await;
    assert_eq!(deleted.revision, PlacementRevision(delete_revision));
    assert_eq!(
        deleted.events,
        vec![OwnershipWatchEvent::ActorDeleted { key: actor }]
    );

    let second_actor = ActorPlacementKey {
        service_kind: service.clone(),
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(8),
    };
    let second_record = actor_record(8, "world-a", 1);
    let shard = VirtualShardPlacementKey {
        service_kind: service.clone(),
        actor_kind: actor_kind!("World"),
        shard_id: VirtualShardId(3),
    };
    let shard_record = VirtualShardPlacementRecord {
        service_kind: service,
        actor_kind: actor_kind!("World"),
        shard_id: VirtualShardId(3),
        owner,
        epoch: Epoch(1),
    };
    let txn = Txn::new().and_then(vec![
        TxnOp::put(
            actor_key(&prefix, &second_actor),
            encode_etcd_value(&EtcdValue::Actor(Box::new(second_record.clone())))
                .expect("encode actor transaction value"),
            None,
        ),
        TxnOp::put(
            vshard_key(&prefix, &shard),
            encode_etcd_value(&EtcdValue::VirtualShard(Box::new(shard_record.clone())))
                .expect("encode shard transaction value"),
            None,
        ),
    ]);
    let transaction = raw
        .txn(txn)
        .await
        .expect("commit same-revision transaction");
    let transaction_revision = response_revision(transaction.header(), "transaction");
    let batch = next_batch(&mut view.watch).await;
    assert_eq!(batch.revision, PlacementRevision(transaction_revision));
    assert_eq!(batch.events.len(), 2);
    assert!(batch.events.contains(&OwnershipWatchEvent::ActorUpserted {
        key: second_actor,
        record: second_record,
    }));
    assert!(
        batch
            .events
            .contains(&OwnershipWatchEvent::VirtualShardUpserted {
                key: shard,
                record: shard_record,
            })
    );
    drop(view);
    delete_namespace(&mut raw, &namespace).await;

    let compact_namespace = unique_namespace("compaction");
    delete_namespace(&mut raw, &compact_namespace).await;
    let compact_prefix = PlacementPrefix::new(compact_namespace.clone());
    let compact_actor = ActorPlacementKey {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(99),
    };
    let compact_path = actor_key(&compact_prefix, &compact_actor);
    let compact_initial_revision = put_value(
        &mut raw,
        compact_path.clone(),
        EtcdValue::Actor(Box::new(actor_record(99, "world-a", 1))),
    )
    .await;
    let compact_gap = Arc::new(Barrier::new(2));
    let mut compact_real = RealEtcdClient::connect(
        endpoints,
        InstanceLeaseTtl::new(30),
        ActivationLockTtl::new(30),
    )
    .await
    .expect("connect compaction ownership-view client");
    compact_real.ownership_view_gap = Some(compact_gap.clone());
    let compact_store = EtcdPlacementStore::new(compact_prefix, compact_real);
    let compact_open = tokio::spawn(async move {
        compact_store
            .open_ownership_view(
                &service_kind!("World"),
                &InstanceId::new("world-a"),
                NonZeroUsize::new(16).unwrap(),
            )
            .await
    });

    timeout(TEST_TIMEOUT, compact_gap.wait())
        .await
        .expect("compaction snapshot did not reach the deterministic gap");
    let first_compacted_revision = put_value(
        &mut raw,
        compact_path.clone(),
        EtcdValue::Actor(Box::new(actor_record(99, "world-a", 2))),
    )
    .await;
    assert!(first_compacted_revision > compact_initial_revision);
    // etcd still permits a watch beginning exactly at the compaction
    // revision. Advance once more so the requested R+1 revision is strictly
    // behind the compaction boundary and must be canceled immediately.
    let compact_revision = put_value(
        &mut raw,
        compact_path,
        EtcdValue::Actor(Box::new(actor_record(99, "world-a", 3))),
    )
    .await;
    assert!(compact_revision > first_compacted_revision);
    raw.compact(
        i64::try_from(compact_revision).expect("compaction revision must fit i64"),
        Some(CompactionOptions::new().with_physical()),
    )
    .await
    .expect("compact the requested watch revision");
    timeout(TEST_TIMEOUT, compact_gap.wait())
        .await
        .expect("failed to release the compaction snapshot/watch gap");

    let error = timeout(TEST_TIMEOUT, compact_open)
        .await
        .expect("compacted ownership view did not finish")
        .expect("compacted ownership-view task panicked")
        .expect_err("compacted R+1 watch must fail closed during open");
    assert_eq!(
        error,
        OwnershipViewError::WatchStart {
            error: OwnershipWatchError::Compacted {
                requested_revision: PlacementRevision(compact_initial_revision + 1),
                compact_revision: PlacementRevision(compact_revision),
            },
        }
    );
    delete_namespace(&mut raw, &compact_namespace).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a real etcd endpoint in LATTICE_TEST_ETCD_ENDPOINT"]
async fn real_etcd_epoch_floors_fence_delete_restart_guards_concurrency_and_overflow() {
    let endpoint = std::env::var(TEST_ETCD_ENDPOINT)
        .unwrap_or_else(|_| panic!("set {TEST_ETCD_ENDPOINT} to a real etcd endpoint"));
    assert!(
        !endpoint.trim().is_empty(),
        "{TEST_ETCD_ENDPOINT} must not be blank"
    );
    let endpoints = vec![endpoint];
    let mut raw = Client::connect(endpoints.clone(), None)
        .await
        .expect("connect raw real-etcd epoch test client");
    let namespace = unique_namespace("epoch-floors");
    delete_namespace(&mut raw, &namespace).await;
    let prefix = PlacementPrefix::new(namespace.clone());
    let real = RealEtcdClient::connect(
        endpoints.clone(),
        InstanceLeaseTtl::new(30),
        ActivationLockTtl::new(30),
    )
    .await
    .expect("connect epoch placement client");
    let inspector = real.clone();
    let store = EtcdPlacementStore::new(prefix.clone(), real);
    let actor = actor_key_for_real_test(7);
    let shard = vshard_key_for_real_test(3);
    let singleton = singleton_key_for_real_test("global");
    let singleton_lease = store
        .grant_instance_lease()
        .await
        .expect("grant singleton owner lease");

    let actor_token = store
        .compare_and_put_actor(actor.clone(), None, actor_record(7, "world-a", 5))
        .await
        .expect("seed actor floor and record");
    let shard_token = store
        .compare_and_put_virtual_shard(
            shard.clone(),
            None,
            vshard_record_for_real_test(3, "world-a", 5),
        )
        .await
        .expect("seed shard floor and record");
    let singleton_token = store
        .compare_and_put_singleton(
            singleton.clone(),
            None,
            singleton_record_for_real_test("global", "world-a", 5, singleton_lease),
        )
        .await
        .expect("seed leased singleton floor and record");

    for (record_token, floor_key) in [
        (actor_token, PlacementEpochKey::Actor(actor.clone())),
        (shard_token, PlacementEpochKey::VirtualShard(shard.clone())),
        (
            singleton_token,
            PlacementEpochKey::Singleton(singleton.clone()),
        ),
    ] {
        let (floor_token, _) = inspector
            .get(&epoch_floor_key(&prefix, &floor_key))
            .await
            .expect("read committed real-etcd floor")
            .expect("committed real-etcd floor must exist");
        assert_eq!(floor_token, record_token);
    }

    raw.delete(actor_key(&prefix, &actor), None)
        .await
        .expect("delete real-etcd actor record");
    raw.delete(vshard_key(&prefix, &shard), None)
        .await
        .expect("delete real-etcd shard record");
    raw.lease_revoke(
        i64::try_from(singleton_lease.0).expect("singleton lease must fit real-etcd i64"),
    )
    .await
    .expect("revoke singleton owner lease");
    assert!(
        inspector
            .get(&singleton_key(&prefix, &singleton))
            .await
            .expect("read singleton after lease revoke")
            .is_none()
    );
    assert!(
        inspector
            .get(&epoch_floor_key(
                &prefix,
                &PlacementEpochKey::Singleton(singleton.clone()),
            ))
            .await
            .expect("read singleton floor after lease revoke")
            .is_some(),
        "the singleton floor must not share its placement lease"
    );
    drop(store);

    let reconstructed_client = RealEtcdClient::connect(
        endpoints.clone(),
        InstanceLeaseTtl::new(30),
        ActivationLockTtl::new(30),
    )
    .await
    .expect("reconstruct real-etcd epoch placement client");
    let reconstructed_inspector = reconstructed_client.clone();
    let reconstructed = EtcdPlacementStore::new(prefix.clone(), reconstructed_client);
    let replacement_lease = reconstructed
        .grant_instance_lease()
        .await
        .expect("grant replacement singleton lease");

    assert_eq!(
        reconstructed
            .compare_and_put_actor(
                actor.clone(),
                Some(actor_token),
                actor_record(7, "world-b", 6),
            )
            .await,
        Err(PlacementError::CompareAndPutFailed)
    );
    assert_eq!(
        reconstructed
            .compare_and_put_virtual_shard(
                shard.clone(),
                Some(shard_token),
                vshard_record_for_real_test(3, "world-b", 6),
            )
            .await,
        Err(PlacementError::CompareAndPutFailed)
    );
    assert_eq!(
        reconstructed
            .compare_and_put_singleton(
                singleton.clone(),
                Some(singleton_token),
                singleton_record_for_real_test("global", "world-b", 6, replacement_lease,),
            )
            .await,
        Err(PlacementError::CompareAndPutFailed)
    );

    let actor_reservation = reconstructed
        .reserve_actor_epoch(actor.clone(), None, None)
        .await
        .expect("reserve post-delete actor epoch");
    let shard_reservation = reconstructed
        .reserve_virtual_shard_epoch(shard.clone(), None)
        .await
        .expect("reserve post-delete shard epoch");
    let singleton_reservation = reconstructed
        .reserve_singleton_epoch(singleton.clone(), None, None)
        .await
        .expect("reserve post-delete singleton epoch");
    assert_eq!(actor_reservation.epoch(), Epoch(6));
    assert_eq!(shard_reservation.epoch(), Epoch(6));
    assert_eq!(singleton_reservation.epoch(), Epoch(6));
    let actor_committed = reconstructed
        .commit_actor_epoch(actor_reservation, actor_record(7, "world-b", 6))
        .await
        .expect("commit post-delete actor");
    let shard_committed = reconstructed
        .commit_virtual_shard_epoch(
            shard_reservation,
            vshard_record_for_real_test(3, "world-b", 6),
        )
        .await
        .expect("commit post-delete shard");
    let singleton_committed = reconstructed
        .commit_singleton_epoch(
            singleton_reservation,
            singleton_record_for_real_test("global", "world-b", 6, replacement_lease),
        )
        .await
        .expect("commit post-delete singleton");
    for (record_token, floor_key) in [
        (actor_committed, PlacementEpochKey::Actor(actor.clone())),
        (
            shard_committed,
            PlacementEpochKey::VirtualShard(shard.clone()),
        ),
        (
            singleton_committed,
            PlacementEpochKey::Singleton(singleton.clone()),
        ),
    ] {
        let floor_token = reconstructed_inspector
            .get(&epoch_floor_key(&prefix, &floor_key))
            .await
            .expect("read reconstructed floor")
            .expect("reconstructed floor must exist")
            .0;
        assert_eq!(floor_token, record_token);
    }

    assert_eq!(
        reconstructed
            .compare_and_put_actor(
                actor.clone(),
                Some(actor_token),
                actor_record(7, "world-c", 7),
            )
            .await,
        Err(PlacementError::CompareAndPutFailed),
        "a pre-delete actor token must not match the recreated key",
    );
    assert_eq!(
        reconstructed
            .compare_and_put_virtual_shard(
                shard.clone(),
                Some(shard_token),
                vshard_record_for_real_test(3, "world-c", 7),
            )
            .await,
        Err(PlacementError::CompareAndPutFailed),
        "a pre-delete shard token must not match the recreated key",
    );
    assert_eq!(
        reconstructed
            .compare_and_put_singleton(
                singleton.clone(),
                Some(singleton_token),
                singleton_record_for_real_test("global", "world-c", 7, replacement_lease),
            )
            .await,
        Err(PlacementError::CompareAndPutFailed),
        "a pre-delete singleton token must not match the recreated key",
    );

    let guarded_actor = actor_key_for_real_test(8);
    let activation_lock = reconstructed
        .acquire_activation_lock(guarded_actor.clone())
        .await
        .expect("acquire real-etcd activation lock");
    let guarded_actor_reservation = reconstructed
        .reserve_actor_epoch(guarded_actor.clone(), None, Some(activation_lock))
        .await
        .expect("reserve guarded actor epoch");
    reconstructed
        .release_activation_lock(&guarded_actor, activation_lock)
        .await
        .expect("release real-etcd activation lock");
    assert_eq!(
        reconstructed
            .commit_actor_epoch(guarded_actor_reservation, actor_record(8, "world-a", 1),)
            .await,
        Err(PlacementError::CompareAndPutFailed)
    );

    let guarded_singleton = singleton_key_for_real_test("guarded");
    let singleton_lock = reconstructed
        .acquire_singleton_lock(guarded_singleton.clone())
        .await
        .expect("acquire real-etcd singleton lock");
    let guarded_singleton_reservation = reconstructed
        .reserve_singleton_epoch(guarded_singleton.clone(), None, Some(singleton_lock))
        .await
        .expect("reserve guarded singleton epoch");
    reconstructed
        .release_singleton_lock(&guarded_singleton, singleton_lock)
        .await
        .expect("release real-etcd singleton lock");
    assert_eq!(
        reconstructed
            .commit_singleton_epoch(
                guarded_singleton_reservation,
                singleton_record_for_real_test("guarded", "world-a", 1, replacement_lease,),
            )
            .await,
        Err(PlacementError::CompareAndPutFailed)
    );

    let racing_actor = actor_key_for_real_test(70);
    let (left, right) = tokio::join!(
        reconstructed.reserve_actor_epoch(racing_actor.clone(), None, None),
        reconstructed.reserve_actor_epoch(racing_actor.clone(), None, None),
    );
    let mut actor_reservations = successful_real_reservations(left, right);
    let actor_winner = actor_reservations.pop().unwrap();
    for loser in actor_reservations {
        let epoch = loser.epoch().0;
        assert_eq!(
            reconstructed
                .commit_actor_epoch(loser, actor_record(70, "world-a", epoch))
                .await,
            Err(PlacementError::CompareAndPutFailed)
        );
    }
    let actor_epoch = actor_winner.epoch().0;
    reconstructed
        .commit_actor_epoch(actor_winner, actor_record(70, "world-a", actor_epoch))
        .await
        .expect("commit winning concurrent actor reservation");

    let racing_shard = vshard_key_for_real_test(30);
    let (left, right) = tokio::join!(
        reconstructed.reserve_virtual_shard_epoch(racing_shard.clone(), None),
        reconstructed.reserve_virtual_shard_epoch(racing_shard.clone(), None),
    );
    let mut shard_reservations = successful_real_reservations(left, right);
    let shard_winner = shard_reservations.pop().unwrap();
    for loser in shard_reservations {
        let epoch = loser.epoch().0;
        assert_eq!(
            reconstructed
                .commit_virtual_shard_epoch(
                    loser,
                    vshard_record_for_real_test(30, "world-a", epoch),
                )
                .await,
            Err(PlacementError::CompareAndPutFailed)
        );
    }
    let shard_epoch = shard_winner.epoch().0;
    reconstructed
        .commit_virtual_shard_epoch(
            shard_winner,
            vshard_record_for_real_test(30, "world-a", shard_epoch),
        )
        .await
        .expect("commit winning concurrent shard reservation");

    let racing_singleton = singleton_key_for_real_test("racing");
    let (left, right) = tokio::join!(
        reconstructed.reserve_singleton_epoch(racing_singleton.clone(), None, None),
        reconstructed.reserve_singleton_epoch(racing_singleton.clone(), None, None),
    );
    let mut singleton_reservations = successful_real_reservations(left, right);
    let singleton_winner = singleton_reservations.pop().unwrap();
    for loser in singleton_reservations {
        let epoch = loser.epoch().0;
        assert_eq!(
            reconstructed
                .commit_singleton_epoch(
                    loser,
                    singleton_record_for_real_test("racing", "world-a", epoch, replacement_lease,),
                )
                .await,
            Err(PlacementError::CompareAndPutFailed)
        );
    }
    let singleton_epoch = singleton_winner.epoch().0;
    reconstructed
        .commit_singleton_epoch(
            singleton_winner,
            singleton_record_for_real_test("racing", "world-a", singleton_epoch, replacement_lease),
        )
        .await
        .expect("commit winning concurrent singleton reservation");

    let exhausted_actor = actor_key_for_real_test(99);
    let exhausted_key = PlacementEpochKey::Actor(exhausted_actor.clone());
    put_value(
        &mut raw,
        epoch_floor_key(&prefix, &exhausted_key),
        EtcdValue::EpochFloor(Box::new(EpochFloorRecord {
            key: exhausted_key,
            epoch: Epoch(u64::MAX),
        })),
    )
    .await;
    assert!(matches!(
        reconstructed
            .reserve_actor_epoch(exhausted_actor, None, None)
            .await,
        Err(PlacementError::EpochExhausted)
    ));

    delete_namespace(&mut raw, &namespace).await;
}

async fn next_batch(watch: &mut OwnershipWatch) -> OwnershipWatchBatch {
    timeout(TEST_TIMEOUT, async {
        loop {
            let update = watch
                .next_update()
                .await
                .expect("real-etcd ownership watch failed");
            match update {
                OwnershipWatchUpdate::Batch(batch) => return batch,
                OwnershipWatchUpdate::Progress { .. } => {}
            }
        }
    })
    .await
    .expect("timed out waiting for a real-etcd ownership batch")
}

async fn next_batch_and_progress(
    watch: &mut OwnershipWatch,
) -> (OwnershipWatchBatch, PlacementRevision) {
    timeout(TEST_TIMEOUT, async {
        let mut batch = None;
        let mut progress = None;
        loop {
            let update = watch
                .next_update()
                .await
                .expect("real-etcd ownership watch failed");
            match update {
                OwnershipWatchUpdate::Progress { revision } => progress = Some(revision),
                OwnershipWatchUpdate::Batch(update) => {
                    assert!(
                        batch.replace(update).is_none(),
                        "received two historical batches"
                    );
                }
            }
            if let Some(progress) = progress
                && let Some(batch) = batch.take()
            {
                return (batch, progress);
            }
        }
    })
    .await
    .expect("timed out waiting for real-etcd historical replay and progress")
}

async fn put_value(client: &mut Client, key: String, value: EtcdValue) -> u64 {
    let response = client
        .put(
            key,
            encode_etcd_value(&value).expect("encode real-etcd test value"),
            None,
        )
        .await
        .expect("put real-etcd test value");
    response_revision(response.header(), "put")
}

async fn delete_namespace(client: &mut Client, namespace: &str) {
    client
        .delete(
            format!("{namespace}/"),
            Some(DeleteOptions::new().with_prefix()),
        )
        .await
        .expect("clean real-etcd test namespace");
}

fn response_revision(header: Option<&etcd_client::ResponseHeader>, operation: &str) -> u64 {
    let revision = header
        .unwrap_or_else(|| panic!("real-etcd {operation} response omitted its header"))
        .revision();
    u64::try_from(revision).expect("real-etcd response revision must be non-negative")
}

fn unique_namespace(label: &str) -> String {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must follow Unix epoch")
        .as_nanos();
    format!(
        "/lattice/real-etcd-tests/{label}-{}-{nonce}",
        std::process::id()
    )
}

fn actor_record(actor_id: u64, owner: &str, epoch: u64) -> ActorPlacementRecord {
    ActorPlacementRecord {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(actor_id),
        owner: InstanceId::new(owner),
        epoch: Epoch(epoch),
        lease_id: LeaseId(1),
        state: PlacementState::Running,
    }
}

fn actor_key_for_real_test(actor_id: u64) -> ActorPlacementKey {
    ActorPlacementKey {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(actor_id),
    }
}

fn vshard_key_for_real_test(shard_id: u32) -> VirtualShardPlacementKey {
    VirtualShardPlacementKey {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        shard_id: VirtualShardId(shard_id),
    }
}

fn vshard_record_for_real_test(
    shard_id: u32,
    owner: &str,
    epoch: u64,
) -> VirtualShardPlacementRecord {
    VirtualShardPlacementRecord {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        shard_id: VirtualShardId(shard_id),
        owner: InstanceId::new(owner),
        epoch: Epoch(epoch),
    }
}

fn singleton_key_for_real_test(scope: &str) -> SingletonKey {
    SingletonKey {
        service_kind: service_kind!("World"),
        singleton_kind: actor_kind!("SeasonManager"),
        scope: scope.to_string(),
    }
}

fn singleton_record_for_real_test(
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

fn successful_real_reservations(
    left: Result<PlacementEpochReservation, PlacementError>,
    right: Result<PlacementEpochReservation, PlacementError>,
) -> Vec<PlacementEpochReservation> {
    let mut reservations = Vec::new();
    for result in [left, right] {
        match result {
            Ok(reservation) => reservations.push(reservation),
            Err(PlacementError::CompareAndPutFailed) => {}
            Err(error) => panic!("concurrent real-etcd reservation failed unexpectedly: {error}"),
        }
    }
    assert!(
        !reservations.is_empty(),
        "at least one concurrent real-etcd reservation must succeed"
    );
    reservations.sort_by_key(|reservation| reservation.epoch());
    assert!(
        reservations
            .windows(2)
            .all(|pair| pair[0].epoch() < pair[1].epoch()),
        "successful concurrent reservations must have distinct epochs"
    );
    reservations
}
