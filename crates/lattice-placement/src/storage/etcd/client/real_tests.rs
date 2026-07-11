use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use etcd_client::{Client, CompactionOptions, DeleteOptions, PutOptions, Txn, TxnOp};
use lattice_core::actor_ref::Epoch;
use lattice_core::id::ActorId;
use lattice_core::instance::InstanceId;
use lattice_core::{actor_kind, service_kind};
use tokio::sync::{Barrier, broadcast};
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

#[test]
fn startup_progress_must_reach_the_post_created_linearizable_barrier() {
    let barrier = PlacementRevision(7);

    assert!(!startup_progress_reaches_barrier(
        &EtcdOwnershipWatchUpdate::Progress {
            revision: PlacementRevision(6),
        },
        barrier,
    ));
    assert!(startup_progress_reaches_barrier(
        &EtcdOwnershipWatchUpdate::Progress { revision: barrier },
        barrier,
    ));
    assert!(startup_progress_reaches_barrier(
        &EtcdOwnershipWatchUpdate::Progress {
            revision: PlacementRevision(8),
        },
        barrier,
    ));
    assert!(!startup_progress_reaches_barrier(
        &EtcdOwnershipWatchUpdate::Batch(EtcdOwnershipWatchBatch {
            revision: PlacementRevision(8),
            events: Vec::new(),
        }),
        barrier,
    ));
}

#[tokio::test]
async fn dropping_real_etcd_ownership_watch_aborts_its_stream_task() {
    let (_tx, rx) = broadcast::channel::<EtcdOwnershipWatchMessage>(1);
    let task = tokio::spawn(std::future::pending::<()>());
    let watch = EtcdOwnershipWatch {
        rx,
        abort_handle: Some(task.abort_handle()),
    };

    drop(watch);

    let error = timeout(Duration::from_secs(1), task)
        .await
        .expect("raw etcd ownership stream task did not stop")
        .expect_err("raw etcd ownership stream task completed instead of being aborted");
    assert!(error.is_cancelled());
}

#[tokio::test]
async fn dropping_high_level_etcd_view_cascades_to_the_raw_watch() {
    let client = InMemoryEtcdClient::new();
    let store = EtcdPlacementStore::new(
        PlacementPrefix::new("/lattice/drop-cancellable-etcd-view"),
        client.clone(),
    );
    let view = store
        .open_ownership_view(
            &service_kind!("World"),
            &InstanceId::new("world-a"),
            NonZeroUsize::new(1).unwrap(),
        )
        .await
        .expect("open cancellable in-memory-etcd ownership view");
    assert_eq!(client.ownership_watcher_count_for_test(), 1);
    assert_eq!(client.active_ownership_watcher_count_for_test(), 1);

    drop(view);

    timeout(Duration::from_secs(1), async {
        while client.active_ownership_watcher_count_for_test() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropping the public view did not drop its raw watch receiver");
    client.progress_ownership_watches_for_test(PlacementRevision(1));
    assert_eq!(client.ownership_watcher_count_for_test(), 0);
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
    let initial_revision = put_actor_with_floor(&mut raw, &prefix, &actor, &initial).await;

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
    let gap_revision = put_actor_with_floor(&mut raw, &prefix, &actor, &changed).await;
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
        crate::storage::OwnershipViewRecord::Actor { revision, record, .. }
            if *revision == PlacementRevision(initial_revision) && record == &initial
    )));

    let (historical, progress) = next_batch_and_progress(&mut view.watch).await;
    assert_eq!(historical.revision, PlacementRevision(gap_revision));
    assert!(matches!(
        historical.events.as_slice(),
        [OwnershipWatchEvent::ActorUpserted { key, record, proof }]
            if key == &actor
                && record == &changed
                && proof.observed_revision() == PlacementRevision(gap_revision)
    ));

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
    assert!(matches!(
        deleted.events.as_slice(),
        [OwnershipWatchEvent::ActorDeleted {
            key,
            previous_record,
            proof,
        }] if key == &actor
            && previous_record == &changed
            && proof.observed_revision() == PlacementRevision(delete_revision)
    ));

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
            epoch_floor_key(&prefix, &PlacementEpochKey::Actor(second_actor.clone())),
            encode_etcd_value(&EtcdValue::EpochFloor(Box::new(EpochFloorRecord {
                key: PlacementEpochKey::Actor(second_actor.clone()),
                epoch: second_record.epoch,
            })))
            .expect("encode actor floor transaction value"),
            None,
        ),
        TxnOp::put(
            vshard_key(&prefix, &shard),
            encode_etcd_value(&EtcdValue::VirtualShard(Box::new(shard_record.clone())))
                .expect("encode shard transaction value"),
            None,
        ),
        TxnOp::put(
            epoch_floor_key(&prefix, &PlacementEpochKey::VirtualShard(shard.clone())),
            encode_etcd_value(&EtcdValue::EpochFloor(Box::new(EpochFloorRecord {
                key: PlacementEpochKey::VirtualShard(shard.clone()),
                epoch: shard_record.epoch,
            })))
            .expect("encode shard floor transaction value"),
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
    assert!(batch.events.iter().any(|event| matches!(
        event,
        OwnershipWatchEvent::ActorUpserted { key, record, proof }
            if key == &second_actor
                && record == &second_record
                && proof.observed_revision() == PlacementRevision(transaction_revision)
    )));
    assert!(batch.events.iter().any(|event| matches!(
        event,
        OwnershipWatchEvent::VirtualShardUpserted { key, record, proof }
            if key == &shard
                && record == &shard_record
                && proof.observed_revision() == PlacementRevision(transaction_revision)
    )));
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
    let compact_initial = actor_record(99, "world-a", 1);
    let compact_initial_revision =
        put_actor_with_floor(&mut raw, &compact_prefix, &compact_actor, &compact_initial).await;
    let compact_gap = Arc::new(Barrier::new(2));
    let mut compact_real = RealEtcdClient::connect(
        endpoints,
        InstanceLeaseTtl::new(30),
        ActivationLockTtl::new(30),
    )
    .await
    .expect("connect compaction ownership-view client");
    compact_real.ownership_view_gap = Some(compact_gap.clone());
    let compact_store = EtcdPlacementStore::new(compact_prefix.clone(), compact_real);
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
    let first_compacted_revision = put_actor_with_floor(
        &mut raw,
        &compact_prefix,
        &compact_actor,
        &actor_record(99, "world-a", 2),
    )
    .await;
    assert!(first_compacted_revision > compact_initial_revision);
    // etcd still permits a watch beginning exactly at the compaction
    // revision. Advance once more so the requested R+1 revision is strictly
    // behind the compaction boundary and must be canceled immediately.
    let compact_revision = put_actor_with_floor(
        &mut raw,
        &compact_prefix,
        &compact_actor,
        &actor_record(99, "world-a", 3),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a real etcd endpoint in LATTICE_TEST_ETCD_ENDPOINT"]
async fn real_etcd_ownership_watch_allows_a_full_capacity_same_revision_replacement() {
    let endpoint = std::env::var(TEST_ETCD_ENDPOINT)
        .unwrap_or_else(|_| panic!("set {TEST_ETCD_ENDPOINT} to a real etcd endpoint"));
    assert!(
        !endpoint.trim().is_empty(),
        "{TEST_ETCD_ENDPOINT} must not be blank"
    );
    let endpoints = vec![endpoint];
    let mut raw = Client::connect(endpoints.clone(), None)
        .await
        .expect("connect raw real-etcd capacity test client");

    let namespace = unique_namespace("ownership-capacity-replacement");
    delete_namespace(&mut raw, &namespace).await;
    let prefix = PlacementPrefix::new(namespace.clone());
    let actor = actor_key_for_real_test(7);
    let actor_record = actor_record(7, "world-a", 1);
    put_actor_with_floor(&mut raw, &prefix, &actor, &actor_record).await;

    let real = RealEtcdClient::connect(
        endpoints,
        InstanceLeaseTtl::new(30),
        ActivationLockTtl::new(30),
    )
    .await
    .expect("connect real-etcd capacity ownership client");
    let store = EtcdPlacementStore::new(prefix.clone(), real);
    let mut view = store
        .open_ownership_view(
            &service_kind!("World"),
            &InstanceId::new("world-a"),
            NonZeroUsize::new(1).unwrap(),
        )
        .await
        .expect("open full-capacity real-etcd ownership view");
    assert_eq!(view.snapshot.records.len(), 1);

    let shard = vshard_key_for_real_test(3);
    let shard_record = vshard_record_for_real_test(3, "world-a", 1);
    let shard_epoch_key = PlacementEpochKey::VirtualShard(shard.clone());
    let mut operations = vec![
        TxnOp::delete(actor_key(&prefix, &actor), None),
        TxnOp::put(
            epoch_floor_key(&prefix, &shard_epoch_key),
            encode_etcd_value(&EtcdValue::EpochFloor(Box::new(EpochFloorRecord {
                key: shard_epoch_key,
                epoch: shard_record.epoch,
            })))
            .expect("encode replacement shard floor"),
            None,
        ),
        TxnOp::put(
            vshard_key(&prefix, &shard),
            encode_etcd_value(&EtcdValue::VirtualShard(Box::new(shard_record.clone())))
                .expect("encode replacement shard"),
            None,
        ),
    ];
    for actor_id in 90..94 {
        let unrelated_key = ActorPlacementKey {
            service_kind: service_kind!("Other"),
            actor_kind: actor_kind!("Other"),
            actor_id: ActorId::U64(actor_id),
        };
        operations.push(TxnOp::put(
            actor_key(&prefix, &unrelated_key),
            b"malformed-unselected-ownership-record".to_vec(),
            None,
        ));
    }
    let transaction = raw
        .txn(Txn::new().and_then(operations))
        .await
        .expect("commit full-capacity replacement transaction");
    let transaction_revision = response_revision(transaction.header(), "capacity transaction");

    let batch = next_batch(&mut view.watch).await;
    assert_eq!(batch.revision, PlacementRevision(transaction_revision));
    assert_eq!(batch.events.len(), 2);
    assert!(batch.events.iter().any(|event| matches!(
        event,
        OwnershipWatchEvent::ActorDeleted {
            key,
            previous_record,
            proof,
        } if key == &actor
            && previous_record == &actor_record
            && proof.observed_revision() == PlacementRevision(transaction_revision)
    )));
    assert!(batch.events.iter().any(|event| matches!(
        event,
        OwnershipWatchEvent::VirtualShardUpserted { key, record, proof }
            if key == &shard
                && record == &shard_record
                && proof.observed_revision() == PlacementRevision(transaction_revision)
    )));

    drop(view);
    let final_view = store
        .open_ownership_view(
            &service_kind!("World"),
            &InstanceId::new("world-a"),
            NonZeroUsize::new(1).unwrap(),
        )
        .await
        .expect("reopen bounded real-etcd ownership view after replacement");
    assert!(matches!(
        final_view.snapshot.records.as_slice(),
        [crate::storage::OwnershipViewRecord::VirtualShard {
            revision,
            record,
            ..
        }] if *revision == PlacementRevision(transaction_revision)
            && record == &shard_record
    ));
    drop(final_view);
    delete_namespace(&mut raw, &namespace).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a real etcd endpoint in LATTICE_TEST_ETCD_ENDPOINT"]
async fn real_etcd_ownership_floor_proofs_reject_missing_leased_and_laundered_state_atomically() {
    let endpoint = std::env::var(TEST_ETCD_ENDPOINT)
        .unwrap_or_else(|_| panic!("set {TEST_ETCD_ENDPOINT} to a real etcd endpoint"));
    let endpoints = vec![endpoint];
    let mut raw = Client::connect(endpoints.clone(), None)
        .await
        .expect("connect raw real-etcd proof client");
    let service = service_kind!("World");
    let owner = InstanceId::new("world-a");

    let missing_namespace = unique_namespace("ownership-proof-missing");
    delete_namespace(&mut raw, &missing_namespace).await;
    let missing_prefix = PlacementPrefix::new(missing_namespace.clone());
    let missing_key = ActorPlacementKey {
        service_kind: service.clone(),
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(1),
    };
    put_value(
        &mut raw,
        actor_key(&missing_prefix, &missing_key),
        EtcdValue::Actor(Box::new(actor_record(1, "world-a", 1))),
    )
    .await;
    let missing_store = EtcdPlacementStore::new(
        missing_prefix,
        RealEtcdClient::connect(
            endpoints.clone(),
            InstanceLeaseTtl::new(30),
            ActivationLockTtl::new(30),
        )
        .await
        .expect("connect missing-floor ownership client"),
    );
    assert!(matches!(
        missing_store
            .open_ownership_view(&service, &owner, NonZeroUsize::new(8).unwrap())
            .await,
        Err(OwnershipViewError::Proof {
            error: OwnershipProofError::MissingFloor { key, .. },
        }) if key == PlacementEpochKey::Actor(missing_key)
    ));
    delete_namespace(&mut raw, &missing_namespace).await;

    let leased_namespace = unique_namespace("ownership-proof-leased");
    delete_namespace(&mut raw, &leased_namespace).await;
    let leased_prefix = PlacementPrefix::new(leased_namespace.clone());
    let leased_key = ActorPlacementKey {
        service_kind: service.clone(),
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(2),
    };
    let leased_record = actor_record(2, "world-a", 1);
    let leased_epoch_key = PlacementEpochKey::Actor(leased_key.clone());
    let lease = raw
        .lease_grant(30, None)
        .await
        .expect("grant real-etcd proof lease")
        .id();
    raw.txn(Txn::new().and_then(vec![
        TxnOp::put(
            actor_key(&leased_prefix, &leased_key),
            encode_etcd_value(&EtcdValue::Actor(Box::new(leased_record.clone())))
                .expect("encode leased-proof actor"),
            None,
        ),
        TxnOp::put(
            epoch_floor_key(&leased_prefix, &leased_epoch_key),
            encode_etcd_value(&EtcdValue::EpochFloor(Box::new(EpochFloorRecord {
                key: leased_epoch_key.clone(),
                epoch: leased_record.epoch,
            })))
            .expect("encode leased epoch floor"),
            Some(PutOptions::new().with_lease(lease)),
        ),
    ]))
    .await
    .expect("put record with leased floor");
    let leased_store = EtcdPlacementStore::new(
        leased_prefix,
        RealEtcdClient::connect(
            endpoints.clone(),
            InstanceLeaseTtl::new(30),
            ActivationLockTtl::new(30),
        )
        .await
        .expect("connect leased-floor ownership client"),
    );
    assert!(matches!(
        leased_store
            .open_ownership_view(&service, &owner, NonZeroUsize::new(8).unwrap())
            .await,
        Err(OwnershipViewError::Proof {
            error: OwnershipProofError::LeasedFloor { key, lease_id, .. },
        }) if key == leased_epoch_key
            && lease_id == LeaseId(u64::try_from(lease).expect("lease ID must be positive"))
    ));
    delete_namespace(&mut raw, &leased_namespace).await;
    raw.lease_revoke(lease)
        .await
        .expect("revoke real-etcd proof lease");

    let watch_namespace = unique_namespace("ownership-proof-watch");
    delete_namespace(&mut raw, &watch_namespace).await;
    let watch_prefix = PlacementPrefix::new(watch_namespace.clone());
    let proof_gap = Arc::new(Barrier::new(2));
    let watching_client = RealEtcdClient::connect(
        endpoints,
        InstanceLeaseTtl::new(30),
        ActivationLockTtl::new(30),
    )
    .await
    .expect("connect watch-proof ownership client");
    let proof_gap_hook = watching_client.ownership_watch_proof_gap.clone();
    let watching_store = EtcdPlacementStore::new(watch_prefix.clone(), watching_client);
    let mut view = timeout(
        TEST_TIMEOUT,
        watching_store.open_ownership_view(&service, &owner, NonZeroUsize::new(8).unwrap()),
    )
    .await
    .expect("empty proven ownership view did not open before its deadline")
    .expect("open empty proven ownership view");
    *proof_gap_hook
        .lock()
        .expect("ownership watch proof-gap mutex poisoned") = Some(proof_gap.clone());
    let valid_key = ActorPlacementKey {
        service_kind: service.clone(),
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(3),
    };
    let invalid_key = ActorPlacementKey {
        service_kind: service,
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(4),
    };
    let valid_record = actor_record(3, "world-a", 1);
    let invalid_record = actor_record(4, "world-a", 1);
    let valid_epoch_key = PlacementEpochKey::Actor(valid_key.clone());
    let invalid_epoch_key = PlacementEpochKey::Actor(invalid_key.clone());
    let transaction = raw
        .txn(Txn::new().and_then(vec![
            TxnOp::put(
                actor_key(&watch_prefix, &valid_key),
                encode_etcd_value(&EtcdValue::Actor(Box::new(valid_record.clone())))
                    .expect("encode valid batched actor"),
                None,
            ),
            TxnOp::put(
                epoch_floor_key(&watch_prefix, &valid_epoch_key),
                encode_etcd_value(&EtcdValue::EpochFloor(Box::new(EpochFloorRecord {
                    key: valid_epoch_key,
                    epoch: valid_record.epoch,
                })))
                .expect("encode valid batched floor"),
                None,
            ),
            TxnOp::put(
                actor_key(&watch_prefix, &invalid_key),
                encode_etcd_value(&EtcdValue::Actor(Box::new(invalid_record.clone())))
                    .expect("encode invalid batched actor"),
                None,
            ),
        ]))
        .await
        .expect("commit mixed proof batch");
    let transaction_revision = response_revision(transaction.header(), "mixed proof batch");
    timeout(TEST_TIMEOUT, proof_gap.wait())
        .await
        .expect("watch did not reach exact floor-proof gap");
    put_value(
        &mut raw,
        epoch_floor_key(&watch_prefix, &invalid_epoch_key),
        EtcdValue::EpochFloor(Box::new(EpochFloorRecord {
            key: invalid_epoch_key.clone(),
            epoch: invalid_record.epoch,
        })),
    )
    .await;
    timeout(TEST_TIMEOUT, proof_gap.wait())
        .await
        .expect("release exact floor-proof gap");

    let error = timeout(TEST_TIMEOUT, async {
        loop {
            match view.watch.next_update().await {
                Ok(OwnershipWatchUpdate::Progress { .. }) => {}
                Ok(OwnershipWatchUpdate::Batch(batch)) => {
                    panic!("invalid same-revision proof published a partial batch: {batch:?}")
                }
                Err(error) => break error,
            }
        }
    })
    .await
    .expect("timed out waiting for exact floor-proof failure");
    assert!(matches!(
        error,
        OwnershipWatchError::Proof {
            error: OwnershipProofError::MissingFloor {
                key,
                observed_revision,
            },
        } if key == invalid_epoch_key
            && observed_revision == PlacementRevision(transaction_revision)
    ));
    delete_namespace(&mut raw, &watch_namespace).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a real etcd endpoint in LATTICE_TEST_ETCD_ENDPOINT"]
async fn real_etcd_snapshot_floor_proof_reports_physical_compaction() {
    let endpoint = std::env::var(TEST_ETCD_ENDPOINT)
        .unwrap_or_else(|_| panic!("set {TEST_ETCD_ENDPOINT} to a real etcd endpoint"));
    let endpoints = vec![endpoint];
    let mut raw = Client::connect(endpoints.clone(), None)
        .await
        .expect("connect raw real-etcd compaction-proof client");
    let namespace = unique_namespace("ownership-proof-compaction");
    delete_namespace(&mut raw, &namespace).await;
    let prefix = PlacementPrefix::new(namespace.clone());
    let actor = ActorPlacementKey {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(1),
    };
    let record = actor_record(1, "world-a", 1);
    let snapshot_revision = put_actor_with_floor(&mut raw, &prefix, &actor, &record).await;

    let proof_gap = Arc::new(Barrier::new(2));
    let mut real = RealEtcdClient::connect(
        endpoints,
        InstanceLeaseTtl::new(30),
        ActivationLockTtl::new(30),
    )
    .await
    .expect("connect compaction-proof ownership client");
    real.ownership_snapshot_proof_gap = Some(proof_gap.clone());
    let store = EtcdPlacementStore::new(prefix, real);
    let open = tokio::spawn(async move {
        store
            .open_ownership_view(
                &service_kind!("World"),
                &InstanceId::new("world-a"),
                NonZeroUsize::new(8).unwrap(),
            )
            .await
    });

    timeout(TEST_TIMEOUT, proof_gap.wait())
        .await
        .expect("snapshot did not reach its exact floor-proof gap");
    let advanced_revision = raw
        .put(format!("{namespace}/compaction-marker"), b"marker", None)
        .await
        .expect("advance real-etcd beyond the proof revision");
    let advanced_revision = response_revision(advanced_revision.header(), "compaction marker");
    assert!(advanced_revision > snapshot_revision);
    raw.compact(
        i64::try_from(advanced_revision).expect("advanced revision must fit i64"),
        Some(CompactionOptions::new().with_physical()),
    )
    .await
    .expect("physically compact past the snapshot floor-proof revision");
    timeout(TEST_TIMEOUT, proof_gap.wait())
        .await
        .expect("release snapshot floor-proof gap");

    let error = timeout(TEST_TIMEOUT, open)
        .await
        .expect("compacted floor-proof view did not finish")
        .expect("compacted floor-proof task panicked")
        .expect_err("compacted exact floor proof must fail closed");
    assert!(matches!(
        error,
        OwnershipViewError::Proof {
            error: OwnershipProofError::RevisionUnavailable {
                requested_revision,
                ..
            },
        } if requested_revision == PlacementRevision(snapshot_revision)
    ));
    delete_namespace(&mut raw, &namespace).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a real etcd endpoint in LATTICE_TEST_ETCD_ENDPOINT"]
async fn real_etcd_snapshot_floor_proofs_chunk_more_than_default_transaction_limit() {
    const RECORD_COUNT: usize = 129;
    const SEED_RECORDS_PER_TRANSACTION: usize = 32;

    let endpoint = std::env::var(TEST_ETCD_ENDPOINT)
        .unwrap_or_else(|_| panic!("set {TEST_ETCD_ENDPOINT} to a real etcd endpoint"));
    let endpoints = vec![endpoint];
    let mut raw = Client::connect(endpoints.clone(), None)
        .await
        .expect("connect raw real-etcd proof-chunk client");
    let namespace = unique_namespace("ownership-proof-chunks");
    delete_namespace(&mut raw, &namespace).await;
    let prefix = PlacementPrefix::new(namespace.clone());
    let records = (0..RECORD_COUNT)
        .map(|actor_id| {
            let actor_id = u64::try_from(actor_id).expect("test actor ID must fit u64");
            (
                ActorPlacementKey {
                    service_kind: service_kind!("World"),
                    actor_kind: actor_kind!("World"),
                    actor_id: ActorId::U64(actor_id),
                },
                actor_record(actor_id, "world-a", 1),
            )
        })
        .collect::<Vec<_>>();
    for chunk in records.chunks(SEED_RECORDS_PER_TRANSACTION) {
        put_actor_batch_with_floors(&mut raw, &prefix, chunk).await;
    }

    let store = EtcdPlacementStore::new(
        prefix,
        RealEtcdClient::connect(
            endpoints,
            InstanceLeaseTtl::new(30),
            ActivationLockTtl::new(30),
        )
        .await
        .expect("connect proof-chunk ownership client"),
    );
    let view = timeout(
        TEST_TIMEOUT,
        store.open_ownership_view(
            &service_kind!("World"),
            &InstanceId::new("world-a"),
            NonZeroUsize::new(RECORD_COUNT).unwrap(),
        ),
    )
    .await
    .expect("chunked ownership proof view timed out")
    .expect("open ownership view with chunked floor proofs");
    assert_eq!(view.snapshot.records.len(), RECORD_COUNT);
    assert!(view.snapshot.records.iter().all(|record| match record {
        crate::storage::OwnershipViewRecord::Actor { proof, .. }
        | crate::storage::OwnershipViewRecord::VirtualShard { proof, .. }
        | crate::storage::OwnershipViewRecord::Singleton { proof, .. } =>
            proof.observed_revision() == view.snapshot.revision,
    }));
    drop(view);
    delete_namespace(&mut raw, &namespace).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a real etcd endpoint in LATTICE_TEST_ETCD_ENDPOINT"]
async fn real_etcd_record_only_replay_cannot_be_laundered_into_a_hardened_epoch() {
    let endpoint = std::env::var(TEST_ETCD_ENDPOINT)
        .unwrap_or_else(|_| panic!("set {TEST_ETCD_ENDPOINT} to a real etcd endpoint"));
    assert!(
        !endpoint.trim().is_empty(),
        "{TEST_ETCD_ENDPOINT} must not be blank"
    );
    let endpoints = vec![endpoint];
    let mut raw = Client::connect(endpoints.clone(), None)
        .await
        .expect("connect raw real-etcd lineage test client");
    let namespace = unique_namespace("epoch-lineage");
    delete_namespace(&mut raw, &namespace).await;
    let prefix = PlacementPrefix::new(namespace.clone());
    let real = RealEtcdClient::connect(
        endpoints,
        InstanceLeaseTtl::new(30),
        ActivationLockTtl::new(30),
    )
    .await
    .expect("connect real-etcd lineage placement client");
    let inspector = real.clone();
    let store = EtcdPlacementStore::new(prefix.clone(), real);
    let actor = actor_key_for_real_test(7);
    let record_path = actor_key(&prefix, &actor);
    let floor_path = epoch_floor_key(&prefix, &PlacementEpochKey::Actor(actor.clone()));

    let record_token = store
        .compare_and_put_actor(actor.clone(), None, actor_record(7, "world-a", 5))
        .await
        .expect("seed hardened actor lineage");
    let burned = store
        .reserve_actor_epoch(actor.clone(), Some(record_token), None)
        .await
        .expect("burn one actor epoch before replay");
    assert_eq!(burned.epoch(), Epoch(6));
    drop(burned);
    let floor_before_replay = inspector
        .get(&floor_path)
        .await
        .expect("read burned real-etcd floor")
        .expect("burned real-etcd floor must exist");

    put_value(
        &mut raw,
        record_path.clone(),
        EtcdValue::Actor(Box::new(actor_record(7, "world-a", 5))),
    )
    .await;
    let replay_pair = inspector
        .get(&record_path)
        .await
        .expect("read record-only real-etcd replay")
        .expect("record-only real-etcd replay must exist");
    assert!(
        replay_pair.0.modification_revision() > floor_before_replay.0.modification_revision(),
        "the replay must be newer than the burned floor for this regression",
    );

    for result in [
        store
            .reserve_actor_epoch(actor.clone(), Some(replay_pair.0), None)
            .await
            .map(|_| ()),
        store
            .compare_and_put_actor(
                actor.clone(),
                Some(replay_pair.0),
                actor_record(7, "world-b", 7),
            )
            .await
            .map(|_| ()),
    ] {
        match result {
            Err(PlacementError::EpochFloorUnproven { record, floor }) => {
                assert_eq!(record, replay_pair.0);
                assert_eq!(floor, Some(floor_before_replay.0));
            }
            Err(error) => panic!("expected unproven real-etcd lineage, got {error}"),
            Ok(()) => panic!("record-only real-etcd replay was laundered"),
        }
    }
    assert_eq!(
        inspector.get(&record_path).await.unwrap().unwrap(),
        replay_pair,
        "rejected laundering must not mutate the replayed record",
    );
    assert_eq!(
        inspector.get(&floor_path).await.unwrap().unwrap(),
        floor_before_replay,
        "rejected laundering must not advance the durable floor",
    );

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

async fn put_actor_with_floor(
    client: &mut Client,
    prefix: &PlacementPrefix,
    key: &ActorPlacementKey,
    record: &ActorPlacementRecord,
) -> u64 {
    let epoch_key = PlacementEpochKey::Actor(key.clone());
    let transaction = client
        .txn(Txn::new().and_then(vec![
            TxnOp::put(
                actor_key(prefix, key),
                encode_etcd_value(&EtcdValue::Actor(Box::new(record.clone())))
                    .expect("encode real-etcd actor"),
                None,
            ),
            TxnOp::put(
                epoch_floor_key(prefix, &epoch_key),
                encode_etcd_value(&EtcdValue::EpochFloor(Box::new(EpochFloorRecord {
                    key: epoch_key,
                    epoch: record.epoch,
                })))
                .expect("encode real-etcd actor floor"),
                None,
            ),
        ]))
        .await
        .expect("put real-etcd actor and floor");
    response_revision(transaction.header(), "actor/floor transaction")
}

async fn put_actor_batch_with_floors(
    client: &mut Client,
    prefix: &PlacementPrefix,
    records: &[(ActorPlacementKey, ActorPlacementRecord)],
) {
    let mut operations = Vec::with_capacity(records.len() * 2);
    for (key, record) in records {
        let epoch_key = PlacementEpochKey::Actor(key.clone());
        operations.push(TxnOp::put(
            actor_key(prefix, key),
            encode_etcd_value(&EtcdValue::Actor(Box::new(record.clone())))
                .expect("encode batched real-etcd actor"),
            None,
        ));
        operations.push(TxnOp::put(
            epoch_floor_key(prefix, &epoch_key),
            encode_etcd_value(&EtcdValue::EpochFloor(Box::new(EpochFloorRecord {
                key: epoch_key,
                epoch: record.epoch,
            })))
            .expect("encode batched real-etcd actor floor"),
            None,
        ));
    }
    assert!(
        operations.len() <= FLOOR_PROOF_TXN_OP_LIMIT,
        "test seeding must remain below the proof chunk limit"
    );
    client
        .txn(Txn::new().and_then(operations))
        .await
        .expect("put batched real-etcd actors and floors");
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
        owner_incarnation: lattice_core::instance::InstanceIncarnation::new(format!(
            "{owner}-boot"
        )),
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
