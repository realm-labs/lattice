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
use crate::storage::etcd::codec::{actor_key, encode_etcd_value, vshard_key};
use crate::storage::{
    ActorPlacementKey, ActorPlacementRecord, OwnershipWatch, OwnershipWatchBatch,
    OwnershipWatchEvent, OwnershipWatchUpdate, PlacementPrefix, PlacementState, PlacementStore,
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
