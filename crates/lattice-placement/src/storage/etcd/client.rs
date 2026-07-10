use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use etcd_client::{
    Client, Compare, CompareOp, EventType, GetOptions, Txn, TxnOp, TxnOpResponse, WatchOptions,
};
use tokio::sync::broadcast;

use crate::error::PlacementError;
use crate::storage::etcd::codec::{
    EtcdValue, codec_error, decode_etcd_value, encode_etcd_value, etcd_error, lease_id,
    placement_revision, placement_version, put_options_for,
};
use crate::storage::{
    ActorPlacementKey, LeaseId, OwnershipProofError, OwnershipViewError, OwnershipWatchError,
    PlacementEpochKey, PlacementRevision, PlacementVersion, SingletonKey, VirtualShardPlacementKey,
};

const WATCH_CAPACITY: usize = 128;
// Keep proof reads below etcd's default `--max-txn-ops=128`. A server with a
// stricter configured limit rejects the request and the ownership view fails
// closed rather than falling back to an unproven latest-value read.
const FLOOR_PROOF_TXN_OP_LIMIT: usize = 64;

/// A revision can replace every retained ownership record: at most
/// `max_entries` old keys can disappear and `max_entries` new keys can appear.
/// The exact local-instance key is the only additional selected watch key.
/// Keeping this separate from the final live-record bound avoids rejecting a
/// safe full-capacity replacement because of its transient event count.
pub(crate) fn ownership_watch_event_limit(max_entries: NonZeroUsize) -> Option<usize> {
    max_entries
        .get()
        .checked_mul(2)
        .and_then(|limit| limit.checked_add(1))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EtcdOwnershipRecordRange {
    pub record_prefix: String,
    pub floor_prefix: String,
}

#[derive(Debug, Clone)]
pub struct EtcdOwnershipRanges {
    pub local_instance_key: String,
    pub record_ranges: Vec<EtcdOwnershipRecordRange>,
    pub watch_prefix: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EtcdOwnershipFloorProof {
    pub observed_revision: PlacementRevision,
    pub key: String,
    pub version: PlacementVersion,
    pub value: EtcdValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EtcdOwnershipSnapshotEntry {
    pub key: String,
    pub revision: PlacementRevision,
    pub value: EtcdValue,
    pub floor: Option<EtcdOwnershipFloorProof>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EtcdOwnershipSnapshot {
    pub revision: PlacementRevision,
    pub entries: Vec<EtcdOwnershipSnapshotEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EtcdOwnershipWatchEvent {
    Upserted {
        key: String,
        version: PlacementVersion,
        value: EtcdValue,
        floor: Option<EtcdOwnershipFloorProof>,
    },
    Deleted {
        key: String,
        previous_version: PlacementVersion,
        previous_value: EtcdValue,
        floor: Option<EtcdOwnershipFloorProof>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EtcdOwnershipWatchBatch {
    pub revision: PlacementRevision,
    pub events: Vec<EtcdOwnershipWatchEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EtcdOwnershipWatchUpdate {
    Batch(EtcdOwnershipWatchBatch),
    Progress { revision: PlacementRevision },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EtcdOwnershipWatchMessage {
    Update(EtcdOwnershipWatchUpdate),
    Failed(OwnershipWatchError),
}

#[derive(Debug)]
pub struct EtcdOwnershipWatch {
    rx: broadcast::Receiver<EtcdOwnershipWatchMessage>,
    abort_handle: Option<tokio::task::AbortHandle>,
}

impl EtcdOwnershipWatch {
    pub async fn next_update(&mut self) -> Result<EtcdOwnershipWatchUpdate, OwnershipWatchError> {
        match self.rx.recv().await {
            Ok(EtcdOwnershipWatchMessage::Update(update)) => Ok(update),
            Ok(EtcdOwnershipWatchMessage::Failed(error)) => Err(error),
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                Err(OwnershipWatchError::Lagged { skipped })
            }
            Err(broadcast::error::RecvError::Closed) => Err(OwnershipWatchError::Closed),
        }
    }
}

impl Drop for EtcdOwnershipWatch {
    fn drop(&mut self) {
        if let Some(abort_handle) = self.abort_handle.take() {
            abort_handle.abort();
        }
    }
}

#[derive(Debug)]
pub struct EtcdOwnershipView {
    pub snapshot: EtcdOwnershipSnapshot,
    pub watch: EtcdOwnershipWatch,
}

#[derive(Debug, Clone)]
pub struct EtcdValueGuard {
    pub key: String,
    pub value: EtcdValue,
}

#[derive(Debug, Clone)]
pub struct EtcdEpochReservationRequest {
    pub record_key: String,
    pub expected_record: Option<PlacementVersion>,
    pub floor_key: String,
    pub expected_floor: Option<(PlacementVersion, EtcdValue)>,
    pub floor_value: EtcdValue,
    pub guard: Option<EtcdValueGuard>,
}

#[derive(Debug, Clone)]
pub struct EtcdEpochCommitRequest {
    pub record_key: String,
    pub expected_record: Option<PlacementVersion>,
    pub floor_key: String,
    pub floor_token: PlacementVersion,
    pub floor_value: EtcdValue,
    pub record_value: EtcdValue,
    pub guard: Option<EtcdValueGuard>,
}

#[derive(Debug, Clone)]
pub struct EtcdLegacyEpochPutRequest {
    pub record_key: String,
    pub expected_record: Option<PlacementVersion>,
    pub floor_key: String,
    pub expected_floor: Option<(PlacementVersion, EtcdValue)>,
    pub floor_value: EtcdValue,
    pub record_value: EtcdValue,
}

#[async_trait]
pub trait EtcdKv: Clone + Send + Sync + 'static {
    async fn put(&self, key: String, value: EtcdValue) -> Result<(), PlacementError>;
    async fn get(&self, key: &str)
    -> Result<Option<(PlacementVersion, EtcdValue)>, PlacementError>;
    async fn list_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, PlacementVersion, EtcdValue)>, PlacementError>;
    async fn compare_and_put(
        &self,
        key: String,
        expected: Option<PlacementVersion>,
        value: EtcdValue,
    ) -> Result<PlacementVersion, PlacementError>;
    async fn reserve_epoch(
        &self,
        _request: EtcdEpochReservationRequest,
    ) -> Result<PlacementVersion, PlacementError> {
        Err(PlacementError::EpochReservationsUnsupported)
    }
    async fn commit_epoch(
        &self,
        _request: EtcdEpochCommitRequest,
    ) -> Result<PlacementVersion, PlacementError> {
        Err(PlacementError::EpochReservationsUnsupported)
    }
    async fn compare_and_put_epoch(
        &self,
        _request: EtcdLegacyEpochPutRequest,
    ) -> Result<PlacementVersion, PlacementError> {
        Err(PlacementError::EpochReservationsUnsupported)
    }
    async fn compare_and_delete(
        &self,
        key: String,
        expected: EtcdValue,
    ) -> Result<(), PlacementError>;
    async fn delete(&self, key: &str) -> Result<(), PlacementError>;
    async fn grant_instance_lease(&self) -> Result<LeaseId, PlacementError>;
    async fn keepalive_instance_lease(&self, lease_id: LeaseId) -> Result<(), PlacementError>;
    async fn next_lease_id(&self) -> Result<LeaseId, PlacementError>;
    async fn open_ownership_view(
        &self,
        ranges: EtcdOwnershipRanges,
        max_entries: NonZeroUsize,
    ) -> Result<EtcdOwnershipView, OwnershipViewError>;
    async fn watch_prefix(&self, prefix: &str) -> Result<EtcdWatch, PlacementError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EtcdWatchEvent {
    pub key: String,
    pub version: PlacementVersion,
    pub value: Option<EtcdValue>,
}

#[derive(Debug)]
pub struct EtcdWatch {
    rx: broadcast::Receiver<EtcdWatchEvent>,
}

impl EtcdWatch {
    pub async fn next(&mut self) -> Result<EtcdWatchEvent, PlacementError> {
        match self.rx.recv().await {
            Ok(event) => Ok(event),
            Err(broadcast::error::RecvError::Lagged(skipped)) => Err(PlacementError::Etcd {
                message: format!("etcd placement watch lagged and skipped {skipped} events"),
            }),
            Err(broadcast::error::RecvError::Closed) => Err(PlacementError::PlacementWatchClosed),
        }
    }
}

#[derive(Clone)]
pub struct RealEtcdClient {
    client: Client,
    instance_lease_ttl: InstanceLeaseTtl,
    activation_lock_ttl: ActivationLockTtl,
    #[cfg(test)]
    ownership_view_gap: Option<Arc<tokio::sync::Barrier>>,
    #[cfg(test)]
    ownership_snapshot_proof_gap: Option<Arc<tokio::sync::Barrier>>,
    #[cfg(test)]
    ownership_watch_proof_gap: Arc<std::sync::Mutex<Option<Arc<tokio::sync::Barrier>>>>,
}

impl fmt::Debug for RealEtcdClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RealEtcdClient")
            .field("instance_lease_ttl", &self.instance_lease_ttl)
            .field("activation_lock_ttl", &self.activation_lock_ttl)
            .finish_non_exhaustive()
    }
}

impl RealEtcdClient {
    pub async fn connect(
        endpoints: Vec<String>,
        instance_lease_ttl: InstanceLeaseTtl,
        activation_lock_ttl: ActivationLockTtl,
    ) -> Result<Self, PlacementError> {
        let client = Client::connect(endpoints, None).await.map_err(etcd_error)?;
        Ok(Self {
            client,
            instance_lease_ttl,
            activation_lock_ttl,
            #[cfg(test)]
            ownership_view_gap: None,
            #[cfg(test)]
            ownership_snapshot_proof_gap: None,
            #[cfg(test)]
            ownership_watch_proof_gap: Arc::new(std::sync::Mutex::new(None)),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct InstanceLeaseTtl(i64);

impl InstanceLeaseTtl {
    const DEFAULT_SECS: i64 = 30;

    pub fn new(seconds: i64) -> Self {
        if seconds > 0 {
            Self(seconds)
        } else {
            Self(Self::DEFAULT_SECS)
        }
    }

    fn as_secs(self) -> i64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ActivationLockTtl(i64);

impl ActivationLockTtl {
    const DEFAULT_SECS: i64 = 30;

    pub fn new(seconds: i64) -> Self {
        if seconds > 0 {
            Self(seconds)
        } else {
            Self(Self::DEFAULT_SECS)
        }
    }

    fn as_secs(self) -> i64 {
        self.0
    }
}

#[async_trait]
impl EtcdKv for RealEtcdClient {
    async fn put(&self, key: String, value: EtcdValue) -> Result<(), PlacementError> {
        let mut client = self.client.clone();
        client
            .put(key, encode_etcd_value(&value)?, put_options_for(&value)?)
            .await
            .map_err(etcd_error)?;
        Ok(())
    }

    async fn get(
        &self,
        key: &str,
    ) -> Result<Option<(PlacementVersion, EtcdValue)>, PlacementError> {
        let mut client = self.client.clone();
        let response = client.get(key, None).await.map_err(etcd_error)?;
        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };
        Ok(Some((
            placement_version(kv.mod_revision())?,
            decode_etcd_value(kv.value())?,
        )))
    }

    async fn list_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, PlacementVersion, EtcdValue)>, PlacementError> {
        let mut client = self.client.clone();
        let response = client
            .get(prefix, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(etcd_error)?;
        response
            .kvs()
            .iter()
            .map(|kv| {
                Ok((
                    String::from_utf8(kv.key().to_vec()).map_err(codec_error)?,
                    placement_version(kv.mod_revision())?,
                    decode_etcd_value(kv.value())?,
                ))
            })
            .collect()
    }

    async fn compare_and_put(
        &self,
        key: String,
        expected: Option<PlacementVersion>,
        value: EtcdValue,
    ) -> Result<PlacementVersion, PlacementError> {
        let compare = match expected {
            Some(version) => Compare::mod_revision(
                key.as_bytes(),
                CompareOp::Equal,
                i64::try_from(version.modification_revision()).map_err(codec_error)?,
            ),
            None => Compare::version(key.as_bytes(), CompareOp::Equal, 0),
        };
        let bytes = encode_etcd_value(&value)?;
        let put_options = put_options_for(&value)?;
        let txn = Txn::new().when(vec![compare]).and_then(vec![TxnOp::put(
            key.clone(),
            bytes,
            put_options,
        )]);
        let mut client = self.client.clone();
        let response = client.txn(txn).await.map_err(etcd_error)?;
        if !response.succeeded() {
            return Err(PlacementError::CompareAndPutFailed);
        }
        response
            .header()
            .ok_or_else(|| codec_error("etcd compare-and-put response omitted its header"))
            .and_then(|header| placement_version(header.revision()))
    }

    async fn reserve_epoch(
        &self,
        request: EtcdEpochReservationRequest,
    ) -> Result<PlacementVersion, PlacementError> {
        let EtcdEpochReservationRequest {
            record_key,
            expected_record,
            floor_key,
            expected_floor,
            floor_value,
            guard,
        } = request;
        let mut compares = vec![revision_compare(&record_key, expected_record)?];
        match expected_floor {
            Some((token, value)) => {
                compares.push(revision_compare(&floor_key, Some(token))?);
                compares.push(Compare::value(
                    floor_key.as_bytes(),
                    CompareOp::Equal,
                    encode_etcd_value(&value)?,
                ));
            }
            None => compares.push(revision_compare(&floor_key, None)?),
        }
        if let Some(guard) = guard {
            compares.push(Compare::value(
                guard.key.as_bytes(),
                CompareOp::Equal,
                encode_etcd_value(&guard.value)?,
            ));
        }
        let floor_bytes = encode_etcd_value(&floor_value)?;
        let floor_options = put_options_for(&floor_value)?;
        let txn = Txn::new().when(compares).and_then(vec![TxnOp::put(
            floor_key,
            floor_bytes,
            floor_options,
        )]);
        let mut client = self.client.clone();
        let response = client.txn(txn).await.map_err(etcd_error)?;
        successful_txn_revision(response, "epoch reservation")
    }

    async fn commit_epoch(
        &self,
        request: EtcdEpochCommitRequest,
    ) -> Result<PlacementVersion, PlacementError> {
        let EtcdEpochCommitRequest {
            record_key,
            expected_record,
            floor_key,
            floor_token,
            floor_value,
            record_value,
            guard,
        } = request;
        let floor_bytes = encode_etcd_value(&floor_value)?;
        let mut compares = vec![
            revision_compare(&record_key, expected_record)?,
            revision_compare(&floor_key, Some(floor_token))?,
            Compare::value(floor_key.as_bytes(), CompareOp::Equal, floor_bytes.clone()),
        ];
        if let Some(guard) = guard {
            compares.push(Compare::value(
                guard.key.as_bytes(),
                CompareOp::Equal,
                encode_etcd_value(&guard.value)?,
            ));
        }
        let floor_options = put_options_for(&floor_value)?;
        let record_bytes = encode_etcd_value(&record_value)?;
        let record_options = put_options_for(&record_value)?;
        let txn = Txn::new().when(compares).and_then(vec![
            TxnOp::put(floor_key, floor_bytes, floor_options),
            TxnOp::put(record_key, record_bytes, record_options),
        ]);
        let mut client = self.client.clone();
        let response = client.txn(txn).await.map_err(etcd_error)?;
        successful_txn_revision(response, "epoch commit")
    }

    async fn compare_and_put_epoch(
        &self,
        request: EtcdLegacyEpochPutRequest,
    ) -> Result<PlacementVersion, PlacementError> {
        let EtcdLegacyEpochPutRequest {
            record_key,
            expected_record,
            floor_key,
            expected_floor,
            floor_value,
            record_value,
        } = request;
        let mut compares = vec![revision_compare(&record_key, expected_record)?];
        match expected_floor {
            Some((token, value)) => {
                compares.push(revision_compare(&floor_key, Some(token))?);
                compares.push(Compare::value(
                    floor_key.as_bytes(),
                    CompareOp::Equal,
                    encode_etcd_value(&value)?,
                ));
            }
            None => compares.push(revision_compare(&floor_key, None)?),
        }
        let floor_bytes = encode_etcd_value(&floor_value)?;
        let floor_options = put_options_for(&floor_value)?;
        let record_bytes = encode_etcd_value(&record_value)?;
        let record_options = put_options_for(&record_value)?;
        let txn = Txn::new().when(compares).and_then(vec![
            TxnOp::put(floor_key, floor_bytes, floor_options),
            TxnOp::put(record_key, record_bytes, record_options),
        ]);
        let mut client = self.client.clone();
        let response = client.txn(txn).await.map_err(etcd_error)?;
        successful_txn_revision(response, "legacy epoch compare-and-put")
    }

    async fn delete(&self, key: &str) -> Result<(), PlacementError> {
        let mut client = self.client.clone();
        client.delete(key, None).await.map_err(etcd_error)?;
        Ok(())
    }

    async fn compare_and_delete(
        &self,
        key: String,
        expected: EtcdValue,
    ) -> Result<(), PlacementError> {
        let expected = encode_etcd_value(&expected)?;
        let txn = Txn::new()
            .when(vec![Compare::value(
                key.as_bytes(),
                CompareOp::Equal,
                expected,
            )])
            .and_then(vec![TxnOp::delete(key, None)]);
        let mut client = self.client.clone();
        let response = client.txn(txn).await.map_err(etcd_error)?;
        if response.succeeded() {
            Ok(())
        } else {
            Err(PlacementError::CompareAndPutFailed)
        }
    }

    async fn grant_instance_lease(&self) -> Result<LeaseId, PlacementError> {
        let mut client = self.client.clone();
        let response = client
            .lease_grant(self.instance_lease_ttl.as_secs(), None)
            .await
            .map_err(etcd_error)?;
        lease_id(response.id())
    }

    async fn keepalive_instance_lease(&self, lease_id: LeaseId) -> Result<(), PlacementError> {
        let lease_id = i64::try_from(lease_id.0).map_err(codec_error)?;
        let mut client = self.client.clone();
        let (mut keeper, mut stream) = client
            .lease_keep_alive(lease_id)
            .await
            .map_err(etcd_error)?;
        keeper.keep_alive().await.map_err(etcd_error)?;
        stream.message().await.map_err(etcd_error)?;
        Ok(())
    }

    async fn next_lease_id(&self) -> Result<LeaseId, PlacementError> {
        let mut client = self.client.clone();
        let response = client
            .lease_grant(self.activation_lock_ttl.as_secs(), None)
            .await
            .map_err(etcd_error)?;
        lease_id(response.id())
    }

    async fn open_ownership_view(
        &self,
        ranges: EtcdOwnershipRanges,
        max_entries: NonZeroUsize,
    ) -> Result<EtcdOwnershipView, OwnershipViewError> {
        let limit = max_entries
            .get()
            .checked_add(1)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or(OwnershipViewError::CapacityExceeded {
                max_entries: max_entries.get(),
            })?;
        let mut operations = Vec::with_capacity(ranges.record_ranges.len() + 1);
        operations.push(TxnOp::get(ranges.local_instance_key.clone(), None));
        operations.extend(ranges.record_ranges.iter().map(|range| {
            TxnOp::get(
                range.record_prefix.clone(),
                Some(GetOptions::new().with_prefix().with_limit(limit)),
            )
        }));
        let mut client = self.client.clone();
        let response = client
            .txn(Txn::new().and_then(operations))
            .await
            .map_err(view_backend_error)?;
        let revision = response
            .header()
            .ok_or_else(|| view_protocol_error("etcd ownership snapshot omitted its header"))
            .and_then(|header| view_revision(header.revision()))?;
        let responses = response.op_responses();
        if responses.len() != ranges.record_ranges.len() + 1 {
            return Err(view_protocol_error(format!(
                "etcd ownership snapshot returned {} ranges, expected {}",
                responses.len(),
                ranges.record_ranges.len() + 1
            )));
        }

        let mut entries = Vec::new();
        let mut scanned_records = 0usize;
        for (index, operation) in responses.into_iter().enumerate() {
            let TxnOpResponse::Get(range) = operation else {
                return Err(view_protocol_error(
                    "etcd ownership snapshot returned a non-range response",
                ));
            };
            if index > 0 {
                if range.more() {
                    return Err(OwnershipViewError::CapacityExceeded {
                        max_entries: max_entries.get(),
                    });
                }
                scanned_records = scanned_records.checked_add(range.kvs().len()).ok_or(
                    OwnershipViewError::CapacityExceeded {
                        max_entries: max_entries.get(),
                    },
                )?;
                if scanned_records > max_entries.get() {
                    return Err(OwnershipViewError::CapacityExceeded {
                        max_entries: max_entries.get(),
                    });
                }
            } else if range.kvs().len() > 1 {
                return Err(view_protocol_error(
                    "etcd ownership snapshot returned multiple local instances",
                ));
            }
            for kv in range.kvs() {
                let entry_revision = view_revision(kv.mod_revision())?;
                if entry_revision > revision {
                    return Err(view_protocol_error(format!(
                        "etcd ownership record revision {:?} exceeds snapshot revision {:?}",
                        entry_revision, revision
                    )));
                }
                entries.push(EtcdOwnershipSnapshotEntry {
                    key: String::from_utf8(kv.key().to_vec()).map_err(view_protocol_error)?,
                    revision: entry_revision,
                    value: decode_etcd_value(kv.value()).map_err(view_protocol_error)?,
                    floor: None,
                });
            }
        }

        #[cfg(test)]
        if let Some(gap) = &self.ownership_snapshot_proof_gap {
            gap.wait().await;
            gap.wait().await;
        }

        let live_records =
            prove_snapshot_entries(&mut client, &ranges, revision, &mut entries, max_entries)
                .await?;

        #[cfg(test)]
        if let Some(gap) = &self.ownership_view_gap {
            // Real-etcd coverage uses the two barrier phases to insert a
            // mutation after the snapshot transaction and before watch
            // creation. This is deliberately test-only: production always
            // proceeds directly from the coherent snapshot to an R+1 watch.
            gap.wait().await;
            gap.wait().await;
        }

        let start_revision = revision
            .0
            .checked_add(1)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or_else(|| view_protocol_error("etcd ownership revision exhausted"))?;
        let watch = start_real_ownership_watch(
            self.client.clone(),
            ranges,
            start_revision,
            max_entries,
            live_records,
            #[cfg(test)]
            self.ownership_watch_proof_gap.clone(),
        )
        .await?;
        Ok(EtcdOwnershipView {
            snapshot: EtcdOwnershipSnapshot { revision, entries },
            watch,
        })
    }

    async fn watch_prefix(&self, prefix: &str) -> Result<EtcdWatch, PlacementError> {
        let mut client = self.client.clone();
        let mut stream = client
            .watch(prefix, Some(WatchOptions::new().with_prefix()))
            .await
            .map_err(etcd_error)?;
        let (tx, rx) = broadcast::channel(128);
        tokio::spawn(async move {
            while let Ok(Some(response)) = stream.message().await {
                for event in response.events() {
                    let Some(kv) = event.kv() else {
                        continue;
                    };
                    let Ok(key) = String::from_utf8(kv.key().to_vec()) else {
                        continue;
                    };
                    let Ok(version) = placement_version(kv.mod_revision()) else {
                        continue;
                    };
                    let value = match event.event_type() {
                        EventType::Put => decode_etcd_value(kv.value()).ok(),
                        EventType::Delete => None,
                    };
                    let _ = tx.send(EtcdWatchEvent {
                        key,
                        version,
                        value,
                    });
                }
            }
        });
        Ok(EtcdWatch { rx })
    }
}

fn revision_compare(
    key: &str,
    expected: Option<PlacementVersion>,
) -> Result<Compare, PlacementError> {
    match expected {
        Some(version) => Ok(Compare::mod_revision(
            key.as_bytes(),
            CompareOp::Equal,
            i64::try_from(version.modification_revision()).map_err(codec_error)?,
        )),
        None => Ok(Compare::version(key.as_bytes(), CompareOp::Equal, 0)),
    }
}

fn successful_txn_revision(
    response: etcd_client::TxnResponse,
    operation: &str,
) -> Result<PlacementVersion, PlacementError> {
    if !response.succeeded() {
        return Err(PlacementError::CompareAndPutFailed);
    }
    response
        .header()
        .ok_or_else(|| codec_error(format!("etcd {operation} response omitted its header")))
        .and_then(|header| placement_version(header.revision()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProvenEtcdOwnershipRecord {
    version: PlacementVersion,
    value: EtcdValue,
}

type ProvenEtcdOwnershipRecords = BTreeMap<String, ProvenEtcdOwnershipRecord>;

#[derive(Debug)]
struct FloorProofRequest {
    record_key: String,
    floor_key: String,
    epoch_key: Option<PlacementEpochKey>,
}

#[derive(Debug)]
enum FloorProofReadError {
    Compacted {
        requested_revision: PlacementRevision,
    },
    Backend {
        message: String,
    },
    Proof {
        error: OwnershipProofError,
    },
    Protocol {
        message: String,
    },
}

impl FloorProofReadError {
    fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol {
            message: message.into(),
        }
    }

    fn from_etcd(error: etcd_client::Error, requested_revision: PlacementRevision) -> Self {
        if matches!(
            &error,
            etcd_client::Error::GRpcStatus(status) if status.code() == tonic::Code::OutOfRange
        ) {
            Self::Compacted { requested_revision }
        } else {
            Self::Backend {
                message: error.to_string(),
            }
        }
    }

    fn into_view_error(self) -> OwnershipViewError {
        match self {
            Self::Compacted { requested_revision } => OwnershipViewError::Proof {
                error: OwnershipProofError::RevisionUnavailable {
                    requested_revision,
                    message: "etcd compacted the requested historical revision".to_string(),
                },
            },
            Self::Backend { message } => OwnershipViewError::Backend { message },
            Self::Proof { error } => OwnershipViewError::Proof { error },
            Self::Protocol { message } => OwnershipViewError::Protocol { message },
        }
    }

    fn into_watch_error(self) -> OwnershipWatchError {
        match self {
            Self::Compacted { requested_revision } => OwnershipWatchError::Proof {
                error: OwnershipProofError::RevisionUnavailable {
                    requested_revision,
                    message: "etcd compacted the requested historical revision".to_string(),
                },
            },
            Self::Backend { message } => OwnershipWatchError::Backend { message },
            Self::Proof { error } => OwnershipWatchError::Proof { error },
            Self::Protocol { message } => OwnershipWatchError::Protocol { message },
        }
    }
}

fn epoch_key_for_value(value: &EtcdValue) -> Option<PlacementEpochKey> {
    match value {
        EtcdValue::Actor(record) => Some(PlacementEpochKey::Actor(ActorPlacementKey {
            service_kind: record.service_kind.clone(),
            actor_kind: record.actor_kind.clone(),
            actor_id: record.actor_id.clone(),
        })),
        EtcdValue::VirtualShard(record) => {
            Some(PlacementEpochKey::VirtualShard(VirtualShardPlacementKey {
                service_kind: record.service_kind.clone(),
                actor_kind: record.actor_kind.clone(),
                shard_id: record.shard_id,
            }))
        }
        EtcdValue::Singleton(record) => Some(PlacementEpochKey::Singleton(SingletonKey {
            service_kind: record.service_kind.clone(),
            singleton_kind: record.singleton_kind.clone(),
            scope: record.scope.clone(),
        })),
        _ => None,
    }
}

fn floor_key_for_record(
    ranges: &EtcdOwnershipRanges,
    record_key: &str,
) -> Result<Option<String>, FloorProofReadError> {
    let mut matched = ranges.record_ranges.iter().filter_map(|range| {
        record_key
            .strip_prefix(&range.record_prefix)
            .map(|suffix| format!("{}{}", range.floor_prefix, suffix))
    });
    let floor_key = matched.next();
    if matched.next().is_some() {
        return Err(FloorProofReadError::protocol(format!(
            "etcd ownership record {record_key} matched multiple floor ranges"
        )));
    }
    Ok(floor_key)
}

async fn prove_snapshot_entries(
    client: &mut Client,
    ranges: &EtcdOwnershipRanges,
    observed_revision: PlacementRevision,
    entries: &mut [EtcdOwnershipSnapshotEntry],
    max_entries: NonZeroUsize,
) -> Result<ProvenEtcdOwnershipRecords, OwnershipViewError> {
    let mut requests = Vec::new();
    let mut entry_indexes = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        match floor_key_for_record(ranges, &entry.key).map_err(|error| error.into_view_error())? {
            Some(floor_key) => {
                requests.push(FloorProofRequest {
                    record_key: entry.key.clone(),
                    floor_key,
                    epoch_key: epoch_key_for_value(&entry.value),
                });
                entry_indexes.push(index);
            }
            None if entry.key == ranges.local_instance_key => {}
            None => {
                return Err(view_protocol_error(format!(
                    "etcd ownership snapshot returned an unrecognized key {}",
                    entry.key
                )));
            }
        }
    }
    if requests.len() > max_entries.get() {
        return Err(OwnershipViewError::CapacityExceeded {
            max_entries: max_entries.get(),
        });
    }

    let proofs = read_floor_proofs(client, observed_revision, &requests)
        .await
        .map_err(FloorProofReadError::into_view_error)?;
    let mut live_records = ProvenEtcdOwnershipRecords::new();
    for ((entry_index, request), proof) in entry_indexes.into_iter().zip(requests).zip(proofs) {
        let entry = &mut entries[entry_index];
        if live_records
            .insert(
                request.record_key.clone(),
                ProvenEtcdOwnershipRecord {
                    version: PlacementVersion::from_modification_revision(entry.revision.0),
                    value: entry.value.clone(),
                },
            )
            .is_some()
        {
            return Err(view_protocol_error(format!(
                "etcd ownership snapshot returned duplicate record {}",
                request.record_key
            )));
        }
        entry.floor = Some(proof);
    }
    Ok(live_records)
}

async fn read_floor_proofs(
    client: &mut Client,
    observed_revision: PlacementRevision,
    requests: &[FloorProofRequest],
) -> Result<Vec<EtcdOwnershipFloorProof>, FloorProofReadError> {
    let revision = i64::try_from(observed_revision.0).map_err(|error| {
        FloorProofReadError::protocol(format!(
            "etcd epoch-floor proof revision did not fit i64: {error}"
        ))
    })?;
    let mut seen = HashSet::with_capacity(requests.len());
    for request in requests {
        if !seen.insert(request.floor_key.as_str()) {
            return Err(FloorProofReadError::protocol(format!(
                "etcd ownership floor proof requested duplicate key {}",
                request.floor_key
            )));
        }
    }

    let mut proofs = Vec::with_capacity(requests.len());
    for chunk in requests.chunks(FLOOR_PROOF_TXN_OP_LIMIT) {
        let operations = chunk
            .iter()
            .map(|request| {
                TxnOp::get(
                    request.floor_key.clone(),
                    Some(GetOptions::new().with_revision(revision).with_limit(1)),
                )
            })
            .collect::<Vec<_>>();
        let response = client
            .txn(Txn::new().and_then(operations))
            .await
            .map_err(|error| FloorProofReadError::from_etcd(error, observed_revision))?;
        let responses = response.op_responses();
        if responses.len() != chunk.len() {
            return Err(FloorProofReadError::protocol(format!(
                "etcd epoch-floor proof returned {} ranges, expected {}",
                responses.len(),
                chunk.len()
            )));
        }
        for (request, response) in chunk.iter().zip(responses) {
            let TxnOpResponse::Get(range) = response else {
                return Err(FloorProofReadError::protocol(
                    "etcd epoch-floor proof returned a non-range response",
                ));
            };
            if range.more() || range.kvs().len() > 1 {
                return Err(FloorProofReadError::protocol(format!(
                    "etcd epoch-floor proof returned multiple values for {}",
                    request.floor_key
                )));
            }
            let Some(kv) = range.kvs().first() else {
                if let Some(key) = request.epoch_key.clone() {
                    return Err(FloorProofReadError::Proof {
                        error: OwnershipProofError::MissingFloor {
                            key,
                            observed_revision,
                        },
                    });
                }
                return Err(FloorProofReadError::protocol(format!(
                    "etcd ownership record {} has no durable epoch floor at {observed_revision:?}",
                    request.record_key
                )));
            };
            if kv.key() != request.floor_key.as_bytes() {
                return Err(FloorProofReadError::protocol(format!(
                    "etcd epoch-floor proof returned a different key for {}",
                    request.record_key
                )));
            }
            if kv.lease() != 0 {
                if let Some(key) = request.epoch_key.clone()
                    && let Ok(lease_id) = u64::try_from(kv.lease())
                {
                    return Err(FloorProofReadError::Proof {
                        error: OwnershipProofError::LeasedFloor {
                            key,
                            observed_revision,
                            lease_id: LeaseId(lease_id),
                        },
                    });
                }
                return Err(FloorProofReadError::protocol(format!(
                    "etcd epoch floor {} is attached to lease {}",
                    request.floor_key,
                    kv.lease()
                )));
            }
            let version = placement_version(kv.mod_revision()).map_err(|error| {
                FloorProofReadError::protocol(format!(
                    "invalid epoch-floor modification revision for {}: {error}",
                    request.floor_key
                ))
            })?;
            if version.modification_revision() > observed_revision.0 {
                return Err(FloorProofReadError::protocol(format!(
                    "etcd epoch-floor proof {} is newer than observed revision {observed_revision:?}",
                    request.floor_key
                )));
            }
            let value = decode_etcd_value(kv.value()).map_err(|error| {
                request.epoch_key.clone().map_or_else(
                    || {
                        FloorProofReadError::protocol(format!(
                            "invalid epoch-floor value for {}: {error}",
                            request.floor_key
                        ))
                    },
                    |key| FloorProofReadError::Proof {
                        error: OwnershipProofError::MalformedFloor {
                            key,
                            message: error.to_string(),
                        },
                    },
                )
            })?;
            if !matches!(value, EtcdValue::EpochFloor(_)) {
                if let Some(key) = request.epoch_key.clone() {
                    return Err(FloorProofReadError::Proof {
                        error: OwnershipProofError::MalformedFloor {
                            key,
                            message: "stored value is not an epoch floor".to_string(),
                        },
                    });
                }
                return Err(FloorProofReadError::protocol(format!(
                    "etcd epoch-floor proof {} contained a non-floor value",
                    request.floor_key
                )));
            }
            proofs.push(EtcdOwnershipFloorProof {
                observed_revision,
                key: request.floor_key.clone(),
                version,
                value,
            });
        }
    }
    Ok(proofs)
}

async fn prove_watch_update(
    client: &mut Client,
    ranges: &EtcdOwnershipRanges,
    max_entries: NonZeroUsize,
    live_records: &mut ProvenEtcdOwnershipRecords,
    update: &mut EtcdOwnershipWatchUpdate,
) -> Result<(), OwnershipWatchError> {
    let EtcdOwnershipWatchUpdate::Batch(batch) = update else {
        return Ok(());
    };
    let mut requests = Vec::new();
    let mut event_indexes = Vec::new();
    for (index, event) in batch.events.iter().enumerate() {
        let record_key = ownership_event_key(event);
        if let Some(floor_key) = floor_key_for_record(ranges, record_key)
            .map_err(FloorProofReadError::into_watch_error)?
        {
            requests.push(FloorProofRequest {
                record_key: record_key.to_string(),
                floor_key,
                epoch_key: match event {
                    EtcdOwnershipWatchEvent::Upserted { value, .. } => epoch_key_for_value(value),
                    EtcdOwnershipWatchEvent::Deleted { previous_value, .. } => {
                        epoch_key_for_value(previous_value)
                    }
                },
            });
            event_indexes.push(index);
        }
    }
    if requests.is_empty() {
        return Ok(());
    }

    let proofs = read_floor_proofs(client, batch.revision, &requests)
        .await
        .map_err(FloorProofReadError::into_watch_error)?;
    let mut staged = live_records.clone();
    for ((event_index, request), proof) in event_indexes.into_iter().zip(requests).zip(proofs) {
        let event = &mut batch.events[event_index];
        match event {
            EtcdOwnershipWatchEvent::Upserted {
                key,
                version,
                value,
                floor,
            } => {
                if version.modification_revision() != batch.revision.0 {
                    return Err(OwnershipWatchError::Protocol {
                        message: format!(
                            "etcd ownership put {key} token {version:?} did not match batch revision {:?}",
                            batch.revision
                        ),
                    });
                }
                staged.insert(
                    key.clone(),
                    ProvenEtcdOwnershipRecord {
                        version: *version,
                        value: value.clone(),
                    },
                );
                *floor = Some(proof);
            }
            EtcdOwnershipWatchEvent::Deleted {
                key,
                previous_version,
                previous_value,
                floor,
            } => {
                if previous_version.modification_revision() >= batch.revision.0 {
                    return Err(OwnershipWatchError::Protocol {
                        message: format!(
                            "etcd delete {key} previous token {previous_version:?} was not older than batch revision {:?}",
                            batch.revision
                        ),
                    });
                }
                let Some(cached) = staged.get(key) else {
                    if let Some(key) = request.epoch_key.clone() {
                        return Err(OwnershipWatchError::Proof {
                            error: OwnershipProofError::DeletePreviousMismatch { key },
                        });
                    }
                    return Err(OwnershipWatchError::Protocol {
                        message: format!(
                            "etcd delete {key} had no previously proven live ownership record"
                        ),
                    });
                };
                if cached.version != *previous_version || cached.value != *previous_value {
                    if let Some(key) = request.epoch_key.clone() {
                        return Err(OwnershipWatchError::Proof {
                            error: OwnershipProofError::DeletePreviousMismatch { key },
                        });
                    }
                    return Err(OwnershipWatchError::Protocol {
                        message: format!(
                            "etcd delete {key} prev_kv did not match the previously proven live record"
                        ),
                    });
                }
                if proof.version.modification_revision() == batch.revision.0 {
                    if let Some(key) = request.epoch_key.clone() {
                        return Err(OwnershipWatchError::Proof {
                            error: OwnershipProofError::FloorModifiedByDelete {
                                key,
                                observed_revision: batch.revision,
                            },
                        });
                    }
                    return Err(OwnershipWatchError::Protocol {
                        message: format!(
                            "etcd delete {key} modified its durable epoch floor in the deletion revision"
                        ),
                    });
                }
                staged.remove(key);
                *floor = Some(proof);
            }
        }
        debug_assert_eq!(ownership_event_key(event), request.record_key);
    }
    if staged.len() > max_entries.get() {
        return Err(OwnershipWatchError::CapacityExceeded {
            max_entries: max_entries.get(),
        });
    }
    *live_records = staged;
    Ok(())
}

async fn start_real_ownership_watch(
    mut client: Client,
    ranges: EtcdOwnershipRanges,
    start_revision: i64,
    max_entries: NonZeroUsize,
    mut live_records: ProvenEtcdOwnershipRecords,
    #[cfg(test)] proof_gap: Arc<std::sync::Mutex<Option<Arc<tokio::sync::Barrier>>>>,
) -> Result<EtcdOwnershipWatch, OwnershipViewError> {
    let requested_revision = view_revision(start_revision)?;
    let mut stream = client
        .watch(
            ranges.watch_prefix.clone(),
            Some(
                WatchOptions::new()
                    .with_prefix()
                    .with_start_revision(start_revision)
                    .with_prev_key()
                    .with_progress_notify(),
            ),
        )
        .await
        .map_err(|error| OwnershipViewError::WatchStart {
            error: OwnershipWatchError::Backend {
                message: error.to_string(),
            },
        })?;

    let response = stream
        .message()
        .await
        .map_err(|error| OwnershipViewError::WatchStart {
            error: OwnershipWatchError::Backend {
                message: error.to_string(),
            },
        })?
        .ok_or(OwnershipViewError::WatchStart {
            error: OwnershipWatchError::Closed,
        })?;
    if let Some(error) = terminal_watch_error(&response, requested_revision)? {
        return Err(OwnershipViewError::WatchStart { error });
    }
    if !response.created() {
        return Err(OwnershipViewError::WatchStart {
            error: OwnershipWatchError::Protocol {
                message: "etcd watch did not acknowledge creation before sending data".to_string(),
            },
        });
    }
    if !response.events().is_empty() {
        return Err(OwnershipViewError::WatchStart {
            error: OwnershipWatchError::Protocol {
                message: "etcd watch creation response contained events".to_string(),
            },
        });
    }

    // A Created response only acknowledges the watch ID. etcd may send
    // historical events or an immediate compaction/cancellation response
    // afterward. When the cluster is still exactly at snapshot revision R,
    // the Created handshake plus a later linearizable read at R is an
    // equivalent no-gap barrier: no R+1 event exists yet and the registered
    // watch will buffer the first one. Otherwise require an explicit progress
    // response after historical replay. Buffering is bounded so a long replay
    // fails closed instead of returning an already-lagged receiver.
    let mut high_water = PlacementRevision(requested_revision.0.saturating_sub(1));
    let mut startup_updates = Vec::new();
    let barrier_revision =
        current_linearizable_revision(&mut client, &ranges.local_instance_key).await?;
    if barrier_revision == high_water {
        push_startup_update(
            &mut startup_updates,
            EtcdOwnershipWatchUpdate::Progress {
                revision: barrier_revision,
            },
        )
        .map_err(|error| OwnershipViewError::WatchStart { error })?;
    } else {
        stream
            .request_progress()
            .await
            .map_err(|error| OwnershipViewError::WatchStart {
                error: OwnershipWatchError::Backend {
                    message: error.to_string(),
                },
            })?;
        loop {
            let response = stream
                .message()
                .await
                .map_err(|error| OwnershipViewError::WatchStart {
                    error: OwnershipWatchError::Backend {
                        message: error.to_string(),
                    },
                })?
                .ok_or(OwnershipViewError::WatchStart {
                    error: OwnershipWatchError::Closed,
                })?;
            let updates =
                decode_watch_response(&response, requested_revision, &ranges, max_entries)
                    .map_err(|error| OwnershipViewError::WatchStart { error })?;
            let mut caught_up = false;
            for mut update in updates {
                validate_watch_update(&update, &mut high_water)
                    .map_err(|error| OwnershipViewError::WatchStart { error })?;
                #[cfg(test)]
                let gap = matches!(&update, EtcdOwnershipWatchUpdate::Batch(_))
                    .then(|| {
                        proof_gap
                            .lock()
                            .expect("ownership watch proof-gap mutex poisoned")
                            .take()
                    })
                    .flatten();
                #[cfg(test)]
                if let Some(gap) = gap {
                    gap.wait().await;
                    gap.wait().await;
                }
                prove_watch_update(
                    &mut client,
                    &ranges,
                    max_entries,
                    &mut live_records,
                    &mut update,
                )
                .await
                .map_err(|error| OwnershipViewError::WatchStart { error })?;
                caught_up |= startup_progress_reaches_barrier(&update, barrier_revision);
                push_startup_update(&mut startup_updates, update)
                    .map_err(|error| OwnershipViewError::WatchStart { error })?;
            }
            if caught_up {
                break;
            }
            // A queued periodic notification or a lagging watch member can
            // report progress below the post-Created linearizable barrier.
            // Request another response after every non-satisfying response;
            // etcd also does not retain a request made during replay.
            stream
                .request_progress()
                .await
                .map_err(|error| OwnershipViewError::WatchStart {
                    error: OwnershipWatchError::Backend {
                        message: error.to_string(),
                    },
                })?;
        }
    }

    let (tx, rx) = broadcast::channel(WATCH_CAPACITY);
    for update in startup_updates {
        let _ = tx.send(EtcdOwnershipWatchMessage::Update(update));
    }
    let task = tokio::spawn(async move {
        loop {
            let response = match stream.message().await {
                Ok(Some(response)) => response,
                Ok(None) => {
                    let _ = tx.send(EtcdOwnershipWatchMessage::Failed(
                        OwnershipWatchError::Closed,
                    ));
                    break;
                }
                Err(error) => {
                    let _ = tx.send(EtcdOwnershipWatchMessage::Failed(
                        OwnershipWatchError::Backend {
                            message: error.to_string(),
                        },
                    ));
                    break;
                }
            };
            match decode_watch_response(&response, requested_revision, &ranges, max_entries) {
                Ok(updates) => {
                    for mut update in updates {
                        if let Err(error) = validate_watch_update(&update, &mut high_water) {
                            let _ = tx.send(EtcdOwnershipWatchMessage::Failed(error));
                            return;
                        }
                        #[cfg(test)]
                        let gap = matches!(&update, EtcdOwnershipWatchUpdate::Batch(_))
                            .then(|| {
                                proof_gap
                                    .lock()
                                    .expect("ownership watch proof-gap mutex poisoned")
                                    .take()
                            })
                            .flatten();
                        #[cfg(test)]
                        if let Some(gap) = gap {
                            gap.wait().await;
                            gap.wait().await;
                        }
                        if let Err(error) = prove_watch_update(
                            &mut client,
                            &ranges,
                            max_entries,
                            &mut live_records,
                            &mut update,
                        )
                        .await
                        {
                            let _ = tx.send(EtcdOwnershipWatchMessage::Failed(error));
                            return;
                        }
                        if tx.send(EtcdOwnershipWatchMessage::Update(update)).is_err() {
                            return;
                        }
                    }
                }
                Err(error) => {
                    let _ = tx.send(EtcdOwnershipWatchMessage::Failed(error));
                    break;
                }
            }
        }
    });
    Ok(EtcdOwnershipWatch {
        rx,
        abort_handle: Some(task.abort_handle()),
    })
}

fn startup_progress_reaches_barrier(
    update: &EtcdOwnershipWatchUpdate,
    barrier_revision: PlacementRevision,
) -> bool {
    matches!(
        update,
        EtcdOwnershipWatchUpdate::Progress { revision } if *revision >= barrier_revision
    )
}

async fn current_linearizable_revision(
    client: &mut Client,
    key: &str,
) -> Result<PlacementRevision, OwnershipViewError> {
    client
        .get(key, Some(GetOptions::new().with_limit(1)))
        .await
        .map_err(view_backend_error)?
        .header()
        .ok_or_else(|| view_protocol_error("etcd ownership barrier read omitted its header"))
        .and_then(|header| view_revision(header.revision()))
}

fn decode_watch_response(
    response: &etcd_client::WatchResponse,
    requested_revision: PlacementRevision,
    ranges: &EtcdOwnershipRanges,
    max_entries: NonZeroUsize,
) -> Result<Vec<EtcdOwnershipWatchUpdate>, OwnershipWatchError> {
    if let Some(error) = terminal_watch_error(response, requested_revision).map_err(|error| {
        OwnershipWatchError::Protocol {
            message: error.to_string(),
        }
    })? {
        return Err(error);
    }
    if response.created() {
        return Err(OwnershipWatchError::Protocol {
            message: "etcd ownership watch was created more than once".to_string(),
        });
    }

    let response_revision = response
        .header()
        .ok_or_else(|| OwnershipWatchError::Protocol {
            message: "etcd ownership watch response omitted its header".to_string(),
        })
        .and_then(|header| watch_revision(header.revision()))?;

    let max_events =
        ownership_watch_event_limit(max_entries).ok_or(OwnershipWatchError::CapacityExceeded {
            max_entries: max_entries.get(),
        })?;
    let mut batches = BTreeMap::<PlacementRevision, Vec<EtcdOwnershipWatchEvent>>::new();
    let mut seen_keys = HashMap::<PlacementRevision, HashSet<String>>::new();
    let mut previous_event_revision = None;
    for event in response.events() {
        let kv = event.kv().ok_or_else(|| OwnershipWatchError::Protocol {
            message: "etcd ownership watch event omitted its key-value".to_string(),
        })?;
        if !ownership_key_is_selected(ranges, kv.key()) {
            continue;
        }
        let revision = watch_revision(kv.mod_revision())?;
        if revision > response_revision {
            return Err(OwnershipWatchError::Protocol {
                message: format!(
                    "etcd ownership event revision {revision:?} exceeds response revision {response_revision:?}"
                ),
            });
        }
        if let Some(previous) = previous_event_revision
            && revision < previous
        {
            return Err(OwnershipWatchError::Protocol {
                message: format!(
                    "etcd ownership watch event revision regressed from {:?} to {:?}",
                    previous, revision
                ),
            });
        }
        previous_event_revision = Some(revision);
        let key = String::from_utf8(kv.key().to_vec()).map_err(|error| {
            OwnershipWatchError::Protocol {
                message: error.to_string(),
            }
        })?;
        if !seen_keys.entry(revision).or_default().insert(key.clone()) {
            return Err(OwnershipWatchError::Protocol {
                message: format!(
                    "etcd ownership watch revision {revision:?} modified selected key {key} more than once"
                ),
            });
        }
        let event = match event.event_type() {
            EventType::Put => EtcdOwnershipWatchEvent::Upserted {
                key,
                version: watch_version(kv.mod_revision())?,
                value: decode_etcd_value(kv.value()).map_err(watch_protocol_error)?,
                floor: None,
            },
            EventType::Delete => {
                let previous = event
                    .prev_kv()
                    .ok_or_else(|| OwnershipWatchError::Protocol {
                        message: format!("etcd delete for {key} omitted prev_kv"),
                    })?;
                if previous.key() != kv.key() {
                    return Err(OwnershipWatchError::Protocol {
                        message: format!("etcd delete for {key} returned a different prev_kv key"),
                    });
                }
                EtcdOwnershipWatchEvent::Deleted {
                    key,
                    previous_version: watch_version(previous.mod_revision())?,
                    previous_value: decode_etcd_value(previous.value())
                        .map_err(watch_protocol_error)?,
                    floor: None,
                }
            }
        };
        let events = batches.entry(revision).or_default();
        if events.len() >= max_events {
            return Err(OwnershipWatchError::BatchCapacityExceeded { max_events });
        }
        events.push(event);
    }

    let mut updates = batches
        .into_iter()
        .map(|(revision, events)| {
            EtcdOwnershipWatchUpdate::Batch(EtcdOwnershipWatchBatch { revision, events })
        })
        .collect::<Vec<_>>();
    if updates.is_empty() {
        // Only an event-free watch response is a progress barrier. The explicit
        // request_progress call made after the Created handshake provides the
        // startup catch-up barrier; an ordinary event response header is only
        // validated as an upper bound above.
        updates.push(EtcdOwnershipWatchUpdate::Progress {
            revision: response_revision,
        });
    }
    Ok(updates)
}

fn ownership_key_is_selected(ranges: &EtcdOwnershipRanges, key: &[u8]) -> bool {
    key == ranges.local_instance_key.as_bytes()
        || ranges
            .record_ranges
            .iter()
            .any(|range| key.starts_with(range.record_prefix.as_bytes()))
}

fn validate_watch_update(
    update: &EtcdOwnershipWatchUpdate,
    high_water: &mut PlacementRevision,
) -> Result<(), OwnershipWatchError> {
    match update {
        EtcdOwnershipWatchUpdate::Batch(batch) => {
            if batch.revision <= *high_water {
                return Err(OwnershipWatchError::Protocol {
                    message: format!(
                        "etcd ownership batch revision {:?} did not advance beyond {:?}",
                        batch.revision, high_water
                    ),
                });
            }
            *high_water = batch.revision;
        }
        EtcdOwnershipWatchUpdate::Progress { revision } => {
            if *revision < *high_water {
                return Err(OwnershipWatchError::Protocol {
                    message: format!(
                        "etcd ownership progress revision {:?} regressed behind {:?}",
                        revision, high_water
                    ),
                });
            }
            *high_water = *revision;
        }
    }
    Ok(())
}

fn push_startup_update(
    updates: &mut Vec<EtcdOwnershipWatchUpdate>,
    update: EtcdOwnershipWatchUpdate,
) -> Result<(), OwnershipWatchError> {
    if updates.len() >= WATCH_CAPACITY {
        return Err(OwnershipWatchError::StartupBacklogExceeded {
            max_updates: WATCH_CAPACITY,
        });
    }
    updates.push(update);
    Ok(())
}

fn terminal_watch_error(
    response: &etcd_client::WatchResponse,
    requested_revision: PlacementRevision,
) -> Result<Option<OwnershipWatchError>, OwnershipViewError> {
    if response.compact_revision() > 0 {
        return Ok(Some(OwnershipWatchError::Compacted {
            requested_revision,
            compact_revision: view_revision(response.compact_revision())?,
        }));
    }
    if response.canceled() {
        return Ok(Some(OwnershipWatchError::Canceled {
            reason: response.cancel_reason().to_string(),
        }));
    }
    Ok(None)
}

fn watch_revision(revision: i64) -> Result<PlacementRevision, OwnershipWatchError> {
    placement_revision(revision).map_err(watch_protocol_error)
}

fn watch_version(version: i64) -> Result<PlacementVersion, OwnershipWatchError> {
    placement_version(version).map_err(watch_protocol_error)
}

fn watch_protocol_error(error: impl std::fmt::Display) -> OwnershipWatchError {
    OwnershipWatchError::Protocol {
        message: error.to_string(),
    }
}

fn view_revision(revision: i64) -> Result<PlacementRevision, OwnershipViewError> {
    placement_revision(revision).map_err(view_protocol_error)
}

fn view_backend_error(error: impl std::fmt::Display) -> OwnershipViewError {
    OwnershipViewError::Backend {
        message: error.to_string(),
    }
}

fn view_protocol_error(error: impl std::fmt::Display) -> OwnershipViewError {
    OwnershipViewError::Protocol {
        message: error.to_string(),
    }
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryEtcdClient {
    inner: Arc<std::sync::Mutex<InMemoryEtcdState>>,
    instance_leases: Arc<std::sync::Mutex<HashMap<LeaseId, u64>>>,
    next_lease_id: Arc<AtomicU64>,
    #[cfg(test)]
    epoch_reservation_barrier: Arc<std::sync::Mutex<Option<Arc<tokio::sync::Barrier>>>>,
}

#[derive(Debug, Default)]
struct InMemoryEtcdState {
    revision: u64,
    values: HashMap<String, InMemoryEtcdValue>,
    watchers: HashMap<String, broadcast::Sender<EtcdWatchEvent>>,
    ownership_watchers: Vec<InMemoryOwnershipWatcher>,
}

#[derive(Debug, Clone)]
struct InMemoryEtcdValue {
    revision: PlacementRevision,
    value: EtcdValue,
}

#[derive(Debug)]
struct InMemoryOwnershipWatcher {
    ranges: EtcdOwnershipRanges,
    max_entries: NonZeroUsize,
    live_records: ProvenEtcdOwnershipRecords,
    tx: broadcast::Sender<EtcdOwnershipWatchMessage>,
}

fn read_in_memory_floor_proof(
    values: &HashMap<String, InMemoryEtcdValue>,
    observed_revision: PlacementRevision,
    request: &FloorProofRequest,
) -> Result<EtcdOwnershipFloorProof, FloorProofReadError> {
    let Some(entry) = values.get(&request.floor_key) else {
        if let Some(key) = request.epoch_key.clone() {
            return Err(FloorProofReadError::Proof {
                error: OwnershipProofError::MissingFloor {
                    key,
                    observed_revision,
                },
            });
        }
        return Err(FloorProofReadError::protocol(format!(
            "in-memory ownership record {} has no durable epoch floor at {observed_revision:?}",
            request.record_key
        )));
    };
    if entry.revision > observed_revision {
        return Err(FloorProofReadError::protocol(format!(
            "in-memory epoch floor {} is newer than observed revision {observed_revision:?}",
            request.floor_key
        )));
    }
    if !matches!(entry.value, EtcdValue::EpochFloor(_)) {
        if let Some(key) = request.epoch_key.clone() {
            return Err(FloorProofReadError::Proof {
                error: OwnershipProofError::MalformedFloor {
                    key,
                    message: "stored value is not an epoch floor".to_string(),
                },
            });
        }
        return Err(FloorProofReadError::protocol(format!(
            "in-memory epoch-floor proof {} contained a non-floor value",
            request.floor_key
        )));
    }
    Ok(EtcdOwnershipFloorProof {
        observed_revision,
        key: request.floor_key.clone(),
        version: placement_version_from_revision(entry.revision),
        value: entry.value.clone(),
    })
}

#[cfg(test)]
pub(crate) enum InMemoryEtcdMutation {
    Put { key: String, value: EtcdValue },
    Delete { key: String },
}

impl InMemoryEtcdClient {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(InMemoryEtcdState::default())),
            instance_leases: Arc::new(std::sync::Mutex::new(HashMap::new())),
            next_lease_id: Arc::new(AtomicU64::new(1)),
            #[cfg(test)]
            epoch_reservation_barrier: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    pub fn keys(&self) -> Vec<String> {
        let mut keys = self
            .inner
            .lock()
            .expect("in-memory etcd mutex poisoned")
            .values
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        keys.sort();
        keys
    }
}

#[async_trait]
impl EtcdKv for InMemoryEtcdClient {
    async fn put(&self, key: String, value: EtcdValue) -> Result<(), PlacementError> {
        let mut inner = self.inner.lock().expect("in-memory etcd mutex poisoned");
        inner.put_values(vec![(key, value)])?;
        Ok(())
    }

    async fn get(
        &self,
        key: &str,
    ) -> Result<Option<(PlacementVersion, EtcdValue)>, PlacementError> {
        Ok(self
            .inner
            .lock()
            .expect("in-memory etcd mutex poisoned")
            .values
            .get(key)
            .map(|entry| {
                (
                    placement_version_from_revision(entry.revision),
                    entry.value.clone(),
                )
            }))
    }

    async fn list_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, PlacementVersion, EtcdValue)>, PlacementError> {
        Ok(self
            .inner
            .lock()
            .expect("in-memory etcd mutex poisoned")
            .values
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, entry)| {
                (
                    key.clone(),
                    placement_version_from_revision(entry.revision),
                    entry.value.clone(),
                )
            })
            .collect())
    }

    async fn compare_and_put(
        &self,
        key: String,
        expected: Option<PlacementVersion>,
        value: EtcdValue,
    ) -> Result<PlacementVersion, PlacementError> {
        let mut inner = self.inner.lock().expect("in-memory etcd mutex poisoned");
        let current = inner
            .values
            .get(&key)
            .map(|entry| placement_version_from_revision(entry.revision));
        if current != expected {
            return Err(PlacementError::CompareAndPutFailed);
        }
        let revision = inner.put_values(vec![(key, value)])?;
        Ok(placement_version_from_revision(revision))
    }

    async fn reserve_epoch(
        &self,
        request: EtcdEpochReservationRequest,
    ) -> Result<PlacementVersion, PlacementError> {
        #[cfg(test)]
        let barrier = {
            self.epoch_reservation_barrier
                .lock()
                .expect("in-memory etcd epoch barrier mutex poisoned")
                .clone()
        };
        #[cfg(test)]
        if let Some(barrier) = barrier {
            barrier.wait().await;
        }
        let mut inner = self.inner.lock().expect("in-memory etcd mutex poisoned");
        if !inner.matches_revision(&request.record_key, request.expected_record)
            || !inner.matches_versioned_value(&request.floor_key, request.expected_floor.as_ref())
            || !inner.matches_guard(request.guard.as_ref())
        {
            return Err(PlacementError::CompareAndPutFailed);
        }
        let revision = inner.put_values(vec![(request.floor_key, request.floor_value)])?;
        Ok(placement_version_from_revision(revision))
    }

    async fn commit_epoch(
        &self,
        request: EtcdEpochCommitRequest,
    ) -> Result<PlacementVersion, PlacementError> {
        let mut inner = self.inner.lock().expect("in-memory etcd mutex poisoned");
        let expected_floor = (request.floor_token, request.floor_value.clone());
        if !inner.matches_revision(&request.record_key, request.expected_record)
            || !inner.matches_versioned_value(&request.floor_key, Some(&expected_floor))
            || !inner.matches_guard(request.guard.as_ref())
        {
            return Err(PlacementError::CompareAndPutFailed);
        }
        let revision = inner.put_values(vec![
            (request.floor_key, request.floor_value),
            (request.record_key, request.record_value),
        ])?;
        Ok(placement_version_from_revision(revision))
    }

    async fn compare_and_put_epoch(
        &self,
        request: EtcdLegacyEpochPutRequest,
    ) -> Result<PlacementVersion, PlacementError> {
        let mut inner = self.inner.lock().expect("in-memory etcd mutex poisoned");
        if !inner.matches_revision(&request.record_key, request.expected_record)
            || !inner.matches_versioned_value(&request.floor_key, request.expected_floor.as_ref())
        {
            return Err(PlacementError::CompareAndPutFailed);
        }
        let revision = inner.put_values(vec![
            (request.floor_key, request.floor_value),
            (request.record_key, request.record_value),
        ])?;
        Ok(placement_version_from_revision(revision))
    }

    async fn delete(&self, key: &str) -> Result<(), PlacementError> {
        self.inner
            .lock()
            .expect("in-memory etcd mutex poisoned")
            .delete_value(key)?;
        Ok(())
    }

    async fn compare_and_delete(
        &self,
        key: String,
        expected: EtcdValue,
    ) -> Result<(), PlacementError> {
        let mut inner = self.inner.lock().expect("in-memory etcd mutex poisoned");
        match inner.values.get(&key) {
            Some(current) if current.value == expected => {}
            _ => return Err(PlacementError::CompareAndPutFailed),
        }
        inner.delete_value(&key)?;
        Ok(())
    }

    async fn grant_instance_lease(&self) -> Result<LeaseId, PlacementError> {
        let lease_id = LeaseId(self.next_lease_id.fetch_add(1, Ordering::SeqCst));
        self.instance_leases
            .lock()
            .expect("in-memory etcd leases mutex poisoned")
            .insert(lease_id, 0);
        Ok(lease_id)
    }

    async fn keepalive_instance_lease(&self, lease_id: LeaseId) -> Result<(), PlacementError> {
        let mut leases = self
            .instance_leases
            .lock()
            .expect("in-memory etcd leases mutex poisoned");
        let Some(keepalives) = leases.get_mut(&lease_id) else {
            return Err(PlacementError::InstanceLeaseNotFound { lease_id });
        };
        *keepalives += 1;
        Ok(())
    }

    async fn next_lease_id(&self) -> Result<LeaseId, PlacementError> {
        Ok(LeaseId(self.next_lease_id.fetch_add(1, Ordering::SeqCst)))
    }

    async fn open_ownership_view(
        &self,
        ranges: EtcdOwnershipRanges,
        max_entries: NonZeroUsize,
    ) -> Result<EtcdOwnershipView, OwnershipViewError> {
        let mut inner = self.inner.lock().expect("in-memory etcd mutex poisoned");
        inner
            .ownership_watchers
            .retain(|watcher| watcher.tx.receiver_count() > 0);
        let mut entries = Vec::new();
        if let Some(entry) = inner.values.get(&ranges.local_instance_key) {
            entries.push(EtcdOwnershipSnapshotEntry {
                key: ranges.local_instance_key.clone(),
                revision: entry.revision,
                value: entry.value.clone(),
                floor: None,
            });
        }
        let mut keys = Vec::new();
        for key in inner.values.keys().filter(|key| {
            ranges
                .record_ranges
                .iter()
                .any(|range| key.starts_with(&range.record_prefix))
        }) {
            keys.push(key.clone());
            if keys.len() > max_entries.get() {
                return Err(OwnershipViewError::CapacityExceeded {
                    max_entries: max_entries.get(),
                });
            }
        }
        keys.sort();
        let revision = PlacementRevision(inner.revision);
        let mut live_records = ProvenEtcdOwnershipRecords::new();
        for key in keys {
            let entry = inner
                .values
                .get(&key)
                .expect("the in-memory ownership key was collected under the same mutex");
            let floor_key = floor_key_for_record(&ranges, &key)
                .map_err(FloorProofReadError::into_view_error)?
                .ok_or_else(|| {
                    view_protocol_error(format!(
                        "in-memory ownership snapshot could not derive a floor key for {key}"
                    ))
                })?;
            let request = FloorProofRequest {
                record_key: key.clone(),
                floor_key,
                epoch_key: epoch_key_for_value(&entry.value),
            };
            let proof = read_in_memory_floor_proof(&inner.values, revision, &request)
                .map_err(FloorProofReadError::into_view_error)?;
            let record = ProvenEtcdOwnershipRecord {
                version: placement_version_from_revision(entry.revision),
                value: entry.value.clone(),
            };
            live_records.insert(key.clone(), record);
            entries.push(EtcdOwnershipSnapshotEntry {
                key,
                revision: entry.revision,
                value: entry.value.clone(),
                floor: Some(proof),
            });
        }
        let (tx, rx) = broadcast::channel(WATCH_CAPACITY);
        inner.ownership_watchers.push(InMemoryOwnershipWatcher {
            ranges,
            max_entries,
            live_records,
            tx,
        });
        Ok(EtcdOwnershipView {
            snapshot: EtcdOwnershipSnapshot { revision, entries },
            watch: EtcdOwnershipWatch {
                rx,
                abort_handle: None,
            },
        })
    }

    async fn watch_prefix(&self, prefix: &str) -> Result<EtcdWatch, PlacementError> {
        let mut inner = self.inner.lock().expect("in-memory etcd mutex poisoned");
        let rx = inner
            .watchers
            .entry(prefix.to_string())
            .or_insert_with(|| {
                let (tx, _rx) = broadcast::channel(WATCH_CAPACITY);
                tx
            })
            .subscribe();
        Ok(EtcdWatch { rx })
    }
}

impl InMemoryEtcdClient {
    #[cfg(test)]
    pub(crate) fn set_epoch_reservation_barrier_for_test(
        &self,
        barrier: Option<Arc<tokio::sync::Barrier>>,
    ) {
        *self
            .epoch_reservation_barrier
            .lock()
            .expect("in-memory etcd epoch barrier mutex poisoned") = barrier;
    }

    #[cfg(test)]
    pub(crate) fn put_same_revision_for_test(
        &self,
        values: Vec<(String, EtcdValue)>,
    ) -> Result<PlacementRevision, PlacementError> {
        self.inner
            .lock()
            .expect("in-memory etcd mutex poisoned")
            .put_values(values)
    }

    #[cfg(test)]
    pub(crate) fn mutate_same_revision_for_test(
        &self,
        mutations: Vec<InMemoryEtcdMutation>,
    ) -> Result<PlacementRevision, PlacementError> {
        self.inner
            .lock()
            .expect("in-memory etcd mutex poisoned")
            .apply_test_mutations(mutations)
    }

    #[cfg(test)]
    pub(crate) fn fail_ownership_watches_for_test(&self, error: OwnershipWatchError) {
        let mut inner = self.inner.lock().expect("in-memory etcd mutex poisoned");
        for watcher in inner.ownership_watchers.drain(..) {
            let _ = watcher
                .tx
                .send(EtcdOwnershipWatchMessage::Failed(error.clone()));
        }
    }

    #[cfg(test)]
    pub(crate) fn progress_ownership_watches_for_test(&self, revision: PlacementRevision) {
        let mut inner = self.inner.lock().expect("in-memory etcd mutex poisoned");
        inner.ownership_watchers.retain(|watcher| {
            watcher
                .tx
                .send(EtcdOwnershipWatchMessage::Update(
                    EtcdOwnershipWatchUpdate::Progress { revision },
                ))
                .is_ok()
        });
    }

    #[cfg(test)]
    pub(crate) fn ownership_watcher_count_for_test(&self) -> usize {
        self.inner
            .lock()
            .expect("in-memory etcd mutex poisoned")
            .ownership_watchers
            .len()
    }

    #[cfg(test)]
    pub(crate) fn active_ownership_watcher_count_for_test(&self) -> usize {
        self.inner
            .lock()
            .expect("in-memory etcd mutex poisoned")
            .ownership_watchers
            .iter()
            .filter(|watcher| watcher.tx.receiver_count() > 0)
            .count()
    }
}

impl InMemoryEtcdState {
    fn matches_revision(&self, key: &str, expected: Option<PlacementVersion>) -> bool {
        self.values
            .get(key)
            .map(|entry| placement_version_from_revision(entry.revision))
            == expected
    }

    fn matches_versioned_value(
        &self,
        key: &str,
        expected: Option<&(PlacementVersion, EtcdValue)>,
    ) -> bool {
        match (self.values.get(key), expected) {
            (None, None) => true,
            (Some(current), Some((token, value))) => {
                placement_version_from_revision(current.revision) == *token
                    && current.value == *value
            }
            _ => false,
        }
    }

    fn matches_guard(&self, guard: Option<&EtcdValueGuard>) -> bool {
        match guard {
            None => true,
            Some(guard) => self
                .values
                .get(&guard.key)
                .is_some_and(|current| current.value == guard.value),
        }
    }

    fn next_revision(&mut self) -> Result<PlacementRevision, PlacementError> {
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| codec_error("in-memory etcd revision exhausted"))?;
        Ok(PlacementRevision(self.revision))
    }

    fn put_values(
        &mut self,
        values: Vec<(String, EtcdValue)>,
    ) -> Result<PlacementRevision, PlacementError> {
        if values.is_empty() {
            return Ok(PlacementRevision(self.revision));
        }
        let mut seen = HashSet::with_capacity(values.len());
        let mut prepared = Vec::with_capacity(values.len());
        for (key, value) in values {
            if !seen.insert(key.clone()) {
                return Err(codec_error(format!(
                    "in-memory etcd batch modifies key {key} more than once"
                )));
            }
            prepared.push((key, value));
        }
        let revision = self.next_revision()?;
        let version = placement_version_from_revision(revision);
        let mut ownership_events = Vec::with_capacity(prepared.len());
        for (key, value) in prepared {
            self.values.insert(
                key.clone(),
                InMemoryEtcdValue {
                    revision,
                    value: value.clone(),
                },
            );
            self.notify_legacy(EtcdWatchEvent {
                key: key.clone(),
                version,
                value: Some(value.clone()),
            });
            ownership_events.push(EtcdOwnershipWatchEvent::Upserted {
                key,
                version,
                value,
                floor: None,
            });
        }
        self.notify_ownership(EtcdOwnershipWatchBatch {
            revision,
            events: ownership_events,
        });
        Ok(revision)
    }

    fn delete_value(&mut self, key: &str) -> Result<(), PlacementError> {
        let Some(previous) = self.values.get(key).cloned() else {
            return Ok(());
        };
        let revision = self.next_revision()?;
        self.values.remove(key);
        self.notify_legacy(EtcdWatchEvent {
            key: key.to_string(),
            version: placement_version_from_revision(revision),
            value: None,
        });
        self.notify_ownership(EtcdOwnershipWatchBatch {
            revision,
            events: vec![EtcdOwnershipWatchEvent::Deleted {
                key: key.to_string(),
                previous_version: placement_version_from_revision(previous.revision),
                previous_value: previous.value,
                floor: None,
            }],
        });
        Ok(())
    }

    #[cfg(test)]
    fn apply_test_mutations(
        &mut self,
        mutations: Vec<InMemoryEtcdMutation>,
    ) -> Result<PlacementRevision, PlacementError> {
        if mutations.is_empty() {
            return Ok(PlacementRevision(self.revision));
        }
        let mut seen = HashSet::with_capacity(mutations.len());
        let mut prepared = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            let key = match &mutation {
                InMemoryEtcdMutation::Put { key, .. } | InMemoryEtcdMutation::Delete { key } => key,
            };
            if !seen.insert(key.clone()) {
                return Err(codec_error(format!(
                    "in-memory etcd batch modifies key {key} more than once"
                )));
            }
            match mutation {
                InMemoryEtcdMutation::Put { key, value } => {
                    prepared.push((key, Some(value), None));
                }
                InMemoryEtcdMutation::Delete { key } => {
                    prepared.push((key.clone(), None, self.values.get(&key).cloned()));
                }
            }
        }
        let revision = self.next_revision()?;
        let version = placement_version_from_revision(revision);
        let mut ownership_events = Vec::new();
        for (key, put, delete) in prepared {
            if let Some(value) = put {
                self.values.insert(
                    key.clone(),
                    InMemoryEtcdValue {
                        revision,
                        value: value.clone(),
                    },
                );
                self.notify_legacy(EtcdWatchEvent {
                    key: key.clone(),
                    version,
                    value: Some(value.clone()),
                });
                ownership_events.push(EtcdOwnershipWatchEvent::Upserted {
                    key,
                    version,
                    value,
                    floor: None,
                });
            } else if let Some(previous) = delete {
                self.values.remove(&key);
                self.notify_legacy(EtcdWatchEvent {
                    key: key.clone(),
                    version,
                    value: None,
                });
                ownership_events.push(EtcdOwnershipWatchEvent::Deleted {
                    key,
                    previous_version: placement_version_from_revision(previous.revision),
                    previous_value: previous.value,
                    floor: None,
                });
            }
        }
        self.notify_ownership(EtcdOwnershipWatchBatch {
            revision,
            events: ownership_events,
        });
        Ok(revision)
    }

    fn notify_legacy(&self, event: EtcdWatchEvent) {
        for (prefix, tx) in &self.watchers {
            if event.key.starts_with(prefix) {
                let _ = tx.send(event.clone());
            }
        }
    }

    fn notify_ownership(&mut self, batch: EtcdOwnershipWatchBatch) {
        let values = &self.values;
        self.ownership_watchers.retain_mut(|watcher| {
            if watcher.tx.receiver_count() == 0 {
                return false;
            }
            let Some(max_events) = ownership_watch_event_limit(watcher.max_entries) else {
                let _ = watcher.tx.send(EtcdOwnershipWatchMessage::Failed(
                    OwnershipWatchError::CapacityExceeded {
                        max_entries: watcher.max_entries.get(),
                    },
                ));
                return false;
            };
            let mut saw_watched_event = false;
            let mut events = Vec::with_capacity(max_events.min(batch.events.len()));
            for event in &batch.events {
                if !ownership_event_key(event).starts_with(&watcher.ranges.watch_prefix) {
                    continue;
                }
                saw_watched_event = true;
                if !ownership_key_is_selected(
                    &watcher.ranges,
                    ownership_event_key(event).as_bytes(),
                ) {
                    continue;
                }
                if events.len() == max_events {
                    let _ = watcher.tx.send(EtcdOwnershipWatchMessage::Failed(
                        OwnershipWatchError::BatchCapacityExceeded { max_events },
                    ));
                    return false;
                }
                events.push(event.clone());
            }
            if !saw_watched_event {
                return true;
            }
            if events.is_empty() {
                return watcher
                    .tx
                    .send(EtcdOwnershipWatchMessage::Update(
                        EtcdOwnershipWatchUpdate::Progress {
                            revision: batch.revision,
                        },
                    ))
                    .is_ok();
            }
            let mut update = EtcdOwnershipWatchBatch {
                revision: batch.revision,
                events,
            };
            match prove_in_memory_watch_batch(values, watcher, &mut update) {
                Ok(()) => watcher
                    .tx
                    .send(EtcdOwnershipWatchMessage::Update(
                        EtcdOwnershipWatchUpdate::Batch(update),
                    ))
                    .is_ok(),
                Err(error) => {
                    let _ = watcher.tx.send(EtcdOwnershipWatchMessage::Failed(error));
                    false
                }
            }
        });
    }
}

fn prove_in_memory_watch_batch(
    values: &HashMap<String, InMemoryEtcdValue>,
    watcher: &mut InMemoryOwnershipWatcher,
    batch: &mut EtcdOwnershipWatchBatch,
) -> Result<(), OwnershipWatchError> {
    let max_events = ownership_watch_event_limit(watcher.max_entries).ok_or(
        OwnershipWatchError::CapacityExceeded {
            max_entries: watcher.max_entries.get(),
        },
    )?;
    if batch.events.len() > max_events {
        return Err(OwnershipWatchError::BatchCapacityExceeded { max_events });
    }
    let mut staged = watcher.live_records.clone();
    let mut seen = HashSet::new();
    for event in &mut batch.events {
        let record_key = ownership_event_key(event).to_string();
        if !seen.insert(record_key.clone()) {
            return Err(OwnershipWatchError::Protocol {
                message: format!(
                    "in-memory ownership batch modified selected key {record_key} more than once"
                ),
            });
        }
        let Some(floor_key) = floor_key_for_record(&watcher.ranges, &record_key)
            .map_err(FloorProofReadError::into_watch_error)?
        else {
            continue;
        };
        let epoch_key = match event {
            EtcdOwnershipWatchEvent::Upserted { value, .. } => epoch_key_for_value(value),
            EtcdOwnershipWatchEvent::Deleted { previous_value, .. } => {
                epoch_key_for_value(previous_value)
            }
        };
        let request = FloorProofRequest {
            record_key: record_key.clone(),
            floor_key,
            epoch_key: epoch_key.clone(),
        };
        let proof = read_in_memory_floor_proof(values, batch.revision, &request)
            .map_err(FloorProofReadError::into_watch_error)?;
        match event {
            EtcdOwnershipWatchEvent::Upserted {
                key,
                version,
                value,
                floor,
            } => {
                if version.modification_revision() != batch.revision.0 {
                    return Err(OwnershipWatchError::Protocol {
                        message: format!(
                            "in-memory ownership put {key} token {version:?} did not match batch revision {:?}",
                            batch.revision
                        ),
                    });
                }
                staged.insert(
                    key.clone(),
                    ProvenEtcdOwnershipRecord {
                        version: *version,
                        value: value.clone(),
                    },
                );
                *floor = Some(proof);
            }
            EtcdOwnershipWatchEvent::Deleted {
                key,
                previous_version,
                previous_value,
                floor,
            } => {
                let Some(cached) = staged.get(key) else {
                    return Err(epoch_key.map_or_else(
                        || OwnershipWatchError::Protocol {
                            message: format!(
                                "in-memory delete {key} had no previously proven live record"
                            ),
                        },
                        |key| OwnershipWatchError::Proof {
                            error: OwnershipProofError::DeletePreviousMismatch { key },
                        },
                    ));
                };
                if cached.version != *previous_version || cached.value != *previous_value {
                    return Err(epoch_key.map_or_else(
                        || OwnershipWatchError::Protocol {
                            message: format!(
                                "in-memory delete {key} prev_kv did not match the proven record"
                            ),
                        },
                        |key| OwnershipWatchError::Proof {
                            error: OwnershipProofError::DeletePreviousMismatch { key },
                        },
                    ));
                }
                if proof.version.modification_revision() == batch.revision.0 {
                    return Err(epoch_key.map_or_else(
                        || OwnershipWatchError::Protocol {
                            message: format!(
                                "in-memory delete {key} modified its floor in the deletion revision"
                            ),
                        },
                        |key| OwnershipWatchError::Proof {
                            error: OwnershipProofError::FloorModifiedByDelete {
                                key,
                                observed_revision: batch.revision,
                            },
                        },
                    ));
                }
                staged.remove(key);
                *floor = Some(proof);
            }
        }
    }
    if staged.len() > watcher.max_entries.get() {
        return Err(OwnershipWatchError::CapacityExceeded {
            max_entries: watcher.max_entries.get(),
        });
    }
    watcher.live_records = staged;
    Ok(())
}

fn placement_version_from_revision(revision: PlacementRevision) -> PlacementVersion {
    PlacementVersion::from_modification_revision(revision.0)
}

fn ownership_event_key(event: &EtcdOwnershipWatchEvent) -> &str {
    match event {
        EtcdOwnershipWatchEvent::Upserted { key, .. }
        | EtcdOwnershipWatchEvent::Deleted { key, .. } => key,
    }
}

#[cfg(test)]
mod real_tests;
