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
    LeaseId, OwnershipViewError, OwnershipWatchError, PlacementRevision, PlacementVersion,
};

const WATCH_CAPACITY: usize = 128;

#[derive(Debug, Clone)]
pub struct EtcdOwnershipRanges {
    pub local_instance_key: String,
    pub record_prefixes: Vec<String>,
    pub watch_prefix: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EtcdOwnershipSnapshotEntry {
    pub key: String,
    pub revision: PlacementRevision,
    pub value: EtcdValue,
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
    },
    Deleted {
        key: String,
        previous_version: PlacementVersion,
        previous_value: EtcdValue,
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
        let mut operations = Vec::with_capacity(ranges.record_prefixes.len() + 1);
        operations.push(TxnOp::get(ranges.local_instance_key.clone(), None));
        operations.extend(ranges.record_prefixes.iter().map(|prefix| {
            TxnOp::get(
                prefix.clone(),
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
        if responses.len() != ranges.record_prefixes.len() + 1 {
            return Err(view_protocol_error(format!(
                "etcd ownership snapshot returned {} ranges, expected {}",
                responses.len(),
                ranges.record_prefixes.len() + 1
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
                });
            }
        }

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
            ranges.watch_prefix,
            start_revision,
            max_entries,
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

async fn start_real_ownership_watch(
    mut client: Client,
    prefix: String,
    start_revision: i64,
    max_entries: NonZeroUsize,
) -> Result<EtcdOwnershipWatch, OwnershipViewError> {
    let requested_revision = view_revision(start_revision)?;
    let mut stream = client
        .watch(
            prefix,
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

    stream
        .request_progress()
        .await
        .map_err(|error| OwnershipViewError::WatchStart {
            error: OwnershipWatchError::Backend {
                message: error.to_string(),
            },
        })?;

    // A Created response only acknowledges the watch ID. etcd may send
    // historical events or an immediate compaction/cancellation response
    // afterward. Do not expose the view until an explicit progress response
    // proves the R+1 watch has caught up. Buffering is bounded so a long replay
    // fails closed instead of returning an already-lagged receiver.
    let mut high_water = PlacementRevision(requested_revision.0.saturating_sub(1));
    let mut startup_updates = Vec::new();
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
        let response_had_events = !response.events().is_empty();
        let updates = decode_watch_response(&response, requested_revision, max_entries)
            .map_err(|error| OwnershipViewError::WatchStart { error })?;
        let mut caught_up = false;
        for update in updates {
            validate_watch_update(&update, &mut high_water)
                .map_err(|error| OwnershipViewError::WatchStart { error })?;
            caught_up |= matches!(&update, EtcdOwnershipWatchUpdate::Progress { .. });
            push_startup_update(&mut startup_updates, update)
                .map_err(|error| OwnershipViewError::WatchStart { error })?;
        }
        if caught_up {
            break;
        }
        if response_had_events {
            // etcd does not retain a progress request made while historical
            // replay is pending, so request another barrier after each replay
            // response until the watcher is caught up.
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
    tokio::spawn(async move {
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
            match decode_watch_response(&response, requested_revision, max_entries) {
                Ok(updates) => {
                    for update in updates {
                        if let Err(error) = validate_watch_update(&update, &mut high_water) {
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
    Ok(EtcdOwnershipWatch { rx })
}

fn decode_watch_response(
    response: &etcd_client::WatchResponse,
    requested_revision: PlacementRevision,
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

    let mut batches = BTreeMap::<PlacementRevision, Vec<EtcdOwnershipWatchEvent>>::new();
    let mut previous_event_revision = None;
    for event in response.events() {
        let kv = event.kv().ok_or_else(|| OwnershipWatchError::Protocol {
            message: "etcd ownership watch event omitted its key-value".to_string(),
        })?;
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
        let event = match event.event_type() {
            EventType::Put => EtcdOwnershipWatchEvent::Upserted {
                key,
                version: watch_version(kv.mod_revision())?,
                value: decode_etcd_value(kv.value()).map_err(watch_protocol_error)?,
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
                }
            }
        };
        let events = batches.entry(revision).or_default();
        if events.len() >= max_entries.get() {
            return Err(OwnershipWatchError::CapacityExceeded {
                max_entries: max_entries.get(),
            });
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
    ownership_watchers: HashMap<String, broadcast::Sender<EtcdOwnershipWatchMessage>>,
}

#[derive(Debug, Clone)]
struct InMemoryEtcdValue {
    revision: PlacementRevision,
    value: EtcdValue,
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
        let mut entries = Vec::new();
        if let Some(entry) = inner.values.get(&ranges.local_instance_key) {
            entries.push(EtcdOwnershipSnapshotEntry {
                key: ranges.local_instance_key.clone(),
                revision: entry.revision,
                value: entry.value.clone(),
            });
        }
        let mut keys = inner
            .values
            .keys()
            .filter(|key| {
                ranges
                    .record_prefixes
                    .iter()
                    .any(|prefix| key.starts_with(prefix))
            })
            .cloned()
            .collect::<Vec<_>>();
        keys.sort();
        if keys.len() > max_entries.get() {
            return Err(OwnershipViewError::CapacityExceeded {
                max_entries: max_entries.get(),
            });
        }
        entries.extend(keys.into_iter().filter_map(|key| {
            inner
                .values
                .get(&key)
                .map(|entry| EtcdOwnershipSnapshotEntry {
                    key,
                    revision: entry.revision,
                    value: entry.value.clone(),
                })
        }));
        let revision = PlacementRevision(inner.revision);
        let rx = inner
            .ownership_watchers
            .entry(ranges.watch_prefix)
            .or_insert_with(|| {
                let (tx, _rx) = broadcast::channel(WATCH_CAPACITY);
                tx
            })
            .subscribe();
        Ok(EtcdOwnershipView {
            snapshot: EtcdOwnershipSnapshot { revision, entries },
            watch: EtcdOwnershipWatch { rx },
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
        let inner = self.inner.lock().expect("in-memory etcd mutex poisoned");
        for tx in inner.ownership_watchers.values() {
            let _ = tx.send(EtcdOwnershipWatchMessage::Failed(error.clone()));
        }
    }

    #[cfg(test)]
    pub(crate) fn progress_ownership_watches_for_test(&self, revision: PlacementRevision) {
        let inner = self.inner.lock().expect("in-memory etcd mutex poisoned");
        for tx in inner.ownership_watchers.values() {
            let _ = tx.send(EtcdOwnershipWatchMessage::Update(
                EtcdOwnershipWatchUpdate::Progress { revision },
            ));
        }
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

    fn notify_ownership(&self, batch: EtcdOwnershipWatchBatch) {
        for (prefix, tx) in &self.ownership_watchers {
            let events = batch
                .events
                .iter()
                .filter(|event| ownership_event_key(event).starts_with(prefix))
                .cloned()
                .collect::<Vec<_>>();
            if !events.is_empty() {
                let _ = tx.send(EtcdOwnershipWatchMessage::Update(
                    EtcdOwnershipWatchUpdate::Batch(EtcdOwnershipWatchBatch {
                        revision: batch.revision,
                        events,
                    }),
                ));
            }
        }
    }
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
