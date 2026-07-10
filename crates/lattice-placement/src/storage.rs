use std::num::NonZeroUsize;

use async_trait::async_trait;
use lattice_core::actor_ref::Epoch;
use lattice_core::id::ActorId;
use lattice_core::instance::InstanceId;
use lattice_core::kind::{ActorKind, ServiceKind};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::error::PlacementError;
use crate::registry::InstanceRecord;
use crate::sharding::VirtualShardId;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActorPlacementKey {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub actor_id: ActorId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LeaseId(pub u64);

/// Opaque non-ABA token for one placement record modification.
///
/// Backends must derive this from a store revision that never resets when a
/// key is deleted and recreated. Callers may compare and return the token but
/// must not construct it or perform arithmetic on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PlacementVersion(u64);

impl PlacementVersion {
    pub(crate) const fn from_modification_revision(revision: u64) -> Self {
        Self(revision)
    }

    pub(crate) const fn modification_revision(self) -> u64 {
        self.0
    }
}

/// A store-wide ordering token for coherent placement snapshots and watches.
///
/// [`PlacementVersion`] is an opaque modification token used only for one
/// record's compare-and-set. `PlacementRevision` orders all ownership
/// mutations observed through one placement store, including changes that do
/// not modify that record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PlacementRevision(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlacementState {
    Activating,
    Running,
    Draining,
    Migrating,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorPlacementRecord {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub actor_id: ActorId,
    pub owner: InstanceId,
    pub epoch: Epoch,
    pub lease_id: LeaseId,
    pub state: PlacementState,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VirtualShardPlacementKey {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub shard_id: VirtualShardId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VirtualShardPlacementRecord {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub shard_id: VirtualShardId,
    pub owner: InstanceId,
    pub epoch: Epoch,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SingletonKey {
    pub service_kind: ServiceKind,
    pub singleton_kind: ActorKind,
    pub scope: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PlacementEpochKey {
    Actor(ActorPlacementKey),
    VirtualShard(VirtualShardPlacementKey),
    Singleton(SingletonKey),
}

/// Durable, non-leased high-water mark for one placement identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochFloorRecord {
    pub key: PlacementEpochKey,
    pub epoch: Epoch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlacementEpochGuard {
    Actor(LeaseId),
    Singleton(LeaseId),
}

/// One-shot proof that a store atomically advanced an identity's epoch floor.
///
/// A reservation is not ownership. It may be consumed only by the matching
/// commit operation, and abandoning it intentionally burns its epoch.
#[derive(Debug)]
pub struct PlacementEpochReservation {
    pub(crate) key: PlacementEpochKey,
    pub(crate) epoch: Epoch,
    pub(crate) expected_record: Option<PlacementVersion>,
    pub(crate) floor_token: PlacementVersion,
    pub(crate) guard: Option<PlacementEpochGuard>,
}

impl PlacementEpochReservation {
    pub(crate) const fn new(
        key: PlacementEpochKey,
        epoch: Epoch,
        expected_record: Option<PlacementVersion>,
        floor_token: PlacementVersion,
        guard: Option<PlacementEpochGuard>,
    ) -> Self {
        Self {
            key,
            epoch,
            expected_record,
            floor_token,
            guard,
        }
    }

    pub fn key(&self) -> &PlacementEpochKey {
        &self.key
    }

    pub const fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub(crate) const fn expected_record(&self) -> Option<PlacementVersion> {
        self.expected_record
    }

    pub(crate) const fn floor_token(&self) -> PlacementVersion {
        self.floor_token
    }

    pub(crate) const fn guard(&self) -> Option<PlacementEpochGuard> {
        self.guard
    }
}

pub(crate) fn next_reserved_epoch(
    record_epoch: Option<Epoch>,
    floor_epoch: Option<Epoch>,
) -> Result<Epoch, PlacementError> {
    validate_floor(record_epoch, floor_epoch)?;
    let base = record_epoch
        .into_iter()
        .chain(floor_epoch)
        .max()
        .unwrap_or(Epoch(0));
    base.0
        .checked_add(1)
        .map(Epoch)
        .ok_or(PlacementError::EpochExhausted)
}

/// Verifies that a live placement record descends from the durable epoch floor.
///
/// A hardened record write updates the record and floor in one transaction, so
/// equal epochs have the same modification revision. A reservation may burn a
/// newer floor without replacing the incumbent record; in that case both the
/// floor epoch and its modification revision are strictly newer. Missing
/// records are valid with or without a floor, but an existing record without a
/// floor cannot be distinguished from a legacy replay and must fail closed.
pub(crate) fn validate_epoch_floor_lineage(
    record: Option<(PlacementVersion, Epoch)>,
    floor: Option<(PlacementVersion, Epoch)>,
) -> Result<(), PlacementError> {
    let Some((record_token, record_epoch)) = record else {
        return Ok(());
    };
    let Some((floor_token, floor_epoch)) = floor else {
        return Err(PlacementError::EpochFloorUnproven {
            record: record_token,
            floor: None,
        });
    };
    if floor_epoch < record_epoch {
        return Err(PlacementError::EpochFloorCorrupt {
            floor: floor_epoch,
            record: record_epoch,
        });
    }

    let proven = if floor_epoch == record_epoch {
        floor_token == record_token
    } else {
        floor_token.modification_revision() > record_token.modification_revision()
    };
    if !proven {
        return Err(PlacementError::EpochFloorUnproven {
            record: record_token,
            floor: Some(floor_token),
        });
    }
    Ok(())
}

pub(crate) fn validate_legacy_epoch(
    record_epoch: Option<Epoch>,
    floor_epoch: Option<Epoch>,
    incoming: Epoch,
    authority_changed: bool,
    reactivating: bool,
) -> Result<(), PlacementError> {
    validate_floor(record_epoch, floor_epoch)?;
    let Some(current) = record_epoch else {
        let floor = floor_epoch.unwrap_or(Epoch(0));
        if floor.0 == u64::MAX {
            return Err(PlacementError::EpochExhausted);
        }
        return if incoming > floor {
            Ok(())
        } else {
            Err(PlacementError::EpochRegression {
                current: floor,
                incoming,
            })
        };
    };
    let floor = floor_epoch.unwrap_or(current);
    let base = current.max(floor);
    let must_advance = authority_changed || reactivating || floor > current;
    if must_advance {
        if base.0 == u64::MAX {
            return Err(PlacementError::EpochExhausted);
        }
        if incoming > base {
            return Ok(());
        }
        if incoming == current && authority_changed {
            return Err(PlacementError::EpochAuthorityConflict { epoch: incoming });
        }
        if incoming == current && reactivating {
            return Err(PlacementError::EpochReactivation { epoch: incoming });
        }
        return Err(PlacementError::EpochRegression {
            current: base,
            incoming,
        });
    }
    if incoming < current {
        return Err(PlacementError::EpochRegression { current, incoming });
    }
    if incoming > current {
        return Err(PlacementError::EpochMismatch {
            expected: current,
            incoming,
        });
    }
    Ok(())
}

fn validate_floor(
    record_epoch: Option<Epoch>,
    floor_epoch: Option<Epoch>,
) -> Result<(), PlacementError> {
    if let (Some(record), Some(floor)) = (record_epoch, floor_epoch)
        && floor < record
    {
        return Err(PlacementError::EpochFloorCorrupt { floor, record });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SingletonPlacementRecord {
    pub service_kind: ServiceKind,
    pub singleton_kind: ActorKind,
    pub scope: String,
    pub owner: InstanceId,
    pub epoch: Epoch,
    pub lease_id: LeaseId,
    pub state: PlacementState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoordinatorLeadership {
    pub candidate_id: InstanceId,
    pub lease_id: LeaseId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlacementWatchEvent {
    InstanceUpdated {
        record: InstanceRecord,
    },
    ActorUpdated {
        key: ActorPlacementKey,
        version: PlacementVersion,
        record: ActorPlacementRecord,
    },
    VirtualShardUpdated {
        key: VirtualShardPlacementKey,
        version: PlacementVersion,
        record: VirtualShardPlacementRecord,
    },
    SingletonUpdated {
        key: SingletonKey,
        version: PlacementVersion,
        record: SingletonPlacementRecord,
    },
}

/// Why a placement record could not be proven against its durable epoch floor.
///
/// Ownership views treat every variant as terminal. A caller must fence its
/// local gate before attempting a no-gap resynchronization; it must never
/// substitute a latest-value read for the requested revision.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OwnershipProofError {
    #[error("ownership proof for {key:?} at {observed_revision:?} has no durable epoch floor")]
    MissingFloor {
        key: PlacementEpochKey,
        observed_revision: PlacementRevision,
    },
    #[error(
        "ownership proof for {key:?} at {observed_revision:?} found leased epoch floor {lease_id:?}"
    )]
    LeasedFloor {
        key: PlacementEpochKey,
        observed_revision: PlacementRevision,
        lease_id: LeaseId,
    },
    #[error("ownership proof for {expected:?} used an epoch floor belonging to {actual:?}")]
    WrongFloorKey {
        expected: Box<PlacementEpochKey>,
        actual: Box<PlacementEpochKey>,
    },
    #[error(
        "ownership proof for {key:?} observed record {record:?} or floor {floor:?} after {observed_revision:?}"
    )]
    ObservationRevisionMismatch {
        key: PlacementEpochKey,
        observed_revision: PlacementRevision,
        record: PlacementVersion,
        floor: PlacementVersion,
    },
    #[error("ownership proof for {key:?} has floor epoch {floor:?} behind record epoch {record:?}")]
    EpochFloorBehind {
        key: PlacementEpochKey,
        floor: Epoch,
        record: Epoch,
    },
    #[error(
        "ownership proof for {key:?} has unproven record token {record:?} and floor token {floor:?}"
    )]
    LineageUnproven {
        key: PlacementEpochKey,
        record: PlacementVersion,
        floor: PlacementVersion,
    },
    #[error("ownership proof is bound to a different placement record for {key:?}")]
    RecordBindingMismatch { key: PlacementEpochKey },
    #[error("ownership proof for {key:?} was used in the wrong snapshot/watch context")]
    ContextMismatch { key: PlacementEpochKey },
    #[error(
        "ownership delete for {key:?} at {observed_revision:?} modified its epoch floor in the delete revision"
    )]
    FloorModifiedByDelete {
        key: PlacementEpochKey,
        observed_revision: PlacementRevision,
    },
    #[error("ownership delete for {key:?} did not match the previously proven record")]
    DeletePreviousMismatch { key: PlacementEpochKey },
    #[error(
        "ownership proof read at {requested_revision:?} was compacted or otherwise unavailable: {message}"
    )]
    RevisionUnavailable {
        requested_revision: PlacementRevision,
        message: String,
    },
    #[error("ownership proof for {key:?} is malformed: {message}")]
    MalformedFloor {
        key: PlacementEpochKey,
        message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OwnershipProofContext {
    Snapshot,
    Upsert,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OwnershipRecordBinding {
    Actor(ActorPlacementRecord),
    VirtualShard(VirtualShardPlacementRecord),
    Singleton(SingletonPlacementRecord),
}

impl OwnershipRecordBinding {
    pub(crate) fn epoch_key(&self) -> PlacementEpochKey {
        match self {
            Self::Actor(record) => PlacementEpochKey::Actor(ActorPlacementKey {
                service_kind: record.service_kind.clone(),
                actor_kind: record.actor_kind.clone(),
                actor_id: record.actor_id.clone(),
            }),
            Self::VirtualShard(record) => {
                PlacementEpochKey::VirtualShard(VirtualShardPlacementKey {
                    service_kind: record.service_kind.clone(),
                    actor_kind: record.actor_kind.clone(),
                    shard_id: record.shard_id,
                })
            }
            Self::Singleton(record) => PlacementEpochKey::Singleton(SingletonKey {
                service_kind: record.service_kind.clone(),
                singleton_kind: record.singleton_kind.clone(),
                scope: record.scope.clone(),
            }),
        }
    }

    pub(crate) const fn epoch(&self) -> Epoch {
        match self {
            Self::Actor(record) => record.epoch,
            Self::VirtualShard(record) => record.epoch,
            Self::Singleton(record) => record.epoch,
        }
    }
}

/// Opaque evidence that one complete placement record was checked against its
/// durable, non-leased epoch floor at an exact ownership-view revision.
///
/// The complete record is part of the private binding so a proof cannot be
/// cloned onto a different owner, lease, state, or epoch. Only placement-store
/// backends can construct proofs; public consumers can carry and compare them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipEpochFloorProof {
    context: OwnershipProofContext,
    observed_revision: PlacementRevision,
    record_version: PlacementVersion,
    binding: OwnershipRecordBinding,
    floor_version: PlacementVersion,
    floor: EpochFloorRecord,
}

impl OwnershipEpochFloorProof {
    pub(crate) fn new(
        context: OwnershipProofContext,
        observed_revision: PlacementRevision,
        record_version: PlacementVersion,
        binding: OwnershipRecordBinding,
        floor_version: PlacementVersion,
        floor: EpochFloorRecord,
        floor_lease: Option<LeaseId>,
    ) -> Result<Self, OwnershipProofError> {
        let key = binding.epoch_key();
        if let Some(lease_id) = floor_lease {
            return Err(OwnershipProofError::LeasedFloor {
                key,
                observed_revision,
                lease_id,
            });
        }
        if floor.key != key {
            return Err(OwnershipProofError::WrongFloorKey {
                expected: Box::new(key),
                actual: Box::new(floor.key),
            });
        }
        if record_version.modification_revision() > observed_revision.0
            || floor_version.modification_revision() > observed_revision.0
        {
            return Err(OwnershipProofError::ObservationRevisionMismatch {
                key,
                observed_revision,
                record: record_version,
                floor: floor_version,
            });
        }
        match context {
            OwnershipProofContext::Snapshot => {}
            OwnershipProofContext::Upsert => {
                if record_version.modification_revision() != observed_revision.0 {
                    return Err(OwnershipProofError::ObservationRevisionMismatch {
                        key,
                        observed_revision,
                        record: record_version,
                        floor: floor_version,
                    });
                }
            }
            OwnershipProofContext::Delete => {
                if record_version.modification_revision() >= observed_revision.0 {
                    return Err(OwnershipProofError::ObservationRevisionMismatch {
                        key,
                        observed_revision,
                        record: record_version,
                        floor: floor_version,
                    });
                }
                if floor_version.modification_revision() == observed_revision.0 {
                    return Err(OwnershipProofError::FloorModifiedByDelete {
                        key,
                        observed_revision,
                    });
                }
            }
        }
        match validate_epoch_floor_lineage(
            Some((record_version, binding.epoch())),
            Some((floor_version, floor.epoch)),
        ) {
            Ok(()) => Ok(Self {
                context,
                observed_revision,
                record_version,
                binding,
                floor_version,
                floor,
            }),
            Err(PlacementError::EpochFloorCorrupt { floor, record }) => {
                Err(OwnershipProofError::EpochFloorBehind { key, floor, record })
            }
            Err(PlacementError::EpochFloorUnproven { record, floor }) => {
                Err(OwnershipProofError::LineageUnproven {
                    key,
                    record,
                    floor: floor.expect("the proof constructor supplied a floor"),
                })
            }
            Err(error) => unreachable!("unexpected epoch-lineage error: {error}"),
        }
    }

    pub const fn observed_revision(&self) -> PlacementRevision {
        self.observed_revision
    }

    pub const fn record_revision(&self) -> PlacementRevision {
        PlacementRevision(self.record_version.modification_revision())
    }

    pub(crate) fn validate_snapshot(
        &self,
        snapshot_revision: PlacementRevision,
        record_revision: PlacementRevision,
        binding: &OwnershipRecordBinding,
    ) -> Result<(), OwnershipProofError> {
        self.validate_binding(OwnershipProofContext::Snapshot, snapshot_revision, binding)?;
        if self.record_revision() != record_revision {
            return Err(OwnershipProofError::RecordBindingMismatch {
                key: binding.epoch_key(),
            });
        }
        Ok(())
    }

    pub(crate) fn validate_upsert(
        &self,
        revision: PlacementRevision,
        binding: &OwnershipRecordBinding,
    ) -> Result<(), OwnershipProofError> {
        self.validate_binding(OwnershipProofContext::Upsert, revision, binding)
    }

    pub(crate) fn validate_delete(
        &self,
        revision: PlacementRevision,
        binding: &OwnershipRecordBinding,
    ) -> Result<(), OwnershipProofError> {
        self.validate_binding(OwnershipProofContext::Delete, revision, binding)
    }

    fn validate_binding(
        &self,
        context: OwnershipProofContext,
        revision: PlacementRevision,
        binding: &OwnershipRecordBinding,
    ) -> Result<(), OwnershipProofError> {
        let key = binding.epoch_key();
        if self.context != context || self.observed_revision != revision {
            return Err(OwnershipProofError::ContextMismatch { key });
        }
        if &self.binding != binding {
            return Err(OwnershipProofError::RecordBindingMismatch { key });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnershipViewRecord {
    Actor {
        revision: PlacementRevision,
        record: ActorPlacementRecord,
        proof: OwnershipEpochFloorProof,
    },
    VirtualShard {
        revision: PlacementRevision,
        record: VirtualShardPlacementRecord,
        proof: OwnershipEpochFloorProof,
    },
    Singleton {
        revision: PlacementRevision,
        record: SingletonPlacementRecord,
        proof: OwnershipEpochFloorProof,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipViewSnapshot {
    pub revision: PlacementRevision,
    /// The exact requested instance record, when it belongs to the requested service.
    pub local_instance: Option<InstanceRecord>,
    /// All bounded actor, virtual-shard, and singleton records for the requested service.
    ///
    /// Consumers may filter this service-wide set for local authorization, but retaining
    /// remote owners lets them preserve ownership epoch floors across resynchronization.
    pub records: Vec<OwnershipViewRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnershipWatchEvent {
    InstanceUpserted {
        record: InstanceRecord,
    },
    InstanceDeleted {
        record: InstanceRecord,
    },
    ActorUpserted {
        key: ActorPlacementKey,
        record: ActorPlacementRecord,
        proof: OwnershipEpochFloorProof,
    },
    ActorDeleted {
        key: ActorPlacementKey,
        previous_record: ActorPlacementRecord,
        proof: OwnershipEpochFloorProof,
    },
    VirtualShardUpserted {
        key: VirtualShardPlacementKey,
        record: VirtualShardPlacementRecord,
        proof: OwnershipEpochFloorProof,
    },
    VirtualShardDeleted {
        key: VirtualShardPlacementKey,
        previous_record: VirtualShardPlacementRecord,
        proof: OwnershipEpochFloorProof,
    },
    SingletonUpserted {
        key: SingletonKey,
        record: SingletonPlacementRecord,
        proof: OwnershipEpochFloorProof,
    },
    SingletonDeleted {
        key: SingletonKey,
        previous_record: SingletonPlacementRecord,
        proof: OwnershipEpochFloorProof,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipWatchBatch {
    pub revision: PlacementRevision,
    pub events: Vec<OwnershipWatchEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OwnershipWatchError {
    #[error("ownership watch lagged and skipped {skipped} batches")]
    Lagged { skipped: u64 },
    #[error("ownership watch closed")]
    Closed,
    #[error("ownership watch backend error: {message}")]
    Backend { message: String },
    #[error(
        "ownership watch requested revision {requested_revision:?}, compacted through {compact_revision:?}"
    )]
    Compacted {
        requested_revision: PlacementRevision,
        compact_revision: PlacementRevision,
    },
    #[error("ownership watch canceled: {reason}")]
    Canceled { reason: String },
    #[error("ownership watch protocol error: {message}")]
    Protocol { message: String },
    #[error("ownership watch final live set exceeded its {max_entries} entry limit")]
    CapacityExceeded { max_entries: usize },
    #[error("ownership watch revision exceeded its {max_events} selected-event limit")]
    BatchCapacityExceeded { max_events: usize },
    #[error("ownership watch startup exceeded its {max_updates} buffered-update limit")]
    StartupBacklogExceeded { max_updates: usize },
    #[error("ownership watch epoch-floor proof failed: {error}")]
    Proof { error: OwnershipProofError },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnershipWatchUpdate {
    Batch(OwnershipWatchBatch),
    Progress { revision: PlacementRevision },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OwnershipWatchMessage {
    Update(OwnershipWatchUpdate),
    Failed(OwnershipWatchError),
}

#[derive(Debug)]
pub struct OwnershipWatch {
    rx: broadcast::Receiver<OwnershipWatchMessage>,
    abort_handle: Option<tokio::task::AbortHandle>,
}

impl OwnershipWatch {
    pub async fn next(&mut self) -> Result<OwnershipWatchBatch, OwnershipWatchError> {
        loop {
            match self.next_update().await? {
                OwnershipWatchUpdate::Batch(batch) => return Ok(batch),
                OwnershipWatchUpdate::Progress { .. } => {}
            }
        }
    }

    pub async fn next_update(&mut self) -> Result<OwnershipWatchUpdate, OwnershipWatchError> {
        match self.rx.recv().await {
            Ok(OwnershipWatchMessage::Update(update)) => Ok(update),
            Ok(OwnershipWatchMessage::Failed(error)) => Err(error),
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                Err(OwnershipWatchError::Lagged { skipped })
            }
            Err(broadcast::error::RecvError::Closed) => Err(OwnershipWatchError::Closed),
        }
    }

    pub(crate) fn new(rx: broadcast::Receiver<OwnershipWatchMessage>) -> Self {
        Self {
            rx,
            abort_handle: None,
        }
    }

    pub(crate) fn new_cancellable(
        rx: broadcast::Receiver<OwnershipWatchMessage>,
        abort_handle: tokio::task::AbortHandle,
    ) -> Self {
        Self {
            rx,
            abort_handle: Some(abort_handle),
        }
    }
}

impl Drop for OwnershipWatch {
    fn drop(&mut self) {
        if let Some(abort_handle) = self.abort_handle.take() {
            abort_handle.abort();
        }
    }
}

#[derive(Debug)]
pub struct OwnershipView {
    /// Coherent state at `snapshot.revision`.
    pub snapshot: OwnershipViewSnapshot,
    /// Mutations committed after the snapshot revision.
    ///
    /// Batch revisions strictly increase for this view but may contain gaps
    /// caused by unrelated store mutations. Lag or closure invalidates the
    /// view and requires callers to fence before opening a new one.
    pub watch: OwnershipWatch,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OwnershipViewError {
    #[error("this placement store does not support coherent ownership views")]
    Unsupported,
    #[error("ownership view exceeded its {max_entries} entry limit")]
    CapacityExceeded { max_entries: usize },
    #[error("ownership view backend error: {message}")]
    Backend { message: String },
    #[error("ownership view protocol error: {message}")]
    Protocol { message: String },
    #[error("ownership view epoch-floor proof failed: {error}")]
    Proof { error: OwnershipProofError },
    #[error("ownership view could not start its watch: {error}")]
    WatchStart { error: OwnershipWatchError },
}

#[derive(Debug)]
pub struct PlacementWatch {
    rx: broadcast::Receiver<PlacementWatchEvent>,
}

impl PlacementWatch {
    pub async fn next(&mut self) -> Result<PlacementWatchEvent, PlacementError> {
        loop {
            match self.rx.recv().await {
                Ok(event) => return Ok(event),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(PlacementError::PlacementWatchClosed);
                }
            }
        }
    }

    pub(crate) fn new(rx: broadcast::Receiver<PlacementWatchEvent>) -> Self {
        Self { rx }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlacementPrefix(String);

impl PlacementPrefix {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[async_trait]
pub trait PlacementStore: Clone + Send + Sync + 'static {
    async fn grant_instance_lease(&self) -> Result<LeaseId, PlacementError>;
    async fn keepalive_instance_lease(&self, lease_id: LeaseId) -> Result<(), PlacementError>;
    async fn campaign_coordinator_leader(
        &self,
        candidate_id: InstanceId,
    ) -> Result<Option<CoordinatorLeadership>, PlacementError>;
    async fn keepalive_coordinator_leader(
        &self,
        leadership: &CoordinatorLeadership,
    ) -> Result<(), PlacementError>;
    async fn resign_coordinator_leader(
        &self,
        leadership: &CoordinatorLeadership,
    ) -> Result<(), PlacementError>;
    async fn upsert_instance(&self, record: InstanceRecord) -> Result<(), PlacementError>;
    async fn get_instance(
        &self,
        instance_id: &InstanceId,
    ) -> Result<Option<InstanceRecord>, PlacementError>;
    async fn list_instances(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<InstanceRecord>, PlacementError>;
    async fn list_all_instances(&self) -> Result<Vec<InstanceRecord>, PlacementError>;
    async fn get_actor(
        &self,
        key: &ActorPlacementKey,
    ) -> Result<Option<(PlacementVersion, ActorPlacementRecord)>, PlacementError>;
    async fn list_actors(
        &self,
    ) -> Result<Vec<(PlacementVersion, ActorPlacementRecord)>, PlacementError>;
    async fn reserve_actor_epoch(
        &self,
        _key: ActorPlacementKey,
        _expected: Option<PlacementVersion>,
        _activation_lock: Option<LeaseId>,
    ) -> Result<PlacementEpochReservation, PlacementError> {
        Err(PlacementError::EpochReservationsUnsupported)
    }
    async fn commit_actor_epoch(
        &self,
        _reservation: PlacementEpochReservation,
        _value: ActorPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        Err(PlacementError::EpochReservationsUnsupported)
    }
    async fn compare_and_put_actor(
        &self,
        key: ActorPlacementKey,
        expected: Option<PlacementVersion>,
        value: ActorPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError>;
    async fn get_virtual_shard(
        &self,
        key: &VirtualShardPlacementKey,
    ) -> Result<Option<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError>;
    async fn list_virtual_shards(
        &self,
        service_kind: &ServiceKind,
        actor_kind: &ActorKind,
    ) -> Result<Vec<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError>;
    async fn list_virtual_shards_for_service(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError>;
    async fn reserve_virtual_shard_epoch(
        &self,
        _key: VirtualShardPlacementKey,
        _expected: Option<PlacementVersion>,
    ) -> Result<PlacementEpochReservation, PlacementError> {
        Err(PlacementError::EpochReservationsUnsupported)
    }
    async fn commit_virtual_shard_epoch(
        &self,
        _reservation: PlacementEpochReservation,
        _value: VirtualShardPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        Err(PlacementError::EpochReservationsUnsupported)
    }
    async fn compare_and_put_virtual_shard(
        &self,
        key: VirtualShardPlacementKey,
        expected: Option<PlacementVersion>,
        value: VirtualShardPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError>;
    async fn get_singleton(
        &self,
        key: &SingletonKey,
    ) -> Result<Option<(PlacementVersion, SingletonPlacementRecord)>, PlacementError>;
    async fn list_singletons(
        &self,
    ) -> Result<Vec<(PlacementVersion, SingletonPlacementRecord)>, PlacementError>;
    async fn reserve_singleton_epoch(
        &self,
        _key: SingletonKey,
        _expected: Option<PlacementVersion>,
        _singleton_lock: Option<LeaseId>,
    ) -> Result<PlacementEpochReservation, PlacementError> {
        Err(PlacementError::EpochReservationsUnsupported)
    }
    async fn commit_singleton_epoch(
        &self,
        _reservation: PlacementEpochReservation,
        _value: SingletonPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        Err(PlacementError::EpochReservationsUnsupported)
    }
    async fn compare_and_put_singleton(
        &self,
        key: SingletonKey,
        expected: Option<PlacementVersion>,
        value: SingletonPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError>;
    async fn acquire_singleton_lock(&self, key: SingletonKey) -> Result<LeaseId, PlacementError>;
    async fn validate_singleton_lock(
        &self,
        key: &SingletonKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError>;
    async fn release_singleton_lock(
        &self,
        key: &SingletonKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError>;
    async fn acquire_activation_lock(
        &self,
        key: ActorPlacementKey,
    ) -> Result<LeaseId, PlacementError>;
    async fn validate_activation_lock(
        &self,
        key: &ActorPlacementKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError>;
    async fn release_activation_lock(
        &self,
        key: &ActorPlacementKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError>;
    /// Atomically snapshots ownership state and subscribes to later changes.
    ///
    /// `max_entries` bounds all selected-service records scanned by the backend
    /// before any consumer-side owner filtering. Exceeding it must fail closed.
    async fn open_ownership_view(
        &self,
        _service_kind: &ServiceKind,
        _instance_id: &InstanceId,
        _max_entries: NonZeroUsize,
    ) -> Result<OwnershipView, OwnershipViewError> {
        Err(OwnershipViewError::Unsupported)
    }
    async fn watch(&self, prefix: PlacementPrefix) -> Result<PlacementWatch, PlacementError>;
    fn prefix(&self) -> &PlacementPrefix;
}

pub mod etcd;
pub mod memory;

#[cfg(test)]
mod ownership_watch_drop_tests {
    use std::future::pending;
    use std::time::Duration;

    use tokio::sync::broadcast;

    use super::{OwnershipWatch, OwnershipWatchMessage};

    #[tokio::test]
    async fn dropping_cancellable_ownership_watch_aborts_its_bridge_task() {
        let (_tx, rx) = broadcast::channel::<OwnershipWatchMessage>(1);
        let task = tokio::spawn(pending::<()>());
        let watch = OwnershipWatch::new_cancellable(rx, task.abort_handle());

        drop(watch);

        let error = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("cancellable ownership bridge did not stop")
            .expect_err("cancellable ownership bridge completed instead of being aborted");
        assert!(error.is_cancelled());
    }
}
