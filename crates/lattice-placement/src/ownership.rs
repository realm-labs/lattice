use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use lattice_core::actor_ref::Epoch;
use lattice_core::id::{ActorId, RouteKey};
use lattice_core::instance::{InstanceId, InstanceIncarnation};
use lattice_core::kind::{ActorKind, ServiceKind};

use crate::registry::{InstanceRecord, InstanceState};
use crate::sharding::VirtualShardMapper;
use crate::storage::{
    ActorPlacementKey, ActorPlacementRecord, LeaseId, OwnershipEpochFloorProof,
    OwnershipProofError, OwnershipRecordBinding, OwnershipViewRecord, OwnershipViewSnapshot,
    OwnershipWatchBatch, OwnershipWatchEvent, PlacementRevision, PlacementState, SingletonKey,
    SingletonPlacementRecord, VirtualShardPlacementKey, VirtualShardPlacementRecord,
};

const DEFAULT_MAX_OWNERSHIP_ENTRIES: usize = 65_536;
const MAX_OWNERSHIP_BATCH_EVENTS: usize = 65_536;

/// Monotonic revision from one authoritative placement snapshot/watch stream.
///
/// This must not be populated from a per-key version that can reset after a
/// delete and recreate. A gate stays fenced until its watcher can supply a
/// no-gap revision order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OwnershipRevision(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalOwnershipSnapshotConfig {
    max_entries: NonZeroUsize,
}

impl LocalOwnershipSnapshotConfig {
    pub fn try_new(max_entries: usize) -> Result<Self, OwnershipSnapshotError> {
        let max_entries =
            NonZeroUsize::new(max_entries).ok_or(OwnershipSnapshotError::InvalidCapacity)?;
        Ok(Self { max_entries })
    }

    pub fn max_entries(self) -> NonZeroUsize {
        self.max_entries
    }
}

impl Default for LocalOwnershipSnapshotConfig {
    fn default() -> Self {
        Self {
            max_entries: NonZeroUsize::new(DEFAULT_MAX_OWNERSHIP_ENTRIES)
                .expect("the default ownership capacity is nonzero"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OwnershipSnapshotError {
    #[error("local ownership snapshot capacity must be greater than zero")]
    InvalidCapacity,
    #[error("local ownership snapshot expected service {expected}, got {actual}")]
    ServiceMismatch {
        expected: ServiceKind,
        actual: ServiceKind,
    },
    #[error("local ownership snapshot expected instance {expected}, got {actual}")]
    InstanceMismatch {
        expected: InstanceId,
        actual: InstanceId,
    },
    #[error("local ownership snapshot expected incarnation {expected}, got {actual}")]
    InstanceIncarnationMismatch {
        expected: InstanceIncarnation,
        actual: InstanceIncarnation,
    },
    #[error("local ownership snapshot has no active instance lease")]
    MissingInstanceLease,
    #[error("coherent ownership view omitted the local instance record")]
    MissingLocalInstanceRecord,
    #[error("local ownership snapshot lease changed from {expected:?} to {actual:?}")]
    StaleLease { expected: LeaseId, actual: LeaseId },
    #[error(
        "local ownership resync changed generation from {expected_generation} to {actual_generation}"
    )]
    StaleResync {
        expected_generation: u64,
        actual_generation: u64,
    },
    #[error("local ownership revision moved backwards from {current:?} to {incoming:?}")]
    StaleRevision {
        current: OwnershipRevision,
        incoming: OwnershipRevision,
    },
    #[error(
        "local ownership record revision {record:?} is ahead of snapshot revision {snapshot:?}"
    )]
    RecordAheadOfSnapshot {
        record: OwnershipRevision,
        snapshot: OwnershipRevision,
    },
    #[error("local ownership snapshot exceeded its {max_entries} entry limit")]
    CapacityExceeded { max_entries: usize },
    #[error("ownership watch emitted an empty batch")]
    EmptyWatchBatch,
    #[error("ownership watch batch exceeded its {max_events} event limit")]
    WatchBatchCapacityExceeded { max_events: usize },
    #[error("ownership watch batch contains multiple instance events")]
    DuplicateInstanceEvent,
    #[error("ownership watch batch contains multiple events for {key:?}")]
    DuplicatePlacementEvent { key: Box<OwnershipKey> },
    #[error("ownership watch event key {event:?} does not match the record-derived key {record:?}")]
    PlacementKeyMismatch {
        event: Box<OwnershipKey>,
        record: Box<OwnershipKey>,
    },
    #[error("ownership epoch for {key:?} moved backwards from {current:?} to {incoming:?}")]
    EpochRegression {
        key: Box<OwnershipKey>,
        current: Epoch,
        incoming: Epoch,
    },
    #[error("ownership authority for {key:?} changed without advancing epoch {epoch:?}")]
    EpochAuthorityConflict {
        key: Box<OwnershipKey>,
        epoch: Epoch,
    },
    #[error("ownership for {key:?} cannot reactivate at its removed epoch {epoch:?}")]
    EpochReactivation {
        key: Box<OwnershipKey>,
        epoch: Epoch,
    },
    #[error(
        "ownership record for {key:?} at {incoming:?} did not follow its absent observation at {absence:?}"
    )]
    ResurrectionRevisionNotAdvanced {
        key: Box<OwnershipKey>,
        absence: OwnershipRevision,
        incoming: OwnershipRevision,
    },
    #[error("local ownership epoch-floor proof failed: {error}")]
    Proof { error: OwnershipProofError },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnershipPlacement {
    Explicit,
    VirtualShard { mapper: VirtualShardMapper },
    Singleton,
}

#[derive(Debug, Clone, Copy)]
pub struct OwnershipRequest<'a> {
    pub expected_service: &'a ServiceKind,
    pub expected_actor_kind: &'a ActorKind,
    pub route_actor_kind: &'a ActorKind,
    pub route_key: &'a RouteKey,
    pub route_epoch: Option<Epoch>,
    pub placement: &'a OwnershipPlacement,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OwnershipKey {
    Explicit(ActorPlacementKey),
    VirtualShard(VirtualShardPlacementKey),
    Singleton(SingletonKey),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnershipRejectionKind {
    NotOwner,
    Fenced,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnershipFenceReason {
    Initializing,
    Resyncing,
    WatchLost,
    LeaseLost,
    CapacityExceeded,
    Manual,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnershipRejectionReason {
    ServiceMismatch {
        local: ServiceKind,
        expected: ServiceKind,
    },
    ActorKindMismatch {
        expected: ActorKind,
        actual: ActorKind,
    },
    InvalidSingletonRoute,
    SnapshotUnavailable {
        reason: OwnershipFenceReason,
    },
    LocalInstanceMissing,
    InstanceNotReady {
        state: InstanceState,
    },
    InstanceLeaseInvalid {
        lease_id: LeaseId,
    },
    PlacementMissing,
    OwnerMismatch,
    OwnerIncarnationMismatch,
    PlacementNotRunning {
        state: PlacementState,
    },
    OwnerLeaseInvalid {
        lease_id: LeaseId,
    },
    MissingEpoch,
    EpochMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipRejection {
    pub kind: OwnershipRejectionKind,
    pub reason: OwnershipRejectionReason,
    pub key: Option<OwnershipKey>,
    pub requested_epoch: Option<Epoch>,
    pub current_epoch: Option<Epoch>,
    pub current_owner: Option<InstanceId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipGrant {
    key: OwnershipKey,
    epoch: Epoch,
    lease_id: LeaseId,
    generation: u64,
}

impl OwnershipGrant {
    pub fn key(&self) -> &OwnershipKey {
        &self.key
    }

    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub fn lease_id(&self) -> LeaseId {
        self.lease_id
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
}

pub trait OwnershipGate: Send + Sync + 'static {
    fn authorize(
        &self,
        request: OwnershipRequest<'_>,
    ) -> Result<OwnershipGrant, Box<OwnershipRejection>>;

    fn is_current(&self, grant: &OwnershipGrant) -> bool;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnershipSnapshotRecord {
    Actor {
        version: OwnershipRevision,
        record: ActorPlacementRecord,
        proof: OwnershipEpochFloorProof,
    },
    VirtualShard {
        version: OwnershipRevision,
        record: VirtualShardPlacementRecord,
        proof: OwnershipEpochFloorProof,
    },
    Singleton {
        version: OwnershipRevision,
        record: SingletonPlacementRecord,
        proof: OwnershipEpochFloorProof,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnershipResyncToken {
    lease_id: LeaseId,
    generation: u64,
}

#[derive(Debug, Clone)]
pub struct LocalOwnershipSnapshot {
    inner: Arc<LocalOwnershipInner>,
}

#[derive(Debug, Clone)]
pub struct LocalOwnershipGate {
    inner: Arc<LocalOwnershipInner>,
}

#[derive(Debug)]
struct LocalOwnershipInner {
    service_kind: ServiceKind,
    instance_id: InstanceId,
    instance_incarnation: InstanceIncarnation,
    max_entries: NonZeroUsize,
    state: RwLock<LocalOwnershipState>,
}

#[derive(Debug, Clone)]
struct LocalOwnershipState {
    generation: u64,
    revision: Option<OwnershipRevision>,
    availability: SnapshotAvailability,
    instance: Option<LocalInstanceAuthority>,
    valid_owner_leases: HashSet<LeaseId>,
    actors: HashMap<ActorPlacementKey, VersionedRecord<ActorPlacementRecord>>,
    virtual_shards: HashMap<VirtualShardPlacementKey, VersionedRecord<VirtualShardPlacementRecord>>,
    singletons: HashMap<SingletonKey, VersionedRecord<SingletonPlacementRecord>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotAvailability {
    Ready,
    Unavailable(OwnershipFenceReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalInstanceAuthority {
    lease_id: LeaseId,
    incarnation: InstanceIncarnation,
    state: InstanceState,
}

#[derive(Debug, Clone)]
struct VersionedRecord<T> {
    version: OwnershipRevision,
    authority: OwnershipAuthorityFloor,
    observation: StoreObservation<T>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AbsenceEvidence {
    ExactDelete,
    CoherentSnapshot,
}

#[derive(Debug, Clone)]
enum StoreObservation<T> {
    PresentLocal(T),
    PresentRemote(T),
    LifecycleFenced(T),
    Absent {
        last_record: T,
        evidence: AbsenceEvidence,
    },
}

impl<T> StoreObservation<T> {
    fn present(record: T, is_local: bool) -> Self {
        if is_local {
            Self::PresentLocal(record)
        } else {
            Self::PresentRemote(record)
        }
    }

    fn local_record(&self) -> Option<&T> {
        match self {
            Self::PresentLocal(record) => Some(record),
            Self::PresentRemote(_) | Self::LifecycleFenced(_) | Self::Absent { .. } => None,
        }
    }

    fn store_present_record(&self) -> Option<&T> {
        match self {
            Self::PresentLocal(record)
            | Self::PresentRemote(record)
            | Self::LifecycleFenced(record) => Some(record),
            Self::Absent { .. } => None,
        }
    }

    fn last_record(&self) -> &T {
        match self {
            Self::PresentLocal(record)
            | Self::PresentRemote(record)
            | Self::LifecycleFenced(record)
            | Self::Absent {
                last_record: record,
                ..
            } => record,
        }
    }

    fn is_absent(&self) -> bool {
        matches!(self, Self::Absent { .. })
    }

    fn is_lifecycle_fenced(&self) -> bool {
        matches!(self, Self::LifecycleFenced(_))
    }
}

impl<T: Clone> StoreObservation<T> {
    fn fence_local_for_lifecycle(&mut self) -> bool {
        let Self::PresentLocal(record) = self else {
            return false;
        };
        *self = Self::LifecycleFenced(record.clone());
        true
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OwnershipAuthorityFloor {
    epoch: Epoch,
    owner: InstanceId,
    owner_incarnation: Option<InstanceIncarnation>,
    lease_id: Option<LeaseId>,
}

impl OwnershipAuthorityFloor {
    fn actor(record: &ActorPlacementRecord) -> Self {
        Self {
            epoch: record.epoch,
            owner: record.owner.clone(),
            owner_incarnation: None,
            lease_id: Some(record.lease_id),
        }
    }

    fn virtual_shard(record: &VirtualShardPlacementRecord) -> Self {
        Self {
            epoch: record.epoch,
            owner: record.owner.clone(),
            owner_incarnation: None,
            lease_id: None,
        }
    }

    fn singleton(record: &SingletonPlacementRecord) -> Self {
        Self {
            epoch: record.epoch,
            owner: record.owner.clone(),
            owner_incarnation: Some(record.owner_incarnation.clone()),
            lease_id: Some(record.lease_id),
        }
    }
}

impl LocalOwnershipSnapshot {
    pub fn new(
        service_kind: ServiceKind,
        instance_id: InstanceId,
        instance_incarnation: InstanceIncarnation,
    ) -> Self {
        Self::with_config(
            service_kind,
            instance_id,
            instance_incarnation,
            LocalOwnershipSnapshotConfig::default(),
        )
    }

    pub fn with_config(
        service_kind: ServiceKind,
        instance_id: InstanceId,
        instance_incarnation: InstanceIncarnation,
        config: LocalOwnershipSnapshotConfig,
    ) -> Self {
        Self {
            inner: Arc::new(LocalOwnershipInner {
                service_kind,
                instance_id,
                instance_incarnation,
                max_entries: config.max_entries,
                state: RwLock::new(LocalOwnershipState {
                    generation: 0,
                    revision: None,
                    availability: SnapshotAvailability::Unavailable(
                        OwnershipFenceReason::Initializing,
                    ),
                    instance: None,
                    valid_owner_leases: HashSet::new(),
                    actors: HashMap::new(),
                    virtual_shards: HashMap::new(),
                    singletons: HashMap::new(),
                }),
            }),
        }
    }

    pub fn gate(&self) -> LocalOwnershipGate {
        LocalOwnershipGate {
            inner: self.inner.clone(),
        }
    }

    /// Installs one coherent backend view before its no-gap watch is consumed.
    ///
    /// The caller must only set `lease_valid` from local keepalive authority;
    /// presence in the registry snapshot alone is not liveness evidence.
    pub fn replace_from_view_snapshot(
        &self,
        snapshot: OwnershipViewSnapshot,
        lease_valid: bool,
    ) -> Result<(), OwnershipSnapshotError> {
        let instance = snapshot
            .local_instance
            .ok_or(OwnershipSnapshotError::MissingLocalInstanceRecord)?;
        let lease_id = instance.lease_id;
        self.install_local_instance(instance, lease_valid)?;
        let token = self.begin_resync(lease_id)?;
        self.replace_from_resync(
            token,
            OwnershipRevision(snapshot.revision.0),
            snapshot.records.into_iter().map(|record| match record {
                OwnershipViewRecord::Actor {
                    revision,
                    record,
                    proof,
                } => OwnershipSnapshotRecord::Actor {
                    version: OwnershipRevision(revision.0),
                    record,
                    proof,
                },
                OwnershipViewRecord::VirtualShard {
                    revision,
                    record,
                    proof,
                } => OwnershipSnapshotRecord::VirtualShard {
                    version: OwnershipRevision(revision.0),
                    record,
                    proof,
                },
                OwnershipViewRecord::Singleton {
                    revision,
                    record,
                    proof,
                } => OwnershipSnapshotRecord::Singleton {
                    version: OwnershipRevision(revision.0),
                    record,
                    proof,
                },
            }),
        )
    }

    pub fn install_local_instance(
        &self,
        record: InstanceRecord,
        lease_valid: bool,
    ) -> Result<(), OwnershipSnapshotError> {
        self.validate_instance_identity(&record)?;
        let mut state = self.inner.write_state();
        let authority_changed = state.instance.as_ref().is_some_and(|instance| {
            instance.lease_id != record.lease_id || instance.incarnation != record.incarnation
        });
        if authority_changed {
            state.fence_local_placements_for_lifecycle();
            state.valid_owner_leases.clear();
            state.availability =
                SnapshotAvailability::Unavailable(OwnershipFenceReason::Initializing);
        }
        state.instance = Some(LocalInstanceAuthority {
            lease_id: record.lease_id,
            incarnation: record.incarnation,
            state: record.state,
        });
        if lease_valid {
            state.valid_owner_leases.insert(record.lease_id);
        } else {
            state.valid_owner_leases.remove(&record.lease_id);
            if !authority_changed {
                state.availability =
                    SnapshotAvailability::Unavailable(OwnershipFenceReason::LeaseLost);
            }
        }
        state.bump_generation();
        Ok(())
    }

    pub fn update_instance_state(&self, lease_id: LeaseId, state: InstanceState) -> bool {
        let mut snapshot = self.inner.write_state();
        let Some(instance) = snapshot.instance.as_mut() else {
            return false;
        };
        if instance.lease_id != lease_id {
            return false;
        }
        if instance.state != state {
            instance.state = state;
            snapshot.bump_generation();
        }
        true
    }

    pub fn set_owner_lease_valid(
        &self,
        lease_id: LeaseId,
        valid: bool,
    ) -> Result<bool, OwnershipSnapshotError> {
        let mut state = self.inner.write_state();
        let changed = if valid {
            if !state.valid_owner_leases.contains(&lease_id)
                && state.valid_owner_leases.len() >= self.inner.max_entries.get().saturating_add(1)
            {
                state.availability =
                    SnapshotAvailability::Unavailable(OwnershipFenceReason::CapacityExceeded);
                state.bump_generation();
                return Err(self.capacity_error());
            }
            state.valid_owner_leases.insert(lease_id)
        } else {
            state.valid_owner_leases.remove(&lease_id)
        };
        if !valid
            && state
                .instance
                .as_ref()
                .is_some_and(|instance| instance.lease_id == lease_id)
        {
            state.availability = SnapshotAvailability::Unavailable(OwnershipFenceReason::LeaseLost);
        }
        if changed {
            state.bump_generation();
        }
        Ok(changed)
    }

    pub fn begin_resync(
        &self,
        lease_id: LeaseId,
    ) -> Result<OwnershipResyncToken, OwnershipSnapshotError> {
        let mut state = self.inner.write_state();
        let current_lease = state
            .instance
            .as_ref()
            .map(|instance| instance.lease_id)
            .ok_or(OwnershipSnapshotError::MissingInstanceLease)?;
        state.availability = SnapshotAvailability::Unavailable(OwnershipFenceReason::Resyncing);
        state.bump_generation();
        if current_lease != lease_id {
            return Err(OwnershipSnapshotError::StaleLease {
                expected: current_lease,
                actual: lease_id,
            });
        }
        Ok(OwnershipResyncToken {
            lease_id,
            generation: state.generation,
        })
    }

    pub fn fence(&self, reason: OwnershipFenceReason) {
        let mut state = self.inner.write_state();
        state.availability = SnapshotAvailability::Unavailable(reason);
        state.bump_generation();
    }

    pub fn replace_from_resync<I>(
        &self,
        token: OwnershipResyncToken,
        snapshot_revision: OwnershipRevision,
        records: I,
    ) -> Result<(), OwnershipSnapshotError>
    where
        I: IntoIterator<Item = OwnershipSnapshotRecord>,
    {
        let (actors, virtual_shards, singletons) =
            self.collect_local_records(snapshot_revision, records)?;
        let mut state = self.inner.write_state();
        let current_lease = state
            .instance
            .as_ref()
            .map(|instance| instance.lease_id)
            .ok_or(OwnershipSnapshotError::MissingInstanceLease)?;
        if current_lease != token.lease_id {
            state.availability = SnapshotAvailability::Unavailable(OwnershipFenceReason::Resyncing);
            state.bump_generation();
            return Err(OwnershipSnapshotError::StaleLease {
                expected: current_lease,
                actual: token.lease_id,
            });
        }
        if state.generation != token.generation {
            state.availability = SnapshotAvailability::Unavailable(OwnershipFenceReason::Resyncing);
            let actual_generation = state.generation;
            state.bump_generation();
            return Err(OwnershipSnapshotError::StaleResync {
                expected_generation: token.generation,
                actual_generation,
            });
        }
        if let Some(current) = state.revision
            && snapshot_revision < current
        {
            state.availability = SnapshotAvailability::Unavailable(OwnershipFenceReason::Resyncing);
            state.bump_generation();
            return Err(OwnershipSnapshotError::StaleRevision {
                current,
                incoming: snapshot_revision,
            });
        }
        let mut actors = actors;
        let mut virtual_shards = virtual_shards;
        let mut singletons = singletons;
        if let Err(error) =
            merge_resync_observations(&state.actors, &mut actors, snapshot_revision, |key| {
                OwnershipKey::Explicit(key.clone())
            })
            .and_then(|()| {
                merge_resync_observations(
                    &state.virtual_shards,
                    &mut virtual_shards,
                    snapshot_revision,
                    |key| OwnershipKey::VirtualShard(key.clone()),
                )
            })
            .and_then(|()| {
                merge_resync_observations(
                    &state.singletons,
                    &mut singletons,
                    snapshot_revision,
                    |key| OwnershipKey::Singleton(key.clone()),
                )
            })
        {
            state.availability = SnapshotAvailability::Unavailable(OwnershipFenceReason::Resyncing);
            state.bump_generation();
            return Err(error);
        }
        if actors.len() + virtual_shards.len() + singletons.len() > self.inner.max_entries.get() {
            state.availability =
                SnapshotAvailability::Unavailable(OwnershipFenceReason::CapacityExceeded);
            state.bump_generation();
            return Err(self.capacity_error());
        }
        state.actors = actors;
        state.virtual_shards = virtual_shards;
        state.singletons = singletons;
        state.prune_owner_leases(current_lease);
        state.revision = Some(snapshot_revision);
        state.availability = SnapshotAvailability::Ready;
        state.bump_generation();
        Ok(())
    }

    /// Applies one globally ordered watch revision as a single local state change.
    ///
    /// Events for other services and instances are ignored because ownership
    /// watches may cover a prefix shared by many service instances. All relevant
    /// events are validated before a staged state is committed. An invalid or
    /// over-capacity batch leaves the prior records and revision intact and
    /// fences the snapshot.
    pub fn apply_watch_batch(
        &self,
        batch: OwnershipWatchBatch,
    ) -> Result<bool, OwnershipSnapshotError> {
        let incoming = OwnershipRevision(batch.revision.0);
        let mut state = self.inner.write_state();
        if state.revision.is_some_and(|current| current >= incoming) {
            return Ok(false);
        }

        if let Err(error) = self
            .validate_watch_events(batch.revision, &batch.events)
            .and_then(|()| self.validate_watch_epoch_progression(&state, &batch.events))
        {
            Self::fence_rejected_batch(&mut state, &error);
            return Err(error);
        }

        let invalidate_resync = state.availability
            == SnapshotAvailability::Unavailable(OwnershipFenceReason::Resyncing);
        let generation = state.generation;
        let mut staged = state.clone();
        staged.revision = Some(incoming);
        let changed = match self.stage_watch_events(&mut staged, incoming, &batch.events) {
            Ok(changed) => changed,
            Err(error) => {
                Self::fence_rejected_batch(&mut state, &error);
                return Err(error);
            }
        };
        staged.generation = if changed || invalidate_resync {
            generation.wrapping_add(1)
        } else {
            generation
        };
        *state = staged;
        Ok(true)
    }

    #[cfg(test)]
    fn apply_actor(
        &self,
        version: OwnershipRevision,
        record: ActorPlacementRecord,
    ) -> Result<bool, OwnershipSnapshotError> {
        let mut state = self.inner.write_state();
        if state.revision.is_some_and(|current| current >= version) {
            return Ok(false);
        }
        if record.service_kind != self.inner.service_kind {
            state.accept_revision(version);
            return Ok(false);
        }
        let key = actor_key(&record);
        let authority = OwnershipAuthorityFloor::actor(&record);
        let is_local = record.owner == self.inner.instance_id;
        if let Some(current) = state.actors.get(&key)
            && let Err(error) = validate_authority_progression(
                OwnershipKey::Explicit(key.clone()),
                &current.authority,
                &authority,
                current.observation.is_absent()
                    || (current.observation.is_lifecycle_fenced() && is_local),
            )
        {
            Self::fence_rejected_batch(&mut state, &error);
            return Err(error);
        }
        let already_present = state.actors.contains_key(&key);
        self.ensure_insert_capacity(&mut state, already_present)?;
        let authorization_changed =
            local_authorization_changed(state.actors.get(&key), is_local.then_some(&record));
        state.accept_revision(version);
        state.actors.insert(
            key,
            VersionedRecord {
                version,
                authority,
                observation: StoreObservation::present(record, is_local),
            },
        );
        if authorization_changed {
            state.bump_generation();
        }
        Ok(authorization_changed)
    }

    #[cfg(test)]
    fn apply_virtual_shard(
        &self,
        version: OwnershipRevision,
        record: VirtualShardPlacementRecord,
    ) -> Result<bool, OwnershipSnapshotError> {
        let mut state = self.inner.write_state();
        if state.revision.is_some_and(|current| current >= version) {
            return Ok(false);
        }
        if record.service_kind != self.inner.service_kind {
            state.accept_revision(version);
            return Ok(false);
        }
        let key = virtual_shard_key(&record);
        let authority = OwnershipAuthorityFloor::virtual_shard(&record);
        let is_local = record.owner == self.inner.instance_id;
        if let Some(current) = state.virtual_shards.get(&key)
            && let Err(error) = validate_authority_progression(
                OwnershipKey::VirtualShard(key.clone()),
                &current.authority,
                &authority,
                current.observation.is_absent()
                    || (current.observation.is_lifecycle_fenced() && is_local),
            )
        {
            Self::fence_rejected_batch(&mut state, &error);
            return Err(error);
        }
        let already_present = state.virtual_shards.contains_key(&key);
        self.ensure_insert_capacity(&mut state, already_present)?;
        let authorization_changed = local_authorization_changed(
            state.virtual_shards.get(&key),
            is_local.then_some(&record),
        );
        state.accept_revision(version);
        state.virtual_shards.insert(
            key,
            VersionedRecord {
                version,
                authority,
                observation: StoreObservation::present(record, is_local),
            },
        );
        if authorization_changed {
            state.bump_generation();
        }
        Ok(authorization_changed)
    }

    #[cfg(test)]
    fn apply_singleton(
        &self,
        version: OwnershipRevision,
        record: SingletonPlacementRecord,
    ) -> Result<bool, OwnershipSnapshotError> {
        let mut state = self.inner.write_state();
        if state.revision.is_some_and(|current| current >= version) {
            return Ok(false);
        }
        if record.service_kind != self.inner.service_kind {
            state.accept_revision(version);
            return Ok(false);
        }
        let key = singleton_key(&record);
        let authority = OwnershipAuthorityFloor::singleton(&record);
        let is_local = record.owner == self.inner.instance_id;
        if let Some(current) = state.singletons.get(&key)
            && let Err(error) = validate_authority_progression(
                OwnershipKey::Singleton(key.clone()),
                &current.authority,
                &authority,
                current.observation.is_absent()
                    || (current.observation.is_lifecycle_fenced() && is_local),
            )
        {
            Self::fence_rejected_batch(&mut state, &error);
            return Err(error);
        }
        let already_present = state.singletons.contains_key(&key);
        self.ensure_insert_capacity(&mut state, already_present)?;
        let authorization_changed =
            local_authorization_changed(state.singletons.get(&key), is_local.then_some(&record));
        state.accept_revision(version);
        state.singletons.insert(
            key,
            VersionedRecord {
                version,
                authority,
                observation: StoreObservation::present(record, is_local),
            },
        );
        if authorization_changed {
            state.bump_generation();
        }
        Ok(authorization_changed)
    }

    #[cfg(test)]
    fn remove(&self, key: &OwnershipKey, version: OwnershipRevision) -> bool {
        let mut state = self.inner.write_state();
        if !state.accept_revision(version) {
            return false;
        }
        let removal = match key {
            OwnershipKey::Explicit(key) => apply_removal(&mut state.actors, key, version),
            OwnershipKey::VirtualShard(key) => {
                apply_removal(&mut state.virtual_shards, key, version)
            }
            OwnershipKey::Singleton(key) => apply_removal(&mut state.singletons, key, version),
        };
        if removal.authorization_changed {
            state.bump_generation();
        }
        removal.applied
    }

    fn validate_instance_identity(
        &self,
        record: &InstanceRecord,
    ) -> Result<(), OwnershipSnapshotError> {
        if record.service_kind != self.inner.service_kind {
            return Err(OwnershipSnapshotError::ServiceMismatch {
                expected: self.inner.service_kind.clone(),
                actual: record.service_kind.clone(),
            });
        }
        if record.instance_id != self.inner.instance_id {
            return Err(OwnershipSnapshotError::InstanceMismatch {
                expected: self.inner.instance_id.clone(),
                actual: record.instance_id.clone(),
            });
        }
        if record.incarnation != self.inner.instance_incarnation {
            return Err(OwnershipSnapshotError::InstanceIncarnationMismatch {
                expected: self.inner.instance_incarnation.clone(),
                actual: record.incarnation.clone(),
            });
        }
        Ok(())
    }

    fn validate_watch_events(
        &self,
        revision: PlacementRevision,
        events: &[OwnershipWatchEvent],
    ) -> Result<(), OwnershipSnapshotError> {
        if events.is_empty() {
            return Err(OwnershipSnapshotError::EmptyWatchBatch);
        }
        if events.len() > MAX_OWNERSHIP_BATCH_EVENTS {
            return Err(OwnershipSnapshotError::WatchBatchCapacityExceeded {
                max_events: MAX_OWNERSHIP_BATCH_EVENTS,
            });
        }

        let mut saw_instance = false;
        let mut placement_keys = HashSet::new();
        let mut relevant_events = 0usize;

        for event in events {
            let relevant_key = match event {
                OwnershipWatchEvent::InstanceUpserted { record }
                | OwnershipWatchEvent::InstanceDeleted { record } => {
                    if record.service_kind != self.inner.service_kind
                        || record.instance_id != self.inner.instance_id
                    {
                        continue;
                    }
                    if saw_instance {
                        return Err(OwnershipSnapshotError::DuplicateInstanceEvent);
                    }
                    saw_instance = true;
                    None
                }
                OwnershipWatchEvent::ActorUpserted { key, record, proof } => {
                    if key.service_kind != self.inner.service_kind {
                        continue;
                    }
                    let event_key = OwnershipKey::Explicit(key.clone());
                    let record_key = OwnershipKey::Explicit(actor_key(record));
                    if event_key != record_key {
                        return Err(OwnershipSnapshotError::PlacementKeyMismatch {
                            event: Box::new(event_key),
                            record: Box::new(record_key),
                        });
                    }
                    proof
                        .validate_upsert(revision, &OwnershipRecordBinding::Actor(record.clone()))
                        .map_err(proof_error)?;
                    Some(OwnershipKey::Explicit(key.clone()))
                }
                OwnershipWatchEvent::ActorDeleted {
                    key,
                    previous_record,
                    proof,
                } => {
                    if key.service_kind != self.inner.service_kind {
                        continue;
                    }
                    let event_key = OwnershipKey::Explicit(key.clone());
                    let record_key = OwnershipKey::Explicit(actor_key(previous_record));
                    if event_key != record_key {
                        return Err(OwnershipSnapshotError::PlacementKeyMismatch {
                            event: Box::new(event_key),
                            record: Box::new(record_key),
                        });
                    }
                    proof
                        .validate_delete(
                            revision,
                            &OwnershipRecordBinding::Actor(previous_record.clone()),
                        )
                        .map_err(proof_error)?;
                    Some(OwnershipKey::Explicit(key.clone()))
                }
                OwnershipWatchEvent::VirtualShardUpserted { key, record, proof } => {
                    if key.service_kind != self.inner.service_kind {
                        continue;
                    }
                    let event_key = OwnershipKey::VirtualShard(key.clone());
                    let record_key = OwnershipKey::VirtualShard(virtual_shard_key(record));
                    if event_key != record_key {
                        return Err(OwnershipSnapshotError::PlacementKeyMismatch {
                            event: Box::new(event_key),
                            record: Box::new(record_key),
                        });
                    }
                    proof
                        .validate_upsert(
                            revision,
                            &OwnershipRecordBinding::VirtualShard(record.clone()),
                        )
                        .map_err(proof_error)?;
                    Some(OwnershipKey::VirtualShard(key.clone()))
                }
                OwnershipWatchEvent::VirtualShardDeleted {
                    key,
                    previous_record,
                    proof,
                } => {
                    if key.service_kind != self.inner.service_kind {
                        continue;
                    }
                    let event_key = OwnershipKey::VirtualShard(key.clone());
                    let record_key = OwnershipKey::VirtualShard(virtual_shard_key(previous_record));
                    if event_key != record_key {
                        return Err(OwnershipSnapshotError::PlacementKeyMismatch {
                            event: Box::new(event_key),
                            record: Box::new(record_key),
                        });
                    }
                    proof
                        .validate_delete(
                            revision,
                            &OwnershipRecordBinding::VirtualShard(previous_record.clone()),
                        )
                        .map_err(proof_error)?;
                    Some(OwnershipKey::VirtualShard(key.clone()))
                }
                OwnershipWatchEvent::SingletonUpserted { key, record, proof } => {
                    if key.service_kind != self.inner.service_kind {
                        continue;
                    }
                    let event_key = OwnershipKey::Singleton(key.clone());
                    let record_key = OwnershipKey::Singleton(singleton_key(record));
                    if event_key != record_key {
                        return Err(OwnershipSnapshotError::PlacementKeyMismatch {
                            event: Box::new(event_key),
                            record: Box::new(record_key),
                        });
                    }
                    proof
                        .validate_upsert(
                            revision,
                            &OwnershipRecordBinding::Singleton(record.clone()),
                        )
                        .map_err(proof_error)?;
                    Some(OwnershipKey::Singleton(key.clone()))
                }
                OwnershipWatchEvent::SingletonDeleted {
                    key,
                    previous_record,
                    proof,
                } => {
                    if key.service_kind != self.inner.service_kind {
                        continue;
                    }
                    let event_key = OwnershipKey::Singleton(key.clone());
                    let record_key = OwnershipKey::Singleton(singleton_key(previous_record));
                    if event_key != record_key {
                        return Err(OwnershipSnapshotError::PlacementKeyMismatch {
                            event: Box::new(event_key),
                            record: Box::new(record_key),
                        });
                    }
                    proof
                        .validate_delete(
                            revision,
                            &OwnershipRecordBinding::Singleton(previous_record.clone()),
                        )
                        .map_err(proof_error)?;
                    Some(OwnershipKey::Singleton(key.clone()))
                }
            };

            let Some(relevant_key) = relevant_key else {
                continue;
            };
            relevant_events = relevant_events.saturating_add(1);
            if relevant_events > self.inner.max_entries.get() {
                return Err(self.capacity_error());
            }
            if !placement_keys.insert(relevant_key.clone()) {
                return Err(OwnershipSnapshotError::DuplicatePlacementEvent {
                    key: Box::new(relevant_key),
                });
            }
        }
        Ok(())
    }

    fn validate_watch_epoch_progression(
        &self,
        state: &LocalOwnershipState,
        events: &[OwnershipWatchEvent],
    ) -> Result<(), OwnershipSnapshotError> {
        let lifecycle_fences_placements = events.iter().any(|event| match event {
            OwnershipWatchEvent::InstanceDeleted { record }
                if record.service_kind == self.inner.service_kind
                    && record.instance_id == self.inner.instance_id =>
            {
                true
            }
            OwnershipWatchEvent::InstanceUpserted { record }
                if record.service_kind == self.inner.service_kind
                    && record.instance_id == self.inner.instance_id =>
            {
                state.instance.as_ref().is_some_and(|current| {
                    current.lease_id != record.lease_id || current.incarnation != record.incarnation
                })
            }
            _ => false,
        });
        for event in events {
            match event {
                OwnershipWatchEvent::ActorUpserted { key, record, .. }
                    if key.service_kind == self.inner.service_kind =>
                {
                    if let Some(current) = state.actors.get(key) {
                        validate_authority_progression(
                            OwnershipKey::Explicit(key.clone()),
                            &current.authority,
                            &OwnershipAuthorityFloor::actor(record),
                            current.observation.is_absent()
                                || ((current.observation.is_lifecycle_fenced()
                                    || lifecycle_fences_placements)
                                    && record.owner == self.inner.instance_id),
                        )?;
                    }
                }
                OwnershipWatchEvent::ActorDeleted {
                    key,
                    previous_record,
                    proof,
                } if key.service_kind == self.inner.service_kind => {
                    validate_delete_previous(
                        state.actors.get(key),
                        previous_record,
                        &OwnershipAuthorityFloor::actor(previous_record),
                        proof,
                        OwnershipKey::Explicit(key.clone()),
                    )?;
                }
                OwnershipWatchEvent::VirtualShardUpserted { key, record, .. }
                    if key.service_kind == self.inner.service_kind =>
                {
                    if let Some(current) = state.virtual_shards.get(key) {
                        validate_authority_progression(
                            OwnershipKey::VirtualShard(key.clone()),
                            &current.authority,
                            &OwnershipAuthorityFloor::virtual_shard(record),
                            current.observation.is_absent()
                                || ((current.observation.is_lifecycle_fenced()
                                    || lifecycle_fences_placements)
                                    && record.owner == self.inner.instance_id),
                        )?;
                    }
                }
                OwnershipWatchEvent::VirtualShardDeleted {
                    key,
                    previous_record,
                    proof,
                } if key.service_kind == self.inner.service_kind => {
                    validate_delete_previous(
                        state.virtual_shards.get(key),
                        previous_record,
                        &OwnershipAuthorityFloor::virtual_shard(previous_record),
                        proof,
                        OwnershipKey::VirtualShard(key.clone()),
                    )?;
                }
                OwnershipWatchEvent::SingletonUpserted { key, record, .. }
                    if key.service_kind == self.inner.service_kind =>
                {
                    if let Some(current) = state.singletons.get(key) {
                        validate_authority_progression(
                            OwnershipKey::Singleton(key.clone()),
                            &current.authority,
                            &OwnershipAuthorityFloor::singleton(record),
                            current.observation.is_absent()
                                || ((current.observation.is_lifecycle_fenced()
                                    || lifecycle_fences_placements)
                                    && record.owner == self.inner.instance_id),
                        )?;
                    }
                }
                OwnershipWatchEvent::SingletonDeleted {
                    key,
                    previous_record,
                    proof,
                } if key.service_kind == self.inner.service_kind => {
                    validate_delete_previous(
                        state.singletons.get(key),
                        previous_record,
                        &OwnershipAuthorityFloor::singleton(previous_record),
                        proof,
                        OwnershipKey::Singleton(key.clone()),
                    )?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn stage_watch_events(
        &self,
        state: &mut LocalOwnershipState,
        revision: OwnershipRevision,
        events: &[OwnershipWatchEvent],
    ) -> Result<bool, OwnershipSnapshotError> {
        let mut changed = false;

        // Instance authority is applied first so a lease reincarnation has the
        // same result regardless of event order in the backend transaction.
        for event in events {
            match event {
                OwnershipWatchEvent::InstanceUpserted { record }
                    if record.service_kind == self.inner.service_kind
                        && record.instance_id == self.inner.instance_id =>
                {
                    changed |= self.stage_instance_upsert(state, record);
                }
                OwnershipWatchEvent::InstanceDeleted { record }
                    if record.service_kind == self.inner.service_kind
                        && record.instance_id == self.inner.instance_id =>
                {
                    changed |= self.stage_instance_delete(state, record);
                }
                _ => {}
            }
        }

        for event in events {
            match event {
                OwnershipWatchEvent::ActorUpserted { key, record, .. }
                    if key.service_kind == self.inner.service_kind =>
                {
                    changed |= self.stage_actor_upsert(state, key, record, revision)?;
                }
                OwnershipWatchEvent::ActorDeleted { key, .. }
                    if key.service_kind == self.inner.service_kind =>
                {
                    changed |=
                        apply_removal(&mut state.actors, key, revision).authorization_changed;
                }
                OwnershipWatchEvent::VirtualShardUpserted { key, record, .. }
                    if key.service_kind == self.inner.service_kind =>
                {
                    changed |= self.stage_virtual_shard_upsert(state, key, record, revision)?;
                }
                OwnershipWatchEvent::VirtualShardDeleted { key, .. }
                    if key.service_kind == self.inner.service_kind =>
                {
                    changed |= apply_removal(&mut state.virtual_shards, key, revision)
                        .authorization_changed;
                }
                OwnershipWatchEvent::SingletonUpserted { key, record, .. }
                    if key.service_kind == self.inner.service_kind =>
                {
                    changed |= self.stage_singleton_upsert(state, key, record, revision)?;
                }
                OwnershipWatchEvent::SingletonDeleted { key, .. }
                    if key.service_kind == self.inner.service_kind =>
                {
                    changed |=
                        apply_removal(&mut state.singletons, key, revision).authorization_changed;
                }
                OwnershipWatchEvent::InstanceUpserted { .. }
                | OwnershipWatchEvent::InstanceDeleted { .. }
                | OwnershipWatchEvent::ActorUpserted { .. }
                | OwnershipWatchEvent::ActorDeleted { .. }
                | OwnershipWatchEvent::VirtualShardUpserted { .. }
                | OwnershipWatchEvent::VirtualShardDeleted { .. }
                | OwnershipWatchEvent::SingletonUpserted { .. }
                | OwnershipWatchEvent::SingletonDeleted { .. } => {}
            }
        }
        Ok(changed)
    }

    fn stage_instance_upsert(
        &self,
        state: &mut LocalOwnershipState,
        record: &InstanceRecord,
    ) -> bool {
        if record.incarnation != self.inner.instance_incarnation {
            state.instance = None;
            state.fence_local_placements_for_lifecycle();
            state.valid_owner_leases.clear();
            state.availability = SnapshotAvailability::Unavailable(OwnershipFenceReason::LeaseLost);
            return true;
        }
        if state.instance.as_ref().is_some_and(|instance| {
            instance.lease_id == record.lease_id && instance.incarnation == record.incarnation
        }) {
            state.instance = Some(LocalInstanceAuthority {
                lease_id: record.lease_id,
                incarnation: record.incarnation.clone(),
                state: record.state,
            });
            // Preserve explicit keepalive validity for the current lease. A
            // registry watch event is not evidence that local keepalive is live.
            return true;
        }

        state.fence_local_placements_for_lifecycle();
        state.valid_owner_leases.clear();
        state.instance = Some(LocalInstanceAuthority {
            lease_id: record.lease_id,
            incarnation: record.incarnation.clone(),
            state: record.state,
        });
        state.availability = SnapshotAvailability::Unavailable(OwnershipFenceReason::Initializing);
        true
    }

    fn stage_instance_delete(
        &self,
        state: &mut LocalOwnershipState,
        _record: &InstanceRecord,
    ) -> bool {
        state.instance = None;
        state.fence_local_placements_for_lifecycle();
        state.valid_owner_leases.clear();
        state.availability = SnapshotAvailability::Unavailable(OwnershipFenceReason::LeaseLost);
        true
    }

    fn stage_actor_upsert(
        &self,
        state: &mut LocalOwnershipState,
        key: &ActorPlacementKey,
        record: &ActorPlacementRecord,
        revision: OwnershipRevision,
    ) -> Result<bool, OwnershipSnapshotError> {
        let is_local = record.owner == self.inner.instance_id;
        self.ensure_staged_insert_capacity(state, state.actors.contains_key(key))?;
        let authorization_changed =
            local_authorization_changed(state.actors.get(key), is_local.then_some(record));
        state.actors.insert(
            key.clone(),
            VersionedRecord {
                version: revision,
                authority: OwnershipAuthorityFloor::actor(record),
                observation: StoreObservation::present(record.clone(), is_local),
            },
        );
        Ok(authorization_changed)
    }

    fn stage_virtual_shard_upsert(
        &self,
        state: &mut LocalOwnershipState,
        key: &VirtualShardPlacementKey,
        record: &VirtualShardPlacementRecord,
        revision: OwnershipRevision,
    ) -> Result<bool, OwnershipSnapshotError> {
        let is_local = record.owner == self.inner.instance_id;
        self.ensure_staged_insert_capacity(state, state.virtual_shards.contains_key(key))?;
        let authorization_changed =
            local_authorization_changed(state.virtual_shards.get(key), is_local.then_some(record));
        state.virtual_shards.insert(
            key.clone(),
            VersionedRecord {
                version: revision,
                authority: OwnershipAuthorityFloor::virtual_shard(record),
                observation: StoreObservation::present(record.clone(), is_local),
            },
        );
        Ok(authorization_changed)
    }

    fn stage_singleton_upsert(
        &self,
        state: &mut LocalOwnershipState,
        key: &SingletonKey,
        record: &SingletonPlacementRecord,
        revision: OwnershipRevision,
    ) -> Result<bool, OwnershipSnapshotError> {
        let is_local = record.owner == self.inner.instance_id;
        self.ensure_staged_insert_capacity(state, state.singletons.contains_key(key))?;
        let authorization_changed =
            local_authorization_changed(state.singletons.get(key), is_local.then_some(record));
        state.singletons.insert(
            key.clone(),
            VersionedRecord {
                version: revision,
                authority: OwnershipAuthorityFloor::singleton(record),
                observation: StoreObservation::present(record.clone(), is_local),
            },
        );
        Ok(authorization_changed)
    }

    fn ensure_staged_insert_capacity(
        &self,
        state: &LocalOwnershipState,
        already_present: bool,
    ) -> Result<(), OwnershipSnapshotError> {
        if !already_present && state.entry_count() >= self.inner.max_entries.get() {
            return Err(self.capacity_error());
        }
        Ok(())
    }

    fn fence_rejected_batch(state: &mut LocalOwnershipState, error: &OwnershipSnapshotError) {
        let reason = if matches!(
            error,
            OwnershipSnapshotError::CapacityExceeded { .. }
                | OwnershipSnapshotError::WatchBatchCapacityExceeded { .. }
        ) {
            OwnershipFenceReason::CapacityExceeded
        } else {
            OwnershipFenceReason::WatchLost
        };
        state.availability = SnapshotAvailability::Unavailable(reason);
        state.bump_generation();
    }

    fn collect_local_records<I>(
        &self,
        snapshot_revision: OwnershipRevision,
        records: I,
    ) -> Result<LocalRecordMaps, OwnershipSnapshotError>
    where
        I: IntoIterator<Item = OwnershipSnapshotRecord>,
    {
        let mut maps = CollectedLocalRecords::default();
        for (index, item) in records.into_iter().enumerate() {
            if index >= self.inner.max_entries.get() {
                self.fence(OwnershipFenceReason::CapacityExceeded);
                return Err(self.capacity_error());
            }
            let record_revision = match &item {
                OwnershipSnapshotRecord::Actor { version, .. }
                | OwnershipSnapshotRecord::VirtualShard { version, .. }
                | OwnershipSnapshotRecord::Singleton { version, .. } => *version,
            };
            if record_revision > snapshot_revision {
                self.fence(OwnershipFenceReason::Resyncing);
                return Err(OwnershipSnapshotError::RecordAheadOfSnapshot {
                    record: record_revision,
                    snapshot: snapshot_revision,
                });
            }
            match item {
                OwnershipSnapshotRecord::Actor {
                    version,
                    record,
                    proof,
                } if record.service_kind == self.inner.service_kind => {
                    self.validate_snapshot_proof(
                        snapshot_revision,
                        version,
                        &proof,
                        &OwnershipRecordBinding::Actor(record.clone()),
                    )?;
                    let key = actor_key(&record);
                    let authority = OwnershipAuthorityFloor::actor(&record);
                    let local = record.owner == self.inner.instance_id;
                    insert_collected_record(
                        &mut maps.actors,
                        key,
                        version,
                        authority,
                        record,
                        local,
                        OwnershipKey::Explicit,
                    )?;
                }
                OwnershipSnapshotRecord::VirtualShard {
                    version,
                    record,
                    proof,
                } if record.service_kind == self.inner.service_kind => {
                    self.validate_snapshot_proof(
                        snapshot_revision,
                        version,
                        &proof,
                        &OwnershipRecordBinding::VirtualShard(record.clone()),
                    )?;
                    let key = virtual_shard_key(&record);
                    let authority = OwnershipAuthorityFloor::virtual_shard(&record);
                    let local = record.owner == self.inner.instance_id;
                    insert_collected_record(
                        &mut maps.virtual_shards,
                        key,
                        version,
                        authority,
                        record,
                        local,
                        OwnershipKey::VirtualShard,
                    )?;
                }
                OwnershipSnapshotRecord::Singleton {
                    version,
                    record,
                    proof,
                } if record.service_kind == self.inner.service_kind => {
                    self.validate_snapshot_proof(
                        snapshot_revision,
                        version,
                        &proof,
                        &OwnershipRecordBinding::Singleton(record.clone()),
                    )?;
                    let key = singleton_key(&record);
                    let authority = OwnershipAuthorityFloor::singleton(&record);
                    let local = record.owner == self.inner.instance_id;
                    insert_collected_record(
                        &mut maps.singletons,
                        key,
                        version,
                        authority,
                        record,
                        local,
                        OwnershipKey::Singleton,
                    )?;
                }
                _ => {}
            }
            self.ensure_collected_capacity(&maps)?;
        }
        Ok((maps.actors, maps.virtual_shards, maps.singletons))
    }

    fn validate_snapshot_proof(
        &self,
        snapshot_revision: OwnershipRevision,
        record_revision: OwnershipRevision,
        proof: &OwnershipEpochFloorProof,
        binding: &OwnershipRecordBinding,
    ) -> Result<(), OwnershipSnapshotError> {
        if let Err(error) = proof.validate_snapshot(
            PlacementRevision(snapshot_revision.0),
            PlacementRevision(record_revision.0),
            binding,
        ) {
            self.fence(OwnershipFenceReason::Resyncing);
            return Err(proof_error(error));
        }
        Ok(())
    }

    fn ensure_collected_capacity(
        &self,
        maps: &CollectedLocalRecords,
    ) -> Result<(), OwnershipSnapshotError> {
        if maps.entry_count() > self.inner.max_entries.get() {
            self.fence(OwnershipFenceReason::CapacityExceeded);
            return Err(self.capacity_error());
        }
        Ok(())
    }

    #[cfg(test)]
    fn ensure_insert_capacity(
        &self,
        state: &mut LocalOwnershipState,
        already_present: bool,
    ) -> Result<(), OwnershipSnapshotError> {
        if !already_present && state.entry_count() >= self.inner.max_entries.get() {
            state.availability =
                SnapshotAvailability::Unavailable(OwnershipFenceReason::CapacityExceeded);
            state.bump_generation();
            return Err(self.capacity_error());
        }
        Ok(())
    }

    fn capacity_error(&self) -> OwnershipSnapshotError {
        OwnershipSnapshotError::CapacityExceeded {
            max_entries: self.inner.max_entries.get(),
        }
    }
}

type LocalRecordMaps = (
    HashMap<ActorPlacementKey, VersionedRecord<ActorPlacementRecord>>,
    HashMap<VirtualShardPlacementKey, VersionedRecord<VirtualShardPlacementRecord>>,
    HashMap<SingletonKey, VersionedRecord<SingletonPlacementRecord>>,
);

#[derive(Debug, Default)]
struct CollectedLocalRecords {
    actors: HashMap<ActorPlacementKey, VersionedRecord<ActorPlacementRecord>>,
    virtual_shards: HashMap<VirtualShardPlacementKey, VersionedRecord<VirtualShardPlacementRecord>>,
    singletons: HashMap<SingletonKey, VersionedRecord<SingletonPlacementRecord>>,
}

impl CollectedLocalRecords {
    fn entry_count(&self) -> usize {
        self.actors.len() + self.virtual_shards.len() + self.singletons.len()
    }
}

fn insert_collected_record<K, V, F>(
    records: &mut HashMap<K, VersionedRecord<V>>,
    key: K,
    version: OwnershipRevision,
    authority: OwnershipAuthorityFloor,
    record: V,
    is_local: bool,
    ownership_key: F,
) -> Result<(), OwnershipSnapshotError>
where
    K: Clone + Eq + std::hash::Hash,
    F: FnOnce(K) -> OwnershipKey,
{
    if records.contains_key(&key) {
        return Err(OwnershipSnapshotError::DuplicatePlacementEvent {
            key: Box::new(ownership_key(key)),
        });
    }
    records.insert(
        key,
        VersionedRecord {
            version,
            authority,
            observation: StoreObservation::present(record, is_local),
        },
    );
    Ok(())
}

fn merge_resync_observations<K, V, F>(
    current: &HashMap<K, VersionedRecord<V>>,
    incoming: &mut HashMap<K, VersionedRecord<V>>,
    snapshot_revision: OwnershipRevision,
    ownership_key: F,
) -> Result<(), OwnershipSnapshotError>
where
    K: Clone + Eq + std::hash::Hash,
    V: Clone,
    F: Fn(&K) -> OwnershipKey,
{
    for (key, incoming_entry) in incoming.iter_mut() {
        let Some(current_entry) = current.get(key) else {
            continue;
        };
        let key = ownership_key(key);
        if current_entry.observation.is_absent() && incoming_entry.version <= current_entry.version
        {
            return Err(OwnershipSnapshotError::ResurrectionRevisionNotAdvanced {
                key: Box::new(key),
                absence: current_entry.version,
                incoming: incoming_entry.version,
            });
        }
        validate_authority_progression(
            key,
            &current_entry.authority,
            &incoming_entry.authority,
            current_entry.observation.is_absent()
                || (current_entry.observation.is_lifecycle_fenced()
                    && incoming_entry.observation.local_record().is_some()),
        )?;
    }

    for (key, current_entry) in current {
        let evidence = match &current_entry.observation {
            StoreObservation::Absent { evidence, .. } => *evidence,
            StoreObservation::PresentLocal(_)
            | StoreObservation::PresentRemote(_)
            | StoreObservation::LifecycleFenced(_) => AbsenceEvidence::CoherentSnapshot,
        };
        incoming
            .entry(key.clone())
            .or_insert_with(|| VersionedRecord {
                version: snapshot_revision,
                authority: current_entry.authority.clone(),
                observation: StoreObservation::Absent {
                    last_record: current_entry.observation.last_record().clone(),
                    evidence,
                },
            });
    }
    Ok(())
}

fn apply_removal<K, V>(
    records: &mut HashMap<K, VersionedRecord<V>>,
    key: &K,
    version: OwnershipRevision,
) -> PlacementMutation
where
    K: Eq + std::hash::Hash,
    V: Clone,
{
    let Some(current) = records.get_mut(key) else {
        return PlacementMutation::default();
    };
    if current.version > version || (current.version == version && current.observation.is_absent())
    {
        return PlacementMutation::default();
    }
    let authorization_changed = current.observation.local_record().is_some();
    let last_record = current.observation.last_record().clone();
    current.version = version;
    current.observation = StoreObservation::Absent {
        last_record,
        evidence: AbsenceEvidence::ExactDelete,
    };
    PlacementMutation {
        applied: true,
        authorization_changed,
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PlacementMutation {
    applied: bool,
    authorization_changed: bool,
}

fn local_authorization_changed<T: PartialEq>(
    current: Option<&VersionedRecord<T>>,
    incoming_local: Option<&T>,
) -> bool {
    current.and_then(|entry| entry.observation.local_record()) != incoming_local
}

fn proof_error(error: OwnershipProofError) -> OwnershipSnapshotError {
    OwnershipSnapshotError::Proof { error }
}

fn validate_delete_previous<T: PartialEq>(
    current: Option<&VersionedRecord<T>>,
    previous_record: &T,
    previous_authority: &OwnershipAuthorityFloor,
    proof: &OwnershipEpochFloorProof,
    key: OwnershipKey,
) -> Result<(), OwnershipSnapshotError> {
    let matches = current.is_some_and(|current| {
        current.version == OwnershipRevision(proof.record_revision().0)
            && &current.authority == previous_authority
            && current
                .observation
                .store_present_record()
                .is_some_and(|record| record == previous_record)
    });
    if matches {
        Ok(())
    } else {
        Err(proof_error(OwnershipProofError::DeletePreviousMismatch {
            key: match key {
                OwnershipKey::Explicit(key) => crate::storage::PlacementEpochKey::Actor(key),
                OwnershipKey::VirtualShard(key) => {
                    crate::storage::PlacementEpochKey::VirtualShard(key)
                }
                OwnershipKey::Singleton(key) => crate::storage::PlacementEpochKey::Singleton(key),
            },
        }))
    }
}

fn validate_authority_progression(
    key: OwnershipKey,
    current: &OwnershipAuthorityFloor,
    incoming: &OwnershipAuthorityFloor,
    reactivating: bool,
) -> Result<(), OwnershipSnapshotError> {
    if incoming.epoch < current.epoch {
        return Err(OwnershipSnapshotError::EpochRegression {
            key: Box::new(key),
            current: current.epoch,
            incoming: incoming.epoch,
        });
    }
    if incoming.epoch == current.epoch
        && (incoming.owner != current.owner || incoming.lease_id != current.lease_id)
    {
        return Err(OwnershipSnapshotError::EpochAuthorityConflict {
            key: Box::new(key),
            epoch: incoming.epoch,
        });
    }
    if incoming.epoch == current.epoch && reactivating {
        return Err(OwnershipSnapshotError::EpochReactivation {
            key: Box::new(key),
            epoch: incoming.epoch,
        });
    }
    Ok(())
}

impl OwnershipGate for LocalOwnershipGate {
    fn authorize(
        &self,
        request: OwnershipRequest<'_>,
    ) -> Result<OwnershipGrant, Box<OwnershipRejection>> {
        let key = self.validate_target_and_derive_key(request)?;
        if request.route_epoch.is_none() {
            return Err(rejection(
                OwnershipRejectionKind::Fenced,
                OwnershipRejectionReason::MissingEpoch,
                Some(key),
                request.route_epoch,
                None,
                None,
            ));
        }

        let state = self.inner.read_state();
        if let SnapshotAvailability::Unavailable(reason) = state.availability {
            return Err(rejection(
                OwnershipRejectionKind::Fenced,
                OwnershipRejectionReason::SnapshotUnavailable { reason },
                Some(key),
                request.route_epoch,
                None,
                None,
            ));
        }
        let instance = state.instance.as_ref().ok_or_else(|| {
            rejection(
                OwnershipRejectionKind::Fenced,
                OwnershipRejectionReason::LocalInstanceMissing,
                Some(key.clone()),
                request.route_epoch,
                None,
                None,
            )
        })?;
        if instance.state != InstanceState::Ready {
            return Err(rejection(
                OwnershipRejectionKind::Fenced,
                OwnershipRejectionReason::InstanceNotReady {
                    state: instance.state,
                },
                Some(key),
                request.route_epoch,
                None,
                Some(self.inner.instance_id.clone()),
            ));
        }
        if !state.valid_owner_leases.contains(&instance.lease_id) {
            return Err(rejection(
                OwnershipRejectionKind::Fenced,
                OwnershipRejectionReason::InstanceLeaseInvalid {
                    lease_id: instance.lease_id,
                },
                Some(key),
                request.route_epoch,
                None,
                Some(self.inner.instance_id.clone()),
            ));
        }

        let authority = state.authority(&key).ok_or_else(|| {
            rejection(
                OwnershipRejectionKind::NotOwner,
                OwnershipRejectionReason::PlacementMissing,
                Some(key.clone()),
                request.route_epoch,
                None,
                None,
            )
        })?;
        if authority.owner != self.inner.instance_id {
            return Err(rejection(
                OwnershipRejectionKind::NotOwner,
                OwnershipRejectionReason::OwnerMismatch,
                Some(key),
                request.route_epoch,
                Some(authority.epoch),
                Some(authority.owner),
            ));
        }
        if authority
            .owner_incarnation
            .as_ref()
            .is_some_and(|incarnation| incarnation != &self.inner.instance_incarnation)
        {
            return Err(rejection(
                OwnershipRejectionKind::Fenced,
                OwnershipRejectionReason::OwnerIncarnationMismatch,
                Some(key),
                request.route_epoch,
                Some(authority.epoch),
                Some(authority.owner),
            ));
        }
        if let Some(placement_state) = authority.state
            && placement_state != PlacementState::Running
        {
            return Err(rejection(
                OwnershipRejectionKind::Fenced,
                OwnershipRejectionReason::PlacementNotRunning {
                    state: placement_state,
                },
                Some(key),
                request.route_epoch,
                Some(authority.epoch),
                Some(authority.owner),
            ));
        }
        let owner_lease = authority.lease_id.unwrap_or(instance.lease_id);
        if !state.valid_owner_leases.contains(&owner_lease) {
            return Err(rejection(
                OwnershipRejectionKind::Fenced,
                OwnershipRejectionReason::OwnerLeaseInvalid {
                    lease_id: owner_lease,
                },
                Some(key),
                request.route_epoch,
                Some(authority.epoch),
                Some(authority.owner),
            ));
        }
        if request.route_epoch != Some(authority.epoch) {
            return Err(rejection(
                OwnershipRejectionKind::Fenced,
                OwnershipRejectionReason::EpochMismatch,
                Some(key),
                request.route_epoch,
                Some(authority.epoch),
                Some(authority.owner),
            ));
        }

        Ok(OwnershipGrant {
            key,
            epoch: authority.epoch,
            lease_id: owner_lease,
            generation: state.generation,
        })
    }

    fn is_current(&self, grant: &OwnershipGrant) -> bool {
        let state = self.inner.read_state();
        state.generation == grant.generation
            && state.availability == SnapshotAvailability::Ready
            && state.valid_owner_leases.contains(&grant.lease_id)
    }
}

impl LocalOwnershipGate {
    fn validate_target_and_derive_key(
        &self,
        request: OwnershipRequest<'_>,
    ) -> Result<OwnershipKey, Box<OwnershipRejection>> {
        if request.expected_service != &self.inner.service_kind {
            return Err(rejection(
                OwnershipRejectionKind::NotOwner,
                OwnershipRejectionReason::ServiceMismatch {
                    local: self.inner.service_kind.clone(),
                    expected: request.expected_service.clone(),
                },
                None,
                request.route_epoch,
                None,
                None,
            ));
        }
        if request.route_actor_kind != request.expected_actor_kind {
            return Err(rejection(
                OwnershipRejectionKind::NotOwner,
                OwnershipRejectionReason::ActorKindMismatch {
                    expected: request.expected_actor_kind.clone(),
                    actual: request.route_actor_kind.clone(),
                },
                None,
                request.route_epoch,
                None,
                None,
            ));
        }

        let key = match request.placement {
            OwnershipPlacement::Explicit => OwnershipKey::Explicit(ActorPlacementKey {
                service_kind: request.expected_service.clone(),
                actor_kind: request.expected_actor_kind.clone(),
                actor_id: actor_id_from_route_key(request.route_key),
            }),
            OwnershipPlacement::VirtualShard { mapper } => {
                OwnershipKey::VirtualShard(VirtualShardPlacementKey {
                    service_kind: request.expected_service.clone(),
                    actor_kind: request.expected_actor_kind.clone(),
                    shard_id: mapper.shard_for_route_key(request.route_key),
                })
            }
            OwnershipPlacement::Singleton => {
                let RouteKey::Str(scope) = request.route_key else {
                    return Err(rejection(
                        OwnershipRejectionKind::NotOwner,
                        OwnershipRejectionReason::InvalidSingletonRoute,
                        None,
                        request.route_epoch,
                        None,
                        None,
                    ));
                };
                OwnershipKey::Singleton(SingletonKey {
                    service_kind: request.expected_service.clone(),
                    singleton_kind: request.expected_actor_kind.clone(),
                    scope: scope.clone(),
                })
            }
        };
        Ok(key)
    }
}

impl LocalOwnershipInner {
    fn read_state(&self) -> RwLockReadGuard<'_, LocalOwnershipState> {
        self.state.read().unwrap_or_else(|error| error.into_inner())
    }

    fn write_state(&self) -> RwLockWriteGuard<'_, LocalOwnershipState> {
        self.state
            .write()
            .unwrap_or_else(|error| error.into_inner())
    }
}

impl LocalOwnershipState {
    fn entry_count(&self) -> usize {
        self.actors.len() + self.virtual_shards.len() + self.singletons.len()
    }

    fn bump_generation(&mut self) {
        self.generation = self.generation.wrapping_add(1);
    }

    fn fence_local_placements_for_lifecycle(&mut self) {
        for entry in self.actors.values_mut() {
            entry.observation.fence_local_for_lifecycle();
        }
        for entry in self.virtual_shards.values_mut() {
            entry.observation.fence_local_for_lifecycle();
        }
        for entry in self.singletons.values_mut() {
            entry.observation.fence_local_for_lifecycle();
        }
    }

    #[cfg(test)]
    fn accept_revision(&mut self, incoming: OwnershipRevision) -> bool {
        if self.revision.is_some_and(|current| current >= incoming) {
            return false;
        }
        self.revision = Some(incoming);
        if self.availability == SnapshotAvailability::Unavailable(OwnershipFenceReason::Resyncing) {
            self.bump_generation();
        }
        true
    }

    fn prune_owner_leases(&mut self, instance_lease: LeaseId) {
        let actor_leases = self.actors.values().filter_map(|entry| {
            entry
                .observation
                .local_record()
                .map(|record| record.lease_id)
        });
        let singleton_leases = self.singletons.values().filter_map(|entry| {
            entry
                .observation
                .local_record()
                .map(|record| record.lease_id)
        });
        let retained = std::iter::once(instance_lease)
            .chain(actor_leases)
            .chain(singleton_leases)
            .collect::<HashSet<_>>();
        self.valid_owner_leases
            .retain(|lease_id| retained.contains(lease_id));
    }

    fn authority(&self, key: &OwnershipKey) -> Option<LocalAuthority> {
        match key {
            OwnershipKey::Explicit(key) => self
                .actors
                .get(key)
                .and_then(|entry| entry.observation.local_record())
                .map(|record| LocalAuthority {
                    owner: record.owner.clone(),
                    owner_incarnation: None,
                    epoch: record.epoch,
                    lease_id: Some(record.lease_id),
                    state: Some(record.state),
                }),
            OwnershipKey::VirtualShard(key) => self
                .virtual_shards
                .get(key)
                .and_then(|entry| entry.observation.local_record())
                .map(|record| LocalAuthority {
                    owner: record.owner.clone(),
                    owner_incarnation: None,
                    epoch: record.epoch,
                    lease_id: None,
                    state: None,
                }),
            OwnershipKey::Singleton(key) => self
                .singletons
                .get(key)
                .and_then(|entry| entry.observation.local_record())
                .map(|record| LocalAuthority {
                    owner: record.owner.clone(),
                    owner_incarnation: Some(record.owner_incarnation.clone()),
                    epoch: record.epoch,
                    lease_id: Some(record.lease_id),
                    state: Some(record.state),
                }),
        }
    }
}

#[derive(Debug)]
struct LocalAuthority {
    owner: InstanceId,
    owner_incarnation: Option<InstanceIncarnation>,
    epoch: Epoch,
    lease_id: Option<LeaseId>,
    state: Option<PlacementState>,
}

fn actor_id_from_route_key(route_key: &RouteKey) -> ActorId {
    match route_key {
        RouteKey::Str(value) => ActorId::Str(value.clone()),
        RouteKey::U64(value) => ActorId::U64(*value),
        RouteKey::I64(value) => ActorId::I64(*value),
        RouteKey::Bytes(value) => ActorId::Bytes(value.clone()),
    }
}

fn actor_key(record: &ActorPlacementRecord) -> ActorPlacementKey {
    ActorPlacementKey {
        service_kind: record.service_kind.clone(),
        actor_kind: record.actor_kind.clone(),
        actor_id: record.actor_id.clone(),
    }
}

fn virtual_shard_key(record: &VirtualShardPlacementRecord) -> VirtualShardPlacementKey {
    VirtualShardPlacementKey {
        service_kind: record.service_kind.clone(),
        actor_kind: record.actor_kind.clone(),
        shard_id: record.shard_id,
    }
}

fn singleton_key(record: &SingletonPlacementRecord) -> SingletonKey {
    SingletonKey {
        service_kind: record.service_kind.clone(),
        singleton_kind: record.singleton_kind.clone(),
        scope: record.scope.clone(),
    }
}

fn rejection(
    kind: OwnershipRejectionKind,
    reason: OwnershipRejectionReason,
    key: Option<OwnershipKey>,
    requested_epoch: Option<Epoch>,
    current_epoch: Option<Epoch>,
    current_owner: Option<InstanceId>,
) -> Box<OwnershipRejection> {
    Box::new(OwnershipRejection {
        kind,
        reason,
        key,
        requested_epoch,
        current_epoch,
        current_owner,
    })
}

#[cfg(test)]
mod tests {
    use lattice_core::instance::{InstanceCapacity, InstanceIncarnation};
    use lattice_core::{actor_kind, service_kind};

    use super::*;
    use crate::storage::{
        EpochFloorRecord, OwnershipProofContext, PlacementRevision, PlacementVersion,
    };

    const INSTANCE_LEASE: LeaseId = LeaseId(11);

    fn assert_present_local<T: std::fmt::Debug>(entry: &VersionedRecord<T>) {
        assert!(
            matches!(&entry.observation, StoreObservation::PresentLocal(_)),
            "expected PresentLocal, got {:?}",
            entry.observation
        );
    }

    fn assert_present_remote<T: std::fmt::Debug>(entry: &VersionedRecord<T>) {
        assert!(
            matches!(&entry.observation, StoreObservation::PresentRemote(_)),
            "expected PresentRemote, got {:?}",
            entry.observation
        );
    }

    fn assert_lifecycle_fenced<T: std::fmt::Debug>(entry: &VersionedRecord<T>) {
        assert!(
            matches!(&entry.observation, StoreObservation::LifecycleFenced(_)),
            "expected LifecycleFenced, got {:?}",
            entry.observation
        );
    }

    fn assert_absent<T: std::fmt::Debug>(
        entry: &VersionedRecord<T>,
        expected_evidence: AbsenceEvidence,
    ) {
        assert!(
            matches!(
                &entry.observation,
                StoreObservation::Absent { evidence, .. } if *evidence == expected_evidence
            ),
            "expected Absent with {expected_evidence:?}, got {:?}",
            entry.observation
        );
    }

    #[test]
    fn gate_starts_fenced_and_requires_epoch_after_resync() {
        let service = service_kind!("World");
        let other_service = service_kind!("Other");
        let actor = actor_kind!("World");
        let route_key = RouteKey::U64(7);
        let placement = OwnershipPlacement::Explicit;
        let snapshot = LocalOwnershipSnapshot::new(
            service.clone(),
            InstanceId::new("world-a"),
            InstanceIncarnation::new("world-a-boot"),
        );
        let gate = snapshot.gate();

        let cold = gate
            .authorize(request(
                &service,
                &actor,
                &actor,
                &route_key,
                Some(Epoch(3)),
                &placement,
            ))
            .unwrap_err();
        assert_eq!(cold.kind, OwnershipRejectionKind::Fenced);
        assert_eq!(
            cold.reason,
            OwnershipRejectionReason::SnapshotUnavailable {
                reason: OwnershipFenceReason::Initializing
            }
        );

        install_ready_instance(&snapshot, INSTANCE_LEASE);
        resync(&snapshot, INSTANCE_LEASE, Vec::new());
        snapshot
            .apply_actor(
                OwnershipRevision(1),
                actor_record(7, "world-a", 3, INSTANCE_LEASE, PlacementState::Running),
            )
            .unwrap();

        let wrong_service = gate
            .authorize(request(
                &other_service,
                &actor,
                &actor,
                &route_key,
                Some(Epoch(3)),
                &placement,
            ))
            .unwrap_err();
        assert_eq!(wrong_service.kind, OwnershipRejectionKind::NotOwner);
        assert!(matches!(
            wrong_service.reason,
            OwnershipRejectionReason::ServiceMismatch { .. }
        ));

        let missing_epoch = gate
            .authorize(request(
                &service, &actor, &actor, &route_key, None, &placement,
            ))
            .unwrap_err();
        assert_eq!(missing_epoch.kind, OwnershipRejectionKind::Fenced);
        assert_eq!(missing_epoch.reason, OwnershipRejectionReason::MissingEpoch);
    }

    #[test]
    fn explicit_gate_revokes_grant_on_owner_change() {
        let (snapshot, gate) = ready_snapshot(8);
        snapshot
            .apply_actor(
                OwnershipRevision(1),
                actor_record(7, "world-a", 3, INSTANCE_LEASE, PlacementState::Running),
            )
            .unwrap();
        let service = service_kind!("World");
        let actor = actor_kind!("World");
        let route_key = RouteKey::U64(7);
        let placement = OwnershipPlacement::Explicit;

        let grant = gate
            .authorize(request(
                &service,
                &actor,
                &actor,
                &route_key,
                Some(Epoch(3)),
                &placement,
            ))
            .unwrap();
        assert!(gate.is_current(&grant));

        snapshot
            .apply_actor(
                OwnershipRevision(2),
                actor_record(7, "world-b", 4, LeaseId(12), PlacementState::Running),
            )
            .unwrap();
        assert!(
            !snapshot
                .apply_actor(
                    OwnershipRevision(1),
                    actor_record(7, "world-a", 3, INSTANCE_LEASE, PlacementState::Running,),
                )
                .unwrap()
        );
        assert!(!gate.is_current(&grant));
        let rejected = gate
            .authorize(request(
                &service,
                &actor,
                &actor,
                &route_key,
                Some(Epoch(3)),
                &placement,
            ))
            .unwrap_err();
        assert_eq!(rejected.kind, OwnershipRejectionKind::NotOwner);
        assert_eq!(rejected.reason, OwnershipRejectionReason::PlacementMissing);

        assert!(
            !snapshot
                .apply_actor(
                    OwnershipRevision(4),
                    actor_record(8, "world-b", 4, LeaseId(12), PlacementState::Running),
                )
                .unwrap()
        );
        assert!(
            !snapshot
                .apply_actor(
                    OwnershipRevision(3),
                    actor_record(8, "world-a", 3, INSTANCE_LEASE, PlacementState::Running,),
                )
                .unwrap()
        );
        let absent_route = RouteKey::U64(8);
        let absent_rejected = gate
            .authorize(request(
                &service,
                &actor,
                &actor,
                &absent_route,
                Some(Epoch(3)),
                &placement,
            ))
            .unwrap_err();
        assert_eq!(absent_rejected.kind, OwnershipRejectionKind::NotOwner);
        assert_eq!(
            absent_rejected.reason,
            OwnershipRejectionReason::PlacementMissing
        );
    }

    #[test]
    fn explicit_gate_rejects_target_state_lease_and_epoch_mismatches() {
        let (snapshot, gate) = ready_snapshot(8);
        let service = service_kind!("World");
        let actor = actor_kind!("World");
        let other_actor = actor_kind!("Other");
        let route_key = RouteKey::U64(7);
        let placement = OwnershipPlacement::Explicit;
        snapshot
            .apply_actor(
                OwnershipRevision(1),
                actor_record(7, "world-a", 3, INSTANCE_LEASE, PlacementState::Running),
            )
            .unwrap();

        let wrong_kind = gate
            .authorize(request(
                &service,
                &actor,
                &other_actor,
                &route_key,
                Some(Epoch(3)),
                &placement,
            ))
            .unwrap_err();
        assert_eq!(wrong_kind.kind, OwnershipRejectionKind::NotOwner);
        assert!(matches!(
            wrong_kind.reason,
            OwnershipRejectionReason::ActorKindMismatch { .. }
        ));

        let stale = gate
            .authorize(request(
                &service,
                &actor,
                &actor,
                &route_key,
                Some(Epoch(2)),
                &placement,
            ))
            .unwrap_err();
        assert_eq!(stale.kind, OwnershipRejectionKind::Fenced);
        assert_eq!(stale.reason, OwnershipRejectionReason::EpochMismatch);
        assert_eq!(stale.current_epoch, Some(Epoch(3)));

        snapshot
            .apply_actor(
                OwnershipRevision(2),
                actor_record(7, "world-a", 3, INSTANCE_LEASE, PlacementState::Migrating),
            )
            .unwrap();
        let migrating = gate
            .authorize(request(
                &service,
                &actor,
                &actor,
                &route_key,
                Some(Epoch(3)),
                &placement,
            ))
            .unwrap_err();
        assert_eq!(
            migrating.reason,
            OwnershipRejectionReason::PlacementNotRunning {
                state: PlacementState::Migrating
            }
        );

        snapshot
            .apply_actor(
                OwnershipRevision(3),
                actor_record(7, "world-a", 4, LeaseId(99), PlacementState::Running),
            )
            .unwrap();
        let invalid_lease = gate
            .authorize(request(
                &service,
                &actor,
                &actor,
                &route_key,
                Some(Epoch(4)),
                &placement,
            ))
            .unwrap_err();
        assert_eq!(
            invalid_lease.reason,
            OwnershipRejectionReason::OwnerLeaseInvalid {
                lease_id: LeaseId(99)
            }
        );

        assert!(snapshot.update_instance_state(INSTANCE_LEASE, InstanceState::Draining));
        let draining_instance = gate
            .authorize(request(
                &service,
                &actor,
                &actor,
                &route_key,
                Some(Epoch(3)),
                &placement,
            ))
            .unwrap_err();
        assert_eq!(
            draining_instance.reason,
            OwnershipRejectionReason::InstanceNotReady {
                state: InstanceState::Draining
            }
        );
    }

    #[test]
    fn virtual_shard_gate_uses_local_assignment_and_instance_lease() {
        let (snapshot, gate) = ready_snapshot(8);
        let service = service_kind!("World");
        let actor = actor_kind!("Player");
        let route_key = RouteKey::U64(42);
        let mapper = VirtualShardMapper::new(16).unwrap();
        let placement = OwnershipPlacement::VirtualShard { mapper };
        let shard_id = mapper.shard_for_route_key(&route_key);
        snapshot
            .apply_virtual_shard(
                OwnershipRevision(1),
                VirtualShardPlacementRecord {
                    service_kind: service.clone(),
                    actor_kind: actor.clone(),
                    shard_id,
                    owner: InstanceId::new("world-a"),
                    epoch: Epoch(5),
                },
            )
            .unwrap();

        let grant = gate
            .authorize(request(
                &service,
                &actor,
                &actor,
                &route_key,
                Some(Epoch(5)),
                &placement,
            ))
            .unwrap();
        assert!(matches!(grant.key(), OwnershipKey::VirtualShard(_)));
        assert_eq!(grant.lease_id(), INSTANCE_LEASE);
    }

    #[test]
    fn singleton_gate_requires_string_scope_running_state_and_live_owner_lease() {
        let (snapshot, gate) = ready_snapshot(8);
        let service = service_kind!("World");
        let actor = actor_kind!("Season");
        let route_key = RouteKey::Str("global".to_string());
        let placement = OwnershipPlacement::Singleton;
        let singleton_lease = LeaseId(21);
        snapshot
            .set_owner_lease_valid(singleton_lease, true)
            .unwrap();
        snapshot
            .apply_singleton(
                OwnershipRevision(1),
                SingletonPlacementRecord {
                    service_kind: service.clone(),
                    singleton_kind: actor.clone(),
                    scope: "global".to_string(),
                    owner: InstanceId::new("world-a"),
                    owner_incarnation: InstanceIncarnation::new("world-a-boot"),
                    epoch: Epoch(9),
                    lease_id: singleton_lease,
                    state: PlacementState::Running,
                },
            )
            .unwrap();

        let grant = gate
            .authorize(request(
                &service,
                &actor,
                &actor,
                &route_key,
                Some(Epoch(9)),
                &placement,
            ))
            .unwrap();
        assert_eq!(grant.lease_id(), singleton_lease);

        snapshot
            .set_owner_lease_valid(singleton_lease, false)
            .unwrap();
        let fenced = gate
            .authorize(request(
                &service,
                &actor,
                &actor,
                &route_key,
                Some(Epoch(9)),
                &placement,
            ))
            .unwrap_err();
        assert_eq!(
            fenced.reason,
            OwnershipRejectionReason::OwnerLeaseInvalid {
                lease_id: singleton_lease
            }
        );

        let numeric_scope = RouteKey::U64(1);
        let invalid_route = gate
            .authorize(request(
                &service,
                &actor,
                &actor,
                &numeric_scope,
                Some(Epoch(9)),
                &placement,
            ))
            .unwrap_err();
        assert_eq!(
            invalid_route.reason,
            OwnershipRejectionReason::InvalidSingletonRoute
        );
    }

    #[test]
    fn singleton_gate_rejects_a_record_from_a_previous_owner_boot() {
        let (snapshot, gate) = ready_snapshot(4);
        let singleton_lease = LeaseId(22);
        snapshot
            .set_owner_lease_valid(singleton_lease, true)
            .unwrap();
        snapshot
            .apply_singleton(
                OwnershipRevision(1),
                SingletonPlacementRecord {
                    service_kind: service_kind!("World"),
                    singleton_kind: actor_kind!("Season"),
                    scope: "global".to_string(),
                    owner: InstanceId::new("world-a"),
                    owner_incarnation: InstanceIncarnation::new("world-a-previous-boot"),
                    epoch: Epoch(10),
                    lease_id: singleton_lease,
                    state: PlacementState::Running,
                },
            )
            .unwrap();

        let rejection = gate
            .authorize(request(
                &service_kind!("World"),
                &actor_kind!("Season"),
                &actor_kind!("Season"),
                &RouteKey::Str("global".to_string()),
                Some(Epoch(10)),
                &OwnershipPlacement::Singleton,
            ))
            .unwrap_err();
        assert_eq!(
            rejection.reason,
            OwnershipRejectionReason::OwnerIncarnationMismatch
        );
    }

    #[test]
    fn lease_loss_and_capacity_overflow_fence_the_snapshot() {
        let (snapshot, gate) = ready_snapshot(1);
        snapshot
            .apply_actor(
                OwnershipRevision(1),
                actor_record(7, "world-a", 3, INSTANCE_LEASE, PlacementState::Running),
            )
            .unwrap();
        let overflow = snapshot
            .apply_virtual_shard(
                OwnershipRevision(2),
                VirtualShardPlacementRecord {
                    service_kind: service_kind!("World"),
                    actor_kind: actor_kind!("Player"),
                    shard_id: crate::sharding::VirtualShardId(0),
                    owner: InstanceId::new("world-a"),
                    epoch: Epoch(1),
                },
            )
            .unwrap_err();
        assert_eq!(
            overflow,
            OwnershipSnapshotError::CapacityExceeded { max_entries: 1 }
        );

        let service = service_kind!("World");
        let actor = actor_kind!("World");
        let route_key = RouteKey::U64(7);
        let placement = OwnershipPlacement::Explicit;
        let capacity_fence = gate
            .authorize(request(
                &service,
                &actor,
                &actor,
                &route_key,
                Some(Epoch(3)),
                &placement,
            ))
            .unwrap_err();
        assert_eq!(
            capacity_fence.reason,
            OwnershipRejectionReason::SnapshotUnavailable {
                reason: OwnershipFenceReason::CapacityExceeded
            }
        );

        resync(
            &snapshot,
            INSTANCE_LEASE,
            vec![snapshot_actor(
                3,
                3,
                actor_record(7, "world-a", 3, INSTANCE_LEASE, PlacementState::Running),
            )],
        );
        snapshot
            .set_owner_lease_valid(INSTANCE_LEASE, false)
            .unwrap();
        let lease_fence = gate
            .authorize(request(
                &service,
                &actor,
                &actor,
                &route_key,
                Some(Epoch(3)),
                &placement,
            ))
            .unwrap_err();
        assert_eq!(
            lease_fence.reason,
            OwnershipRejectionReason::SnapshotUnavailable {
                reason: OwnershipFenceReason::LeaseLost
            }
        );

        install_ready_instance(&snapshot, LeaseId(12));
        assert_eq!(
            snapshot.set_owner_lease_valid(INSTANCE_LEASE, true),
            Ok(true)
        );
        let stale_view = gate
            .authorize(request(
                &service,
                &actor,
                &actor,
                &route_key,
                Some(Epoch(3)),
                &placement,
            ))
            .unwrap_err();
        assert_eq!(
            stale_view.reason,
            OwnershipRejectionReason::SnapshotUnavailable {
                reason: OwnershipFenceReason::Initializing
            }
        );
    }

    #[test]
    fn resync_rejects_concurrent_updates_and_bounds_all_scanned_records() {
        let (snapshot, gate) = ready_snapshot(2);
        let token = snapshot.begin_resync(INSTANCE_LEASE).unwrap();
        snapshot
            .apply_actor(
                OwnershipRevision(1),
                actor_record(7, "world-a", 3, INSTANCE_LEASE, PlacementState::Running),
            )
            .unwrap();

        let stale_resync = snapshot
            .replace_from_resync(
                token,
                OwnershipRevision(1),
                std::iter::empty::<OwnershipSnapshotRecord>(),
            )
            .unwrap_err();
        assert!(matches!(
            stale_resync,
            OwnershipSnapshotError::StaleResync { .. }
        ));
        let service = service_kind!("World");
        let actor = actor_kind!("World");
        let route_key = RouteKey::U64(7);
        let placement = OwnershipPlacement::Explicit;
        let fenced = gate
            .authorize(request(
                &service,
                &actor,
                &actor,
                &route_key,
                Some(Epoch(3)),
                &placement,
            ))
            .unwrap_err();
        assert_eq!(
            fenced.reason,
            OwnershipRejectionReason::SnapshotUnavailable {
                reason: OwnershipFenceReason::Resyncing
            }
        );

        let bounded = LocalOwnershipSnapshot::with_config(
            service_kind!("World"),
            InstanceId::new("world-a"),
            InstanceIncarnation::new("world-a-boot"),
            LocalOwnershipSnapshotConfig::try_new(1).unwrap(),
        );
        install_ready_instance(&bounded, INSTANCE_LEASE);
        let token = bounded.begin_resync(INSTANCE_LEASE).unwrap();
        let overflow = bounded
            .replace_from_resync(
                token,
                OwnershipRevision(1),
                [
                    snapshot_actor(
                        1,
                        1,
                        actor_record(1, "world-b", 1, LeaseId(20), PlacementState::Running),
                    ),
                    snapshot_actor(
                        1,
                        1,
                        actor_record(2, "world-b", 1, LeaseId(20), PlacementState::Running),
                    ),
                ],
            )
            .unwrap_err();
        assert_eq!(
            overflow,
            OwnershipSnapshotError::CapacityExceeded { max_entries: 1 }
        );
    }

    #[test]
    fn watch_batch_applies_every_same_revision_event_and_invalidates_once() {
        let (snapshot, gate) = ready_snapshot(8);
        let existing = actor_record(2, "world-a", 1, INSTANCE_LEASE, PlacementState::Running);
        resync(
            &snapshot,
            INSTANCE_LEASE,
            vec![snapshot_actor(1, 1, existing)],
        );

        let service = service_kind!("World");
        let actor_kind = actor_kind!("World");
        let explicit = OwnershipPlacement::Explicit;
        let old_route = RouteKey::U64(2);
        let old_grant = gate
            .authorize(request(
                &service,
                &actor_kind,
                &actor_kind,
                &old_route,
                Some(Epoch(1)),
                &explicit,
            ))
            .unwrap();

        let actor = actor_record(1, "world-a", 3, INSTANCE_LEASE, PlacementState::Running);
        let actor_placement_key = actor_key(&actor);
        let mapper = VirtualShardMapper::new(16).unwrap();
        let shard_route = RouteKey::U64(42);
        let shard = VirtualShardPlacementRecord {
            service_kind: service.clone(),
            actor_kind: actor_kind!("Player"),
            shard_id: mapper.shard_for_route_key(&shard_route),
            owner: InstanceId::new("world-a"),
            epoch: Epoch(4),
        };
        let shard_key = virtual_shard_key(&shard);
        let singleton = SingletonPlacementRecord {
            service_kind: service.clone(),
            singleton_kind: actor_kind!("Season"),
            scope: "global".to_string(),
            owner: InstanceId::new("world-a"),
            owner_incarnation: InstanceIncarnation::new("world-a-boot"),
            epoch: Epoch(5),
            lease_id: INSTANCE_LEASE,
            state: PlacementState::Running,
        };
        let singleton_key = singleton_key(&singleton);
        let deleted_actor = actor_record(2, "world-a", 1, INSTANCE_LEASE, PlacementState::Running);
        let before_generation = snapshot.inner.read_state().generation;

        assert!(
            snapshot
                .apply_watch_batch(watch_batch(
                    2,
                    vec![
                        actor_upsert(2, actor_placement_key.clone(), actor),
                        actor_delete(2, 1, actor_key(&deleted_actor), deleted_actor),
                        virtual_shard_upsert(2, shard_key.clone(), shard),
                        singleton_upsert(2, singleton_key.clone(), singleton),
                    ],
                ))
                .unwrap()
        );

        let state = snapshot.inner.read_state();
        assert_eq!(state.revision, Some(OwnershipRevision(2)));
        assert_eq!(
            state.generation,
            before_generation.wrapping_add(1),
            "one backend transaction invalidates grants once"
        );
        assert_present_local(&state.actors[&actor_placement_key]);
        assert_absent(
            &state.actors[&actor_key(&actor_record(
                2,
                "world-a",
                1,
                INSTANCE_LEASE,
                PlacementState::Running,
            ))],
            AbsenceEvidence::ExactDelete,
        );
        assert_present_local(&state.virtual_shards[&shard_key]);
        assert_present_local(&state.singletons[&singleton_key]);
        drop(state);
        assert!(!gate.is_current(&old_grant));

        let actor_route = RouteKey::U64(1);
        gate.authorize(request(
            &service,
            &actor_kind,
            &actor_kind,
            &actor_route,
            Some(Epoch(3)),
            &explicit,
        ))
        .unwrap();
        let player = actor_kind!("Player");
        let shard_placement = OwnershipPlacement::VirtualShard { mapper };
        gate.authorize(request(
            &service,
            &player,
            &player,
            &shard_route,
            Some(Epoch(4)),
            &shard_placement,
        ))
        .unwrap();
        let season = actor_kind!("Season");
        let singleton_route = RouteKey::Str("global".to_string());
        gate.authorize(request(
            &service,
            &season,
            &season,
            &singleton_route,
            Some(Epoch(5)),
            &OwnershipPlacement::Singleton,
        ))
        .unwrap();
    }

    #[test]
    fn watch_batch_remote_updates_and_deletes_revoke_all_local_grants() {
        let (snapshot, gate) = ready_snapshot(8);
        let service = service_kind!("World");
        let world = actor_kind!("World");
        let actor = actor_record(1, "world-a", 1, INSTANCE_LEASE, PlacementState::Running);
        let actor_key = actor_key(&actor);
        let mapper = VirtualShardMapper::new(8).unwrap();
        let shard_route = RouteKey::U64(9);
        let shard = VirtualShardPlacementRecord {
            service_kind: service.clone(),
            actor_kind: actor_kind!("Player"),
            shard_id: mapper.shard_for_route_key(&shard_route),
            owner: InstanceId::new("world-a"),
            epoch: Epoch(1),
        };
        let shard_key = virtual_shard_key(&shard);
        let singleton = SingletonPlacementRecord {
            service_kind: service.clone(),
            singleton_kind: actor_kind!("Season"),
            scope: "global".to_string(),
            owner: InstanceId::new("world-a"),
            owner_incarnation: InstanceIncarnation::new("world-a-boot"),
            epoch: Epoch(1),
            lease_id: INSTANCE_LEASE,
            state: PlacementState::Running,
        };
        let singleton_key = singleton_key(&singleton);
        resync(
            &snapshot,
            INSTANCE_LEASE,
            vec![
                snapshot_actor(1, 1, actor.clone()),
                snapshot_virtual_shard(1, 1, shard.clone()),
                snapshot_singleton(1, 1, singleton.clone()),
            ],
        );
        let actor_route = RouteKey::U64(1);
        let actor_grant = gate
            .authorize(request(
                &service,
                &world,
                &world,
                &actor_route,
                Some(Epoch(1)),
                &OwnershipPlacement::Explicit,
            ))
            .unwrap();
        let player = actor_kind!("Player");
        let shard_grant = gate
            .authorize(request(
                &service,
                &player,
                &player,
                &shard_route,
                Some(Epoch(1)),
                &OwnershipPlacement::VirtualShard { mapper },
            ))
            .unwrap();
        let season = actor_kind!("Season");
        let singleton_route = RouteKey::Str("global".to_string());
        let singleton_grant = gate
            .authorize(request(
                &service,
                &season,
                &season,
                &singleton_route,
                Some(Epoch(1)),
                &OwnershipPlacement::Singleton,
            ))
            .unwrap();
        let before_generation = snapshot.inner.read_state().generation;

        let mut remote_actor = actor;
        remote_actor.owner = InstanceId::new("world-b");
        remote_actor.epoch = Epoch(2);
        remote_actor.lease_id = LeaseId(22);
        let mut remote_singleton = singleton;
        remote_singleton.owner = InstanceId::new("world-b");
        remote_singleton.epoch = Epoch(2);
        remote_singleton.lease_id = LeaseId(22);
        snapshot
            .apply_watch_batch(watch_batch(
                2,
                vec![
                    actor_upsert(2, actor_key.clone(), remote_actor),
                    virtual_shard_delete(2, 1, shard_key.clone(), shard),
                    singleton_upsert(2, singleton_key.clone(), remote_singleton),
                ],
            ))
            .unwrap();

        let state = snapshot.inner.read_state();
        assert_present_remote(&state.actors[&actor_key]);
        assert_absent(
            &state.virtual_shards[&shard_key],
            AbsenceEvidence::ExactDelete,
        );
        assert_present_remote(&state.singletons[&singleton_key]);
        assert_eq!(state.generation, before_generation.wrapping_add(1));
        drop(state);
        assert!(!gate.is_current(&actor_grant));
        assert!(!gate.is_current(&shard_grant));
        assert!(!gate.is_current(&singleton_grant));
    }

    #[test]
    fn invalid_and_over_capacity_watch_batches_commit_no_records_and_fence() {
        let (mismatched, _) = ready_snapshot(2);
        let record = actor_record(2, "world-a", 1, INSTANCE_LEASE, PlacementState::Running);
        let mismatch = mismatched
            .apply_watch_batch(watch_batch(
                1,
                vec![actor_upsert(
                    1,
                    actor_key(&actor_record(
                        1,
                        "world-a",
                        1,
                        INSTANCE_LEASE,
                        PlacementState::Running,
                    )),
                    record,
                )],
            ))
            .unwrap_err();
        assert!(matches!(
            mismatch,
            OwnershipSnapshotError::PlacementKeyMismatch { .. }
        ));
        let state = mismatched.inner.read_state();
        assert_eq!(state.revision, Some(OwnershipRevision(0)));
        assert!(state.actors.is_empty());
        assert_eq!(
            state.availability,
            SnapshotAvailability::Unavailable(OwnershipFenceReason::WatchLost)
        );
        drop(state);

        let (duplicate, _) = ready_snapshot(2);
        let record = actor_record(1, "world-a", 1, INSTANCE_LEASE, PlacementState::Running);
        let key = actor_key(&record);
        assert!(matches!(
            duplicate.apply_watch_batch(watch_batch(
                1,
                vec![
                    actor_upsert(1, key.clone(), record.clone()),
                    actor_delete(1, 0, key, record),
                ],
            )),
            Err(OwnershipSnapshotError::DuplicatePlacementEvent { .. })
        ));
        assert!(duplicate.inner.read_state().actors.is_empty());

        let (duplicate_instance, _) = ready_snapshot(2);
        assert_eq!(
            duplicate_instance.apply_watch_batch(watch_batch(
                1,
                vec![
                    OwnershipWatchEvent::InstanceUpserted {
                        record: instance_record(
                            "World",
                            "world-a",
                            INSTANCE_LEASE,
                            InstanceState::Ready,
                        ),
                    },
                    OwnershipWatchEvent::InstanceDeleted {
                        record: instance_record(
                            "World",
                            "world-a",
                            INSTANCE_LEASE,
                            InstanceState::Ready,
                        ),
                    },
                ],
            )),
            Err(OwnershipSnapshotError::DuplicateInstanceEvent)
        );
        let state = duplicate_instance.inner.read_state();
        assert_eq!(state.revision, Some(OwnershipRevision(0)));
        assert!(state.instance.is_some());
        assert_eq!(
            state.availability,
            SnapshotAvailability::Unavailable(OwnershipFenceReason::WatchLost)
        );
        drop(state);

        let (bounded, _) = ready_snapshot(2);
        let existing = actor_record(1, "world-a", 1, INSTANCE_LEASE, PlacementState::Running);
        resync(
            &bounded,
            INSTANCE_LEASE,
            vec![snapshot_actor(1, 1, existing.clone())],
        );
        let new_actor = actor_record(2, "world-a", 1, INSTANCE_LEASE, PlacementState::Running);
        let shard = VirtualShardPlacementRecord {
            service_kind: service_kind!("World"),
            actor_kind: actor_kind!("Player"),
            shard_id: crate::sharding::VirtualShardId(0),
            owner: InstanceId::new("world-a"),
            epoch: Epoch(1),
        };
        let before_generation = bounded.inner.read_state().generation;
        assert_eq!(
            bounded.apply_watch_batch(watch_batch(
                2,
                vec![
                    actor_upsert(2, actor_key(&new_actor), new_actor),
                    virtual_shard_upsert(2, virtual_shard_key(&shard), shard),
                ],
            )),
            Err(OwnershipSnapshotError::CapacityExceeded { max_entries: 2 })
        );
        let state = bounded.inner.read_state();
        assert_eq!(state.revision, Some(OwnershipRevision(1)));
        assert_eq!(state.actors.len(), 1);
        assert_present_local(&state.actors[&actor_key(&existing)]);
        assert!(state.virtual_shards.is_empty());
        assert_eq!(state.generation, before_generation.wrapping_add(1));
        assert_eq!(
            state.availability,
            SnapshotAvailability::Unavailable(OwnershipFenceReason::CapacityExceeded)
        );
    }

    #[test]
    fn opaque_floor_proofs_are_record_context_and_delete_previous_bound() {
        let (watch_snapshot, _) = ready_snapshot(1);
        let proven = actor_record(1, "world-a", 1, INSTANCE_LEASE, PlacementState::Running);
        let proof = test_proof(
            OwnershipProofContext::Upsert,
            1,
            1,
            &OwnershipRecordBinding::Actor(proven.clone()),
        );
        let mut altered = proven.clone();
        altered.state = PlacementState::Stopped;
        assert!(matches!(
            watch_snapshot.apply_watch_batch(watch_batch(
                1,
                vec![OwnershipWatchEvent::ActorUpserted {
                    key: actor_key(&altered),
                    record: altered,
                    proof,
                }],
            )),
            Err(OwnershipSnapshotError::Proof {
                error: OwnershipProofError::RecordBindingMismatch { .. }
            })
        ));
        let state = watch_snapshot.inner.read_state();
        assert_eq!(state.revision, Some(OwnershipRevision(0)));
        assert!(state.actors.is_empty());
        assert_eq!(
            state.availability,
            SnapshotAvailability::Unavailable(OwnershipFenceReason::WatchLost)
        );
        drop(state);

        let snapshot = LocalOwnershipSnapshot::with_config(
            service_kind!("World"),
            InstanceId::new("world-a"),
            InstanceIncarnation::new("world-a-boot"),
            LocalOwnershipSnapshotConfig::try_new(1).unwrap(),
        );
        install_ready_instance(&snapshot, INSTANCE_LEASE);
        let token = snapshot.begin_resync(INSTANCE_LEASE).unwrap();
        let proof = test_proof(
            OwnershipProofContext::Snapshot,
            1,
            1,
            &OwnershipRecordBinding::Actor(proven.clone()),
        );
        let mut altered = proven.clone();
        altered.owner = InstanceId::new("world-b");
        assert!(matches!(
            snapshot.replace_from_resync(
                token,
                OwnershipRevision(1),
                vec![OwnershipSnapshotRecord::Actor {
                    version: OwnershipRevision(1),
                    record: altered,
                    proof,
                }],
            ),
            Err(OwnershipSnapshotError::Proof {
                error: OwnershipProofError::RecordBindingMismatch { .. }
            })
        ));
        assert!(snapshot.inner.read_state().actors.is_empty());

        let (deleted, _) = ready_snapshot(1);
        deleted
            .apply_watch_batch(watch_batch(
                1,
                vec![actor_upsert(1, actor_key(&proven), proven.clone())],
            ))
            .unwrap();
        let mut wrong_previous = proven;
        wrong_previous.lease_id = LeaseId(99);
        assert!(matches!(
            deleted.apply_watch_batch(watch_batch(
                2,
                vec![actor_delete(
                    2,
                    1,
                    actor_key(&wrong_previous),
                    wrong_previous,
                )],
            )),
            Err(OwnershipSnapshotError::Proof {
                error: OwnershipProofError::DeletePreviousMismatch { .. }
            })
        ));
        let state = deleted.inner.read_state();
        assert_eq!(state.revision, Some(OwnershipRevision(1)));
        assert_present_local(state.actors.values().next().unwrap());
    }

    #[test]
    fn unrelated_stale_and_resync_watch_batches_preserve_global_ordering() {
        let (snapshot, gate) = ready_snapshot(2);
        let actor = actor_record(1, "world-a", 1, INSTANCE_LEASE, PlacementState::Running);
        snapshot
            .apply_actor(OwnershipRevision(1), actor.clone())
            .unwrap();
        let service = service_kind!("World");
        let kind = actor_kind!("World");
        let route = RouteKey::U64(1);
        let grant = gate
            .authorize(request(
                &service,
                &kind,
                &kind,
                &route,
                Some(Epoch(1)),
                &OwnershipPlacement::Explicit,
            ))
            .unwrap();
        let before_generation = snapshot.inner.read_state().generation;
        let unrelated = ActorPlacementRecord {
            service_kind: service_kind!("Other"),
            actor_kind: actor_kind!("Other"),
            actor_id: ActorId::U64(7),
            owner: InstanceId::new("other-a"),
            epoch: Epoch(1),
            lease_id: LeaseId(77),
            state: PlacementState::Running,
        };
        snapshot
            .apply_watch_batch(watch_batch(
                5,
                vec![
                    OwnershipWatchEvent::InstanceUpserted {
                        record: instance_record(
                            "World",
                            "world-b",
                            LeaseId(22),
                            InstanceState::Ready,
                        ),
                    },
                    actor_upsert(5, actor_key(&unrelated), unrelated.clone()),
                ],
            ))
            .unwrap();
        let state = snapshot.inner.read_state();
        assert_eq!(state.revision, Some(OwnershipRevision(5)));
        assert_eq!(state.generation, before_generation);
        drop(state);
        assert!(gate.is_current(&grant));

        assert!(
            !snapshot
                .apply_watch_batch(watch_batch(5, Vec::new()))
                .unwrap()
        );
        assert!(gate.is_current(&grant));

        let token = snapshot.begin_resync(INSTANCE_LEASE).unwrap();
        let resync_generation = snapshot.inner.read_state().generation;
        snapshot
            .apply_watch_batch(watch_batch(
                8,
                vec![actor_upsert(8, actor_key(&unrelated), unrelated)],
            ))
            .unwrap();
        let state = snapshot.inner.read_state();
        assert_eq!(state.generation, resync_generation.wrapping_add(1));
        assert_eq!(
            state.availability,
            SnapshotAvailability::Unavailable(OwnershipFenceReason::Resyncing)
        );
        drop(state);
        assert!(matches!(
            snapshot.replace_from_resync(
                token,
                OwnershipRevision(8),
                std::iter::empty::<OwnershipSnapshotRecord>(),
            ),
            Err(OwnershipSnapshotError::StaleResync { .. })
        ));

        let before_empty = snapshot.inner.read_state().generation;
        assert_eq!(
            snapshot.apply_watch_batch(watch_batch(9, Vec::new())),
            Err(OwnershipSnapshotError::EmptyWatchBatch)
        );
        let state = snapshot.inner.read_state();
        assert_eq!(state.revision, Some(OwnershipRevision(8)));
        assert_eq!(state.generation, before_empty.wrapping_add(1));
        assert_eq!(
            state.availability,
            SnapshotAvailability::Unavailable(OwnershipFenceReason::WatchLost)
        );
    }

    #[test]
    fn instance_events_preserve_explicit_lease_validation_and_fence_deletes() {
        let (snapshot, gate) = ready_snapshot(2);
        let actor = actor_record(1, "world-a", 1, INSTANCE_LEASE, PlacementState::Running);
        snapshot
            .apply_watch_batch(watch_batch(
                1,
                vec![
                    OwnershipWatchEvent::InstanceUpserted {
                        record: instance_record(
                            "World",
                            "world-a",
                            INSTANCE_LEASE,
                            InstanceState::Draining,
                        ),
                    },
                    actor_upsert(1, actor_key(&actor), actor),
                ],
            ))
            .unwrap();
        let state = snapshot.inner.read_state();
        assert_eq!(state.actors.len(), 1);
        assert!(state.valid_owner_leases.contains(&INSTANCE_LEASE));
        assert_eq!(
            state.instance.as_ref().unwrap().state,
            InstanceState::Draining
        );
        drop(state);

        let service = service_kind!("World");
        let kind = actor_kind!("World");
        let route = RouteKey::U64(1);
        let rejected = gate
            .authorize(request(
                &service,
                &kind,
                &kind,
                &route,
                Some(Epoch(1)),
                &OwnershipPlacement::Explicit,
            ))
            .unwrap_err();
        assert_eq!(
            rejected.reason,
            OwnershipRejectionReason::InstanceNotReady {
                state: InstanceState::Draining
            }
        );

        let new_lease = LeaseId(22);
        snapshot
            .apply_watch_batch(watch_batch(
                2,
                vec![OwnershipWatchEvent::InstanceUpserted {
                    record: instance_record("World", "world-a", new_lease, InstanceState::Ready),
                }],
            ))
            .unwrap();
        let state = snapshot.inner.read_state();
        assert_eq!(state.actors.len(), 1);
        assert_lifecycle_fenced(state.actors.values().next().unwrap());
        assert!(state.valid_owner_leases.is_empty());
        assert_eq!(state.instance.as_ref().unwrap().lease_id, new_lease);
        assert_eq!(
            state.availability,
            SnapshotAvailability::Unavailable(OwnershipFenceReason::Initializing)
        );
        drop(state);

        snapshot.set_owner_lease_valid(new_lease, true).unwrap();
        let token = snapshot.begin_resync(new_lease).unwrap();
        snapshot
            .replace_from_resync(
                token,
                OwnershipRevision(2),
                std::iter::empty::<OwnershipSnapshotRecord>(),
            )
            .unwrap();
        let unknown_lease_actor =
            actor_record(2, "world-a", 1, LeaseId(99), PlacementState::Running);
        snapshot
            .apply_watch_batch(watch_batch(
                3,
                vec![actor_upsert(
                    3,
                    actor_key(&unknown_lease_actor),
                    unknown_lease_actor,
                )],
            ))
            .unwrap();
        let route = RouteKey::U64(2);
        let rejected = gate
            .authorize(request(
                &service,
                &kind,
                &kind,
                &route,
                Some(Epoch(1)),
                &OwnershipPlacement::Explicit,
            ))
            .unwrap_err();
        assert_eq!(
            rejected.reason,
            OwnershipRejectionReason::OwnerLeaseInvalid {
                lease_id: LeaseId(99)
            }
        );

        snapshot
            .apply_watch_batch(watch_batch(
                4,
                vec![OwnershipWatchEvent::InstanceDeleted {
                    record: instance_record(
                        "World",
                        "world-a",
                        INSTANCE_LEASE,
                        InstanceState::Ready,
                    ),
                }],
            ))
            .unwrap();
        let state = snapshot.inner.read_state();
        assert!(state.instance.is_none());
        assert_eq!(state.actors.len(), 2);
        assert!(
            state
                .actors
                .values()
                .all(|entry| entry.observation.local_record().is_none())
        );
        assert!(state.valid_owner_leases.is_empty());
        assert_eq!(
            state.availability,
            SnapshotAvailability::Unavailable(OwnershipFenceReason::LeaseLost)
        );

        let (matching_delete, _) = ready_snapshot(1);
        matching_delete
            .apply_watch_batch(watch_batch(
                1,
                vec![OwnershipWatchEvent::InstanceDeleted {
                    record: instance_record(
                        "World",
                        "world-a",
                        INSTANCE_LEASE,
                        InstanceState::Ready,
                    ),
                }],
            ))
            .unwrap();
        assert_eq!(
            matching_delete.inner.read_state().availability,
            SnapshotAvailability::Unavailable(OwnershipFenceReason::LeaseLost)
        );

        let (same_lease_invalid, _) = ready_snapshot(1);
        same_lease_invalid
            .set_owner_lease_valid(INSTANCE_LEASE, false)
            .unwrap();
        same_lease_invalid
            .apply_watch_batch(watch_batch(
                1,
                vec![OwnershipWatchEvent::InstanceUpserted {
                    record: instance_record(
                        "World",
                        "world-a",
                        INSTANCE_LEASE,
                        InstanceState::Ready,
                    ),
                }],
            ))
            .unwrap();
        let state = same_lease_invalid.inner.read_state();
        assert!(!state.valid_owner_leases.contains(&INSTANCE_LEASE));
        assert_eq!(
            state.availability,
            SnapshotAvailability::Unavailable(OwnershipFenceReason::LeaseLost)
        );
    }

    #[test]
    fn epoch_regression_and_equal_epoch_authority_changes_fence_without_commit() {
        let current = actor_record(1, "world-a", 5, INSTANCE_LEASE, PlacementState::Running);
        let key = actor_key(&current);
        let make_snapshot = || {
            let (snapshot, _) = ready_snapshot(2);
            resync(
                &snapshot,
                INSTANCE_LEASE,
                vec![snapshot_actor(1, 1, current.clone())],
            );
            snapshot
        };

        let regressed = make_snapshot();
        let lower = actor_record(1, "world-a", 4, INSTANCE_LEASE, PlacementState::Running);
        assert!(matches!(
            regressed.apply_watch_batch(watch_batch(2, vec![actor_upsert(2, key.clone(), lower)],)),
            Err(OwnershipSnapshotError::EpochRegression { .. })
        ));
        let state = regressed.inner.read_state();
        assert_eq!(state.revision, Some(OwnershipRevision(1)));
        assert_eq!(
            state.actors[&key].observation.local_record().unwrap().epoch,
            Epoch(5)
        );
        assert_eq!(
            state.availability,
            SnapshotAvailability::Unavailable(OwnershipFenceReason::WatchLost)
        );
        drop(state);

        let conflicted = make_snapshot();
        let equal_epoch_remote =
            actor_record(1, "world-b", 5, LeaseId(22), PlacementState::Running);
        assert!(matches!(
            conflicted.apply_watch_batch(watch_batch(
                2,
                vec![actor_upsert(2, key.clone(), equal_epoch_remote)],
            )),
            Err(OwnershipSnapshotError::EpochAuthorityConflict { .. })
        ));
        assert_present_local(&conflicted.inner.read_state().actors[&key]);
    }

    #[test]
    fn all_placement_floors_reject_lower_and_equal_conflicts_but_allow_higher_returns() {
        let moved_actor = || {
            let (snapshot, _) = ready_snapshot(1);
            snapshot
                .apply_actor(
                    OwnershipRevision(1),
                    actor_record(1, "world-a", 5, INSTANCE_LEASE, PlacementState::Running),
                )
                .unwrap();
            snapshot
                .apply_actor(
                    OwnershipRevision(2),
                    actor_record(1, "world-b", 6, LeaseId(22), PlacementState::Running),
                )
                .unwrap();
            snapshot
        };
        let actor_lower = moved_actor();
        assert!(matches!(
            actor_lower.apply_actor(
                OwnershipRevision(3),
                actor_record(1, "world-a", 5, INSTANCE_LEASE, PlacementState::Running),
            ),
            Err(OwnershipSnapshotError::EpochRegression { .. })
        ));
        assert_eq!(
            actor_lower.inner.read_state().revision,
            Some(OwnershipRevision(2))
        );

        let actor_equal = moved_actor();
        assert!(matches!(
            actor_equal.apply_actor(
                OwnershipRevision(3),
                actor_record(1, "world-a", 6, INSTANCE_LEASE, PlacementState::Running),
            ),
            Err(OwnershipSnapshotError::EpochAuthorityConflict { .. })
        ));

        let actor_higher = moved_actor();
        actor_higher
            .apply_actor(
                OwnershipRevision(3),
                actor_record(1, "world-a", 7, INSTANCE_LEASE, PlacementState::Running),
            )
            .unwrap();
        let actor_key = actor_key(&actor_record(
            1,
            "world-a",
            7,
            INSTANCE_LEASE,
            PlacementState::Running,
        ));
        assert_eq!(
            actor_higher.inner.read_state().actors[&actor_key]
                .observation
                .local_record()
                .unwrap()
                .epoch,
            Epoch(7)
        );

        let moved_shard = || {
            let (snapshot, _) = ready_snapshot(1);
            snapshot
                .apply_virtual_shard(OwnershipRevision(1), virtual_shard_record(1, "world-a", 5))
                .unwrap();
            snapshot
                .apply_virtual_shard(OwnershipRevision(2), virtual_shard_record(1, "world-b", 6))
                .unwrap();
            snapshot
        };
        let shard_lower = moved_shard();
        assert!(matches!(
            shard_lower
                .apply_virtual_shard(OwnershipRevision(3), virtual_shard_record(1, "world-a", 5),),
            Err(OwnershipSnapshotError::EpochRegression { .. })
        ));

        let shard_equal = moved_shard();
        let equal_shard = virtual_shard_record(1, "world-a", 6);
        assert!(matches!(
            shard_equal.apply_watch_batch(watch_batch(
                3,
                vec![virtual_shard_upsert(
                    3,
                    virtual_shard_key(&equal_shard),
                    equal_shard,
                )],
            )),
            Err(OwnershipSnapshotError::EpochAuthorityConflict { .. })
        ));
        assert_eq!(
            shard_equal.inner.read_state().revision,
            Some(OwnershipRevision(2))
        );

        let shard_higher = moved_shard();
        shard_higher
            .apply_virtual_shard(OwnershipRevision(3), virtual_shard_record(1, "world-a", 7))
            .unwrap();
        let shard_key = virtual_shard_key(&virtual_shard_record(1, "world-a", 7));
        assert_eq!(
            shard_higher.inner.read_state().virtual_shards[&shard_key]
                .observation
                .local_record()
                .unwrap()
                .epoch,
            Epoch(7)
        );

        let moved_singleton = || {
            let (snapshot, _) = ready_snapshot(1);
            snapshot
                .apply_singleton(
                    OwnershipRevision(1),
                    singleton_record(
                        "global",
                        "world-a",
                        5,
                        INSTANCE_LEASE,
                        PlacementState::Running,
                    ),
                )
                .unwrap();
            snapshot
                .apply_singleton(
                    OwnershipRevision(2),
                    singleton_record("global", "world-b", 6, LeaseId(22), PlacementState::Running),
                )
                .unwrap();
            snapshot
        };
        let singleton_lower = moved_singleton();
        assert!(matches!(
            singleton_lower.apply_singleton(
                OwnershipRevision(3),
                singleton_record(
                    "global",
                    "world-a",
                    5,
                    INSTANCE_LEASE,
                    PlacementState::Running,
                ),
            ),
            Err(OwnershipSnapshotError::EpochRegression { .. })
        ));

        let singleton_equal = moved_singleton();
        assert!(matches!(
            singleton_equal.apply_singleton(
                OwnershipRevision(3),
                singleton_record(
                    "global",
                    "world-a",
                    6,
                    INSTANCE_LEASE,
                    PlacementState::Running,
                ),
            ),
            Err(OwnershipSnapshotError::EpochAuthorityConflict { .. })
        ));

        let singleton_higher = moved_singleton();
        singleton_higher
            .apply_singleton(
                OwnershipRevision(3),
                singleton_record(
                    "global",
                    "world-a",
                    7,
                    INSTANCE_LEASE,
                    PlacementState::Running,
                ),
            )
            .unwrap();
        let singleton_key = singleton_key(&singleton_record(
            "global",
            "world-a",
            7,
            INSTANCE_LEASE,
            PlacementState::Running,
        ));
        assert_eq!(
            singleton_higher.inner.read_state().singletons[&singleton_key]
                .observation
                .local_record()
                .unwrap()
                .epoch,
            Epoch(7)
        );

        let (state_only, _) = ready_snapshot(1);
        state_only
            .apply_singleton(
                OwnershipRevision(1),
                singleton_record(
                    "state-only",
                    "world-a",
                    5,
                    INSTANCE_LEASE,
                    PlacementState::Running,
                ),
            )
            .unwrap();
        state_only
            .apply_singleton(
                OwnershipRevision(2),
                singleton_record(
                    "state-only",
                    "world-a",
                    5,
                    INSTANCE_LEASE,
                    PlacementState::Draining,
                ),
            )
            .unwrap();
        assert_eq!(
            state_only
                .inner
                .read_state()
                .singletons
                .values()
                .next()
                .unwrap()
                .observation
                .local_record()
                .unwrap()
                .state,
            PlacementState::Draining
        );
    }

    #[test]
    fn deleted_placements_require_higher_epochs_to_reactivate_atomically() {
        let deleted = || {
            let (snapshot, gate) = ready_snapshot(3);
            let actor = actor_record(1, "world-a", 5, INSTANCE_LEASE, PlacementState::Running);
            let shard = virtual_shard_record(1, "world-a", 5);
            let singleton = singleton_record(
                "global",
                "world-a",
                5,
                INSTANCE_LEASE,
                PlacementState::Running,
            );
            snapshot
                .apply_watch_batch(watch_batch(
                    1,
                    vec![
                        actor_upsert(1, actor_key(&actor), actor.clone()),
                        virtual_shard_upsert(1, virtual_shard_key(&shard), shard.clone()),
                        singleton_upsert(1, singleton_key(&singleton), singleton.clone()),
                    ],
                ))
                .unwrap();
            let service = service_kind!("World");
            let kind = actor_kind!("World");
            let route = RouteKey::U64(1);
            let grant = gate
                .authorize(request(
                    &service,
                    &kind,
                    &kind,
                    &route,
                    Some(Epoch(5)),
                    &OwnershipPlacement::Explicit,
                ))
                .unwrap();
            snapshot
                .apply_watch_batch(watch_batch(
                    2,
                    vec![
                        actor_delete(2, 1, actor_key(&actor), actor.clone()),
                        virtual_shard_delete(2, 1, virtual_shard_key(&shard), shard.clone()),
                        singleton_delete(2, 1, singleton_key(&singleton), singleton.clone()),
                    ],
                ))
                .unwrap();
            assert!(!gate.is_current(&grant));
            (snapshot, actor, shard, singleton)
        };

        let (same_epoch, actor, _, _) = deleted();
        assert!(matches!(
            same_epoch.apply_watch_batch(watch_batch(
                3,
                vec![actor_upsert(3, actor_key(&actor), actor)],
            )),
            Err(OwnershipSnapshotError::EpochReactivation { .. })
        ));
        let state = same_epoch.inner.read_state();
        assert_eq!(state.revision, Some(OwnershipRevision(2)));
        assert!(state.actors.values().all(|entry| matches!(
            &entry.observation,
            StoreObservation::Absent {
                evidence: AbsenceEvidence::ExactDelete,
                ..
            }
        )));
        assert!(state.virtual_shards.values().all(|entry| matches!(
            &entry.observation,
            StoreObservation::Absent {
                evidence: AbsenceEvidence::ExactDelete,
                ..
            }
        )));
        assert!(state.singletons.values().all(|entry| matches!(
            &entry.observation,
            StoreObservation::Absent {
                evidence: AbsenceEvidence::ExactDelete,
                ..
            }
        )));
        drop(state);

        let (resynced_absence, _, _, _) = deleted();
        let token = resynced_absence.begin_resync(INSTANCE_LEASE).unwrap();
        resynced_absence
            .replace_from_resync(
                token,
                OwnershipRevision(3),
                std::iter::empty::<OwnershipSnapshotRecord>(),
            )
            .unwrap();
        let state = resynced_absence.inner.read_state();
        assert!(state.actors.values().all(|entry| matches!(
            &entry.observation,
            StoreObservation::Absent {
                evidence: AbsenceEvidence::ExactDelete,
                ..
            }
        ) && entry.version == OwnershipRevision(3)));
        assert!(state.virtual_shards.values().all(|entry| matches!(
            &entry.observation,
            StoreObservation::Absent {
                evidence: AbsenceEvidence::ExactDelete,
                ..
            }
        ) && entry.version
            == OwnershipRevision(3)));
        assert!(state.singletons.values().all(|entry| matches!(
            &entry.observation,
            StoreObservation::Absent {
                evidence: AbsenceEvidence::ExactDelete,
                ..
            }
        ) && entry.version == OwnershipRevision(3)));
        drop(state);

        let (same_shard, _, shard, _) = deleted();
        assert!(matches!(
            same_shard.apply_watch_batch(watch_batch(
                3,
                vec![virtual_shard_upsert(3, virtual_shard_key(&shard), shard)],
            )),
            Err(OwnershipSnapshotError::EpochReactivation { .. })
        ));
        assert_eq!(
            same_shard.inner.read_state().revision,
            Some(OwnershipRevision(2))
        );

        let (same_singleton, _, _, singleton) = deleted();
        assert!(matches!(
            same_singleton.apply_watch_batch(watch_batch(
                3,
                vec![singleton_upsert(3, singleton_key(&singleton), singleton)],
            )),
            Err(OwnershipSnapshotError::EpochReactivation { .. })
        ));
        assert_eq!(
            same_singleton.inner.read_state().revision,
            Some(OwnershipRevision(2))
        );

        let (higher, _, _, _) = deleted();
        let actor = actor_record(1, "world-a", 6, INSTANCE_LEASE, PlacementState::Running);
        let shard = virtual_shard_record(1, "world-a", 6);
        let singleton = singleton_record(
            "global",
            "world-a",
            6,
            INSTANCE_LEASE,
            PlacementState::Running,
        );
        higher
            .apply_watch_batch(watch_batch(
                3,
                vec![
                    actor_upsert(3, actor_key(&actor), actor),
                    virtual_shard_upsert(3, virtual_shard_key(&shard), shard),
                    singleton_upsert(3, singleton_key(&singleton), singleton),
                ],
            ))
            .unwrap();
        let state = higher.inner.read_state();
        assert!(
            state
                .actors
                .values()
                .all(|entry| matches!(&entry.observation, StoreObservation::PresentLocal(_)))
        );
        assert!(
            state
                .virtual_shards
                .values()
                .all(|entry| matches!(&entry.observation, StoreObservation::PresentLocal(_)))
        );
        assert!(
            state
                .singletons
                .values()
                .all(|entry| matches!(&entry.observation, StoreObservation::PresentLocal(_)))
        );
        drop(state);

        let (legacy, _) = ready_snapshot(1);
        let actor = actor_record(9, "world-a", 4, INSTANCE_LEASE, PlacementState::Running);
        let key = OwnershipKey::Explicit(actor_key(&actor));
        legacy
            .apply_actor(OwnershipRevision(1), actor.clone())
            .unwrap();
        assert!(legacy.remove(&key, OwnershipRevision(2)));
        assert!(matches!(
            legacy.apply_actor(OwnershipRevision(3), actor),
            Err(OwnershipSnapshotError::EpochReactivation { .. })
        ));
        assert_eq!(
            legacy.inner.read_state().revision,
            Some(OwnershipRevision(2))
        );

        let (bounded_deleted, _) = ready_snapshot(1);
        let first = actor_record(1, "world-a", 1, INSTANCE_LEASE, PlacementState::Running);
        bounded_deleted
            .apply_actor(OwnershipRevision(1), first.clone())
            .unwrap();
        assert!(bounded_deleted.remove(
            &OwnershipKey::Explicit(actor_key(&first)),
            OwnershipRevision(2),
        ));
        assert_eq!(
            bounded_deleted.apply_actor(
                OwnershipRevision(3),
                actor_record(2, "world-a", 1, INSTANCE_LEASE, PlacementState::Running),
            ),
            Err(OwnershipSnapshotError::CapacityExceeded { max_entries: 1 })
        );
        let state = bounded_deleted.inner.read_state();
        assert_eq!(state.actors.len(), 1);
        assert_absent(
            state.actors.values().next().unwrap(),
            AbsenceEvidence::ExactDelete,
        );
    }

    #[test]
    fn resync_merges_remote_and_missing_records_into_monotonic_authority_floors() {
        let moved_by_resync = || {
            let (snapshot, gate) = ready_snapshot(3);
            resync(
                &snapshot,
                INSTANCE_LEASE,
                vec![
                    snapshot_actor(
                        1,
                        1,
                        actor_record(1, "world-a", 1, INSTANCE_LEASE, PlacementState::Running),
                    ),
                    snapshot_virtual_shard(1, 1, virtual_shard_record(1, "world-a", 1)),
                    snapshot_singleton(
                        1,
                        1,
                        singleton_record(
                            "global",
                            "world-a",
                            1,
                            INSTANCE_LEASE,
                            PlacementState::Running,
                        ),
                    ),
                ],
            );
            let token = snapshot.begin_resync(INSTANCE_LEASE).unwrap();
            snapshot
                .replace_from_resync(
                    token,
                    OwnershipRevision(10),
                    vec![
                        snapshot_actor(
                            10,
                            8,
                            actor_record(1, "world-b", 2, LeaseId(22), PlacementState::Running),
                        ),
                        snapshot_virtual_shard(10, 9, virtual_shard_record(1, "world-b", 2)),
                        snapshot_singleton(
                            10,
                            10,
                            singleton_record(
                                "global",
                                "world-b",
                                2,
                                LeaseId(22),
                                PlacementState::Running,
                            ),
                        ),
                    ],
                )
                .unwrap();
            (snapshot, gate)
        };

        let (lower, _) = moved_by_resync();
        let token = lower.begin_resync(INSTANCE_LEASE).unwrap();
        assert!(matches!(
            lower.replace_from_resync(
                token,
                OwnershipRevision(11),
                vec![snapshot_actor(
                    11,
                    11,
                    actor_record(1, "world-a", 1, INSTANCE_LEASE, PlacementState::Running,),
                )],
            ),
            Err(OwnershipSnapshotError::EpochRegression { .. })
        ));
        let state = lower.inner.read_state();
        assert_eq!(state.revision, Some(OwnershipRevision(10)));
        assert_eq!(
            state.actors.values().next().unwrap().authority.epoch,
            Epoch(2)
        );
        assert_present_remote(state.actors.values().next().unwrap());
        assert_eq!(
            state.availability,
            SnapshotAvailability::Unavailable(OwnershipFenceReason::Resyncing)
        );
        drop(state);

        let (equal_conflict, _) = moved_by_resync();
        let token = equal_conflict.begin_resync(INSTANCE_LEASE).unwrap();
        assert!(matches!(
            equal_conflict.replace_from_resync(
                token,
                OwnershipRevision(11),
                vec![snapshot_virtual_shard(
                    11,
                    11,
                    virtual_shard_record(1, "world-a", 2),
                )],
            ),
            Err(OwnershipSnapshotError::EpochAuthorityConflict { .. })
        ));
        assert_eq!(
            equal_conflict.inner.read_state().revision,
            Some(OwnershipRevision(10))
        );

        let (missing, _) = moved_by_resync();
        let token = missing.begin_resync(INSTANCE_LEASE).unwrap();
        missing
            .replace_from_resync(
                token,
                OwnershipRevision(11),
                std::iter::empty::<OwnershipSnapshotRecord>(),
            )
            .unwrap();
        let state = missing.inner.read_state();
        assert_eq!(state.entry_count(), 3);
        assert!(state.actors.values().all(|entry| matches!(
            &entry.observation,
            StoreObservation::Absent {
                evidence: AbsenceEvidence::CoherentSnapshot,
                ..
            }
        )));
        assert!(state.virtual_shards.values().all(|entry| matches!(
            &entry.observation,
            StoreObservation::Absent {
                evidence: AbsenceEvidence::CoherentSnapshot,
                ..
            }
        )));
        assert!(state.singletons.values().all(|entry| matches!(
            &entry.observation,
            StoreObservation::Absent {
                evidence: AbsenceEvidence::CoherentSnapshot,
                ..
            }
        )));
        drop(state);
        assert!(matches!(
            missing.apply_singleton(
                OwnershipRevision(12),
                singleton_record(
                    "global",
                    "world-a",
                    1,
                    INSTANCE_LEASE,
                    PlacementState::Running,
                ),
            ),
            Err(OwnershipSnapshotError::EpochRegression { .. })
        ));

        let (higher, gate) = moved_by_resync();
        higher
            .apply_actor(
                OwnershipRevision(11),
                actor_record(1, "world-a", 3, INSTANCE_LEASE, PlacementState::Running),
            )
            .unwrap();
        higher
            .apply_virtual_shard(OwnershipRevision(12), virtual_shard_record(1, "world-a", 3))
            .unwrap();
        higher
            .apply_singleton(
                OwnershipRevision(13),
                singleton_record(
                    "global",
                    "world-a",
                    3,
                    INSTANCE_LEASE,
                    PlacementState::Running,
                ),
            )
            .unwrap();
        let state = higher.inner.read_state();
        assert!(
            state
                .actors
                .values()
                .all(|entry| matches!(&entry.observation, StoreObservation::PresentLocal(_)))
        );
        assert!(
            state
                .virtual_shards
                .values()
                .all(|entry| matches!(&entry.observation, StoreObservation::PresentLocal(_)))
        );
        assert!(
            state
                .singletons
                .values()
                .all(|entry| matches!(&entry.observation, StoreObservation::PresentLocal(_)))
        );
        drop(state);
        let service = service_kind!("World");
        let kind = actor_kind!("World");
        let route = RouteKey::U64(1);
        assert!(
            gate.authorize(request(
                &service,
                &kind,
                &kind,
                &route,
                Some(Epoch(3)),
                &OwnershipPlacement::Explicit,
            ))
            .is_ok()
        );

        let (never_local, _) = ready_snapshot(1);
        resync(
            &never_local,
            INSTANCE_LEASE,
            vec![snapshot_actor(
                5,
                5,
                actor_record(9, "world-b", 5, LeaseId(22), PlacementState::Running),
            )],
        );
        assert!(matches!(
            never_local.apply_actor(
                OwnershipRevision(6),
                actor_record(9, "world-a", 4, INSTANCE_LEASE, PlacementState::Running),
            ),
            Err(OwnershipSnapshotError::EpochRegression { .. })
        ));
        assert_present_remote(
            never_local
                .inner
                .read_state()
                .actors
                .values()
                .next()
                .unwrap(),
        );
    }

    #[test]
    fn resync_rejects_duplicate_logical_keys_for_every_placement_family() {
        let actor_snapshot = ready_snapshot(2).0;
        let token = actor_snapshot.begin_resync(INSTANCE_LEASE).unwrap();
        assert!(matches!(
            actor_snapshot.replace_from_resync(
                token,
                OwnershipRevision(2),
                vec![
                    snapshot_actor(
                        2,
                        1,
                        actor_record(1, "world-a", 1, INSTANCE_LEASE, PlacementState::Running,),
                    ),
                    snapshot_actor(
                        2,
                        2,
                        actor_record(1, "world-b", 2, LeaseId(22), PlacementState::Running,),
                    ),
                ],
            ),
            Err(OwnershipSnapshotError::DuplicatePlacementEvent { .. })
        ));
        assert_eq!(
            actor_snapshot.inner.read_state().revision,
            Some(OwnershipRevision(0))
        );

        let shard_snapshot = ready_snapshot(2).0;
        let token = shard_snapshot.begin_resync(INSTANCE_LEASE).unwrap();
        assert!(matches!(
            shard_snapshot.replace_from_resync(
                token,
                OwnershipRevision(2),
                vec![
                    snapshot_virtual_shard(2, 1, virtual_shard_record(1, "world-a", 1)),
                    snapshot_virtual_shard(2, 2, virtual_shard_record(1, "world-b", 2)),
                ],
            ),
            Err(OwnershipSnapshotError::DuplicatePlacementEvent { .. })
        ));
        assert_eq!(
            shard_snapshot.inner.read_state().revision,
            Some(OwnershipRevision(0))
        );

        let singleton_snapshot = ready_snapshot(2).0;
        let token = singleton_snapshot.begin_resync(INSTANCE_LEASE).unwrap();
        assert!(matches!(
            singleton_snapshot.replace_from_resync(
                token,
                OwnershipRevision(2),
                vec![
                    snapshot_singleton(
                        2,
                        1,
                        singleton_record(
                            "global",
                            "world-a",
                            1,
                            INSTANCE_LEASE,
                            PlacementState::Running,
                        ),
                    ),
                    snapshot_singleton(
                        2,
                        2,
                        singleton_record(
                            "global",
                            "world-b",
                            2,
                            LeaseId(22),
                            PlacementState::Running,
                        ),
                    ),
                ],
            ),
            Err(OwnershipSnapshotError::DuplicatePlacementEvent { .. })
        ));
        assert_eq!(
            singleton_snapshot.inner.read_state().revision,
            Some(OwnershipRevision(0))
        );
    }

    #[test]
    fn instance_incarnation_change_fences_even_if_a_backend_reuses_the_lease_id() {
        let (snapshot, gate) = ready_snapshot(1);
        let actor = actor_record(1, "world-a", 1, INSTANCE_LEASE, PlacementState::Running);
        snapshot
            .apply_actor(OwnershipRevision(1), actor.clone())
            .unwrap();
        let grant = gate
            .authorize(request(
                &service_kind!("World"),
                &actor_kind!("World"),
                &actor_kind!("World"),
                &RouteKey::U64(1),
                Some(Epoch(1)),
                &OwnershipPlacement::Explicit,
            ))
            .unwrap();

        let mut replacement =
            instance_record("World", "world-a", INSTANCE_LEASE, InstanceState::Ready);
        replacement.incarnation = InstanceIncarnation::new("world-a-replacement-boot");
        snapshot
            .apply_watch_batch(watch_batch(
                2,
                vec![OwnershipWatchEvent::InstanceUpserted {
                    record: replacement,
                }],
            ))
            .unwrap();

        assert!(!gate.is_current(&grant));
        let state = snapshot.inner.read_state();
        assert_lifecycle_fenced(&state.actors[&actor_key(&actor)]);
        assert_eq!(
            state.availability,
            SnapshotAvailability::Unavailable(OwnershipFenceReason::LeaseLost)
        );
    }

    #[test]
    fn instance_reincarnation_preserves_floors_and_global_revision() {
        let (snapshot, gate) = ready_snapshot(3);
        let actor = actor_record(1, "world-a", 5, INSTANCE_LEASE, PlacementState::Running);
        let shard = virtual_shard_record(1, "world-a", 5);
        let singleton = singleton_record(
            "global",
            "world-a",
            5,
            INSTANCE_LEASE,
            PlacementState::Running,
        );
        snapshot
            .apply_watch_batch(watch_batch(
                10,
                vec![
                    actor_upsert(10, actor_key(&actor), actor.clone()),
                    virtual_shard_upsert(10, virtual_shard_key(&shard), shard.clone()),
                    singleton_upsert(10, singleton_key(&singleton), singleton.clone()),
                ],
            ))
            .unwrap();
        let service = service_kind!("World");
        let kind = actor_kind!("World");
        let route = RouteKey::U64(1);
        let grant = gate
            .authorize(request(
                &service,
                &kind,
                &kind,
                &route,
                Some(Epoch(5)),
                &OwnershipPlacement::Explicit,
            ))
            .unwrap();

        let new_lease = LeaseId(22);
        snapshot
            .install_local_instance(
                instance_record("World", "world-a", new_lease, InstanceState::Ready),
                true,
            )
            .unwrap();
        let actor_key = actor_key(&actor);
        let shard_key = virtual_shard_key(&shard);
        let singleton_key = singleton_key(&singleton);
        let state = snapshot.inner.read_state();
        assert_eq!(state.revision, Some(OwnershipRevision(10)));
        assert_eq!(state.actors[&actor_key].version, OwnershipRevision(10));
        assert_eq!(state.actors[&actor_key].authority.epoch, Epoch(5));
        assert_lifecycle_fenced(&state.actors[&actor_key]);
        assert_eq!(
            state.virtual_shards[&shard_key].version,
            OwnershipRevision(10)
        );
        assert_eq!(state.virtual_shards[&shard_key].authority.epoch, Epoch(5));
        assert_lifecycle_fenced(&state.virtual_shards[&shard_key]);
        assert_eq!(
            state.singletons[&singleton_key].version,
            OwnershipRevision(10)
        );
        assert_eq!(state.singletons[&singleton_key].authority.epoch, Epoch(5));
        assert_lifecycle_fenced(&state.singletons[&singleton_key]);
        drop(state);
        assert!(!gate.is_current(&grant));

        assert!(
            !snapshot
                .apply_actor(
                    OwnershipRevision(9),
                    actor_record(1, "world-a", 9, new_lease, PlacementState::Running),
                )
                .unwrap()
        );
        assert!(
            !snapshot
                .apply_virtual_shard(OwnershipRevision(9), virtual_shard_record(1, "world-a", 9),)
                .unwrap()
        );
        assert!(
            !snapshot
                .apply_singleton(
                    OwnershipRevision(9),
                    singleton_record("global", "world-a", 9, new_lease, PlacementState::Running,),
                )
                .unwrap()
        );
        let state = snapshot.inner.read_state();
        assert_eq!(state.revision, Some(OwnershipRevision(10)));
        assert_eq!(state.actors[&actor_key].authority.epoch, Epoch(5));
        assert_eq!(state.actors[&actor_key].version, OwnershipRevision(10));
        assert_eq!(state.virtual_shards[&shard_key].authority.epoch, Epoch(5));
        assert_eq!(state.singletons[&singleton_key].authority.epoch, Epoch(5));
        drop(state);

        assert!(matches!(
            snapshot.apply_actor(
                OwnershipRevision(11),
                actor_record(1, "world-a", 5, new_lease, PlacementState::Running),
            ),
            Err(OwnershipSnapshotError::EpochAuthorityConflict { .. })
        ));
        assert!(matches!(
            snapshot
                .apply_virtual_shard(OwnershipRevision(11), virtual_shard_record(1, "world-a", 5),),
            Err(OwnershipSnapshotError::EpochReactivation { .. })
        ));
        assert!(matches!(
            snapshot.apply_singleton(
                OwnershipRevision(11),
                singleton_record("global", "world-a", 5, new_lease, PlacementState::Running,),
            ),
            Err(OwnershipSnapshotError::EpochAuthorityConflict { .. })
        ));
        assert_eq!(
            snapshot.inner.read_state().revision,
            Some(OwnershipRevision(10))
        );

        let (atomic_actor, _) = ready_snapshot(1);
        atomic_actor
            .apply_actor(OwnershipRevision(10), actor.clone())
            .unwrap();
        assert!(matches!(
            atomic_actor.apply_watch_batch(watch_batch(
                11,
                vec![
                    OwnershipWatchEvent::InstanceUpserted {
                        record: instance_record(
                            "World",
                            "world-a",
                            new_lease,
                            InstanceState::Ready,
                        ),
                    },
                    actor_upsert(11, actor_key, actor),
                ],
            )),
            Err(OwnershipSnapshotError::EpochReactivation { .. })
        ));
        let state = atomic_actor.inner.read_state();
        assert_eq!(state.revision, Some(OwnershipRevision(10)));
        assert_eq!(state.instance.as_ref().unwrap().lease_id, INSTANCE_LEASE);
        assert_present_local(state.actors.values().next().unwrap());
        drop(state);

        let (atomic_shard, _) = ready_snapshot(1);
        atomic_shard
            .apply_virtual_shard(OwnershipRevision(10), shard.clone())
            .unwrap();
        assert!(matches!(
            atomic_shard.apply_watch_batch(watch_batch(
                11,
                vec![
                    OwnershipWatchEvent::InstanceUpserted {
                        record: instance_record(
                            "World",
                            "world-a",
                            new_lease,
                            InstanceState::Ready,
                        ),
                    },
                    virtual_shard_upsert(11, shard_key, shard),
                ],
            )),
            Err(OwnershipSnapshotError::EpochReactivation { .. })
        ));
        let state = atomic_shard.inner.read_state();
        assert_eq!(state.revision, Some(OwnershipRevision(10)));
        assert_eq!(state.instance.as_ref().unwrap().lease_id, INSTANCE_LEASE);
        assert_present_local(state.virtual_shards.values().next().unwrap());
        drop(state);

        let (atomic_singleton, _) = ready_snapshot(1);
        atomic_singleton
            .apply_singleton(OwnershipRevision(10), singleton.clone())
            .unwrap();
        assert!(matches!(
            atomic_singleton.apply_watch_batch(watch_batch(
                11,
                vec![
                    OwnershipWatchEvent::InstanceUpserted {
                        record: instance_record(
                            "World",
                            "world-a",
                            new_lease,
                            InstanceState::Ready,
                        ),
                    },
                    singleton_upsert(11, singleton_key, singleton),
                ],
            )),
            Err(OwnershipSnapshotError::EpochReactivation { .. })
        ));
        let state = atomic_singleton.inner.read_state();
        assert_eq!(state.revision, Some(OwnershipRevision(10)));
        assert_eq!(state.instance.as_ref().unwrap().lease_id, INSTANCE_LEASE);
        assert_present_local(state.singletons.values().next().unwrap());
    }

    #[test]
    fn remote_floor_updates_invalidate_an_active_resync_without_staling_ready_grants() {
        let (snapshot, gate) = ready_snapshot(2);
        snapshot
            .apply_actor(
                OwnershipRevision(1),
                actor_record(1, "world-a", 1, INSTANCE_LEASE, PlacementState::Running),
            )
            .unwrap();
        let service = service_kind!("World");
        let kind = actor_kind!("World");
        let route = RouteKey::U64(1);
        let grant = gate
            .authorize(request(
                &service,
                &kind,
                &kind,
                &route,
                Some(Epoch(1)),
                &OwnershipPlacement::Explicit,
            ))
            .unwrap();
        assert!(
            !snapshot
                .apply_actor(
                    OwnershipRevision(2),
                    actor_record(2, "world-b", 5, LeaseId(22), PlacementState::Running),
                )
                .unwrap()
        );
        assert!(gate.is_current(&grant));
        assert!(matches!(
            snapshot.apply_actor(
                OwnershipRevision(3),
                actor_record(2, "world-a", 4, INSTANCE_LEASE, PlacementState::Running),
            ),
            Err(OwnershipSnapshotError::EpochRegression { .. })
        ));
        let remote_key = actor_key(&actor_record(
            2,
            "world-b",
            5,
            LeaseId(22),
            PlacementState::Running,
        ));
        let state = snapshot.inner.read_state();
        assert_eq!(state.revision, Some(OwnershipRevision(2)));
        assert_eq!(state.actors[&remote_key].authority.epoch, Epoch(5));
        assert_present_remote(&state.actors[&remote_key]);
        drop(state);

        let token = snapshot.begin_resync(INSTANCE_LEASE).unwrap();
        assert!(
            !snapshot
                .apply_actor(
                    OwnershipRevision(3),
                    actor_record(2, "world-b", 6, LeaseId(22), PlacementState::Running),
                )
                .unwrap()
        );
        assert!(matches!(
            snapshot.replace_from_resync(
                token,
                OwnershipRevision(3),
                std::iter::empty::<OwnershipSnapshotRecord>(),
            ),
            Err(OwnershipSnapshotError::StaleResync { .. })
        ));
        let state = snapshot.inner.read_state();
        assert_eq!(state.actors.len(), 2);
        assert_eq!(state.actors[&remote_key].authority.epoch, Epoch(6));
        assert_present_remote(&state.actors[&remote_key]);
    }

    #[test]
    fn remote_authority_floors_are_bounded_and_existing_remote_updates_revoke() {
        let (snapshot, gate) = ready_snapshot(2);
        let local = actor_record(1, "world-a", 1, INSTANCE_LEASE, PlacementState::Running);
        snapshot
            .apply_watch_batch(watch_batch(
                1,
                vec![actor_upsert(1, actor_key(&local), local.clone())],
            ))
            .unwrap();
        let service = service_kind!("World");
        let kind = actor_kind!("World");
        let route = RouteKey::U64(1);
        let grant = gate
            .authorize(request(
                &service,
                &kind,
                &kind,
                &route,
                Some(Epoch(1)),
                &OwnershipPlacement::Explicit,
            ))
            .unwrap();
        let absent_remote = actor_record(2, "world-b", 1, LeaseId(22), PlacementState::Running);
        let absent_remote_key = actor_key(&absent_remote);
        snapshot
            .apply_watch_batch(watch_batch(
                2,
                vec![actor_upsert(2, actor_key(&absent_remote), absent_remote)],
            ))
            .unwrap();
        let state = snapshot.inner.read_state();
        assert_eq!(state.actors.len(), 2);
        assert_present_remote(&state.actors[&absent_remote_key]);
        assert_eq!(state.actors[&absent_remote_key].authority.epoch, Epoch(1));
        drop(state);
        assert!(gate.is_current(&grant));

        let mut remote = local;
        remote.owner = InstanceId::new("world-b");
        remote.epoch = Epoch(2);
        remote.lease_id = LeaseId(22);
        snapshot
            .apply_watch_batch(watch_batch(
                3,
                vec![actor_upsert(3, actor_key(&remote), remote)],
            ))
            .unwrap();
        assert_present_remote(
            &snapshot.inner.read_state().actors[&actor_key(&actor_record(
                1,
                "world-a",
                1,
                INSTANCE_LEASE,
                PlacementState::Running,
            ))],
        );
        assert!(!gate.is_current(&grant));

        let (bounded, _) = ready_snapshot(1);
        bounded
            .apply_actor(
                OwnershipRevision(1),
                actor_record(1, "world-a", 1, INSTANCE_LEASE, PlacementState::Running),
            )
            .unwrap();
        assert_eq!(
            bounded.apply_actor(
                OwnershipRevision(2),
                actor_record(2, "world-b", 9, LeaseId(22), PlacementState::Running),
            ),
            Err(OwnershipSnapshotError::CapacityExceeded { max_entries: 1 })
        );
        let state = bounded.inner.read_state();
        assert_eq!(state.revision, Some(OwnershipRevision(1)));
        assert_eq!(state.actors.len(), 1);
        assert_eq!(
            state.availability,
            SnapshotAvailability::Unavailable(OwnershipFenceReason::CapacityExceeded)
        );
    }

    #[test]
    fn lifecycle_fencing_preserves_every_local_record_and_does_not_reclaim_capacity() {
        let (snapshot, _) = ready_snapshot(3);
        let actor = actor_record(1, "world-a", 1, INSTANCE_LEASE, PlacementState::Running);
        let shard = virtual_shard_record(1, "world-a", 1);
        let singleton = singleton_record(
            "global",
            "world-a",
            1,
            INSTANCE_LEASE,
            PlacementState::Running,
        );
        let actor_key = actor_key(&actor);
        let shard_key = virtual_shard_key(&shard);
        let singleton_key = singleton_key(&singleton);
        snapshot
            .apply_watch_batch(watch_batch(
                1,
                vec![
                    actor_upsert(1, actor_key.clone(), actor.clone()),
                    virtual_shard_upsert(1, shard_key.clone(), shard.clone()),
                    singleton_upsert(1, singleton_key.clone(), singleton.clone()),
                ],
            ))
            .unwrap();

        {
            let state = snapshot.inner.read_state();
            assert_present_local(&state.actors[&actor_key]);
            assert_present_local(&state.virtual_shards[&shard_key]);
            assert_present_local(&state.singletons[&singleton_key]);
        }

        let new_lease = LeaseId(22);
        snapshot
            .install_local_instance(
                instance_record("World", "world-a", new_lease, InstanceState::Ready),
                true,
            )
            .unwrap();

        let state = snapshot.inner.read_state();
        assert_eq!(state.entry_count(), 3);
        assert_lifecycle_fenced(&state.actors[&actor_key]);
        assert_lifecycle_fenced(&state.virtual_shards[&shard_key]);
        assert_lifecycle_fenced(&state.singletons[&singleton_key]);
        assert_eq!(
            state.actors[&actor_key].observation.store_present_record(),
            Some(&actor)
        );
        assert_eq!(
            state.virtual_shards[&shard_key]
                .observation
                .store_present_record(),
            Some(&shard)
        );
        assert_eq!(
            state.singletons[&singleton_key]
                .observation
                .store_present_record(),
            Some(&singleton)
        );
        drop(state);

        assert_eq!(
            snapshot.apply_actor(
                OwnershipRevision(2),
                actor_record(2, "world-a", 1, new_lease, PlacementState::Running),
            ),
            Err(OwnershipSnapshotError::CapacityExceeded { max_entries: 3 })
        );
        assert_eq!(snapshot.inner.read_state().entry_count(), 3);
    }

    #[test]
    fn resync_resurrection_requires_a_newer_record_revision_for_every_family() {
        let (snapshot, _) = ready_snapshot(3);
        let actor = actor_record(1, "world-a", 1, INSTANCE_LEASE, PlacementState::Running);
        let shard = virtual_shard_record(1, "world-a", 1);
        let singleton = singleton_record(
            "global",
            "world-a",
            1,
            INSTANCE_LEASE,
            PlacementState::Running,
        );
        let actor_key = actor_key(&actor);
        let shard_key = virtual_shard_key(&shard);
        let singleton_key = singleton_key(&singleton);
        snapshot
            .apply_watch_batch(watch_batch(
                1,
                vec![
                    actor_upsert(1, actor_key.clone(), actor),
                    virtual_shard_upsert(1, shard_key.clone(), shard),
                    singleton_upsert(1, singleton_key.clone(), singleton),
                ],
            ))
            .unwrap();
        let token = snapshot.begin_resync(INSTANCE_LEASE).unwrap();
        snapshot
            .replace_from_resync(
                token,
                OwnershipRevision(10),
                std::iter::empty::<OwnershipSnapshotRecord>(),
            )
            .unwrap();

        {
            let state = snapshot.inner.read_state();
            assert_absent(&state.actors[&actor_key], AbsenceEvidence::CoherentSnapshot);
            assert_absent(
                &state.virtual_shards[&shard_key],
                AbsenceEvidence::CoherentSnapshot,
            );
            assert_absent(
                &state.singletons[&singleton_key],
                AbsenceEvidence::CoherentSnapshot,
            );
            assert_eq!(state.actors[&actor_key].version, OwnershipRevision(10));
            assert_eq!(
                state.virtual_shards[&shard_key].version,
                OwnershipRevision(10)
            );
            assert_eq!(
                state.singletons[&singleton_key].version,
                OwnershipRevision(10)
            );
        }

        let stale_actor = actor_record(1, "world-a", 2, INSTANCE_LEASE, PlacementState::Running);
        let token = snapshot.begin_resync(INSTANCE_LEASE).unwrap();
        assert!(matches!(
            snapshot.replace_from_resync(
                token,
                OwnershipRevision(11),
                vec![snapshot_actor(11, 10, stale_actor)],
            ),
            Err(OwnershipSnapshotError::ResurrectionRevisionNotAdvanced {
                key,
                absence: OwnershipRevision(10),
                incoming: OwnershipRevision(10),
            }) if *key == OwnershipKey::Explicit(actor_key.clone())
        ));

        let stale_shard = virtual_shard_record(1, "world-a", 2);
        let token = snapshot.begin_resync(INSTANCE_LEASE).unwrap();
        assert!(matches!(
            snapshot.replace_from_resync(
                token,
                OwnershipRevision(11),
                vec![snapshot_virtual_shard(11, 10, stale_shard)],
            ),
            Err(OwnershipSnapshotError::ResurrectionRevisionNotAdvanced {
                key,
                absence: OwnershipRevision(10),
                incoming: OwnershipRevision(10),
            }) if *key == OwnershipKey::VirtualShard(shard_key.clone())
        ));

        let stale_singleton = singleton_record(
            "global",
            "world-a",
            2,
            INSTANCE_LEASE,
            PlacementState::Running,
        );
        let token = snapshot.begin_resync(INSTANCE_LEASE).unwrap();
        assert!(matches!(
            snapshot.replace_from_resync(
                token,
                OwnershipRevision(11),
                vec![snapshot_singleton(11, 10, stale_singleton)],
            ),
            Err(OwnershipSnapshotError::ResurrectionRevisionNotAdvanced {
                key,
                absence: OwnershipRevision(10),
                incoming: OwnershipRevision(10),
            }) if *key == OwnershipKey::Singleton(singleton_key.clone())
        ));

        let state = snapshot.inner.read_state();
        assert_eq!(state.revision, Some(OwnershipRevision(10)));
        assert_absent(&state.actors[&actor_key], AbsenceEvidence::CoherentSnapshot);
        assert_absent(
            &state.virtual_shards[&shard_key],
            AbsenceEvidence::CoherentSnapshot,
        );
        assert_absent(
            &state.singletons[&singleton_key],
            AbsenceEvidence::CoherentSnapshot,
        );
        drop(state);

        let actor = actor_record(1, "world-a", 2, INSTANCE_LEASE, PlacementState::Running);
        let shard = virtual_shard_record(1, "world-a", 2);
        let singleton = singleton_record(
            "global",
            "world-a",
            2,
            INSTANCE_LEASE,
            PlacementState::Running,
        );
        let token = snapshot.begin_resync(INSTANCE_LEASE).unwrap();
        snapshot
            .replace_from_resync(
                token,
                OwnershipRevision(11),
                vec![
                    snapshot_actor(11, 11, actor),
                    snapshot_virtual_shard(11, 11, shard),
                    snapshot_singleton(11, 11, singleton),
                ],
            )
            .unwrap();
        let state = snapshot.inner.read_state();
        assert_present_local(&state.actors[&actor_key]);
        assert_present_local(&state.virtual_shards[&shard_key]);
        assert_present_local(&state.singletons[&singleton_key]);
    }

    fn resync(
        snapshot: &LocalOwnershipSnapshot,
        lease_id: LeaseId,
        records: Vec<OwnershipSnapshotRecord>,
    ) {
        let snapshot_revision = records
            .iter()
            .map(|record| match record {
                OwnershipSnapshotRecord::Actor { version, .. }
                | OwnershipSnapshotRecord::VirtualShard { version, .. }
                | OwnershipSnapshotRecord::Singleton { version, .. } => *version,
            })
            .max()
            .unwrap_or(OwnershipRevision(0));
        let token = snapshot.begin_resync(lease_id).unwrap();
        snapshot
            .replace_from_resync(token, snapshot_revision, records)
            .unwrap();
    }

    fn ready_snapshot(max_entries: usize) -> (LocalOwnershipSnapshot, LocalOwnershipGate) {
        let snapshot = LocalOwnershipSnapshot::with_config(
            service_kind!("World"),
            InstanceId::new("world-a"),
            InstanceIncarnation::new("world-a-boot"),
            LocalOwnershipSnapshotConfig::try_new(max_entries).unwrap(),
        );
        install_ready_instance(&snapshot, INSTANCE_LEASE);
        resync(&snapshot, INSTANCE_LEASE, Vec::new());
        let gate = snapshot.gate();
        (snapshot, gate)
    }

    fn install_ready_instance(snapshot: &LocalOwnershipSnapshot, lease_id: LeaseId) {
        snapshot
            .install_local_instance(
                instance_record("World", "world-a", lease_id, InstanceState::Ready),
                true,
            )
            .unwrap();
    }

    fn instance_record(
        service: &str,
        instance: &str,
        lease_id: LeaseId,
        state: InstanceState,
    ) -> InstanceRecord {
        InstanceRecord {
            service_kind: ServiceKind::new(service),
            instance_id: InstanceId::new(instance),
            incarnation: InstanceIncarnation::new(format!("{instance}-boot")),
            lease_id,
            advertised_endpoint: "http://127.0.0.1:18080".parse().unwrap(),
            control_endpoint: "http://127.0.0.1:18081".parse().unwrap(),
            version: "test".to_string(),
            state,
            capacity: InstanceCapacity::default(),
            labels: Default::default(),
        }
    }

    fn watch_batch(revision: u64, events: Vec<OwnershipWatchEvent>) -> OwnershipWatchBatch {
        OwnershipWatchBatch {
            revision: PlacementRevision(revision),
            events,
        }
    }

    fn snapshot_actor(
        snapshot_revision: u64,
        record_revision: u64,
        record: ActorPlacementRecord,
    ) -> OwnershipSnapshotRecord {
        let binding = OwnershipRecordBinding::Actor(record.clone());
        OwnershipSnapshotRecord::Actor {
            version: OwnershipRevision(record_revision),
            proof: test_proof(
                OwnershipProofContext::Snapshot,
                snapshot_revision,
                record_revision,
                &binding,
            ),
            record,
        }
    }

    fn snapshot_virtual_shard(
        snapshot_revision: u64,
        record_revision: u64,
        record: VirtualShardPlacementRecord,
    ) -> OwnershipSnapshotRecord {
        let binding = OwnershipRecordBinding::VirtualShard(record.clone());
        OwnershipSnapshotRecord::VirtualShard {
            version: OwnershipRevision(record_revision),
            proof: test_proof(
                OwnershipProofContext::Snapshot,
                snapshot_revision,
                record_revision,
                &binding,
            ),
            record,
        }
    }

    fn snapshot_singleton(
        snapshot_revision: u64,
        record_revision: u64,
        record: SingletonPlacementRecord,
    ) -> OwnershipSnapshotRecord {
        let binding = OwnershipRecordBinding::Singleton(record.clone());
        OwnershipSnapshotRecord::Singleton {
            version: OwnershipRevision(record_revision),
            proof: test_proof(
                OwnershipProofContext::Snapshot,
                snapshot_revision,
                record_revision,
                &binding,
            ),
            record,
        }
    }

    fn actor_upsert(
        revision: u64,
        key: ActorPlacementKey,
        record: ActorPlacementRecord,
    ) -> OwnershipWatchEvent {
        let binding = OwnershipRecordBinding::Actor(record.clone());
        OwnershipWatchEvent::ActorUpserted {
            key,
            proof: test_proof(OwnershipProofContext::Upsert, revision, revision, &binding),
            record,
        }
    }

    fn virtual_shard_upsert(
        revision: u64,
        key: VirtualShardPlacementKey,
        record: VirtualShardPlacementRecord,
    ) -> OwnershipWatchEvent {
        let binding = OwnershipRecordBinding::VirtualShard(record.clone());
        OwnershipWatchEvent::VirtualShardUpserted {
            key,
            proof: test_proof(OwnershipProofContext::Upsert, revision, revision, &binding),
            record,
        }
    }

    fn singleton_upsert(
        revision: u64,
        key: SingletonKey,
        record: SingletonPlacementRecord,
    ) -> OwnershipWatchEvent {
        let binding = OwnershipRecordBinding::Singleton(record.clone());
        OwnershipWatchEvent::SingletonUpserted {
            key,
            proof: test_proof(OwnershipProofContext::Upsert, revision, revision, &binding),
            record,
        }
    }

    fn actor_delete(
        revision: u64,
        previous_revision: u64,
        key: ActorPlacementKey,
        previous_record: ActorPlacementRecord,
    ) -> OwnershipWatchEvent {
        let binding = OwnershipRecordBinding::Actor(previous_record.clone());
        OwnershipWatchEvent::ActorDeleted {
            key,
            proof: test_proof(
                OwnershipProofContext::Delete,
                revision,
                previous_revision,
                &binding,
            ),
            previous_record,
        }
    }

    fn virtual_shard_delete(
        revision: u64,
        previous_revision: u64,
        key: VirtualShardPlacementKey,
        previous_record: VirtualShardPlacementRecord,
    ) -> OwnershipWatchEvent {
        let binding = OwnershipRecordBinding::VirtualShard(previous_record.clone());
        OwnershipWatchEvent::VirtualShardDeleted {
            key,
            proof: test_proof(
                OwnershipProofContext::Delete,
                revision,
                previous_revision,
                &binding,
            ),
            previous_record,
        }
    }

    fn singleton_delete(
        revision: u64,
        previous_revision: u64,
        key: SingletonKey,
        previous_record: SingletonPlacementRecord,
    ) -> OwnershipWatchEvent {
        let binding = OwnershipRecordBinding::Singleton(previous_record.clone());
        OwnershipWatchEvent::SingletonDeleted {
            key,
            proof: test_proof(
                OwnershipProofContext::Delete,
                revision,
                previous_revision,
                &binding,
            ),
            previous_record,
        }
    }

    fn test_proof(
        context: OwnershipProofContext,
        observed_revision: u64,
        record_revision: u64,
        binding: &OwnershipRecordBinding,
    ) -> OwnershipEpochFloorProof {
        let floor = EpochFloorRecord {
            key: binding.epoch_key(),
            epoch: binding.epoch(),
        };
        OwnershipEpochFloorProof::new(
            context,
            PlacementRevision(observed_revision),
            PlacementVersion::from_modification_revision(record_revision),
            binding.clone(),
            PlacementVersion::from_modification_revision(record_revision),
            floor,
            None,
        )
        .unwrap()
    }

    fn actor_record(
        actor_id: u64,
        owner: &str,
        epoch: u64,
        lease_id: LeaseId,
        state: PlacementState,
    ) -> ActorPlacementRecord {
        ActorPlacementRecord {
            service_kind: service_kind!("World"),
            actor_kind: actor_kind!("World"),
            actor_id: ActorId::U64(actor_id),
            owner: InstanceId::new(owner),
            epoch: Epoch(epoch),
            lease_id,
            state,
        }
    }

    fn virtual_shard_record(shard_id: u32, owner: &str, epoch: u64) -> VirtualShardPlacementRecord {
        VirtualShardPlacementRecord {
            service_kind: service_kind!("World"),
            actor_kind: actor_kind!("Player"),
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
        state: PlacementState,
    ) -> SingletonPlacementRecord {
        SingletonPlacementRecord {
            service_kind: service_kind!("World"),
            singleton_kind: actor_kind!("Season"),
            scope: scope.to_string(),
            owner: InstanceId::new(owner),
            owner_incarnation: InstanceIncarnation::new(format!("{owner}-boot")),
            epoch: Epoch(epoch),
            lease_id,
            state,
        }
    }

    fn request<'a>(
        service: &'a ServiceKind,
        expected_actor: &'a ActorKind,
        route_actor: &'a ActorKind,
        route_key: &'a RouteKey,
        route_epoch: Option<Epoch>,
        placement: &'a OwnershipPlacement,
    ) -> OwnershipRequest<'a> {
        OwnershipRequest {
            expected_service: service,
            expected_actor_kind: expected_actor,
            route_actor_kind: route_actor,
            route_key,
            route_epoch,
            placement,
        }
    }
}
