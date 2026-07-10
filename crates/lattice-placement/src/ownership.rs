use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use lattice_core::actor_ref::Epoch;
use lattice_core::id::{ActorId, RouteKey};
use lattice_core::instance::InstanceId;
use lattice_core::kind::{ActorKind, ServiceKind};

use crate::registry::{InstanceRecord, InstanceState};
use crate::sharding::VirtualShardMapper;
use crate::storage::{
    ActorPlacementKey, ActorPlacementRecord, LeaseId, PlacementState, SingletonKey,
    SingletonPlacementRecord, VirtualShardPlacementKey, VirtualShardPlacementRecord,
};

const DEFAULT_MAX_OWNERSHIP_ENTRIES: usize = 65_536;

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
    #[error("local ownership snapshot has no active instance lease")]
    MissingInstanceLease,
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
    },
    VirtualShard {
        version: OwnershipRevision,
        record: VirtualShardPlacementRecord,
    },
    Singleton {
        version: OwnershipRevision,
        record: SingletonPlacementRecord,
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
    max_entries: NonZeroUsize,
    state: RwLock<LocalOwnershipState>,
}

#[derive(Debug)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LocalInstanceAuthority {
    lease_id: LeaseId,
    state: InstanceState,
}

#[derive(Debug, Clone)]
struct VersionedRecord<T> {
    version: OwnershipRevision,
    record: Option<T>,
}

impl LocalOwnershipSnapshot {
    pub fn new(service_kind: ServiceKind, instance_id: InstanceId) -> Self {
        Self::with_config(
            service_kind,
            instance_id,
            LocalOwnershipSnapshotConfig::default(),
        )
    }

    pub fn with_config(
        service_kind: ServiceKind,
        instance_id: InstanceId,
        config: LocalOwnershipSnapshotConfig,
    ) -> Self {
        Self {
            inner: Arc::new(LocalOwnershipInner {
                service_kind,
                instance_id,
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

    pub fn install_local_instance(
        &self,
        record: InstanceRecord,
        lease_valid: bool,
    ) -> Result<(), OwnershipSnapshotError> {
        self.validate_instance_identity(&record)?;
        let mut state = self.inner.write_state();
        let lease_changed = state
            .instance
            .is_some_and(|instance| instance.lease_id != record.lease_id);
        if lease_changed {
            state.actors.clear();
            state.virtual_shards.clear();
            state.singletons.clear();
            state.valid_owner_leases.clear();
            state.revision = None;
            state.availability =
                SnapshotAvailability::Unavailable(OwnershipFenceReason::Initializing);
        }
        state.instance = Some(LocalInstanceAuthority {
            lease_id: record.lease_id,
            state: record.state,
        });
        if lease_valid {
            state.valid_owner_leases.insert(record.lease_id);
        } else {
            state.valid_owner_leases.remove(&record.lease_id);
            if !lease_changed {
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
        state.actors = actors;
        state.virtual_shards = virtual_shards;
        state.singletons = singletons;
        state.prune_owner_leases(current_lease);
        state.revision = Some(snapshot_revision);
        state.availability = SnapshotAvailability::Ready;
        state.bump_generation();
        Ok(())
    }

    pub fn apply_actor(
        &self,
        version: OwnershipRevision,
        record: ActorPlacementRecord,
    ) -> Result<bool, OwnershipSnapshotError> {
        let mut state = self.inner.write_state();
        if !state.accept_revision(version) {
            return Ok(false);
        }
        if record.service_kind != self.inner.service_kind {
            return Ok(false);
        }
        let key = actor_key(&record);
        if record.owner != self.inner.instance_id {
            let changed = apply_remote_update(&mut state.actors, key, version);
            if changed {
                state.bump_generation();
            }
            return Ok(changed);
        }
        if state
            .actors
            .get(&key)
            .is_some_and(|current| current.version >= version)
        {
            return Ok(false);
        }
        let already_present = state.actors.contains_key(&key);
        self.ensure_insert_capacity(&mut state, already_present)?;
        state.actors.insert(
            key,
            VersionedRecord {
                version,
                record: Some(record),
            },
        );
        state.bump_generation();
        Ok(true)
    }

    pub fn apply_virtual_shard(
        &self,
        version: OwnershipRevision,
        record: VirtualShardPlacementRecord,
    ) -> Result<bool, OwnershipSnapshotError> {
        let mut state = self.inner.write_state();
        if !state.accept_revision(version) {
            return Ok(false);
        }
        if record.service_kind != self.inner.service_kind {
            return Ok(false);
        }
        let key = virtual_shard_key(&record);
        if record.owner != self.inner.instance_id {
            let changed = apply_remote_update(&mut state.virtual_shards, key, version);
            if changed {
                state.bump_generation();
            }
            return Ok(changed);
        }
        if state
            .virtual_shards
            .get(&key)
            .is_some_and(|current| current.version >= version)
        {
            return Ok(false);
        }
        let already_present = state.virtual_shards.contains_key(&key);
        self.ensure_insert_capacity(&mut state, already_present)?;
        state.virtual_shards.insert(
            key,
            VersionedRecord {
                version,
                record: Some(record),
            },
        );
        state.bump_generation();
        Ok(true)
    }

    pub fn apply_singleton(
        &self,
        version: OwnershipRevision,
        record: SingletonPlacementRecord,
    ) -> Result<bool, OwnershipSnapshotError> {
        let mut state = self.inner.write_state();
        if !state.accept_revision(version) {
            return Ok(false);
        }
        if record.service_kind != self.inner.service_kind {
            return Ok(false);
        }
        let key = singleton_key(&record);
        if record.owner != self.inner.instance_id {
            let changed = apply_remote_update(&mut state.singletons, key, version);
            if changed {
                state.bump_generation();
            }
            return Ok(changed);
        }
        if state
            .singletons
            .get(&key)
            .is_some_and(|current| current.version >= version)
        {
            return Ok(false);
        }
        let already_present = state.singletons.contains_key(&key);
        self.ensure_insert_capacity(&mut state, already_present)?;
        state.singletons.insert(
            key,
            VersionedRecord {
                version,
                record: Some(record),
            },
        );
        state.bump_generation();
        Ok(true)
    }

    pub fn remove(&self, key: &OwnershipKey, version: OwnershipRevision) -> bool {
        let mut state = self.inner.write_state();
        if !state.accept_revision(version) {
            return false;
        }
        let removed = match key {
            OwnershipKey::Explicit(key) => apply_removal(&mut state.actors, key, version),
            OwnershipKey::VirtualShard(key) => {
                apply_removal(&mut state.virtual_shards, key, version)
            }
            OwnershipKey::Singleton(key) => apply_removal(&mut state.singletons, key, version),
        };
        if removed {
            state.bump_generation();
        }
        removed
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
        Ok(())
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
                OwnershipSnapshotRecord::Actor { version, record }
                    if record.service_kind == self.inner.service_kind
                        && record.owner == self.inner.instance_id =>
                {
                    insert_collected_record(&mut maps.actors, actor_key(&record), version, record);
                }
                OwnershipSnapshotRecord::VirtualShard { version, record }
                    if record.service_kind == self.inner.service_kind
                        && record.owner == self.inner.instance_id =>
                {
                    insert_collected_record(
                        &mut maps.virtual_shards,
                        virtual_shard_key(&record),
                        version,
                        record,
                    );
                }
                OwnershipSnapshotRecord::Singleton { version, record }
                    if record.service_kind == self.inner.service_kind
                        && record.owner == self.inner.instance_id =>
                {
                    insert_collected_record(
                        &mut maps.singletons,
                        singleton_key(&record),
                        version,
                        record,
                    );
                }
                _ => {}
            }
            self.ensure_collected_capacity(&maps)?;
        }
        Ok((maps.actors, maps.virtual_shards, maps.singletons))
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

fn insert_collected_record<K, V>(
    records: &mut HashMap<K, VersionedRecord<V>>,
    key: K,
    version: OwnershipRevision,
    record: V,
) where
    K: Eq + std::hash::Hash,
{
    if records
        .get(&key)
        .is_some_and(|current| current.version >= version)
    {
        return;
    }
    records.insert(
        key,
        VersionedRecord {
            version,
            record: Some(record),
        },
    );
}

fn apply_remote_update<K, V>(
    records: &mut HashMap<K, VersionedRecord<V>>,
    key: K,
    version: OwnershipRevision,
) -> bool
where
    K: Eq + std::hash::Hash,
{
    let Some(current) = records.get_mut(&key) else {
        return false;
    };
    if current.version >= version {
        return false;
    }
    current.version = version;
    current.record = None;
    true
}

fn apply_removal<K, V>(
    records: &mut HashMap<K, VersionedRecord<V>>,
    key: &K,
    version: OwnershipRevision,
) -> bool
where
    K: Eq + std::hash::Hash,
{
    let Some(current) = records.get_mut(key) else {
        return false;
    };
    if current.version > version || (current.version == version && current.record.is_none()) {
        return false;
    }
    current.version = version;
    current.record = None;
    true
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
        let instance = state.instance.ok_or_else(|| {
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
        let actor_leases = self
            .actors
            .values()
            .filter_map(|entry| entry.record.as_ref().map(|record| record.lease_id));
        let singleton_leases = self
            .singletons
            .values()
            .filter_map(|entry| entry.record.as_ref().map(|record| record.lease_id));
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
                .and_then(|entry| entry.record.as_ref())
                .map(|record| LocalAuthority {
                    owner: record.owner.clone(),
                    epoch: record.epoch,
                    lease_id: Some(record.lease_id),
                    state: Some(record.state),
                }),
            OwnershipKey::VirtualShard(key) => self
                .virtual_shards
                .get(key)
                .and_then(|entry| entry.record.as_ref())
                .map(|record| LocalAuthority {
                    owner: record.owner.clone(),
                    epoch: record.epoch,
                    lease_id: None,
                    state: None,
                }),
            OwnershipKey::Singleton(key) => self
                .singletons
                .get(key)
                .and_then(|entry| entry.record.as_ref())
                .map(|record| LocalAuthority {
                    owner: record.owner.clone(),
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
    use lattice_core::instance::InstanceCapacity;
    use lattice_core::{actor_kind, service_kind};

    use super::*;

    const INSTANCE_LEASE: LeaseId = LeaseId(11);

    #[test]
    fn gate_starts_fenced_and_requires_epoch_after_resync() {
        let service = service_kind!("World");
        let other_service = service_kind!("Other");
        let actor = actor_kind!("World");
        let route_key = RouteKey::U64(7);
        let placement = OwnershipPlacement::Explicit;
        let snapshot = LocalOwnershipSnapshot::new(service.clone(), InstanceId::new("world-a"));
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
                actor_record(7, "world-a", 3, LeaseId(99), PlacementState::Running),
            )
            .unwrap();
        let invalid_lease = gate
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
            vec![OwnershipSnapshotRecord::Actor {
                version: OwnershipRevision(3),
                record: actor_record(7, "world-a", 3, INSTANCE_LEASE, PlacementState::Running),
            }],
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
            LocalOwnershipSnapshotConfig::try_new(1).unwrap(),
        );
        install_ready_instance(&bounded, INSTANCE_LEASE);
        let token = bounded.begin_resync(INSTANCE_LEASE).unwrap();
        let overflow = bounded
            .replace_from_resync(
                token,
                OwnershipRevision(1),
                [
                    OwnershipSnapshotRecord::Actor {
                        version: OwnershipRevision(1),
                        record: actor_record(1, "world-b", 1, LeaseId(20), PlacementState::Running),
                    },
                    OwnershipSnapshotRecord::Actor {
                        version: OwnershipRevision(1),
                        record: actor_record(2, "world-b", 1, LeaseId(20), PlacementState::Running),
                    },
                ],
            )
            .unwrap_err();
        assert_eq!(
            overflow,
            OwnershipSnapshotError::CapacityExceeded { max_entries: 1 }
        );
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
                InstanceRecord {
                    service_kind: service_kind!("World"),
                    instance_id: InstanceId::new("world-a"),
                    lease_id,
                    advertised_endpoint: "http://127.0.0.1:18080".parse().unwrap(),
                    control_endpoint: "http://127.0.0.1:18081".parse().unwrap(),
                    version: "test".to_string(),
                    state: InstanceState::Ready,
                    capacity: InstanceCapacity::default(),
                    labels: Default::default(),
                },
                true,
            )
            .unwrap();
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
