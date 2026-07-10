use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use lattice_core::instance::InstanceId;
use lattice_core::kind::{ActorKind, ServiceKind};
use tokio::sync::broadcast;

use crate::error::PlacementError;
use crate::registry::InstanceRecord;
use crate::storage::{
    ActorPlacementKey, ActorPlacementRecord, CoordinatorLeadership, LeaseId, OwnershipView,
    OwnershipViewError, OwnershipViewRecord, OwnershipViewSnapshot, OwnershipWatch,
    OwnershipWatchBatch, OwnershipWatchEvent, OwnershipWatchMessage, OwnershipWatchUpdate,
    PlacementPrefix, PlacementRevision, PlacementStore, PlacementVersion, PlacementWatch,
    PlacementWatchEvent, SingletonKey, SingletonPlacementRecord, VirtualShardPlacementKey,
    VirtualShardPlacementRecord,
};

const WATCH_CAPACITY: usize = 128;

#[derive(Debug, Clone)]
pub struct InMemoryPlacementStore {
    prefix: PlacementPrefix,
    inner: Arc<std::sync::Mutex<PlacementStoreInner>>,
    next_lease_id: Arc<AtomicU64>,
}

impl InMemoryPlacementStore {
    pub fn new(prefix: PlacementPrefix) -> Self {
        Self {
            prefix,
            inner: Arc::new(std::sync::Mutex::new(PlacementStoreInner::default())),
            next_lease_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn with_shared_inner(prefix: PlacementPrefix, other: &Self) -> Self {
        Self {
            prefix,
            inner: other.inner.clone(),
            next_lease_id: other.next_lease_id.clone(),
        }
    }

    pub fn instance_lease_keepalive_count(&self, lease_id: LeaseId) -> Option<u64> {
        self.inner
            .lock()
            .expect("placement store mutex poisoned")
            .instance_leases
            .get(&lease_id)
            .copied()
    }

    pub fn coordinator_leader(&self) -> Option<CoordinatorLeadership> {
        self.inner
            .lock()
            .expect("placement store mutex poisoned")
            .coordinator_leader
            .clone()
    }

    #[cfg(test)]
    pub(crate) fn remove_instance_for_test(
        &self,
        instance_id: &InstanceId,
    ) -> Option<InstanceRecord> {
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let record = inner
            .instances
            .remove(&self.prefixed_instance_key(instance_id))?;
        let revision = inner.next_placement_revision();
        inner.notify_ownership(
            &self.prefix,
            OwnershipWatchBatch {
                revision,
                events: vec![OwnershipWatchEvent::InstanceDeleted {
                    record: record.clone(),
                }],
            },
        );
        Some(record)
    }

    fn prefixed_actor_key(&self, key: &ActorPlacementKey) -> PrefixedActorKey {
        PrefixedActorKey {
            prefix: self.prefix.clone(),
            key: key.clone(),
        }
    }

    fn prefixed_vshard_key(&self, key: &VirtualShardPlacementKey) -> PrefixedVShardKey {
        PrefixedVShardKey {
            prefix: self.prefix.clone(),
            key: key.clone(),
        }
    }

    fn prefixed_singleton_key(&self, key: &SingletonKey) -> PrefixedSingletonKey {
        PrefixedSingletonKey {
            prefix: self.prefix.clone(),
            key: key.clone(),
        }
    }

    fn prefixed_instance_key(&self, instance_id: &InstanceId) -> PrefixedInstanceKey {
        PrefixedInstanceKey {
            prefix: self.prefix.clone(),
            instance_id: instance_id.clone(),
        }
    }
}

#[async_trait]
impl PlacementStore for InMemoryPlacementStore {
    async fn grant_instance_lease(&self) -> Result<LeaseId, PlacementError> {
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let lease_id = LeaseId(self.next_lease_id.fetch_add(1, Ordering::SeqCst));
        inner.instance_leases.insert(lease_id, 0);
        Ok(lease_id)
    }

    async fn keepalive_instance_lease(&self, lease_id: LeaseId) -> Result<(), PlacementError> {
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let Some(keepalives) = inner.instance_leases.get_mut(&lease_id) else {
            return Err(PlacementError::InstanceLeaseNotFound { lease_id });
        };
        *keepalives += 1;
        Ok(())
    }

    async fn campaign_coordinator_leader(
        &self,
        candidate_id: InstanceId,
    ) -> Result<Option<CoordinatorLeadership>, PlacementError> {
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        if inner.coordinator_leader.is_some() {
            return Ok(None);
        }
        let leadership = CoordinatorLeadership {
            candidate_id,
            lease_id: LeaseId(self.next_lease_id.fetch_add(1, Ordering::SeqCst)),
        };
        inner.instance_leases.insert(leadership.lease_id, 0);
        inner.coordinator_leader = Some(leadership.clone());
        Ok(Some(leadership))
    }

    async fn keepalive_coordinator_leader(
        &self,
        leadership: &CoordinatorLeadership,
    ) -> Result<(), PlacementError> {
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        if inner.coordinator_leader.as_ref() != Some(leadership) {
            return Err(PlacementError::CoordinatorLeadershipLost);
        }
        let Some(keepalives) = inner.instance_leases.get_mut(&leadership.lease_id) else {
            return Err(PlacementError::InstanceLeaseNotFound {
                lease_id: leadership.lease_id,
            });
        };
        *keepalives += 1;
        Ok(())
    }

    async fn resign_coordinator_leader(
        &self,
        leadership: &CoordinatorLeadership,
    ) -> Result<(), PlacementError> {
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        if inner.coordinator_leader.as_ref() == Some(leadership) {
            inner.coordinator_leader = None;
            inner.instance_leases.remove(&leadership.lease_id);
        }
        Ok(())
    }

    async fn upsert_instance(&self, record: InstanceRecord) -> Result<(), PlacementError> {
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let revision = inner.next_placement_revision();
        inner.instances.insert(
            self.prefixed_instance_key(&record.instance_id),
            record.clone(),
        );
        inner.notify(
            &self.prefix,
            PlacementWatchEvent::InstanceUpdated {
                record: record.clone(),
            },
        );
        inner.notify_ownership(
            &self.prefix,
            OwnershipWatchBatch {
                revision,
                events: vec![OwnershipWatchEvent::InstanceUpserted { record }],
            },
        );
        Ok(())
    }

    async fn get_instance(
        &self,
        instance_id: &InstanceId,
    ) -> Result<Option<InstanceRecord>, PlacementError> {
        Ok(self
            .inner
            .lock()
            .expect("placement store mutex poisoned")
            .instances
            .get(&self.prefixed_instance_key(instance_id))
            .cloned())
    }

    async fn list_instances(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<InstanceRecord>, PlacementError> {
        Ok(self
            .inner
            .lock()
            .expect("placement store mutex poisoned")
            .instances
            .iter()
            .filter(|(key, record)| {
                key.prefix == self.prefix && &record.service_kind == service_kind
            })
            .map(|(_, record)| record.clone())
            .collect())
    }

    async fn list_all_instances(&self) -> Result<Vec<InstanceRecord>, PlacementError> {
        Ok(self
            .inner
            .lock()
            .expect("placement store mutex poisoned")
            .instances
            .iter()
            .filter(|(key, _)| key.prefix == self.prefix)
            .map(|(_, record)| record.clone())
            .collect())
    }

    async fn get_actor(
        &self,
        key: &ActorPlacementKey,
    ) -> Result<Option<(PlacementVersion, ActorPlacementRecord)>, PlacementError> {
        Ok(self
            .inner
            .lock()
            .expect("placement store mutex poisoned")
            .actors
            .get(&self.prefixed_actor_key(key))
            .map(|(version, _revision, record)| (*version, record.clone())))
    }

    async fn list_actors(
        &self,
    ) -> Result<Vec<(PlacementVersion, ActorPlacementRecord)>, PlacementError> {
        Ok(self
            .inner
            .lock()
            .expect("placement store mutex poisoned")
            .actors
            .iter()
            .filter(|(key, _)| key.prefix == self.prefix)
            .map(|(_, (version, _revision, record))| (*version, record.clone()))
            .collect())
    }

    async fn compare_and_put_actor(
        &self,
        key: ActorPlacementKey,
        expected: Option<PlacementVersion>,
        value: ActorPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let key = self.prefixed_actor_key(&key);
        let current = inner
            .actors
            .get(&key)
            .map(|(version, _revision, _record)| *version);
        if current != expected {
            return Err(PlacementError::CompareAndPutFailed);
        }
        let next = PlacementVersion(current.map_or(1, |version| version.0 + 1));
        let revision = inner.next_placement_revision();
        let watch_key = key.key.clone();
        inner.actors.insert(key, (next, revision, value.clone()));
        inner.notify(
            &self.prefix,
            PlacementWatchEvent::ActorUpdated {
                key: watch_key.clone(),
                version: next,
                record: value.clone(),
            },
        );
        inner.notify_ownership(
            &self.prefix,
            OwnershipWatchBatch {
                revision,
                events: vec![OwnershipWatchEvent::ActorUpserted {
                    key: watch_key,
                    record: value,
                }],
            },
        );
        Ok(next)
    }

    async fn get_virtual_shard(
        &self,
        key: &VirtualShardPlacementKey,
    ) -> Result<Option<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError> {
        Ok(self
            .inner
            .lock()
            .expect("placement store mutex poisoned")
            .vshards
            .get(&self.prefixed_vshard_key(key))
            .map(|(version, _revision, record)| (*version, record.clone())))
    }

    async fn list_virtual_shards(
        &self,
        service_kind: &ServiceKind,
        actor_kind: &ActorKind,
    ) -> Result<Vec<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError> {
        Ok(self
            .inner
            .lock()
            .expect("placement store mutex poisoned")
            .vshards
            .iter()
            .filter(|(key, _)| {
                key.prefix == self.prefix
                    && &key.key.service_kind == service_kind
                    && &key.key.actor_kind == actor_kind
            })
            .map(|(_, (version, _revision, record))| (*version, record.clone()))
            .collect())
    }

    async fn list_virtual_shards_for_service(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError> {
        Ok(self
            .inner
            .lock()
            .expect("placement store mutex poisoned")
            .vshards
            .iter()
            .filter(|(key, _)| key.prefix == self.prefix && &key.key.service_kind == service_kind)
            .map(|(_, (version, _revision, record))| (*version, record.clone()))
            .collect())
    }

    async fn compare_and_put_virtual_shard(
        &self,
        key: VirtualShardPlacementKey,
        expected: Option<PlacementVersion>,
        value: VirtualShardPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let key = self.prefixed_vshard_key(&key);
        let current = inner
            .vshards
            .get(&key)
            .map(|(version, _revision, _record)| *version);
        if current != expected {
            return Err(PlacementError::CompareAndPutFailed);
        }
        let next = PlacementVersion(current.map_or(1, |version| version.0 + 1));
        let revision = inner.next_placement_revision();
        let watch_key = key.key.clone();
        inner.vshards.insert(key, (next, revision, value.clone()));
        inner.notify(
            &self.prefix,
            PlacementWatchEvent::VirtualShardUpdated {
                key: watch_key.clone(),
                version: next,
                record: value.clone(),
            },
        );
        inner.notify_ownership(
            &self.prefix,
            OwnershipWatchBatch {
                revision,
                events: vec![OwnershipWatchEvent::VirtualShardUpserted {
                    key: watch_key,
                    record: value,
                }],
            },
        );
        Ok(next)
    }

    async fn get_singleton(
        &self,
        key: &SingletonKey,
    ) -> Result<Option<(PlacementVersion, SingletonPlacementRecord)>, PlacementError> {
        Ok(self
            .inner
            .lock()
            .expect("placement store mutex poisoned")
            .singletons
            .get(&self.prefixed_singleton_key(key))
            .map(|(version, _revision, record)| (*version, record.clone())))
    }

    async fn list_singletons(
        &self,
    ) -> Result<Vec<(PlacementVersion, SingletonPlacementRecord)>, PlacementError> {
        Ok(self
            .inner
            .lock()
            .expect("placement store mutex poisoned")
            .singletons
            .iter()
            .filter(|(key, _)| key.prefix == self.prefix)
            .map(|(_, (version, _revision, record))| (*version, record.clone()))
            .collect())
    }

    async fn compare_and_put_singleton(
        &self,
        key: SingletonKey,
        expected: Option<PlacementVersion>,
        value: SingletonPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let key = self.prefixed_singleton_key(&key);
        let current = inner
            .singletons
            .get(&key)
            .map(|(version, _revision, _record)| *version);
        if current != expected {
            return Err(PlacementError::CompareAndPutFailed);
        }
        let next = PlacementVersion(current.map_or(1, |version| version.0 + 1));
        let revision = inner.next_placement_revision();
        let watch_key = key.key.clone();
        inner
            .singletons
            .insert(key, (next, revision, value.clone()));
        inner.notify(
            &self.prefix,
            PlacementWatchEvent::SingletonUpdated {
                key: watch_key.clone(),
                version: next,
                record: value.clone(),
            },
        );
        inner.notify_ownership(
            &self.prefix,
            OwnershipWatchBatch {
                revision,
                events: vec![OwnershipWatchEvent::SingletonUpserted {
                    key: watch_key,
                    record: value,
                }],
            },
        );
        Ok(next)
    }

    async fn acquire_singleton_lock(&self, key: SingletonKey) -> Result<LeaseId, PlacementError> {
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let key = self.prefixed_singleton_key(&key);
        if inner.singleton_locks.contains_key(&key) {
            return Err(PlacementError::SingletonLockHeld);
        }
        let lease = LeaseId(self.next_lease_id.fetch_add(1, Ordering::SeqCst));
        inner.singleton_locks.insert(key, lease);
        Ok(lease)
    }

    async fn validate_singleton_lock(
        &self,
        key: &SingletonKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError> {
        let inner = self.inner.lock().expect("placement store mutex poisoned");
        match inner.singleton_locks.get(&self.prefixed_singleton_key(key)) {
            Some(current) if *current == lease_id => Ok(()),
            _ => Err(PlacementError::SingletonLockLost),
        }
    }

    async fn release_singleton_lock(
        &self,
        key: &SingletonKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError> {
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let key = self.prefixed_singleton_key(key);
        match inner.singleton_locks.get(&key) {
            Some(current) if *current == lease_id => {
                inner.singleton_locks.remove(&key);
                Ok(())
            }
            _ => Err(PlacementError::SingletonLockLost),
        }
    }

    async fn acquire_activation_lock(
        &self,
        key: ActorPlacementKey,
    ) -> Result<LeaseId, PlacementError> {
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let key = self.prefixed_actor_key(&key);
        if inner.activation_locks.contains_key(&key) {
            return Err(PlacementError::ActivationLockHeld);
        }
        let lease = LeaseId(self.next_lease_id.fetch_add(1, Ordering::SeqCst));
        inner.activation_locks.insert(key, lease);
        Ok(lease)
    }

    async fn validate_activation_lock(
        &self,
        key: &ActorPlacementKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError> {
        let inner = self.inner.lock().expect("placement store mutex poisoned");
        match inner.activation_locks.get(&self.prefixed_actor_key(key)) {
            Some(current) if *current == lease_id => Ok(()),
            _ => Err(PlacementError::ActivationLockLost),
        }
    }

    async fn release_activation_lock(
        &self,
        key: &ActorPlacementKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError> {
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let key = self.prefixed_actor_key(key);
        match inner.activation_locks.get(&key) {
            Some(current) if *current == lease_id => {
                inner.activation_locks.remove(&key);
                Ok(())
            }
            _ => Err(PlacementError::ActivationLockLost),
        }
    }

    async fn open_ownership_view(
        &self,
        service_kind: &ServiceKind,
        instance_id: &InstanceId,
        max_entries: NonZeroUsize,
    ) -> Result<OwnershipView, OwnershipViewError> {
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let local_instance = inner
            .instances
            .get(&self.prefixed_instance_key(instance_id))
            .filter(|record| &record.service_kind == service_kind)
            .cloned();
        let mut records = Vec::new();
        let mut scanned_entries = 0;

        for (key, (_version, revision, record)) in &inner.actors {
            if key.prefix == self.prefix && &record.service_kind == service_kind {
                ensure_ownership_view_capacity(scanned_entries, max_entries)?;
                scanned_entries += 1;
            }
            if key.prefix == self.prefix
                && &record.service_kind == service_kind
                && &record.owner == instance_id
            {
                records.push(OwnershipViewRecord::Actor {
                    revision: *revision,
                    record: record.clone(),
                });
            }
        }
        for (key, (_version, revision, record)) in &inner.vshards {
            if key.prefix == self.prefix && &record.service_kind == service_kind {
                ensure_ownership_view_capacity(scanned_entries, max_entries)?;
                scanned_entries += 1;
            }
            if key.prefix == self.prefix
                && &record.service_kind == service_kind
                && &record.owner == instance_id
            {
                records.push(OwnershipViewRecord::VirtualShard {
                    revision: *revision,
                    record: record.clone(),
                });
            }
        }
        for (key, (_version, revision, record)) in &inner.singletons {
            if key.prefix == self.prefix && &record.service_kind == service_kind {
                ensure_ownership_view_capacity(scanned_entries, max_entries)?;
                scanned_entries += 1;
            }
            if key.prefix == self.prefix
                && &record.service_kind == service_kind
                && &record.owner == instance_id
            {
                records.push(OwnershipViewRecord::Singleton {
                    revision: *revision,
                    record: record.clone(),
                });
            }
        }

        let snapshot = OwnershipViewSnapshot {
            revision: PlacementRevision(inner.placement_revision),
            local_instance,
            records,
        };
        let rx = inner
            .ownership_watchers
            .entry(self.prefix.clone())
            .or_insert_with(|| {
                let (tx, _rx) = broadcast::channel(WATCH_CAPACITY);
                tx
            })
            .subscribe();
        Ok(OwnershipView {
            snapshot,
            watch: OwnershipWatch::new(rx),
        })
    }

    async fn watch(&self, prefix: PlacementPrefix) -> Result<PlacementWatch, PlacementError> {
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let rx = inner
            .watchers
            .entry(prefix)
            .or_insert_with(|| {
                let (tx, _rx) = broadcast::channel(WATCH_CAPACITY);
                tx
            })
            .subscribe();
        Ok(PlacementWatch::new(rx))
    }

    fn prefix(&self) -> &PlacementPrefix {
        &self.prefix
    }
}

#[derive(Debug, Default)]
struct PlacementStoreInner {
    placement_revision: u64,
    instance_leases: HashMap<LeaseId, u64>,
    coordinator_leader: Option<CoordinatorLeadership>,
    instances: HashMap<PrefixedInstanceKey, InstanceRecord>,
    actors: HashMap<PrefixedActorKey, (PlacementVersion, PlacementRevision, ActorPlacementRecord)>,
    vshards: HashMap<
        PrefixedVShardKey,
        (
            PlacementVersion,
            PlacementRevision,
            VirtualShardPlacementRecord,
        ),
    >,
    singletons: HashMap<
        PrefixedSingletonKey,
        (
            PlacementVersion,
            PlacementRevision,
            SingletonPlacementRecord,
        ),
    >,
    activation_locks: HashMap<PrefixedActorKey, LeaseId>,
    singleton_locks: HashMap<PrefixedSingletonKey, LeaseId>,
    watchers: HashMap<PlacementPrefix, broadcast::Sender<PlacementWatchEvent>>,
    ownership_watchers: HashMap<PlacementPrefix, broadcast::Sender<OwnershipWatchMessage>>,
}

impl PlacementStoreInner {
    fn next_placement_revision(&mut self) -> PlacementRevision {
        self.placement_revision = self
            .placement_revision
            .checked_add(1)
            .expect("in-memory placement revision exhausted");
        PlacementRevision(self.placement_revision)
    }

    fn notify(&self, prefix: &PlacementPrefix, event: PlacementWatchEvent) {
        if let Some(tx) = self.watchers.get(prefix) {
            let _ = tx.send(event);
        }
    }

    fn notify_ownership(&self, prefix: &PlacementPrefix, batch: OwnershipWatchBatch) {
        if let Some(tx) = self.ownership_watchers.get(prefix) {
            let _ = tx.send(OwnershipWatchMessage::Update(OwnershipWatchUpdate::Batch(
                batch,
            )));
        }
    }
}

fn ensure_ownership_view_capacity(
    current_len: usize,
    max_entries: NonZeroUsize,
) -> Result<(), OwnershipViewError> {
    if current_len >= max_entries.get() {
        return Err(OwnershipViewError::CapacityExceeded {
            max_entries: max_entries.get(),
        });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PrefixedInstanceKey {
    prefix: PlacementPrefix,
    instance_id: InstanceId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PrefixedActorKey {
    prefix: PlacementPrefix,
    key: ActorPlacementKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PrefixedVShardKey {
    prefix: PlacementPrefix,
    key: VirtualShardPlacementKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PrefixedSingletonKey {
    prefix: PlacementPrefix,
    key: SingletonKey,
}
