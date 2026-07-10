use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use lattice_core::actor_ref::Epoch;
use lattice_core::instance::InstanceId;
use lattice_core::kind::{ActorKind, ServiceKind};
use tokio::sync::broadcast;

use crate::error::PlacementError;
use crate::registry::InstanceRecord;
use crate::storage::{
    ActorPlacementKey, ActorPlacementRecord, CoordinatorLeadership, EpochFloorRecord, LeaseId,
    OwnershipEpochFloorProof, OwnershipProofContext, OwnershipProofError, OwnershipRecordBinding,
    OwnershipView, OwnershipViewError, OwnershipViewRecord, OwnershipViewSnapshot, OwnershipWatch,
    OwnershipWatchBatch, OwnershipWatchError, OwnershipWatchEvent, OwnershipWatchMessage,
    OwnershipWatchUpdate, PlacementEpochGuard, PlacementEpochKey, PlacementEpochReservation,
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

    #[cfg(test)]
    pub(crate) fn remove_actor_for_test(
        &self,
        key: &ActorPlacementKey,
    ) -> Option<ActorPlacementRecord> {
        let epoch_key = PlacementEpochKey::Actor(key.clone());
        let floor_key = self.prefixed_epoch_key(&epoch_key);
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let (token, _record_revision, record) =
            inner.actors.remove(&self.prefixed_actor_key(key))?;
        let revision = inner.next_placement_revision();
        let proof = memory_ownership_proof(
            &inner,
            &floor_key,
            OwnershipProofContext::Delete,
            revision,
            token,
            OwnershipRecordBinding::Actor(record.clone()),
        );
        inner.notify_ownership_event(
            &self.prefix,
            revision,
            proof.map(|proof| OwnershipWatchEvent::ActorDeleted {
                key: key.clone(),
                previous_record: record.clone(),
                proof,
            }),
        );
        Some(record)
    }

    #[cfg(test)]
    pub(crate) fn remove_virtual_shard_for_test(
        &self,
        key: &VirtualShardPlacementKey,
    ) -> Option<VirtualShardPlacementRecord> {
        let epoch_key = PlacementEpochKey::VirtualShard(key.clone());
        let floor_key = self.prefixed_epoch_key(&epoch_key);
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let (token, _record_revision, record) =
            inner.vshards.remove(&self.prefixed_vshard_key(key))?;
        let revision = inner.next_placement_revision();
        let proof = memory_ownership_proof(
            &inner,
            &floor_key,
            OwnershipProofContext::Delete,
            revision,
            token,
            OwnershipRecordBinding::VirtualShard(record.clone()),
        );
        inner.notify_ownership_event(
            &self.prefix,
            revision,
            proof.map(|proof| OwnershipWatchEvent::VirtualShardDeleted {
                key: key.clone(),
                previous_record: record.clone(),
                proof,
            }),
        );
        Some(record)
    }

    #[cfg(test)]
    pub(crate) fn remove_singleton_for_test(
        &self,
        key: &SingletonKey,
    ) -> Option<SingletonPlacementRecord> {
        let epoch_key = PlacementEpochKey::Singleton(key.clone());
        let floor_key = self.prefixed_epoch_key(&epoch_key);
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let (token, _record_revision, record) =
            inner.singletons.remove(&self.prefixed_singleton_key(key))?;
        let revision = inner.next_placement_revision();
        let proof = memory_ownership_proof(
            &inner,
            &floor_key,
            OwnershipProofContext::Delete,
            revision,
            token,
            OwnershipRecordBinding::Singleton(record.clone()),
        );
        inner.notify_ownership_event(
            &self.prefix,
            revision,
            proof.map(|proof| OwnershipWatchEvent::SingletonDeleted {
                key: key.clone(),
                previous_record: record.clone(),
                proof,
            }),
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

    fn prefixed_epoch_key(&self, key: &PlacementEpochKey) -> PrefixedEpochKey {
        PrefixedEpochKey {
            prefix: self.prefix.clone(),
            key: key.clone(),
        }
    }

    #[cfg(test)]
    pub(crate) fn epoch_floor_for_test(
        &self,
        key: &PlacementEpochKey,
    ) -> Option<(PlacementVersion, EpochFloorRecord)> {
        self.inner
            .lock()
            .expect("placement store mutex poisoned")
            .epoch_floors
            .get(&self.prefixed_epoch_key(key))
            .cloned()
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

    async fn reserve_actor_epoch(
        &self,
        key: ActorPlacementKey,
        expected: Option<PlacementVersion>,
        activation_lock: Option<LeaseId>,
    ) -> Result<PlacementEpochReservation, PlacementError> {
        let placement_key = self.prefixed_actor_key(&key);
        let epoch_key = PlacementEpochKey::Actor(key);
        let floor_key = self.prefixed_epoch_key(&epoch_key);
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let current = inner
            .actors
            .get(&placement_key)
            .map(|(token, _revision, record)| (*token, record.epoch));
        if current.map(|(token, _epoch)| token) != expected {
            return Err(PlacementError::CompareAndPutFailed);
        }
        let guard = activation_lock.map(PlacementEpochGuard::Actor);
        validate_memory_guard(&inner, Some(&placement_key), None, guard)?;
        reserve_memory_epoch(&mut inner, floor_key, epoch_key, current, expected, guard)
    }

    async fn commit_actor_epoch(
        &self,
        reservation: PlacementEpochReservation,
        value: ActorPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        let PlacementEpochKey::Actor(key) = reservation.key().clone() else {
            return Err(PlacementError::EpochReservationMismatch);
        };
        if actor_record_key(&value) != key || value.epoch != reservation.epoch() {
            return Err(PlacementError::EpochReservationMismatch);
        }
        let placement_key = self.prefixed_actor_key(&key);
        let floor_key = self.prefixed_epoch_key(reservation.key());
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        validate_memory_reservation(
            &inner,
            &floor_key,
            reservation.key(),
            reservation.epoch(),
            reservation.floor_token(),
        )?;
        let current = inner
            .actors
            .get(&placement_key)
            .map(|(token, _revision, _record)| *token);
        if current != reservation.expected_record() {
            return Err(PlacementError::CompareAndPutFailed);
        }
        validate_memory_guard(&inner, Some(&placement_key), None, reservation.guard())?;

        let revision = inner.next_placement_revision();
        let token = placement_token(revision);
        let floor = EpochFloorRecord {
            key: reservation.key.clone(),
            epoch: reservation.epoch,
        };
        let proof = OwnershipEpochFloorProof::new(
            OwnershipProofContext::Upsert,
            revision,
            token,
            OwnershipRecordBinding::Actor(value.clone()),
            token,
            floor.clone(),
            None,
        );
        inner.epoch_floors.insert(floor_key, (token, floor));
        inner
            .actors
            .insert(placement_key, (token, revision, value.clone()));
        inner.notify(
            &self.prefix,
            PlacementWatchEvent::ActorUpdated {
                key: key.clone(),
                version: token,
                record: value.clone(),
            },
        );
        inner.notify_ownership_event(
            &self.prefix,
            revision,
            proof.map(|proof| OwnershipWatchEvent::ActorUpserted {
                key,
                record: value,
                proof,
            }),
        );
        Ok(token)
    }

    async fn compare_and_put_actor(
        &self,
        key: ActorPlacementKey,
        expected: Option<PlacementVersion>,
        value: ActorPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        if actor_record_key(&value) != key {
            return Err(PlacementError::EpochReservationMismatch);
        }
        let placement_key = self.prefixed_actor_key(&key);
        let epoch_key = PlacementEpochKey::Actor(key.clone());
        let floor_key = self.prefixed_epoch_key(&epoch_key);
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let current = inner
            .actors
            .get(&placement_key)
            .map(|(token, _revision, record)| (*token, record.clone()));
        if current.as_ref().map(|(token, _record)| *token) != expected {
            return Err(PlacementError::CompareAndPutFailed);
        }
        let floor = memory_floor(&inner, &floor_key, &epoch_key)?;
        crate::storage::validate_epoch_floor_lineage(
            current
                .as_ref()
                .map(|(token, record)| (*token, record.epoch)),
            floor.as_ref().map(|(token, floor)| (*token, floor.epoch)),
        )?;
        let authority_changed = current.as_ref().is_some_and(|(_token, record)| {
            record.owner != value.owner || record.lease_id != value.lease_id
        });
        let reactivating = current.as_ref().is_some_and(|(_token, record)| {
            record.state == crate::storage::PlacementState::Stopped
                && value.state != crate::storage::PlacementState::Stopped
        });
        crate::storage::validate_legacy_epoch(
            current.as_ref().map(|(_token, record)| record.epoch),
            floor.as_ref().map(|(_token, floor)| floor.epoch),
            value.epoch,
            authority_changed,
            reactivating,
        )?;
        let revision = inner.next_placement_revision();
        let token = placement_token(revision);
        let floor = EpochFloorRecord {
            key: epoch_key,
            epoch: value.epoch,
        };
        let proof = OwnershipEpochFloorProof::new(
            OwnershipProofContext::Upsert,
            revision,
            token,
            OwnershipRecordBinding::Actor(value.clone()),
            token,
            floor.clone(),
            None,
        );
        inner.epoch_floors.insert(floor_key, (token, floor));
        inner
            .actors
            .insert(placement_key, (token, revision, value.clone()));
        inner.notify(
            &self.prefix,
            PlacementWatchEvent::ActorUpdated {
                key: key.clone(),
                version: token,
                record: value.clone(),
            },
        );
        inner.notify_ownership_event(
            &self.prefix,
            revision,
            proof.map(|proof| OwnershipWatchEvent::ActorUpserted {
                key,
                record: value,
                proof,
            }),
        );
        Ok(token)
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

    async fn reserve_virtual_shard_epoch(
        &self,
        key: VirtualShardPlacementKey,
        expected: Option<PlacementVersion>,
    ) -> Result<PlacementEpochReservation, PlacementError> {
        let placement_key = self.prefixed_vshard_key(&key);
        let epoch_key = PlacementEpochKey::VirtualShard(key);
        let floor_key = self.prefixed_epoch_key(&epoch_key);
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let current = inner
            .vshards
            .get(&placement_key)
            .map(|(token, _revision, record)| (*token, record.epoch));
        if current.map(|(token, _epoch)| token) != expected {
            return Err(PlacementError::CompareAndPutFailed);
        }
        reserve_memory_epoch(&mut inner, floor_key, epoch_key, current, expected, None)
    }

    async fn commit_virtual_shard_epoch(
        &self,
        reservation: PlacementEpochReservation,
        value: VirtualShardPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        let PlacementEpochKey::VirtualShard(key) = reservation.key().clone() else {
            return Err(PlacementError::EpochReservationMismatch);
        };
        if virtual_shard_record_key(&value) != key || value.epoch != reservation.epoch() {
            return Err(PlacementError::EpochReservationMismatch);
        }
        let placement_key = self.prefixed_vshard_key(&key);
        let floor_key = self.prefixed_epoch_key(reservation.key());
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        validate_memory_reservation(
            &inner,
            &floor_key,
            reservation.key(),
            reservation.epoch(),
            reservation.floor_token(),
        )?;
        let current = inner
            .vshards
            .get(&placement_key)
            .map(|(token, _revision, _record)| *token);
        if current != reservation.expected_record() || reservation.guard().is_some() {
            return Err(PlacementError::CompareAndPutFailed);
        }
        let revision = inner.next_placement_revision();
        let token = placement_token(revision);
        let floor = EpochFloorRecord {
            key: reservation.key.clone(),
            epoch: reservation.epoch,
        };
        let proof = OwnershipEpochFloorProof::new(
            OwnershipProofContext::Upsert,
            revision,
            token,
            OwnershipRecordBinding::VirtualShard(value.clone()),
            token,
            floor.clone(),
            None,
        );
        inner.epoch_floors.insert(floor_key, (token, floor));
        inner
            .vshards
            .insert(placement_key, (token, revision, value.clone()));
        inner.notify(
            &self.prefix,
            PlacementWatchEvent::VirtualShardUpdated {
                key: key.clone(),
                version: token,
                record: value.clone(),
            },
        );
        inner.notify_ownership_event(
            &self.prefix,
            revision,
            proof.map(|proof| OwnershipWatchEvent::VirtualShardUpserted {
                key,
                record: value,
                proof,
            }),
        );
        Ok(token)
    }

    async fn compare_and_put_virtual_shard(
        &self,
        key: VirtualShardPlacementKey,
        expected: Option<PlacementVersion>,
        value: VirtualShardPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        if virtual_shard_record_key(&value) != key {
            return Err(PlacementError::EpochReservationMismatch);
        }
        let placement_key = self.prefixed_vshard_key(&key);
        let epoch_key = PlacementEpochKey::VirtualShard(key.clone());
        let floor_key = self.prefixed_epoch_key(&epoch_key);
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let current = inner
            .vshards
            .get(&placement_key)
            .map(|(token, _revision, record)| (*token, record.clone()));
        if current.as_ref().map(|(token, _record)| *token) != expected {
            return Err(PlacementError::CompareAndPutFailed);
        }
        let floor = memory_floor(&inner, &floor_key, &epoch_key)?;
        crate::storage::validate_epoch_floor_lineage(
            current
                .as_ref()
                .map(|(token, record)| (*token, record.epoch)),
            floor.as_ref().map(|(token, floor)| (*token, floor.epoch)),
        )?;
        let authority_changed = current
            .as_ref()
            .is_some_and(|(_token, record)| record.owner != value.owner);
        crate::storage::validate_legacy_epoch(
            current.as_ref().map(|(_token, record)| record.epoch),
            floor.as_ref().map(|(_token, floor)| floor.epoch),
            value.epoch,
            authority_changed,
            false,
        )?;
        let revision = inner.next_placement_revision();
        let token = placement_token(revision);
        let floor = EpochFloorRecord {
            key: epoch_key,
            epoch: value.epoch,
        };
        let proof = OwnershipEpochFloorProof::new(
            OwnershipProofContext::Upsert,
            revision,
            token,
            OwnershipRecordBinding::VirtualShard(value.clone()),
            token,
            floor.clone(),
            None,
        );
        inner.epoch_floors.insert(floor_key, (token, floor));
        inner
            .vshards
            .insert(placement_key, (token, revision, value.clone()));
        inner.notify(
            &self.prefix,
            PlacementWatchEvent::VirtualShardUpdated {
                key: key.clone(),
                version: token,
                record: value.clone(),
            },
        );
        inner.notify_ownership_event(
            &self.prefix,
            revision,
            proof.map(|proof| OwnershipWatchEvent::VirtualShardUpserted {
                key,
                record: value,
                proof,
            }),
        );
        Ok(token)
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

    async fn reserve_singleton_epoch(
        &self,
        key: SingletonKey,
        expected: Option<PlacementVersion>,
        singleton_lock: Option<LeaseId>,
    ) -> Result<PlacementEpochReservation, PlacementError> {
        let placement_key = self.prefixed_singleton_key(&key);
        let epoch_key = PlacementEpochKey::Singleton(key);
        let floor_key = self.prefixed_epoch_key(&epoch_key);
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let current = inner
            .singletons
            .get(&placement_key)
            .map(|(token, _revision, record)| (*token, record.epoch));
        if current.map(|(token, _epoch)| token) != expected {
            return Err(PlacementError::CompareAndPutFailed);
        }
        let guard = singleton_lock.map(PlacementEpochGuard::Singleton);
        validate_memory_guard(&inner, None, Some(&placement_key), guard)?;
        reserve_memory_epoch(&mut inner, floor_key, epoch_key, current, expected, guard)
    }

    async fn commit_singleton_epoch(
        &self,
        reservation: PlacementEpochReservation,
        value: SingletonPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        let PlacementEpochKey::Singleton(key) = reservation.key().clone() else {
            return Err(PlacementError::EpochReservationMismatch);
        };
        if singleton_record_key(&value) != key || value.epoch != reservation.epoch() {
            return Err(PlacementError::EpochReservationMismatch);
        }
        let placement_key = self.prefixed_singleton_key(&key);
        let floor_key = self.prefixed_epoch_key(reservation.key());
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        validate_memory_reservation(
            &inner,
            &floor_key,
            reservation.key(),
            reservation.epoch(),
            reservation.floor_token(),
        )?;
        let current = inner
            .singletons
            .get(&placement_key)
            .map(|(token, _revision, _record)| *token);
        if current != reservation.expected_record() {
            return Err(PlacementError::CompareAndPutFailed);
        }
        validate_memory_guard(&inner, None, Some(&placement_key), reservation.guard())?;
        let revision = inner.next_placement_revision();
        let token = placement_token(revision);
        let floor = EpochFloorRecord {
            key: reservation.key.clone(),
            epoch: reservation.epoch,
        };
        let proof = OwnershipEpochFloorProof::new(
            OwnershipProofContext::Upsert,
            revision,
            token,
            OwnershipRecordBinding::Singleton(value.clone()),
            token,
            floor.clone(),
            None,
        );
        inner.epoch_floors.insert(floor_key, (token, floor));
        inner
            .singletons
            .insert(placement_key, (token, revision, value.clone()));
        inner.notify(
            &self.prefix,
            PlacementWatchEvent::SingletonUpdated {
                key: key.clone(),
                version: token,
                record: value.clone(),
            },
        );
        inner.notify_ownership_event(
            &self.prefix,
            revision,
            proof.map(|proof| OwnershipWatchEvent::SingletonUpserted {
                key,
                record: value,
                proof,
            }),
        );
        Ok(token)
    }

    async fn compare_and_put_singleton(
        &self,
        key: SingletonKey,
        expected: Option<PlacementVersion>,
        value: SingletonPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        if singleton_record_key(&value) != key {
            return Err(PlacementError::EpochReservationMismatch);
        }
        let placement_key = self.prefixed_singleton_key(&key);
        let epoch_key = PlacementEpochKey::Singleton(key.clone());
        let floor_key = self.prefixed_epoch_key(&epoch_key);
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let current = inner
            .singletons
            .get(&placement_key)
            .map(|(token, _revision, record)| (*token, record.clone()));
        if current.as_ref().map(|(token, _record)| *token) != expected {
            return Err(PlacementError::CompareAndPutFailed);
        }
        let floor = memory_floor(&inner, &floor_key, &epoch_key)?;
        crate::storage::validate_epoch_floor_lineage(
            current
                .as_ref()
                .map(|(token, record)| (*token, record.epoch)),
            floor.as_ref().map(|(token, floor)| (*token, floor.epoch)),
        )?;
        let authority_changed = current.as_ref().is_some_and(|(_token, record)| {
            record.owner != value.owner || record.lease_id != value.lease_id
        });
        let reactivating = current.as_ref().is_some_and(|(_token, record)| {
            record.state == crate::storage::PlacementState::Stopped
                && value.state != crate::storage::PlacementState::Stopped
        });
        crate::storage::validate_legacy_epoch(
            current.as_ref().map(|(_token, record)| record.epoch),
            floor.as_ref().map(|(_token, floor)| floor.epoch),
            value.epoch,
            authority_changed,
            reactivating,
        )?;
        let revision = inner.next_placement_revision();
        let token = placement_token(revision);
        let floor = EpochFloorRecord {
            key: epoch_key,
            epoch: value.epoch,
        };
        let proof = OwnershipEpochFloorProof::new(
            OwnershipProofContext::Upsert,
            revision,
            token,
            OwnershipRecordBinding::Singleton(value.clone()),
            token,
            floor.clone(),
            None,
        );
        inner.epoch_floors.insert(floor_key, (token, floor));
        inner
            .singletons
            .insert(placement_key, (token, revision, value.clone()));
        inner.notify(
            &self.prefix,
            PlacementWatchEvent::SingletonUpdated {
                key: key.clone(),
                version: token,
                record: value.clone(),
            },
        );
        inner.notify_ownership_event(
            &self.prefix,
            revision,
            proof.map(|proof| OwnershipWatchEvent::SingletonUpserted {
                key,
                record: value,
                proof,
            }),
        );
        Ok(token)
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
        let snapshot_revision = PlacementRevision(inner.placement_revision);
        let local_instance = inner
            .instances
            .get(&self.prefixed_instance_key(instance_id))
            .filter(|record| &record.service_kind == service_kind)
            .cloned();
        let mut records = Vec::new();
        let mut scanned_entries = 0;

        for (key, (version, revision, record)) in &inner.actors {
            if key.prefix == self.prefix && &record.service_kind == service_kind {
                ensure_ownership_view_capacity(scanned_entries, max_entries)?;
                scanned_entries += 1;
            }
            if key.prefix == self.prefix && &record.service_kind == service_kind {
                let epoch_key = PlacementEpochKey::Actor(key.key.clone());
                let proof = memory_ownership_proof(
                    &inner,
                    &self.prefixed_epoch_key(&epoch_key),
                    OwnershipProofContext::Snapshot,
                    snapshot_revision,
                    *version,
                    OwnershipRecordBinding::Actor(record.clone()),
                )
                .map_err(|error| OwnershipViewError::Proof { error })?;
                records.push(OwnershipViewRecord::Actor {
                    revision: *revision,
                    record: record.clone(),
                    proof,
                });
            }
        }
        for (key, (version, revision, record)) in &inner.vshards {
            if key.prefix == self.prefix && &record.service_kind == service_kind {
                ensure_ownership_view_capacity(scanned_entries, max_entries)?;
                scanned_entries += 1;
            }
            if key.prefix == self.prefix && &record.service_kind == service_kind {
                let epoch_key = PlacementEpochKey::VirtualShard(key.key.clone());
                let proof = memory_ownership_proof(
                    &inner,
                    &self.prefixed_epoch_key(&epoch_key),
                    OwnershipProofContext::Snapshot,
                    snapshot_revision,
                    *version,
                    OwnershipRecordBinding::VirtualShard(record.clone()),
                )
                .map_err(|error| OwnershipViewError::Proof { error })?;
                records.push(OwnershipViewRecord::VirtualShard {
                    revision: *revision,
                    record: record.clone(),
                    proof,
                });
            }
        }
        for (key, (version, revision, record)) in &inner.singletons {
            if key.prefix == self.prefix && &record.service_kind == service_kind {
                ensure_ownership_view_capacity(scanned_entries, max_entries)?;
                scanned_entries += 1;
            }
            if key.prefix == self.prefix && &record.service_kind == service_kind {
                let epoch_key = PlacementEpochKey::Singleton(key.key.clone());
                let proof = memory_ownership_proof(
                    &inner,
                    &self.prefixed_epoch_key(&epoch_key),
                    OwnershipProofContext::Snapshot,
                    snapshot_revision,
                    *version,
                    OwnershipRecordBinding::Singleton(record.clone()),
                )
                .map_err(|error| OwnershipViewError::Proof { error })?;
                records.push(OwnershipViewRecord::Singleton {
                    revision: *revision,
                    record: record.clone(),
                    proof,
                });
            }
        }

        let snapshot = OwnershipViewSnapshot {
            revision: snapshot_revision,
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

fn placement_token(revision: PlacementRevision) -> PlacementVersion {
    PlacementVersion::from_modification_revision(revision.0)
}

fn reserve_memory_epoch(
    inner: &mut PlacementStoreInner,
    floor_key: PrefixedEpochKey,
    epoch_key: PlacementEpochKey,
    current: Option<(PlacementVersion, Epoch)>,
    expected: Option<PlacementVersion>,
    guard: Option<PlacementEpochGuard>,
) -> Result<PlacementEpochReservation, PlacementError> {
    let floor = memory_floor(inner, &floor_key, &epoch_key)?;
    crate::storage::validate_epoch_floor_lineage(
        current,
        floor.as_ref().map(|(token, floor)| (*token, floor.epoch)),
    )?;
    let epoch = crate::storage::next_reserved_epoch(
        current.map(|(_token, epoch)| epoch),
        floor.as_ref().map(|(_token, floor)| floor.epoch),
    )?;
    let revision = inner.next_placement_revision();
    let floor_token = placement_token(revision);
    inner.epoch_floors.insert(
        floor_key,
        (
            floor_token,
            EpochFloorRecord {
                key: epoch_key.clone(),
                epoch,
            },
        ),
    );
    Ok(PlacementEpochReservation::new(
        epoch_key,
        epoch,
        expected,
        floor_token,
        guard,
    ))
}

fn memory_floor(
    inner: &PlacementStoreInner,
    floor_key: &PrefixedEpochKey,
    expected_key: &PlacementEpochKey,
) -> Result<Option<(PlacementVersion, EpochFloorRecord)>, PlacementError> {
    let floor = inner.epoch_floors.get(floor_key).cloned();
    if floor
        .as_ref()
        .is_some_and(|(_token, floor)| &floor.key != expected_key)
    {
        return Err(PlacementError::EpochReservationMismatch);
    }
    Ok(floor)
}

fn memory_ownership_proof(
    inner: &PlacementStoreInner,
    floor_key: &PrefixedEpochKey,
    context: OwnershipProofContext,
    observed_revision: PlacementRevision,
    record_token: PlacementVersion,
    binding: OwnershipRecordBinding,
) -> Result<OwnershipEpochFloorProof, OwnershipProofError> {
    let expected_key = binding.epoch_key();
    let Some((floor_token, floor)) = inner.epoch_floors.get(floor_key).cloned() else {
        return Err(OwnershipProofError::MissingFloor {
            key: expected_key,
            observed_revision,
        });
    };
    OwnershipEpochFloorProof::new(
        context,
        observed_revision,
        record_token,
        binding,
        floor_token,
        floor,
        None,
    )
}

fn validate_memory_reservation(
    inner: &PlacementStoreInner,
    floor_key: &PrefixedEpochKey,
    expected_key: &PlacementEpochKey,
    expected_epoch: Epoch,
    expected_token: PlacementVersion,
) -> Result<(), PlacementError> {
    match memory_floor(inner, floor_key, expected_key)? {
        Some((token, floor)) if token == expected_token && floor.epoch == expected_epoch => Ok(()),
        _ => Err(PlacementError::CompareAndPutFailed),
    }
}

fn validate_memory_guard(
    inner: &PlacementStoreInner,
    actor_key: Option<&PrefixedActorKey>,
    singleton_key: Option<&PrefixedSingletonKey>,
    guard: Option<PlacementEpochGuard>,
) -> Result<(), PlacementError> {
    match guard {
        None => Ok(()),
        Some(PlacementEpochGuard::Actor(expected)) => match actor_key {
            Some(key) if inner.activation_locks.get(key) == Some(&expected) => Ok(()),
            _ => Err(PlacementError::ActivationLockLost),
        },
        Some(PlacementEpochGuard::Singleton(expected)) => match singleton_key {
            Some(key) if inner.singleton_locks.get(key) == Some(&expected) => Ok(()),
            _ => Err(PlacementError::SingletonLockLost),
        },
    }
}

fn actor_record_key(record: &ActorPlacementRecord) -> ActorPlacementKey {
    ActorPlacementKey {
        service_kind: record.service_kind.clone(),
        actor_kind: record.actor_kind.clone(),
        actor_id: record.actor_id.clone(),
    }
}

fn virtual_shard_record_key(record: &VirtualShardPlacementRecord) -> VirtualShardPlacementKey {
    VirtualShardPlacementKey {
        service_kind: record.service_kind.clone(),
        actor_kind: record.actor_kind.clone(),
        shard_id: record.shard_id,
    }
}

fn singleton_record_key(record: &SingletonPlacementRecord) -> SingletonKey {
    SingletonKey {
        service_kind: record.service_kind.clone(),
        singleton_kind: record.singleton_kind.clone(),
        scope: record.scope.clone(),
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
    epoch_floors: HashMap<PrefixedEpochKey, (PlacementVersion, EpochFloorRecord)>,
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

    fn notify_ownership_event(
        &mut self,
        prefix: &PlacementPrefix,
        revision: PlacementRevision,
        event: Result<OwnershipWatchEvent, OwnershipProofError>,
    ) {
        match event {
            Ok(event) => {
                if let Some(tx) = self.ownership_watchers.get(prefix) {
                    let _ = tx.send(OwnershipWatchMessage::Update(OwnershipWatchUpdate::Batch(
                        OwnershipWatchBatch {
                            revision,
                            events: vec![event],
                        },
                    )));
                }
            }
            Err(error) => {
                // A proof failure invalidates every receiver subscribed to this
                // coherent prefix. Remove the only sender after queuing the
                // terminal error so callers cannot continue on the same view.
                if let Some(tx) = self.ownership_watchers.remove(prefix) {
                    let _ = tx.send(OwnershipWatchMessage::Failed(OwnershipWatchError::Proof {
                        error,
                    }));
                }
            }
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PrefixedEpochKey {
    prefix: PlacementPrefix,
    key: PlacementEpochKey,
}

#[cfg(test)]
mod tests;
