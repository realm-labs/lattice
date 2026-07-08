use std::time::Duration;

use lattice_core::actor_ref::Epoch;
use lattice_core::id::ActorId;
use lattice_core::instance::InstanceId;
use lattice_core::kind::{ActorKind, ServiceKind};
use tracing::Instrument;

use crate::coordination::logic::LogicControl;
use crate::coordination::reports::{DrainReport, FailoverReport};
use crate::coordination::singleton::SingletonControl;
use crate::error::PlacementError;
use crate::registry::InstanceState;
use crate::storage::{
    ActorPlacementKey, ActorPlacementRecord, LeaseId, PlacementState, PlacementStore, SingletonKey,
    SingletonPlacementRecord, VirtualShardPlacementKey, VirtualShardPlacementRecord,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivateActorRequest {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub actor_id: ActorId,
}

#[derive(Debug, Clone)]
pub struct PlacementCoordinator<S, L> {
    pub(crate) store: S,
    pub(crate) logic: L,
}

impl<S, L> PlacementCoordinator<S, L> {
    pub fn new(store: S, logic: L) -> Self {
        Self { store, logic }
    }
}

impl<S, L> PlacementCoordinator<S, L>
where
    S: Clone,
    L: Clone,
{
    pub(crate) fn parts(&self) -> (S, L) {
        (self.store.clone(), self.logic.clone())
    }
}

impl<S, L> PlacementCoordinator<S, L>
where
    S: PlacementStore,
    L: LogicControl,
{
    pub async fn activate_actor(
        &self,
        request: ActivateActorRequest,
    ) -> Result<ActorPlacementRecord, PlacementError> {
        let service_kind = request.service_kind;
        let key = ActorPlacementKey {
            service_kind: service_kind.clone(),
            actor_kind: request.actor_kind,
            actor_id: request.actor_id,
        };
        let span = tracing::info_span!(
            "placement.activate",
            otel.kind = "internal",
            service.kind = service_kind.as_str(),
            actor.kind = key.actor_kind.as_str(),
            actor.id = ?key.actor_id
        );
        async {
            if let Some((_, record)) = self.store.get_actor(&key).await? {
                return Ok(record);
            }

            let lock_span = tracing::info_span!(
                "placement.lock.acquire",
                otel.kind = "internal",
                lock.kind = "actor_activation",
                actor.kind = key.actor_kind.as_str(),
                actor.id = ?key.actor_id
            );
            let lease_id = match self
                .store
                .acquire_activation_lock(key.clone())
                .instrument(lock_span)
                .await
            {
                Ok(lease_id) => lease_id,
                Err(PlacementError::ActivationLockHeld) => {
                    return self.wait_for_existing_owner(&key).await;
                }
                Err(error) => return Err(error),
            };

            let result = self
                .activate_actor_with_lock(service_kind, key.clone(), lease_id)
                .await;
            let release_span = tracing::info_span!(
                "placement.lock.release",
                otel.kind = "internal",
                lock.kind = "actor_activation",
                actor.kind = key.actor_kind.as_str(),
                actor.id = ?key.actor_id
            );
            self.store
                .release_activation_lock(&key, lease_id)
                .instrument(release_span)
                .await?;
            result
        }
        .instrument(span)
        .await
    }

    pub async fn move_actor(
        &self,
        key: ActorPlacementKey,
        new_owner: InstanceId,
    ) -> Result<ActorPlacementRecord, PlacementError> {
        let span = tracing::info_span!(
            "placement.owner.move",
            otel.kind = "internal",
            actor.kind = key.actor_kind.as_str(),
            actor.id = ?key.actor_id,
            new.owner = new_owner.as_str()
        );
        async {
            let (version, current) = self
                .store
                .get_actor(&key)
                .await?
                .ok_or(PlacementError::NoRoute)?;
            let record = ActorPlacementRecord {
                owner: new_owner,
                epoch: Epoch(current.epoch.0 + 1),
                lease_id: LeaseId(current.lease_id.0 + 1),
                state: PlacementState::Running,
                ..current
            };
            self.store
                .compare_and_put_actor(key, Some(version), record.clone())
                .await?;
            Ok(record)
        }
        .instrument(span)
        .await
    }

    pub async fn drain_instance(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
    ) -> Result<DrainReport, PlacementError> {
        let span = tracing::info_span!(
            "placement.drain",
            otel.kind = "internal",
            service.kind = service_kind.as_str(),
            instance.id = instance_id.as_str()
        );
        async {
            let mut instance = self
                .store
                .get_instance(&instance_id)
                .await?
                .ok_or_else(|| PlacementError::InstanceNotFound {
                    instance_id: instance_id.clone(),
                })?;
            instance.state = InstanceState::Draining;
            self.store.upsert_instance(instance).await?;

            let replacement = self
                .store
                .list_instances(&service_kind)
                .await?
                .into_iter()
                .filter(|candidate| {
                    candidate.state == InstanceState::Ready && candidate.instance_id != instance_id
                })
                .min_by_key(|candidate| candidate.instance_id.clone())
                .ok_or(PlacementError::NoReadyInstances)?;
            let mut migrated_actors = 0;
            for (version, record) in self.store.list_actors().await? {
                if record.service_kind != service_kind || record.owner != instance_id {
                    continue;
                }
                let key = ActorPlacementKey {
                    service_kind: record.service_kind.clone(),
                    actor_kind: record.actor_kind.clone(),
                    actor_id: record.actor_id.clone(),
                };
                let migrated = ActorPlacementRecord {
                    owner: replacement.instance_id.clone(),
                    epoch: Epoch(record.epoch.0 + 1),
                    lease_id: LeaseId(record.lease_id.0 + 1),
                    state: PlacementState::Running,
                    ..record
                };
                self.store
                    .compare_and_put_actor(key, Some(version), migrated)
                    .await?;
                migrated_actors += 1;
            }
            let mut migrated_virtual_shards = 0;
            for (version, record) in self
                .store
                .list_virtual_shards_for_service(&service_kind)
                .await?
            {
                if record.owner != instance_id {
                    continue;
                }
                let key = VirtualShardPlacementKey {
                    service_kind: record.service_kind.clone(),
                    actor_kind: record.actor_kind.clone(),
                    shard_id: record.shard_id,
                };
                let migrated = VirtualShardPlacementRecord {
                    owner: replacement.instance_id.clone(),
                    epoch: Epoch(record.epoch.0 + 1),
                    ..record
                };
                self.store
                    .compare_and_put_virtual_shard(key, Some(version), migrated)
                    .await?;
                migrated_virtual_shards += 1;
            }

            Ok(DrainReport {
                drained_instance: instance_id,
                migrated_actors,
                migrated_virtual_shards,
            })
        }
        .instrument(span)
        .await
    }

    pub async fn failover_expired_instance(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
    ) -> Result<FailoverReport, PlacementError>
    where
        L: SingletonControl,
    {
        let span = tracing::info_span!(
            "placement.failover",
            otel.kind = "internal",
            service.kind = service_kind.as_str(),
            instance.id = instance_id.as_str()
        );
        async {
            let replacement = self
                .store
                .list_instances(&service_kind)
                .await?
                .into_iter()
                .filter(|candidate| {
                    candidate.state == InstanceState::Ready && candidate.instance_id != instance_id
                })
                .min_by_key(|candidate| candidate.instance_id.clone())
                .ok_or(PlacementError::NoReadyInstances)?;
            let mut reassigned_actors = 0;
            for (version, record) in self.store.list_actors().await? {
                if record.service_kind != service_kind || record.owner != instance_id {
                    continue;
                }
                let key = ActorPlacementKey {
                    service_kind: record.service_kind.clone(),
                    actor_kind: record.actor_kind.clone(),
                    actor_id: record.actor_id.clone(),
                };
                let reassigned = ActorPlacementRecord {
                    owner: replacement.instance_id.clone(),
                    epoch: Epoch(record.epoch.0 + 1),
                    lease_id: LeaseId(record.lease_id.0 + 1),
                    state: PlacementState::Running,
                    ..record
                };
                self.store
                    .compare_and_put_actor(key, Some(version), reassigned)
                    .await?;
                reassigned_actors += 1;
            }

            let mut reassigned_singletons = 0;
            for (version, record) in self.store.list_singletons().await? {
                if record.service_kind != service_kind || record.owner != instance_id {
                    continue;
                }
                let key = SingletonKey {
                    service_kind: record.service_kind.clone(),
                    singleton_kind: record.singleton_kind.clone(),
                    scope: record.scope.clone(),
                };
                let lease_id = self.store.grant_instance_lease().await?;
                let reassigned = SingletonPlacementRecord {
                    owner: replacement.instance_id.clone(),
                    epoch: Epoch(record.epoch.0 + 1),
                    lease_id,
                    state: PlacementState::Running,
                    ..record
                };
                self.logic
                    .activate_singleton(&replacement, &key, reassigned.epoch)
                    .await?;
                self.store
                    .compare_and_put_singleton(key, Some(version), reassigned)
                    .await?;
                reassigned_singletons += 1;
            }

            Ok(FailoverReport {
                failed_instance: instance_id,
                reassigned_actors,
                reassigned_singletons,
            })
        }
        .instrument(span)
        .await
    }

    async fn activate_actor_with_lock(
        &self,
        service_kind: ServiceKind,
        key: ActorPlacementKey,
        lease_id: LeaseId,
    ) -> Result<ActorPlacementRecord, PlacementError> {
        if let Some((_, record)) = self.store.get_actor(&key).await? {
            return Ok(record);
        }

        let instance = self
            .store
            .list_instances(&service_kind)
            .await?
            .into_iter()
            .filter(|instance| instance.state == InstanceState::Ready)
            .min_by_key(|instance| instance.instance_id.clone())
            .ok_or(PlacementError::NoReadyInstances)?;
        let record = ActorPlacementRecord {
            service_kind: key.service_kind.clone(),
            actor_kind: key.actor_kind.clone(),
            actor_id: key.actor_id.clone(),
            owner: instance.instance_id.clone(),
            epoch: Epoch(1),
            lease_id,
            state: PlacementState::Running,
        };
        self.logic
            .activate_actor(&instance, &key, record.epoch)
            .await?;
        self.store.validate_activation_lock(&key, lease_id).await?;
        self.store
            .compare_and_put_actor(key, None, record.clone())
            .await?;
        Ok(record)
    }

    async fn wait_for_existing_owner(
        &self,
        key: &ActorPlacementKey,
    ) -> Result<ActorPlacementRecord, PlacementError> {
        for _ in 0..50 {
            if let Some((_, record)) = self.store.get_actor(key).await? {
                return Ok(record);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        Err(PlacementError::ActivationLockHeld)
    }
}

#[cfg(test)]
mod tests;
