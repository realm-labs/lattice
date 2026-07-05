use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use lattice_core::{ActorId, ActorKind, Epoch, InstanceId, RouteKey, ServiceKind};
use lattice_rpc::RouteTarget;
use tracing::Instrument;

use crate::cache::{CacheLookup, LocalRouteCache, RouteCacheConfig};
use crate::error::PlacementError;
use crate::instance::{InstanceRecord, InstanceState};
use crate::route::{InvalidateReason, ResolveRequest, RouteCacheKey, RouteResolver};
use crate::store::{
    ActorPlacementKey, ActorPlacementRecord, LeaseId, PlacementState, PlacementStore,
    PlacementWatchEvent, VirtualShardPlacementKey, VirtualShardPlacementRecord,
};
use crate::vshard::{
    VirtualShardAssignInput, VirtualShardAssigner, VirtualShardAssignment, VirtualShardId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivateActorRequest {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub actor_id: ActorId,
}

#[async_trait]
pub trait LogicControl: Clone + Send + Sync + 'static {
    async fn activate_actor(
        &self,
        instance: &InstanceRecord,
        key: &ActorPlacementKey,
        epoch: Epoch,
    ) -> Result<(), PlacementError>;
}

#[derive(Debug, Clone, Default)]
pub struct NoopLogicControl;

#[async_trait]
impl LogicControl for NoopLogicControl {
    async fn activate_actor(
        &self,
        _instance: &InstanceRecord,
        _key: &ActorPlacementKey,
        _epoch: Epoch,
    ) -> Result<(), PlacementError> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PlacementCoordinator<S, L> {
    store: S,
    logic: L,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainReport {
    pub drained_instance: InstanceId,
    pub migrated_actors: usize,
    pub migrated_virtual_shards: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailoverReport {
    pub failed_instance: InstanceId,
    pub reassigned_actors: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebalanceVirtualShardsRequest {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub shard_count: u32,
    pub eligible_shards: BTreeSet<VirtualShardId>,
    pub max_migrations: usize,
    pub movement_policy: VirtualShardMovementPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebalanceVirtualShardsReport {
    pub ready_instances: usize,
    pub assignments_written: usize,
    pub moved_shards: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VirtualShardMovementPolicy {
    EligibleOnly,
    AllowRunningMigration,
}

impl<S, L> PlacementCoordinator<S, L> {
    pub fn new(store: S, logic: L) -> Self {
        Self { store, logic }
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
                .release_activation_lock(&key)
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
                if record.owner != instance_id {
                    continue;
                }
                let key = ActorPlacementKey {
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
    ) -> Result<FailoverReport, PlacementError> {
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
                if record.owner != instance_id {
                    continue;
                }
                let key = ActorPlacementKey {
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

            Ok(FailoverReport {
                failed_instance: instance_id,
                reassigned_actors,
            })
        }
        .instrument(span)
        .await
    }

    pub async fn rebalance_virtual_shards<A>(
        &self,
        request: RebalanceVirtualShardsRequest,
        assigner: &A,
    ) -> Result<RebalanceVirtualShardsReport, PlacementError>
    where
        A: VirtualShardAssigner,
    {
        let span = tracing::info_span!(
            "placement.vshards.rebalance",
            otel.kind = "internal",
            service.kind = request.service_kind.as_str(),
            actor.kind = request.actor_kind.as_str(),
            shard.count = request.shard_count
        );
        async {
            let mut instances = self
                .store
                .list_instances(&request.service_kind)
                .await?
                .into_iter()
                .filter(|instance| instance.state == InstanceState::Ready)
                .map(|instance| instance.instance_id)
                .collect::<Vec<_>>();
            instances.sort();
            if instances.is_empty() {
                return Err(PlacementError::NoReadyInstances);
            }
            let ready_instances = instances.len();

            let existing = self
                .store
                .list_virtual_shards(&request.service_kind, &request.actor_kind)
                .await?;
            let previous = existing
                .iter()
                .map(|(_, record)| VirtualShardAssignment {
                    shard_id: record.shard_id,
                    owner: record.owner.clone(),
                    epoch: record.epoch,
                })
                .collect::<Vec<_>>();
            let current_by_shard = existing
                .into_iter()
                .map(|(version, record)| (record.shard_id, (version, record)))
                .collect::<BTreeMap<_, _>>();
            let plan = assigner
                .plan(VirtualShardAssignInput {
                    service_kind: request.service_kind.clone(),
                    actor_kind: request.actor_kind.clone(),
                    shard_count: request.shard_count,
                    instances,
                    previous,
                    eligible_shards: match request.movement_policy {
                        VirtualShardMovementPolicy::EligibleOnly => request.eligible_shards.clone(),
                        VirtualShardMovementPolicy::AllowRunningMigration => BTreeSet::new(),
                    },
                    max_migrations: match request.movement_policy {
                        VirtualShardMovementPolicy::EligibleOnly
                            if request.eligible_shards.is_empty() =>
                        {
                            0
                        }
                        _ => request.max_migrations,
                    },
                })
                .await?;

            let mut assignments_written = 0;
            let mut moved_shards = 0;
            for assignment in plan.assignments {
                let key = VirtualShardPlacementKey {
                    service_kind: request.service_kind.clone(),
                    actor_kind: request.actor_kind.clone(),
                    shard_id: assignment.shard_id,
                };
                let current = current_by_shard.get(&assignment.shard_id);
                if let Some((_, record)) = current
                    && record.owner == assignment.owner
                    && record.epoch == assignment.epoch
                {
                    continue;
                }
                let moved = current
                    .map(|(_, current)| current.owner != assignment.owner)
                    .unwrap_or(false);

                let record = VirtualShardPlacementRecord {
                    service_kind: request.service_kind.clone(),
                    actor_kind: request.actor_kind.clone(),
                    shard_id: assignment.shard_id,
                    owner: assignment.owner,
                    epoch: assignment.epoch,
                };
                self.store
                    .compare_and_put_virtual_shard(
                        key,
                        current.map(|(version, _)| *version),
                        record,
                    )
                    .await?;
                assignments_written += 1;
                if moved {
                    moved_shards += 1;
                }
            }

            Ok(RebalanceVirtualShardsReport {
                ready_instances,
                assignments_written,
                moved_shards,
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

#[derive(Debug, Clone)]
pub struct ExplicitRouteResolver<S, L> {
    service_kind: ServiceKind,
    store: S,
    coordinator: PlacementCoordinator<S, L>,
    cache: Arc<LocalRouteCache>,
    placement_lookups: Arc<AtomicU64>,
}

pub type PlacementRouteResolver<S, L> = ExplicitRouteResolver<S, L>;

impl<S, L> ExplicitRouteResolver<S, L> {
    pub fn new(
        service_kind: ServiceKind,
        store: S,
        coordinator: PlacementCoordinator<S, L>,
        cache_config: RouteCacheConfig,
    ) -> Self {
        Self {
            service_kind,
            store,
            coordinator,
            cache: Arc::new(LocalRouteCache::new(cache_config)),
            placement_lookups: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn placement_lookups(&self) -> u64 {
        self.placement_lookups.load(Ordering::SeqCst)
    }
}

impl<S, L> ExplicitRouteResolver<S, L>
where
    S: PlacementStore,
{
    pub async fn watch_cache_updates(&self) -> Result<PlacementWatchTask, PlacementError> {
        let mut watch = self.store.watch(self.store.prefix().clone()).await?;
        let store = self.store.clone();
        let cache = self.cache.clone();
        let service_kind = self.service_kind.clone();
        let handle = tokio::spawn(async move {
            while let Ok(event) = watch.next().await {
                refresh_cache_from_watch_event(&service_kind, &store, &cache, event).await;
            }
        });
        Ok(PlacementWatchTask { handle })
    }
}

#[async_trait]
pub trait PlacementWatchStarter: Clone + Send + Sync + 'static {
    async fn start_placement_watch(&self) -> Result<PlacementWatchTask, PlacementError>;
}

#[async_trait]
impl<S, L> PlacementWatchStarter for ExplicitRouteResolver<S, L>
where
    S: PlacementStore,
    L: Clone + Send + Sync + 'static,
{
    async fn start_placement_watch(&self) -> Result<PlacementWatchTask, PlacementError> {
        self.watch_cache_updates().await
    }
}

#[derive(Debug)]
pub struct PlacementWatchTask {
    handle: tokio::task::JoinHandle<()>,
}

impl PlacementWatchTask {
    pub fn cancel(&self) {
        self.handle.abort();
    }
}

impl Drop for PlacementWatchTask {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[async_trait]
impl<S, L> RouteResolver for ExplicitRouteResolver<S, L>
where
    S: PlacementStore,
    L: LogicControl,
{
    async fn resolve(&self, request: ResolveRequest) -> Result<RouteTarget, PlacementError> {
        let key = request.cache_key();
        match self.cache.get(&key) {
            CacheLookup::Fresh(target) | CacheLookup::Stale(target) => {
                return Ok(target);
            }
            CacheLookup::Miss => {}
        }

        self.placement_lookups.fetch_add(1, Ordering::SeqCst);
        let actor_id = actor_id_from_route_key(request.route_key);
        let placement_key = ActorPlacementKey {
            actor_kind: request.actor_kind.clone(),
            actor_id: actor_id.clone(),
        };
        let record = match self.store.get_actor(&placement_key).await? {
            Some((_, record)) => record,
            None => {
                self.coordinator
                    .activate_actor(ActivateActorRequest {
                        service_kind: self.service_kind.clone(),
                        actor_kind: request.actor_kind,
                        actor_id,
                    })
                    .await?
            }
        };
        let instance = self
            .store
            .get_instance(&record.owner)
            .await?
            .ok_or_else(|| PlacementError::InstanceNotFound {
                instance_id: record.owner.clone(),
            })?;
        let target = RouteTarget {
            service_kind: self.service_kind.clone(),
            instance_id: instance.instance_id,
            advertised_endpoint: instance.advertised_endpoint,
            owner_epoch: Some(record.epoch),
        };
        self.cache.insert(key, target.clone());
        Ok(target)
    }

    async fn invalidate(&self, key: RouteCacheKey, _reason: InvalidateReason) {
        self.cache.invalidate(&key);
    }
}

fn actor_id_from_route_key(route_key: RouteKey) -> ActorId {
    match route_key {
        RouteKey::Str(value) => ActorId::Str(value),
        RouteKey::U64(value) => ActorId::U64(value),
        RouteKey::I64(value) => ActorId::I64(value),
        RouteKey::Bytes(value) => ActorId::Bytes(value),
    }
}

fn route_key_from_actor_id(actor_id: &ActorId) -> RouteKey {
    match actor_id {
        ActorId::Str(value) => RouteKey::Str(value.clone()),
        ActorId::U64(value) => RouteKey::U64(*value),
        ActorId::I64(value) => RouteKey::I64(*value),
        ActorId::Bytes(value) => RouteKey::Bytes(value.clone()),
    }
}

async fn refresh_cache_from_watch_event<S>(
    service_kind: &ServiceKind,
    store: &S,
    cache: &Arc<LocalRouteCache>,
    event: PlacementWatchEvent,
) where
    S: PlacementStore,
{
    let PlacementWatchEvent::ActorUpdated { record, .. } = event else {
        return;
    };
    let cache_key = RouteCacheKey::new(
        service_kind.clone(),
        record.actor_kind.clone(),
        route_key_from_actor_id(&record.actor_id),
    );

    if record.state != PlacementState::Running {
        cache.invalidate(&cache_key);
        return;
    }

    let target = match store.get_instance(&record.owner).await {
        Ok(Some(instance)) if instance.state == InstanceState::Ready => RouteTarget {
            service_kind: service_kind.clone(),
            instance_id: instance.instance_id,
            advertised_endpoint: instance.advertised_endpoint,
            owner_epoch: Some(record.epoch),
        },
        _ => {
            cache.invalidate(&cache_key);
            return;
        }
    };

    cache.insert(cache_key, target);
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use lattice_core::instance::InstanceCapacity;
    use lattice_core::{actor_kind, service_kind};

    use super::*;
    use crate::instance::InstanceState;
    use crate::vshard::{GradualRebalanceShardAssigner, RoundRobinShardAssigner, VirtualShardId};
    use crate::{InMemoryPlacementStore, PlacementPrefix};

    #[derive(Debug, Clone, Default)]
    struct CountingLogicControl {
        calls: Arc<AtomicU64>,
        delay: Duration,
    }

    #[async_trait]
    impl LogicControl for CountingLogicControl {
        async fn activate_actor(
            &self,
            _instance: &InstanceRecord,
            _key: &ActorPlacementKey,
            _epoch: Epoch,
        ) -> Result<(), PlacementError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn coordinator_activates_missing_actor_owner_without_prewritten_logic_key() {
        let store = ready_store().await;
        let logic = CountingLogicControl::default();
        let coordinator = PlacementCoordinator::new(store.clone(), logic.clone());

        let record = coordinator
            .activate_actor(ActivateActorRequest {
                service_kind: service_kind!("World"),
                actor_kind: actor_kind!("World"),
                actor_id: ActorId::U64(7),
            })
            .await
            .unwrap();

        assert_eq!(record.owner, InstanceId::new("world-a"));
        assert_eq!(record.epoch, Epoch(1));
        assert_eq!(logic.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_coordinator_activation_creates_one_owner() {
        let store = ready_store().await;
        let logic = CountingLogicControl {
            calls: Arc::new(AtomicU64::new(0)),
            delay: Duration::from_millis(10),
        };
        let coordinator = Arc::new(PlacementCoordinator::new(store, logic.clone()));

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let coordinator = coordinator.clone();
            tasks.push(tokio::spawn(async move {
                coordinator
                    .activate_actor(ActivateActorRequest {
                        service_kind: service_kind!("World"),
                        actor_kind: actor_kind!("World"),
                        actor_id: ActorId::U64(7),
                    })
                    .await
            }));
        }

        for task in tasks {
            let record = task.await.unwrap().unwrap();
            assert_eq!(record.owner, InstanceId::new("world-a"));
        }
        assert_eq!(logic.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn explicit_route_resolver_activates_missing_owner_and_uses_cache() {
        let store = ready_store().await;
        let logic = CountingLogicControl::default();
        let coordinator = PlacementCoordinator::new(store.clone(), logic.clone());
        let resolver = ExplicitRouteResolver::new(
            service_kind!("World"),
            store,
            coordinator,
            RouteCacheConfig::default(),
        );
        let request = ResolveRequest {
            service_kind: service_kind!("World"),
            actor_kind: actor_kind!("World"),
            route_key: RouteKey::U64(7),
        };

        let first = resolver.resolve(request.clone()).await.unwrap();
        let second = resolver.resolve(request).await.unwrap();

        assert_eq!(first.instance_id, InstanceId::new("world-a"));
        assert_eq!(second.instance_id, InstanceId::new("world-a"));
        assert_eq!(first.owner_epoch, Some(Epoch(1)));
        assert_eq!(resolver.placement_lookups(), 1);
        assert_eq!(logic.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn placement_route_resolver_reads_existing_store_record_without_activation() {
        let store = ready_store().await;
        let key = ActorPlacementKey {
            actor_kind: actor_kind!("World"),
            actor_id: ActorId::U64(7),
        };
        store
            .compare_and_put_actor(
                key,
                None,
                ActorPlacementRecord {
                    actor_kind: actor_kind!("World"),
                    actor_id: ActorId::U64(7),
                    owner: InstanceId::new("world-a"),
                    epoch: Epoch(3),
                    lease_id: LeaseId(10),
                    state: PlacementState::Running,
                },
            )
            .await
            .unwrap();
        let logic = CountingLogicControl::default();
        let coordinator = PlacementCoordinator::new(store.clone(), logic.clone());
        let resolver = PlacementRouteResolver::new(
            service_kind!("World"),
            store,
            coordinator,
            RouteCacheConfig::default(),
        );

        let target = resolver
            .resolve(ResolveRequest {
                service_kind: service_kind!("World"),
                actor_kind: actor_kind!("World"),
                route_key: RouteKey::U64(7),
            })
            .await
            .unwrap();

        assert_eq!(target.instance_id, InstanceId::new("world-a"));
        assert_eq!(target.owner_epoch, Some(Epoch(3)));
        assert_eq!(resolver.placement_lookups(), 1);
        assert_eq!(logic.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn coordinator_owner_move_increments_epoch() {
        let store = ready_store().await;
        store
            .upsert_instance(instance_record("world-b", InstanceState::Ready))
            .await
            .unwrap();
        let coordinator = PlacementCoordinator::new(store, NoopLogicControl);
        let key = ActorPlacementKey {
            actor_kind: actor_kind!("World"),
            actor_id: ActorId::U64(7),
        };
        coordinator
            .activate_actor(ActivateActorRequest {
                service_kind: service_kind!("World"),
                actor_kind: actor_kind!("World"),
                actor_id: ActorId::U64(7),
            })
            .await
            .unwrap();

        let moved = coordinator
            .move_actor(key, InstanceId::new("world-b"))
            .await
            .unwrap();

        assert_eq!(moved.owner, InstanceId::new("world-b"));
        assert_eq!(moved.epoch, Epoch(2));
    }

    #[tokio::test]
    async fn explicit_route_resolver_refreshes_cache_from_placement_watch() {
        let store = ready_store().await;
        store
            .upsert_instance(instance_record("world-b", InstanceState::Ready))
            .await
            .unwrap();
        let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);
        let resolver = ExplicitRouteResolver::new(
            service_kind!("World"),
            store,
            coordinator.clone(),
            RouteCacheConfig::default(),
        );
        let watch_task = resolver.watch_cache_updates().await.unwrap();
        let request = ResolveRequest {
            service_kind: service_kind!("World"),
            actor_kind: actor_kind!("World"),
            route_key: RouteKey::U64(7),
        };
        let key = ActorPlacementKey {
            actor_kind: actor_kind!("World"),
            actor_id: ActorId::U64(7),
        };

        let first = resolver.resolve(request.clone()).await.unwrap();
        let lookups_after_first_resolve = resolver.placement_lookups();
        coordinator
            .move_actor(key, InstanceId::new("world-b"))
            .await
            .unwrap();

        for _ in 0..50 {
            let refreshed = resolver.resolve(request.clone()).await.unwrap();
            if refreshed.instance_id == InstanceId::new("world-b") {
                assert_eq!(first.instance_id, InstanceId::new("world-a"));
                assert_eq!(refreshed.owner_epoch, Some(Epoch(2)));
                assert_eq!(resolver.placement_lookups(), lookups_after_first_resolve);
                watch_task.cancel();
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        watch_task.cancel();
        panic!("placement watch did not refresh route cache");
    }

    #[tokio::test]
    async fn scale_out_ready_instance_participates_in_virtual_shard_assignment() {
        let store = ready_store().await;
        let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);
        let request = RebalanceVirtualShardsRequest {
            service_kind: service_kind!("World"),
            actor_kind: actor_kind!("World"),
            shard_count: 4,
            eligible_shards: BTreeSet::new(),
            max_migrations: usize::MAX,
            movement_policy: VirtualShardMovementPolicy::EligibleOnly,
        };

        let initial = coordinator
            .rebalance_virtual_shards(request.clone(), &RoundRobinShardAssigner)
            .await
            .unwrap();
        assert_eq!(
            initial,
            RebalanceVirtualShardsReport {
                ready_instances: 1,
                assignments_written: 4,
                moved_shards: 0,
            }
        );

        store
            .upsert_instance(instance_record("world-b", InstanceState::Ready))
            .await
            .unwrap();
        let after_scale_out = coordinator
            .rebalance_virtual_shards(
                RebalanceVirtualShardsRequest {
                    eligible_shards: BTreeSet::from([VirtualShardId(1), VirtualShardId(3)]),
                    max_migrations: 2,
                    movement_policy: VirtualShardMovementPolicy::EligibleOnly,
                    ..request
                },
                &GradualRebalanceShardAssigner,
            )
            .await
            .unwrap();
        let assignments = store
            .list_virtual_shards(&service_kind!("World"), &actor_kind!("World"))
            .await
            .unwrap()
            .into_iter()
            .map(|(_, record)| (record.shard_id, (record.owner, record.epoch)))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(
            after_scale_out,
            RebalanceVirtualShardsReport {
                ready_instances: 2,
                assignments_written: 2,
                moved_shards: 2,
            }
        );
        assert_eq!(
            assignments.get(&VirtualShardId(1)),
            Some(&(InstanceId::new("world-b"), Epoch(2)))
        );
        assert_eq!(
            assignments.get(&VirtualShardId(3)),
            Some(&(InstanceId::new("world-b"), Epoch(2)))
        );
    }

    #[tokio::test]
    async fn virtual_shard_rebalance_respects_running_actor_movement_policy() {
        let store = ready_store().await;
        let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);
        let initial = RebalanceVirtualShardsRequest {
            service_kind: service_kind!("World"),
            actor_kind: actor_kind!("World"),
            shard_count: 4,
            eligible_shards: BTreeSet::new(),
            max_migrations: usize::MAX,
            movement_policy: VirtualShardMovementPolicy::AllowRunningMigration,
        };
        coordinator
            .rebalance_virtual_shards(initial, &RoundRobinShardAssigner)
            .await
            .unwrap();
        store
            .upsert_instance(instance_record("world-b", InstanceState::Ready))
            .await
            .unwrap();

        let running_guarded = coordinator
            .rebalance_virtual_shards(
                RebalanceVirtualShardsRequest {
                    service_kind: service_kind!("World"),
                    actor_kind: actor_kind!("World"),
                    shard_count: 4,
                    eligible_shards: BTreeSet::new(),
                    max_migrations: usize::MAX,
                    movement_policy: VirtualShardMovementPolicy::EligibleOnly,
                },
                &GradualRebalanceShardAssigner,
            )
            .await
            .unwrap();
        let running_allowed = coordinator
            .rebalance_virtual_shards(
                RebalanceVirtualShardsRequest {
                    service_kind: service_kind!("World"),
                    actor_kind: actor_kind!("World"),
                    shard_count: 4,
                    eligible_shards: BTreeSet::new(),
                    max_migrations: usize::MAX,
                    movement_policy: VirtualShardMovementPolicy::AllowRunningMigration,
                },
                &GradualRebalanceShardAssigner,
            )
            .await
            .unwrap();

        assert_eq!(running_guarded.moved_shards, 0);
        assert_eq!(running_allowed.moved_shards, 2);
    }

    #[tokio::test]
    async fn coordinator_drain_marks_instance_draining_and_migrates_owned_actors() {
        let store = ready_store().await;
        store
            .upsert_instance(instance_record("world-b", InstanceState::Ready))
            .await
            .unwrap();
        let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);
        let key = ActorPlacementKey {
            actor_kind: actor_kind!("World"),
            actor_id: ActorId::U64(7),
        };
        coordinator
            .activate_actor(ActivateActorRequest {
                service_kind: service_kind!("World"),
                actor_kind: actor_kind!("World"),
                actor_id: ActorId::U64(7),
            })
            .await
            .unwrap();
        store
            .compare_and_put_virtual_shard(
                VirtualShardPlacementKey {
                    service_kind: service_kind!("World"),
                    actor_kind: actor_kind!("World"),
                    shard_id: VirtualShardId(3),
                },
                None,
                VirtualShardPlacementRecord {
                    service_kind: service_kind!("World"),
                    actor_kind: actor_kind!("World"),
                    shard_id: VirtualShardId(3),
                    owner: InstanceId::new("world-a"),
                    epoch: Epoch(1),
                },
            )
            .await
            .unwrap();

        let report = coordinator
            .drain_instance(service_kind!("World"), InstanceId::new("world-a"))
            .await
            .unwrap();
        let drained = store
            .get_instance(&InstanceId::new("world-a"))
            .await
            .unwrap()
            .unwrap();
        let migrated = store.get_actor(&key).await.unwrap().unwrap().1;
        let migrated_shard = store
            .get_virtual_shard(&VirtualShardPlacementKey {
                service_kind: service_kind!("World"),
                actor_kind: actor_kind!("World"),
                shard_id: VirtualShardId(3),
            })
            .await
            .unwrap()
            .unwrap()
            .1;

        assert_eq!(
            report,
            DrainReport {
                drained_instance: InstanceId::new("world-a"),
                migrated_actors: 1,
                migrated_virtual_shards: 1,
            }
        );
        assert_eq!(drained.state, InstanceState::Draining);
        assert_eq!(migrated.owner, InstanceId::new("world-b"));
        assert_eq!(migrated.epoch, Epoch(2));
        assert_eq!(migrated_shard.owner, InstanceId::new("world-b"));
        assert_eq!(migrated_shard.epoch, Epoch(2));
    }

    async fn ready_store() -> InMemoryPlacementStore {
        let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test"));
        store
            .upsert_instance(instance_record("world-a", InstanceState::Ready))
            .await
            .unwrap();
        store
    }

    fn instance_record(instance_id: &str, state: InstanceState) -> InstanceRecord {
        InstanceRecord {
            service_kind: service_kind!("World"),
            instance_id: InstanceId::new(instance_id),
            lease_id: LeaseId(1),
            advertised_endpoint: format!("http://{instance_id}.world:18080").parse().unwrap(),
            control_endpoint: format!("http://{instance_id}.world:18081").parse().unwrap(),
            version: "test".to_string(),
            state,
            capacity: InstanceCapacity::default(),
            labels: BTreeMap::new(),
        }
    }
}
