use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use lattice_core::{ActorId, ActorKind, Epoch, InstanceId, RouteKey, ServiceKind};
use lattice_rpc::RouteTarget;
use tracing::{Instrument, warn};

use crate::cache::{CacheLookup, LocalRouteCache, RouteCacheConfig};
use crate::error::PlacementError;
use crate::instance::{InstanceRecord, InstanceState};
use crate::route::{InvalidateReason, ResolveRequest, RouteCacheKey, RouteResolver};
use crate::singleton::SingletonControl;
use crate::store::{
    ActorPlacementKey, ActorPlacementRecord, LeaseId, PlacementState, PlacementStore,
    PlacementWatchEvent, SingletonKey, SingletonPlacementRecord, VirtualShardPlacementKey,
    VirtualShardPlacementRecord,
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

#[async_trait]
pub trait VirtualShardMigrationControl: LogicControl {
    async fn prepare_virtual_shard_migration(
        &self,
        instance: &InstanceRecord,
        request: PrepareVirtualShardMigrationRequest,
    ) -> Result<VirtualShardMigrationOutcome, PlacementError>;
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

#[async_trait]
impl SingletonControl for NoopLogicControl {
    async fn activate_singleton(
        &self,
        _instance: &InstanceRecord,
        _key: &SingletonKey,
        _epoch: Epoch,
    ) -> Result<(), PlacementError> {
        Ok(())
    }
}

#[async_trait]
impl VirtualShardMigrationControl for NoopLogicControl {
    async fn prepare_virtual_shard_migration(
        &self,
        _instance: &InstanceRecord,
        request: PrepareVirtualShardMigrationRequest,
    ) -> Result<VirtualShardMigrationOutcome, PlacementError> {
        Ok(VirtualShardMigrationOutcome {
            shard_id: request.shard_id,
            eligible: true,
            running_actors: 0,
            passivated_actors: 0,
        })
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
    pub reassigned_singletons: usize,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrepareVirtualShardMigrationRequest {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub shard_id: VirtualShardId,
    pub shard_count: u32,
    pub owner_epoch: Epoch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualShardMigrationOutcome {
    pub shard_id: VirtualShardId,
    pub eligible: bool,
    pub running_actors: usize,
    pub passivated_actors: usize,
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

    pub async fn rebalance_virtual_shards<A>(
        &self,
        request: RebalanceVirtualShardsRequest,
        assigner: &A,
    ) -> Result<RebalanceVirtualShardsReport, PlacementError>
    where
        A: VirtualShardAssigner + ?Sized,
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

    pub async fn prepare_and_rebalance_virtual_shards<A>(
        &self,
        request: RebalanceVirtualShardsRequest,
        assigner: &A,
    ) -> Result<RebalanceVirtualShardsReport, PlacementError>
    where
        A: VirtualShardAssigner + ?Sized,
        L: VirtualShardMigrationControl,
    {
        if request.movement_policy == VirtualShardMovementPolicy::AllowRunningMigration {
            return self.rebalance_virtual_shards(request, assigner).await;
        }

        let candidates = self
            .planned_virtual_shard_moves(request.clone(), assigner)
            .await?;
        let mut eligible_shards = request.eligible_shards.clone();
        for record in candidates {
            let Some(instance) = self.store.get_instance(&record.owner).await? else {
                continue;
            };
            if instance.state != InstanceState::Ready {
                continue;
            }

            let outcome = self
                .logic
                .prepare_virtual_shard_migration(
                    &instance,
                    PrepareVirtualShardMigrationRequest {
                        service_kind: record.service_kind.clone(),
                        actor_kind: record.actor_kind.clone(),
                        shard_id: record.shard_id,
                        shard_count: request.shard_count,
                        owner_epoch: record.epoch,
                    },
                )
                .await?;
            if outcome.eligible {
                eligible_shards.insert(record.shard_id);
            }
        }

        self.rebalance_virtual_shards(
            RebalanceVirtualShardsRequest {
                eligible_shards,
                movement_policy: VirtualShardMovementPolicy::EligibleOnly,
                ..request
            },
            assigner,
        )
        .await
    }

    pub async fn start_virtual_shard_scale_out_watch<A>(
        &self,
        requests: Vec<RebalanceVirtualShardsRequest>,
        assigner: A,
    ) -> Result<PlacementWatchTask, PlacementError>
    where
        A: VirtualShardAssigner,
        L: VirtualShardMigrationControl,
    {
        let mut watch = self.store.watch(self.store.prefix().clone()).await?;
        let coordinator = self.clone();
        let assigner: Arc<dyn VirtualShardAssigner> = Arc::new(assigner);
        let handle = tokio::spawn(async move {
            while let Ok(event) = watch.next().await {
                let PlacementWatchEvent::InstanceUpdated { record } = event else {
                    continue;
                };
                if record.state != InstanceState::Ready {
                    continue;
                }

                for request in requests
                    .iter()
                    .filter(|request| request.service_kind == record.service_kind)
                {
                    if let Err(error) = coordinator
                        .prepare_and_rebalance_virtual_shards(request.clone(), assigner.as_ref())
                        .await
                    {
                        warn!(
                            service.kind = request.service_kind.as_str(),
                            actor.kind = request.actor_kind.as_str(),
                            instance.id = record.instance_id.as_str(),
                            error = %error,
                            "automatic virtual shard scale-out rebalance failed"
                        );
                    }
                }
            }
        });

        Ok(PlacementWatchTask { handle })
    }

    async fn planned_virtual_shard_moves<A>(
        &self,
        request: RebalanceVirtualShardsRequest,
        assigner: &A,
    ) -> Result<Vec<VirtualShardPlacementRecord>, PlacementError>
    where
        A: VirtualShardAssigner + ?Sized,
    {
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
            .map(|(_, record)| (record.shard_id, record))
            .collect::<BTreeMap<_, _>>();
        let plan = assigner
            .plan(VirtualShardAssignInput {
                service_kind: request.service_kind,
                actor_kind: request.actor_kind,
                shard_count: request.shard_count,
                instances,
                previous,
                eligible_shards: BTreeSet::new(),
                max_migrations: request.max_migrations,
            })
            .await?;

        Ok(plan
            .assignments
            .into_iter()
            .filter_map(|assignment| {
                let current = current_by_shard.get(&assignment.shard_id)?;
                (current.owner != assignment.owner).then(|| current.clone())
            })
            .collect())
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
    pub fn noop() -> Self {
        Self {
            handle: tokio::spawn(async {}),
        }
    }

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
mod tests;
