use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use lattice_core::{ActorId, ActorKind, Epoch, InstanceId, RouteKey, ServiceKind};
use lattice_rpc::RouteTarget;
use tracing::Instrument;

use crate::{
    ActorPlacementKey, ActorPlacementRecord, InstanceRecord, InstanceState, InvalidateReason,
    LeaseId, LocalRouteCache, PlacementError, PlacementState, PlacementStore, PlacementWatchEvent,
    ResolveRequest, RouteCacheConfig, RouteCacheKey, RouteResolver,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailoverReport {
    pub failed_instance: InstanceId,
    pub reassigned_actors: usize,
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

            Ok(DrainReport {
                drained_instance: instance_id,
                migrated_actors,
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

#[derive(Debug)]
pub struct PlacementWatchTask {
    handle: tokio::task::JoinHandle<()>,
}

impl PlacementWatchTask {
    pub fn cancel(&self) {
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
            crate::CacheLookup::Fresh(target) | crate::CacheLookup::Stale(target) => {
                return Ok(target);
            }
            crate::CacheLookup::Miss => {}
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
    use std::collections::BTreeMap;

    use lattice_core::{InstanceCapacity, actor_kind, service_kind};

    use super::*;
    use crate::{InMemoryPlacementStore, InstanceState, PlacementPrefix};

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

        assert_eq!(
            report,
            DrainReport {
                drained_instance: InstanceId::new("world-a"),
                migrated_actors: 1
            }
        );
        assert_eq!(drained.state, InstanceState::Draining);
        assert_eq!(migrated.owner, InstanceId::new("world-b"));
        assert_eq!(migrated.epoch, Epoch(2));
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
            advertised_endpoint: format!("http://{instance_id}.world:18080").parse().unwrap(),
            control_endpoint: format!("http://{instance_id}.world:18081").parse().unwrap(),
            version: "test".to_string(),
            state,
            capacity: InstanceCapacity::default(),
            labels: BTreeMap::new(),
        }
    }
}
