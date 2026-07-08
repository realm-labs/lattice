use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use lattice_core::id::{ActorId, RouteKey};
use lattice_core::kind::ServiceKind;
use lattice_rpc::types::RouteTarget;

use crate::coordination::actor::{ActivateActorRequest, PlacementCoordinator};
use crate::coordination::logic::LogicControl;
use crate::error::PlacementError;
use crate::registry::InstanceState;
use crate::routing::cache::{CacheLookup, LocalRouteCache, RouteCacheConfig};
use crate::routing::resolver::{InvalidateReason, ResolveRequest, RouteCacheKey, RouteResolver};
use crate::storage::{ActorPlacementKey, PlacementState, PlacementStore, PlacementWatchEvent};

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
    pub(crate) fn new(handle: tokio::task::JoinHandle<()>) -> Self {
        Self { handle }
    }

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
            service_kind: self.service_kind.clone(),
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
