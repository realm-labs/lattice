use std::collections::BTreeMap;

use async_trait::async_trait;
use lattice_core::id::RouteKey;
use lattice_core::kind::{ActorKind, ServiceKind};
use lattice_rpc::types::RouteTarget;

use crate::error::PlacementError;
use crate::registry::{InMemoryInstanceRegistry, InstanceRegistry, InstanceState};
use crate::sharding::{VirtualShardAssignment, VirtualShardId, VirtualShardMapper};

#[derive(Debug, Clone)]
pub struct VirtualShardRouteTable {
    service_kind: ServiceKind,
    actor_kind: ActorKind,
    mapper: VirtualShardMapper,
    assignments: BTreeMap<VirtualShardId, VirtualShardAssignment>,
    instances: InMemoryInstanceRegistry,
}

impl VirtualShardRouteTable {
    pub fn new(
        service_kind: ServiceKind,
        actor_kind: ActorKind,
        mapper: VirtualShardMapper,
        assignments: Vec<VirtualShardAssignment>,
        instances: InMemoryInstanceRegistry,
    ) -> Self {
        Self {
            service_kind,
            actor_kind,
            mapper,
            assignments: assignments
                .into_iter()
                .map(|assignment| (assignment.shard_id, assignment))
                .collect(),
            instances,
        }
    }

    pub async fn resolve(&self, route_key: &RouteKey) -> Result<RouteTarget, PlacementError> {
        let shard_id = self.mapper.shard_for_route_key(route_key);
        let assignment = self
            .assignments
            .get(&shard_id)
            .ok_or(PlacementError::NoRoute)?;
        let instance = self
            .instances
            .get(&assignment.owner)
            .await?
            .ok_or_else(|| PlacementError::InstanceNotFound {
                instance_id: assignment.owner.clone(),
            })?;
        if instance.state != InstanceState::Ready {
            return Err(PlacementError::InstanceNotReady {
                instance_id: instance.instance_id,
                state: instance.state,
            });
        }
        Ok(RouteTarget {
            service_kind: self.service_kind.clone(),
            instance_id: instance.instance_id,
            advertised_endpoint: instance.advertised_endpoint,
            owner_epoch: Some(assignment.epoch),
        })
    }

    pub fn actor_kind(&self) -> &ActorKind {
        &self.actor_kind
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RouteCacheKey {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub route_key: RouteKey,
}

impl RouteCacheKey {
    pub fn new(service_kind: ServiceKind, actor_kind: ActorKind, route_key: RouteKey) -> Self {
        Self {
            service_kind,
            actor_kind,
            route_key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveRequest {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub route_key: RouteKey,
}

impl ResolveRequest {
    pub fn cache_key(&self) -> RouteCacheKey {
        RouteCacheKey::new(
            self.service_kind.clone(),
            self.actor_kind.clone(),
            self.route_key.clone(),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidateReason {
    NotOwner,
    Fenced,
    OwnerChanged,
    Manual,
}

#[async_trait]
pub trait RouteResolver: Clone + Send + Sync + 'static {
    async fn resolve(&self, request: ResolveRequest) -> Result<RouteTarget, PlacementError>;
    async fn invalidate(&self, key: RouteCacheKey, reason: InvalidateReason);
}

#[async_trait]
pub trait DynRouteResolver: Send + Sync + 'static {
    async fn resolve(&self, request: ResolveRequest) -> Result<RouteTarget, PlacementError>;
    async fn invalidate(&self, key: RouteCacheKey, reason: InvalidateReason);
}

#[async_trait]
impl<T> DynRouteResolver for T
where
    T: RouteResolver,
{
    async fn resolve(&self, request: ResolveRequest) -> Result<RouteTarget, PlacementError> {
        RouteResolver::resolve(self, request).await
    }

    async fn invalidate(&self, key: RouteCacheKey, reason: InvalidateReason) {
        RouteResolver::invalidate(self, key, reason).await;
    }
}

#[derive(Clone)]
pub struct BoxRouteResolver {
    inner: std::sync::Arc<dyn DynRouteResolver>,
}

impl std::fmt::Debug for BoxRouteResolver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BoxRouteResolver")
            .finish_non_exhaustive()
    }
}

impl BoxRouteResolver {
    pub fn new<R>(resolver: R) -> Self
    where
        R: RouteResolver,
    {
        Self {
            inner: std::sync::Arc::new(resolver),
        }
    }
}

#[async_trait]
impl RouteResolver for BoxRouteResolver {
    async fn resolve(&self, request: ResolveRequest) -> Result<RouteTarget, PlacementError> {
        self.inner.resolve(request).await
    }

    async fn invalidate(&self, key: RouteCacheKey, reason: InvalidateReason) {
        self.inner.invalidate(key, reason).await;
    }
}
