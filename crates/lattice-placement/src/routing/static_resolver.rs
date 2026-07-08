use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use lattice_core::id::RouteKey;
use lattice_core::kind::{ActorKind, ServiceKind};
use lattice_rpc::types::RouteTarget;

use crate::error::PlacementError;
use crate::routing::cache::{CacheLookup, LocalRouteCache, RouteCacheConfig};
use crate::routing::resolver::{InvalidateReason, ResolveRequest, RouteCacheKey, RouteResolver};

#[derive(Debug, Clone)]
pub struct StaticPlacementConfig {
    pub ranges: Vec<StaticRouteRange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticRouteRange {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub start_inclusive: u64,
    pub end_exclusive: u64,
    pub target: RouteTarget,
}

#[derive(Debug, Clone)]
pub struct StaticRouteResolver {
    config: StaticPlacementConfig,
    cache: Arc<LocalRouteCache>,
    placement_lookups: Arc<AtomicU64>,
}

impl StaticRouteResolver {
    pub fn new(config: StaticPlacementConfig, cache_config: RouteCacheConfig) -> Self {
        Self {
            config,
            cache: Arc::new(LocalRouteCache::new(cache_config)),
            placement_lookups: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn placement_lookups(&self) -> u64 {
        self.placement_lookups.load(Ordering::SeqCst)
    }

    fn resolve_from_static_config(
        &self,
        request: &ResolveRequest,
    ) -> Result<RouteTarget, PlacementError> {
        self.placement_lookups.fetch_add(1, Ordering::SeqCst);
        let RouteKey::U64(value) = request.route_key else {
            return Err(PlacementError::UnsupportedRouteKey);
        };

        self.config
            .ranges
            .iter()
            .find(|range| {
                range.service_kind == request.service_kind
                    && range.actor_kind == request.actor_kind
                    && range.start_inclusive <= value
                    && value < range.end_exclusive
            })
            .map(|range| range.target.clone())
            .ok_or(PlacementError::NoRoute)
    }
}

#[async_trait]
impl RouteResolver for StaticRouteResolver {
    async fn resolve(&self, request: ResolveRequest) -> Result<RouteTarget, PlacementError> {
        let span = tracing::info_span!(
            "placement.resolve",
            otel.kind = "internal",
            service.kind = request.service_kind.as_str(),
            actor.kind = request.actor_kind.as_str(),
            route.key = ?request.route_key
        );
        let _entered = span.enter();
        let key = request.cache_key();
        match self.cache.get(&key) {
            CacheLookup::Fresh(target) | CacheLookup::Stale(target) => return Ok(target),
            CacheLookup::Miss => {}
        }

        let target = self.resolve_from_static_config(&request)?;
        self.cache.insert(key, target.clone());
        Ok(target)
    }

    async fn invalidate(&self, key: RouteCacheKey, _reason: InvalidateReason) {
        self.cache.invalidate(&key);
    }
}
