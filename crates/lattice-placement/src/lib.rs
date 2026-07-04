use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use lattice_core::{ActorKind, InstanceId, RouteKey, ServiceKind};
use lattice_rpc::RouteTarget;

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

#[derive(Debug, Clone)]
pub struct RouteCacheConfig {
    pub soft_ttl: Duration,
    pub hard_ttl: Duration,
}

impl Default for RouteCacheConfig {
    fn default() -> Self {
        Self {
            soft_ttl: Duration::from_secs(5),
            hard_ttl: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocalRouteCache {
    config: RouteCacheConfig,
    entries: HashMap<RouteCacheKey, RouteCacheEntry>,
}

impl LocalRouteCache {
    pub fn new(config: RouteCacheConfig) -> Self {
        assert!(
            config.soft_ttl <= config.hard_ttl,
            "soft ttl must not exceed hard ttl"
        );
        Self {
            config,
            entries: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: RouteCacheKey, target: RouteTarget) {
        self.entries.insert(
            key,
            RouteCacheEntry {
                target,
                inserted_at: Instant::now(),
            },
        );
    }

    pub fn get(&mut self, key: &RouteCacheKey) -> CacheLookup {
        let Some(entry) = self.entries.get(key) else {
            return CacheLookup::Miss;
        };
        match entry.state(&self.config) {
            CacheEntryState::Fresh => CacheLookup::Fresh(entry.target.clone()),
            CacheEntryState::Stale => CacheLookup::Stale(entry.target.clone()),
            CacheEntryState::Expired => {
                self.entries.remove(key);
                CacheLookup::Miss
            }
        }
    }

    pub fn invalidate(&mut self, key: &RouteCacheKey) {
        self.entries.remove(key);
    }
}

impl Default for LocalRouteCache {
    fn default() -> Self {
        Self::new(RouteCacheConfig::default())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheLookup {
    Fresh(RouteTarget),
    Stale(RouteTarget),
    Miss,
}

#[derive(Debug, Clone)]
struct RouteCacheEntry {
    target: RouteTarget,
    inserted_at: Instant,
}

impl RouteCacheEntry {
    fn state(&self, config: &RouteCacheConfig) -> CacheEntryState {
        let age = self.inserted_at.elapsed();
        if age < config.soft_ttl {
            CacheEntryState::Fresh
        } else if age < config.hard_ttl {
            CacheEntryState::Stale
        } else {
            CacheEntryState::Expired
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CacheEntryState {
    Fresh,
    Stale,
    Expired,
}

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
    cache: Arc<std::sync::Mutex<LocalRouteCache>>,
    placement_lookups: Arc<AtomicU64>,
}

impl StaticRouteResolver {
    pub fn new(config: StaticPlacementConfig, cache_config: RouteCacheConfig) -> Self {
        Self {
            config,
            cache: Arc::new(std::sync::Mutex::new(LocalRouteCache::new(cache_config))),
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
        let key = request.cache_key();
        {
            let mut cache = self.cache.lock().expect("route cache mutex poisoned");
            match cache.get(&key) {
                CacheLookup::Fresh(target) | CacheLookup::Stale(target) => return Ok(target),
                CacheLookup::Miss => {}
            }
        }

        let target = self.resolve_from_static_config(&request)?;
        self.cache
            .lock()
            .expect("route cache mutex poisoned")
            .insert(key, target.clone());
        Ok(target)
    }

    async fn invalidate(&self, key: RouteCacheKey, _reason: InvalidateReason) {
        self.cache
            .lock()
            .expect("route cache mutex poisoned")
            .invalidate(&key);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EndpointPoolKey {
    pub instance_id: InstanceId,
    pub advertised_endpoint: String,
}

impl EndpointPoolKey {
    pub fn from_target(target: &RouteTarget) -> Self {
        Self {
            instance_id: target.instance_id.clone(),
            advertised_endpoint: target.advertised_endpoint.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointLease {
    pub key: EndpointPoolKey,
    pub connection_id: u64,
}

#[derive(Debug, Default, Clone)]
pub struct EndpointPool {
    connections: Arc<std::sync::Mutex<HashMap<EndpointPoolKey, EndpointLease>>>,
    next_connection_id: Arc<AtomicU64>,
}

impl EndpointPool {
    pub fn new() -> Self {
        Self {
            connections: Arc::new(std::sync::Mutex::new(HashMap::new())),
            next_connection_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn get_or_connect(&self, target: &RouteTarget) -> EndpointLease {
        let key = EndpointPoolKey::from_target(target);
        let mut connections = self
            .connections
            .lock()
            .expect("endpoint pool mutex poisoned");
        connections
            .entry(key.clone())
            .or_insert_with(|| EndpointLease {
                key,
                connection_id: self.next_connection_id.fetch_add(1, Ordering::SeqCst),
            })
            .clone()
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PlacementError {
    #[error("no route found")]
    NoRoute,
    #[error("static placement supports only u64 route keys in phase 3")]
    UnsupportedRouteKey,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use lattice_core::{Epoch, InstanceId, RouteKey, actor_kind, service_kind};

    use super::*;

    #[test]
    fn local_route_cache_reports_fresh_stale_and_hard_expired_entries() {
        let mut cache = LocalRouteCache::new(RouteCacheConfig {
            soft_ttl: Duration::from_millis(5),
            hard_ttl: Duration::from_millis(25),
        });
        let key = RouteCacheKey::new(
            service_kind!("World"),
            actor_kind!("World"),
            RouteKey::U64(1),
        );
        let target = route_target("world-0", 1);

        cache.insert(key.clone(), target.clone());
        assert_eq!(cache.get(&key), CacheLookup::Fresh(target.clone()));

        std::thread::sleep(Duration::from_millis(8));
        assert_eq!(cache.get(&key), CacheLookup::Stale(target));

        std::thread::sleep(Duration::from_millis(25));
        assert_eq!(cache.get(&key), CacheLookup::Miss);
    }

    #[tokio::test]
    async fn static_route_resolver_uses_cache_after_first_lookup() {
        let resolver = static_resolver();
        let request = ResolveRequest {
            service_kind: service_kind!("World"),
            actor_kind: actor_kind!("World"),
            route_key: RouteKey::U64(7),
        };

        let first = resolver.resolve(request.clone()).await.unwrap();
        let second = resolver.resolve(request).await.unwrap();

        assert_eq!(first.instance_id, InstanceId::new("world-a"));
        assert_eq!(second.instance_id, InstanceId::new("world-a"));
        assert_eq!(resolver.placement_lookups(), 1);
    }

    #[tokio::test]
    async fn static_route_resolver_maps_ranges_to_different_instances() {
        let resolver = static_resolver();

        let low = resolver
            .resolve(ResolveRequest {
                service_kind: service_kind!("World"),
                actor_kind: actor_kind!("World"),
                route_key: RouteKey::U64(40),
            })
            .await
            .unwrap();
        let high = resolver
            .resolve(ResolveRequest {
                service_kind: service_kind!("World"),
                actor_kind: actor_kind!("World"),
                route_key: RouteKey::U64(60),
            })
            .await
            .unwrap();

        assert_eq!(low.instance_id, InstanceId::new("world-a"));
        assert_eq!(high.instance_id, InstanceId::new("world-b"));
    }

    #[test]
    fn endpoint_pool_reuses_by_instance_and_endpoint() {
        let pool = EndpointPool::new();
        let first = pool.get_or_connect(&route_target("world-a", 1));
        let same = pool.get_or_connect(&route_target("world-a", 2));
        let other = pool.get_or_connect(&route_target("world-b", 1));

        assert_eq!(first.connection_id, same.connection_id);
        assert_ne!(first.connection_id, other.connection_id);
    }

    fn static_resolver() -> StaticRouteResolver {
        StaticRouteResolver::new(
            StaticPlacementConfig {
                ranges: vec![
                    StaticRouteRange {
                        service_kind: service_kind!("World"),
                        actor_kind: actor_kind!("World"),
                        start_inclusive: 0,
                        end_exclusive: 50,
                        target: route_target("world-a", 1),
                    },
                    StaticRouteRange {
                        service_kind: service_kind!("World"),
                        actor_kind: actor_kind!("World"),
                        start_inclusive: 50,
                        end_exclusive: 100,
                        target: route_target("world-b", 1),
                    },
                ],
            },
            RouteCacheConfig {
                soft_ttl: Duration::from_secs(30),
                hard_ttl: Duration::from_secs(60),
            },
        )
    }

    fn route_target(instance_id: &str, epoch: u64) -> RouteTarget {
        RouteTarget {
            service_kind: service_kind!("World"),
            instance_id: InstanceId::new(instance_id),
            advertised_endpoint: format!("http://{instance_id}.world:18080").parse().unwrap(),
            owner_epoch: Some(Epoch(epoch)),
        }
    }
}
