use std::time::{Duration, Instant};

use dashmap::DashMap;
use lattice_rpc::types::RouteTarget;

use crate::routing::resolver::RouteCacheKey;

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
    entries: DashMap<RouteCacheKey, RouteCacheEntry>,
}

impl LocalRouteCache {
    pub fn new(config: RouteCacheConfig) -> Self {
        assert!(
            config.soft_ttl <= config.hard_ttl,
            "soft ttl must not exceed hard ttl"
        );
        Self {
            config,
            entries: DashMap::new(),
        }
    }

    pub fn insert(&self, key: RouteCacheKey, target: RouteTarget) {
        self.entries.insert(
            key,
            RouteCacheEntry {
                target,
                inserted_at: Instant::now(),
            },
        );
    }

    pub fn get(&self, key: &RouteCacheKey) -> CacheLookup {
        let Some(entry) = self.entries.get(key) else {
            return CacheLookup::Miss;
        };
        match entry.state(&self.config) {
            CacheEntryState::Fresh => CacheLookup::Fresh(entry.target.clone()),
            CacheEntryState::Stale => CacheLookup::Stale(entry.target.clone()),
            CacheEntryState::Expired => {
                drop(entry);
                self.entries.remove(key);
                CacheLookup::Miss
            }
        }
    }

    pub fn invalidate(&self, key: &RouteCacheKey) {
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
