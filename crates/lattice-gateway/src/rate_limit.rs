use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;

use crate::error::GatewayError;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RateLimitKey {
    pub principal_id: String,
    pub session_id: String,
    pub rate_class: String,
}

#[derive(Debug)]
pub struct KeyedRateLimiter {
    limit: u32,
    window: Duration,
    buckets: DashMap<RateLimitKey, RateBucket>,
}

impl KeyedRateLimiter {
    pub fn new(limit: u32, window: Duration) -> Self {
        Self {
            limit,
            window,
            buckets: DashMap::new(),
        }
    }

    pub fn check(&self, key: RateLimitKey) -> Result<(), GatewayError> {
        let now = Instant::now();
        let mut bucket = match self.buckets.entry(key) {
            Entry::Occupied(entry) => entry.into_ref(),
            Entry::Vacant(entry) => entry.insert(RateBucket {
                window_started: now,
                used: 0,
            }),
        };
        if now.duration_since(bucket.window_started) >= self.window {
            bucket.window_started = now;
            bucket.used = 0;
        }
        if bucket.used >= self.limit {
            return Err(GatewayError::RateLimited);
        }
        bucket.used += 1;
        Ok(())
    }
}

#[derive(Debug)]
struct RateBucket {
    window_started: Instant,
    used: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayRequestContext {
    pub principal_id: String,
    pub session_id: String,
    pub rate_class: String,
}

impl From<GatewayRequestContext> for RateLimitKey {
    fn from(value: GatewayRequestContext) -> Self {
        Self {
            principal_id: value.principal_id,
            session_id: value.session_id,
            rate_class: value.rate_class,
        }
    }
}

#[derive(Debug)]
pub struct GatewayTowerPipeline {
    limiter: KeyedRateLimiter,
    max_in_flight: usize,
    in_flight: Arc<AtomicUsize>,
}

impl GatewayTowerPipeline {
    pub fn new(limiter: KeyedRateLimiter, max_in_flight: usize) -> Self {
        Self {
            limiter,
            max_in_flight,
            in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn enter(
        &self,
        ctx: GatewayRequestContext,
    ) -> Result<GatewayConcurrencyPermit, GatewayError> {
        self.limiter.check(ctx.into())?;
        self.acquire_concurrency()
    }

    fn acquire_concurrency(&self) -> Result<GatewayConcurrencyPermit, GatewayError> {
        let mut current = self.in_flight.load(Ordering::SeqCst);
        loop {
            if current >= self.max_in_flight {
                return Err(GatewayError::LoadShed);
            }
            match self.in_flight.compare_exchange(
                current,
                current + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return Ok(GatewayConcurrencyPermit {
                        in_flight: self.in_flight.clone(),
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }
}

#[derive(Debug)]
pub struct GatewayConcurrencyPermit {
    in_flight: Arc<AtomicUsize>,
}

impl Drop for GatewayConcurrencyPermit {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}
