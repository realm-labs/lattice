use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use lattice_core::RequestId;

const DEFAULT_REQUEST_DEDUP_TTL: Duration = Duration::from_secs(120);
const DEFAULT_REQUEST_DEDUP_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequestDedupKey {
    method: &'static str,
    request_id: RequestId,
}

impl RequestDedupKey {
    pub fn new(method: &'static str, request_id: &RequestId) -> Self {
        Self {
            method,
            request_id: request_id.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RequestDeduplicator {
    seen: Arc<DashMap<RequestDedupKey, Instant>>,
    ttl: Duration,
    sweep_interval: Duration,
    last_sweep: Arc<Mutex<Instant>>,
}

impl Default for RequestDeduplicator {
    fn default() -> Self {
        Self {
            seen: Arc::new(DashMap::new()),
            ttl: DEFAULT_REQUEST_DEDUP_TTL,
            sweep_interval: DEFAULT_REQUEST_DEDUP_SWEEP_INTERVAL,
            last_sweep: Arc::new(Mutex::new(Instant::now())),
        }
    }
}

impl RequestDeduplicator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            ttl,
            sweep_interval: ttl.min(DEFAULT_REQUEST_DEDUP_SWEEP_INTERVAL),
            ..Self::default()
        }
    }

    /// Reserves a request key for lightweight duplicate protection.
    ///
    /// This intentionally stores only the key. It does not cache or replay
    /// business replies; duplicate callers must reconcile an unknown result.
    pub fn begin(&self, key: &RequestDedupKey) -> bool {
        let now = Instant::now();
        self.sweep_expired(now);
        match self.seen.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                if *entry.get() <= now {
                    entry.insert(now + self.ttl);
                    true
                } else {
                    false
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(now + self.ttl);
                true
            }
        }
    }

    pub fn forget(&self, key: &RequestDedupKey) {
        self.seen.remove(key);
    }

    pub fn contains(&self, key: &RequestDedupKey) -> bool {
        self.seen
            .get(key)
            .is_some_and(|expires_at| *expires_at > Instant::now())
    }

    fn sweep_expired(&self, now: Instant) {
        let Ok(mut last_sweep) = self.last_sweep.try_lock() else {
            return;
        };
        if now.duration_since(*last_sweep) < self.sweep_interval {
            return;
        }
        self.seen.retain(|_, expires_at| *expires_at > now);
        *last_sweep = now;
    }
}
