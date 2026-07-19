//! Fenced worker-ID ownership and backend-independent lease operations.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use lattice_core::actor_ref::{ClusterId, NodeIncarnation};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

/// The worker-ID pool size defined by the default 10-bit distributed layout.
pub const MAX_DISTRIBUTED_WORKER_ID: u64 = 1_023;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkerId(u64);

impl WorkerId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerIdRange {
    start: WorkerId,
    end_inclusive: WorkerId,
}

impl WorkerIdRange {
    pub fn new(start: u64, end_inclusive: u64) -> Result<Self, WorkerIdError> {
        if start > end_inclusive || end_inclusive > MAX_DISTRIBUTED_WORKER_ID {
            return Err(WorkerIdError::InvalidRange {
                start,
                end_inclusive,
            });
        }
        Ok(Self {
            start: WorkerId::new(start),
            end_inclusive: WorkerId::new(end_inclusive),
        })
    }

    pub const fn start(self) -> WorkerId {
        self.start
    }

    pub const fn end_inclusive(self) -> WorkerId {
        self.end_inclusive
    }

    pub fn ids(self) -> impl Iterator<Item = WorkerId> {
        (self.start.get()..=self.end_inclusive.get()).map(WorkerId::new)
    }

    pub const fn contains(self, id: WorkerId) -> bool {
        self.start.get() <= id.get() && id.get() <= self.end_inclusive.get()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerIdOwner {
    cluster_id: ClusterId,
    node_id: String,
    incarnation: NodeIncarnation,
}

impl WorkerIdOwner {
    pub fn for_node(
        cluster_id: ClusterId,
        node_id: impl Into<String>,
        incarnation: NodeIncarnation,
    ) -> Result<Self, WorkerIdError> {
        let node_id = node_id.into();
        if node_id.is_empty()
            || node_id.len() > 128
            || node_id.contains(['/', '\\', '\0'])
            || node_id.chars().any(char::is_control)
        {
            return Err(WorkerIdError::InvalidOwner);
        }
        Ok(Self {
            cluster_id,
            node_id,
            incarnation,
        })
    }

    pub const fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub const fn incarnation(&self) -> NodeIncarnation {
        self.incarnation
    }
}

impl fmt::Display for WorkerIdOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}/{}@{}",
            self.cluster_id,
            self.node_id,
            self.incarnation.get()
        )
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkerIdLeaseToken(String);

impl WorkerIdLeaseToken {
    pub fn new(value: impl Into<String>) -> Result<Self, WorkerIdError> {
        let value = value.into();
        if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
            return Err(WorkerIdError::InvalidLeaseToken);
        }
        Ok(Self(value))
    }

    /// Exposes the token to lease-store implementations. Never log this value.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for WorkerIdLeaseToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WorkerIdLeaseToken([REDACTED])")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerIdLease {
    id: WorkerId,
    owner: WorkerIdOwner,
    token: WorkerIdLeaseToken,
    valid_for: Duration,
}

impl WorkerIdLease {
    pub fn new(
        id: WorkerId,
        owner: WorkerIdOwner,
        token: WorkerIdLeaseToken,
        valid_for: Duration,
    ) -> Result<Self, WorkerIdError> {
        if valid_for.is_zero() {
            return Err(WorkerIdError::InvalidLeaseDuration);
        }
        Ok(Self {
            id,
            owner,
            token,
            valid_for,
        })
    }

    pub const fn id(&self) -> WorkerId {
        self.id
    }

    pub const fn owner(&self) -> &WorkerIdOwner {
        &self.owner
    }

    pub const fn token(&self) -> &WorkerIdLeaseToken {
        &self.token
    }

    pub const fn valid_for(&self) -> Duration {
        self.valid_for
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerIdAcquisition {
    FirstUse(WorkerIdLease),
    Reused(WorkerIdLease),
}

impl WorkerIdAcquisition {
    pub const fn lease(&self) -> &WorkerIdLease {
        match self {
            Self::FirstUse(lease) | Self::Reused(lease) => lease,
        }
    }

    pub fn into_lease(self) -> WorkerIdLease {
        match self {
            Self::FirstUse(lease) | Self::Reused(lease) => lease,
        }
    }

    pub const fn is_reused(&self) -> bool {
        matches!(self, Self::Reused(_))
    }
}

#[async_trait]
pub trait WorkerIdLeaseStore: Send + Sync + 'static {
    async fn acquire(
        &self,
        owner: &WorkerIdOwner,
        range: WorkerIdRange,
        ttl: Duration,
    ) -> Result<WorkerIdAcquisition, WorkerIdStoreError>;

    async fn renew(
        &self,
        lease: &WorkerIdLease,
        ttl: Duration,
    ) -> Result<Option<WorkerIdLease>, WorkerIdStoreError>;

    async fn release(&self, lease: &WorkerIdLease) -> Result<bool, WorkerIdStoreError>;
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum WorkerIdError {
    #[error("worker id range {start}..={end_inclusive} is invalid")]
    InvalidRange { start: u64, end_inclusive: u64 },
    #[error("worker id owner is not canonical")]
    InvalidOwner,
    #[error("worker id lease token is not canonical")]
    InvalidLeaseToken,
    #[error("worker id lease duration must be nonzero")]
    InvalidLeaseDuration,
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum WorkerIdStoreError {
    #[error("worker id store configuration is invalid: {message}")]
    InvalidConfiguration { message: String },
    #[error(
        "no worker id is available in range {start}..={end_inclusive} for cluster {cluster_id}"
    )]
    Unavailable {
        cluster_id: String,
        start: u64,
        end_inclusive: u64,
    },
    #[error("worker id store backend is unavailable: {message}")]
    Backend { message: String },
    #[error("worker id store data is malformed: {message}")]
    Codec { message: String },
}

impl WorkerIdStoreError {
    pub fn unavailable(owner: &WorkerIdOwner, range: WorkerIdRange) -> Self {
        Self::Unavailable {
            cluster_id: owner.cluster_id().to_string(),
            start: range.start().get(),
            end_inclusive: range.end_inclusive().get(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryWorkerIdLeaseStore {
    state: Arc<Mutex<MemoryState>>,
}

#[derive(Debug, Default)]
struct MemoryState {
    leases: BTreeMap<(ClusterId, WorkerId), MemoryLease>,
    history: BTreeSet<(ClusterId, WorkerId)>,
}

#[derive(Debug)]
struct MemoryLease {
    lease: WorkerIdLease,
    deadline: tokio::time::Instant,
}

#[async_trait]
impl WorkerIdLeaseStore for InMemoryWorkerIdLeaseStore {
    async fn acquire(
        &self,
        owner: &WorkerIdOwner,
        range: WorkerIdRange,
        ttl: Duration,
    ) -> Result<WorkerIdAcquisition, WorkerIdStoreError> {
        validate_ttl(ttl)?;
        let now = tokio::time::Instant::now();
        let mut state = self.state.lock().await;
        state.leases.retain(|_, lease| lease.deadline > now);
        for id in range.ids() {
            let key = (owner.cluster_id().clone(), id);
            if state.leases.contains_key(&key) {
                continue;
            }
            let reused = !state.history.insert(key.clone());
            let token = WorkerIdLeaseToken::new(uuid::Uuid::new_v4().to_string())
                .expect("UUID is a canonical lease token");
            let lease =
                WorkerIdLease::new(id, owner.clone(), token, ttl).expect("validated memory lease");
            state.leases.insert(
                key,
                MemoryLease {
                    lease: lease.clone(),
                    deadline: now + ttl,
                },
            );
            return Ok(if reused {
                WorkerIdAcquisition::Reused(lease)
            } else {
                WorkerIdAcquisition::FirstUse(lease)
            });
        }
        Err(WorkerIdStoreError::unavailable(owner, range))
    }

    async fn renew(
        &self,
        lease: &WorkerIdLease,
        ttl: Duration,
    ) -> Result<Option<WorkerIdLease>, WorkerIdStoreError> {
        validate_ttl(ttl)?;
        let now = tokio::time::Instant::now();
        let key = (lease.owner().cluster_id().clone(), lease.id());
        let mut state = self.state.lock().await;
        let Some(current) = state.leases.get_mut(&key) else {
            return Ok(None);
        };
        if current.deadline <= now {
            state.leases.remove(&key);
            return Ok(None);
        }
        if current.lease.owner() != lease.owner() || current.lease.token() != lease.token() {
            return Ok(None);
        }
        let renewed = WorkerIdLease::new(
            lease.id(),
            lease.owner().clone(),
            lease.token().clone(),
            ttl,
        )
        .expect("validated memory renewal");
        current.lease = renewed.clone();
        current.deadline = now + ttl;
        Ok(Some(renewed))
    }

    async fn release(&self, lease: &WorkerIdLease) -> Result<bool, WorkerIdStoreError> {
        let key = (lease.owner().cluster_id().clone(), lease.id());
        let mut state = self.state.lock().await;
        if state
            .leases
            .get(&key)
            .is_some_and(|current| current.deadline <= tokio::time::Instant::now())
        {
            state.leases.remove(&key);
            return Ok(false);
        }
        let matches = state.leases.get(&key).is_some_and(|current| {
            current.lease.owner() == lease.owner() && current.lease.token() == lease.token()
        });
        if matches {
            state.leases.remove(&key);
        }
        Ok(matches)
    }
}

fn validate_ttl(ttl: Duration) -> Result<(), WorkerIdStoreError> {
    if ttl.is_zero() {
        return Err(WorkerIdStoreError::InvalidConfiguration {
            message: "lease TTL must be nonzero".to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use lattice_core::actor_ref::{ClusterId, NodeIncarnation};

    use super::{
        InMemoryWorkerIdLeaseStore, WorkerIdAcquisition, WorkerIdLeaseStore, WorkerIdOwner,
        WorkerIdRange, WorkerIdStoreError,
    };

    fn owner(cluster: &str, node: &str, incarnation: u128) -> WorkerIdOwner {
        WorkerIdOwner::for_node(
            ClusterId::new(cluster).unwrap(),
            node,
            NodeIncarnation::new(incarnation).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn worker_ranges_are_bounded_by_the_ten_bit_pool() {
        assert!(WorkerIdRange::new(0, 1_023).is_ok());
        assert!(WorkerIdRange::new(0, 1_024).is_err());
    }

    #[tokio::test]
    async fn allocations_are_unique_per_cluster_and_exhaust_bounded_ranges() {
        let store = InMemoryWorkerIdLeaseStore::default();
        let range = WorkerIdRange::new(0, 1).unwrap();
        let first = store
            .acquire(&owner("one", "a", 1), range, Duration::from_secs(5))
            .await
            .unwrap();
        let second = store
            .acquire(&owner("one", "b", 2), range, Duration::from_secs(5))
            .await
            .unwrap();
        assert_ne!(first.lease().id(), second.lease().id());
        assert!(matches!(
            store
                .acquire(&owner("one", "c", 3), range, Duration::from_secs(5))
                .await,
            Err(WorkerIdStoreError::Unavailable { .. })
        ));
        let other_cluster = store
            .acquire(&owner("two", "a", 1), range, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(first.lease().id(), other_cluster.lease().id());
    }

    #[tokio::test]
    async fn stale_tokens_cannot_renew_or_release_reused_slots() {
        let store = InMemoryWorkerIdLeaseStore::default();
        let range = WorkerIdRange::new(7, 7).unwrap();
        let first = store
            .acquire(&owner("cluster", "a", 1), range, Duration::from_secs(5))
            .await
            .unwrap()
            .into_lease();
        assert!(store.release(&first).await.unwrap());
        let second = store
            .acquire(&owner("cluster", "b", 2), range, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(matches!(second, WorkerIdAcquisition::Reused(_)));
        assert_eq!(
            store.renew(&first, Duration::from_secs(5)).await.unwrap(),
            None
        );
        assert!(!store.release(&first).await.unwrap());
        assert!(store.release(second.lease()).await.unwrap());
    }
}
