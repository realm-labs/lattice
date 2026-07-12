use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use thiserror::Error;

use crate::coordinator::{LeaderRecord, NodeHello};
use crate::plan::RebalancePlan;
use crate::types::{ClaimGrant, PlacementSlot, PlacementSlotKey, Revision};

pub mod etcd;

#[async_trait]
pub trait PlacementStore: Send + Sync + 'static {
    async fn get_slot(&self, key: &PlacementSlotKey)
    -> Result<Option<PlacementSlot>, StorageError>;

    async fn compare_and_put_slot(
        &self,
        expected_revision: Option<Revision>,
        slot: PlacementSlot,
    ) -> Result<(), StorageError>;

    async fn get_plan(&self, plan_id: u128) -> Result<Option<RebalancePlan>, StorageError>;

    async fn compare_and_put_plan(
        &self,
        expected_revision: Option<Revision>,
        plan: RebalancePlan,
        revision: Revision,
    ) -> Result<(), StorageError>;
}

#[async_trait]
pub trait CoordinatorStore: PlacementStore {
    async fn ensure_schema_generation(&self) -> Result<(), StorageError>;
    async fn grant_lease(&self, ttl: std::time::Duration) -> Result<i64, StorageError>;
    async fn keep_lease_alive(&self, lease_id: i64) -> Result<(), StorageError>;
    async fn revoke_lease(&self, lease_id: i64) -> Result<(), StorageError>;
    async fn campaign_leader(
        &self,
        leader: &LeaderRecord,
        lease_id: i64,
    ) -> Result<bool, StorageError>;
    async fn get_leader(&self) -> Result<Option<LeaderRecord>, StorageError>;
    async fn register_member(&self, hello: &NodeHello, lease_id: i64) -> Result<(), StorageError>;
    async fn list_members(&self) -> Result<Vec<NodeHello>, StorageError>;
    async fn put_claim(&self, grant: &ClaimGrant, lease_id: i64) -> Result<(), StorageError>;
    async fn get_claim(&self, key: &PlacementSlotKey) -> Result<Option<ClaimGrant>, StorageError>;
    async fn delete_claim(&self, expected: &ClaimGrant) -> Result<(), StorageError>;
    async fn list_slots(&self) -> Result<Vec<PlacementSlot>, StorageError>;
    async fn list_plans(&self) -> Result<Vec<RebalancePlan>, StorageError>;
}

#[derive(Debug, Clone)]
pub struct InMemoryPlacementStore {
    inner: Arc<Mutex<MemoryState>>,
    maximum_slots: usize,
    maximum_plans: usize,
}

#[derive(Debug, Default)]
struct MemoryState {
    slots: BTreeMap<PlacementSlotKey, PlacementSlot>,
    plans: BTreeMap<u128, (Revision, RebalancePlan)>,
    schema_generation: Option<u64>,
    next_lease: i64,
    leases: BTreeMap<i64, bool>,
    leader: Option<(i64, LeaderRecord)>,
    leader_term: u64,
    members: BTreeMap<String, (i64, NodeHello)>,
    claims: BTreeMap<PlacementSlotKey, (i64, ClaimGrant)>,
}

#[async_trait]
impl CoordinatorStore for InMemoryPlacementStore {
    async fn ensure_schema_generation(&self) -> Result<(), StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        match state.schema_generation {
            Some(etcd::STORAGE_SCHEMA_GENERATION) => Ok(()),
            Some(_) => Err(StorageError::SchemaGenerationMismatch),
            None => {
                state.schema_generation = Some(etcd::STORAGE_SCHEMA_GENERATION);
                Ok(())
            }
        }
    }

    async fn grant_lease(&self, ttl: std::time::Duration) -> Result<i64, StorageError> {
        if ttl.is_zero() {
            return Err(StorageError::InvalidConfig);
        }
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        state.next_lease = state
            .next_lease
            .checked_add(1)
            .ok_or(StorageError::Capacity)?;
        let lease = state.next_lease;
        state.leases.insert(lease, true);
        Ok(lease)
    }

    async fn keep_lease_alive(&self, lease_id: i64) -> Result<(), StorageError> {
        let state = self.inner.lock().expect("placement memory store poisoned");
        state
            .leases
            .get(&lease_id)
            .filter(|active| **active)
            .ok_or(StorageError::Unavailable)
            .map(|_| ())
    }

    async fn revoke_lease(&self, lease_id: i64) -> Result<(), StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        state.leases.remove(&lease_id);
        if state
            .leader
            .as_ref()
            .is_some_and(|(lease, _)| *lease == lease_id)
        {
            state.leader = None;
        }
        state.members.retain(|_, (lease, _)| *lease != lease_id);
        state.claims.retain(|_, (lease, _)| *lease != lease_id);
        Ok(())
    }

    async fn campaign_leader(
        &self,
        leader: &LeaderRecord,
        lease_id: i64,
    ) -> Result<bool, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        if !state.leases.contains_key(&lease_id) || state.leader.is_some() {
            return Ok(false);
        }
        let expected = state
            .leader_term
            .checked_add(1)
            .ok_or(StorageError::Capacity)?;
        if leader.term.get() != expected {
            return Err(StorageError::CompareFailed);
        }
        state.leader_term = expected;
        state.leader = Some((lease_id, leader.clone()));
        Ok(true)
    }

    async fn get_leader(&self) -> Result<Option<LeaderRecord>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .leader
            .as_ref()
            .map(|(_, leader)| leader.clone()))
    }

    async fn register_member(&self, hello: &NodeHello, lease_id: i64) -> Result<(), StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        if !state.leases.contains_key(&lease_id) {
            return Err(StorageError::Unavailable);
        }
        if state
            .members
            .get(&hello.node.node_id)
            .is_some_and(|(_, current)| current.node.incarnation != hello.node.incarnation)
        {
            return Err(StorageError::IncarnationConflict);
        }
        state
            .members
            .insert(hello.node.node_id.clone(), (lease_id, hello.clone()));
        Ok(())
    }

    async fn list_members(&self) -> Result<Vec<NodeHello>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .members
            .values()
            .map(|(_, hello)| hello.clone())
            .collect())
    }

    async fn put_claim(&self, grant: &ClaimGrant, lease_id: i64) -> Result<(), StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        if !state.leases.contains_key(&lease_id) {
            return Err(StorageError::Unavailable);
        }
        if let Some((_, current)) = state.claims.get(&grant.slot)
            && (current.assignment_generation > grant.assignment_generation
                || (current.assignment_generation == grant.assignment_generation
                    && (current.owner != grant.owner
                        || current.coordinator_term > grant.coordinator_term
                        || current.grant_sequence > grant.grant_sequence)))
        {
            return Err(StorageError::CompareFailed);
        }
        state
            .claims
            .insert(grant.slot.clone(), (lease_id, grant.clone()));
        Ok(())
    }

    async fn get_claim(&self, key: &PlacementSlotKey) -> Result<Option<ClaimGrant>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .claims
            .get(key)
            .map(|(_, claim)| claim.clone()))
    }

    async fn delete_claim(&self, expected: &ClaimGrant) -> Result<(), StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        if state
            .claims
            .get(&expected.slot)
            .is_none_or(|(_, current)| current != expected)
        {
            return Err(StorageError::CompareFailed);
        }
        state.claims.remove(&expected.slot);
        Ok(())
    }

    async fn list_slots(&self) -> Result<Vec<PlacementSlot>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .slots
            .values()
            .cloned()
            .collect())
    }

    async fn list_plans(&self) -> Result<Vec<RebalancePlan>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .plans
            .values()
            .map(|(_, plan)| plan.clone())
            .collect())
    }
}

impl InMemoryPlacementStore {
    pub fn new(maximum_slots: usize, maximum_plans: usize) -> Result<Self, StorageError> {
        if maximum_slots == 0 || maximum_plans == 0 {
            return Err(StorageError::ZeroLimit);
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(MemoryState::default())),
            maximum_slots,
            maximum_plans,
        })
    }
}

#[async_trait]
impl PlacementStore for InMemoryPlacementStore {
    async fn get_slot(
        &self,
        key: &PlacementSlotKey,
    ) -> Result<Option<PlacementSlot>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .slots
            .get(key)
            .cloned())
    }

    async fn compare_and_put_slot(
        &self,
        expected_revision: Option<Revision>,
        slot: PlacementSlot,
    ) -> Result<(), StorageError> {
        slot.validate().map_err(|_| StorageError::InvalidRecord)?;
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        if state.slots.len() == self.maximum_slots && !state.slots.contains_key(&slot.key) {
            return Err(StorageError::Capacity);
        }
        if state.slots.get(&slot.key).map(|current| current.revision) != expected_revision {
            return Err(StorageError::CompareFailed);
        }
        state.slots.insert(slot.key.clone(), slot);
        Ok(())
    }

    async fn get_plan(&self, plan_id: u128) -> Result<Option<RebalancePlan>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .plans
            .get(&plan_id)
            .map(|(_, plan)| plan.clone()))
    }

    async fn compare_and_put_plan(
        &self,
        expected_revision: Option<Revision>,
        plan: RebalancePlan,
        revision: Revision,
    ) -> Result<(), StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        if state.plans.len() == self.maximum_plans && !state.plans.contains_key(&plan.plan_id) {
            return Err(StorageError::Capacity);
        }
        if state.plans.get(&plan.plan_id).map(|(current, _)| *current) != expected_revision {
            return Err(StorageError::CompareFailed);
        }
        state.plans.insert(plan.plan_id, (revision, plan));
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StorageError {
    #[error("placement store limits must be nonzero")]
    ZeroLimit,
    #[error("placement storage configuration is invalid")]
    InvalidConfig,
    #[error("placement store capacity reached")]
    Capacity,
    #[error("placement compare-and-put revision changed")]
    CompareFailed,
    #[error("placement record is invalid")]
    InvalidRecord,
    #[error("placement backend is unavailable")]
    Unavailable,
    #[error("placement backend returned malformed data")]
    Codec,
    #[error("placement schema generation differs; mixed clusters are forbidden")]
    SchemaGenerationMismatch,
    #[error("node ID is still leased to another incarnation")]
    IncarnationConflict,
}
