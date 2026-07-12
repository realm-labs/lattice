use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use thiserror::Error;

use crate::plan::RebalancePlan;
use crate::types::{PlacementSlot, PlacementSlotKey, Revision};

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
}
