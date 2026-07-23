use std::time::Duration;

use lattice_core::coordinator::CoordinatorScope;

use super::{InMemoryPlacementStore, LeaseState, StorageError, etcd, initial_revision};
use crate::coordinator::LeaderRecord;

impl InMemoryPlacementStore {
    pub(super) async fn ensure_schema_generation(&self) -> Result<(), StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        match state.schema_generation {
            Some(etcd::STORAGE_SCHEMA_GENERATION) => Ok(()),
            Some(_) => Err(StorageError::SchemaGenerationMismatch),
            None => {
                state.schema_generation = Some(etcd::STORAGE_SCHEMA_GENERATION);
                state.membership_revision = Some(initial_revision());
                Ok(())
            }
        }
    }

    pub(super) async fn grant_lease(&self, ttl: Duration) -> Result<i64, StorageError> {
        if ttl.is_zero() {
            return Err(StorageError::InvalidConfig);
        }
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        state.next_lease = state
            .next_lease
            .checked_add(1)
            .ok_or(StorageError::Capacity)?;
        let lease = state.next_lease;
        state.leases.insert(lease, LeaseState { ttl });
        Ok(lease)
    }

    pub(super) async fn keep_lease_alive(&self, lease_id: i64) -> Result<(), StorageError> {
        let state = self.inner.lock().expect("placement memory store poisoned");
        state
            .leases
            .get(&lease_id)
            .ok_or(StorageError::Unavailable)
            .map(|_| ())
    }

    pub(super) async fn revoke_lease(&self, lease_id: i64) -> Result<(), StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        state.leases.remove(&lease_id);
        state.leaders.retain(|_, (lease, _)| *lease != lease_id);
        state
            .members
            .retain(|_, member| member.lease_id != lease_id);
        state.claims.retain(|_, claim| claim.lease_id != lease_id);
        Ok(())
    }

    pub(super) async fn lease_time_to_live(
        &self,
        lease_id: i64,
    ) -> Result<Option<Duration>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .leases
            .get(&lease_id)
            .map(|lease| lease.ttl))
    }

    pub(super) async fn campaign_leader(
        &self,
        leader: &LeaderRecord,
        lease_id: i64,
    ) -> Result<bool, StorageError> {
        leader.validate().map_err(|_| StorageError::InvalidRecord)?;
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        if !state.leases.contains_key(&lease_id) || state.leaders.contains_key(&leader.scope) {
            return Ok(false);
        }
        let expected = state
            .leader_terms
            .get(&leader.scope)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(StorageError::CounterExhausted)?;
        if leader.term.get() != expected {
            return Err(StorageError::CompareFailed);
        }
        state.leader_terms.insert(leader.scope.clone(), expected);
        state
            .leaders
            .insert(leader.scope.clone(), (lease_id, leader.clone()));
        Ok(true)
    }

    pub(super) async fn get_leader_inner(
        &self,
        scope: &CoordinatorScope,
    ) -> Result<Option<LeaderRecord>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .leaders
            .get(scope)
            .map(|(_, leader)| leader.clone()))
    }

    pub(super) async fn get_leader_term_inner(
        &self,
        scope: &CoordinatorScope,
    ) -> Result<u64, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .leader_terms
            .get(scope)
            .copied()
            .unwrap_or(0))
    }
}
