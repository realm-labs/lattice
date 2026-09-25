use std::{collections::BTreeSet, time::Duration};

use lattice_model::{
    cluster::CoordinatorScope,
    framework::LatticeVersion,
    run::{ClusterLifecycle, RunEpoch},
};

use super::{InMemoryCoordinationStore, LeaseState, StorageError, initial_revision};
use crate::coordinator::LeaderRecord;

#[cfg(test)]
mod framework_tests {
    use super::{InMemoryCoordinationStore, LatticeVersion, StorageError};

    #[tokio::test]
    async fn startup_requires_the_exact_framework_and_never_overwrites_foreign_metadata() {
        let store = InMemoryCoordinationStore::new(4, 4).unwrap();
        store.ensure_framework().await.unwrap();
        store.ensure_framework().await.unwrap();
        assert_eq!(
            store.inner.lock().unwrap().framework_identity.as_deref(),
            Some(LatticeVersion::CURRENT)
        );
        store.inner.lock().unwrap().framework_identity = Some("foreign-framework".to_owned());
        assert!(matches!(
            store.ensure_framework().await,
            Err(StorageError::FrameworkMismatch)
        ));
        assert_eq!(
            store.inner.lock().unwrap().framework_identity.as_deref(),
            Some("foreign-framework")
        );
        store.inner.lock().unwrap().framework_identity = None;
        assert!(matches!(
            store.ensure_framework().await,
            Err(StorageError::FrameworkMismatch)
        ));
        assert!(store.inner.lock().unwrap().framework_identity.is_none());
    }
}

impl InMemoryCoordinationStore {
    pub(super) async fn ensure_framework(&self) -> Result<RunEpoch, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        match state.framework_identity.as_deref() {
            Some(LatticeVersion::CURRENT) if state.membership_revision.is_some() => state
                .lifecycle
                .as_ref()
                .ok_or(StorageError::StorageMetadataMismatch)
                .and_then(|lifecycle| {
                    if lifecycle.permits_election() {
                        Ok(lifecycle.epoch)
                    } else {
                        Err(StorageError::RunNotRunning)
                    }
                }),
            Some(LatticeVersion::CURRENT) => Err(StorageError::StorageMetadataMismatch),
            Some(_) => Err(StorageError::FrameworkMismatch),
            None if state.membership_revision.is_some() => Err(StorageError::FrameworkMismatch),
            None => {
                state.framework_identity = Some(LatticeVersion::CURRENT.to_owned());
                state.membership_revision = Some(initial_revision());
                state.lifecycle = Some(ClusterLifecycle::initial());
                Ok(RunEpoch::INITIAL)
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
        let removed = state
            .members
            .values()
            .filter(|member| member.lease_id == lease_id)
            .map(|member| member.node.clone())
            .collect::<BTreeSet<_>>();
        state
            .group_members
            .retain(|_, member| !removed.contains(&member.node));
        state.leases.remove(&lease_id);
        state.leaders.retain(|_, (lease, _)| *lease != lease_id);
        state
            .candidate_registrations
            .retain(|_, registration| registration.lease_id != lease_id);
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
        let lifecycle = state
            .lifecycle
            .as_ref()
            .ok_or(StorageError::StorageMetadataMismatch)?;
        if lifecycle.epoch != leader.epoch {
            return Err(StorageError::RunMismatch);
        }
        if !lifecycle.permits_election() {
            return Err(StorageError::RunNotRunning);
        }
        super::validate_candidate(&state, leader)?;
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
