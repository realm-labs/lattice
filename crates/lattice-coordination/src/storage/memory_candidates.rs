use async_trait::async_trait;
use lattice_model::{cluster::CoordinatorScope, run::RunEpoch};

use super::{InMemoryCoordinationStore, MemoryState, StorageError, candidates::CandidateStore};
use crate::candidates::{
    CandidateAuthorization, CandidateChange, CandidateChangeResult, CandidateRegistration,
    CandidateSetState,
};

fn validate_run(state: &MemoryState, epoch: RunEpoch) -> Result<(), StorageError> {
    let lifecycle = state
        .lifecycle
        .as_ref()
        .ok_or(StorageError::RunNotInitialized)?;
    if lifecycle.epoch != epoch {
        return Err(StorageError::RunMismatch);
    }
    if !lifecycle.permits_election() {
        return Err(StorageError::RunNotRunning);
    }
    Ok(())
}

#[async_trait]
impl CandidateStore for InMemoryCoordinationStore {
    async fn candidate_set(
        &self,
        scope: &CoordinatorScope,
    ) -> Result<CandidateSetState, StorageError> {
        let state = self
            .inner
            .lock()
            .expect("coordination memory store poisoned");
        Ok(state.candidate_sets.get(scope).cloned().unwrap_or_default())
    }

    async fn candidate_authorization(
        &self,
        scope: &CoordinatorScope,
        node_id: &str,
    ) -> Result<Option<CandidateAuthorization>, StorageError> {
        let state = self
            .inner
            .lock()
            .expect("coordination memory store poisoned");
        Ok(state
            .candidate_authorizations
            .get(&(scope.clone(), node_id.to_owned()))
            .cloned())
    }

    async fn change_candidate(
        &self,
        request: CandidateChange,
    ) -> Result<CandidateChangeResult, StorageError> {
        let mut state = self
            .inner
            .lock()
            .expect("coordination memory store poisoned");
        validate_run(&state, request.epoch)?;
        let key = (request.scope.clone(), request.node_id.clone());
        let current = state
            .candidate_sets
            .get(&request.scope)
            .cloned()
            .unwrap_or_default();
        let (next, result) = current.apply(&request, state.candidate_authorizations.get(&key))?;
        if let Some(authorization) = &result.authorization {
            state
                .candidate_authorizations
                .insert(key, authorization.clone());
        } else {
            state.candidate_authorizations.remove(&key);
            state.candidate_registrations.remove(&key);
        }
        state.candidate_sets.insert(request.scope, next);
        Ok(result)
    }

    async fn register_candidate(
        &self,
        registration: &CandidateRegistration,
    ) -> Result<(), StorageError> {
        registration
            .node
            .validate()
            .map_err(|_| StorageError::InvalidRecord)?;
        if registration.node.node_id != registration.authorization.node_id
            || registration.lease_id <= 0
        {
            return Err(StorageError::InvalidRecord);
        }
        let mut state = self
            .inner
            .lock()
            .expect("coordination memory store poisoned");
        validate_run(&state, registration.epoch)?;
        if !state.leases.contains_key(&registration.lease_id) {
            return Err(StorageError::Unavailable);
        }
        let key = (
            registration.authorization.scope.clone(),
            registration.node.node_id.clone(),
        );
        if state.candidate_authorizations.get(&key) != Some(&registration.authorization) {
            return Err(StorageError::CandidateNotEligible);
        }
        if state
            .candidate_registrations
            .get(&key)
            .is_some_and(|previous| {
                previous != registration && previous.authorization == registration.authorization
            })
        {
            return Err(StorageError::IncarnationConflict);
        }
        state
            .candidate_registrations
            .insert(key, registration.clone());
        Ok(())
    }

    async fn unregister_candidate(
        &self,
        expected: &CandidateRegistration,
    ) -> Result<(), StorageError> {
        let mut state = self
            .inner
            .lock()
            .expect("coordination memory store poisoned");
        validate_run(&state, expected.epoch)?;
        let key = (
            expected.authorization.scope.clone(),
            expected.node.node_id.clone(),
        );
        if state.candidate_registrations.get(&key) != Some(expected) {
            return Err(StorageError::CompareFailed);
        }
        state.candidate_registrations.remove(&key);
        Ok(())
    }

    async fn online_candidates(
        &self,
        scope: &CoordinatorScope,
    ) -> Result<Vec<CandidateRegistration>, StorageError> {
        let state = self
            .inner
            .lock()
            .expect("coordination memory store poisoned");
        Ok(state
            .candidate_registrations
            .iter()
            .filter(|((registered_scope, _), _)| registered_scope == scope)
            .map(|(_, registration)| registration.clone())
            .collect())
    }
}
