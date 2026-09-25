use async_trait::async_trait;
use lattice_model::{cluster::CoordinatorScope, run::ControlOperationId};

use super::{CoordinatorLeaseStore, StorageError};
use crate::candidates::{
    CandidateAuthorization, CandidateChange, CandidateChangeResult, CandidateError,
    CandidateRegistration, CandidateSetState,
};

/// Policy mutations are privileged. Remote administration must authorize the
/// mTLS principal and requested scope before invoking this store boundary.
#[async_trait]
pub trait CandidateStore: CoordinatorLeaseStore {
    async fn candidate_set(
        &self,
        scope: &CoordinatorScope,
    ) -> Result<CandidateSetState, StorageError>;
    async fn candidate_authorization(
        &self,
        scope: &CoordinatorScope,
        node_id: &str,
    ) -> Result<Option<CandidateAuthorization>, StorageError>;
    async fn change_candidate(
        &self,
        request: CandidateChange,
    ) -> Result<CandidateChangeResult, StorageError>;
    async fn register_candidate(
        &self,
        registration: &CandidateRegistration,
    ) -> Result<(), StorageError>;
    async fn unregister_candidate(
        &self,
        expected: &CandidateRegistration,
    ) -> Result<(), StorageError>;
    async fn online_candidates(
        &self,
        scope: &CoordinatorScope,
    ) -> Result<Vec<CandidateRegistration>, StorageError>;
}
impl From<CandidateError> for StorageError {
    fn from(error: CandidateError) -> Self {
        match error {
            CandidateError::InvalidRecord => Self::InvalidRecord,
            CandidateError::Conflict => Self::CompareFailed,
            CandidateError::Exhausted => Self::CounterExhausted,
            CandidateError::Capacity => Self::Capacity,
            CandidateError::LastCandidate => Self::LastCandidate,
            CandidateError::NotEligible => Self::CandidateNotEligible,
        }
    }
}
/// Explicit privileged provisioning helper for deployment tooling. This never
/// changes an existing authorization and is not called by election code.
/// Removal/re-addition must use an explicit revisioned `CandidateChange`.
pub async fn provision_candidate<S: CandidateStore + ?Sized>(
    store: &S,
    scope: CoordinatorScope,
    node_id: String,
) -> Result<CandidateAuthorization, StorageError> {
    let epoch = store.ensure_framework().await?;
    if let Some(existing) = store.candidate_authorization(&scope, &node_id).await? {
        return Ok(existing);
    }
    let current = store.candidate_set(&scope).await?;
    let request = CandidateChange {
        epoch,
        scope,
        node_id,
        expected_revision: current.revision,
        enabled: true,
        operation: ControlOperationId::new(uuid::Uuid::new_v4().to_string())
            .map_err(|_| StorageError::InvalidRecord)?,
    };
    store
        .change_candidate(request)
        .await?
        .authorization
        .ok_or(StorageError::CandidateNotEligible)
}
