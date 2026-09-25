//! Candidate eligibility is persistent administrative policy. Online registration
//! is leased runtime state; neither one alone proves current leadership.

use std::num::NonZeroU64;

use lattice_model::{
    cluster::CoordinatorScope,
    run::{ControlOperationId, RunEpoch},
};
use serde::{Deserialize, Serialize};

use crate::types::NodeKey;
use thiserror::Error;

pub const MAX_CANDIDATES_PER_SCOPE: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CandidateGeneration(NonZeroU64);

impl CandidateGeneration {
    pub fn new(value: u64) -> Result<Self, CandidateError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(CandidateError::InvalidRecord)
    }
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateAuthorization {
    pub scope: CoordinatorScope,
    pub node_id: String,
    pub generation: CandidateGeneration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateRegistration {
    pub epoch: RunEpoch,
    pub authorization: CandidateAuthorization,
    pub node: NodeKey,
    /// Part of the exact registration identity as well as its backing etcd lease.
    /// A delayed unregister must not erase a replacement registration.
    pub lease_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateChangeResult {
    pub revision: u64,
    pub eligible_count: usize,
    pub authorization: Option<CandidateAuthorization>,
}

/// The scope revision is mandatory even for creation. It prevents a delayed
/// enable from reviving a removed principal after its per-node record is deleted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateChange {
    pub epoch: RunEpoch,
    pub scope: CoordinatorScope,
    pub expected_revision: u64,
    pub node_id: String,
    pub enabled: bool,
    pub operation: ControlOperationId,
}

impl CandidateChange {
    pub(crate) fn validate(&self) -> Result<(), CandidateError> {
        if self.node_id.is_empty()
            || self.node_id.len() > 128
            || self.node_id.contains(['/', '\\'])
            || self.node_id.chars().any(char::is_control)
        {
            return Err(CandidateError::InvalidRecord);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateSetState {
    pub revision: u64,
    pub eligible_count: usize,
    /// One bounded retry receipt. Older operations require reconciliation after
    /// another mutation; their stale expected revision still prevents replay.
    pub(crate) last_change: Option<(CandidateChange, CandidateChangeResult)>,
}

impl CandidateSetState {
    pub(crate) fn apply(
        &self,
        request: &CandidateChange,
        existing: Option<&CandidateAuthorization>,
    ) -> Result<(Self, CandidateChangeResult), CandidateError> {
        request.validate()?;
        if let Some((previous, result)) = &self.last_change {
            if previous.operation == request.operation {
                return if previous == request {
                    Ok((self.clone(), result.clone()))
                } else {
                    Err(CandidateError::Conflict)
                };
            }
        }
        if self.revision != request.expected_revision {
            return Err(CandidateError::Conflict);
        }
        if existing.is_some_and(|record| {
            record.node_id != request.node_id || record.scope != request.scope
        }) {
            return Err(CandidateError::InvalidRecord);
        }
        let revision = self
            .revision
            .checked_add(1)
            .ok_or(CandidateError::Exhausted)?;
        let eligible_count = match (existing.is_some(), request.enabled) {
            (false, true) => self
                .eligible_count
                .checked_add(1)
                .ok_or(CandidateError::Capacity)?,
            (true, false) if self.eligible_count <= 1 => return Err(CandidateError::LastCandidate),
            (true, false) => self.eligible_count - 1,
            (false, false) => return Err(CandidateError::NotEligible),
            (true, true) => self.eligible_count,
        };
        if eligible_count > MAX_CANDIDATES_PER_SCOPE {
            return Err(CandidateError::Capacity);
        }
        // A scope-wide monotonic generation survives deletion and re-addition,
        // without accumulating a permanent tombstone for every historical node.
        let authorization = request.enabled.then(|| {
            existing.cloned().unwrap_or_else(|| CandidateAuthorization {
                scope: request.scope.clone(),
                node_id: request.node_id.clone(),
                generation: CandidateGeneration::new(revision).expect("checked revision increment"),
            })
        });
        let result = CandidateChangeResult {
            revision,
            eligible_count,
            authorization,
        };
        Ok((
            Self {
                revision,
                eligible_count,
                last_change: Some((request.clone(), result.clone())),
            },
            result,
        ))
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum CandidateError {
    #[error("candidate record is invalid")]
    InvalidRecord,
    #[error("candidate state changed or operation ID was reused")]
    Conflict,
    #[error("candidate revision exhausted")]
    Exhausted,
    #[error("candidate capacity exhausted")]
    Capacity,
    #[error("cannot remove the last eligible candidate")]
    LastCandidate,
    #[error("candidate is not eligible")]
    NotEligible,
}
