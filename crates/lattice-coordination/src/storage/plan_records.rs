//! Shared bounded plan metadata codec; proposals never become persisted arrays.
use super::{StorageError, records::encode_record};
use crate::{
    plan::{MAXIMUM_PLAN_MOVES, PlanReason, PlanStatus, RebalancePlan},
    types::{CoordinatorTerm, PlacementVersion, PlanRevision},
};
use lattice_model::cluster::{ActorGroupId, EntityType};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(crate) struct PlanMetadata {
    pub(crate) plan_id: u128,
    pub(crate) group: ActorGroupId,
    pub(crate) entity_type: EntityType,
    pub(crate) reason: PlanReason,
    pub(crate) coordinator_term: CoordinatorTerm,
    pub(crate) base_version: PlacementVersion,
    pub(crate) record_revision: PlanRevision,
    pub(crate) policy_id: String,
    pub(crate) policy_version: u32,
    pub(crate) status: PlanStatus,
    pub(crate) move_count: usize,
    pub(crate) moves_digest: [u8; 32],
}
impl PlanMetadata {
    pub(crate) fn capture(plan: &RebalancePlan) -> Result<Self, StorageError> {
        Ok(Self {
            plan_id: plan.plan_id,
            group: plan.group.clone(),
            entity_type: plan.entity_type.clone(),
            reason: plan.reason.clone(),
            coordinator_term: plan.coordinator_term,
            base_version: plan.base_version.clone(),
            record_revision: plan.record_revision,
            policy_id: plan.policy_id.clone(),
            policy_version: plan.policy_version,
            status: plan.status,
            move_count: plan.moves.len(),
            moves_digest: *blake3::hash(
                &serde_json::to_vec(&plan.moves).map_err(|_| StorageError::Codec)?,
            )
            .as_bytes(),
        })
    }
    pub(crate) fn into_plan(self) -> RebalancePlan {
        RebalancePlan {
            plan_id: self.plan_id,
            group: self.group,
            entity_type: self.entity_type,
            reason: self.reason,
            coordinator_term: self.coordinator_term,
            base_version: self.base_version,
            record_revision: self.record_revision,
            policy_id: self.policy_id,
            policy_version: self.policy_version,
            status: self.status,
            moves: Vec::new(),
        }
    }
}

pub(crate) fn validate_plan_payload(plan: &RebalancePlan) -> Result<(), StorageError> {
    if plan.moves.is_empty() || plan.moves.len() > MAXIMUM_PLAN_MOVES {
        return Err(StorageError::InvalidRecord);
    }
    encode_record(&PlanMetadata::capture(plan)?)?;
    for movement in &plan.moves {
        encode_record(movement)?;
    }
    Ok(())
}
