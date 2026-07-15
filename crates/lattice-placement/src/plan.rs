use std::collections::BTreeSet;

use lattice_core::actor_ref::{EntityType, NodeIncarnation, PlacementDomainId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::allocation::{ProposedMove, RebalanceProposal, RebalanceTrigger};
use crate::types::{
    AssignmentGeneration, CoordinatorTerm, NodeKey, PlacementVersion, PlanRevision, ShardId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanStatus {
    Planned,
    Running,
    Completed,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MoveProgress {
    Pending,
    Handoff,
    Completed,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebalanceMove {
    pub shard_id: ShardId,
    pub expected_generation: AssignmentGeneration,
    pub source: NodeKey,
    pub target: NodeKey,
    pub estimated_weight: u64,
    pub progress: MoveProgress,
    pub barrier_version: Option<PlacementVersion>,
    pub barrier_sessions: BTreeSet<NodeIncarnation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebalancePlan {
    pub plan_id: u128,
    pub domain: PlacementDomainId,
    pub entity_type: EntityType,
    pub reason: PlanReason,
    pub coordinator_term: CoordinatorTerm,
    pub base_version: PlacementVersion,
    pub record_revision: PlanRevision,
    pub policy_id: String,
    pub policy_version: u32,
    pub status: PlanStatus,
    pub moves: Vec<RebalanceMove>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanReason {
    Recovery,
    Drain,
    Manual,
    Automatic,
}

impl From<&RebalanceTrigger> for PlanReason {
    fn from(value: &RebalanceTrigger) -> Self {
        match value {
            RebalanceTrigger::Recovery { .. } => Self::Recovery,
            RebalanceTrigger::Drain { .. } => Self::Drain,
            RebalanceTrigger::Manual { .. } => Self::Manual,
            RebalanceTrigger::Automatic => Self::Automatic,
        }
    }
}

impl RebalancePlan {
    pub fn from_proposal(
        proposal: RebalanceProposal,
        entity_type: EntityType,
        coordinator_term: CoordinatorTerm,
        maximum_moves: usize,
    ) -> Result<Self, PlanError> {
        if maximum_moves == 0 || proposal.moves.is_empty() || proposal.moves.len() > maximum_moves {
            return Err(PlanError::InvalidMoveCount);
        }
        let mut shards = BTreeSet::new();
        for movement in &proposal.moves {
            validate_move(movement, &proposal.domain, &entity_type)?;
            if !shards.insert(movement.shard_id) {
                return Err(PlanError::DuplicateShard);
            }
        }
        Ok(Self {
            plan_id: uuid::Uuid::new_v4().as_u128(),
            domain: proposal.domain,
            entity_type,
            reason: PlanReason::from(&proposal.trigger),
            coordinator_term,
            base_version: proposal.base_version,
            record_revision: PlanRevision::new(1).expect("one is a valid plan revision"),
            policy_id: proposal.policy_id.to_owned(),
            policy_version: proposal.policy_version,
            status: PlanStatus::Planned,
            moves: proposal
                .moves
                .into_iter()
                .map(|movement| RebalanceMove {
                    shard_id: movement.shard_id,
                    expected_generation: movement.expected_generation,
                    source: movement.source,
                    target: movement.target,
                    estimated_weight: movement.estimated_weight,
                    progress: MoveProgress::Pending,
                    barrier_version: None,
                    barrier_sessions: BTreeSet::new(),
                })
                .collect(),
        })
    }

    pub fn begin_move(
        &mut self,
        shard_id: ShardId,
        current_generation: AssignmentGeneration,
        active_slot_move: Option<u128>,
    ) -> Result<(), PlanError> {
        if self.status == PlanStatus::Planned {
            self.status = PlanStatus::Running;
        }
        if self.status != PlanStatus::Running || active_slot_move.is_some() {
            return Err(PlanError::MoveConflict);
        }
        let movement = self
            .moves
            .iter_mut()
            .find(|movement| movement.shard_id == shard_id)
            .ok_or(PlanError::UnknownShard)?;
        if movement.progress != MoveProgress::Pending
            || movement.expected_generation != current_generation
        {
            return Err(PlanError::StaleGeneration);
        }
        movement.progress = MoveProgress::Handoff;
        Ok(())
    }

    pub fn install_barrier(
        &mut self,
        shard_id: ShardId,
        version: PlacementVersion,
        sessions: BTreeSet<NodeIncarnation>,
    ) -> Result<(), PlanError> {
        let movement = self
            .moves
            .iter_mut()
            .find(|movement| movement.shard_id == shard_id)
            .ok_or(PlanError::UnknownShard)?;
        if movement.progress != MoveProgress::Handoff || movement.barrier_version.is_some() {
            return Err(PlanError::IllegalProgress);
        }
        movement.barrier_version = Some(version);
        movement.barrier_sessions = sessions;
        Ok(())
    }

    pub fn cancel_pending_move(&mut self, shard_id: ShardId) -> Result<(), PlanError> {
        let movement = self
            .moves
            .iter_mut()
            .find(|movement| movement.shard_id == shard_id)
            .ok_or(PlanError::UnknownShard)?;
        if movement.progress != MoveProgress::Pending {
            return Err(PlanError::CannotRollbackHandoff);
        }
        movement.progress = MoveProgress::Cancelled;
        self.refresh_status();
        Ok(())
    }

    pub fn complete_move(&mut self, shard_id: ShardId) -> Result<(), PlanError> {
        let movement = self
            .moves
            .iter_mut()
            .find(|movement| movement.shard_id == shard_id)
            .ok_or(PlanError::UnknownShard)?;
        if movement.progress != MoveProgress::Handoff {
            return Err(PlanError::IllegalProgress);
        }
        movement.progress = MoveProgress::Completed;
        self.refresh_status();
        Ok(())
    }

    pub fn target_reservations(&self) -> impl Iterator<Item = (&NodeKey, u64)> {
        self.moves.iter().filter_map(|movement| {
            matches!(
                movement.progress,
                MoveProgress::Pending | MoveProgress::Handoff
            )
            .then_some((&movement.target, movement.estimated_weight))
        })
    }

    pub fn recover_forward_shards(&self) -> impl Iterator<Item = ShardId> + '_ {
        self.moves.iter().filter_map(|movement| {
            (movement.progress == MoveProgress::Handoff).then_some(movement.shard_id)
        })
    }

    fn refresh_status(&mut self) {
        if self.moves.iter().all(|movement| {
            matches!(
                movement.progress,
                MoveProgress::Completed | MoveProgress::Cancelled
            )
        }) {
            self.status = if self
                .moves
                .iter()
                .any(|movement| movement.progress == MoveProgress::Completed)
            {
                PlanStatus::Completed
            } else {
                PlanStatus::Cancelled
            };
        }
    }
}

fn validate_move(
    movement: &ProposedMove,
    domain: &PlacementDomainId,
    entity_type: &EntityType,
) -> Result<(), PlanError> {
    if &movement.domain != domain
        || &movement.entity_type != entity_type
        || movement.source == movement.target
        || movement.estimated_weight == 0
    {
        return Err(PlanError::InvalidMove);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PlanError {
    #[error("rebalance plan move count is empty, unbounded, or over limit")]
    InvalidMoveCount,
    #[error("rebalance plan contains an invalid move")]
    InvalidMove,
    #[error("rebalance plan contains the same shard more than once")]
    DuplicateShard,
    #[error("rebalance plan does not contain the shard")]
    UnknownShard,
    #[error("shard generation changed before handoff")]
    StaleGeneration,
    #[error("shard already has an active move")]
    MoveConflict,
    #[error("a move that entered handoff cannot be rolled back")]
    CannotRollbackHandoff,
    #[error("rebalance move progress transition is illegal")]
    IllegalProgress,
}
