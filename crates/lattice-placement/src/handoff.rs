use std::collections::BTreeSet;

use lattice_core::actor_ref::NodeIncarnation;
use thiserror::Error;

use crate::types::{
    AssignmentGeneration, NodeKey, PlacementSlot, PlacementSlotKey, PlacementSlotState, Revision,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandoffPhase {
    Invalidating,
    Draining,
    ReplacingAuthority,
    Starting,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandoffEffect {
    DrainSource,
    ReplaceAuthority,
    PublishActive,
    StopFailed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandoffEvent {
    AppliedRevision {
        session: NodeIncarnation,
        revision: Revision,
    },
    FenceSession(NodeIncarnation),
    SourceDrained {
        source: NodeKey,
        generation: AssignmentGeneration,
    },
    SourceAuthorityInvalid {
        source: NodeKey,
        generation: AssignmentGeneration,
    },
    SourceStopFailed {
        source: NodeKey,
        generation: AssignmentGeneration,
    },
    TargetClaimInstalled {
        target: NodeKey,
        generation: AssignmentGeneration,
    },
    TargetReady {
        target: NodeKey,
        generation: AssignmentGeneration,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoffMachine {
    pub slot: PlacementSlotKey,
    pub plan_id: u128,
    pub source: NodeKey,
    pub target: NodeKey,
    pub source_generation: AssignmentGeneration,
    pub target_generation: AssignmentGeneration,
    pub phase: HandoffPhase,
    barrier: RevisionBarrier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RevisionBarrier {
    revision: Revision,
    required: BTreeSet<NodeIncarnation>,
    applied: BTreeSet<NodeIncarnation>,
}

impl HandoffMachine {
    pub fn begin(
        slot: PlacementSlotKey,
        plan_id: u128,
        source: NodeKey,
        target: NodeKey,
        source_generation: AssignmentGeneration,
        barrier_revision: Revision,
        barrier_sessions: BTreeSet<NodeIncarnation>,
    ) -> Result<Self, HandoffError> {
        let target_generation = source_generation
            .next()
            .map_err(|_| HandoffError::GenerationExhausted)?;
        Ok(Self {
            slot,
            plan_id,
            source,
            target,
            source_generation,
            target_generation,
            phase: HandoffPhase::Invalidating,
            barrier: RevisionBarrier {
                revision: barrier_revision,
                required: barrier_sessions,
                applied: BTreeSet::new(),
            },
        })
    }

    pub fn recover(
        slot: &PlacementSlot,
        plan_id: u128,
        source: NodeKey,
        target: NodeKey,
        source_generation: AssignmentGeneration,
        barrier_revision: Revision,
        barrier_sessions: BTreeSet<NodeIncarnation>,
    ) -> Result<Self, HandoffError> {
        if slot.active_move != Some(plan_id) {
            return Err(HandoffError::SlotMismatch);
        }
        let mut machine = Self::begin(
            slot.key.clone(),
            plan_id,
            source,
            target.clone(),
            source_generation,
            barrier_revision,
            barrier_sessions,
        )?;
        machine.phase = match slot.state {
            PlacementSlotState::BeginHandoff => HandoffPhase::Invalidating,
            PlacementSlotState::Stopping | PlacementSlotState::StopFailed => HandoffPhase::Draining,
            PlacementSlotState::Fenced => HandoffPhase::ReplacingAuthority,
            PlacementSlotState::Allocating
                if slot.owner.as_ref() == Some(&target)
                    && slot.assignment_generation == machine.target_generation =>
            {
                HandoffPhase::Starting
            }
            _ => return Err(HandoffError::SlotMismatch),
        };
        Ok(machine)
    }

    pub fn barrier_revision(&self) -> Revision {
        self.barrier.revision
    }

    pub fn required_sessions(&self) -> &BTreeSet<NodeIncarnation> {
        &self.barrier.required
    }

    pub fn start(&mut self) -> Vec<HandoffEffect> {
        if self.phase == HandoffPhase::Invalidating && self.barrier.required == self.barrier.applied
        {
            self.phase = HandoffPhase::Draining;
            vec![HandoffEffect::DrainSource]
        } else {
            Vec::new()
        }
    }

    pub fn transition(&mut self, event: HandoffEvent) -> Result<Vec<HandoffEffect>, HandoffError> {
        match event {
            HandoffEvent::AppliedRevision { session, revision }
                if self.phase == HandoffPhase::Invalidating =>
            {
                if revision < self.barrier.revision || !self.barrier.required.contains(&session) {
                    return Err(HandoffError::UnexpectedBarrierMember);
                }
                self.barrier.applied.insert(session);
                if self.barrier.required == self.barrier.applied {
                    self.phase = HandoffPhase::Draining;
                    Ok(vec![HandoffEffect::DrainSource])
                } else {
                    Ok(Vec::new())
                }
            }
            HandoffEvent::FenceSession(session) if self.phase == HandoffPhase::Invalidating => {
                self.barrier.required.remove(&session);
                if self.barrier.required == self.barrier.applied {
                    self.phase = HandoffPhase::Draining;
                    Ok(vec![HandoffEffect::DrainSource])
                } else {
                    Ok(Vec::new())
                }
            }
            HandoffEvent::SourceDrained { source, generation }
            | HandoffEvent::SourceAuthorityInvalid { source, generation }
                if self.phase == HandoffPhase::Draining
                    && source == self.source
                    && generation == self.source_generation =>
            {
                self.phase = HandoffPhase::ReplacingAuthority;
                Ok(vec![HandoffEffect::ReplaceAuthority])
            }
            HandoffEvent::SourceStopFailed { source, generation }
                if self.phase == HandoffPhase::Draining
                    && source == self.source
                    && generation == self.source_generation =>
            {
                Ok(vec![HandoffEffect::StopFailed])
            }
            HandoffEvent::TargetClaimInstalled { target, generation }
                if self.phase == HandoffPhase::ReplacingAuthority
                    && target == self.target
                    && generation == self.target_generation =>
            {
                self.phase = HandoffPhase::Starting;
                Ok(Vec::new())
            }
            HandoffEvent::TargetReady { target, generation }
                if self.phase == HandoffPhase::Starting
                    && target == self.target
                    && generation == self.target_generation =>
            {
                self.phase = HandoffPhase::Completed;
                Ok(vec![HandoffEffect::PublishActive])
            }
            _ => Err(HandoffError::IllegalTransition),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HandoffError {
    #[error("handoff assignment generation is exhausted")]
    GenerationExhausted,
    #[error("persisted slot does not match handoff progress")]
    SlotMismatch,
    #[error("handoff event is illegal in the current phase")]
    IllegalTransition,
    #[error("handoff revision barrier rejected acknowledgement")]
    UnexpectedBarrierMember,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ShardId;
    use lattice_core::actor_ref::{EntityType, NodeAddress, NodeIncarnation};

    fn node(id: &str, incarnation: u128) -> NodeKey {
        NodeKey {
            node_id: id.to_owned(),
            address: NodeAddress::new("127.0.0.1", 2500 + incarnation as u16).unwrap(),
            incarnation: NodeIncarnation::new(incarnation).unwrap(),
        }
    }

    #[test]
    fn barrier_excludes_unrelated_and_move_never_rolls_back() {
        let subscribed = NodeIncarnation::new(3).unwrap();
        let mut machine = HandoffMachine::begin(
            PlacementSlotKey::Shard {
                entity_type: EntityType::new("entity").unwrap(),
                shard_id: ShardId::new(1),
            },
            9,
            node("source", 1),
            node("target", 2),
            AssignmentGeneration::new(4).unwrap(),
            Revision::new(8).unwrap(),
            [subscribed].into_iter().collect(),
        )
        .unwrap();
        assert!(
            machine
                .transition(HandoffEvent::AppliedRevision {
                    session: NodeIncarnation::new(99).unwrap(),
                    revision: Revision::new(8).unwrap(),
                })
                .is_err()
        );
        assert_eq!(
            machine
                .transition(HandoffEvent::AppliedRevision {
                    session: subscribed,
                    revision: Revision::new(8).unwrap(),
                })
                .unwrap(),
            vec![HandoffEffect::DrainSource]
        );
        assert_eq!(machine.phase, HandoffPhase::Draining);
    }
}
