//! An [`Explorable`] view of the production handoff reducer.
//!
//! The bounded state explorer drives the real [`HandoffMachine`] over every enabled step and
//! checks the safety properties the reducer must hold in every reachable state, independently of
//! the seeded workload.

use lattice_placement::handoff::{HandoffEffect, HandoffMachine, HandoffPhase};

use crate::{
    explorer::Explorable,
    scenario::{HandoffStep, handoff::handoff_event, incarnation},
};

#[derive(Debug, Clone)]
pub(super) struct HandoffExploration {
    pub(super) machine: HandoffMachine,
    pub(super) published: bool,
    pub(super) stop_failed: bool,
    pub(super) completed_seen: bool,
}

impl HandoffExploration {
    fn key(&self) -> String {
        format!(
            "{:?}|{}|{}|{}",
            self.machine, self.published, self.stop_failed, self.completed_seen
        )
    }
}

impl PartialEq for HandoffExploration {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for HandoffExploration {}

impl PartialOrd for HandoffExploration {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HandoffExploration {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key().cmp(&other.key())
    }
}

impl Explorable for HandoffExploration {
    type Event = HandoffStep;
    type Error = ();

    fn enabled(&self) -> Vec<Self::Event> {
        vec![
            HandoffStep::ApplyBarrier(incarnation(1)),
            HandoffStep::ApplyBarrier(incarnation(2)),
            HandoffStep::ApplyBarrier(incarnation(9)),
            HandoffStep::FenceBarrier(incarnation(1)),
            HandoffStep::FenceBarrier(incarnation(2)),
            HandoffStep::SourceInvalid,
            HandoffStep::TargetClaimInstalled,
            HandoffStep::TargetReady,
        ]
    }

    fn step(&self, event: &Self::Event) -> Result<Self, Self::Error> {
        let mut next = self.clone();
        let before = format!("{:?}", next.machine);
        match next.machine.transition(handoff_event(*event)) {
            Ok(effects) => {
                for effect in effects {
                    match effect {
                        HandoffEffect::PublishActive => next.published = true,
                        HandoffEffect::StopFailed => next.stop_failed = true,
                        HandoffEffect::DrainSource | HandoffEffect::ReplaceAuthority => {}
                    }
                }
            }
            Err(_) if format!("{:?}", next.machine) == before => {}
            Err(_) => return Err(()),
        }
        next.completed_seen |= next.machine.phase == HandoffPhase::Completed;
        Ok(next)
    }

    fn invariant(&self) -> Result<(), String> {
        if self.published && self.machine.phase != HandoffPhase::Completed {
            return Err("Active was published outside the completed phase".to_owned());
        }
        if self.completed_seen && self.machine.phase != HandoffPhase::Completed {
            return Err("the completed handoff phase was not absorbing".to_owned());
        }
        if self.machine.target_generation.get() != self.machine.source_generation.get() + 1 {
            return Err("handoff target generation is not the exact successor".to_owned());
        }
        if self.stop_failed {
            return Err("an unexpected voluntary stop failure was emitted".to_owned());
        }
        Ok(())
    }
}
