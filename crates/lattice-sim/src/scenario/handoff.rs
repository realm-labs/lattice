//! Driving the production handoff reducer.
//!
//! Scenario steps are translated into the events [`HandoffMachine`](lattice_placement::handoff::HandoffMachine)
//! accepts, and the effects it returns are applied back onto the scenario state. A rejected
//! transition is a legitimate outcome the workload retries; only a rejection that still moved the
//! reducer forward is a fault.

use lattice_core::failpoint::{self, Failpoint};
use lattice_placement::{
    handoff::{HandoffEffect, HandoffError, HandoffEvent},
    types::{AssignmentGeneration, CoordinatorTerm, PlacementVersion, Revision},
};

use super::{
    HandoffStep, MAXIMUM_ATTEMPTS, Scenario, ScenarioError, ScenarioEvent, incarnation, node,
    placement_domain,
};
use crate::fault::{FailAction, FaultOrigin, FaultOutcome, FaultTarget};

impl Scenario {
    pub(super) fn apply_handoff_step(
        &mut self,
        step: HandoffStep,
        attempts: u8,
    ) -> Result<(), ScenarioError> {
        if self.interrupted_at_boundary(step, attempts) {
            return Ok(());
        }
        let phase = self.handoff.phase;
        match self.handoff.transition(handoff_event(step)) {
            Ok(effects) => {
                if step == HandoffStep::TargetClaimInstalled {
                    self.state.assignment_generation = 2;
                    self.state.claim_owner_incarnation = Some(self.state.target_incarnation);
                }
                self.apply_handoff_effects(effects)
            }
            Err(HandoffError::IllegalTransition | HandoffError::UnexpectedBarrierMember) => {
                if self.handoff.phase != phase {
                    return Err(ScenarioError::RejectedTransitionAdvancedPhase);
                }
                self.state.rejected_transitions += 1;
                self.retry(step, attempts);
                Ok(())
            }
            Err(error) => Err(ScenarioError::Handoff(error)),
        }
    }

    fn apply_handoff_effects(&mut self, effects: Vec<HandoffEffect>) -> Result<(), ScenarioError> {
        for effect in effects {
            match effect {
                HandoffEffect::DrainSource => {}
                HandoffEffect::ReplaceAuthority => {
                    self.state.claim_owner_incarnation = None;
                }
                HandoffEffect::PublishActive => self.publish_active(MAXIMUM_ATTEMPTS),
                HandoffEffect::StopFailed => return Err(ScenarioError::UnexpectedStopFailure),
            }
        }
        Ok(())
    }

    pub(super) fn publish_active(&mut self, attempts: u8) {
        let action = failpoint::hit_decision(Failpoint::HandoffAfterActivePersistBeforeDelta);
        if !action.is_continue() {
            let (target, outcome) = match action {
                FailAction::Crash => {
                    self.coordinator_process.crash();
                    self.coordinator_process
                        .restart(incarnation(self.coordinator_process.incarnation.get() + 1));
                    (FaultTarget::Coordinator, FaultOutcome::ProcessCrashed)
                }
                _ => (FaultTarget::Store, FaultOutcome::CommitRejected),
            };
            self.record_evidence(
                Failpoint::HandoffAfterActivePersistBeforeDelta,
                target,
                action,
                outcome,
                FaultOrigin::SimulatedExecutor,
            );
            if attempts > 0 {
                self.schedule_soon(ScenarioEvent::PublishActive(attempts - 1));
            }
            return;
        }
        self.state.assignment_generation = 2;
        self.state.claim_owner_incarnation = Some(self.state.target_incarnation);
        self.state.running = true;
    }
}

pub(super) fn handoff_event(step: HandoffStep) -> HandoffEvent {
    match step {
        HandoffStep::ApplyBarrier(session) => HandoffEvent::AppliedRevision {
            session,
            version: PlacementVersion::new(
                placement_domain(),
                CoordinatorTerm::new(1).unwrap(),
                Revision::new(2).unwrap(),
            ),
        },
        HandoffStep::FenceBarrier(session) => HandoffEvent::FenceSession(session),
        HandoffStep::SourceInvalid => HandoffEvent::SourceAuthorityInvalid {
            source: node("source", 1, 28001),
            generation: AssignmentGeneration::new(1).unwrap(),
        },
        HandoffStep::TargetClaimInstalled => HandoffEvent::TargetClaimInstalled {
            target: node("target", 2, 28002),
            generation: AssignmentGeneration::new(2).unwrap(),
        },
        HandoffStep::TargetReady => HandoffEvent::TargetReady {
            target: node("target", 2, 28002),
            generation: AssignmentGeneration::new(2).unwrap(),
        },
    }
}
