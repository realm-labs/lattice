//! The failpoint decision interpreter.
//!
//! A [`FailAction`] only describes what should go wrong; this module turns each decision into the
//! consequence the simulated deployment would actually suffer — a crashed or paused process, a
//! lost or duplicated frame, a rejected commit — and records the evidence that proves the boundary
//! was exercised.

use lattice_core::failpoint::{self, Failpoint};

use super::{HandoffStep, Scenario, ScenarioEvent, incarnation};
use crate::fault::{FailAction, FaultEvidence, FaultOrigin, FaultOutcome, FaultTarget};

impl Scenario {
    pub(super) fn interrupted_at_boundary(&mut self, step: HandoffStep, attempts: u8) -> bool {
        let point = match step {
            HandoffStep::ApplyBarrier(_) | HandoffStep::FenceBarrier(_) => {
                Failpoint::HandoffAfterPartialBarrier
            }
            HandoffStep::SourceInvalid => Failpoint::HandoffAfterShardDrainedBeforeClaimRevoke,
            HandoffStep::TargetClaimInstalled => Failpoint::HandoffAfterNewClaimBeforeGrantSend,
            HandoffStep::TargetReady => Failpoint::HandoffAfterGrantBeforeShardReady,
        };
        let action = failpoint::hit_decision(point);
        if action.is_continue() {
            self.resume_processes();
            return false;
        }
        let (target, outcome) = match action {
            FailAction::Crash => {
                self.coordinator_process.crash();
                self.coordinator_process
                    .restart(incarnation(self.coordinator_process.incarnation.get() + 1));
                (FaultTarget::Coordinator, FaultOutcome::ProcessCrashed)
            }
            FailAction::Pause => {
                self.target_process.pause();
                (FaultTarget::Target, FaultOutcome::ProcessPaused)
            }
            FailAction::StoreFailure => (FaultTarget::Store, FaultOutcome::CommitRejected),
            FailAction::Drop => (FaultTarget::Network, FaultOutcome::MessageLost),
            FailAction::Duplicate => (FaultTarget::Network, FaultOutcome::MessageDuplicated),
            FailAction::Continue => unreachable!("continue is handled before interpretation"),
        };
        self.record_evidence(
            point,
            target,
            action,
            outcome,
            FaultOrigin::SimulatedExecutor,
        );
        if action == FailAction::Duplicate {
            self.schedule_soon(ScenarioEvent::Handoff(step, attempts));
        }
        self.retry(step, attempts);
        true
    }

    fn resume_processes(&mut self) {
        self.source_process.resume();
        self.target_process.resume();
    }

    pub(super) fn retry(&mut self, step: HandoffStep, attempts: u8) {
        if attempts == 0 {
            return;
        }
        self.resume_processes();
        self.schedule_soon(ScenarioEvent::Handoff(step, attempts - 1));
    }

    pub(super) fn schedule_soon(&mut self, event: ScenarioEvent) {
        let delay = 1 + u64::try_from(self.delivery.below(3)).unwrap_or(0);
        let at = self.clock.now_millis().saturating_add(delay);
        self.scheduler.schedule(at, event);
    }

    pub(super) fn record_evidence(
        &mut self,
        point: Failpoint,
        target: FaultTarget,
        action: FailAction,
        outcome: FaultOutcome,
        origin: FaultOrigin,
    ) {
        self.state.injected_faults += 1;
        self.faults.record(FaultEvidence {
            point,
            target,
            action,
            outcome,
            origin,
        });
    }
}
