//! Driving the production DeathWatch registry.
//!
//! Installing a watch and terminating its target both cross a failpoint boundary. A dropped
//! acknowledgement is honoured by discarding the command; a dropped terminal notification that the
//! registry still produced means production ignored the injected decision, which the scenario
//! records so [`check_invariants`](Scenario::check_invariants) can reject it.

use lattice_core::{failpoint::Failpoint, watch::TerminatedReason};
use lattice_remoting::{association::AssociationId, watch::WatchCommand};

use super::{Scenario, ScenarioError};
use crate::fault::{FailAction, FaultOrigin, FaultOutcome, FaultTarget};

impl Scenario {
    pub(super) fn install_watch(&mut self) -> Result<(), ScenarioError> {
        let command = self
            .watches
            .receive_watch(
                AssociationId::new(1).unwrap(),
                self.watch_id,
                self.watch_target.clone(),
                |_| true,
            )
            .map_err(ScenarioError::Watch)?;
        let action = self
            .faults
            .take_injection(Failpoint::WatchAfterInstallBeforeAck)
            .unwrap_or(FailAction::Continue);
        if action == FailAction::Drop {
            self.record_evidence(
                Failpoint::WatchAfterInstallBeforeAck,
                FaultTarget::Network,
                action,
                FaultOutcome::MessageLost,
                FaultOrigin::SimulatedExecutor,
            );
            return Ok(());
        }
        self.apply_watch_command(command);
        Ok(())
    }

    fn apply_watch_command(&mut self, command: WatchCommand) {
        match command {
            WatchCommand::WatchAck { watch_id, target } => {
                self.state.watch_acknowledged = self.watches.receive_ack(watch_id, &target);
            }
            WatchCommand::Terminated {
                watch_id,
                target,
                reason,
            } => {
                if self.watches.receive_terminated(watch_id, &target, reason) {
                    self.state.terminal_watches += 1;
                }
            }
            WatchCommand::Watch { .. } | WatchCommand::Unwatch { .. } => {}
        }
    }

    pub(super) fn terminate_watch_target(&mut self) {
        let target = self.watch_target.clone();
        let commands = self
            .watches
            .target_terminated(&target, TerminatedReason::Migrated);
        let action = self
            .faults
            .take_injection(Failpoint::WatchAfterTerminatedBeforeAck)
            .unwrap_or(FailAction::Continue);
        if action == FailAction::Drop {
            if !commands.is_empty() {
                self.state.unhonoured_injections += 1;
                return;
            }
            self.record_evidence(
                Failpoint::WatchAfterTerminatedBeforeAck,
                FaultTarget::Network,
                action,
                FaultOutcome::MessageLost,
                FaultOrigin::ProductionCallSite,
            );
            return;
        }
        for (_, command) in commands {
            self.apply_watch_command(command);
        }
    }
}
