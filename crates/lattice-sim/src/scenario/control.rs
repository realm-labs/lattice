//! The control plane between the coordinator and the member endpoint.
//!
//! Commands are enqueued on the production [`ReliableControl`](lattice_remoting::control::ReliableControl)
//! outbox, framed by the production codec, and handed to
//! [`SimNetwork`](crate::network::SimNetwork), which decides whether a frame is delivered,
//! partitioned away or queued. Delivery decodes the frame on the far side and acknowledges it back
//! over the same simulated topology.

use bytes::Bytes;
use lattice_core::failpoint::{self, Failpoint};
use lattice_remoting::{
    control::{
        CommandId, ControlApply, ReliableControlError, control_ack_frame, control_envelope_frame,
        decode_control_ack, decode_control_envelope,
    },
    wire::FrameKind,
};

use super::{COORDINATOR, MEMBER, Scenario, ScenarioError, ScenarioEvent, incarnation};
use crate::fault::{FailAction, FaultOrigin, FaultOutcome, FaultTarget};

impl Scenario {
    pub(super) fn send_control(&mut self, command: u128) -> Result<(), ScenarioError> {
        let command_id = CommandId::new(command).ok_or(ScenarioError::InvalidConfig)?;
        let envelope = match self
            .coordinator
            .enqueue(command_id, Bytes::from_static(b"command"))
        {
            Ok(envelope) => envelope,
            Err(ReliableControlError::OutboxFull) => {
                self.state.lost_control_frames += 1;
                return Ok(());
            }
            Err(error) => return Err(ScenarioError::Control(error)),
        };
        self.commands.insert(command);
        let action = failpoint::hit_decision(Failpoint::ControlAfterOutboxBeforeSocketWrite);
        let copies = match action {
            FailAction::Drop => 0,
            FailAction::Duplicate => 2,
            _ => 1,
        };
        if !action.is_continue() {
            let (target, outcome) = match action {
                FailAction::Drop => (FaultTarget::Network, FaultOutcome::MessageLost),
                FailAction::Duplicate => (FaultTarget::Network, FaultOutcome::MessageDuplicated),
                _ => (FaultTarget::Queue, FaultOutcome::TransitionRetried),
            };
            self.record_evidence(
                Failpoint::ControlAfterOutboxBeforeSocketWrite,
                target,
                action,
                outcome,
                FaultOrigin::SimulatedExecutor,
            );
        }
        let frame = self
            .codec
            .encode(&control_envelope_frame(&envelope))
            .map_err(ScenarioError::Wire)?;
        for _ in 0..copies {
            self.transmit(COORDINATOR, MEMBER, frame.clone());
        }
        Ok(())
    }

    pub(super) fn replay_control(&mut self) {
        let pending = self
            .coordinator
            .replay()
            .map(control_envelope_frame)
            .collect::<Vec<_>>();
        for frame in pending {
            let Ok(encoded) = self.codec.encode(&frame) else {
                continue;
            };
            self.transmit(COORDINATOR, MEMBER, encoded);
        }
    }

    fn transmit(&mut self, source: &str, target: &str, payload: Bytes) {
        match self.network.send(source, target, payload) {
            Some(frame) => self.schedule_soon(ScenarioEvent::DeliverFrame(frame)),
            None => {
                self.state.lost_control_frames += 1;
                self.record_evidence(
                    Failpoint::ControlAfterOutboxBeforeSocketWrite,
                    FaultTarget::Queue,
                    FailAction::Drop,
                    FaultOutcome::MessageLost,
                    FaultOrigin::SimulatedExecutor,
                );
            }
        }
    }

    pub(super) fn deliver_frame(&mut self, id: u64) -> Result<(), ScenarioError> {
        let Some(frame) = self.network.deliver(id) else {
            self.state.lost_control_frames += 1;
            return Ok(());
        };
        let decoded = self
            .codec
            .decode(frame.payload)
            .map_err(ScenarioError::Wire)?;
        match decoded.kind {
            FrameKind::ControlEnvelope => {
                let envelope = decode_control_envelope(&decoded).map_err(ScenarioError::Control)?;
                match self.member.receive(envelope) {
                    ControlApply::Apply(_) => {
                        self.state.applied_control_commands += 1;
                        self.acknowledge();
                    }
                    ControlApply::Duplicate(_) => {
                        self.state.duplicate_control_commands += 1;
                        self.acknowledge();
                    }
                    ControlApply::Gap(_) | ControlApply::ReconcileEpoch => {}
                }
            }
            FrameKind::ControlAck => {
                let ack = decode_control_ack(&decoded).map_err(ScenarioError::Control)?;
                self.coordinator
                    .acknowledge(ack)
                    .map_err(ScenarioError::Control)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn acknowledge(&mut self) {
        let action = failpoint::hit_decision(Failpoint::ControlAfterRemoteApplyBeforeAck);
        if !action.is_continue() {
            let (target, outcome) = match action {
                FailAction::Crash => (FaultTarget::Target, FaultOutcome::ProcessCrashed),
                _ => (FaultTarget::Network, FaultOutcome::MessageLost),
            };
            if action == FailAction::Crash {
                self.target_process.crash();
                self.target_process
                    .restart(incarnation(self.state.target_incarnation));
            }
            self.record_evidence(
                Failpoint::ControlAfterRemoteApplyBeforeAck,
                target,
                action,
                outcome,
                FaultOrigin::SimulatedExecutor,
            );
            return;
        }
        let ack = self.member.current_ack();
        let Ok(encoded) = self.codec.encode(&control_ack_frame(ack)) else {
            return;
        };
        self.transmit(MEMBER, COORDINATOR, encoded);
    }
}
