use std::sync::{Arc, atomic::Ordering};

use lattice_core::{actor_ref::PlacementDomainId, coordinator::CoordinatorScope};
use tokio::{sync::Notify, time::Instant};

use super::{LocalAuthorityEvent, LogicCoordinatorHandle, LogicSessionError};
use crate::{
    control::{PlacementControlCommand, control_stream_id, encode_control_command_for_term},
    coordinator::{NodeLoadReport, ShardLoadReport},
    types::PlacementSlotKey,
};

impl LogicCoordinatorHandle {
    pub fn domain(&self) -> &PlacementDomainId {
        &self.domain
    }

    pub fn ready(&self) -> bool {
        self.state
            .lock()
            .expect("logic placement state poisoned")
            .ready()
    }

    /// Returns true when the domain snapshot is installed and every locally owned slot can admit
    /// messages.
    pub fn ready_for_admission(&self) -> bool {
        self.state
            .lock()
            .expect("logic placement state poisoned")
            .ready_for_admission()
    }

    pub fn change_notifier(&self) -> Arc<Notify> {
        self.state
            .lock()
            .expect("logic placement state poisoned")
            .change_notifier()
    }

    pub async fn complete_drain(
        &self,
        slot: PlacementSlotKey,
        succeeded: bool,
    ) -> Result<(), LogicSessionError> {
        self.local_events
            .send(LocalAuthorityEvent { slot, succeeded })
            .await
            .map_err(|_| LogicSessionError::ControlClosed)
    }

    pub fn publish_ready(&self, slot: &PlacementSlotKey) -> Result<(), LogicSessionError> {
        self.send_slot_command(slot, true, false)
    }

    pub fn publish_drained(&self, slot: &PlacementSlotKey) -> Result<(), LogicSessionError> {
        self.send_slot_command(slot, false, false)
    }

    pub fn publish_stop_failed(&self, slot: &PlacementSlotKey) -> Result<(), LogicSessionError> {
        self.send_slot_command(slot, false, true)
    }

    pub fn report_node_load(&self, report: NodeLoadReport) -> Result<(), LogicSessionError> {
        self.send_ephemeral(PlacementControlCommand::NodeLoad(report))
    }

    pub fn report_shard_load(&self, report: ShardLoadReport) -> Result<(), LogicSessionError> {
        self.send_ephemeral(PlacementControlCommand::ShardLoad(report))
    }

    pub fn begin_drain(&self, operation_id: String) -> Result<(), LogicSessionError> {
        let incarnation = self
            .state
            .lock()
            .expect("logic placement state poisoned")
            .local_node
            .incarnation;
        self.send_reliable(PlacementControlCommand::BeginDrain {
            operation_id,
            expected_incarnation: incarnation,
        })
    }

    pub async fn complete_member_drain(
        &self,
        operation_id: String,
    ) -> Result<(), LogicSessionError> {
        let incarnation = self
            .state
            .lock()
            .expect("logic placement state poisoned")
            .local_node
            .incarnation;
        let association = self
            .associations
            .get(&self.coordinator)
            .ok_or(LogicSessionError::AssociationUnavailable)?;
        let command_id = association.admit_control_command_in(
            control_stream_id(&CoordinatorScope::Placement(self.domain.clone())),
            encode_control_command_for_term(
                &CoordinatorScope::Placement(self.domain.clone()),
                self.coordinator_term.load(Ordering::Acquire),
                &PlacementControlCommand::DrainComplete {
                    operation_id,
                    expected_incarnation: incarnation,
                },
                self.maximum_control_payload,
            )
            .map_err(LogicSessionError::Control)?,
        )?;
        // Reliable control has no completion signal, so the acknowledgement is polled. The wait is
        // bounded: an unbounded poll turns a lost Coordinator into a drain that never returns.
        let deadline = Instant::now() + self.drain_acknowledgement_timeout;
        while association.control_command_pending(command_id) {
            if Instant::now() >= deadline {
                return Err(LogicSessionError::DrainNotAcknowledged);
            }
            tokio::time::sleep(self.drain_poll_interval).await;
        }
        Ok(())
    }

    fn send_ephemeral(&self, command: PlacementControlCommand) -> Result<(), LogicSessionError> {
        let association = self
            .associations
            .get(&self.coordinator)
            .ok_or(LogicSessionError::AssociationUnavailable)?;
        association.admit_ephemeral_control(
            encode_control_command_for_term(
                &CoordinatorScope::Placement(self.domain.clone()),
                self.coordinator_term.load(Ordering::Acquire),
                &command,
                self.maximum_control_payload,
            )
            .map_err(LogicSessionError::Control)?,
        )?;
        Ok(())
    }

    fn send_reliable(&self, command: PlacementControlCommand) -> Result<(), LogicSessionError> {
        let association = self
            .associations
            .get(&self.coordinator)
            .ok_or(LogicSessionError::AssociationUnavailable)?;
        association.admit_control_command_in(
            control_stream_id(&CoordinatorScope::Placement(self.domain.clone())),
            encode_control_command_for_term(
                &CoordinatorScope::Placement(self.domain.clone()),
                self.coordinator_term.load(Ordering::Acquire),
                &command,
                self.maximum_control_payload,
            )
            .map_err(LogicSessionError::Control)?,
        )?;
        Ok(())
    }

    fn send_slot_command(
        &self,
        slot: &PlacementSlotKey,
        ready: bool,
        stop_failed: bool,
    ) -> Result<(), LogicSessionError> {
        let generation = self
            .state
            .lock()
            .expect("logic placement state poisoned")
            .slot(slot)
            .ok_or(LogicSessionError::UnknownAuthority)?
            .assignment_generation;
        let command = if ready {
            PlacementControlCommand::SlotReady {
                slot: slot.clone(),
                generation,
            }
        } else if stop_failed {
            PlacementControlCommand::SlotStopFailed {
                slot: slot.clone(),
                generation,
            }
        } else {
            PlacementControlCommand::SlotDrained {
                slot: slot.clone(),
                generation,
            }
        };
        let association = self
            .associations
            .get(&self.coordinator)
            .ok_or(LogicSessionError::AssociationUnavailable)?;
        association.admit_control_command_in(
            control_stream_id(&CoordinatorScope::Placement(self.domain.clone())),
            encode_control_command_for_term(
                &CoordinatorScope::Placement(self.domain.clone()),
                self.coordinator_term.load(Ordering::Acquire),
                &command,
                self.maximum_control_payload,
            )
            .map_err(LogicSessionError::Control)?,
        )?;
        Ok(())
    }
}
