use std::sync::{Arc, atomic::Ordering};

use lattice_core::{actor_address::PlacementDomainId, coordinator::CoordinatorScope};
use tokio::{sync::Notify, time::Instant};

use super::{LocalAuthorityEvent, LogicCoordinatorHandle, LogicSessionError};
use crate::{
    control::{PlacementControlCommand, control_stream_id, encode_control_command_for_term},
    coordinator::{NodeLoadReport, ShardLoadReport},
    types::PlacementSlotKey,
};

impl LogicCoordinatorHandle {
    /// The operation for which this session accepted an authenticated coordinator commit response.
    pub fn committed_member_drain(&self) -> Option<String> {
        self.drain_confirmation.committed_operation()
    }

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

    /// Starts or resumes this operation at the last locally confirmed drain stage.
    pub fn begin_drain(&self, operation_id: String) -> Result<(), LogicSessionError> {
        if self.committed_member_drain().as_deref() == Some(&operation_id) {
            return Ok(());
        }
        let node = self
            .state
            .lock()
            .expect("logic placement state poisoned")
            .local_node
            .clone();
        let command = if self.drain_confirmation.requested() {
            self.drain_confirmation
                .request(&operation_id, self.coordinator_term.load(Ordering::Acquire))?;
            // The coordinator may already have removed its session after committing. It can
            // replay that durable outcome, but cannot send DrainReady for a new BeginDrain.
            PlacementControlCommand::DrainComplete {
                operation_id,
                node_id: node.node_id,
                expected_incarnation: node.incarnation,
            }
        } else {
            PlacementControlCommand::BeginDrain {
                operation_id,
                expected_incarnation: node.incarnation,
            }
        };
        self.send_reliable(command)
    }

    pub async fn complete_member_drain(
        &self,
        operation_id: String,
    ) -> Result<(), LogicSessionError> {
        self.complete_member_drain_until(
            operation_id,
            Instant::now() + self.drain_acknowledgement_timeout,
        )
        .await
    }

    pub async fn complete_member_drain_until(
        &self,
        operation_id: String,
        deadline: Instant,
    ) -> Result<(), LogicSessionError> {
        if self.committed_member_drain().as_deref() == Some(&operation_id) {
            return Ok(());
        }
        let term = self
            .request_member_drain_until(&operation_id, deadline)
            .await?;
        self.drain_confirmation
            .wait(&operation_id, term, deadline)
            .await
    }

    /// Enqueues completion without blocking the effect consumer that applies its later commit
    /// notification. The service's leave loop owns the absolute operation deadline.
    pub async fn request_member_drain(&self, operation_id: &str) -> Result<(), LogicSessionError> {
        self.request_member_drain_until(
            operation_id,
            Instant::now() + super::CONTROL_ADMISSION_TIMEOUT,
        )
        .await
        .map(|_| ())
    }

    async fn request_member_drain_until(
        &self,
        operation_id: &str,
        deadline: Instant,
    ) -> Result<u64, LogicSessionError> {
        if Instant::now() >= deadline {
            return Err(LogicSessionError::DrainNotAcknowledged);
        }
        let node = self
            .state
            .lock()
            .expect("logic placement state poisoned")
            .local_node
            .clone();
        let term = self.coordinator_term.load(Ordering::Acquire);
        self.drain_confirmation.request(operation_id, term)?;
        if self.drain_confirmation.committed_operation().as_deref() == Some(operation_id) {
            return Ok(term);
        }
        let association = self
            .associations
            .get(&self.coordinator)
            .ok_or(LogicSessionError::AssociationUnavailable)?;
        tokio::time::timeout_at(
            deadline,
            association.admit_control_command_in_wait(
                control_stream_id(&CoordinatorScope::Placement(self.domain.clone())),
                encode_control_command_for_term(
                    &CoordinatorScope::Placement(self.domain.clone()),
                    term,
                    &PlacementControlCommand::DrainComplete {
                        operation_id: operation_id.to_owned(),
                        node_id: node.node_id,
                        expected_incarnation: node.incarnation,
                    },
                    self.maximum_control_payload,
                )
                .map_err(LogicSessionError::Control)?,
                super::CONTROL_ADMISSION_TIMEOUT,
            ),
        )
        .await
        .map_err(|_| LogicSessionError::DrainNotAcknowledged)??;
        Ok(term)
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
        if ready {
            // Readiness is level-triggered and replayed by the session heartbeat. Keeping it out
            // of reliable control prevents a large first-allocation burst from starving the
            // association's membership and placement heartbeats.
            match self.send_ephemeral(command) {
                Ok(()) | Err(LogicSessionError::Association(_)) => Ok(()),
                Err(error) => Err(error),
            }
        } else {
            self.send_reliable(command)
        }
    }
}
