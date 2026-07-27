use lattice_core::actor_ref::ProtocolId;

use super::{Association, AssociationError, AssociationState};
use crate::{
    control::{
        CommandId, ControlAck, ControlApply, ControlEnvelope, ControlStreamId,
        ReliableControlError, control_envelope_frame,
    },
    protocol::{
        CatalogueDecision, CatalogueError, ProtocolCatalogue, ProtocolDescriptor,
        ProtocolFingerprint,
    },
    wire::{Frame, FrameKind},
};

impl Association {
    pub fn try_admit_control(&self, frame: Frame) -> Result<(), AssociationError> {
        self.try_admit(&self.control, frame)
    }

    pub fn admit_control_command(
        &self,
        payload: bytes::Bytes,
    ) -> Result<CommandId, AssociationError> {
        self.admit_control_command_in(ControlStreamId::DEFAULT, payload)
    }

    pub fn admit_control_command_in(
        &self,
        stream_id: ControlStreamId,
        payload: bytes::Bytes,
    ) -> Result<CommandId, AssociationError> {
        let command_id = CommandId::generate();
        let mut reliable_control = self
            .reliable_control
            .lock()
            .expect("reliable control state poisoned");
        let envelope = reliable_control
            .enqueue_in(stream_id, command_id, payload)
            .map_err(AssociationError::ReliableControl)?;
        if self.state() == AssociationState::Active
            && let Err(error) = self.try_admit_control(control_envelope_frame(&envelope))
        {
            reliable_control.rollback_last(command_id);
            return Err(error);
        }
        Ok(command_id)
    }

    /// Waits for reliable-control outbox capacity without rebuilding or restarting the logical
    /// operation that produced `payload`. The timeout is an inactivity bound: every caller can
    /// renew it for the next frame after making progress.
    pub async fn admit_control_command_in_wait(
        &self,
        stream_id: ControlStreamId,
        payload: bytes::Bytes,
        wait_timeout: std::time::Duration,
    ) -> Result<CommandId, AssociationError> {
        let deadline = tokio::time::Instant::now() + wait_timeout;
        loop {
            let outbox_changed = self.control_outbox_changed.notified();
            tokio::pin!(outbox_changed);
            outbox_changed.as_mut().enable();
            match self.admit_control_command_in(stream_id, payload.clone()) {
                Ok(command_id) => return Ok(command_id),
                Err(AssociationError::ReliableControl(ReliableControlError::OutboxFull)) => {
                    tokio::select! {
                        () = outbox_changed.as_mut() => {}
                        () = tokio::time::sleep_until(deadline) => {
                            return Err(AssociationError::ReliableControl(
                                ReliableControlError::OutboxFull,
                            ));
                        }
                    }
                }
                Err(AssociationError::QueueFull) => {
                    tokio::select! {
                        permit = self.control.reserve() => {
                            drop(permit.map_err(|_| AssociationError::Closed)?);
                        }
                        () = tokio::time::sleep_until(deadline) => {
                            return Err(AssociationError::QueueFull);
                        }
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub fn admit_ephemeral_control(&self, payload: bytes::Bytes) -> Result<(), AssociationError> {
        self.try_admit_control(Frame::new(FrameKind::CoordinatorEvent, payload))
    }

    pub fn replay_control_frames(&self) -> Vec<Frame> {
        self.reliable_control
            .lock()
            .expect("reliable control state poisoned")
            .replay()
            .map(control_envelope_frame)
            .collect()
    }

    pub fn control_outbox_len(&self) -> usize {
        self.reliable_control
            .lock()
            .expect("reliable control state poisoned")
            .replay()
            .len()
    }

    pub fn control_command_pending(&self, command_id: CommandId) -> bool {
        self.reliable_control
            .lock()
            .expect("reliable control state poisoned")
            .contains_outbound(command_id)
    }

    pub fn preview_control(&self, envelope: &ControlEnvelope) -> ControlApply {
        self.reliable_control
            .lock()
            .expect("reliable control state poisoned")
            .preview(envelope)
    }

    pub fn commit_control(&self, envelope: ControlEnvelope) -> ControlAck {
        self.reliable_control
            .lock()
            .expect("reliable control state poisoned")
            .commit(envelope)
    }

    pub fn acknowledge_control(&self, ack: ControlAck) -> Result<(), AssociationError> {
        self.reliable_control
            .lock()
            .expect("reliable control state poisoned")
            .acknowledge(ack)
            .map_err(AssociationError::ReliableControl)?;
        self.control_outbox_changed.notify_waiters();
        Ok(())
    }

    pub fn current_control_ack(&self, stream_id: ControlStreamId) -> ControlAck {
        self.reliable_control
            .lock()
            .expect("reliable control state poisoned")
            .current_ack(stream_id)
    }

    pub fn install_peer_catalogue<I>(&self, descriptors: I) -> Result<(), AssociationError>
    where
        I: IntoIterator<Item = ProtocolDescriptor>,
    {
        let mut catalogue = ProtocolCatalogue::new(self.config.max_protocols_per_peer)
            .expect("validated protocol catalogue limit");
        catalogue
            .install(descriptors)
            .map_err(AssociationError::Catalogue)?;
        if let Some(installed) = self.peer_catalogue.get() {
            return if installed == &catalogue {
                Ok(())
            } else {
                Err(AssociationError::Catalogue(
                    CatalogueError::ChangedAfterInstall,
                ))
            };
        }
        match self.peer_catalogue.set(catalogue) {
            Ok(()) => Ok(()),
            Err(catalogue) if self.peer_catalogue.get() == Some(&catalogue) => Ok(()),
            Err(_) => Err(AssociationError::Catalogue(
                CatalogueError::ChangedAfterInstall,
            )),
        }
    }

    pub fn protocol_decision(
        &self,
        protocol_id: ProtocolId,
        fingerprint: ProtocolFingerprint,
    ) -> CatalogueDecision {
        self.peer_catalogue
            .get()
            .map_or(CatalogueDecision::Unsupported, |catalogue| {
                catalogue.compare(protocol_id, fingerprint)
            })
    }
}
