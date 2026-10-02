//! Reliable control admission, replay watermarks and the immutable peer catalogue.
//!
//! The reliable outbox survives socket reconnection. A command first enters the outbox and,
//! while active, the Control frame queue. Queue rejection rolls that new command back so a
//! caller can retry without leaving an unreported command behind. Activation replays outstanding
//! commands under the same lock. An acknowledgement frees outbox capacity and wakes waiters.

use lattice_model::actor::ProtocolId;

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
    /// Enqueues a Control frame without waiting or retaining it for reliable replay.
    ///
    /// # Errors
    ///
    /// Returns an error if admission is not active, the frame queue is full, or a payload budget
    /// is exhausted. Reliable commands should use [`Self::admit_control_command_in`] instead.
    pub fn try_admit_control(&self, frame: Frame) -> Result<(), AssociationError> {
        self.try_admit(&self.control, frame)
    }

    /// Admits a replayable command to the default reliable control stream.
    ///
    /// See [`Self::admit_control_command_in`] for buffering and error behavior.
    pub fn admit_control_command(
        &self,
        payload: bytes::Bytes,
    ) -> Result<CommandId, AssociationError> {
        self.admit_control_command_in(ControlStreamId::DEFAULT, payload)
    }

    /// Records a replayable command and returns its generated identifier.
    ///
    /// While active, also enqueues the command's frame. Otherwise the outbox retains it for
    /// a later activation replay. Success means local acceptance, not remote application.
    ///
    /// # Errors
    ///
    /// Returns an error for outbox/stream limits or failure to enqueue an active command.
    /// Frame admission failure rolls back the newly recorded command and its sequence number.
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

    /// Admits a reliable command using the configured control retry timeout for capacity waits.
    ///
    /// See [`Self::admit_control_command_in_wait`] for the wait and returned errors.
    pub async fn admit_control_command_in_wait_configured(
        &self,
        stream_id: ControlStreamId,
        payload: bytes::Bytes,
    ) -> Result<CommandId, AssociationError> {
        self.admit_control_command_in_wait(
            stream_id,
            payload,
            self.config.control_apply_retry_timeout,
        )
        .await
    }

    /// Enqueues a coordinator event without reliable replay or acknowledgement tracking.
    ///
    /// Returns the same admission errors as [`Self::try_admit_control`].
    pub fn admit_ephemeral_control(&self, payload: bytes::Bytes) -> Result<(), AssociationError> {
        self.try_admit_control(Frame::new(FrameKind::CoordinatorEvent, payload))
    }

    /// Builds frames for all currently unacknowledged outbound commands without removing them.
    pub fn replay_control_frames(&self) -> Vec<Frame> {
        self.reliable_control
            .lock()
            .expect("reliable control state poisoned")
            .replay()
            .map(control_envelope_frame)
            .collect()
    }

    /// Returns the number of commands retained for acknowledgement or reconnection replay.
    pub fn control_outbox_len(&self) -> usize {
        self.reliable_control
            .lock()
            .expect("reliable control state poisoned")
            .replay()
            .len()
    }

    /// Returns whether an outbound command is still retained in the reliable outbox.
    pub fn control_command_pending(&self, command_id: CommandId) -> bool {
        self.reliable_control
            .lock()
            .expect("reliable control state poisoned")
            .contains_outbound(command_id)
    }

    /// Checks whether an inbound command can be applied without advancing its stream watermark.
    ///
    /// The control worker applies the operation before calling [`Self::commit_control`], so a
    /// transient application failure can retry without acknowledging an unapplied command.
    pub fn preview_control(&self, envelope: &ControlEnvelope) -> ControlApply {
        self.reliable_control
            .lock()
            .expect("reliable control state poisoned")
            .preview(envelope)
    }

    /// Commits an applied inbound command's watermark and returns the cumulative acknowledgement.
    ///
    /// Call only after preview and successful application; this does not run the operation itself.
    pub fn commit_control(&self, envelope: ControlEnvelope) -> ControlAck {
        self.reliable_control
            .lock()
            .expect("reliable control state poisoned")
            .commit(envelope)
    }

    /// Releases outbound commands covered by a peer's cumulative acknowledgement.
    ///
    /// Wakes callers waiting for outbox capacity after accepting the acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns [`AssociationError::ReliableControl`] if the acknowledgement fails validation.
    pub fn acknowledge_control(&self, ack: ControlAck) -> Result<(), AssociationError> {
        self.reliable_control
            .lock()
            .expect("reliable control state poisoned")
            .acknowledge(ack)
            .map_err(AssociationError::ReliableControl)?;
        self.control_outbox_changed.notify_waiters();
        Ok(())
    }

    /// Returns the acknowledgement for the current inbound watermark of a control stream.
    pub fn current_control_ack(&self, stream_id: ControlStreamId) -> ControlAck {
        self.reliable_control
            .lock()
            .expect("reliable control state poisoned")
            .current_ack(stream_id)
    }

    /// Installs the peer's protocol catalogue for this generation.
    ///
    /// Reinstalling identical descriptors is allowed; changing them requires another generation.
    ///
    /// # Errors
    ///
    /// Returns [`AssociationError::Catalogue`] for invalid descriptors, excessive entries or a
    /// catalogue that differs from the one already installed.
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

    /// Compares an expected protocol fingerprint with the peer's installed catalogue.
    ///
    /// Returns [`CatalogueDecision::Unsupported`] if no catalogue has been installed.
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
