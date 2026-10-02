//! Receiver ownership and nonce-qualified connection registration.
//!
//! Registration does not transfer a socket or a receiver. The supervisor coordinates those
//! resources separately. The nonce check on detach prevents an old socket completion from
//! unregistering its replacement.

use std::sync::atomic::Ordering;

use tokio::sync::mpsc;

use super::{
    Association, AssociationError, AssociationReceivers, AssociationState, AttachmentDecision,
    LaneAttachment, LaneKind, attached_lanes_mask, lane_mask,
};
use crate::{control::control_envelope_frame, wire::Frame};

impl Association {
    /// Drops only receivers still held in the association, releasing their queued frames.
    pub(super) fn discard_unowned_receivers(&self) {
        let mut slots = self
            .receivers
            .lock()
            .expect("association receivers poisoned");
        slots.control.take();
        slots.interactive.take();
        for receiver in &mut slots.bulk {
            receiver.take();
        }
    }

    /// Transfers all configured receivers if none has already been taken.
    ///
    /// Returns `None` without transferring anything if any receiver is unavailable.
    /// Endpoint supervisors call this once when taking ownership of a generation.
    pub fn take_receivers(&self) -> Option<AssociationReceivers> {
        let mut slots = self
            .receivers
            .lock()
            .expect("association receivers poisoned");
        if slots.control.is_none()
            || slots.interactive.is_none()
            || slots.bulk.iter().any(Option::is_none)
        {
            return None;
        }
        Some(AssociationReceivers {
            control: slots.control.take().expect("checked control receiver"),
            interactive: slots
                .interactive
                .take()
                .expect("checked interactive receiver"),
            bulk: slots
                .bulk
                .iter_mut()
                .map(|receiver| receiver.take().expect("checked bulk receiver"))
                .collect(),
        })
    }

    /// Transfers a single lane receiver, or returns `None` if it is unavailable or invalid.
    ///
    /// Intended for standalone lane runners. Do not take a receiver already owned by a supervisor.
    pub fn take_lane_receiver(&self, lane: LaneKind) -> Option<mpsc::Receiver<Frame>> {
        let mut slots = self
            .receivers
            .lock()
            .expect("association receivers poisoned");
        match lane {
            LaneKind::Control => slots.control.take(),
            LaneKind::Interactive => slots.interactive.take(),
            LaneKind::Bulk(index) => slots.bulk.get_mut(usize::from(index))?.take(),
        }
    }

    /// Returns a stopped standalone lane's receiver for reuse.
    ///
    /// A retiring association discards the receiver instead of storing it again.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid Bulk stripe or a receiver slot that is already occupied.
    pub fn return_lane_receiver(
        &self,
        lane: LaneKind,
        receiver: mpsc::Receiver<Frame>,
    ) -> Result<(), AssociationError> {
        let mut slots = self
            .receivers
            .lock()
            .expect("association receivers poisoned");
        if matches!(
            self.state(),
            AssociationState::Closing | AssociationState::Closed
        ) {
            return Ok(());
        }
        let slot = match lane {
            LaneKind::Control => &mut slots.control,
            LaneKind::Interactive => &mut slots.interactive,
            LaneKind::Bulk(index) => slots
                .bulk
                .get_mut(usize::from(index))
                .ok_or(AssociationError::InvalidBulkStripe(index))?,
        };
        if slot.is_some() {
            return Err(AssociationError::LaneReceiverConflict);
        }
        *slot = Some(receiver);
        Ok(())
    }

    /// Registers a negotiated connection and publishes activation once all lanes are present.
    ///
    /// Duplicate connections are resolved by keeping the lower nonce. This method does not
    /// open or close sockets, transfer receivers, or replay reliable control commands.
    ///
    /// # Errors
    ///
    /// Returns an error if the generation/key does not match, the Bulk stripe is invalid, or
    /// retirement has begun.
    pub fn attach(
        &self,
        attachment: LaneAttachment,
    ) -> Result<AttachmentDecision, AssociationError> {
        self.attach_with_activation(attachment)
            .map(|(decision, _)| decision)
    }

    /// Registers a lane and reports whether this attachment transitioned the group to `Active`.
    ///
    /// The registration lock also serializes `begin_close`, preventing an attachment from
    /// reviving a retiring generation. Publish the mask before activation so active senders
    /// observe the corresponding lane presence.
    pub(crate) fn attach_with_activation(
        &self,
        attachment: LaneAttachment,
    ) -> Result<(AttachmentDecision, bool), AssociationError> {
        if attachment.association_id != self.id || attachment.key != self.key {
            return Err(AssociationError::IdentityMismatch);
        }
        if let LaneKind::Bulk(index) = attachment.lane
            && usize::from(index) >= self.config.bulk_stripes
        {
            return Err(AssociationError::InvalidBulkStripe(index));
        }
        let mut inner = self.inner.lock().expect("association state poisoned");
        if matches!(
            self.state(),
            AssociationState::Closing | AssociationState::Closed
        ) {
            return Err(AssociationError::Closed);
        }
        let decision = match inner.lanes.get_mut(&attachment.lane) {
            None => {
                inner
                    .lanes
                    .insert(attachment.lane, attachment.connection_nonce);
                AttachmentDecision::Attached
            }
            Some(current) if attachment.connection_nonce < *current => {
                *current = attachment.connection_nonce;
                AttachmentDecision::ReplacedDuplicate
            }
            Some(_) => AttachmentDecision::RejectedDuplicate,
        };
        if decision != AttachmentDecision::RejectedDuplicate
            && let LaneKind::Bulk(index) = attachment.lane
        {
            self.bulk_lane_epochs[usize::from(index)].fetch_add(1, Ordering::AcqRel);
        }
        let lane_mask = lane_mask(attachment.lane);
        if attachment.lane == LaneKind::Control && decision != AttachmentDecision::RejectedDuplicate
        {
            inner.authenticated_control_peer = None;
        }
        self.attached_lanes
            .store(attached_lanes_mask(&inner.lanes), Ordering::Release);
        self.wake_pending_lanes
            .fetch_and(!lane_mask, Ordering::AcqRel);
        self.record_peer_activity();
        let activated =
            self.state() != AssociationState::Active && self.has_complete_lane_group(&inner.lanes);
        if activated {
            self.state
                .store(AssociationState::Active as u8, Ordering::Release);
            self.ever_active.store(true, Ordering::Release);
            self.state_changed.notify_waiters();
        }
        Ok((decision, activated))
    }

    /// Registers a lane and queues unacknowledged control commands when activation is restored.
    ///
    /// Holding the reliable-control lock across activation prevents newly admitted commands
    /// from overtaking the replay. If replay fails after registration, the caller must detach
    /// the candidate; the endpoint supervisor performs that rollback in the same turn.
    pub(crate) fn attach_and_replay(
        &self,
        attachment: LaneAttachment,
    ) -> Result<AttachmentDecision, AssociationError> {
        let reliable_control = self
            .reliable_control
            .lock()
            .expect("reliable control state poisoned");
        let (decision, activated) = self.attach_with_activation(attachment)?;
        if activated {
            for envelope in reliable_control.replay() {
                self.try_admit_control(control_envelope_frame(envelope))?;
            }
        }
        Ok(decision)
    }

    /// Unregisters a lane only if `connection_nonce` still identifies its current connection.
    ///
    /// A stale completion has no effect. Detaching an active data lane preserves `Active`,
    /// allowing independent sleep or reconnection. Detaching Control publishes `Reconnecting`,
    /// invalidates its authentication proof, and wakes data-lane supervision for restoration.
    /// This method does not close the socket or release the consumer's receiver.
    pub fn detach(&self, lane: LaneKind, connection_nonce: u128) {
        let mut inner = self.inner.lock().expect("association state poisoned");
        if inner.lanes.get(&lane) != Some(&connection_nonce) {
            return;
        }
        inner.lanes.remove(&lane);
        if lane == LaneKind::Control {
            inner.authenticated_control_peer = None;
        }
        self.attached_lanes
            .store(attached_lanes_mask(&inner.lanes), Ordering::Release);
        if lane == LaneKind::Control || self.state() != AssociationState::Active {
            self.state
                .store(AssociationState::Reconnecting as u8, Ordering::Release);
            self.state_changed.notify_waiters();
        }
        if lane == LaneKind::Control {
            self.wake_pending_lanes.store(0, Ordering::Release);
            drop(inner);
            self.interactive_wake.notify_one();
            for wake in &self.bulk_wakes {
                wake.notify_one();
            }
        }
    }
}
