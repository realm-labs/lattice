use std::sync::atomic::Ordering;

use tokio::sync::mpsc;

use super::{
    Association, AssociationError, AssociationReceivers, AssociationState, AttachmentDecision,
    LaneAttachment, LaneKind, attached_lanes_mask, lane_mask,
};
use crate::{control::control_envelope_frame, wire::Frame};

impl Association {
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

    pub(crate) fn lane_receiver_available(&self, lane: LaneKind) -> bool {
        let slots = self
            .receivers
            .lock()
            .expect("association receivers poisoned");
        match lane {
            LaneKind::Control => slots.control.is_some(),
            LaneKind::Interactive => slots.interactive.is_some(),
            LaneKind::Bulk(index) => slots
                .bulk
                .get(usize::from(index))
                .is_some_and(Option::is_some),
        }
    }

    pub fn return_lane_receiver(
        &self,
        lane: LaneKind,
        receiver: mpsc::Receiver<Frame>,
    ) -> Result<(), AssociationError> {
        let mut slots = self
            .receivers
            .lock()
            .expect("association receivers poisoned");
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

    pub fn attach(
        &self,
        attachment: LaneAttachment,
    ) -> Result<AttachmentDecision, AssociationError> {
        self.attach_with_activation(attachment)
            .map(|(decision, _)| decision)
    }

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

    /// Attaches a lane to the connection that owns its queue receiver.
    ///
    /// Attaching and taking the receiver must not be separable: a connection that attaches
    /// without going on to run the lane leaves an entry in the lane map that only its own
    /// nonce could ever remove, which permanently pins the association as live. Claiming
    /// the receiver first means every attachment has a running lane behind it, and every
    /// failure after the attachment undoes it.
    pub(crate) fn attach_owned_lane(
        &self,
        attachment: LaneAttachment,
    ) -> Result<mpsc::Receiver<Frame>, AssociationError> {
        let lane = attachment.lane;
        let connection_nonce = attachment.connection_nonce;
        let receiver = self
            .take_lane_receiver(lane)
            .ok_or(AssociationError::LaneReceiverConflict)?;
        match self.attach_and_replay(attachment) {
            Ok(_) => Ok(receiver),
            Err(error) => {
                self.detach(lane, connection_nonce);
                let _ = self.return_lane_receiver(lane, receiver);
                Err(error)
            }
        }
    }

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

    pub fn detach(&self, lane: LaneKind, connection_nonce: u128) {
        let mut inner = self.inner.lock().expect("association state poisoned");
        if inner.lanes.get(&lane) != Some(&connection_nonce) {
            return;
        }
        inner.lanes.remove(&lane);
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
