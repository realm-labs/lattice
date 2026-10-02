//! Coalesced requests to restore sleeping data lanes.
//!
//! A sender wakes the local supervisor and sends a `LaneWake` frame over the existing Control
//! connection. The deterministic dialer establishes the replacement socket; its peer waits for
//! an inbound candidate. Control never sleeps and is restored by the retry path if it fails.

use bytes::Bytes;
use std::sync::atomic::Ordering;

use super::{Association, AssociationError, AssociationManager, LaneKind, lane_mask};
use crate::wire::{Frame, FrameKind};

impl Association {
    /// Reads the published presence bit without acquiring the registration lock.
    pub(crate) fn is_lane_attached(&self, lane: LaneKind) -> bool {
        self.attached_lanes.load(Ordering::Acquire) & lane_mask(lane) != 0
    }

    /// Waits for one coalesced data-lane wake; Control has no wake event.
    ///
    /// `notify_one` retains a permit when no waiter exists yet, so rearming after handling an
    /// event does not lose a concurrent wake. Retirement cancels the waiter through actor cleanup.
    pub(crate) async fn wait_for_lane_wake(&self, lane: LaneKind) {
        match lane {
            LaneKind::Control => std::future::pending().await,
            LaneKind::Interactive => self.interactive_wake.notified().await,
            LaneKind::Bulk(index) => {
                if let Some(wake) = self.bulk_wakes.get(usize::from(index)) {
                    wake.notified().await;
                }
            }
        }
    }

    /// Signals local supervision for a data lane, without dialing or contacting the peer.
    pub(crate) fn notify_lane_wake(&self, lane: LaneKind) -> Result<(), AssociationError> {
        match lane {
            LaneKind::Control => return Err(AssociationError::InvalidLaneWake),
            LaneKind::Interactive => self.interactive_wake.notify_one(),
            LaneKind::Bulk(index) => self
                .bulk_wakes
                .get(usize::from(index))
                .ok_or(AssociationError::InvalidBulkStripe(index))?
                .notify_one(),
        }
        Ok(())
    }

    /// Returns the number of locally registered connections, excluding sleeping data lanes.
    ///
    /// This is a concurrent snapshot, not a network liveness probe.
    pub fn attached_lane_count(&self) -> usize {
        self.attached_lanes.load(Ordering::Acquire).count_ones() as usize
    }

    /// Requests restoration of an absent data lane before admitting its business frame.
    ///
    /// Requires `Active`; a missing Control connection is handled by supervision, not this
    /// path. The pending bit coalesces repeated sends until attachment clears it. Rechecking
    /// state and presence after claiming the bit handles concurrent retirement or attachment.
    ///
    /// Success means the lane is attached or a wake has been requested, not that socket setup
    /// has completed. The frame can wait in its bounded queue while the connection is restored.
    pub(super) fn prepare_data_lane(&self, lane: LaneKind) -> Result<(), AssociationError> {
        self.ensure_active()?;
        let mask = lane_mask(lane);
        if self.attached_lanes.load(Ordering::Acquire) & mask != 0 {
            return Ok(());
        }
        if self.wake_pending_lanes.fetch_or(mask, Ordering::AcqRel) & mask != 0 {
            return Ok(());
        }
        if let Err(error) = self.ensure_active() {
            self.wake_pending_lanes.fetch_and(!mask, Ordering::AcqRel);
            return Err(error);
        }
        if self.attached_lanes.load(Ordering::Acquire) & mask != 0 {
            self.wake_pending_lanes.fetch_and(!mask, Ordering::AcqRel);
            return Ok(());
        }
        self.notify_lane_wake(lane)?;
        let frame = Frame::new(
            FrameKind::LaneWake,
            Bytes::copy_from_slice(&[encode_lane_wake(lane)?]),
        );
        if let Err(error) = self.try_admit_control(frame) {
            self.wake_pending_lanes.fetch_and(!mask, Ordering::AcqRel);
            return Err(error);
        }
        Ok(())
    }
}

impl AssociationManager {
    /// Returns the sum of registered lanes across current associations.
    pub fn attached_lane_count(&self) -> usize {
        self.associations
            .lock()
            .expect("association registry poisoned")
            .values()
            .map(|association| association.attached_lane_count())
            .sum()
    }
}

fn encode_lane_wake(lane: LaneKind) -> Result<u8, AssociationError> {
    match lane {
        LaneKind::Control => Err(AssociationError::InvalidLaneWake),
        LaneKind::Interactive => Ok(0),
        LaneKind::Bulk(index) => index
            .checked_add(1)
            .ok_or(AssociationError::InvalidLaneWake),
    }
}
