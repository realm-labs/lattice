use bytes::Bytes;
use std::sync::atomic::Ordering;

use super::{Association, AssociationError, AssociationManager, LaneKind, lane_mask};
use crate::wire::{Frame, FrameKind};

impl Association {
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

    pub fn attached_lane_count(&self) -> usize {
        self.attached_lanes.load(Ordering::Acquire).count_ones() as usize
    }

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
        let frame = Frame {
            kind: FrameKind::LaneWake,
            payload: Bytes::copy_from_slice(&[encode_lane_wake(lane)?]),
        };
        if let Err(error) = self.try_admit_control(frame) {
            self.wake_pending_lanes.fetch_and(!mask, Ordering::AcqRel);
            return Err(error);
        }
        Ok(())
    }
}

impl AssociationManager {
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
