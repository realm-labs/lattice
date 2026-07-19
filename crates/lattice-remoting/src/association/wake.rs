use bytes::Bytes;

use super::{Association, AssociationError, AssociationManager, AssociationState, LaneKind};
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
        self.inner
            .lock()
            .expect("association state poisoned")
            .lanes
            .len()
    }

    pub(super) fn prepare_data_lane(&self, lane: LaneKind) -> Result<(), AssociationError> {
        let needs_wake = {
            let mut inner = self.inner.lock().expect("association state poisoned");
            if inner.state != AssociationState::Active {
                return Err(AssociationError::NotActive);
            }
            !inner.lanes.contains_key(&lane) && inner.wake_pending.insert(lane)
        };
        if !needs_wake {
            return Ok(());
        }
        self.notify_lane_wake(lane)?;
        let frame = Frame {
            kind: FrameKind::LaneWake,
            payload: Bytes::copy_from_slice(&[encode_lane_wake(lane)?]),
        };
        if let Err(error) = self.try_admit_control(frame) {
            self.inner
                .lock()
                .expect("association state poisoned")
                .wake_pending
                .remove(&lane);
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
