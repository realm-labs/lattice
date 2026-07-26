use std::sync::atomic::Ordering;

use super::{Association, AssociationError, BulkAdmission, LaneKind};
use crate::wire::Frame;

impl Association {
    pub fn try_admit_interactive(&self, frame: Frame) -> Result<(), AssociationError> {
        self.prepare_data_lane(LaneKind::Interactive)?;
        self.try_admit(&self.interactive, frame)
    }

    pub(crate) fn try_reserve_bulk<F>(
        &self,
        update_route_hash: F,
        bytes: usize,
    ) -> Result<(usize, BulkAdmission<'_>), AssociationError>
    where
        F: FnOnce(&mut blake3::Hasher),
    {
        let stripe = self.bulk_stripe(update_route_hash)?;
        let admission = self.try_reserve_prepared_bulk(stripe, bytes)?;
        Ok((stripe, admission))
    }

    pub(crate) fn bulk_stripe<F>(&self, update_route_hash: F) -> Result<usize, AssociationError>
    where
        F: FnOnce(&mut blake3::Hasher),
    {
        self.ensure_active()?;
        Ok(if self.bulk.len() == 1 {
            0
        } else {
            let mut hasher = blake3::Hasher::new();
            update_route_hash(&mut hasher);
            stripe_from_hash(&hasher.finalize(), self.bulk.len())
        })
    }

    pub(crate) fn allocate_exact_target_dictionary_id(&self, stripe: usize) -> Option<u64> {
        let next = self.next_outbound_exact_target_ids.get(stripe)?;
        next.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (current
                < crate::messaging::target_dictionary::MAX_EXACT_TARGET_DICTIONARY_ENTRIES as u64)
                .then_some(current + 1)
        })
        .ok()
        .map(|previous| previous + 1)
    }

    pub(crate) fn bulk_lane_epoch(&self, stripe: usize) -> u64 {
        self.bulk_lane_epochs
            .get(stripe)
            .map_or(0, |epoch| epoch.load(Ordering::Acquire))
    }
}

fn stripe_from_hash(hash: &blake3::Hash, stripes: usize) -> usize {
    debug_assert!(stripes > 0);
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(&hash.as_bytes()[..8]);
    (u64::from_be_bytes(prefix) as usize) % stripes
}
