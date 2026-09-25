//! Durable, scope-local transfer reservations. A reservation survives every
//! handoff phase and a Coordinator change, and is released with slot completion.

use serde::{Deserialize, Serialize};

use crate::{
    allocation::RebalanceLimits,
    types::{NodeKey, PlacementSlot, PlacementSlotKey},
};

use super::barrier::{BarrierProgress, BarrierSnapshot, TransferBarrier, participant_digest};
use super::{MemoryState, StorageError, validate_assignment_members};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TransferReservation {
    pub slot: PlacementSlotKey,
    pub operation: u128,
    pub source: NodeKey,
    pub target: NodeKey,
}

impl TransferReservation {
    pub fn from_slot(slot: &PlacementSlot) -> Result<Self, StorageError> {
        Ok(Self {
            slot: slot.key.clone(),
            operation: slot.active_move.ok_or(StorageError::InvalidRecord)?,
            source: slot.owner.clone().ok_or(StorageError::InvalidRecord)?,
            target: slot.target.clone().ok_or(StorageError::InvalidRecord)?,
        })
    }

    pub fn dimensions(&self, limits: RebalanceLimits) -> Vec<(String, usize)> {
        let mut result = vec![
            ("group".to_owned(), limits.concurrent_group),
            (
                format!("source/{:032x}", self.source.incarnation.get()),
                limits.concurrent_source,
            ),
            (
                format!("target/{:032x}", self.target.incarnation.get()),
                limits.concurrent_target,
            ),
        ];
        if let PlacementSlotKey::Shard { entity_type, .. } = &self.slot {
            result.push((
                format!("entity/{}", entity_type.as_str()),
                limits.concurrent_entity,
            ));
        }
        result
    }
}

pub(super) fn reserve_memory(
    state: &mut MemoryState,
    slot: &PlacementSlot,
    limits: RebalanceLimits,
) -> Result<(), StorageError> {
    limits.validate().map_err(|_| StorageError::InvalidConfig)?;
    let reservation = TransferReservation::from_slot(slot)?;
    if state.transfer_reservations.contains_key(&slot.key) {
        return Err(StorageError::CompareFailed);
    }
    let global = state
        .members
        .get(&reservation.target.node_id)
        .ok_or(StorageError::CompareFailed)?;
    let group = state
        .group_members
        .get(&(slot.key.group().clone(), reservation.target.node_id.clone()))
        .ok_or(StorageError::CompareFailed)?;
    validate_assignment_members(state, global, group, &reservation.target)?;
    for (dimension, maximum) in reservation.dimensions(limits) {
        let active = state
            .transfer_reservations
            .values()
            .filter(|existing| existing.slot.group() == reservation.slot.group())
            .filter(|existing| {
                existing
                    .dimensions(limits)
                    .iter()
                    .any(|(key, _)| key == &dimension)
            })
            .count();
        if active >= maximum {
            return Err(StorageError::Capacity);
        }
    }
    let snapshot = BarrierSnapshot {
        manifest: TransferBarrier {
            slot: slot.key.clone(),
            operation: reservation.operation,
            version: slot.version.clone(),
            source_revision: i64::try_from(slot.version.revision.get())
                .map_err(|_| StorageError::CounterExhausted)?,
            count: slot.barrier_sessions.len(),
            digest: participant_digest(&slot.barrier_sessions),
            sealed: true,
        },
        participants: slot
            .barrier_sessions
            .iter()
            .copied()
            .map(|p| (p, BarrierProgress::Pending))
            .collect(),
    };
    let barrier_key = (slot.key.clone(), reservation.operation);
    if let Some(previous) = state.transfer_barriers.get(&barrier_key) {
        if previous.manifest != snapshot.manifest {
            return Err(StorageError::CompareFailed);
        }
        previous.verify()?;
    } else {
        state.transfer_barriers.insert(barrier_key, snapshot);
    }
    state
        .transfer_reservations
        .insert(slot.key.clone(), reservation);
    Ok(())
}

pub(super) fn release_memory(
    state: &mut MemoryState,
    expected: &PlacementSlot,
) -> Result<(), StorageError> {
    let Some(operation) = expected.active_move else {
        return Ok(());
    };
    let reservation = state
        .transfer_reservations
        .get(&expected.key)
        .ok_or(StorageError::StorageMetadataMismatch)?;
    if reservation.operation != operation || expected.owner.as_ref() != Some(&reservation.target) {
        return Err(StorageError::CompareFailed);
    }
    state.transfer_reservations.remove(&expected.key);
    Ok(())
}
