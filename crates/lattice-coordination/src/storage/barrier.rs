//! Immutable participant manifests and separately persisted progress.
//!
//! A missing participant is an integrity error, never a terminal outcome. The
//! complete identity set is verified before a caller may advance a handoff.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use lattice_model::cluster::NodeIncarnation;
use serde::{Deserialize, Serialize};

use crate::{
    coordinator::GroupLeaderGuard,
    types::{NodeKey, PlacementSlotKey, PlacementVersion},
};

use super::{CoordinatorLeaseStore, InMemoryCoordinationStore, StorageError, validate_guard};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BarrierProgress {
    Pending,
    Applied,
    SessionFenced,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferBarrier {
    pub slot: PlacementSlotKey,
    pub operation: u128,
    pub version: PlacementVersion,
    pub source_revision: i64,
    pub count: usize,
    pub digest: [u8; 32],
    pub sealed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarrierSnapshot {
    pub manifest: TransferBarrier,
    pub participants: BTreeMap<NodeIncarnation, BarrierProgress>,
}

impl BarrierSnapshot {
    pub fn verify(&self) -> Result<(), StorageError> {
        let identities = self.participants.keys().copied().collect();
        if !self.manifest.sealed
            || self.manifest.count != self.participants.len()
            || self.manifest.digest != participant_digest(&identities)
        {
            return Err(StorageError::StorageMetadataMismatch);
        }
        Ok(())
    }

    pub fn required(&self) -> BTreeSet<NodeIncarnation> {
        self.participants.keys().copied().collect()
    }
}

pub(crate) fn participant_digest(participants: &BTreeSet<NodeIncarnation>) -> [u8; 32] {
    let mut digest = blake3::Hasher::new();
    digest.update(b"lattice-transfer-participants");
    for participant in participants {
        digest.update(&participant.get().to_be_bytes());
    }
    *digest.finalize().as_bytes()
}

/// Internal control-plane storage. Remote acknowledgements must first validate
/// their authenticated session and the exact barrier version.
#[async_trait]
pub trait TransferBarrierStore: CoordinatorLeaseStore {
    /// Exact durable endpoints, including the source after ownership changed.
    async fn transfer_parties(
        &self,
        slot: &PlacementSlotKey,
        operation: u128,
    ) -> Result<(NodeKey, NodeKey), StorageError>;

    async fn prepare_transfer_barrier(
        &self,
        guard: &GroupLeaderGuard,
        slot: &PlacementSlotKey,
        operation: u128,
        version: &PlacementVersion,
        participants: BTreeSet<NodeIncarnation>,
    ) -> Result<BarrierSnapshot, StorageError>;

    async fn transfer_barrier(
        &self,
        slot: &PlacementSlotKey,
        operation: u128,
    ) -> Result<BarrierSnapshot, StorageError>;

    async fn reclaim_transfer_barrier(
        &self,
        guard: &GroupLeaderGuard,
        slot: &PlacementSlotKey,
        operation: u128,
    ) -> Result<(), StorageError>;

    /// Reclaims at most one small batch, preserving a restartable cursor.
    async fn reclaim_orphan_barriers(
        &self,
        guard: &GroupLeaderGuard,
        cursor: Option<&str>,
    ) -> Result<Option<String>, StorageError>;

    async fn advance_transfer_barrier(
        &self,
        guard: &GroupLeaderGuard,
        slot: &PlacementSlotKey,
        operation: u128,
        participant: NodeIncarnation,
        progress: BarrierProgress,
    ) -> Result<(), StorageError>;
}

#[async_trait]
impl TransferBarrierStore for InMemoryCoordinationStore {
    async fn transfer_parties(
        &self,
        slot: &PlacementSlotKey,
        operation: u128,
    ) -> Result<(NodeKey, NodeKey), StorageError> {
        let state = self.inner.lock().expect("coordination store poisoned");
        let reservation = state
            .transfer_reservations
            .get(slot)
            .filter(|reservation| reservation.operation == operation)
            .ok_or(StorageError::StorageMetadataMismatch)?;
        Ok((reservation.source.clone(), reservation.target.clone()))
    }

    async fn prepare_transfer_barrier(
        &self,
        guard: &GroupLeaderGuard,
        slot: &PlacementSlotKey,
        operation: u128,
        version: &PlacementVersion,
        participants: BTreeSet<NodeIncarnation>,
    ) -> Result<BarrierSnapshot, StorageError> {
        if slot.group() != guard.group()
            || &version.group != guard.group()
            || version.term != guard.term()
            || participants.len() > self.maximum_members
        {
            return Err(StorageError::InvalidRecord);
        }
        let mut state = self.inner.lock().expect("coordination store poisoned");
        validate_guard(&state, guard)?;
        let snapshot = BarrierSnapshot {
            manifest: TransferBarrier {
                slot: slot.clone(),
                operation,
                version: version.clone(),
                source_revision: i64::try_from(version.revision.get())
                    .map_err(|_| StorageError::CounterExhausted)?,
                count: participants.len(),
                digest: participant_digest(&participants),
                sealed: true,
            },
            participants: participants
                .into_iter()
                .map(|p| (p, BarrierProgress::Pending))
                .collect(),
        };
        let key = (slot.clone(), operation);
        if let Some(previous) = state.transfer_barriers.get(&key) {
            if previous.manifest != snapshot.manifest {
                return Err(StorageError::CompareFailed);
            }
            previous.verify()?;
            return Ok(previous.clone());
        }
        state.transfer_barriers.insert(key, snapshot.clone());
        Ok(snapshot)
    }

    async fn transfer_barrier(
        &self,
        slot: &PlacementSlotKey,
        operation: u128,
    ) -> Result<BarrierSnapshot, StorageError> {
        let state = self.inner.lock().expect("coordination store poisoned");
        let snapshot = state
            .transfer_barriers
            .get(&(slot.clone(), operation))
            .ok_or(StorageError::StorageMetadataMismatch)?
            .clone();
        snapshot.verify()?;
        Ok(snapshot)
    }

    async fn reclaim_transfer_barrier(
        &self,
        guard: &GroupLeaderGuard,
        slot: &PlacementSlotKey,
        operation: u128,
    ) -> Result<(), StorageError> {
        let mut state = self.inner.lock().expect("coordination store poisoned");
        validate_guard(&state, guard)?;
        if !state
            .transfer_barriers
            .contains_key(&(slot.clone(), operation))
        {
            return Ok(());
        }
        if slot.group() != guard.group()
            || state
                .slots
                .get(slot)
                .is_none_or(|record| record.active_move == Some(operation))
        {
            return Err(StorageError::CompareFailed);
        }
        state.transfer_barriers.remove(&(slot.clone(), operation));
        Ok(())
    }

    async fn advance_transfer_barrier(
        &self,
        guard: &GroupLeaderGuard,
        slot: &PlacementSlotKey,
        operation: u128,
        participant: NodeIncarnation,
        progress: BarrierProgress,
    ) -> Result<(), StorageError> {
        let mut state = self.inner.lock().expect("coordination store poisoned");
        validate_guard(&state, guard)?;
        if slot.group() != guard.group() || progress == BarrierProgress::Pending {
            return Err(StorageError::InvalidRecord);
        }
        let snapshot = state
            .transfer_barriers
            .get_mut(&(slot.clone(), operation))
            .ok_or(StorageError::StorageMetadataMismatch)?;
        snapshot.verify()?;
        let current = snapshot
            .participants
            .get_mut(&participant)
            .ok_or(StorageError::StorageMetadataMismatch)?;
        if *current == progress {
            return Ok(());
        }
        if *current != BarrierProgress::Pending {
            return Ok(());
        }
        *current = progress;
        Ok(())
    }
    async fn reclaim_orphan_barriers(
        &self,
        guard: &GroupLeaderGuard,
        cursor: Option<&str>,
    ) -> Result<Option<String>, StorageError> {
        let mut state = self.inner.lock().expect("coordination store poisoned");
        validate_guard(&state, guard)?;
        let next = state
            .transfer_barriers
            .keys()
            .filter(|(slot, _)| slot.group() == guard.group())
            .map(|key| (format!("{:?}/{:032x}", key.0, key.1), key.clone()))
            .filter(|(key, _)| cursor.is_none_or(|cursor| key.as_str() >= cursor))
            .min_by(|a, b| a.0.cmp(&b.0));
        let Some((cursor, key)) = next else {
            return Ok(None);
        };
        if state
            .slots
            .get(&key.0)
            .is_none_or(|slot| slot.active_move != Some(key.1))
        {
            state.transfer_barriers.remove(&key);
        }
        Ok(Some(format!("{cursor}\0")))
    }
}
