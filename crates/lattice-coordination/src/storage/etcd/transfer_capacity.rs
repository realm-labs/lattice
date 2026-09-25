use etcd_client::{Compare, CompareOp, TxnOp};

use crate::{
    allocation::RebalanceLimits,
    coordinator::{GroupLeaderGuard, GroupMemberRecord, MemberRecord},
    storage::{
        StorageError, barrier::TransferBarrierStore, transfer_capacity::TransferReservation,
    },
    types::{PlacementSlot, PlacementSlotKey},
};

use super::{EtcdCoordinationStore, decode, encode, transactions::assignment_compares};

/// These mutations are merged into the same transaction as the slot and plan.
/// Never commit a reservation independently of the transfer it accounts for.
pub(super) struct CapacityMutation {
    pub compares: Vec<Compare>,
    pub operations: Vec<TxnOp>,
}

impl EtcdCoordinationStore {
    pub(super) fn reservation_key(&self, slot: &PlacementSlotKey) -> Result<String, StorageError> {
        let encoded = encode(slot)?;
        Ok(self.key(&format!(
            "groups/{}/transfers/reservations/{}",
            slot.group().as_str(),
            blake3::hash(&encoded).to_hex(),
        )))
    }

    pub(super) async fn reserve_transfer_capacity(
        &self,
        guard: &GroupLeaderGuard,
        slot: &PlacementSlot,
        limits: RebalanceLimits,
    ) -> Result<CapacityMutation, StorageError> {
        limits.validate().map_err(|_| StorageError::InvalidConfig)?;
        let reservation = TransferReservation::from_slot(slot)?;
        let global: MemberRecord = self
            .get_json_key(&self.key(&format!(
                "membership/members/{}",
                reservation.target.node_id
            )))
            .await?
            .ok_or(StorageError::CompareFailed)?;
        let member: GroupMemberRecord = self
            .get_json_key(&self.group_member_key(slot.key.group(), &reservation.target.node_id))
            .await?
            .ok_or(StorageError::CompareFailed)?;
        let assignment = assignment_compares(self, &global, &member, &reservation.target).await?;
        let key = self.reservation_key(&slot.key)?;
        let mut mutation = self.change_capacity(&reservation, limits, true).await?;
        let snapshot = self
            .prepare_transfer_barrier(
                guard,
                &slot.key,
                reservation.operation,
                &slot.version,
                slot.barrier_sessions.clone(),
            )
            .await?;
        mutation.compares.push(Compare::value(
            format!(
                "{}manifest",
                self.barrier_prefix(&slot.key, reservation.operation)?
            ),
            CompareOp::Equal,
            encode(&snapshot.manifest)?,
        ));
        mutation.compares.extend(assignment);
        mutation
            .compares
            .push(Compare::version(key.clone(), CompareOp::Equal, 0));
        mutation
            .operations
            .push(TxnOp::put(key, encode(&reservation)?, None));
        Ok(mutation)
    }

    pub(super) async fn release_transfer_capacity(
        &self,
        slot: &PlacementSlot,
    ) -> Result<CapacityMutation, StorageError> {
        let Some(operation) = slot.active_move else {
            return Ok(CapacityMutation {
                compares: Vec::new(),
                operations: Vec::new(),
            });
        };
        let key = self.reservation_key(&slot.key)?;
        let (value, revision, _) = self
            .read_raw(&key)
            .await?
            .ok_or(StorageError::StorageMetadataMismatch)?;
        let reservation: TransferReservation = decode(&value)?;
        if reservation.operation != operation
            || reservation.slot != slot.key
            || slot.owner.as_ref() != Some(&reservation.target)
        {
            return Err(StorageError::CompareFailed);
        }
        // Limits apply only to admission. Lowering them cannot strand existing
        // work or prevent release of a recovered reservation.
        let mut mutation = self
            .change_capacity(&reservation, RebalanceLimits::default(), false)
            .await?;
        mutation.compares.push(Compare::mod_revision(
            key.clone(),
            CompareOp::Equal,
            revision,
        ));
        mutation.operations.push(TxnOp::delete(key, None));
        Ok(mutation)
    }

    async fn change_capacity(
        &self,
        reservation: &TransferReservation,
        limits: RebalanceLimits,
        acquire: bool,
    ) -> Result<CapacityMutation, StorageError> {
        let mut mutation = CapacityMutation {
            compares: Vec::new(),
            operations: Vec::new(),
        };
        for (dimension, maximum) in reservation.dimensions(limits) {
            let key = self.key(&format!(
                "groups/{}/transfers/capacity/{dimension}",
                reservation.slot.group().as_str(),
            ));
            let current = self.read_raw(&key).await?;
            let count = current
                .as_ref()
                .map(|(bytes, _, _)| decode::<usize>(bytes))
                .transpose()?
                .unwrap_or(0);
            let next = if acquire {
                if count >= maximum {
                    return Err(StorageError::Capacity);
                }
                count.checked_add(1).ok_or(StorageError::CounterExhausted)?
            } else {
                count
                    .checked_sub(1)
                    .ok_or(StorageError::StorageMetadataMismatch)?
            };
            mutation.compares.push(match current {
                Some((_, revision, _)) => {
                    Compare::mod_revision(key.clone(), CompareOp::Equal, revision)
                }
                None => Compare::version(key.clone(), CompareOp::Equal, 0),
            });
            mutation.operations.push(if next == 0 {
                // Do not retain one permanent counter per departed incarnation.
                TxnOp::delete(key, None)
            } else {
                TxnOp::put(key, encode(&next)?, None)
            });
        }
        Ok(mutation)
    }
}
