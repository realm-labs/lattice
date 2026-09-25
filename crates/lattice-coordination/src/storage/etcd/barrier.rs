use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use etcd_client::{Compare, CompareOp, GetOptions, SortOrder, SortTarget, TxnOp};
use lattice_model::cluster::NodeIncarnation;

use crate::{
    coordinator::{GroupLeaderGuard, GroupMemberRecord},
    storage::{
        StorageError,
        barrier::{
            BarrierProgress, BarrierSnapshot, TransferBarrier, TransferBarrierStore,
            participant_digest,
        },
        transfer_capacity::TransferReservation,
    },
    types::{NodeKey, PlacementSlot, PlacementSlotKey, PlacementVersion},
};

use super::{EtcdCoordinationStore, decode, encode, prefix_range_end, transactions::commit};

const BATCH_SIZE: usize = 16;

impl EtcdCoordinationStore {
    pub(super) async fn hydrate_slot_barrier(
        &self,
        mut slot: PlacementSlot,
    ) -> Result<PlacementSlot, StorageError> {
        if let Some(operation) = slot.active_move {
            slot.barrier_sessions = self
                .transfer_barrier(&slot.key, operation)
                .await?
                .required();
        }
        Ok(slot)
    }

    pub(super) fn barrier_prefix(
        &self,
        slot: &PlacementSlotKey,
        operation: u128,
    ) -> Result<String, StorageError> {
        Ok(self.key(&format!(
            "groups/{}/transfers/barriers/{}/{operation:032x}/",
            slot.group().as_str(),
            blake3::hash(&encode(slot)?).to_hex(),
        )))
    }

    async fn discard_unpublished_barrier(
        &self,
        guard: &GroupLeaderGuard,
        manifest: &TransferBarrier,
    ) -> Result<(), StorageError> {
        let prefix = self.barrier_prefix(&manifest.slot, manifest.operation)?;
        let slot_key = self.slot_key(&manifest.slot);
        let (slot_value, slot_revision, _) = self
            .read_raw(&slot_key)
            .await?
            .ok_or(StorageError::CompareFailed)?;
        let slot: PlacementSlot = decode(&slot_value)?;
        if slot.active_move.is_some() {
            return Err(StorageError::CompareFailed);
        }
        loop {
            let mut client = self.client.clone();
            let page = self
                .read_deadline(
                    client.get(
                        format!("{prefix}participants/"),
                        Some(
                            GetOptions::new()
                                .with_prefix()
                                .with_limit(BATCH_SIZE as i64),
                        ),
                    ),
                )
                .await?;
            let mut compares = vec![
                Compare::value(
                    format!("{prefix}manifest"),
                    CompareOp::Equal,
                    encode(manifest)?,
                ),
                Compare::mod_revision(slot_key.clone(), CompareOp::Equal, slot_revision),
            ];
            let mut operations = Vec::new();
            for kv in page.kvs() {
                compares.push(Compare::mod_revision(
                    kv.key(),
                    CompareOp::Equal,
                    kv.mod_revision(),
                ));
                operations.push(TxnOp::delete(kv.key(), None));
            }
            if operations.is_empty() {
                operations.push(TxnOp::delete(format!("{prefix}manifest"), None));
                commit(self, guard, compares, operations).await?;
                return Ok(());
            }
            commit(self, guard, compares, operations).await?;
        }
    }

    async fn snapshot_participants(
        &self,
        slot: &PlacementSlotKey,
        revision: i64,
    ) -> Result<BTreeSet<NodeIncarnation>, StorageError> {
        let prefix = self.key(&format!("groups/{}/members/", slot.group().as_str()));
        let mut start = prefix.into_bytes();
        let end = prefix_range_end(start.clone())?;
        let mut participants = BTreeSet::new();
        let mut read_count = 0;
        loop {
            let mut client = self.client.clone();
            let page = self
                .read_deadline(
                    client.get(
                        start,
                        Some(
                            GetOptions::new()
                                .with_range(end.clone())
                                .with_revision(revision)
                                .with_limit(BATCH_SIZE as i64)
                                .with_sort(SortTarget::Key, SortOrder::Ascend),
                        ),
                    ),
                )
                .await?;
            for kv in page.kvs() {
                read_count += 1;
                if read_count > self.limits.maximum_members {
                    return Err(StorageError::Capacity);
                }
                let member: GroupMemberRecord = decode(kv.value())?;
                participants.insert(member.node.incarnation);
            }
            if !page.more() {
                return Ok(participants);
            }
            start = page.kvs().last().ok_or(StorageError::Codec)?.key().to_vec();
            start.push(0);
        }
    }

    async fn read_barrier_entries(
        &self,
        prefix: &str,
        revision: i64,
    ) -> Result<BTreeMap<NodeIncarnation, BarrierProgress>, StorageError> {
        let mut result = BTreeMap::new();
        let mut start = prefix.as_bytes().to_vec();
        let end = prefix_range_end(start.clone())?;
        loop {
            let mut client = self.client.clone();
            let response = self
                .read_deadline(
                    client.get(
                        start,
                        Some(
                            GetOptions::new()
                                .with_range(end.clone())
                                .with_revision(revision)
                                .with_limit(BATCH_SIZE as i64)
                                .with_sort(SortTarget::Key, SortOrder::Ascend),
                        ),
                    ),
                )
                .await?;
            for kv in response.kvs() {
                let (identity, progress): (NodeIncarnation, BarrierProgress) = decode(kv.value())?;
                let expected_key = format!("{prefix}{:032x}", identity.get());
                if kv.key() != expected_key.as_bytes()
                    || result.insert(identity, progress).is_some()
                {
                    return Err(StorageError::StorageMetadataMismatch);
                }
            }
            if result.len() > self.limits.maximum_members {
                return Err(StorageError::Capacity);
            }
            if !response.more() {
                break;
            }
            start = response
                .kvs()
                .last()
                .ok_or(StorageError::Codec)?
                .key()
                .to_vec();
            start.push(0);
        }
        Ok(result)
    }
}

#[async_trait]
impl TransferBarrierStore for EtcdCoordinationStore {
    async fn transfer_parties(
        &self,
        slot: &PlacementSlotKey,
        operation: u128,
    ) -> Result<(NodeKey, NodeKey), StorageError> {
        let reservation: TransferReservation = self
            .get_json_key(&self.reservation_key(slot)?)
            .await?
            .ok_or(StorageError::StorageMetadataMismatch)?;
        if reservation.slot != *slot || reservation.operation != operation {
            return Err(StorageError::StorageMetadataMismatch);
        }
        Ok((reservation.source, reservation.target))
    }

    async fn prepare_transfer_barrier(
        &self,
        guard: &GroupLeaderGuard,
        slot: &PlacementSlotKey,
        operation: u128,
        version: &PlacementVersion,
        participants: BTreeSet<NodeIncarnation>,
    ) -> Result<BarrierSnapshot, StorageError> {
        self.bound_epoch()?;
        if slot.group() != guard.group()
            || &version.group != guard.group()
            || version.term != guard.term()
            || participants.len() > self.limits.maximum_members
        {
            return Err(StorageError::InvalidRecord);
        }
        let prefix = self.barrier_prefix(slot, operation)?;
        let header_key = format!("{prefix}manifest");
        let mut client = self.client.clone();
        let initial = self
            .read_deadline(client.get(header_key.clone(), None))
            .await?;
        let source_revision = initial.header().ok_or(StorageError::Codec)?.revision();
        let mut manifest = TransferBarrier {
            slot: slot.clone(),
            operation,
            version: version.clone(),
            source_revision,
            count: participants.len(),
            digest: participant_digest(&participants),
            sealed: false,
        };
        if let Some(kv) = initial.kvs().first() {
            let previous: TransferBarrier = decode(kv.value())?;
            manifest.source_revision = previous.source_revision;
            manifest.sealed = previous.sealed;
            if manifest != previous {
                self.discard_unpublished_barrier(guard, &previous).await?;
                return Box::pin(self.prepare_transfer_barrier(
                    guard,
                    slot,
                    operation,
                    version,
                    participants,
                ))
                .await;
            }
            if previous.sealed {
                return self.transfer_barrier(slot, operation).await;
            }
        } else {
            commit(
                self,
                guard,
                vec![Compare::version(header_key.clone(), CompareOp::Equal, 0)],
                vec![TxnOp::put(header_key.clone(), encode(&manifest)?, None)],
            )
            .await?;
        }
        // Capture every page at the manifest's fixed source revision. A mismatch
        // means the caller's session view is incomplete; never seal its subset.
        // A compacted source fails closed before any invalidation is published.
        match self
            .snapshot_participants(slot, manifest.source_revision)
            .await
        {
            Ok(snapshot) if snapshot == participants => {}
            Ok(_) => {
                self.discard_unpublished_barrier(guard, &manifest).await?;
                return Err(StorageError::CompareFailed);
            }
            Err(StorageError::SnapshotCompacted) => {
                self.discard_unpublished_barrier(guard, &manifest).await?;
                return Err(StorageError::SnapshotCompacted);
            }
            Err(error) => return Err(error),
        }
        // Batches are individually idempotent, guarded by the immutable Building
        // header. Neither slot publication nor invalidation is allowed yet.
        let identities = participants.iter().copied().collect::<Vec<_>>();
        for batch in identities.chunks(BATCH_SIZE) {
            let mut comparisons = vec![Compare::value(
                header_key.clone(),
                CompareOp::Equal,
                encode(&manifest)?,
            )];
            let mut operations = Vec::new();
            for identity in batch {
                let key = format!("{prefix}participants/{:032x}", identity.get());
                let value = encode(&(*identity, BarrierProgress::Pending))?;
                if let Some((existing, _, _)) = self.read_raw(&key).await? {
                    if existing != value {
                        return Err(StorageError::StorageMetadataMismatch);
                    }
                    continue;
                }
                comparisons.push(Compare::version(key.clone(), CompareOp::Equal, 0));
                operations.push(TxnOp::put(key, value, None));
            }
            commit(self, guard, comparisons, operations).await?;
        }
        let mut client = self.client.clone();
        let verify = self
            .read_deadline(client.get(header_key.clone(), None))
            .await?;
        let revision = verify.header().ok_or(StorageError::Codec)?.revision();
        let entries = self
            .read_barrier_entries(&format!("{prefix}participants/"), revision)
            .await?;
        let mut sealed = manifest.clone();
        sealed.sealed = true;
        let snapshot = BarrierSnapshot {
            manifest: sealed.clone(),
            participants: entries,
        };
        snapshot.verify()?;
        commit(
            self,
            guard,
            vec![Compare::value(
                header_key.clone(),
                CompareOp::Equal,
                encode(&manifest)?,
            )],
            vec![TxnOp::put(header_key, encode(&sealed)?, None)],
        )
        .await?;
        Ok(snapshot)
    }

    async fn transfer_barrier(
        &self,
        slot: &PlacementSlotKey,
        operation: u128,
    ) -> Result<BarrierSnapshot, StorageError> {
        self.bound_epoch()?;
        let prefix = self.barrier_prefix(slot, operation)?;
        let mut client = self.client.clone();
        let response = self
            .read_deadline(client.get(format!("{prefix}manifest"), None))
            .await?;
        let revision = response.header().ok_or(StorageError::Codec)?.revision();
        let kv = response
            .kvs()
            .first()
            .ok_or(StorageError::StorageMetadataMismatch)?;
        let manifest: TransferBarrier = decode(kv.value())?;
        if &manifest.slot != slot || manifest.operation != operation {
            return Err(StorageError::StorageMetadataMismatch);
        }
        let participants = self
            .read_barrier_entries(&format!("{prefix}participants/"), revision)
            .await?;
        let snapshot = BarrierSnapshot {
            manifest,
            participants,
        };
        snapshot.verify()?;
        Ok(snapshot)
    }

    async fn reclaim_transfer_barrier(
        &self,
        guard: &GroupLeaderGuard,
        slot: &PlacementSlotKey,
        operation: u128,
    ) -> Result<(), StorageError> {
        if slot.group() != guard.group() {
            return Err(StorageError::InvalidRecord);
        }
        let prefix = self.barrier_prefix(slot, operation)?;
        if self.read_raw(&format!("{prefix}manifest")).await?.is_none() {
            return Ok(());
        }
        let slot_key = self.slot_key(slot);
        let (value, slot_revision, _) = self
            .read_raw(&slot_key)
            .await?
            .ok_or(StorageError::CompareFailed)?;
        let current: PlacementSlot = decode(&value)?;
        if current.active_move == Some(operation) {
            return Err(StorageError::CompareFailed);
        }
        loop {
            let mut client = self.client.clone();
            let response = self
                .read_deadline(
                    client.get(
                        format!("{prefix}participants/"),
                        Some(
                            GetOptions::new()
                                .with_prefix()
                                .with_limit(BATCH_SIZE as i64),
                        ),
                    ),
                )
                .await?;
            if response.kvs().is_empty() {
                commit(
                    self,
                    guard,
                    vec![Compare::mod_revision(
                        slot_key.clone(),
                        CompareOp::Equal,
                        slot_revision,
                    )],
                    vec![TxnOp::delete(format!("{prefix}manifest"), None)],
                )
                .await?;
                return Ok(());
            }
            let mut compares = vec![Compare::mod_revision(
                slot_key.clone(),
                CompareOp::Equal,
                slot_revision,
            )];
            let mut operations = Vec::new();
            for kv in response.kvs() {
                compares.push(Compare::mod_revision(
                    kv.key(),
                    CompareOp::Equal,
                    kv.mod_revision(),
                ));
                operations.push(TxnOp::delete(kv.key(), None));
            }
            commit(self, guard, compares, operations).await?;
        }
    }

    async fn reclaim_orphan_barriers(
        &self,
        guard: &GroupLeaderGuard,
        cursor: Option<&str>,
    ) -> Result<Option<String>, StorageError> {
        self.reclaim_orphan_barrier_batch(guard, cursor).await
    }

    async fn advance_transfer_barrier(
        &self,
        guard: &GroupLeaderGuard,
        slot: &PlacementSlotKey,
        operation: u128,
        participant: NodeIncarnation,
        progress: BarrierProgress,
    ) -> Result<(), StorageError> {
        if slot.group() != guard.group() || progress == BarrierProgress::Pending {
            return Err(StorageError::InvalidRecord);
        }
        let prefix = self.barrier_prefix(slot, operation)?;
        let header_key = format!("{prefix}manifest");
        let (header, _, _) = self
            .read_raw(&header_key)
            .await?
            .ok_or(StorageError::StorageMetadataMismatch)?;
        let manifest: TransferBarrier = decode(&header)?;
        if !manifest.sealed || &manifest.slot != slot || manifest.operation != operation {
            return Err(StorageError::StorageMetadataMismatch);
        }
        let key = format!("{prefix}participants/{:032x}", participant.get());
        let (value, revision, _) = self
            .read_raw(&key)
            .await?
            .ok_or(StorageError::StorageMetadataMismatch)?;
        let (identity, current): (NodeIncarnation, BarrierProgress) = decode(&value)?;
        if identity != participant {
            return Err(StorageError::StorageMetadataMismatch);
        }
        let progress = if current == BarrierProgress::Pending {
            progress
        } else {
            current
        };
        // Even duplicate acknowledgements recheck run/leader authorization.
        commit(
            self,
            guard,
            vec![
                Compare::value(header_key, CompareOp::Equal, header),
                Compare::mod_revision(key.clone(), CompareOp::Equal, revision),
            ],
            vec![TxnOp::put(key, encode(&(identity, progress))?, None)],
        )
        .await
    }
}
