//! Restartable garbage collection of unpublished and completed barriers.
//! A manifest remains until its last participant row is deleted.

use super::{EtcdCoordinationStore, decode, prefix_range_end, transactions::commit};
use crate::{
    coordinator::GroupLeaderGuard,
    storage::{StorageError, barrier::TransferBarrier},
    types::PlacementSlot,
};
use etcd_client::{Compare, CompareOp, GetOptions, SortOrder, SortTarget, TxnOp};
use std::str::from_utf8;

const BATCH: i64 = 16;

impl EtcdCoordinationStore {
    pub(super) async fn reclaim_orphan_barrier_batch(
        &self,
        guard: &GroupLeaderGuard,
        cursor: Option<&str>,
    ) -> Result<Option<String>, StorageError> {
        let prefix = self.key(&format!(
            "groups/{}/transfers/barriers/",
            guard.group().as_str()
        ));
        let start = cursor.unwrap_or(&prefix);
        if !start.starts_with(&prefix) {
            return Err(StorageError::InvalidRecord);
        }
        let mut client = self.client.clone();
        let response = self
            .read_deadline(
                client.get(
                    start,
                    Some(
                        GetOptions::new()
                            .with_range(prefix_range_end(prefix.as_bytes().to_vec())?)
                            .with_sort(SortTarget::Key, SortOrder::Ascend)
                            .with_limit(BATCH),
                    ),
                ),
            )
            .await?;
        let Some(header) = response
            .kvs()
            .iter()
            .find(|kv| kv.key().ends_with(b"/manifest"))
        else {
            return if response.more() {
                let key = response.kvs().last().ok_or(StorageError::Codec)?.key();
                Ok(Some(format!(
                    "{}\0",
                    from_utf8(key).map_err(|_| StorageError::Codec)?
                )))
            } else {
                Ok(None)
            };
        };
        let manifest: TransferBarrier = decode(header.value())?;
        let barrier_prefix = self.barrier_prefix(&manifest.slot, manifest.operation)?;
        let header_key = format!("{barrier_prefix}manifest");
        if header.key() != header_key.as_bytes() || manifest.slot.group() != guard.group() {
            return Err(StorageError::StorageMetadataMismatch);
        }
        let slot_key = self.slot_key(&manifest.slot);
        let slot = self.read_raw(&slot_key).await?;
        let slot_compare = if let Some((value, revision, _)) = slot {
            let slot: PlacementSlot = decode(&value)?;
            if slot.active_move == Some(manifest.operation) {
                // Skip all participant rows of an active barrier.
                return Ok(Some(
                    String::from_utf8(prefix_range_end(barrier_prefix.into_bytes())?)
                        .map_err(|_| StorageError::Codec)?,
                ));
            }
            Compare::mod_revision(slot_key, CompareOp::Equal, revision)
        } else {
            Compare::version(slot_key, CompareOp::Equal, 0)
        };
        let mut client = self.client.clone();
        let page = self
            .read_deadline(client.get(
                format!("{barrier_prefix}participants/"),
                Some(GetOptions::new().with_prefix().with_limit(BATCH)),
            ))
            .await?;
        let mut compares = vec![
            slot_compare,
            Compare::mod_revision(header_key.clone(), CompareOp::Equal, header.mod_revision()),
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
        let finished = operations.is_empty();
        if finished {
            operations.push(TxnOp::delete(header_key.clone(), None));
        }
        commit(self, guard, compares, operations).await?;
        Ok(Some(if finished {
            format!("{header_key}\0")
        } else {
            header_key
        }))
    }
}
