use etcd_client::{GetOptions, SortOrder, SortTarget, Txn, TxnOp, TxnOpResponse};
use lattice_model::cluster::ActorGroupId;

use super::{EtcdCoordinationStore, MAX_STORE_PAGE_RECORDS, decode, prefix_range_end};
use crate::{
    coordinator::MemberRecord,
    plan::RebalancePlan,
    storage::{
        StorageError,
        page::{PageCursor, StorePage},
        records::LeasedClaim,
    },
    types::{PlacementSlot, PlacementSlotState},
};

struct RawPage {
    revision: i64,
    records: Vec<(Vec<u8>, Vec<u8>, i64)>,
    next_cursor: Option<PageCursor>,
    remaining: usize,
}

impl EtcdCoordinationStore {
    /// Reads one page across `suffixes` with a single request. Each suffix contributes one range
    /// that starts at the cursor when the cursor falls inside it, so the suffixes must already be
    /// in ascending key order and the concatenated ranges are the scan order the cursor resumes.
    ///
    /// etcd reports the size of the whole range in `count` while it only reads `limit` values out
    /// of the backend, so `remaining` costs an index walk instead of a second range request.
    async fn list_page_raw(
        &self,
        suffixes: &[String],
        cursor: Option<&PageCursor>,
        limit: usize,
    ) -> Result<RawPage, StorageError> {
        if limit == 0 {
            return Err(StorageError::BackendArgument);
        }
        let page_limit = i64::try_from(limit.min(MAX_STORE_PAGE_RECORDS))
            .map_err(|_| StorageError::BackendArgument)?;
        let position = cursor.map(PageCursor::as_bytes);
        let mut ranges = Vec::new();
        for suffix in suffixes {
            let prefix = self.key(suffix).into_bytes();
            let end = prefix_range_end(prefix.clone())?;
            let start = match position {
                Some(position) if position >= end.as_slice() => continue,
                Some(position) if position > prefix.as_slice() => position.to_vec(),
                _ => prefix,
            };
            ranges.push(TxnOp::get(
                start,
                Some(
                    GetOptions::new()
                        .with_range(end)
                        .with_limit(page_limit)
                        .with_sort(SortTarget::Key, SortOrder::Ascend),
                ),
            ));
        }
        if ranges.is_empty() {
            return Ok(RawPage {
                revision: 0,
                records: Vec::new(),
                next_cursor: None,
                remaining: 0,
            });
        }
        let mut client = self.client.clone();
        let response = self
            .read_deadline(client.txn(Txn::new().and_then(ranges)))
            .await?;
        let mut records = Vec::new();
        let mut total = 0_usize;
        for operation in response.op_responses() {
            let TxnOpResponse::Get(range) = operation else {
                return Err(StorageError::Codec);
            };
            total = total
                .saturating_add(usize::try_from(range.count()).map_err(|_| StorageError::Codec)?);
            records.extend(range.kvs().iter().map(|record| {
                (
                    record.key().to_vec(),
                    record.value().to_vec(),
                    record.lease(),
                )
            }));
        }
        records.truncate(limit);
        let remaining = total.saturating_sub(records.len());
        if remaining > 0 && records.is_empty() {
            return Err(StorageError::Codec);
        }
        let next_cursor = (remaining > 0)
            .then(|| {
                records.last().map(|(key, _, _)| {
                    let mut position = key.clone();
                    position.push(0);
                    PageCursor::new(position)
                })
            })
            .flatten();
        Ok(RawPage {
            revision: response.header().ok_or(StorageError::Codec)?.revision(),
            records,
            next_cursor,
            remaining,
        })
    }

    pub(super) async fn list_slots_page_inner(
        &self,
        group: &ActorGroupId,
        states: &[PlacementSlotState],
        cursor: Option<&PageCursor>,
        limit: usize,
    ) -> Result<StorePage<PlacementSlot>, StorageError> {
        let page = self
            .list_page_raw(
                &[
                    format!("groups/{}/shards/", group.as_str()),
                    format!("groups/{}/singletons/", group.as_str()),
                ],
                cursor,
                limit,
            )
            .await?;
        let mut records = Vec::new();
        for (_, value, _) in &page.records {
            let slot: PlacementSlot = decode(value)?;
            if states.is_empty() || states.contains(&slot.state) {
                records.push(self.hydrate_slot_barrier(slot).await?);
            }
        }
        Ok(StorePage {
            records,
            next_cursor: page.next_cursor,
            remaining: page.remaining,
        })
    }

    pub(super) async fn list_plans_page_inner(
        &self,
        group: &ActorGroupId,
        cursor: Option<&PageCursor>,
        limit: usize,
    ) -> Result<StorePage<RebalancePlan>, StorageError> {
        let page = self
            .list_page_raw(
                &[format!("groups/{}/rebalances/", group.as_str())],
                cursor,
                limit,
            )
            .await?;
        let mut records = Vec::with_capacity(page.records.len());
        for (_, value, _) in &page.records {
            records.push(self.hydrate_plan(value, page.revision).await?);
        }
        Ok(StorePage {
            records,
            next_cursor: page.next_cursor,
            remaining: page.remaining,
        })
    }

    pub(super) async fn list_claims_page_inner(
        &self,
        group: &ActorGroupId,
        cursor: Option<&PageCursor>,
        limit: usize,
    ) -> Result<StorePage<LeasedClaim>, StorageError> {
        let page = self
            .list_page_raw(
                &[
                    format!("groups/{}/shard_claims/", group.as_str()),
                    format!("groups/{}/singleton_claims/", group.as_str()),
                ],
                cursor,
                limit,
            )
            .await?;
        Ok(StorePage {
            records: page
                .records
                .iter()
                .map(|(_, value, lease_id)| {
                    Ok(LeasedClaim {
                        grant: decode(value)?,
                        lease_id: *lease_id,
                    })
                })
                .collect::<Result<Vec<_>, StorageError>>()?,
            next_cursor: page.next_cursor,
            remaining: page.remaining,
        })
    }

    pub(super) async fn list_members_page_inner(
        &self,
        cursor: Option<&PageCursor>,
        limit: usize,
    ) -> Result<StorePage<MemberRecord>, StorageError> {
        let page = self
            .list_page_raw(&["membership/members/".to_owned()], cursor, limit)
            .await?;
        Ok(StorePage {
            records: page
                .records
                .iter()
                .map(|(_, value, _)| decode(value))
                .collect::<Result<Vec<_>, _>>()?,
            next_cursor: page.next_cursor,
            remaining: page.remaining,
        })
    }
}
