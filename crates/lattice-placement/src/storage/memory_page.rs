use std::ops::Bound;

use lattice_core::actor_ref::PlacementDomainId;
use serde::{Serialize, de::DeserializeOwned};

use super::{
    InMemoryPlacementStore, StorageError,
    domain::LeasedClaim,
    page::{PageCursor, StorePage},
};
use crate::{
    coordinator::MemberRecord,
    plan::RebalancePlan,
    types::{PlacementSlot, PlacementSlotKey, PlacementSlotState},
};

/// The scan order is the store's own map order, so a page resumes at the first key that sorts
/// strictly after the cursor. Records inserted or removed behind the cursor therefore change what
/// a later sweep sees without moving any record the current sweep has yet to visit.
struct Scan<T> {
    page: StorePage<T>,
    visited: usize,
}

fn paginate<'a, K: 'a + Serialize, V: 'a, T>(
    entries: impl Iterator<Item = (&'a K, &'a V)>,
    limit: usize,
    in_scope: impl Fn(&K, &V) -> bool,
    retained: impl Fn(&V) -> bool,
    project: impl Fn(&V) -> T,
) -> Result<Scan<T>, StorageError> {
    if limit == 0 {
        return Err(StorageError::BackendArgument);
    }
    let mut records = Vec::new();
    let mut visited = 0_usize;
    let mut remaining = 0_usize;
    let mut last = None;
    for (key, value) in entries {
        if !in_scope(key, value) {
            continue;
        }
        if visited == limit {
            remaining += 1;
            continue;
        }
        visited += 1;
        last = Some(key);
        if retained(value) {
            records.push(project(value));
        }
    }
    let next_cursor = match last {
        Some(key) if remaining > 0 => Some(encode_cursor(key)?),
        _ => None,
    };
    Ok(Scan {
        page: StorePage {
            records,
            next_cursor,
            remaining,
        },
        visited,
    })
}

fn encode_cursor<K: Serialize>(key: &K) -> Result<PageCursor, StorageError> {
    serde_json::to_vec(key)
        .map(PageCursor::new)
        .map_err(|_| StorageError::Codec)
}

fn position<K: DeserializeOwned>(cursor: Option<&PageCursor>) -> Result<Bound<K>, StorageError> {
    let Some(cursor) = cursor else {
        return Ok(Bound::Unbounded);
    };
    serde_json::from_slice(cursor.as_bytes())
        .map(Bound::Excluded)
        .map_err(|_| StorageError::BackendArgument)
}

impl InMemoryPlacementStore {
    pub(super) async fn list_slots_page_inner(
        &self,
        domain: &PlacementDomainId,
        states: &[PlacementSlotState],
        cursor: Option<&PageCursor>,
        limit: usize,
    ) -> Result<StorePage<PlacementSlot>, StorageError> {
        let start = position::<PlacementSlotKey>(cursor)?;
        let state = self.inner.lock().expect("placement memory store poisoned");
        let scan = paginate(
            state.slots.range((start, Bound::Unbounded)),
            limit,
            |key, _| key.domain() == domain,
            |slot: &PlacementSlot| states.is_empty() || states.contains(&slot.state),
            Clone::clone,
        )?;
        drop(state);
        self.counters.record(|counts| {
            counts.list_slots_page += 1;
            counts.slot_records += u64::try_from(scan.visited).unwrap_or(u64::MAX);
        });
        Ok(scan.page)
    }

    pub(super) async fn list_plans_page_inner(
        &self,
        domain: &PlacementDomainId,
        cursor: Option<&PageCursor>,
        limit: usize,
    ) -> Result<StorePage<RebalancePlan>, StorageError> {
        let start = position::<(PlacementDomainId, u128)>(cursor)?;
        let state = self.inner.lock().expect("placement memory store poisoned");
        let scan = paginate(
            state.plans.range((start, Bound::Unbounded)),
            limit,
            |(candidate, _), _| candidate == domain,
            |_| true,
            Clone::clone,
        )?;
        drop(state);
        self.counters.record(|counts| counts.list_plans_page += 1);
        Ok(scan.page)
    }

    pub(super) async fn list_claims_page_inner(
        &self,
        domain: &PlacementDomainId,
        cursor: Option<&PageCursor>,
        limit: usize,
    ) -> Result<StorePage<LeasedClaim>, StorageError> {
        let start = position::<PlacementSlotKey>(cursor)?;
        let state = self.inner.lock().expect("placement memory store poisoned");
        let scan = paginate(
            state.claims.range((start, Bound::Unbounded)),
            limit,
            |_, claim: &LeasedClaim| &claim.grant.domain == domain,
            |_| true,
            Clone::clone,
        )?;
        drop(state);
        self.counters.record(|counts| counts.list_claims_page += 1);
        Ok(scan.page)
    }

    pub(super) async fn list_members_page_inner(
        &self,
        cursor: Option<&PageCursor>,
        limit: usize,
    ) -> Result<StorePage<MemberRecord>, StorageError> {
        let start = position::<String>(cursor)?;
        let state = self.inner.lock().expect("placement memory store poisoned");
        let scan = paginate(
            state.members.range((start, Bound::Unbounded)),
            limit,
            |_, _| true,
            |_| true,
            Clone::clone,
        )?;
        drop(state);
        self.counters.record(|counts| counts.list_members_page += 1);
        Ok(scan.page)
    }
}
