use std::sync::Mutex;

/// Read amplification observed by [`super::InMemoryPlacementStore`].
///
/// The counts model what the etcd backend pays for. `slot_records` counts the slot values a call
/// decoded for its caller, which is the part a range request reads out of the backing store and
/// puts on the wire; the index walk a backend performs to report [`super::page::StorePage`]
/// `remaining` transfers no value and is deliberately not counted. The call counts separate an
/// unbounded prefix scan from a bounded page, so a hot path that fell back to a full scan is
/// visible as a count rather than only as a latency.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreReadCounts {
    pub list_slots: u64,
    pub list_slots_page: u64,
    pub list_plans: u64,
    pub list_plans_page: u64,
    pub list_claims: u64,
    pub list_claims_page: u64,
    pub list_members: u64,
    pub list_members_page: u64,
    pub list_domain_members: u64,
    pub get_slot: u64,
    pub get_claim: u64,
    pub slot_records: u64,
}

#[derive(Debug, Default)]
pub(super) struct StoreReadCounters(Mutex<StoreReadCounts>);

impl StoreReadCounters {
    pub(super) fn record(&self, observe: impl FnOnce(&mut StoreReadCounts)) {
        observe(&mut self.0.lock().expect("placement store counters poisoned"));
    }

    pub(super) fn snapshot(&self) -> StoreReadCounts {
        *self.0.lock().expect("placement store counters poisoned")
    }

    pub(super) fn reset(&self) {
        *self.0.lock().expect("placement store counters poisoned") = StoreReadCounts::default();
    }
}
