use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use thiserror::Error;

use crate::coordinator::{LeaderGuard, LeaderRecord, MemberRecord};
use crate::plan::{MoveProgress, RebalancePlan};
use crate::types::{PlacementSlot, PlacementSlotKey, PlacementSlotState, Revision};

pub mod domain;
pub mod etcd;
mod memory_admin;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorePage<T> {
    pub records: Vec<T>,
    pub next_offset: Option<usize>,
    pub total: usize,
}

fn bounded_page<T: Clone>(
    records: &[T],
    offset: usize,
    limit: usize,
) -> Result<StorePage<T>, StorageError> {
    if limit == 0 || offset > records.len() {
        return Err(StorageError::BackendArgument);
    }
    let end = offset.saturating_add(limit).min(records.len());
    Ok(StorePage {
        records: records[offset..end].to_vec(),
        next_offset: (end < records.len()).then_some(end),
        total: records.len(),
    })
}

#[cfg(test)]
mod tests;

use domain::{
    ActivateAuthority, AdminOperationRecord, AdoptAuthority, AllocateInitial, AuthorityCommit,
    AutomaticBalanceSettings, ClaimPredicate, CommitAutomaticSettings, CompactAdminOperations,
    CompleteMove, CreateMember, CreatePlan, CreatePlanWithOperation, DeletePlan,
    DurableStorageLimits, FenceAuthority, FenceMissingAuthority, InstallAuthority, LeasedClaim,
    MemberCommit, MoveCommit, PlanCommit, RecordAdminOperation, RemoveMember,
    RemoveMemberWithOperation, ReserveHandoff, ReserveMove, SlotCommit, TransitionSlot,
    UpdateMember, UpdatePlan, UpdatePlanWithOperation,
};

/// Read-only placement access. Authoritative writes are available only through
/// the leader-guarded named commits on [`CoordinatorStore`].
#[async_trait]
pub trait PlacementStore: Send + Sync + 'static {
    fn durable_limits(&self) -> DurableStorageLimits;
    async fn get_state_revision(&self) -> Result<Revision, StorageError>;
    async fn get_slot(&self, key: &PlacementSlotKey)
    -> Result<Option<PlacementSlot>, StorageError>;
    async fn get_plan(&self, plan_id: u128) -> Result<Option<RebalancePlan>, StorageError>;
    async fn get_claim(&self, key: &PlacementSlotKey) -> Result<Option<LeasedClaim>, StorageError>;
    async fn get_member(&self, node_id: &str) -> Result<Option<MemberRecord>, StorageError>;
    async fn list_members(&self) -> Result<Vec<MemberRecord>, StorageError>;
    async fn list_slots(&self) -> Result<Vec<PlacementSlot>, StorageError>;
    async fn list_plans(&self) -> Result<Vec<RebalancePlan>, StorageError>;
    async fn list_claims(&self) -> Result<Vec<LeasedClaim>, StorageError>;
    async fn get_automatic_settings(
        &self,
    ) -> Result<Option<AutomaticBalanceSettings>, StorageError>;
    async fn get_admin_operation(
        &self,
        operation_id: &str,
    ) -> Result<Option<AdminOperationRecord>, StorageError>;
    async fn list_admin_operations(&self) -> Result<Vec<AdminOperationRecord>, StorageError>;
    async fn list_members_page(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<StorePage<MemberRecord>, StorageError> {
        bounded_page(&self.list_members().await?, offset, limit)
    }
    async fn list_slots_page(
        &self,
        states: &[PlacementSlotState],
        offset: usize,
        limit: usize,
    ) -> Result<StorePage<PlacementSlot>, StorageError> {
        let records = self
            .list_slots()
            .await?
            .into_iter()
            .filter(|slot| states.is_empty() || states.contains(&slot.state))
            .collect::<Vec<_>>();
        bounded_page(&records, offset, limit)
    }
    async fn list_plans_page(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<StorePage<RebalancePlan>, StorageError> {
        bounded_page(&self.list_plans().await?, offset, limit)
    }
    async fn list_claims_page(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<StorePage<LeasedClaim>, StorageError> {
        bounded_page(&self.list_claims().await?, offset, limit)
    }
}

#[async_trait]
pub trait CoordinatorStore: PlacementStore {
    async fn ensure_schema_generation(&self) -> Result<(), StorageError>;
    async fn grant_lease(&self, ttl: std::time::Duration) -> Result<i64, StorageError>;
    async fn keep_lease_alive(&self, lease_id: i64) -> Result<(), StorageError>;
    async fn revoke_lease(&self, lease_id: i64) -> Result<(), StorageError>;
    async fn lease_time_to_live(
        &self,
        lease_id: i64,
    ) -> Result<Option<std::time::Duration>, StorageError>;
    async fn campaign_leader(
        &self,
        leader: &LeaderRecord,
        lease_id: i64,
    ) -> Result<bool, StorageError>;
    async fn get_leader(&self) -> Result<Option<LeaderRecord>, StorageError>;

    async fn create_member(
        &self,
        guard: &LeaderGuard,
        request: CreateMember,
    ) -> Result<MemberCommit, StorageError>;
    async fn update_member(
        &self,
        guard: &LeaderGuard,
        request: UpdateMember,
    ) -> Result<MemberCommit, StorageError>;
    async fn remove_member(
        &self,
        guard: &LeaderGuard,
        request: RemoveMember,
    ) -> Result<MemberCommit, StorageError>;
    async fn create_plan(
        &self,
        guard: &LeaderGuard,
        request: CreatePlan,
    ) -> Result<PlanCommit, StorageError>;
    async fn update_plan(
        &self,
        guard: &LeaderGuard,
        request: UpdatePlan,
    ) -> Result<PlanCommit, StorageError>;
    async fn delete_plan(
        &self,
        guard: &LeaderGuard,
        request: DeletePlan,
    ) -> Result<PlanCommit, StorageError>;
    async fn transition_slot(
        &self,
        guard: &LeaderGuard,
        request: TransitionSlot,
    ) -> Result<SlotCommit, StorageError>;
    async fn allocate_initial(
        &self,
        guard: &LeaderGuard,
        request: AllocateInitial,
    ) -> Result<AuthorityCommit, StorageError>;
    async fn activate_authority(
        &self,
        guard: &LeaderGuard,
        request: ActivateAuthority,
    ) -> Result<SlotCommit, StorageError>;
    async fn reserve_move(
        &self,
        guard: &LeaderGuard,
        request: ReserveMove,
    ) -> Result<MoveCommit, StorageError>;
    async fn reserve_handoff(
        &self,
        guard: &LeaderGuard,
        request: ReserveHandoff,
    ) -> Result<SlotCommit, StorageError>;
    async fn fence_authority(
        &self,
        guard: &LeaderGuard,
        request: FenceAuthority,
    ) -> Result<SlotCommit, StorageError>;
    async fn fence_missing_authority(
        &self,
        guard: &LeaderGuard,
        request: FenceMissingAuthority,
    ) -> Result<SlotCommit, StorageError>;
    async fn install_authority(
        &self,
        guard: &LeaderGuard,
        request: InstallAuthority,
    ) -> Result<AuthorityCommit, StorageError>;
    async fn adopt_authority(
        &self,
        guard: &LeaderGuard,
        request: AdoptAuthority,
    ) -> Result<AuthorityCommit, StorageError>;
    async fn complete_move(
        &self,
        guard: &LeaderGuard,
        request: CompleteMove,
    ) -> Result<MoveCommit, StorageError>;
    async fn commit_automatic_settings(
        &self,
        guard: &LeaderGuard,
        request: CommitAutomaticSettings,
    ) -> Result<AutomaticBalanceSettings, StorageError>;
    async fn create_plan_with_operation(
        &self,
        guard: &LeaderGuard,
        request: CreatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError>;
    async fn update_plan_with_operation(
        &self,
        guard: &LeaderGuard,
        request: UpdatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError>;
    async fn remove_member_with_operation(
        &self,
        guard: &LeaderGuard,
        request: RemoveMemberWithOperation,
    ) -> Result<MemberCommit, StorageError>;
    async fn record_admin_operation(
        &self,
        guard: &LeaderGuard,
        request: RecordAdminOperation,
    ) -> Result<AdminOperationRecord, StorageError>;
    async fn compact_admin_operations(
        &self,
        guard: &LeaderGuard,
        request: CompactAdminOperations,
    ) -> Result<(), StorageError>;
}

#[derive(Debug, Clone)]
pub struct InMemoryPlacementStore {
    inner: Arc<Mutex<MemoryState>>,
    maximum_slots: usize,
    maximum_plans: usize,
    maximum_members: usize,
    maximum_admin_operations: usize,
}

#[derive(Debug, Default)]
struct MemoryState {
    slots: BTreeMap<PlacementSlotKey, PlacementSlot>,
    plans: BTreeMap<u128, RebalancePlan>,
    schema_generation: Option<u64>,
    state_revision: Option<Revision>,
    next_lease: i64,
    leases: BTreeMap<i64, LeaseState>,
    leader: Option<(i64, LeaderRecord)>,
    leader_term: u64,
    members: BTreeMap<String, MemberRecord>,
    claims: BTreeMap<PlacementSlotKey, LeasedClaim>,
    automatic_settings: Option<AutomaticBalanceSettings>,
    admin_operations: BTreeMap<String, AdminOperationRecord>,
}

#[derive(Debug, Clone, Copy)]
struct LeaseState {
    ttl: std::time::Duration,
}

impl InMemoryPlacementStore {
    pub fn new(maximum_slots: usize, maximum_plans: usize) -> Result<Self, StorageError> {
        if maximum_slots == 0 || maximum_plans == 0 {
            return Err(StorageError::ZeroLimit);
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(MemoryState::default())),
            maximum_slots,
            maximum_plans,
            maximum_members: maximum_slots,
            maximum_admin_operations: maximum_plans,
        })
    }

    #[cfg(test)]
    pub(crate) fn insert_generation_three_slot(&self, slot: PlacementSlot) {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        state.state_revision = Some(slot.version.revision);
        state.slots.insert(slot.key.clone(), slot);
    }

    async fn fence_missing_authority(
        &self,
        guard: &LeaderGuard,
        request: FenceMissingAuthority,
    ) -> Result<SlotCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_slot_common(guard, &state, Some(&request.expected_slot), &request.slot)?;
        if state.claims.contains_key(&request.slot.key)
            || !matches!(
                request.expected_slot.state,
                PlacementSlotState::Allocating | PlacementSlotState::Running
            )
            || request.slot.state != PlacementSlotState::Fenced
            || request.expected_slot.owner != request.slot.owner
            || request.expected_slot.assignment_generation != request.slot.assignment_generation
            || request.expected_slot.active_move != request.slot.active_move
        {
            return Err(StorageError::InvalidTransition);
        }
        state.state_revision = Some(request.slot.version.revision);
        state
            .slots
            .insert(request.slot.key.clone(), request.slot.clone());
        Ok(SlotCommit { slot: request.slot })
    }
}

fn initial_revision() -> Revision {
    Revision::new(1).expect("one is a valid state revision")
}

fn validate_guard(state: &MemoryState, guard: &LeaderGuard) -> Result<(), StorageError> {
    let Some((lease_id, leader)) = state.leader.as_ref() else {
        return Err(StorageError::LeadershipLost);
    };
    if leader != guard.record()
        || state.leader_term != guard.term().get()
        || !state.leases.contains_key(lease_id)
    {
        return Err(StorageError::LeadershipLost);
    }
    Ok(())
}

fn validate_member_record(member: &MemberRecord) -> Result<(), StorageError> {
    if member.node != member.hello.node || member.lease_id <= 0 || member.node.validate().is_err() {
        return Err(StorageError::InvalidRecord);
    }
    Ok(())
}

fn validate_admin_operation(
    guard: &LeaderGuard,
    operation: &AdminOperationRecord,
) -> Result<(), StorageError> {
    if operation.operation_id.is_empty()
        || operation.operation_id.len() > 256
        || operation.fingerprint.is_empty()
        || operation.fingerprint.len() > 1024
        || operation.version.term != guard.term()
        || operation.expires_unix_millis <= operation.created_unix_millis
    {
        return Err(StorageError::InvalidRecord);
    }
    Ok(())
}

fn validate_next_revision(state: &MemoryState, revision: Revision) -> Result<(), StorageError> {
    let expected = state
        .state_revision
        .unwrap_or_else(initial_revision)
        .next()
        .map_err(|_| StorageError::CounterExhausted)?;
    if revision != expected {
        return Err(StorageError::CompareFailed);
    }
    Ok(())
}

fn validate_plan_update(
    expected: &RebalancePlan,
    plan: &RebalancePlan,
) -> Result<(), StorageError> {
    if expected.plan_id != plan.plan_id
        || plan.record_revision
            != expected
                .record_revision
                .next()
                .map_err(|_| StorageError::CounterExhausted)?
    {
        return Err(StorageError::CompareFailed);
    }
    Ok(())
}

fn validate_claim_lease(state: &MemoryState, claim: &LeasedClaim) -> Result<(), StorageError> {
    if claim.lease_id <= 0
        || !state.leases.contains_key(&claim.lease_id)
        || claim.grant.ttl.is_zero()
    {
        return Err(StorageError::InvalidRecord);
    }
    Ok(())
}

fn claim_matches(state: &MemoryState, expected: &crate::types::ClaimGrant) -> bool {
    state
        .claims
        .get(&expected.slot)
        .is_some_and(|current| current.grant == *expected)
}

fn validate_slot_common(
    guard: &LeaderGuard,
    state: &MemoryState,
    expected: Option<&PlacementSlot>,
    slot: &PlacementSlot,
) -> Result<(), StorageError> {
    slot.validate().map_err(|_| StorageError::InvalidRecord)?;
    if slot.version.term != guard.term() {
        return Err(StorageError::InvalidRecord);
    }
    if let Some(expected) = expected
        && (expected.key != slot.key || state.slots.get(&slot.key) != Some(expected))
    {
        return Err(StorageError::CompareFailed);
    }
    validate_next_revision(state, slot.version.revision)
}

#[async_trait]
impl PlacementStore for InMemoryPlacementStore {
    fn durable_limits(&self) -> DurableStorageLimits {
        DurableStorageLimits {
            maximum_slots: self.maximum_slots,
            maximum_plans: self.maximum_plans,
            maximum_members: self.maximum_members,
            maximum_admin_operations: self.maximum_admin_operations,
            maximum_entity_configs: self.maximum_members,
            maximum_singleton_configs: self.maximum_members,
        }
    }

    async fn get_state_revision(&self) -> Result<Revision, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .state_revision
            .unwrap_or_else(initial_revision))
    }

    async fn get_slot(
        &self,
        key: &PlacementSlotKey,
    ) -> Result<Option<PlacementSlot>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .slots
            .get(key)
            .cloned())
    }

    async fn get_plan(&self, plan_id: u128) -> Result<Option<RebalancePlan>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .plans
            .get(&plan_id)
            .cloned())
    }

    async fn get_claim(&self, key: &PlacementSlotKey) -> Result<Option<LeasedClaim>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .claims
            .get(key)
            .cloned())
    }

    async fn get_member(&self, node_id: &str) -> Result<Option<MemberRecord>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .members
            .get(node_id)
            .cloned())
    }

    async fn list_members(&self) -> Result<Vec<MemberRecord>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .members
            .values()
            .cloned()
            .collect())
    }

    async fn list_slots(&self) -> Result<Vec<PlacementSlot>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .slots
            .values()
            .cloned()
            .collect())
    }

    async fn list_plans(&self) -> Result<Vec<RebalancePlan>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .plans
            .values()
            .cloned()
            .collect())
    }

    async fn list_claims(&self) -> Result<Vec<LeasedClaim>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .claims
            .values()
            .cloned()
            .collect())
    }

    async fn get_automatic_settings(
        &self,
    ) -> Result<Option<AutomaticBalanceSettings>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .automatic_settings
            .clone())
    }

    async fn get_admin_operation(
        &self,
        operation_id: &str,
    ) -> Result<Option<AdminOperationRecord>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .admin_operations
            .get(operation_id)
            .cloned())
    }

    async fn list_admin_operations(&self) -> Result<Vec<AdminOperationRecord>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .admin_operations
            .values()
            .cloned()
            .collect())
    }
}

#[async_trait]
impl CoordinatorStore for InMemoryPlacementStore {
    async fn ensure_schema_generation(&self) -> Result<(), StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        match state.schema_generation {
            Some(etcd::STORAGE_SCHEMA_GENERATION) => Ok(()),
            Some(_) => Err(StorageError::SchemaGenerationMismatch),
            None => {
                state.schema_generation = Some(etcd::STORAGE_SCHEMA_GENERATION);
                state.state_revision = Some(initial_revision());
                Ok(())
            }
        }
    }

    async fn grant_lease(&self, ttl: std::time::Duration) -> Result<i64, StorageError> {
        if ttl.is_zero() {
            return Err(StorageError::InvalidConfig);
        }
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        state.next_lease = state
            .next_lease
            .checked_add(1)
            .ok_or(StorageError::Capacity)?;
        let lease = state.next_lease;
        state.leases.insert(lease, LeaseState { ttl });
        Ok(lease)
    }

    async fn keep_lease_alive(&self, lease_id: i64) -> Result<(), StorageError> {
        let state = self.inner.lock().expect("placement memory store poisoned");
        state
            .leases
            .get(&lease_id)
            .ok_or(StorageError::Unavailable)
            .map(|_| ())
    }

    async fn revoke_lease(&self, lease_id: i64) -> Result<(), StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        state.leases.remove(&lease_id);
        if state
            .leader
            .as_ref()
            .is_some_and(|(lease, _)| *lease == lease_id)
        {
            state.leader = None;
        }
        state
            .members
            .retain(|_, member| member.lease_id != lease_id);
        state.claims.retain(|_, claim| claim.lease_id != lease_id);
        Ok(())
    }

    async fn lease_time_to_live(
        &self,
        lease_id: i64,
    ) -> Result<Option<std::time::Duration>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .leases
            .get(&lease_id)
            .map(|lease| lease.ttl))
    }

    async fn campaign_leader(
        &self,
        leader: &LeaderRecord,
        lease_id: i64,
    ) -> Result<bool, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        if !state.leases.contains_key(&lease_id) || state.leader.is_some() {
            return Ok(false);
        }
        let expected = state
            .leader_term
            .checked_add(1)
            .ok_or(StorageError::CounterExhausted)?;
        if leader.term.get() != expected {
            return Err(StorageError::CompareFailed);
        }
        state.leader_term = expected;
        state.leader = Some((lease_id, leader.clone()));
        Ok(true)
    }

    async fn get_leader(&self) -> Result<Option<LeaderRecord>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .leader
            .as_ref()
            .map(|(_, leader)| leader.clone()))
    }

    async fn create_member(
        &self,
        guard: &LeaderGuard,
        request: CreateMember,
    ) -> Result<MemberCommit, StorageError> {
        validate_member_record(&request.member)?;
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_next_revision(&state, request.member.version.revision)?;
        if !state.leases.contains_key(&request.member.lease_id) {
            return Err(StorageError::InvalidRecord);
        }
        if let Some(current) = state.members.get(&request.member.node.node_id) {
            return if current.node.incarnation == request.member.node.incarnation {
                Err(StorageError::CompareFailed)
            } else {
                Err(StorageError::IncarnationConflict)
            };
        }
        if state.members.len() == self.maximum_members {
            return Err(StorageError::Capacity);
        }
        state.state_revision = Some(request.member.version.revision);
        state
            .members
            .insert(request.member.node.node_id.clone(), request.member.clone());
        Ok(MemberCommit {
            revision: request.member.version.revision,
            member: request.member,
        })
    }

    async fn update_member(
        &self,
        guard: &LeaderGuard,
        request: UpdateMember,
    ) -> Result<MemberCommit, StorageError> {
        validate_member_record(&request.member)?;
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_next_revision(&state, request.member.version.revision)?;
        if request.expected.node.node_id != request.member.node.node_id
            || request.expected.node.incarnation != request.member.node.incarnation
            || state.members.get(&request.expected.node.node_id) != Some(&request.expected)
            || !state.leases.contains_key(&request.member.lease_id)
        {
            return Err(StorageError::CompareFailed);
        }
        state.state_revision = Some(request.member.version.revision);
        state
            .members
            .insert(request.member.node.node_id.clone(), request.member.clone());
        Ok(MemberCommit {
            revision: request.member.version.revision,
            member: request.member,
        })
    }

    async fn remove_member(
        &self,
        guard: &LeaderGuard,
        request: RemoveMember,
    ) -> Result<MemberCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        if state.members.get(&request.expected.node.node_id) != Some(&request.expected) {
            return Err(StorageError::CompareFailed);
        }
        let next = state
            .state_revision
            .unwrap_or_else(initial_revision)
            .next()
            .map_err(|_| StorageError::CounterExhausted)?;
        state.state_revision = Some(next);
        state.members.remove(&request.expected.node.node_id);
        Ok(MemberCommit {
            revision: next,
            member: request.expected,
        })
    }

    async fn create_plan(
        &self,
        guard: &LeaderGuard,
        request: CreatePlan,
    ) -> Result<PlanCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        if request.plan.coordinator_term != guard.term()
            || request.plan.record_revision.get() != 1
            || state.plans.contains_key(&request.plan.plan_id)
        {
            return Err(StorageError::CompareFailed);
        }
        if state.plans.len() == self.maximum_plans {
            return Err(StorageError::Capacity);
        }
        state
            .plans
            .insert(request.plan.plan_id, request.plan.clone());
        Ok(PlanCommit { plan: request.plan })
    }

    async fn update_plan(
        &self,
        guard: &LeaderGuard,
        request: UpdatePlan,
    ) -> Result<PlanCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_plan_update(&request.expected, &request.plan)?;
        if state.plans.get(&request.expected.plan_id) != Some(&request.expected) {
            return Err(StorageError::CompareFailed);
        }
        state
            .plans
            .insert(request.plan.plan_id, request.plan.clone());
        Ok(PlanCommit { plan: request.plan })
    }

    async fn delete_plan(
        &self,
        guard: &LeaderGuard,
        request: DeletePlan,
    ) -> Result<PlanCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        if state.plans.get(&request.expected.plan_id) != Some(&request.expected) {
            return Err(StorageError::CompareFailed);
        }
        state.plans.remove(&request.expected.plan_id);
        Ok(PlanCommit {
            plan: request.expected,
        })
    }

    async fn transition_slot(
        &self,
        guard: &LeaderGuard,
        request: TransitionSlot,
    ) -> Result<SlotCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_slot_common(guard, &state, Some(&request.expected), &request.slot)?;
        if request.expected.owner != request.slot.owner
            || request.expected.assignment_generation != request.slot.assignment_generation
            || request.expected.active_move != request.slot.active_move
            || matches!(
                request.slot.state,
                PlacementSlotState::Allocating | PlacementSlotState::Running
            )
            || !matches!(
                (request.expected.state, request.slot.state),
                (
                    PlacementSlotState::BeginHandoff,
                    PlacementSlotState::Stopping
                ) | (PlacementSlotState::Stopping, PlacementSlotState::StopFailed)
                    | (PlacementSlotState::StopFailed, PlacementSlotState::Stopping)
            )
        {
            return Err(StorageError::InvalidTransition);
        }
        state.state_revision = Some(request.slot.version.revision);
        state
            .slots
            .insert(request.slot.key.clone(), request.slot.clone());
        Ok(SlotCommit { slot: request.slot })
    }

    async fn allocate_initial(
        &self,
        guard: &LeaderGuard,
        request: AllocateInitial,
    ) -> Result<AuthorityCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_slot_common(guard, &state, None, &request.slot)?;
        validate_claim_lease(&state, &request.claim)?;
        if request.slot.state != PlacementSlotState::Allocating
            || request.slot.active_move.is_some()
            || !request.claim.matches_slot(&request.slot)
            || state.slots.contains_key(&request.slot.key)
            || state.claims.contains_key(&request.slot.key)
        {
            return Err(StorageError::InvalidTransition);
        }
        if state.slots.len() == self.maximum_slots {
            return Err(StorageError::Capacity);
        }
        state.state_revision = Some(request.slot.version.revision);
        state
            .slots
            .insert(request.slot.key.clone(), request.slot.clone());
        state
            .claims
            .insert(request.slot.key.clone(), request.claim.clone());
        Ok(AuthorityCommit {
            slot: request.slot,
            claim: request.claim,
        })
    }

    async fn activate_authority(
        &self,
        guard: &LeaderGuard,
        request: ActivateAuthority,
    ) -> Result<SlotCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_slot_common(guard, &state, Some(&request.expected_slot), &request.slot)?;
        if request.expected_slot.state != PlacementSlotState::Allocating
            || request.slot.state != PlacementSlotState::Running
            || request.expected_slot.owner != request.slot.owner
            || request.expected_slot.assignment_generation != request.slot.assignment_generation
            || !claim_matches(&state, &request.expected_claim)
            || request.expected_claim.slot != request.slot.key
            || request.slot.owner.as_ref() != Some(&request.expected_claim.owner)
            || request.slot.assignment_generation != request.expected_claim.assignment_generation
        {
            return Err(StorageError::InvalidTransition);
        }
        state.state_revision = Some(request.slot.version.revision);
        state
            .slots
            .insert(request.slot.key.clone(), request.slot.clone());
        Ok(SlotCommit { slot: request.slot })
    }

    async fn reserve_move(
        &self,
        guard: &LeaderGuard,
        request: ReserveMove,
    ) -> Result<MoveCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_slot_common(guard, &state, Some(&request.expected_slot), &request.slot)?;
        validate_plan_update(&request.expected_plan, &request.plan)?;
        if state.plans.get(&request.expected_plan.plan_id) != Some(&request.expected_plan)
            || request.expected_slot.state != PlacementSlotState::Running
            || request.expected_slot.active_move.is_some()
            || request.slot.state != PlacementSlotState::BeginHandoff
            || request.slot.active_move != Some(request.plan.plan_id)
            || request.expected_slot.owner != request.slot.owner
            || request.expected_slot.assignment_generation != request.slot.assignment_generation
            || !request.plan.moves.iter().any(|movement| {
                movement.progress == MoveProgress::Handoff
                    && request.slot.target.as_ref() == Some(&movement.target)
            })
        {
            return Err(StorageError::InvalidTransition);
        }
        state.state_revision = Some(request.slot.version.revision);
        state
            .slots
            .insert(request.slot.key.clone(), request.slot.clone());
        state
            .plans
            .insert(request.plan.plan_id, request.plan.clone());
        Ok(MoveCommit {
            slot: request.slot,
            plan: request.plan,
        })
    }

    async fn reserve_handoff(
        &self,
        guard: &LeaderGuard,
        request: ReserveHandoff,
    ) -> Result<SlotCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_slot_common(guard, &state, Some(&request.expected_slot), &request.slot)?;
        if !matches!(request.slot.key, PlacementSlotKey::Singleton(_))
            || request.expected_slot.state != PlacementSlotState::Running
            || request.expected_slot.active_move.is_some()
            || request.slot.state != PlacementSlotState::BeginHandoff
            || request.slot.active_move.is_none()
            || request.expected_slot.owner != request.slot.owner
            || request.expected_slot.assignment_generation != request.slot.assignment_generation
        {
            return Err(StorageError::InvalidTransition);
        }
        state.state_revision = Some(request.slot.version.revision);
        state
            .slots
            .insert(request.slot.key.clone(), request.slot.clone());
        Ok(SlotCommit { slot: request.slot })
    }

    async fn fence_authority(
        &self,
        guard: &LeaderGuard,
        request: FenceAuthority,
    ) -> Result<SlotCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_slot_common(guard, &state, Some(&request.expected_slot), &request.slot)?;
        let claim_ok = match &request.expected_claim {
            ClaimPredicate::Present(expected) => {
                expected.slot == request.slot.key && claim_matches(&state, expected)
            }
            ClaimPredicate::Absent => !state.claims.contains_key(&request.slot.key),
        };
        if !claim_ok
            || !matches!(
                request.expected_slot.state,
                PlacementSlotState::Stopping | PlacementSlotState::StopFailed
            )
            || request.slot.state != PlacementSlotState::Fenced
            || request.expected_slot.owner != request.slot.owner
            || request.expected_slot.assignment_generation != request.slot.assignment_generation
            || request.expected_slot.active_move != request.slot.active_move
        {
            return Err(StorageError::InvalidTransition);
        }
        if matches!(request.expected_claim, ClaimPredicate::Present(_)) {
            state.claims.remove(&request.slot.key);
        }
        state.state_revision = Some(request.slot.version.revision);
        state
            .slots
            .insert(request.slot.key.clone(), request.slot.clone());
        Ok(SlotCommit { slot: request.slot })
    }

    async fn fence_missing_authority(
        &self,
        guard: &LeaderGuard,
        request: FenceMissingAuthority,
    ) -> Result<SlotCommit, StorageError> {
        InMemoryPlacementStore::fence_missing_authority(self, guard, request).await
    }

    async fn install_authority(
        &self,
        guard: &LeaderGuard,
        request: InstallAuthority,
    ) -> Result<AuthorityCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_slot_common(guard, &state, Some(&request.expected_slot), &request.slot)?;
        validate_claim_lease(&state, &request.claim)?;
        if request.expected_slot.state != PlacementSlotState::Fenced
            || request.slot.state != PlacementSlotState::Allocating
            || request.expected_slot.active_move != request.slot.active_move
            || request.slot.assignment_generation
                != request
                    .expected_slot
                    .assignment_generation
                    .next()
                    .map_err(|_| StorageError::CounterExhausted)?
            || !request.claim.matches_slot(&request.slot)
            || state.claims.contains_key(&request.slot.key)
        {
            return Err(StorageError::InvalidTransition);
        }
        state.state_revision = Some(request.slot.version.revision);
        state
            .slots
            .insert(request.slot.key.clone(), request.slot.clone());
        state
            .claims
            .insert(request.slot.key.clone(), request.claim.clone());
        Ok(AuthorityCommit {
            slot: request.slot,
            claim: request.claim,
        })
    }

    async fn adopt_authority(
        &self,
        guard: &LeaderGuard,
        request: AdoptAuthority,
    ) -> Result<AuthorityCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_slot_common(guard, &state, Some(&request.expected_slot), &request.slot)?;
        validate_claim_lease(&state, &request.claim)?;
        if !claim_matches(&state, &request.expected_claim)
            || request.expected_slot.owner != request.slot.owner
            || request.expected_slot.assignment_generation != request.slot.assignment_generation
            || request.expected_slot.state != request.slot.state
            || request.expected_claim.owner != request.claim.grant.owner
            || request.expected_claim.assignment_generation
                != request.claim.grant.assignment_generation
            || request.expected_claim.coordinator_term >= request.claim.grant.coordinator_term
            || !request.claim.matches_slot(&request.slot)
        {
            return Err(StorageError::InvalidTransition);
        }
        state.state_revision = Some(request.slot.version.revision);
        state
            .slots
            .insert(request.slot.key.clone(), request.slot.clone());
        state
            .claims
            .insert(request.slot.key.clone(), request.claim.clone());
        Ok(AuthorityCommit {
            slot: request.slot,
            claim: request.claim,
        })
    }

    async fn complete_move(
        &self,
        guard: &LeaderGuard,
        request: CompleteMove,
    ) -> Result<MoveCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_slot_common(guard, &state, Some(&request.expected_slot), &request.slot)?;
        validate_plan_update(&request.expected_plan, &request.plan)?;
        if !claim_matches(&state, &request.expected_claim)
            || state.plans.get(&request.expected_plan.plan_id) != Some(&request.expected_plan)
            || request.expected_slot.state != PlacementSlotState::Allocating
            || request.slot.state != PlacementSlotState::Running
            || request.expected_slot.active_move != Some(request.plan.plan_id)
            || request.slot.active_move.is_some()
            || request.expected_slot.owner != request.slot.owner
            || request.expected_slot.assignment_generation != request.slot.assignment_generation
            || request.slot.owner.as_ref() != Some(&request.expected_claim.owner)
            || request
                .plan
                .moves
                .iter()
                .filter(|movement| movement.progress == MoveProgress::Completed)
                .count()
                <= request
                    .expected_plan
                    .moves
                    .iter()
                    .filter(|movement| movement.progress == MoveProgress::Completed)
                    .count()
        {
            return Err(StorageError::InvalidTransition);
        }
        state.state_revision = Some(request.slot.version.revision);
        state
            .slots
            .insert(request.slot.key.clone(), request.slot.clone());
        state
            .plans
            .insert(request.plan.plan_id, request.plan.clone());
        Ok(MoveCommit {
            slot: request.slot,
            plan: request.plan,
        })
    }

    async fn commit_automatic_settings(
        &self,
        guard: &LeaderGuard,
        request: CommitAutomaticSettings,
    ) -> Result<AutomaticBalanceSettings, StorageError> {
        memory_admin::commit_automatic_settings(self, guard, request)
    }

    async fn create_plan_with_operation(
        &self,
        guard: &LeaderGuard,
        request: CreatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError> {
        memory_admin::create_plan_with_operation(self, guard, request)
    }

    async fn update_plan_with_operation(
        &self,
        guard: &LeaderGuard,
        request: UpdatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError> {
        memory_admin::update_plan_with_operation(self, guard, request)
    }

    async fn remove_member_with_operation(
        &self,
        guard: &LeaderGuard,
        request: RemoveMemberWithOperation,
    ) -> Result<MemberCommit, StorageError> {
        memory_admin::remove_member_with_operation(self, guard, request)
    }

    async fn record_admin_operation(
        &self,
        guard: &LeaderGuard,
        request: RecordAdminOperation,
    ) -> Result<AdminOperationRecord, StorageError> {
        memory_admin::record_admin_operation(self, guard, request)
    }

    async fn compact_admin_operations(
        &self,
        guard: &LeaderGuard,
        request: CompactAdminOperations,
    ) -> Result<(), StorageError> {
        memory_admin::compact_admin_operations(self, guard, request)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StorageError {
    #[error("placement store limits must be nonzero")]
    ZeroLimit,
    #[error("placement storage configuration is invalid")]
    InvalidConfig,
    #[error("placement store capacity reached")]
    Capacity,
    #[error("placement monotonic counter is exhausted")]
    CounterExhausted,
    #[error("the exact lease-backed Coordinator leader no longer exists")]
    LeadershipLost,
    #[error("placement domain compare failed")]
    CompareFailed,
    #[error("placement transition would violate a cross-record invariant")]
    InvalidTransition,
    #[error("placement record is invalid")]
    InvalidRecord,
    #[error("placement backend transport is unavailable")]
    Unavailable,
    #[error("placement backend read deadline expired")]
    Deadline,
    #[error("placement backend deadline expired; commit outcome may be unknown")]
    OutcomeUnknown,
    #[error("placement backend authentication failed")]
    Authentication,
    #[error("placement backend rejected an argument")]
    BackendArgument,
    #[error("placement backend returned malformed data")]
    Codec,
    #[error("placement schema generation differs; mixed clusters are forbidden")]
    SchemaGenerationMismatch,
    #[error("node ID is still leased to another incarnation")]
    IncarnationConflict,
}
