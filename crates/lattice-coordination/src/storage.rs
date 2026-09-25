use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::shutdown::{ShutdownManifest, ShutdownReport, participant_key};
use async_trait::async_trait;
use lattice_model::run::{ClusterLifecycle, ControlOperationId, RunEpoch};
use lattice_model::{
    cluster::CoordinatorScope,
    cluster::{ActorGroupId, EntityType, SingletonKind},
};
use thiserror::Error;

use crate::{
    candidates::{CandidateAuthorization, CandidateRegistration, CandidateSetState},
    coordinator::{
        ClusterLeaderGuard, ExactLeaderGuard, GroupLeaderGuard, GroupMemberRecord,
        GroupMemberStatus, LeaderRecord, MemberRecord, MemberStatus, SessionLimits,
        SingletonConfig,
    },
    plan::{MAXIMUM_PLAN_MOVES, MoveProgress, RebalancePlan},
    region::EntityConfig,
    types::{PlacementSlot, PlacementSlotKey, PlacementSlotState, Revision},
};

pub mod barrier;
pub mod candidates;
pub mod counters;
pub mod etcd;
mod lifecycle;
pub mod maintenance;
mod memory_admin;
mod memory_candidates;
mod memory_coordination;
mod memory_page;
mod memory_shutdown;
mod memory_traits;
pub mod page;
pub(crate) mod plan_records;
pub mod records;
mod transfer_capacity;

#[cfg(test)]
mod tests;

use counters::{StoreReadCounters, StoreReadCounts};
use page::{PageCursor, StorePage};

use plan_records::validate_plan_payload;
use records::{
    ActivateAuthority, AdminOperationRecord, AdoptAuthority, AllocateInitial, AuthorityCommit,
    AutomaticBalanceSettings, ClaimPredicate, CommitAutomaticSettings, CompactAdminOperations,
    CompleteMove, CreateGroupMember, CreateMember, CreatePlan, CreatePlanWithOperation, DeletePlan,
    DurableStorageLimits, EntityConfigCommit, FenceAuthority, FenceMissingAuthority,
    GroupMemberCommit, InstallAuthority, LeasedClaim, MemberCommit, MoveCommit, PlanCommit,
    PutEntityConfig, PutSingletonConfig, RecordAdminOperation, RemoveExpiredMember,
    RemoveGroupMember, RemoveMember, ReserveHandoff, ReserveMove, SingletonConfigCommit,
    SlotCommit, TransitionSlot, UpdateGroupMember, UpdateMember, UpdatePlan,
    UpdatePlanWithOperation,
};

#[async_trait]
pub trait CoordinatorLeaseStore: Send + Sync + 'static {
    /// Validates or atomically initializes a pristine namespace and returns its
    /// current run. An etcd handle binds to that epoch permanently; access before
    /// initialization fails, and a later run requires a fresh handle.
    async fn ensure_framework(&self) -> Result<RunEpoch, StorageError>;
    async fn grant_lease(&self, ttl: Duration) -> Result<i64, StorageError>;
    async fn keep_lease_alive(&self, lease_id: i64) -> Result<(), StorageError>;
    async fn revoke_lease(&self, lease_id: i64) -> Result<(), StorageError>;
    async fn lease_time_to_live(&self, lease_id: i64) -> Result<Option<Duration>, StorageError>;
}

/// Low-level, privileged lifecycle operations. Remote callers must pass the
/// management authorization boundary before reaching these store operations.
#[async_trait]
pub trait ClusterLifecycleStore: CoordinatorLeaseStore {
    async fn lifecycle(&self) -> Result<ClusterLifecycle, StorageError>;
    async fn begin_shutdown(
        &self,
        guard: &ClusterLeaderGuard,
        operation: ControlOperationId,
    ) -> Result<ClusterLifecycle, StorageError>;
}

#[async_trait]
pub trait ScopedElectionStore: candidates::CandidateStore {
    async fn campaign_leader(
        &self,
        leader: &LeaderRecord,
        lease_id: i64,
    ) -> Result<bool, StorageError>;
    async fn get_leader(
        &self,
        scope: &CoordinatorScope,
    ) -> Result<Option<LeaderRecord>, StorageError>;
    async fn get_leader_term(&self, scope: &CoordinatorScope) -> Result<u64, StorageError>;
}

#[async_trait]
pub trait MembershipStore: crate::shutdown::ClusterShutdownStore {
    async fn get_membership_revision(&self) -> Result<Revision, StorageError>;
    async fn get_member(&self, node_id: &str) -> Result<Option<MemberRecord>, StorageError>;
    async fn list_members(&self) -> Result<Vec<MemberRecord>, StorageError>;
    /// Reads one bounded page starting strictly after `cursor`. A backend has to answer this from a
    /// bounded range request: a full scan that slices its own result in memory bounds the caller's
    /// memory but not the read the durable store performs.
    async fn list_members_page(
        &self,
        cursor: Option<&PageCursor>,
        limit: usize,
    ) -> Result<StorePage<MemberRecord>, StorageError>;
    async fn create_member(
        &self,
        guard: &ClusterLeaderGuard,
        request: CreateMember,
    ) -> Result<MemberCommit, StorageError>;
    async fn update_member(
        &self,
        guard: &ClusterLeaderGuard,
        request: UpdateMember,
    ) -> Result<MemberCommit, StorageError>;
    async fn remove_member(
        &self,
        guard: &ClusterLeaderGuard,
        request: RemoveMember,
    ) -> Result<MemberCommit, StorageError>;
    async fn remove_expired_member(
        &self,
        guard: &ClusterLeaderGuard,
        request: RemoveExpiredMember,
    ) -> Result<MemberCommit, StorageError>;
}

#[async_trait]
pub trait ActorGroupStore: CoordinatorLeaseStore + barrier::TransferBarrierStore {
    fn durable_limits(&self, group: &ActorGroupId) -> DurableStorageLimits;
    async fn get_placement_revision(&self, group: &ActorGroupId) -> Result<Revision, StorageError>;
    async fn get_group_member(
        &self,
        group: &ActorGroupId,
        node_id: &str,
    ) -> Result<Option<GroupMemberRecord>, StorageError>;
    async fn list_group_members(
        &self,
        group: &ActorGroupId,
    ) -> Result<Vec<GroupMemberRecord>, StorageError>;
    async fn create_group_member(
        &self,
        guard: &GroupLeaderGuard,
        request: CreateGroupMember,
    ) -> Result<GroupMemberCommit, StorageError>;
    async fn update_group_member(
        &self,
        guard: &GroupLeaderGuard,
        request: UpdateGroupMember,
    ) -> Result<GroupMemberCommit, StorageError>;
    async fn remove_group_member(
        &self,
        guard: &GroupLeaderGuard,
        request: RemoveGroupMember,
    ) -> Result<GroupMemberCommit, StorageError>;
    async fn get_entity_config(
        &self,
        group: &ActorGroupId,
        entity_type: &EntityType,
    ) -> Result<Option<EntityConfig>, StorageError>;
    async fn list_entity_configs(
        &self,
        group: &ActorGroupId,
    ) -> Result<Vec<EntityConfig>, StorageError>;
    async fn put_entity_config(
        &self,
        guard: &GroupLeaderGuard,
        request: PutEntityConfig,
    ) -> Result<EntityConfigCommit, StorageError>;
    async fn get_singleton_config(
        &self,
        group: &ActorGroupId,
        kind: &SingletonKind,
    ) -> Result<Option<SingletonConfig>, StorageError>;
    async fn list_singleton_configs(
        &self,
        group: &ActorGroupId,
    ) -> Result<Vec<SingletonConfig>, StorageError>;
    async fn put_singleton_config(
        &self,
        guard: &GroupLeaderGuard,
        request: PutSingletonConfig,
    ) -> Result<SingletonConfigCommit, StorageError>;
    async fn get_slot(&self, key: &PlacementSlotKey)
    -> Result<Option<PlacementSlot>, StorageError>;
    async fn get_plan(
        &self,
        group: &ActorGroupId,
        plan_id: u128,
    ) -> Result<Option<RebalancePlan>, StorageError>;
    async fn get_claim(&self, key: &PlacementSlotKey) -> Result<Option<LeasedClaim>, StorageError>;
    async fn list_slots(&self, group: &ActorGroupId) -> Result<Vec<PlacementSlot>, StorageError>;
    async fn list_plans(&self, group: &ActorGroupId) -> Result<Vec<RebalancePlan>, StorageError>;
    async fn list_claims(&self, group: &ActorGroupId) -> Result<Vec<LeasedClaim>, StorageError>;
    async fn get_automatic_settings(
        &self,
        group: &ActorGroupId,
    ) -> Result<Option<AutomaticBalanceSettings>, StorageError>;
    async fn get_admin_operation(
        &self,
        group: &ActorGroupId,
        operation_id: &str,
    ) -> Result<Option<AdminOperationRecord>, StorageError>;
    async fn list_admin_operations(
        &self,
        group: &ActorGroupId,
    ) -> Result<Vec<AdminOperationRecord>, StorageError>;
    /// Reads one bounded page of slots starting strictly after `cursor`. `limit` bounds the records
    /// the backend reads, not the records that survive `states`, because a durable range cannot
    /// filter on a decoded value.
    async fn list_slots_page(
        &self,
        group: &ActorGroupId,
        states: &[PlacementSlotState],
        cursor: Option<&PageCursor>,
        limit: usize,
    ) -> Result<StorePage<PlacementSlot>, StorageError>;
    async fn list_plans_page(
        &self,
        group: &ActorGroupId,
        cursor: Option<&PageCursor>,
        limit: usize,
    ) -> Result<StorePage<RebalancePlan>, StorageError>;
    async fn list_claims_page(
        &self,
        group: &ActorGroupId,
        cursor: Option<&PageCursor>,
        limit: usize,
    ) -> Result<StorePage<LeasedClaim>, StorageError>;

    async fn create_plan(
        &self,
        guard: &GroupLeaderGuard,
        request: CreatePlan,
    ) -> Result<PlanCommit, StorageError>;
    async fn update_plan(
        &self,
        guard: &GroupLeaderGuard,
        request: UpdatePlan,
    ) -> Result<PlanCommit, StorageError>;
    async fn delete_plan(
        &self,
        guard: &GroupLeaderGuard,
        request: DeletePlan,
    ) -> Result<PlanCommit, StorageError>;
    async fn transition_slot(
        &self,
        guard: &GroupLeaderGuard,
        request: TransitionSlot,
    ) -> Result<SlotCommit, StorageError>;
    async fn allocate_initial(
        &self,
        guard: &GroupLeaderGuard,
        request: AllocateInitial,
    ) -> Result<AuthorityCommit, StorageError>;
    async fn activate_authority(
        &self,
        guard: &GroupLeaderGuard,
        request: ActivateAuthority,
    ) -> Result<SlotCommit, StorageError>;
    async fn reserve_move(
        &self,
        guard: &GroupLeaderGuard,
        request: ReserveMove,
    ) -> Result<MoveCommit, StorageError>;
    async fn reserve_handoff(
        &self,
        guard: &GroupLeaderGuard,
        request: ReserveHandoff,
    ) -> Result<SlotCommit, StorageError>;
    async fn fence_authority(
        &self,
        guard: &GroupLeaderGuard,
        request: FenceAuthority,
    ) -> Result<SlotCommit, StorageError>;
    async fn fence_missing_authority(
        &self,
        guard: &GroupLeaderGuard,
        request: FenceMissingAuthority,
    ) -> Result<SlotCommit, StorageError>;
    async fn install_authority(
        &self,
        guard: &GroupLeaderGuard,
        request: InstallAuthority,
    ) -> Result<AuthorityCommit, StorageError>;
    async fn adopt_authority(
        &self,
        guard: &GroupLeaderGuard,
        request: AdoptAuthority,
    ) -> Result<LeasedClaim, StorageError>;
    async fn complete_move(
        &self,
        guard: &GroupLeaderGuard,
        request: CompleteMove,
    ) -> Result<MoveCommit, StorageError>;
    async fn commit_automatic_settings(
        &self,
        guard: &GroupLeaderGuard,
        request: CommitAutomaticSettings,
    ) -> Result<AutomaticBalanceSettings, StorageError>;
    async fn create_plan_with_operation(
        &self,
        guard: &GroupLeaderGuard,
        request: CreatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError>;
    async fn update_plan_with_operation(
        &self,
        guard: &GroupLeaderGuard,
        request: UpdatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError>;
    async fn record_admin_operation(
        &self,
        guard: &GroupLeaderGuard,
        request: RecordAdminOperation,
    ) -> Result<AdminOperationRecord, StorageError>;
    async fn compact_admin_operations(
        &self,
        guard: &GroupLeaderGuard,
        request: CompactAdminOperations,
    ) -> Result<(), StorageError>;
}

#[derive(Debug, Clone)]
pub struct InMemoryCoordinationStore {
    inner: Arc<Mutex<MemoryState>>,
    counters: Arc<StoreReadCounters>,
    maximum_slots: usize,
    maximum_plans: usize,
    maximum_members: usize,
    maximum_admin_operations: usize,
}

#[derive(Debug, Default)]
struct MemoryState {
    shutdown_obligations: BTreeMap<String, NodeKey>,
    shutdown_manifest: Option<ShutdownManifest>,
    shutdown_reports: BTreeMap<String, ShutdownReport>,
    stopped_nodes: BTreeMap<String, NodeKey>,
    lifecycle: Option<ClusterLifecycle>,
    candidate_sets: BTreeMap<CoordinatorScope, CandidateSetState>,
    candidate_authorizations: BTreeMap<(CoordinatorScope, String), CandidateAuthorization>,
    candidate_registrations: BTreeMap<(CoordinatorScope, String), CandidateRegistration>,
    slots: BTreeMap<PlacementSlotKey, PlacementSlot>,
    transfer_reservations: BTreeMap<PlacementSlotKey, transfer_capacity::TransferReservation>,
    transfer_barriers: BTreeMap<(PlacementSlotKey, u128), barrier::BarrierSnapshot>,
    plans: BTreeMap<(ActorGroupId, u128), RebalancePlan>,
    framework_identity: Option<String>,
    membership_revision: Option<Revision>,
    placement_revisions: BTreeMap<ActorGroupId, Revision>,
    next_lease: i64,
    leases: BTreeMap<i64, LeaseState>,
    leaders: BTreeMap<CoordinatorScope, (i64, LeaderRecord)>,
    leader_terms: BTreeMap<CoordinatorScope, u64>,
    members: BTreeMap<String, MemberRecord>,
    group_members: BTreeMap<(ActorGroupId, String), GroupMemberRecord>,
    entity_configs: BTreeMap<(ActorGroupId, EntityType), EntityConfig>,
    singleton_configs: BTreeMap<(ActorGroupId, SingletonKind), SingletonConfig>,
    claims: BTreeMap<PlacementSlotKey, LeasedClaim>,
    automatic_settings: BTreeMap<ActorGroupId, AutomaticBalanceSettings>,
    admin_operations: BTreeMap<(ActorGroupId, String), AdminOperationRecord>,
}

#[derive(Debug, Clone, Copy)]
struct LeaseState {
    ttl: Duration,
}

include!("storage/memory_core.rs");

impl InMemoryCoordinationStore {
    async fn create_member(
        &self,
        guard: &ClusterLeaderGuard,
        request: CreateMember,
    ) -> Result<MemberCommit, StorageError> {
        validate_member_record(&request.member)?;
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        if guard.scope() != &CoordinatorScope::Cluster {
            return Err(StorageError::InvalidRecord);
        }
        validate_next_revision(&state, guard.scope(), request.member.version.revision)?;
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
        if !state
            .shutdown_obligations
            .contains_key(&participant_key(&request.member.node))
            && state.shutdown_obligations.len() >= self.maximum_members
        {
            return Err(StorageError::Capacity);
        }
        set_revision(&mut state, guard.scope(), request.member.version.revision);
        state.shutdown_obligations.insert(
            participant_key(&request.member.node),
            request.member.node.clone(),
        );
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
        guard: &ClusterLeaderGuard,
        request: UpdateMember,
    ) -> Result<MemberCommit, StorageError> {
        validate_member_record(&request.member)?;
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        if guard.scope() != &CoordinatorScope::Cluster {
            return Err(StorageError::InvalidRecord);
        }
        validate_next_revision(&state, guard.scope(), request.member.version.revision)?;
        if request.expected.node.node_id != request.member.node.node_id
            || request.expected.node.incarnation != request.member.node.incarnation
            || state.members.get(&request.expected.node.node_id) != Some(&request.expected)
            || !state.leases.contains_key(&request.member.lease_id)
        {
            return Err(StorageError::CompareFailed);
        }
        set_revision(&mut state, guard.scope(), request.member.version.revision);
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
        guard: &ClusterLeaderGuard,
        request: RemoveMember,
    ) -> Result<MemberCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        if state.members.get(&request.expected.node.node_id) != Some(&request.expected) {
            return Err(StorageError::CompareFailed);
        }
        if guard.scope() != &CoordinatorScope::Cluster {
            return Err(StorageError::InvalidRecord);
        }
        let next = current_revision(&state, guard.scope())
            .next()
            .map_err(|_| StorageError::CounterExhausted)?;
        set_revision(&mut state, guard.scope(), next);
        state.members.remove(&request.expected.node.node_id);
        Ok(MemberCommit {
            revision: next,
            member: request.expected,
        })
    }

    async fn remove_expired_member(
        &self,
        guard: &ClusterLeaderGuard,
        request: RemoveExpiredMember,
    ) -> Result<MemberCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        if guard.scope() != &CoordinatorScope::Cluster {
            return Err(StorageError::InvalidRecord);
        }
        if state.members.contains_key(&request.expected.node.node_id) {
            return Err(StorageError::CompareFailed);
        }
        let next = current_revision(&state, guard.scope())
            .next()
            .map_err(|_| StorageError::CounterExhausted)?;
        set_revision(&mut state, guard.scope(), next);
        Ok(MemberCommit {
            revision: next,
            member: request.expected,
        })
    }

    async fn create_group_member(
        &self,
        guard: &GroupLeaderGuard,
        request: CreateGroupMember,
    ) -> Result<GroupMemberCommit, StorageError> {
        validate_group_member_record(guard, &request.member)?;
        if request.member.version.term != guard.term() {
            return Err(StorageError::InvalidRecord);
        }
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        if state
            .members
            .get(&request.expected_global_member.node.node_id)
            != Some(&request.expected_global_member)
            || request.expected_global_member.status != MemberStatus::Up
            || request.expected_global_member.node != request.member.node
            || state.group_members.contains_key(&(
                request.member.version.group.clone(),
                request.member.node.node_id.clone(),
            ))
        {
            return Err(StorageError::CompareFailed);
        }
        validate_next_revision(&state, guard.scope(), request.member.version.revision)?;
        if state
            .group_members
            .keys()
            .filter(|(group, _)| group == &request.member.version.group)
            .count()
            == self.maximum_members
        {
            return Err(StorageError::Capacity);
        }
        set_revision(&mut state, guard.scope(), request.member.version.revision);
        state.group_members.insert(
            (
                request.member.version.group.clone(),
                request.member.node.node_id.clone(),
            ),
            request.member.clone(),
        );
        Ok(GroupMemberCommit {
            member: request.member,
        })
    }

    async fn update_group_member(
        &self,
        guard: &GroupLeaderGuard,
        request: UpdateGroupMember,
    ) -> Result<GroupMemberCommit, StorageError> {
        validate_group_member_record(guard, &request.expected)?;
        validate_group_member_record(guard, &request.member)?;
        if request.member.version.term != guard.term() {
            return Err(StorageError::InvalidRecord);
        }
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        if state
            .members
            .get(&request.expected_global_member.node.node_id)
            != Some(&request.expected_global_member)
            || request.expected_global_member.status != MemberStatus::Up
            || request.expected_global_member.node != request.member.node
            || request.expected.node != request.member.node
            || request.expected.version.group != request.member.version.group
            || state.group_members.get(&(
                request.expected.version.group.clone(),
                request.expected.node.node_id.clone(),
            )) != Some(&request.expected)
        {
            return Err(StorageError::CompareFailed);
        }
        validate_next_revision(&state, guard.scope(), request.member.version.revision)?;
        set_revision(&mut state, guard.scope(), request.member.version.revision);
        state.group_members.insert(
            (
                request.member.version.group.clone(),
                request.member.node.node_id.clone(),
            ),
            request.member.clone(),
        );
        Ok(GroupMemberCommit {
            member: request.member,
        })
    }

    async fn remove_group_member(
        &self,
        guard: &GroupLeaderGuard,
        request: RemoveGroupMember,
    ) -> Result<GroupMemberCommit, StorageError> {
        validate_group_member_record(guard, &request.expected)?;
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        let key = (
            request.expected.version.group.clone(),
            request.expected.node.node_id.clone(),
        );
        if state.group_members.get(&key) != Some(&request.expected) {
            return Err(StorageError::CompareFailed);
        }
        let next = current_revision(&state, guard.scope())
            .next()
            .map_err(|_| StorageError::CounterExhausted)?;
        set_revision(&mut state, guard.scope(), next);
        state.group_members.remove(&key);
        Ok(GroupMemberCommit {
            member: request.expected,
        })
    }

    async fn create_plan(
        &self,
        guard: &GroupLeaderGuard,
        request: CreatePlan,
    ) -> Result<PlanCommit, StorageError> {
        validate_plan_group(guard, &request.plan)?;
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        if request.plan.coordinator_term != guard.term()
            || request.plan.record_revision.get() != 1
            || state
                .plans
                .contains_key(&(request.plan.group.clone(), request.plan.plan_id))
        {
            return Err(StorageError::CompareFailed);
        }
        if request.plan.durable().is_some()
            && state
                .plans
                .keys()
                .filter(|(group, _)| group == &request.plan.group)
                .count()
                == self.maximum_plans
        {
            return Err(StorageError::Capacity);
        }
        if let Some(record) = request.plan.durable() {
            state
                .plans
                .insert((record.group.clone(), record.plan_id), record);
        }
        Ok(PlanCommit { plan: request.plan })
    }

    async fn update_plan(
        &self,
        guard: &GroupLeaderGuard,
        request: UpdatePlan,
    ) -> Result<PlanCommit, StorageError> {
        validate_plan_group(guard, &request.expected)?;
        validate_plan_group(guard, &request.plan)?;
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_plan_update(&request.expected, &request.plan)?;
        if state
            .plans
            .get(&(request.expected.group.clone(), request.expected.plan_id))
            != request.expected.durable().as_ref()
        {
            return Err(StorageError::CompareFailed);
        }
        if let Some(record) = request.plan.durable() {
            state
                .plans
                .insert((record.group.clone(), record.plan_id), record);
        }
        Ok(PlanCommit { plan: request.plan })
    }

    async fn delete_plan(
        &self,
        guard: &GroupLeaderGuard,
        request: DeletePlan,
    ) -> Result<PlanCommit, StorageError> {
        validate_plan_group(guard, &request.expected)?;
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        if state
            .plans
            .get(&(request.expected.group.clone(), request.expected.plan_id))
            != request.expected.durable().as_ref()
        {
            return Err(StorageError::CompareFailed);
        }
        state
            .plans
            .remove(&(request.expected.group.clone(), request.expected.plan_id));
        Ok(PlanCommit {
            plan: request.expected,
        })
    }

    async fn transition_slot(
        &self,
        guard: &GroupLeaderGuard,
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
        set_revision(&mut state, guard.scope(), request.slot.version.revision);
        state
            .slots
            .insert(request.slot.key.clone(), request.slot.clone());
        Ok(SlotCommit { slot: request.slot })
    }

    async fn allocate_initial(
        &self,
        guard: &GroupLeaderGuard,
        request: AllocateInitial,
    ) -> Result<AuthorityCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_slot_common(guard, &state, None, &request.slot)?;
        validate_assignment_members(
            &state,
            &request.expected_global_member,
            &request.expected_group_member,
            request
                .slot
                .owner
                .as_ref()
                .ok_or(StorageError::InvalidRecord)?,
        )?;
        validate_claim_lease(&state, &request.claim)?;
        if request.slot.state != PlacementSlotState::Allocating
            || request.slot.active_move.is_some()
            || request.claim.grant.coordinator_term != guard.term()
            || !request.claim.matches_slot(&request.slot)
            || state.slots.contains_key(&request.slot.key)
            || state.claims.contains_key(&request.slot.key)
        {
            return Err(StorageError::InvalidTransition);
        }
        if state.slots.len() == self.maximum_slots {
            return Err(StorageError::Capacity);
        }
        set_revision(&mut state, guard.scope(), request.slot.version.revision);
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
        guard: &GroupLeaderGuard,
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
        transfer_capacity::release_memory(&mut state, &request.expected_slot)?;
        set_revision(&mut state, guard.scope(), request.slot.version.revision);
        state
            .slots
            .insert(request.slot.key.clone(), request.slot.clone());
        Ok(SlotCommit { slot: request.slot })
    }

    async fn reserve_move(
        &self,
        guard: &GroupLeaderGuard,
        request: ReserveMove,
    ) -> Result<MoveCommit, StorageError> {
        validate_plan_group(guard, &request.expected_plan)?;
        validate_plan_group(guard, &request.plan)?;
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_slot_common(guard, &state, Some(&request.expected_slot), &request.slot)?;
        validate_plan_update(&request.expected_plan, &request.plan)?;
        if state.plans.get(&(
            request.expected_plan.group.clone(),
            request.expected_plan.plan_id,
        )) != request.expected_plan.durable().as_ref()
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
        if request.expected_plan.durable().is_none()
            && state
                .plans
                .keys()
                .filter(|(group, _)| group == &request.plan.group)
                .count()
                >= self.maximum_plans
        {
            return Err(StorageError::Capacity);
        }
        if request.slot.barrier_sessions.len() > self.maximum_members {
            return Err(StorageError::Capacity);
        }
        transfer_capacity::reserve_memory(&mut state, &request.slot, request.limits)?;
        set_revision(&mut state, guard.scope(), request.slot.version.revision);
        state
            .slots
            .insert(request.slot.key.clone(), request.slot.clone());
        if let Some(record) = request.plan.durable() {
            state
                .plans
                .insert((record.group.clone(), record.plan_id), record);
        }
        Ok(MoveCommit {
            slot: request.slot,
            plan: request.plan,
        })
    }

    async fn reserve_handoff(
        &self,
        guard: &GroupLeaderGuard,
        request: ReserveHandoff,
    ) -> Result<SlotCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_slot_common(guard, &state, Some(&request.expected_slot), &request.slot)?;
        if !matches!(request.slot.key, PlacementSlotKey::Singleton { .. })
            || request.expected_slot.state != PlacementSlotState::Running
            || request.expected_slot.active_move.is_some()
            || request.slot.state != PlacementSlotState::BeginHandoff
            || request.slot.active_move.is_none()
            || request.expected_slot.owner != request.slot.owner
            || request.expected_slot.assignment_generation != request.slot.assignment_generation
        {
            return Err(StorageError::InvalidTransition);
        }
        if request.slot.barrier_sessions.len() > self.maximum_members {
            return Err(StorageError::Capacity);
        }
        transfer_capacity::reserve_memory(&mut state, &request.slot, request.limits)?;
        set_revision(&mut state, guard.scope(), request.slot.version.revision);
        state
            .slots
            .insert(request.slot.key.clone(), request.slot.clone());
        Ok(SlotCommit { slot: request.slot })
    }

    async fn fence_authority(
        &self,
        guard: &GroupLeaderGuard,
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
        set_revision(&mut state, guard.scope(), request.slot.version.revision);
        state
            .slots
            .insert(request.slot.key.clone(), request.slot.clone());
        Ok(SlotCommit { slot: request.slot })
    }

    async fn install_authority(
        &self,
        guard: &GroupLeaderGuard,
        request: InstallAuthority,
    ) -> Result<AuthorityCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_assignment_members(
            &state,
            &request.expected_global_member,
            &request.expected_group_member,
            request
                .slot
                .owner
                .as_ref()
                .ok_or(StorageError::InvalidRecord)?,
        )?;
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
            || request.claim.grant.coordinator_term != guard.term()
            || !request.claim.matches_slot(&request.slot)
            || state.claims.contains_key(&request.slot.key)
        {
            return Err(StorageError::InvalidTransition);
        }
        set_revision(&mut state, guard.scope(), request.slot.version.revision);
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
        guard: &GroupLeaderGuard,
        request: AdoptAuthority,
    ) -> Result<LeasedClaim, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_assignment_members(
            &state,
            &request.expected_global_member,
            &request.expected_group_member,
            request
                .expected_slot
                .owner
                .as_ref()
                .ok_or(StorageError::InvalidRecord)?,
        )?;
        validate_claim_lease(&state, &request.claim)?;
        if !claim_matches(&state, &request.expected_claim)
            || state.slots.get(&request.expected_slot.key) != Some(&request.expected_slot)
            || request.expected_claim.owner != request.claim.grant.owner
            || request.expected_claim.assignment_generation
                != request.claim.grant.assignment_generation
            || request.expected_claim.coordinator_term > request.claim.grant.coordinator_term
            || request.claim.grant.grant_sequence <= request.expected_claim.grant_sequence
            || (request.expected_claim.coordinator_term == request.claim.grant.coordinator_term
                && request.claim.grant.request_id == 0)
            || request.claim.grant.coordinator_term != guard.term()
            || !request.claim.matches_slot(&request.expected_slot)
        {
            return Err(StorageError::InvalidTransition);
        }
        state
            .claims
            .insert(request.expected_slot.key, request.claim.clone());
        Ok(request.claim)
    }

    async fn complete_move(
        &self,
        guard: &GroupLeaderGuard,
        request: CompleteMove,
    ) -> Result<MoveCommit, StorageError> {
        validate_plan_group(guard, &request.expected_plan)?;
        validate_plan_group(guard, &request.plan)?;
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_slot_common(guard, &state, Some(&request.expected_slot), &request.slot)?;
        validate_plan_update(&request.expected_plan, &request.plan)?;
        if !claim_matches(&state, &request.expected_claim)
            || state.plans.get(&(
                request.expected_plan.group.clone(),
                request.expected_plan.plan_id,
            )) != request.expected_plan.durable().as_ref()
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
        transfer_capacity::release_memory(&mut state, &request.expected_slot)?;
        set_revision(&mut state, guard.scope(), request.slot.version.revision);
        state
            .slots
            .insert(request.slot.key.clone(), request.slot.clone());
        if let Some(record) = request.plan.durable() {
            state
                .plans
                .insert((record.group.clone(), record.plan_id), record);
        }
        Ok(MoveCommit {
            slot: request.slot,
            plan: request.plan,
        })
    }

    async fn commit_automatic_settings(
        &self,
        guard: &GroupLeaderGuard,
        request: CommitAutomaticSettings,
    ) -> Result<AutomaticBalanceSettings, StorageError> {
        memory_admin::commit_automatic_settings(self, guard, request)
    }

    async fn create_plan_with_operation(
        &self,
        guard: &GroupLeaderGuard,
        request: CreatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError> {
        memory_admin::create_plan_with_operation(self, guard, request)
    }

    async fn update_plan_with_operation(
        &self,
        guard: &GroupLeaderGuard,
        request: UpdatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError> {
        memory_admin::update_plan_with_operation(self, guard, request)
    }

    async fn record_admin_operation(
        &self,
        guard: &GroupLeaderGuard,
        request: RecordAdminOperation,
    ) -> Result<AdminOperationRecord, StorageError> {
        memory_admin::record_admin_operation(self, guard, request)
    }

    async fn compact_admin_operations(
        &self,
        guard: &GroupLeaderGuard,
        request: CompactAdminOperations,
    ) -> Result<(), StorageError> {
        memory_admin::compact_admin_operations(self, guard, request)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StorageError {
    #[error("durable record is {actual} bytes, exceeding the {maximum}-byte limit")]
    RecordTooLarge { actual: usize, maximum: usize },
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
    #[error("placement group compare failed")]
    CompareFailed,
    #[error("placement transition would violate a cross-record invariant")]
    InvalidTransition,
    #[error("placement record is invalid")]
    InvalidRecord,
    #[error("placement backend transport is unavailable")]
    Unavailable,
    #[error("the fixed source revision was compacted before publication")]
    SnapshotCompacted,
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
    #[error("coordination storage metadata or configured limits differ")]
    StorageMetadataMismatch,
    #[error(
        "framework identity is missing or differs from this build; an explicit full-stop upgrade is required"
    )]
    FrameworkMismatch,
    #[error("the operation belongs to another cluster run")]
    RunMismatch,
    #[error("initialize the coordination store before accessing a run")]
    RunNotInitialized,
    #[error("offline maintenance requires exact stopped-deployment confirmation")]
    ResetConfirmationRequired,
    #[error("offline cleanup is blocked by live leased records")]
    ResetLiveLease,
    #[error("candidate is not currently eligible for this scope")]
    CandidateNotEligible,
    #[error("cannot remove the last eligible candidate")]
    LastCandidate,
    #[error("runtime namespace contains an unrecognized key family; inspect before cleanup")]
    UnrecognizedRuntimeKey,
    #[error("the cluster run does not permit this operation")]
    RunNotRunning,
    #[error("cluster shutdown is blocked by missing or unsuccessful stop evidence")]
    ShutdownBlocked,
    #[error("node ID is still leased to another incarnation")]
    IncarnationConflict,
}
