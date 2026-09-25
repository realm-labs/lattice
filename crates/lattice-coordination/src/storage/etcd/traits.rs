use lattice_model::run::RunEpoch;
use std::time::Duration;

use async_trait::async_trait;
use lattice_model::{
    cluster::CoordinatorScope,
    cluster::{ActorGroupId, EntityType, SingletonKind},
};

use super::{EtcdCoordinationStore, parse_revision_value};
use crate::{
    coordinator::{
        ClusterLeaderGuard, GroupLeaderGuard, GroupMemberRecord, LeaderRecord, MemberRecord,
        SingletonConfig,
    },
    plan::RebalancePlan,
    region::EntityConfig,
    storage::{
        ActorGroupStore, CoordinatorLeaseStore, MembershipStore, ScopedElectionStore, StorageError,
        page::{PageCursor, StorePage},
        records::{
            ActivateAuthority, AdminOperationRecord, AdoptAuthority, AllocateInitial,
            AuthorityCommit, AutomaticBalanceSettings, CommitAutomaticSettings,
            CompactAdminOperations, CompleteMove, CreateGroupMember, CreateMember, CreatePlan,
            CreatePlanWithOperation, DeletePlan, DurableStorageLimits, EntityConfigCommit,
            FenceAuthority, FenceMissingAuthority, GroupMemberCommit, InstallAuthority,
            LeasedClaim, MemberCommit, MoveCommit, PlanCommit, PutEntityConfig, PutSingletonConfig,
            RecordAdminOperation, RemoveExpiredMember, RemoveGroupMember, RemoveMember,
            ReserveHandoff, ReserveMove, SingletonConfigCommit, SlotCommit, TransitionSlot,
            UpdateGroupMember, UpdateMember, UpdatePlan, UpdatePlanWithOperation,
        },
    },
    types::{PlacementSlot, PlacementSlotKey, PlacementSlotState, Revision},
};

#[async_trait]
impl CoordinatorLeaseStore for EtcdCoordinationStore {
    async fn ensure_framework(&self) -> Result<RunEpoch, StorageError> {
        EtcdCoordinationStore::ensure_framework(self).await
    }
    async fn grant_lease(&self, ttl: Duration) -> Result<i64, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::grant_lease(self, ttl).await
    }
    async fn keep_lease_alive(&self, lease_id: i64) -> Result<(), StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::keep_lease_alive(self, lease_id).await
    }
    async fn revoke_lease(&self, lease_id: i64) -> Result<(), StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::revoke_lease(self, lease_id).await
    }
    async fn lease_time_to_live(&self, lease_id: i64) -> Result<Option<Duration>, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::lease_time_to_live(self, lease_id).await
    }
}

#[async_trait]
impl ScopedElectionStore for EtcdCoordinationStore {
    async fn campaign_leader(
        &self,
        leader: &LeaderRecord,
        lease_id: i64,
    ) -> Result<bool, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::campaign_leader(self, leader, lease_id).await
    }
    async fn get_leader(
        &self,
        scope: &CoordinatorScope,
    ) -> Result<Option<LeaderRecord>, StorageError> {
        self.bound_epoch()?;
        self.get_leader_inner(scope).await
    }
    async fn get_leader_term(&self, scope: &CoordinatorScope) -> Result<u64, StorageError> {
        self.bound_epoch()?;
        self.get_leader_term_inner(scope).await
    }
}

#[async_trait]
impl MembershipStore for EtcdCoordinationStore {
    async fn get_membership_revision(&self) -> Result<Revision, StorageError> {
        self.bound_epoch()?;
        self.get_membership_revision_inner().await
    }
    async fn get_member(&self, node_id: &str) -> Result<Option<MemberRecord>, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::get_member(self, node_id).await
    }
    async fn list_members(&self) -> Result<Vec<MemberRecord>, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::list_members(self).await
    }
    async fn list_members_page(
        &self,
        cursor: Option<&PageCursor>,
        limit: usize,
    ) -> Result<StorePage<MemberRecord>, StorageError> {
        self.bound_epoch()?;
        self.list_members_page_inner(cursor, limit).await
    }
    async fn create_member(
        &self,
        guard: &ClusterLeaderGuard,
        request: CreateMember,
    ) -> Result<MemberCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::create_member(self, guard, request).await
    }
    async fn update_member(
        &self,
        guard: &ClusterLeaderGuard,
        request: UpdateMember,
    ) -> Result<MemberCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::update_member(self, guard, request).await
    }
    async fn remove_member(
        &self,
        guard: &ClusterLeaderGuard,
        request: RemoveMember,
    ) -> Result<MemberCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::remove_member(self, guard, request).await
    }

    async fn remove_expired_member(
        &self,
        guard: &ClusterLeaderGuard,
        request: RemoveExpiredMember,
    ) -> Result<MemberCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::remove_expired_member(self, guard, request).await
    }
}

#[async_trait]
impl ActorGroupStore for EtcdCoordinationStore {
    fn durable_limits(&self, _group: &ActorGroupId) -> DurableStorageLimits {
        self.durable_limits_inner()
    }
    async fn get_placement_revision(&self, group: &ActorGroupId) -> Result<Revision, StorageError> {
        self.bound_epoch()?;
        let key = self.scope_key(&CoordinatorScope::Group(group.clone()), "state_revision");
        self.read_raw(&key)
            .await?
            .map(|(bytes, _, _)| parse_revision_value(&bytes))
            .transpose()
            .map(|revision| revision.unwrap_or_else(|| Revision::new(1).expect("one is valid")))
    }
    async fn get_group_member(
        &self,
        group: &ActorGroupId,
        node_id: &str,
    ) -> Result<Option<GroupMemberRecord>, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::get_group_member(self, group, node_id).await
    }
    async fn list_group_members(
        &self,
        group: &ActorGroupId,
    ) -> Result<Vec<GroupMemberRecord>, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::list_group_members(self, group).await
    }
    async fn create_group_member(
        &self,
        guard: &GroupLeaderGuard,
        request: CreateGroupMember,
    ) -> Result<GroupMemberCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::create_group_member(self, guard, request).await
    }
    async fn update_group_member(
        &self,
        guard: &GroupLeaderGuard,
        request: UpdateGroupMember,
    ) -> Result<GroupMemberCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::update_group_member(self, guard, request).await
    }
    async fn remove_group_member(
        &self,
        guard: &GroupLeaderGuard,
        request: RemoveGroupMember,
    ) -> Result<GroupMemberCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::remove_group_member(self, guard, request).await
    }
    async fn get_entity_config(
        &self,
        group: &ActorGroupId,
        entity_type: &EntityType,
    ) -> Result<Option<EntityConfig>, StorageError> {
        self.bound_epoch()?;
        self.get_json_key(&self.entity_config_key(group, entity_type))
            .await
    }
    async fn list_entity_configs(
        &self,
        group: &ActorGroupId,
    ) -> Result<Vec<EntityConfig>, StorageError> {
        self.bound_epoch()?;
        self.list_json(
            &format!("definitions/groups/{}/entity_types/", group.as_str()),
            self.limits.maximum_entity_configs,
        )
        .await
    }
    async fn put_entity_config(
        &self,
        guard: &GroupLeaderGuard,
        request: PutEntityConfig,
    ) -> Result<EntityConfigCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::put_entity_config(self, guard, request).await
    }
    async fn get_singleton_config(
        &self,
        group: &ActorGroupId,
        kind: &SingletonKind,
    ) -> Result<Option<SingletonConfig>, StorageError> {
        self.bound_epoch()?;
        self.get_json_key(&self.singleton_config_key(group, kind))
            .await
    }
    async fn list_singleton_configs(
        &self,
        group: &ActorGroupId,
    ) -> Result<Vec<SingletonConfig>, StorageError> {
        self.bound_epoch()?;
        self.list_json(
            &format!("definitions/groups/{}/singleton_types/", group.as_str()),
            self.limits.maximum_singleton_configs,
        )
        .await
    }
    async fn put_singleton_config(
        &self,
        guard: &GroupLeaderGuard,
        request: PutSingletonConfig,
    ) -> Result<SingletonConfigCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::put_singleton_config(self, guard, request).await
    }
    async fn get_slot(
        &self,
        key: &PlacementSlotKey,
    ) -> Result<Option<PlacementSlot>, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::get_slot(self, key).await
    }
    async fn get_plan(
        &self,
        group: &ActorGroupId,
        plan_id: u128,
    ) -> Result<Option<RebalancePlan>, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::get_plan(self, group, plan_id).await
    }
    async fn get_claim(&self, key: &PlacementSlotKey) -> Result<Option<LeasedClaim>, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::get_claim(self, key).await
    }
    async fn list_slots(&self, group: &ActorGroupId) -> Result<Vec<PlacementSlot>, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::list_slots(self, group).await
    }
    async fn list_plans(&self, group: &ActorGroupId) -> Result<Vec<RebalancePlan>, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::list_plans(self, group).await
    }
    async fn list_claims(&self, group: &ActorGroupId) -> Result<Vec<LeasedClaim>, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::list_claims(self, group).await
    }
    async fn list_slots_page(
        &self,
        group: &ActorGroupId,
        states: &[PlacementSlotState],
        cursor: Option<&PageCursor>,
        limit: usize,
    ) -> Result<StorePage<PlacementSlot>, StorageError> {
        self.bound_epoch()?;
        self.list_slots_page_inner(group, states, cursor, limit)
            .await
    }
    async fn list_plans_page(
        &self,
        group: &ActorGroupId,
        cursor: Option<&PageCursor>,
        limit: usize,
    ) -> Result<StorePage<RebalancePlan>, StorageError> {
        self.bound_epoch()?;
        self.list_plans_page_inner(group, cursor, limit).await
    }
    async fn list_claims_page(
        &self,
        group: &ActorGroupId,
        cursor: Option<&PageCursor>,
        limit: usize,
    ) -> Result<StorePage<LeasedClaim>, StorageError> {
        self.bound_epoch()?;
        self.list_claims_page_inner(group, cursor, limit).await
    }
    async fn get_automatic_settings(
        &self,
        group: &ActorGroupId,
    ) -> Result<Option<AutomaticBalanceSettings>, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::get_automatic_settings(self, group).await
    }
    async fn get_admin_operation(
        &self,
        group: &ActorGroupId,
        operation_id: &str,
    ) -> Result<Option<AdminOperationRecord>, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::get_admin_operation(self, group, operation_id).await
    }
    async fn list_admin_operations(
        &self,
        group: &ActorGroupId,
    ) -> Result<Vec<AdminOperationRecord>, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::list_admin_operations(self, group).await
    }
    async fn create_plan(
        &self,
        guard: &GroupLeaderGuard,
        request: CreatePlan,
    ) -> Result<PlanCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::create_plan(self, guard, request).await
    }
    async fn update_plan(
        &self,
        guard: &GroupLeaderGuard,
        request: UpdatePlan,
    ) -> Result<PlanCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::update_plan(self, guard, request).await
    }
    async fn delete_plan(
        &self,
        guard: &GroupLeaderGuard,
        request: DeletePlan,
    ) -> Result<PlanCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::delete_plan(self, guard, request).await
    }
    async fn transition_slot(
        &self,
        guard: &GroupLeaderGuard,
        request: TransitionSlot,
    ) -> Result<SlotCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::transition_slot(self, guard, request).await
    }
    async fn allocate_initial(
        &self,
        guard: &GroupLeaderGuard,
        request: AllocateInitial,
    ) -> Result<AuthorityCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::allocate_initial(self, guard, request).await
    }
    async fn activate_authority(
        &self,
        guard: &GroupLeaderGuard,
        request: ActivateAuthority,
    ) -> Result<SlotCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::activate_authority(self, guard, request).await
    }
    async fn reserve_move(
        &self,
        guard: &GroupLeaderGuard,
        request: ReserveMove,
    ) -> Result<MoveCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::reserve_move(self, guard, request).await
    }
    async fn reserve_handoff(
        &self,
        guard: &GroupLeaderGuard,
        request: ReserveHandoff,
    ) -> Result<SlotCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::reserve_handoff(self, guard, request).await
    }
    async fn fence_authority(
        &self,
        guard: &GroupLeaderGuard,
        request: FenceAuthority,
    ) -> Result<SlotCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::fence_authority(self, guard, request).await
    }
    async fn fence_missing_authority(
        &self,
        guard: &GroupLeaderGuard,
        request: FenceMissingAuthority,
    ) -> Result<SlotCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::fence_missing_authority(self, guard, request).await
    }
    async fn install_authority(
        &self,
        guard: &GroupLeaderGuard,
        request: InstallAuthority,
    ) -> Result<AuthorityCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::install_authority(self, guard, request).await
    }
    async fn adopt_authority(
        &self,
        guard: &GroupLeaderGuard,
        request: AdoptAuthority,
    ) -> Result<LeasedClaim, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::adopt_authority(self, guard, request).await
    }
    async fn complete_move(
        &self,
        guard: &GroupLeaderGuard,
        request: CompleteMove,
    ) -> Result<MoveCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::complete_move(self, guard, request).await
    }
    async fn commit_automatic_settings(
        &self,
        guard: &GroupLeaderGuard,
        request: CommitAutomaticSettings,
    ) -> Result<AutomaticBalanceSettings, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::commit_automatic_settings(self, guard, request).await
    }
    async fn create_plan_with_operation(
        &self,
        guard: &GroupLeaderGuard,
        request: CreatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::create_plan_with_operation(self, guard, request).await
    }
    async fn update_plan_with_operation(
        &self,
        guard: &GroupLeaderGuard,
        request: UpdatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::update_plan_with_operation(self, guard, request).await
    }
    async fn record_admin_operation(
        &self,
        guard: &GroupLeaderGuard,
        request: RecordAdminOperation,
    ) -> Result<AdminOperationRecord, StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::record_admin_operation(self, guard, request).await
    }
    async fn compact_admin_operations(
        &self,
        guard: &GroupLeaderGuard,
        request: CompactAdminOperations,
    ) -> Result<(), StorageError> {
        self.bound_epoch()?;
        EtcdCoordinationStore::compact_admin_operations(self, guard, request).await
    }
}
