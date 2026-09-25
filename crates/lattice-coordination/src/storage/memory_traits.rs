use std::time::Duration;

use async_trait::async_trait;
use lattice_model::{
    cluster::CoordinatorScope,
    cluster::{ActorGroupId, EntityType, SingletonKind},
};

use super::{
    ActorGroupStore, CoordinatorLeaseStore, InMemoryCoordinationStore, MembershipStore,
    ScopedElectionStore, StorageError, current_revision, initial_revision,
    page::{PageCursor, StorePage},
    records::{
        ActivateAuthority, AdminOperationRecord, AdoptAuthority, AllocateInitial, AuthorityCommit,
        AutomaticBalanceSettings, CommitAutomaticSettings, CompactAdminOperations, CompleteMove,
        CreateGroupMember, CreateMember, CreatePlan, CreatePlanWithOperation, DeletePlan,
        DurableStorageLimits, EntityConfigCommit, FenceAuthority, FenceMissingAuthority,
        GroupMemberCommit, InstallAuthority, LeasedClaim, MemberCommit, MoveCommit, PlanCommit,
        PutEntityConfig, PutSingletonConfig, RecordAdminOperation, RemoveExpiredMember,
        RemoveGroupMember, RemoveMember, ReserveHandoff, ReserveMove, SingletonConfigCommit,
        SlotCommit, TransitionSlot, UpdateGroupMember, UpdateMember, UpdatePlan,
        UpdatePlanWithOperation,
    },
    set_revision, validate_guard,
};
use crate::{
    coordinator::{
        ClusterLeaderGuard, ExactLeaderGuard, GroupLeaderGuard, GroupMemberRecord, LeaderRecord,
        MemberRecord, SingletonConfig,
    },
    plan::RebalancePlan,
    region::EntityConfig,
    types::{PlacementSlot, PlacementSlotKey, PlacementSlotState, PlacementVersion, Revision},
};

#[async_trait]
impl CoordinatorLeaseStore for InMemoryCoordinationStore {
    async fn ensure_framework(&self) -> Result<(), StorageError> {
        InMemoryCoordinationStore::ensure_framework(self).await
    }

    async fn grant_lease(&self, ttl: Duration) -> Result<i64, StorageError> {
        InMemoryCoordinationStore::grant_lease(self, ttl).await
    }

    async fn keep_lease_alive(&self, lease_id: i64) -> Result<(), StorageError> {
        InMemoryCoordinationStore::keep_lease_alive(self, lease_id).await
    }

    async fn revoke_lease(&self, lease_id: i64) -> Result<(), StorageError> {
        InMemoryCoordinationStore::revoke_lease(self, lease_id).await
    }

    async fn lease_time_to_live(&self, lease_id: i64) -> Result<Option<Duration>, StorageError> {
        InMemoryCoordinationStore::lease_time_to_live(self, lease_id).await
    }
}

#[async_trait]
impl ScopedElectionStore for InMemoryCoordinationStore {
    async fn campaign_leader(
        &self,
        leader: &LeaderRecord,
        lease_id: i64,
    ) -> Result<bool, StorageError> {
        InMemoryCoordinationStore::campaign_leader(self, leader, lease_id).await
    }

    async fn get_leader(
        &self,
        scope: &CoordinatorScope,
    ) -> Result<Option<LeaderRecord>, StorageError> {
        self.get_leader_inner(scope).await
    }

    async fn get_leader_term(&self, scope: &CoordinatorScope) -> Result<u64, StorageError> {
        self.get_leader_term_inner(scope).await
    }
}

#[async_trait]
impl MembershipStore for InMemoryCoordinationStore {
    async fn get_membership_revision(&self) -> Result<Revision, StorageError> {
        self.get_membership_revision_inner().await
    }

    async fn get_member(&self, node_id: &str) -> Result<Option<MemberRecord>, StorageError> {
        InMemoryCoordinationStore::get_member(self, node_id).await
    }

    async fn list_members(&self) -> Result<Vec<MemberRecord>, StorageError> {
        InMemoryCoordinationStore::list_members(self).await
    }

    async fn list_members_page(
        &self,
        cursor: Option<&PageCursor>,
        limit: usize,
    ) -> Result<StorePage<MemberRecord>, StorageError> {
        self.list_members_page_inner(cursor, limit).await
    }

    async fn create_member(
        &self,
        guard: &ClusterLeaderGuard,
        request: CreateMember,
    ) -> Result<MemberCommit, StorageError> {
        InMemoryCoordinationStore::create_member(self, guard, request).await
    }

    async fn update_member(
        &self,
        guard: &ClusterLeaderGuard,
        request: UpdateMember,
    ) -> Result<MemberCommit, StorageError> {
        InMemoryCoordinationStore::update_member(self, guard, request).await
    }

    async fn remove_member(
        &self,
        guard: &ClusterLeaderGuard,
        request: RemoveMember,
    ) -> Result<MemberCommit, StorageError> {
        InMemoryCoordinationStore::remove_member(self, guard, request).await
    }

    async fn remove_expired_member(
        &self,
        guard: &ClusterLeaderGuard,
        request: RemoveExpiredMember,
    ) -> Result<MemberCommit, StorageError> {
        InMemoryCoordinationStore::remove_expired_member(self, guard, request).await
    }
}

#[async_trait]
impl ActorGroupStore for InMemoryCoordinationStore {
    fn durable_limits(&self, _group: &ActorGroupId) -> DurableStorageLimits {
        self.durable_limits_inner()
    }

    async fn get_placement_revision(&self, group: &ActorGroupId) -> Result<Revision, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .placement_revisions
            .get(group)
            .copied()
            .unwrap_or_else(initial_revision))
    }

    async fn get_group_member(
        &self,
        group: &ActorGroupId,
        node_id: &str,
    ) -> Result<Option<GroupMemberRecord>, StorageError> {
        InMemoryCoordinationStore::get_group_member(self, group, node_id).await
    }

    async fn list_group_members(
        &self,
        group: &ActorGroupId,
    ) -> Result<Vec<GroupMemberRecord>, StorageError> {
        InMemoryCoordinationStore::list_group_members(self, group).await
    }

    async fn create_group_member(
        &self,
        guard: &GroupLeaderGuard,
        request: CreateGroupMember,
    ) -> Result<GroupMemberCommit, StorageError> {
        InMemoryCoordinationStore::create_group_member(self, guard, request).await
    }

    async fn update_group_member(
        &self,
        guard: &GroupLeaderGuard,
        request: UpdateGroupMember,
    ) -> Result<GroupMemberCommit, StorageError> {
        InMemoryCoordinationStore::update_group_member(self, guard, request).await
    }

    async fn remove_group_member(
        &self,
        guard: &GroupLeaderGuard,
        request: RemoveGroupMember,
    ) -> Result<GroupMemberCommit, StorageError> {
        InMemoryCoordinationStore::remove_group_member(self, guard, request).await
    }

    async fn get_entity_config(
        &self,
        group: &ActorGroupId,
        entity_type: &EntityType,
    ) -> Result<Option<EntityConfig>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .entity_configs
            .get(&(group.clone(), entity_type.clone()))
            .cloned())
    }

    async fn list_entity_configs(
        &self,
        group: &ActorGroupId,
    ) -> Result<Vec<EntityConfig>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .entity_configs
            .iter()
            .filter_map(|((stored_group, _), config)| {
                (stored_group == group).then_some(config.clone())
            })
            .collect())
    }

    async fn put_entity_config(
        &self,
        guard: &GroupLeaderGuard,
        request: PutEntityConfig,
    ) -> Result<EntityConfigCommit, StorageError> {
        let CoordinatorScope::Group(group) = guard.scope() else {
            return Err(StorageError::InvalidRecord);
        };
        if &request.config.group != group || request.config.validate().is_err() {
            return Err(StorageError::InvalidRecord);
        }
        let key = (group.clone(), request.config.entity_type.clone());
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        if state.entity_configs.get(&key).cloned() != request.expected {
            return Err(StorageError::CompareFailed);
        }
        if request.expected.is_none()
            && state
                .entity_configs
                .keys()
                .filter(|(stored_group, _)| stored_group == group)
                .count()
                == self.maximum_members
        {
            return Err(StorageError::Capacity);
        }
        let revision = current_revision(&state, guard.scope())
            .next()
            .map_err(|_| StorageError::CounterExhausted)?;
        state.entity_configs.insert(key, request.config.clone());
        set_revision(&mut state, guard.scope(), revision);
        Ok(EntityConfigCommit {
            config: request.config,
            version: PlacementVersion::new(group.clone(), guard.term(), revision),
        })
    }

    async fn get_singleton_config(
        &self,
        group: &ActorGroupId,
        kind: &SingletonKind,
    ) -> Result<Option<SingletonConfig>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .singleton_configs
            .get(&(group.clone(), kind.clone()))
            .cloned())
    }

    async fn list_singleton_configs(
        &self,
        group: &ActorGroupId,
    ) -> Result<Vec<SingletonConfig>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .singleton_configs
            .iter()
            .filter_map(|((stored_group, _), config)| {
                (stored_group == group).then_some(config.clone())
            })
            .collect())
    }

    async fn put_singleton_config(
        &self,
        guard: &GroupLeaderGuard,
        request: PutSingletonConfig,
    ) -> Result<SingletonConfigCommit, StorageError> {
        let CoordinatorScope::Group(group) = guard.scope() else {
            return Err(StorageError::InvalidRecord);
        };
        if &request.config.group != group || !request.config.validate() {
            return Err(StorageError::InvalidRecord);
        }
        let key = (group.clone(), request.config.kind.clone());
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        if state.singleton_configs.get(&key).cloned() != request.expected {
            return Err(StorageError::CompareFailed);
        }
        if request.expected.is_none()
            && state
                .singleton_configs
                .keys()
                .filter(|(stored_group, _)| stored_group == group)
                .count()
                == self.maximum_members
        {
            return Err(StorageError::Capacity);
        }
        let revision = current_revision(&state, guard.scope())
            .next()
            .map_err(|_| StorageError::CounterExhausted)?;
        state.singleton_configs.insert(key, request.config.clone());
        set_revision(&mut state, guard.scope(), revision);
        Ok(SingletonConfigCommit {
            config: request.config,
            version: PlacementVersion::new(group.clone(), guard.term(), revision),
        })
    }

    async fn get_slot(
        &self,
        key: &PlacementSlotKey,
    ) -> Result<Option<PlacementSlot>, StorageError> {
        InMemoryCoordinationStore::get_slot(self, key).await
    }

    async fn get_plan(
        &self,
        group: &ActorGroupId,
        plan_id: u128,
    ) -> Result<Option<RebalancePlan>, StorageError> {
        InMemoryCoordinationStore::get_plan(self, group, plan_id).await
    }

    async fn get_claim(&self, key: &PlacementSlotKey) -> Result<Option<LeasedClaim>, StorageError> {
        InMemoryCoordinationStore::get_claim(self, key).await
    }

    async fn list_slots(&self, group: &ActorGroupId) -> Result<Vec<PlacementSlot>, StorageError> {
        InMemoryCoordinationStore::list_slots(self, group).await
    }

    async fn list_plans(&self, group: &ActorGroupId) -> Result<Vec<RebalancePlan>, StorageError> {
        InMemoryCoordinationStore::list_plans(self, group).await
    }

    async fn list_claims(&self, group: &ActorGroupId) -> Result<Vec<LeasedClaim>, StorageError> {
        InMemoryCoordinationStore::list_claims(self, group).await
    }

    async fn list_slots_page(
        &self,
        group: &ActorGroupId,
        states: &[PlacementSlotState],
        cursor: Option<&PageCursor>,
        limit: usize,
    ) -> Result<StorePage<PlacementSlot>, StorageError> {
        self.list_slots_page_inner(group, states, cursor, limit)
            .await
    }

    async fn list_plans_page(
        &self,
        group: &ActorGroupId,
        cursor: Option<&PageCursor>,
        limit: usize,
    ) -> Result<StorePage<RebalancePlan>, StorageError> {
        self.list_plans_page_inner(group, cursor, limit).await
    }

    async fn list_claims_page(
        &self,
        group: &ActorGroupId,
        cursor: Option<&PageCursor>,
        limit: usize,
    ) -> Result<StorePage<LeasedClaim>, StorageError> {
        self.list_claims_page_inner(group, cursor, limit).await
    }

    async fn get_automatic_settings(
        &self,
        group: &ActorGroupId,
    ) -> Result<Option<AutomaticBalanceSettings>, StorageError> {
        InMemoryCoordinationStore::get_automatic_settings(self, group).await
    }

    async fn get_admin_operation(
        &self,
        group: &ActorGroupId,
        operation_id: &str,
    ) -> Result<Option<AdminOperationRecord>, StorageError> {
        InMemoryCoordinationStore::get_admin_operation(self, group, operation_id).await
    }

    async fn list_admin_operations(
        &self,
        group: &ActorGroupId,
    ) -> Result<Vec<AdminOperationRecord>, StorageError> {
        Ok(InMemoryCoordinationStore::list_admin_operations(self)
            .await?
            .into_iter()
            .filter(|operation| &operation.version.group == group)
            .collect())
    }

    async fn create_plan(
        &self,
        guard: &GroupLeaderGuard,
        request: CreatePlan,
    ) -> Result<PlanCommit, StorageError> {
        InMemoryCoordinationStore::create_plan(self, guard, request).await
    }
    async fn update_plan(
        &self,
        guard: &GroupLeaderGuard,
        request: UpdatePlan,
    ) -> Result<PlanCommit, StorageError> {
        InMemoryCoordinationStore::update_plan(self, guard, request).await
    }
    async fn delete_plan(
        &self,
        guard: &GroupLeaderGuard,
        request: DeletePlan,
    ) -> Result<PlanCommit, StorageError> {
        InMemoryCoordinationStore::delete_plan(self, guard, request).await
    }
    async fn transition_slot(
        &self,
        guard: &GroupLeaderGuard,
        request: TransitionSlot,
    ) -> Result<SlotCommit, StorageError> {
        InMemoryCoordinationStore::transition_slot(self, guard, request).await
    }
    async fn allocate_initial(
        &self,
        guard: &GroupLeaderGuard,
        request: AllocateInitial,
    ) -> Result<AuthorityCommit, StorageError> {
        InMemoryCoordinationStore::allocate_initial(self, guard, request).await
    }
    async fn activate_authority(
        &self,
        guard: &GroupLeaderGuard,
        request: ActivateAuthority,
    ) -> Result<SlotCommit, StorageError> {
        InMemoryCoordinationStore::activate_authority(self, guard, request).await
    }
    async fn reserve_move(
        &self,
        guard: &GroupLeaderGuard,
        request: ReserveMove,
    ) -> Result<MoveCommit, StorageError> {
        InMemoryCoordinationStore::reserve_move(self, guard, request).await
    }
    async fn reserve_handoff(
        &self,
        guard: &GroupLeaderGuard,
        request: ReserveHandoff,
    ) -> Result<SlotCommit, StorageError> {
        InMemoryCoordinationStore::reserve_handoff(self, guard, request).await
    }
    async fn fence_authority(
        &self,
        guard: &GroupLeaderGuard,
        request: FenceAuthority,
    ) -> Result<SlotCommit, StorageError> {
        InMemoryCoordinationStore::fence_authority(self, guard, request).await
    }
    async fn fence_missing_authority(
        &self,
        guard: &GroupLeaderGuard,
        request: FenceMissingAuthority,
    ) -> Result<SlotCommit, StorageError> {
        self.fence_missing_authority_inner(guard, request).await
    }
    async fn install_authority(
        &self,
        guard: &GroupLeaderGuard,
        request: InstallAuthority,
    ) -> Result<AuthorityCommit, StorageError> {
        InMemoryCoordinationStore::install_authority(self, guard, request).await
    }
    async fn adopt_authority(
        &self,
        guard: &GroupLeaderGuard,
        request: AdoptAuthority,
    ) -> Result<LeasedClaim, StorageError> {
        InMemoryCoordinationStore::adopt_authority(self, guard, request).await
    }
    async fn complete_move(
        &self,
        guard: &GroupLeaderGuard,
        request: CompleteMove,
    ) -> Result<MoveCommit, StorageError> {
        InMemoryCoordinationStore::complete_move(self, guard, request).await
    }
    async fn commit_automatic_settings(
        &self,
        guard: &GroupLeaderGuard,
        request: CommitAutomaticSettings,
    ) -> Result<AutomaticBalanceSettings, StorageError> {
        InMemoryCoordinationStore::commit_automatic_settings(self, guard, request).await
    }
    async fn create_plan_with_operation(
        &self,
        guard: &GroupLeaderGuard,
        request: CreatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError> {
        InMemoryCoordinationStore::create_plan_with_operation(self, guard, request).await
    }
    async fn update_plan_with_operation(
        &self,
        guard: &GroupLeaderGuard,
        request: UpdatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError> {
        InMemoryCoordinationStore::update_plan_with_operation(self, guard, request).await
    }
    async fn record_admin_operation(
        &self,
        guard: &GroupLeaderGuard,
        request: RecordAdminOperation,
    ) -> Result<AdminOperationRecord, StorageError> {
        InMemoryCoordinationStore::record_admin_operation(self, guard, request).await
    }
    async fn compact_admin_operations(
        &self,
        guard: &GroupLeaderGuard,
        request: CompactAdminOperations,
    ) -> Result<(), StorageError> {
        InMemoryCoordinationStore::compact_admin_operations(self, guard, request).await
    }
}
