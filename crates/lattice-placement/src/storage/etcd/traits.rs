use std::time::Duration;

use async_trait::async_trait;
use lattice_core::actor_ref::PlacementDomainId;
use lattice_core::coordinator::CoordinatorScope;

use super::{EtcdPlacementStore, parse_revision_value};
use crate::coordinator::{
    DomainMemberRecord, LeaderRecord, MemberRecord, MembershipLeaderGuard, PlacementLeaderGuard,
    SingletonConfig,
};
use crate::plan::RebalancePlan;
use crate::region::EntityConfig;
use crate::storage::domain::{
    ActivateAuthority, AdminOperationRecord, AdoptAuthority, AllocateInitial, AuthorityCommit,
    AutomaticBalanceSettings, CommitAutomaticSettings, CompactAdminOperations, CompleteMove,
    CreateDomainMember, CreateMember, CreatePlan, CreatePlanWithOperation, DeletePlan,
    DomainMemberCommit, DurableStorageLimits, EntityConfigCommit, FenceAuthority,
    FenceMissingAuthority, InstallAuthority, LeasedClaim, MemberCommit, MoveCommit, PlanCommit,
    PutEntityConfig, PutSingletonConfig, RecordAdminOperation, RemoveDomainMember, RemoveMember,
    ReserveHandoff, ReserveMove, SingletonConfigCommit, SlotCommit, TransitionSlot,
    UpdateDomainMember, UpdateMember, UpdatePlan, UpdatePlanWithOperation,
};
use crate::storage::{
    CoordinatorLeaseStore, MembershipStore, PlacementDomainStore, ScopedElectionStore, StorageError,
};
use crate::types::{PlacementSlot, PlacementSlotKey, Revision};

#[async_trait]
impl CoordinatorLeaseStore for EtcdPlacementStore {
    async fn ensure_schema_generation(&self) -> Result<(), StorageError> {
        EtcdPlacementStore::ensure_schema_generation(self).await
    }
    async fn grant_lease(&self, ttl: Duration) -> Result<i64, StorageError> {
        EtcdPlacementStore::grant_lease(self, ttl).await
    }
    async fn keep_lease_alive(&self, lease_id: i64) -> Result<(), StorageError> {
        EtcdPlacementStore::keep_lease_alive(self, lease_id).await
    }
    async fn revoke_lease(&self, lease_id: i64) -> Result<(), StorageError> {
        EtcdPlacementStore::revoke_lease(self, lease_id).await
    }
    async fn lease_time_to_live(&self, lease_id: i64) -> Result<Option<Duration>, StorageError> {
        EtcdPlacementStore::lease_time_to_live(self, lease_id).await
    }
}

#[async_trait]
impl ScopedElectionStore for EtcdPlacementStore {
    async fn campaign_leader(
        &self,
        leader: &LeaderRecord,
        lease_id: i64,
    ) -> Result<bool, StorageError> {
        EtcdPlacementStore::campaign_leader(self, leader, lease_id).await
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
impl MembershipStore for EtcdPlacementStore {
    async fn get_membership_revision(&self) -> Result<Revision, StorageError> {
        self.get_membership_revision_inner().await
    }
    async fn get_member(&self, node_id: &str) -> Result<Option<MemberRecord>, StorageError> {
        EtcdPlacementStore::get_member(self, node_id).await
    }
    async fn list_members(&self) -> Result<Vec<MemberRecord>, StorageError> {
        EtcdPlacementStore::list_members(self).await
    }
    async fn create_member(
        &self,
        guard: &MembershipLeaderGuard,
        request: CreateMember,
    ) -> Result<MemberCommit, StorageError> {
        EtcdPlacementStore::create_member(self, guard, request).await
    }
    async fn update_member(
        &self,
        guard: &MembershipLeaderGuard,
        request: UpdateMember,
    ) -> Result<MemberCommit, StorageError> {
        EtcdPlacementStore::update_member(self, guard, request).await
    }
    async fn remove_member(
        &self,
        guard: &MembershipLeaderGuard,
        request: RemoveMember,
    ) -> Result<MemberCommit, StorageError> {
        EtcdPlacementStore::remove_member(self, guard, request).await
    }
}

#[async_trait]
impl PlacementDomainStore for EtcdPlacementStore {
    fn durable_limits(&self, _domain: &PlacementDomainId) -> DurableStorageLimits {
        self.durable_limits_inner()
    }
    async fn get_placement_revision(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Revision, StorageError> {
        let key = self.scope_key(
            &CoordinatorScope::Placement(domain.clone()),
            "state_revision",
        );
        self.read_raw(&key)
            .await?
            .map(|(bytes, _, _)| parse_revision_value(&bytes))
            .transpose()
            .map(|revision| revision.unwrap_or_else(|| Revision::new(1).expect("one is valid")))
    }
    async fn get_domain_member(
        &self,
        domain: &PlacementDomainId,
        node_id: &str,
    ) -> Result<Option<DomainMemberRecord>, StorageError> {
        EtcdPlacementStore::get_domain_member(self, domain, node_id).await
    }
    async fn list_domain_members(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<DomainMemberRecord>, StorageError> {
        EtcdPlacementStore::list_domain_members(self, domain).await
    }
    async fn create_domain_member(
        &self,
        guard: &PlacementLeaderGuard,
        request: CreateDomainMember,
    ) -> Result<DomainMemberCommit, StorageError> {
        EtcdPlacementStore::create_domain_member(self, guard, request).await
    }
    async fn update_domain_member(
        &self,
        guard: &PlacementLeaderGuard,
        request: UpdateDomainMember,
    ) -> Result<DomainMemberCommit, StorageError> {
        EtcdPlacementStore::update_domain_member(self, guard, request).await
    }
    async fn remove_domain_member(
        &self,
        guard: &PlacementLeaderGuard,
        request: RemoveDomainMember,
    ) -> Result<DomainMemberCommit, StorageError> {
        EtcdPlacementStore::remove_domain_member(self, guard, request).await
    }
    async fn get_entity_config(
        &self,
        domain: &PlacementDomainId,
        entity_type: &lattice_core::actor_ref::EntityType,
    ) -> Result<Option<EntityConfig>, StorageError> {
        self.get_json_key(&self.entity_config_key(domain, entity_type))
            .await
    }
    async fn list_entity_configs(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<EntityConfig>, StorageError> {
        self.list_json(
            &format!("domains/{}/entity_types/", domain.as_str()),
            self.limits.maximum_entity_configs,
        )
        .await
    }
    async fn put_entity_config(
        &self,
        guard: &PlacementLeaderGuard,
        request: PutEntityConfig,
    ) -> Result<EntityConfigCommit, StorageError> {
        EtcdPlacementStore::put_entity_config(self, guard, request).await
    }
    async fn get_singleton_config(
        &self,
        domain: &PlacementDomainId,
        kind: &lattice_core::actor_ref::SingletonKind,
    ) -> Result<Option<SingletonConfig>, StorageError> {
        self.get_json_key(&self.singleton_config_key(domain, kind))
            .await
    }
    async fn list_singleton_configs(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<SingletonConfig>, StorageError> {
        self.list_json(
            &format!("domains/{}/singleton_types/", domain.as_str()),
            self.limits.maximum_singleton_configs,
        )
        .await
    }
    async fn put_singleton_config(
        &self,
        guard: &PlacementLeaderGuard,
        request: PutSingletonConfig,
    ) -> Result<SingletonConfigCommit, StorageError> {
        EtcdPlacementStore::put_singleton_config(self, guard, request).await
    }
    async fn get_slot(
        &self,
        key: &PlacementSlotKey,
    ) -> Result<Option<PlacementSlot>, StorageError> {
        EtcdPlacementStore::get_slot(self, key).await
    }
    async fn get_plan(
        &self,
        domain: &PlacementDomainId,
        plan_id: u128,
    ) -> Result<Option<RebalancePlan>, StorageError> {
        EtcdPlacementStore::get_plan(self, domain, plan_id).await
    }
    async fn get_claim(&self, key: &PlacementSlotKey) -> Result<Option<LeasedClaim>, StorageError> {
        EtcdPlacementStore::get_claim(self, key).await
    }
    async fn list_slots(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<PlacementSlot>, StorageError> {
        EtcdPlacementStore::list_slots(self, domain).await
    }
    async fn list_plans(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<RebalancePlan>, StorageError> {
        EtcdPlacementStore::list_plans(self, domain).await
    }
    async fn list_claims(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<LeasedClaim>, StorageError> {
        EtcdPlacementStore::list_claims(self, domain).await
    }
    async fn get_automatic_settings(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Option<AutomaticBalanceSettings>, StorageError> {
        EtcdPlacementStore::get_automatic_settings(self, domain).await
    }
    async fn get_admin_operation(
        &self,
        domain: &PlacementDomainId,
        operation_id: &str,
    ) -> Result<Option<AdminOperationRecord>, StorageError> {
        EtcdPlacementStore::get_admin_operation(self, domain, operation_id).await
    }
    async fn list_admin_operations(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<AdminOperationRecord>, StorageError> {
        EtcdPlacementStore::list_admin_operations(self, domain).await
    }
    async fn create_plan(
        &self,
        guard: &PlacementLeaderGuard,
        request: CreatePlan,
    ) -> Result<PlanCommit, StorageError> {
        EtcdPlacementStore::create_plan(self, guard, request).await
    }
    async fn update_plan(
        &self,
        guard: &PlacementLeaderGuard,
        request: UpdatePlan,
    ) -> Result<PlanCommit, StorageError> {
        EtcdPlacementStore::update_plan(self, guard, request).await
    }
    async fn delete_plan(
        &self,
        guard: &PlacementLeaderGuard,
        request: DeletePlan,
    ) -> Result<PlanCommit, StorageError> {
        EtcdPlacementStore::delete_plan(self, guard, request).await
    }
    async fn transition_slot(
        &self,
        guard: &PlacementLeaderGuard,
        request: TransitionSlot,
    ) -> Result<SlotCommit, StorageError> {
        EtcdPlacementStore::transition_slot(self, guard, request).await
    }
    async fn allocate_initial(
        &self,
        guard: &PlacementLeaderGuard,
        request: AllocateInitial,
    ) -> Result<AuthorityCommit, StorageError> {
        EtcdPlacementStore::allocate_initial(self, guard, request).await
    }
    async fn activate_authority(
        &self,
        guard: &PlacementLeaderGuard,
        request: ActivateAuthority,
    ) -> Result<SlotCommit, StorageError> {
        EtcdPlacementStore::activate_authority(self, guard, request).await
    }
    async fn reserve_move(
        &self,
        guard: &PlacementLeaderGuard,
        request: ReserveMove,
    ) -> Result<MoveCommit, StorageError> {
        EtcdPlacementStore::reserve_move(self, guard, request).await
    }
    async fn reserve_handoff(
        &self,
        guard: &PlacementLeaderGuard,
        request: ReserveHandoff,
    ) -> Result<SlotCommit, StorageError> {
        EtcdPlacementStore::reserve_handoff(self, guard, request).await
    }
    async fn fence_authority(
        &self,
        guard: &PlacementLeaderGuard,
        request: FenceAuthority,
    ) -> Result<SlotCommit, StorageError> {
        EtcdPlacementStore::fence_authority(self, guard, request).await
    }
    async fn fence_missing_authority(
        &self,
        guard: &PlacementLeaderGuard,
        request: FenceMissingAuthority,
    ) -> Result<SlotCommit, StorageError> {
        EtcdPlacementStore::fence_missing_authority(self, guard, request).await
    }
    async fn install_authority(
        &self,
        guard: &PlacementLeaderGuard,
        request: InstallAuthority,
    ) -> Result<AuthorityCommit, StorageError> {
        EtcdPlacementStore::install_authority(self, guard, request).await
    }
    async fn adopt_authority(
        &self,
        guard: &PlacementLeaderGuard,
        request: AdoptAuthority,
    ) -> Result<AuthorityCommit, StorageError> {
        EtcdPlacementStore::adopt_authority(self, guard, request).await
    }
    async fn complete_move(
        &self,
        guard: &PlacementLeaderGuard,
        request: CompleteMove,
    ) -> Result<MoveCommit, StorageError> {
        EtcdPlacementStore::complete_move(self, guard, request).await
    }
    async fn commit_automatic_settings(
        &self,
        guard: &PlacementLeaderGuard,
        request: CommitAutomaticSettings,
    ) -> Result<AutomaticBalanceSettings, StorageError> {
        EtcdPlacementStore::commit_automatic_settings(self, guard, request).await
    }
    async fn create_plan_with_operation(
        &self,
        guard: &PlacementLeaderGuard,
        request: CreatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError> {
        EtcdPlacementStore::create_plan_with_operation(self, guard, request).await
    }
    async fn update_plan_with_operation(
        &self,
        guard: &PlacementLeaderGuard,
        request: UpdatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError> {
        EtcdPlacementStore::update_plan_with_operation(self, guard, request).await
    }
    async fn record_admin_operation(
        &self,
        guard: &PlacementLeaderGuard,
        request: RecordAdminOperation,
    ) -> Result<AdminOperationRecord, StorageError> {
        EtcdPlacementStore::record_admin_operation(self, guard, request).await
    }
    async fn compact_admin_operations(
        &self,
        guard: &PlacementLeaderGuard,
        request: CompactAdminOperations,
    ) -> Result<(), StorageError> {
        EtcdPlacementStore::compact_admin_operations(self, guard, request).await
    }
}
