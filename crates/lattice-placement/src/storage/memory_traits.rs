use super::domain::{
    ActivateAuthority, AdminOperationRecord, AdoptAuthority, AllocateInitial, AuthorityCommit,
    AutomaticBalanceSettings, CommitAutomaticSettings, CompactAdminOperations, CompleteMove,
    CreateDomainMember, CreateMember, CreatePlan, CreatePlanWithOperation, DeletePlan,
    DomainMemberCommit, DurableStorageLimits, EntityConfigCommit, FenceAuthority,
    FenceMissingAuthority, InstallAuthority, LeasedClaim, MemberCommit, MoveCommit, PlanCommit,
    PutEntityConfig, PutSingletonConfig, RecordAdminOperation, RemoveDomainMember, RemoveMember,
    ReserveHandoff, ReserveMove, SingletonConfigCommit, SlotCommit, TransitionSlot,
    UpdateDomainMember, UpdateMember, UpdatePlan, UpdatePlanWithOperation,
};
use super::{
    CoordinatorLeaseStore, InMemoryPlacementStore, MembershipStore, PlacementDomainStore,
    ScopedElectionStore, StorageError, current_revision, initial_revision, set_revision,
    validate_guard,
};
use crate::coordinator::{
    DomainMemberRecord, ExactLeaderGuard, LeaderRecord, MemberRecord, MembershipLeaderGuard,
    PlacementLeaderGuard, SingletonConfig,
};
use crate::plan::RebalancePlan;
use crate::region::EntityConfig;
use crate::types::{PlacementSlot, PlacementSlotKey, PlacementVersion, Revision};
use async_trait::async_trait;
use lattice_core::actor_ref::PlacementDomainId;
use lattice_core::coordinator::CoordinatorScope;

#[async_trait]
impl CoordinatorLeaseStore for InMemoryPlacementStore {
    async fn ensure_schema_generation(&self) -> Result<(), StorageError> {
        InMemoryPlacementStore::ensure_schema_generation(self).await
    }

    async fn grant_lease(&self, ttl: std::time::Duration) -> Result<i64, StorageError> {
        InMemoryPlacementStore::grant_lease(self, ttl).await
    }

    async fn keep_lease_alive(&self, lease_id: i64) -> Result<(), StorageError> {
        InMemoryPlacementStore::keep_lease_alive(self, lease_id).await
    }

    async fn revoke_lease(&self, lease_id: i64) -> Result<(), StorageError> {
        InMemoryPlacementStore::revoke_lease(self, lease_id).await
    }

    async fn lease_time_to_live(
        &self,
        lease_id: i64,
    ) -> Result<Option<std::time::Duration>, StorageError> {
        InMemoryPlacementStore::lease_time_to_live(self, lease_id).await
    }
}

#[async_trait]
impl ScopedElectionStore for InMemoryPlacementStore {
    async fn campaign_leader(
        &self,
        leader: &LeaderRecord,
        lease_id: i64,
    ) -> Result<bool, StorageError> {
        InMemoryPlacementStore::campaign_leader(self, leader, lease_id).await
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
impl MembershipStore for InMemoryPlacementStore {
    async fn get_membership_revision(&self) -> Result<Revision, StorageError> {
        self.get_membership_revision_inner().await
    }

    async fn get_member(&self, node_id: &str) -> Result<Option<MemberRecord>, StorageError> {
        InMemoryPlacementStore::get_member(self, node_id).await
    }

    async fn list_members(&self) -> Result<Vec<MemberRecord>, StorageError> {
        InMemoryPlacementStore::list_members(self).await
    }

    async fn create_member(
        &self,
        guard: &MembershipLeaderGuard,
        request: CreateMember,
    ) -> Result<MemberCommit, StorageError> {
        InMemoryPlacementStore::create_member(self, guard, request).await
    }

    async fn update_member(
        &self,
        guard: &MembershipLeaderGuard,
        request: UpdateMember,
    ) -> Result<MemberCommit, StorageError> {
        InMemoryPlacementStore::update_member(self, guard, request).await
    }

    async fn remove_member(
        &self,
        guard: &MembershipLeaderGuard,
        request: RemoveMember,
    ) -> Result<MemberCommit, StorageError> {
        InMemoryPlacementStore::remove_member(self, guard, request).await
    }
}

#[async_trait]
impl PlacementDomainStore for InMemoryPlacementStore {
    fn durable_limits(&self, _domain: &PlacementDomainId) -> DurableStorageLimits {
        self.durable_limits_inner()
    }

    async fn get_placement_revision(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Revision, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .placement_revisions
            .get(domain)
            .copied()
            .unwrap_or_else(initial_revision))
    }

    async fn get_domain_member(
        &self,
        domain: &PlacementDomainId,
        node_id: &str,
    ) -> Result<Option<DomainMemberRecord>, StorageError> {
        InMemoryPlacementStore::get_domain_member(self, domain, node_id).await
    }

    async fn list_domain_members(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<DomainMemberRecord>, StorageError> {
        InMemoryPlacementStore::list_domain_members(self, domain).await
    }

    async fn create_domain_member(
        &self,
        guard: &PlacementLeaderGuard,
        request: CreateDomainMember,
    ) -> Result<DomainMemberCommit, StorageError> {
        InMemoryPlacementStore::create_domain_member(self, guard, request).await
    }

    async fn update_domain_member(
        &self,
        guard: &PlacementLeaderGuard,
        request: UpdateDomainMember,
    ) -> Result<DomainMemberCommit, StorageError> {
        InMemoryPlacementStore::update_domain_member(self, guard, request).await
    }

    async fn remove_domain_member(
        &self,
        guard: &PlacementLeaderGuard,
        request: RemoveDomainMember,
    ) -> Result<DomainMemberCommit, StorageError> {
        InMemoryPlacementStore::remove_domain_member(self, guard, request).await
    }

    async fn get_entity_config(
        &self,
        domain: &PlacementDomainId,
        entity_type: &lattice_core::actor_ref::EntityType,
    ) -> Result<Option<EntityConfig>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .entity_configs
            .get(&(domain.clone(), entity_type.clone()))
            .cloned())
    }

    async fn list_entity_configs(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<EntityConfig>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .entity_configs
            .iter()
            .filter_map(|((stored_domain, _), config)| {
                (stored_domain == domain).then_some(config.clone())
            })
            .collect())
    }

    async fn put_entity_config(
        &self,
        guard: &PlacementLeaderGuard,
        request: PutEntityConfig,
    ) -> Result<EntityConfigCommit, StorageError> {
        let CoordinatorScope::Placement(domain) = guard.scope() else {
            return Err(StorageError::InvalidRecord);
        };
        if &request.config.domain != domain || request.config.validate().is_err() {
            return Err(StorageError::InvalidRecord);
        }
        let key = (domain.clone(), request.config.entity_type.clone());
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        if state.entity_configs.get(&key).cloned() != request.expected {
            return Err(StorageError::CompareFailed);
        }
        if request.expected.is_none()
            && state
                .entity_configs
                .keys()
                .filter(|(stored_domain, _)| stored_domain == domain)
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
            version: PlacementVersion::new(domain.clone(), guard.term(), revision),
        })
    }

    async fn get_singleton_config(
        &self,
        domain: &PlacementDomainId,
        kind: &lattice_core::actor_ref::SingletonKind,
    ) -> Result<Option<SingletonConfig>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .singleton_configs
            .get(&(domain.clone(), kind.clone()))
            .cloned())
    }

    async fn list_singleton_configs(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<SingletonConfig>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .singleton_configs
            .iter()
            .filter_map(|((stored_domain, _), config)| {
                (stored_domain == domain).then_some(config.clone())
            })
            .collect())
    }

    async fn put_singleton_config(
        &self,
        guard: &PlacementLeaderGuard,
        request: PutSingletonConfig,
    ) -> Result<SingletonConfigCommit, StorageError> {
        let CoordinatorScope::Placement(domain) = guard.scope() else {
            return Err(StorageError::InvalidRecord);
        };
        if &request.config.domain != domain || !request.config.validate() {
            return Err(StorageError::InvalidRecord);
        }
        let key = (domain.clone(), request.config.kind.clone());
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        if state.singleton_configs.get(&key).cloned() != request.expected {
            return Err(StorageError::CompareFailed);
        }
        if request.expected.is_none()
            && state
                .singleton_configs
                .keys()
                .filter(|(stored_domain, _)| stored_domain == domain)
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
            version: PlacementVersion::new(domain.clone(), guard.term(), revision),
        })
    }

    async fn get_slot(
        &self,
        key: &PlacementSlotKey,
    ) -> Result<Option<PlacementSlot>, StorageError> {
        InMemoryPlacementStore::get_slot(self, key).await
    }

    async fn get_plan(
        &self,
        domain: &PlacementDomainId,
        plan_id: u128,
    ) -> Result<Option<RebalancePlan>, StorageError> {
        InMemoryPlacementStore::get_plan(self, domain, plan_id).await
    }

    async fn get_claim(&self, key: &PlacementSlotKey) -> Result<Option<LeasedClaim>, StorageError> {
        InMemoryPlacementStore::get_claim(self, key).await
    }

    async fn list_slots(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<PlacementSlot>, StorageError> {
        InMemoryPlacementStore::list_slots(self, domain).await
    }

    async fn list_plans(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<RebalancePlan>, StorageError> {
        InMemoryPlacementStore::list_plans(self, domain).await
    }

    async fn list_claims(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<LeasedClaim>, StorageError> {
        InMemoryPlacementStore::list_claims(self, domain).await
    }

    async fn get_automatic_settings(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Option<AutomaticBalanceSettings>, StorageError> {
        InMemoryPlacementStore::get_automatic_settings(self, domain).await
    }

    async fn get_admin_operation(
        &self,
        domain: &PlacementDomainId,
        operation_id: &str,
    ) -> Result<Option<AdminOperationRecord>, StorageError> {
        InMemoryPlacementStore::get_admin_operation(self, domain, operation_id).await
    }

    async fn list_admin_operations(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<AdminOperationRecord>, StorageError> {
        Ok(InMemoryPlacementStore::list_admin_operations(self)
            .await?
            .into_iter()
            .filter(|operation| &operation.version.domain == domain)
            .collect())
    }

    async fn create_plan(
        &self,
        guard: &PlacementLeaderGuard,
        request: CreatePlan,
    ) -> Result<PlanCommit, StorageError> {
        InMemoryPlacementStore::create_plan(self, guard, request).await
    }
    async fn update_plan(
        &self,
        guard: &PlacementLeaderGuard,
        request: UpdatePlan,
    ) -> Result<PlanCommit, StorageError> {
        InMemoryPlacementStore::update_plan(self, guard, request).await
    }
    async fn delete_plan(
        &self,
        guard: &PlacementLeaderGuard,
        request: DeletePlan,
    ) -> Result<PlanCommit, StorageError> {
        InMemoryPlacementStore::delete_plan(self, guard, request).await
    }
    async fn transition_slot(
        &self,
        guard: &PlacementLeaderGuard,
        request: TransitionSlot,
    ) -> Result<SlotCommit, StorageError> {
        InMemoryPlacementStore::transition_slot(self, guard, request).await
    }
    async fn allocate_initial(
        &self,
        guard: &PlacementLeaderGuard,
        request: AllocateInitial,
    ) -> Result<AuthorityCommit, StorageError> {
        InMemoryPlacementStore::allocate_initial(self, guard, request).await
    }
    async fn activate_authority(
        &self,
        guard: &PlacementLeaderGuard,
        request: ActivateAuthority,
    ) -> Result<SlotCommit, StorageError> {
        InMemoryPlacementStore::activate_authority(self, guard, request).await
    }
    async fn reserve_move(
        &self,
        guard: &PlacementLeaderGuard,
        request: ReserveMove,
    ) -> Result<MoveCommit, StorageError> {
        InMemoryPlacementStore::reserve_move(self, guard, request).await
    }
    async fn reserve_handoff(
        &self,
        guard: &PlacementLeaderGuard,
        request: ReserveHandoff,
    ) -> Result<SlotCommit, StorageError> {
        InMemoryPlacementStore::reserve_handoff(self, guard, request).await
    }
    async fn fence_authority(
        &self,
        guard: &PlacementLeaderGuard,
        request: FenceAuthority,
    ) -> Result<SlotCommit, StorageError> {
        InMemoryPlacementStore::fence_authority(self, guard, request).await
    }
    async fn fence_missing_authority(
        &self,
        guard: &PlacementLeaderGuard,
        request: FenceMissingAuthority,
    ) -> Result<SlotCommit, StorageError> {
        self.fence_missing_authority_inner(guard, request).await
    }
    async fn install_authority(
        &self,
        guard: &PlacementLeaderGuard,
        request: InstallAuthority,
    ) -> Result<AuthorityCommit, StorageError> {
        InMemoryPlacementStore::install_authority(self, guard, request).await
    }
    async fn adopt_authority(
        &self,
        guard: &PlacementLeaderGuard,
        request: AdoptAuthority,
    ) -> Result<AuthorityCommit, StorageError> {
        InMemoryPlacementStore::adopt_authority(self, guard, request).await
    }
    async fn complete_move(
        &self,
        guard: &PlacementLeaderGuard,
        request: CompleteMove,
    ) -> Result<MoveCommit, StorageError> {
        InMemoryPlacementStore::complete_move(self, guard, request).await
    }
    async fn commit_automatic_settings(
        &self,
        guard: &PlacementLeaderGuard,
        request: CommitAutomaticSettings,
    ) -> Result<AutomaticBalanceSettings, StorageError> {
        InMemoryPlacementStore::commit_automatic_settings(self, guard, request).await
    }
    async fn create_plan_with_operation(
        &self,
        guard: &PlacementLeaderGuard,
        request: CreatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError> {
        InMemoryPlacementStore::create_plan_with_operation(self, guard, request).await
    }
    async fn update_plan_with_operation(
        &self,
        guard: &PlacementLeaderGuard,
        request: UpdatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError> {
        InMemoryPlacementStore::update_plan_with_operation(self, guard, request).await
    }
    async fn record_admin_operation(
        &self,
        guard: &PlacementLeaderGuard,
        request: RecordAdminOperation,
    ) -> Result<AdminOperationRecord, StorageError> {
        InMemoryPlacementStore::record_admin_operation(self, guard, request).await
    }
    async fn compact_admin_operations(
        &self,
        guard: &PlacementLeaderGuard,
        request: CompactAdminOperations,
    ) -> Result<(), StorageError> {
        InMemoryPlacementStore::compact_admin_operations(self, guard, request).await
    }
}
