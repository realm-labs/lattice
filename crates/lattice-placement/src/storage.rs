use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use lattice_core::{
    actor_ref::{EntityType, PlacementDomainId, SingletonKind},
    coordinator::CoordinatorScope,
};
use thiserror::Error;

use crate::{
    coordinator::{
        DomainMemberRecord, DomainMemberStatus, ExactLeaderGuard, LeaderRecord, MemberRecord,
        MemberStatus, MembershipLeaderGuard, PlacementLeaderGuard, SessionLimits, SingletonConfig,
    },
    plan::{MoveProgress, RebalancePlan},
    region::EntityConfig,
    types::{PlacementSlot, PlacementSlotKey, PlacementSlotState, Revision},
};

pub mod domain;
pub mod etcd;
mod memory_admin;
mod memory_traits;

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
    CompleteMove, CreateDomainMember, CreateMember, CreatePlan, CreatePlanWithOperation,
    DeletePlan, DomainMemberCommit, DurableStorageLimits, EntityConfigCommit, FenceAuthority,
    FenceMissingAuthority, InstallAuthority, LeasedClaim, MemberCommit, MoveCommit, PlanCommit,
    PutEntityConfig, PutSingletonConfig, RecordAdminOperation, RemoveDomainMember, RemoveMember,
    ReserveHandoff, ReserveMove, SingletonConfigCommit, SlotCommit, TransitionSlot,
    UpdateDomainMember, UpdateMember, UpdatePlan, UpdatePlanWithOperation,
};

#[async_trait]
pub trait CoordinatorLeaseStore: Send + Sync + 'static {
    async fn ensure_schema_generation(&self) -> Result<(), StorageError>;
    async fn grant_lease(&self, ttl: Duration) -> Result<i64, StorageError>;
    async fn keep_lease_alive(&self, lease_id: i64) -> Result<(), StorageError>;
    async fn revoke_lease(&self, lease_id: i64) -> Result<(), StorageError>;
    async fn lease_time_to_live(&self, lease_id: i64) -> Result<Option<Duration>, StorageError>;
}

#[async_trait]
pub trait ScopedElectionStore: CoordinatorLeaseStore {
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
pub trait MembershipStore: CoordinatorLeaseStore {
    async fn get_membership_revision(&self) -> Result<Revision, StorageError>;
    async fn get_member(&self, node_id: &str) -> Result<Option<MemberRecord>, StorageError>;
    async fn list_members(&self) -> Result<Vec<MemberRecord>, StorageError>;
    async fn list_members_page(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<StorePage<MemberRecord>, StorageError> {
        bounded_page(&self.list_members().await?, offset, limit)
    }
    async fn create_member(
        &self,
        guard: &MembershipLeaderGuard,
        request: CreateMember,
    ) -> Result<MemberCommit, StorageError>;
    async fn update_member(
        &self,
        guard: &MembershipLeaderGuard,
        request: UpdateMember,
    ) -> Result<MemberCommit, StorageError>;
    async fn remove_member(
        &self,
        guard: &MembershipLeaderGuard,
        request: RemoveMember,
    ) -> Result<MemberCommit, StorageError>;
}

#[async_trait]
pub trait PlacementDomainStore: CoordinatorLeaseStore {
    fn durable_limits(&self, domain: &PlacementDomainId) -> DurableStorageLimits;
    async fn get_placement_revision(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Revision, StorageError>;
    async fn get_domain_member(
        &self,
        domain: &PlacementDomainId,
        node_id: &str,
    ) -> Result<Option<DomainMemberRecord>, StorageError>;
    async fn list_domain_members(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<DomainMemberRecord>, StorageError>;
    async fn create_domain_member(
        &self,
        guard: &PlacementLeaderGuard,
        request: CreateDomainMember,
    ) -> Result<DomainMemberCommit, StorageError>;
    async fn update_domain_member(
        &self,
        guard: &PlacementLeaderGuard,
        request: UpdateDomainMember,
    ) -> Result<DomainMemberCommit, StorageError>;
    async fn remove_domain_member(
        &self,
        guard: &PlacementLeaderGuard,
        request: RemoveDomainMember,
    ) -> Result<DomainMemberCommit, StorageError>;
    async fn get_entity_config(
        &self,
        domain: &PlacementDomainId,
        entity_type: &EntityType,
    ) -> Result<Option<EntityConfig>, StorageError>;
    async fn list_entity_configs(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<EntityConfig>, StorageError>;
    async fn put_entity_config(
        &self,
        guard: &PlacementLeaderGuard,
        request: PutEntityConfig,
    ) -> Result<EntityConfigCommit, StorageError>;
    async fn get_singleton_config(
        &self,
        domain: &PlacementDomainId,
        kind: &SingletonKind,
    ) -> Result<Option<SingletonConfig>, StorageError>;
    async fn list_singleton_configs(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<SingletonConfig>, StorageError>;
    async fn put_singleton_config(
        &self,
        guard: &PlacementLeaderGuard,
        request: PutSingletonConfig,
    ) -> Result<SingletonConfigCommit, StorageError>;
    async fn get_slot(&self, key: &PlacementSlotKey)
    -> Result<Option<PlacementSlot>, StorageError>;
    async fn get_plan(
        &self,
        domain: &PlacementDomainId,
        plan_id: u128,
    ) -> Result<Option<RebalancePlan>, StorageError>;
    async fn get_claim(&self, key: &PlacementSlotKey) -> Result<Option<LeasedClaim>, StorageError>;
    async fn list_slots(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<PlacementSlot>, StorageError>;
    async fn list_plans(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<RebalancePlan>, StorageError>;
    async fn list_claims(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<LeasedClaim>, StorageError>;
    async fn get_automatic_settings(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Option<AutomaticBalanceSettings>, StorageError>;
    async fn get_admin_operation(
        &self,
        domain: &PlacementDomainId,
        operation_id: &str,
    ) -> Result<Option<AdminOperationRecord>, StorageError>;
    async fn list_admin_operations(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<AdminOperationRecord>, StorageError>;
    async fn list_slots_page(
        &self,
        domain: &PlacementDomainId,
        states: &[PlacementSlotState],
        offset: usize,
        limit: usize,
    ) -> Result<StorePage<PlacementSlot>, StorageError> {
        let records = self
            .list_slots(domain)
            .await?
            .into_iter()
            .filter(|slot| states.is_empty() || states.contains(&slot.state))
            .collect::<Vec<_>>();
        bounded_page(&records, offset, limit)
    }
    async fn list_plans_page(
        &self,
        domain: &PlacementDomainId,
        offset: usize,
        limit: usize,
    ) -> Result<StorePage<RebalancePlan>, StorageError> {
        bounded_page(&self.list_plans(domain).await?, offset, limit)
    }
    async fn list_claims_page(
        &self,
        domain: &PlacementDomainId,
        offset: usize,
        limit: usize,
    ) -> Result<StorePage<LeasedClaim>, StorageError> {
        bounded_page(&self.list_claims(domain).await?, offset, limit)
    }

    async fn create_plan(
        &self,
        guard: &PlacementLeaderGuard,
        request: CreatePlan,
    ) -> Result<PlanCommit, StorageError>;
    async fn update_plan(
        &self,
        guard: &PlacementLeaderGuard,
        request: UpdatePlan,
    ) -> Result<PlanCommit, StorageError>;
    async fn delete_plan(
        &self,
        guard: &PlacementLeaderGuard,
        request: DeletePlan,
    ) -> Result<PlanCommit, StorageError>;
    async fn transition_slot(
        &self,
        guard: &PlacementLeaderGuard,
        request: TransitionSlot,
    ) -> Result<SlotCommit, StorageError>;
    async fn allocate_initial(
        &self,
        guard: &PlacementLeaderGuard,
        request: AllocateInitial,
    ) -> Result<AuthorityCommit, StorageError>;
    async fn activate_authority(
        &self,
        guard: &PlacementLeaderGuard,
        request: ActivateAuthority,
    ) -> Result<SlotCommit, StorageError>;
    async fn reserve_move(
        &self,
        guard: &PlacementLeaderGuard,
        request: ReserveMove,
    ) -> Result<MoveCommit, StorageError>;
    async fn reserve_handoff(
        &self,
        guard: &PlacementLeaderGuard,
        request: ReserveHandoff,
    ) -> Result<SlotCommit, StorageError>;
    async fn fence_authority(
        &self,
        guard: &PlacementLeaderGuard,
        request: FenceAuthority,
    ) -> Result<SlotCommit, StorageError>;
    async fn fence_missing_authority(
        &self,
        guard: &PlacementLeaderGuard,
        request: FenceMissingAuthority,
    ) -> Result<SlotCommit, StorageError>;
    async fn install_authority(
        &self,
        guard: &PlacementLeaderGuard,
        request: InstallAuthority,
    ) -> Result<AuthorityCommit, StorageError>;
    async fn adopt_authority(
        &self,
        guard: &PlacementLeaderGuard,
        request: AdoptAuthority,
    ) -> Result<AuthorityCommit, StorageError>;
    async fn complete_move(
        &self,
        guard: &PlacementLeaderGuard,
        request: CompleteMove,
    ) -> Result<MoveCommit, StorageError>;
    async fn commit_automatic_settings(
        &self,
        guard: &PlacementLeaderGuard,
        request: CommitAutomaticSettings,
    ) -> Result<AutomaticBalanceSettings, StorageError>;
    async fn create_plan_with_operation(
        &self,
        guard: &PlacementLeaderGuard,
        request: CreatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError>;
    async fn update_plan_with_operation(
        &self,
        guard: &PlacementLeaderGuard,
        request: UpdatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError>;
    async fn record_admin_operation(
        &self,
        guard: &PlacementLeaderGuard,
        request: RecordAdminOperation,
    ) -> Result<AdminOperationRecord, StorageError>;
    async fn compact_admin_operations(
        &self,
        guard: &PlacementLeaderGuard,
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
    plans: BTreeMap<(PlacementDomainId, u128), RebalancePlan>,
    schema_generation: Option<u64>,
    membership_revision: Option<Revision>,
    placement_revisions: BTreeMap<PlacementDomainId, Revision>,
    next_lease: i64,
    leases: BTreeMap<i64, LeaseState>,
    leaders: BTreeMap<CoordinatorScope, (i64, LeaderRecord)>,
    leader_terms: BTreeMap<CoordinatorScope, u64>,
    members: BTreeMap<String, MemberRecord>,
    domain_members: BTreeMap<(PlacementDomainId, String), DomainMemberRecord>,
    entity_configs: BTreeMap<(PlacementDomainId, EntityType), EntityConfig>,
    singleton_configs: BTreeMap<(PlacementDomainId, SingletonKind), SingletonConfig>,
    claims: BTreeMap<PlacementSlotKey, LeasedClaim>,
    automatic_settings: BTreeMap<PlacementDomainId, AutomaticBalanceSettings>,
    admin_operations: BTreeMap<(PlacementDomainId, String), AdminOperationRecord>,
}

#[derive(Debug, Clone, Copy)]
struct LeaseState {
    ttl: Duration,
}

include!("storage/memory_core.rs");

impl InMemoryPlacementStore {
    async fn ensure_schema_generation(&self) -> Result<(), StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        match state.schema_generation {
            Some(etcd::STORAGE_SCHEMA_GENERATION) => Ok(()),
            Some(_) => Err(StorageError::SchemaGenerationMismatch),
            None => {
                state.schema_generation = Some(etcd::STORAGE_SCHEMA_GENERATION);
                state.membership_revision = Some(initial_revision());
                Ok(())
            }
        }
    }

    async fn grant_lease(&self, ttl: Duration) -> Result<i64, StorageError> {
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
        state.leaders.retain(|_, (lease, _)| *lease != lease_id);
        state
            .members
            .retain(|_, member| member.lease_id != lease_id);
        state.claims.retain(|_, claim| claim.lease_id != lease_id);
        Ok(())
    }

    async fn lease_time_to_live(&self, lease_id: i64) -> Result<Option<Duration>, StorageError> {
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
        leader.validate().map_err(|_| StorageError::InvalidRecord)?;
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        if !state.leases.contains_key(&lease_id) || state.leaders.contains_key(&leader.scope) {
            return Ok(false);
        }
        let expected = state
            .leader_terms
            .get(&leader.scope)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(StorageError::CounterExhausted)?;
        if leader.term.get() != expected {
            return Err(StorageError::CompareFailed);
        }
        state.leader_terms.insert(leader.scope.clone(), expected);
        state
            .leaders
            .insert(leader.scope.clone(), (lease_id, leader.clone()));
        Ok(true)
    }

    async fn get_leader_inner(
        &self,
        scope: &CoordinatorScope,
    ) -> Result<Option<LeaderRecord>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .leaders
            .get(scope)
            .map(|(_, leader)| leader.clone()))
    }

    async fn get_leader_term_inner(&self, scope: &CoordinatorScope) -> Result<u64, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .leader_terms
            .get(scope)
            .copied()
            .unwrap_or(0))
    }

    async fn create_member(
        &self,
        guard: &MembershipLeaderGuard,
        request: CreateMember,
    ) -> Result<MemberCommit, StorageError> {
        validate_member_record(&request.member)?;
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        if guard.scope() != &CoordinatorScope::Membership {
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
        set_revision(&mut state, guard.scope(), request.member.version.revision);
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
        guard: &MembershipLeaderGuard,
        request: UpdateMember,
    ) -> Result<MemberCommit, StorageError> {
        validate_member_record(&request.member)?;
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        if guard.scope() != &CoordinatorScope::Membership {
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
        guard: &MembershipLeaderGuard,
        request: RemoveMember,
    ) -> Result<MemberCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        if state.members.get(&request.expected.node.node_id) != Some(&request.expected) {
            return Err(StorageError::CompareFailed);
        }
        if guard.scope() != &CoordinatorScope::Membership {
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

    async fn create_domain_member(
        &self,
        guard: &PlacementLeaderGuard,
        request: CreateDomainMember,
    ) -> Result<DomainMemberCommit, StorageError> {
        validate_domain_member_record(guard, &request.member)?;
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
            || state.domain_members.contains_key(&(
                request.member.version.domain.clone(),
                request.member.node.node_id.clone(),
            ))
        {
            return Err(StorageError::CompareFailed);
        }
        validate_next_revision(&state, guard.scope(), request.member.version.revision)?;
        if state
            .domain_members
            .keys()
            .filter(|(domain, _)| domain == &request.member.version.domain)
            .count()
            == self.maximum_members
        {
            return Err(StorageError::Capacity);
        }
        set_revision(&mut state, guard.scope(), request.member.version.revision);
        state.domain_members.insert(
            (
                request.member.version.domain.clone(),
                request.member.node.node_id.clone(),
            ),
            request.member.clone(),
        );
        Ok(DomainMemberCommit {
            member: request.member,
        })
    }

    async fn update_domain_member(
        &self,
        guard: &PlacementLeaderGuard,
        request: UpdateDomainMember,
    ) -> Result<DomainMemberCommit, StorageError> {
        validate_domain_member_record(guard, &request.expected)?;
        validate_domain_member_record(guard, &request.member)?;
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
            || request.expected.version.domain != request.member.version.domain
            || state.domain_members.get(&(
                request.expected.version.domain.clone(),
                request.expected.node.node_id.clone(),
            )) != Some(&request.expected)
        {
            return Err(StorageError::CompareFailed);
        }
        validate_next_revision(&state, guard.scope(), request.member.version.revision)?;
        set_revision(&mut state, guard.scope(), request.member.version.revision);
        state.domain_members.insert(
            (
                request.member.version.domain.clone(),
                request.member.node.node_id.clone(),
            ),
            request.member.clone(),
        );
        Ok(DomainMemberCommit {
            member: request.member,
        })
    }

    async fn remove_domain_member(
        &self,
        guard: &PlacementLeaderGuard,
        request: RemoveDomainMember,
    ) -> Result<DomainMemberCommit, StorageError> {
        validate_domain_member_record(guard, &request.expected)?;
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        let key = (
            request.expected.version.domain.clone(),
            request.expected.node.node_id.clone(),
        );
        if state.domain_members.get(&key) != Some(&request.expected) {
            return Err(StorageError::CompareFailed);
        }
        let next = current_revision(&state, guard.scope())
            .next()
            .map_err(|_| StorageError::CounterExhausted)?;
        set_revision(&mut state, guard.scope(), next);
        state.domain_members.remove(&key);
        Ok(DomainMemberCommit {
            member: request.expected,
        })
    }

    async fn create_plan(
        &self,
        guard: &PlacementLeaderGuard,
        request: CreatePlan,
    ) -> Result<PlanCommit, StorageError> {
        validate_plan_domain(guard, &request.plan)?;
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        if request.plan.coordinator_term != guard.term()
            || request.plan.record_revision.get() != 1
            || state
                .plans
                .contains_key(&(request.plan.domain.clone(), request.plan.plan_id))
        {
            return Err(StorageError::CompareFailed);
        }
        if state
            .plans
            .keys()
            .filter(|(domain, _)| domain == &request.plan.domain)
            .count()
            == self.maximum_plans
        {
            return Err(StorageError::Capacity);
        }
        state.plans.insert(
            (request.plan.domain.clone(), request.plan.plan_id),
            request.plan.clone(),
        );
        Ok(PlanCommit { plan: request.plan })
    }

    async fn update_plan(
        &self,
        guard: &PlacementLeaderGuard,
        request: UpdatePlan,
    ) -> Result<PlanCommit, StorageError> {
        validate_plan_domain(guard, &request.expected)?;
        validate_plan_domain(guard, &request.plan)?;
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_plan_update(&request.expected, &request.plan)?;
        if state
            .plans
            .get(&(request.expected.domain.clone(), request.expected.plan_id))
            != Some(&request.expected)
        {
            return Err(StorageError::CompareFailed);
        }
        state.plans.insert(
            (request.plan.domain.clone(), request.plan.plan_id),
            request.plan.clone(),
        );
        Ok(PlanCommit { plan: request.plan })
    }

    async fn delete_plan(
        &self,
        guard: &PlacementLeaderGuard,
        request: DeletePlan,
    ) -> Result<PlanCommit, StorageError> {
        validate_plan_domain(guard, &request.expected)?;
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        if state
            .plans
            .get(&(request.expected.domain.clone(), request.expected.plan_id))
            != Some(&request.expected)
        {
            return Err(StorageError::CompareFailed);
        }
        state
            .plans
            .remove(&(request.expected.domain.clone(), request.expected.plan_id));
        Ok(PlanCommit {
            plan: request.expected,
        })
    }

    async fn transition_slot(
        &self,
        guard: &PlacementLeaderGuard,
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
        guard: &PlacementLeaderGuard,
        request: AllocateInitial,
    ) -> Result<AuthorityCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_slot_common(guard, &state, None, &request.slot)?;
        validate_assignment_members(
            &state,
            &request.expected_global_member,
            &request.expected_domain_member,
            request
                .slot
                .owner
                .as_ref()
                .ok_or(StorageError::InvalidRecord)?,
        )?;
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
        guard: &PlacementLeaderGuard,
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
        set_revision(&mut state, guard.scope(), request.slot.version.revision);
        state
            .slots
            .insert(request.slot.key.clone(), request.slot.clone());
        Ok(SlotCommit { slot: request.slot })
    }

    async fn reserve_move(
        &self,
        guard: &PlacementLeaderGuard,
        request: ReserveMove,
    ) -> Result<MoveCommit, StorageError> {
        validate_plan_domain(guard, &request.expected_plan)?;
        validate_plan_domain(guard, &request.plan)?;
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_slot_common(guard, &state, Some(&request.expected_slot), &request.slot)?;
        validate_plan_update(&request.expected_plan, &request.plan)?;
        if state.plans.get(&(
            request.expected_plan.domain.clone(),
            request.expected_plan.plan_id,
        )) != Some(&request.expected_plan)
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
        set_revision(&mut state, guard.scope(), request.slot.version.revision);
        state
            .slots
            .insert(request.slot.key.clone(), request.slot.clone());
        state.plans.insert(
            (request.plan.domain.clone(), request.plan.plan_id),
            request.plan.clone(),
        );
        Ok(MoveCommit {
            slot: request.slot,
            plan: request.plan,
        })
    }

    async fn reserve_handoff(
        &self,
        guard: &PlacementLeaderGuard,
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
        set_revision(&mut state, guard.scope(), request.slot.version.revision);
        state
            .slots
            .insert(request.slot.key.clone(), request.slot.clone());
        Ok(SlotCommit { slot: request.slot })
    }

    async fn fence_authority(
        &self,
        guard: &PlacementLeaderGuard,
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
        guard: &PlacementLeaderGuard,
        request: InstallAuthority,
    ) -> Result<AuthorityCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_assignment_members(
            &state,
            &request.expected_global_member,
            &request.expected_domain_member,
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
        guard: &PlacementLeaderGuard,
        request: AdoptAuthority,
    ) -> Result<AuthorityCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_assignment_members(
            &state,
            &request.expected_global_member,
            &request.expected_domain_member,
            request
                .slot
                .owner
                .as_ref()
                .ok_or(StorageError::InvalidRecord)?,
        )?;
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

    async fn complete_move(
        &self,
        guard: &PlacementLeaderGuard,
        request: CompleteMove,
    ) -> Result<MoveCommit, StorageError> {
        validate_plan_domain(guard, &request.expected_plan)?;
        validate_plan_domain(guard, &request.plan)?;
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_slot_common(guard, &state, Some(&request.expected_slot), &request.slot)?;
        validate_plan_update(&request.expected_plan, &request.plan)?;
        if !claim_matches(&state, &request.expected_claim)
            || state.plans.get(&(
                request.expected_plan.domain.clone(),
                request.expected_plan.plan_id,
            )) != Some(&request.expected_plan)
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
        set_revision(&mut state, guard.scope(), request.slot.version.revision);
        state
            .slots
            .insert(request.slot.key.clone(), request.slot.clone());
        state.plans.insert(
            (request.plan.domain.clone(), request.plan.plan_id),
            request.plan.clone(),
        );
        Ok(MoveCommit {
            slot: request.slot,
            plan: request.plan,
        })
    }

    async fn commit_automatic_settings(
        &self,
        guard: &PlacementLeaderGuard,
        request: CommitAutomaticSettings,
    ) -> Result<AutomaticBalanceSettings, StorageError> {
        memory_admin::commit_automatic_settings(self, guard, request)
    }

    async fn create_plan_with_operation(
        &self,
        guard: &PlacementLeaderGuard,
        request: CreatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError> {
        memory_admin::create_plan_with_operation(self, guard, request)
    }

    async fn update_plan_with_operation(
        &self,
        guard: &PlacementLeaderGuard,
        request: UpdatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError> {
        memory_admin::update_plan_with_operation(self, guard, request)
    }

    async fn record_admin_operation(
        &self,
        guard: &PlacementLeaderGuard,
        request: RecordAdminOperation,
    ) -> Result<AdminOperationRecord, StorageError> {
        memory_admin::record_admin_operation(self, guard, request)
    }

    async fn compact_admin_operations(
        &self,
        guard: &PlacementLeaderGuard,
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
