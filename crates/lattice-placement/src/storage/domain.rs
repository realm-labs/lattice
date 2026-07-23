use std::collections::BTreeSet;

use lattice_core::actor_ref::{EntityType, NodeIncarnation};
use serde::{Deserialize, Serialize};

use crate::{
    coordinator::{DomainMemberRecord, MemberRecord, SingletonConfig},
    plan::RebalancePlan,
    region::EntityConfig,
    types::{ClaimGrant, PlacementSlot, PlacementVersion, Revision, ShardId},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeasedClaim {
    pub grant: ClaimGrant,
    pub lease_id: i64,
}

impl LeasedClaim {
    pub(crate) fn matches_slot(&self, slot: &PlacementSlot) -> bool {
        self.lease_id > 0
            && !self.grant.ttl.is_zero()
            && self.grant.slot == slot.key
            && slot.owner.as_ref() == Some(&self.grant.owner)
            && self.grant.assignment_generation == slot.assignment_generation
            && self.grant.coordinator_term == slot.version.term
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateMember {
    pub member: MemberRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateMember {
    pub expected: MemberRecord,
    pub member: MemberRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoveMember {
    pub expected: MemberRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoveExpiredMember {
    pub expected: MemberRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberCommit {
    pub member: MemberRecord,
    pub revision: Revision,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDomainMember {
    pub expected_global_member: MemberRecord,
    pub member: DomainMemberRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateDomainMember {
    pub expected_global_member: MemberRecord,
    pub expected: DomainMemberRecord,
    pub member: DomainMemberRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoveDomainMember {
    pub expected: DomainMemberRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainMemberCommit {
    pub member: DomainMemberRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutEntityConfig {
    pub expected: Option<EntityConfig>,
    pub config: EntityConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutSingletonConfig {
    pub expected: Option<SingletonConfig>,
    pub config: SingletonConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityConfigCommit {
    pub config: EntityConfig,
    pub version: PlacementVersion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonConfigCommit {
    pub config: SingletonConfig,
    pub version: PlacementVersion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePlan {
    pub plan: RebalancePlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatePlan {
    pub expected: RebalancePlan,
    pub plan: RebalancePlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletePlan {
    pub expected: RebalancePlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanCommit {
    pub plan: RebalancePlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionSlot {
    pub expected: PlacementSlot,
    pub slot: PlacementSlot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotCommit {
    pub slot: PlacementSlot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocateInitial {
    pub expected_global_member: MemberRecord,
    pub expected_domain_member: DomainMemberRecord,
    pub slot: PlacementSlot,
    pub claim: LeasedClaim,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivateAuthority {
    pub expected_slot: PlacementSlot,
    pub expected_claim: ClaimGrant,
    pub slot: PlacementSlot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReserveMove {
    pub expected_plan: RebalancePlan,
    pub plan: RebalancePlan,
    pub expected_slot: PlacementSlot,
    pub slot: PlacementSlot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReserveHandoff {
    pub expected_slot: PlacementSlot,
    pub slot: PlacementSlot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
// Keeping the exact claim inline avoids an allocation in every authoritative
// compare-and-swap request; these requests are bounded and short-lived.
#[allow(clippy::large_enum_variant)]
pub enum ClaimPredicate {
    Present(ClaimGrant),
    Absent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FenceAuthority {
    pub expected_slot: PlacementSlot,
    pub expected_claim: ClaimPredicate,
    pub slot: PlacementSlot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FenceMissingAuthority {
    pub expected_slot: PlacementSlot,
    pub slot: PlacementSlot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallAuthority {
    pub expected_global_member: MemberRecord,
    pub expected_domain_member: DomainMemberRecord,
    pub expected_slot: PlacementSlot,
    pub slot: PlacementSlot,
    pub claim: LeasedClaim,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdoptAuthority {
    pub expected_global_member: MemberRecord,
    pub expected_domain_member: DomainMemberRecord,
    pub expected_slot: PlacementSlot,
    pub expected_claim: ClaimGrant,
    pub slot: PlacementSlot,
    pub claim: LeasedClaim,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteMove {
    pub expected_slot: PlacementSlot,
    pub slot: PlacementSlot,
    pub expected_plan: RebalancePlan,
    pub plan: RebalancePlan,
    pub expected_claim: ClaimGrant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorityCommit {
    pub slot: PlacementSlot,
    pub claim: LeasedClaim,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveCommit {
    pub slot: PlacementSlot,
    pub plan: RebalancePlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutomaticBalanceSettings {
    pub globally_paused: bool,
    pub paused_entity_types: BTreeSet<EntityType>,
    pub version: PlacementVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdminOperationStatus {
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdminOperationResult {
    AutomaticBalanceUpdated,
    PlanCreated {
        plan_id: u128,
    },
    EvaluationCompleted {
        plan_id: Option<u128>,
    },
    PendingMoveCancelled {
        plan_id: u128,
        shard_id: ShardId,
    },
    MemberRemoved {
        node_id: String,
        incarnation: NodeIncarnation,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminOperationRecord {
    pub operation_id: String,
    pub fingerprint: String,
    pub status: AdminOperationStatus,
    pub result: AdminOperationResult,
    pub version: PlacementVersion,
    pub created_unix_millis: u64,
    pub expires_unix_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitAutomaticSettings {
    pub expected: Option<AutomaticBalanceSettings>,
    pub settings: AutomaticBalanceSettings,
    pub operation: AdminOperationRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePlanWithOperation {
    pub plan: RebalancePlan,
    pub operation: AdminOperationRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatePlanWithOperation {
    pub expected_plan: RebalancePlan,
    pub plan: RebalancePlan,
    pub operation: AdminOperationRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordAdminOperation {
    pub operation: AdminOperationRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactAdminOperations {
    pub expected: Vec<AdminOperationRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableStorageLimits {
    pub maximum_slots: usize,
    pub maximum_plans: usize,
    pub maximum_members: usize,
    pub maximum_admin_operations: usize,
    pub maximum_entity_configs: usize,
    pub maximum_singleton_configs: usize,
}

impl DurableStorageLimits {
    pub fn validate(self) -> bool {
        self.maximum_slots > 0
            && self.maximum_plans > 0
            && self.maximum_members > 0
            && self.maximum_admin_operations > 0
            && self.maximum_entity_configs > 0
            && self.maximum_singleton_configs > 0
    }
}
