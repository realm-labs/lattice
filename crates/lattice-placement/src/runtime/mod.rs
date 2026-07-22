use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use lattice_core::{
    actor_ref::{EntityType, NodeIncarnation, PlacementDomainId, SingletonKind},
    coordinator::CoordinatorScope,
};
use lattice_remoting::association::{
    Association, AssociationError, AssociationKey, AssociationManager, AssociationState,
};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot::Sender, watch},
    time::Instant,
};

use crate::{
    allocation::{
        AllocationError, AllocationRequest, LoadSample, PlacedShard, PlacementNode, PlacementView,
        RebalanceLimits, RebalanceProposal, RebalanceTrigger, ShardAllocationStrategy,
        registry::ShardAllocationStrategies,
    },
    control::{
        DEFAULT_MAX_CONTROL_PAYLOAD, PlacementControlCommand, PlacementControlError,
        PlacementControlEvent, encode_control_command,
    },
    coordinator::{
        COORDINATOR_PROTOCOL_GENERATION, CoordinatorError, DomainMemberRecord, DomainMemberStatus,
        LeaderRecord, LoadTable, MemberRecord, MemberRemovalReason, MemberStatus,
        PlacementDomainHello, PlacementLeaderGuard, SessionLimits, SingletonConfig, SnapshotLimits,
        SnapshotRecord, build_snapshot,
    },
    handoff::{HandoffEffect, HandoffError, HandoffEvent, HandoffMachine, HandoffPhase},
    plan::{MoveProgress, PlanError, PlanReason, PlanStatus, RebalancePlan},
    region::EntityConfig,
    storage::{
        CoordinatorLeaseStore, MembershipStore, PlacementDomainStore, ScopedElectionStore,
        StorageError,
        domain::{AdminOperationRecord, AutomaticBalanceSettings, DurableStorageLimits},
    },
    types::{
        AssignmentGeneration, ClaimGrant, CoordinatorTerm, GrantSequence, MembershipVersion,
        MonotonicTime, NodeKey, PlacementSlot, PlacementSlotKey, PlacementSlotState,
        PlacementVersion, ShardId,
    },
};

mod admin;
mod allocation;
pub mod host;
mod lifecycle;
mod membership;
pub mod membership_plane;
mod rebalance;
mod reconciliation;

#[derive(Debug, Clone)]
pub struct PlacementDomainLeaderConfig {
    pub leader_lease_ttl: Duration,
    pub member_lease_ttl: Duration,
    pub claim_ttl: Duration,
    pub renewal_interval: Duration,
    pub member_heartbeat_timeout: Duration,
    pub session_limits: SessionLimits,
    pub snapshot_limits: SnapshotLimits,
    pub maximum_sessions: usize,
    pub maximum_node_loads: usize,
    pub maximum_shard_loads: usize,
    pub maximum_control_payload: usize,
    pub maximum_operations: usize,
    pub maximum_admin_operation_records: usize,
    pub admin_operation_retention: Duration,
    pub maximum_plan_moves: usize,
    pub maximum_completed_plan_history: usize,
    pub maximum_entity_configs: usize,
    pub maximum_singleton_configs: usize,
    pub rebalance_limits: RebalanceLimits,
    pub rebalance_interval: Duration,
    pub reconciliation_interval: Duration,
    pub reconciliation_page_size: usize,
    pub maximum_reconciliation_work_per_pass: usize,
    pub maximum_quarantined_records: usize,
}

impl Default for PlacementDomainLeaderConfig {
    fn default() -> Self {
        Self {
            leader_lease_ttl: Duration::from_secs(10),
            member_lease_ttl: Duration::from_secs(15),
            claim_ttl: Duration::from_secs(15),
            renewal_interval: Duration::from_secs(5),
            member_heartbeat_timeout: Duration::from_secs(15),
            session_limits: SessionLimits::default(),
            snapshot_limits: SnapshotLimits {
                maximum_chunk_bytes: 192 * 1024,
                ..SnapshotLimits::default()
            },
            maximum_sessions: 256,
            maximum_node_loads: 256,
            maximum_shard_loads: 65_536,
            maximum_control_payload: DEFAULT_MAX_CONTROL_PAYLOAD,
            maximum_operations: 128,
            maximum_admin_operation_records: 1024,
            admin_operation_retention: Duration::from_secs(24 * 60 * 60),
            maximum_plan_moves: 64,
            maximum_completed_plan_history: 64,
            maximum_entity_configs: 1024,
            maximum_singleton_configs: 1024,
            rebalance_limits: RebalanceLimits {
                moves_per_round: 16,
                concurrent_cluster: 8,
                concurrent_entity: 2,
                concurrent_source: 1,
                concurrent_target: 1,
            },
            rebalance_interval: Duration::from_secs(30),
            reconciliation_interval: Duration::from_secs(5),
            reconciliation_page_size: 128,
            maximum_reconciliation_work_per_pass: 256,
            maximum_quarantined_records: 128,
        }
    }
}

impl PlacementDomainLeaderConfig {
    fn validate(&self) -> Result<(), CoordinatorRuntimeError> {
        if self.leader_lease_ttl.is_zero()
            || self.member_lease_ttl.is_zero()
            || self.claim_ttl.is_zero()
            || self.renewal_interval.is_zero()
            || self.member_heartbeat_timeout.is_zero()
            || self.renewal_interval >= self.leader_lease_ttl
            || self.renewal_interval >= self.member_lease_ttl
            || self.renewal_interval >= self.claim_ttl
            || self.maximum_sessions == 0
            || self.maximum_node_loads == 0
            || self.maximum_shard_loads == 0
            || self.maximum_control_payload == 0
            || self.maximum_operations == 0
            || self.maximum_admin_operation_records == 0
            || self.admin_operation_retention.is_zero()
            || self.maximum_plan_moves == 0
            || self.maximum_completed_plan_history == 0
            || self.maximum_entity_configs == 0
            || self.maximum_singleton_configs == 0
            || self.rebalance_limits.validate().is_err()
            || self.rebalance_interval.is_zero()
            || self.reconciliation_interval.is_zero()
            || self.reconciliation_page_size == 0
            || self.maximum_reconciliation_work_per_pass == 0
            || self.maximum_quarantined_records == 0
        {
            return Err(CoordinatorRuntimeError::InvalidConfig);
        }
        Ok(())
    }
}

struct MemberSession {
    hello: PlacementDomainHello,
    record: MemberRecord,
    domain_record: Option<DomainMemberRecord>,
    association: AssociationKey,
    lease_id: i64,
    heartbeat_sequence: u64,
    last_heartbeat: Instant,
    applied_version: Option<PlacementVersion>,
    snapshot_version: Option<MembershipVersion>,
    draining: bool,
    drain_operation: Option<String>,
    drain_ready: bool,
    joined_at: MonotonicTime,
}

impl MemberSession {
    fn placement_up(&self) -> bool {
        self.record.status == MemberStatus::Up
            && self
                .domain_record
                .as_ref()
                .is_some_and(|member| member.status == DomainMemberStatus::Up)
    }
}

struct ClaimLease {
    lease_id: i64,
    grant: ClaimGrant,
}

#[derive(Debug, Clone)]
pub struct ManualRelocationRequest {
    pub domain: PlacementDomainId,
    pub operation_id: String,
    pub entity_type: EntityType,
    pub shard_id: ShardId,
    pub expected_generation: AssignmentGeneration,
    pub target_node_id: String,
}

#[derive(Debug, Clone)]
pub struct ForceRemoveRequest {
    pub domain: PlacementDomainId,
    pub operation_id: String,
    pub node_id: String,
    pub expected_incarnation: NodeIncarnation,
}

#[derive(Debug, Clone)]
pub struct CoordinatorInspection {
    pub version: PlacementVersion,
    pub automatic_globally_paused: bool,
    pub paused_entity_types: Vec<EntityType>,
    pub slots: Vec<PlacementSlot>,
    pub plans: Vec<RebalancePlan>,
    pub reconciliation_backlog: usize,
    pub reconciliation_oldest_pending_millis: Option<u64>,
    pub reconciliation_last_success_age_millis: Option<u64>,
    pub quarantined_records: Vec<(String, String)>,
    pub durable_limits: DurableStorageLimits,
    pub retained_admin_operations: usize,
    pub leadership_loss_count: u64,
    pub commit_conflict_count: u64,
    pub unknown_outcome_count: u64,
    pub capacity_rejection_count: u64,
}

#[derive(Default)]
struct ReconciliationState {
    initial_complete: bool,
    cursor: usize,
    backlog: usize,
    oldest_pending: Option<Instant>,
    last_success: Option<Instant>,
    quarantined: BTreeMap<String, String>,
    focused: bool,
}

enum CoordinatorOperation {
    SubmitRebalance {
        proposal: RebalanceProposal,
        entity_type: EntityType,
        completion: Sender<Result<u128, CoordinatorRuntimeError>>,
    },
    CancelPending {
        operation_id: String,
        plan_id: u128,
        shard_id: ShardId,
        completion: Sender<Result<(), CoordinatorRuntimeError>>,
    },
    Evaluate {
        operation_id: String,
        entity_type: EntityType,
        trigger: RebalanceTrigger,
        completion: Sender<Result<Option<u128>, CoordinatorRuntimeError>>,
    },
    SetAutomatic {
        operation_id: String,
        entity_type: Option<EntityType>,
        paused: bool,
        completion: Sender<Result<(), CoordinatorRuntimeError>>,
    },
    ManualRelocate {
        request: ManualRelocationRequest,
        completion: Sender<Result<u128, CoordinatorRuntimeError>>,
    },
    ForceRemove {
        request: ForceRemoveRequest,
        completion: Sender<Result<(), CoordinatorRuntimeError>>,
    },
    Inspect {
        completion: Sender<Result<CoordinatorInspection, CoordinatorRuntimeError>>,
    },
}

#[derive(Clone)]
pub struct CoordinatorHandle {
    domain: PlacementDomainId,
    operations: mpsc::Sender<CoordinatorOperation>,
}

impl CoordinatorHandle {
    pub async fn submit_rebalance(
        &self,
        proposal: RebalanceProposal,
        entity_type: EntityType,
    ) -> Result<u128, CoordinatorRuntimeError> {
        if proposal.domain != self.domain {
            return Err(CoordinatorRuntimeError::InvalidAdminOperation);
        }
        let (completion, result) = tokio::sync::oneshot::channel();
        self.operations
            .send(CoordinatorOperation::SubmitRebalance {
                proposal,
                entity_type,
                completion,
            })
            .await
            .map_err(|_| CoordinatorRuntimeError::OperationClosed)?;
        result
            .await
            .map_err(|_| CoordinatorRuntimeError::OperationClosed)?
    }

    pub async fn cancel_pending(
        &self,
        domain: PlacementDomainId,
        operation_id: String,
        plan_id: u128,
        shard_id: ShardId,
    ) -> Result<(), CoordinatorRuntimeError> {
        self.require_domain(&domain)?;
        let (completion, result) = tokio::sync::oneshot::channel();
        self.operations
            .send(CoordinatorOperation::CancelPending {
                operation_id,
                plan_id,
                shard_id,
                completion,
            })
            .await
            .map_err(|_| CoordinatorRuntimeError::OperationClosed)?;
        result
            .await
            .map_err(|_| CoordinatorRuntimeError::OperationClosed)?
    }

    pub async fn evaluate_rebalance(
        &self,
        domain: PlacementDomainId,
        operation_id: String,
        entity_type: EntityType,
        trigger: RebalanceTrigger,
    ) -> Result<Option<u128>, CoordinatorRuntimeError> {
        self.require_domain(&domain)?;
        let (completion, result) = tokio::sync::oneshot::channel();
        self.operations
            .send(CoordinatorOperation::Evaluate {
                operation_id,
                entity_type,
                trigger,
                completion,
            })
            .await
            .map_err(|_| CoordinatorRuntimeError::OperationClosed)?;
        result
            .await
            .map_err(|_| CoordinatorRuntimeError::OperationClosed)?
    }

    pub async fn set_automatic_paused(
        &self,
        domain: PlacementDomainId,
        operation_id: String,
        entity_type: Option<EntityType>,
        paused: bool,
    ) -> Result<(), CoordinatorRuntimeError> {
        self.require_domain(&domain)?;
        let (completion, result) = tokio::sync::oneshot::channel();
        self.operations
            .send(CoordinatorOperation::SetAutomatic {
                operation_id,
                entity_type,
                paused,
                completion,
            })
            .await
            .map_err(|_| CoordinatorRuntimeError::OperationClosed)?;
        result
            .await
            .map_err(|_| CoordinatorRuntimeError::OperationClosed)?
    }

    pub async fn relocate_shard(
        &self,
        request: ManualRelocationRequest,
    ) -> Result<u128, CoordinatorRuntimeError> {
        self.require_domain(&request.domain)?;
        let (completion, result) = tokio::sync::oneshot::channel();
        self.operations
            .send(CoordinatorOperation::ManualRelocate {
                request,
                completion,
            })
            .await
            .map_err(|_| CoordinatorRuntimeError::OperationClosed)?;
        result
            .await
            .map_err(|_| CoordinatorRuntimeError::OperationClosed)?
    }

    pub async fn force_remove(
        &self,
        request: ForceRemoveRequest,
    ) -> Result<(), CoordinatorRuntimeError> {
        self.require_domain(&request.domain)?;
        let (completion, result) = tokio::sync::oneshot::channel();
        self.operations
            .send(CoordinatorOperation::ForceRemove {
                request,
                completion,
            })
            .await
            .map_err(|_| CoordinatorRuntimeError::OperationClosed)?;
        result
            .await
            .map_err(|_| CoordinatorRuntimeError::OperationClosed)?
    }

    pub async fn inspect(
        &self,
        domain: PlacementDomainId,
    ) -> Result<CoordinatorInspection, CoordinatorRuntimeError> {
        self.require_domain(&domain)?;
        let (completion, result) = tokio::sync::oneshot::channel();
        self.operations
            .send(CoordinatorOperation::Inspect { completion })
            .await
            .map_err(|_| CoordinatorRuntimeError::OperationClosed)?;
        result
            .await
            .map_err(|_| CoordinatorRuntimeError::OperationClosed)?
    }

    fn require_domain(&self, domain: &PlacementDomainId) -> Result<(), CoordinatorRuntimeError> {
        if domain == &self.domain {
            Ok(())
        } else {
            Err(CoordinatorRuntimeError::InvalidAdminOperation)
        }
    }
}

pub struct PlacementDomainLeader<S>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + PlacementDomainStore,
{
    store: Arc<S>,
    associations: Arc<AssociationManager>,
    leader: LeaderRecord,
    leader_guard: PlacementLeaderGuard,
    leader_lease_id: i64,
    config: PlacementDomainLeaderConfig,
    membership_version: MembershipVersion,
    version: PlacementVersion,
    sessions: BTreeMap<NodeIncarnation, MemberSession>,
    claims: BTreeMap<PlacementSlotKey, ClaimLease>,
    loads: LoadTable,
    plans: BTreeMap<u128, RebalancePlan>,
    handoffs: BTreeMap<PlacementSlotKey, HandoffMachine>,
    operations: mpsc::Sender<CoordinatorOperation>,
    operation_receiver: mpsc::Receiver<CoordinatorOperation>,
    entity_configs: BTreeMap<EntityType, EntityConfig>,
    singleton_configs: BTreeMap<SingletonKind, SingletonConfig>,
    strategies: BTreeMap<(String, u32), Arc<dyn ShardAllocationStrategy>>,
    origin: Instant,
    slot_assigned_at: BTreeMap<PlacementSlotKey, MonotonicTime>,
    last_automatic_move_at: Option<MonotonicTime>,
    node_load_received: BTreeMap<NodeIncarnation, MonotonicTime>,
    shard_load_received: BTreeMap<(NodeIncarnation, EntityType, ShardId), MonotonicTime>,
    automatic_globally_paused: bool,
    paused_entity_types: BTreeSet<EntityType>,
    automatic_settings: Option<AutomaticBalanceSettings>,
    applied_admin_operations: BTreeMap<String, AdminOperationRecord>,
    reconciliation: ReconciliationState,
    leadership_loss_count: u64,
    commit_conflict_count: u64,
    unknown_outcome_count: u64,
    capacity_rejection_count: u64,
}

impl<S> PlacementDomainLeader<S>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + PlacementDomainStore,
{
    pub async fn elect(
        store: Arc<S>,
        associations: Arc<AssociationManager>,
        node: NodeKey,
        scope: CoordinatorScope,
        term: CoordinatorTerm,
        config: PlacementDomainLeaderConfig,
    ) -> Result<Self, CoordinatorRuntimeError> {
        Self::elect_with_strategies(
            store,
            associations,
            node,
            scope,
            term,
            config,
            ShardAllocationStrategies::default(),
        )
        .await
    }

    pub async fn elect_with_strategies(
        store: Arc<S>,
        associations: Arc<AssociationManager>,
        node: NodeKey,
        scope: CoordinatorScope,
        term: CoordinatorTerm,
        config: PlacementDomainLeaderConfig,
        strategies: ShardAllocationStrategies,
    ) -> Result<Self, CoordinatorRuntimeError> {
        config.validate()?;
        store.ensure_schema_generation().await?;
        let leader_lease_id = store.grant_lease(config.leader_lease_ttl).await?;
        let domain = match &scope {
            CoordinatorScope::Placement(domain) => domain.clone(),
            CoordinatorScope::Membership => return Err(CoordinatorRuntimeError::InvalidConfig),
        };
        let leader = LeaderRecord {
            scope,
            node: node.clone(),
            protocol_generation: COORDINATOR_PROTOCOL_GENERATION,
            term,
        };
        if !store.campaign_leader(&leader, leader_lease_id).await? {
            let _ = store.revoke_lease(leader_lease_id).await;
            return Err(CoordinatorRuntimeError::NotLeader);
        }
        let leader_guard = PlacementLeaderGuard::new(leader.clone())
            .map_err(CoordinatorRuntimeError::Coordinator)?;
        let membership_scope = CoordinatorScope::Membership;
        let active_membership_term = store
            .get_leader(&membership_scope)
            .await?
            .map(|leader| leader.term);
        let persisted_membership_term = store.get_leader_term(&membership_scope).await?;
        let membership_term = active_membership_term
            .or_else(|| CoordinatorTerm::new(persisted_membership_term).ok())
            .unwrap_or(term);
        let slots = store.list_slots(&domain).await?;
        let entity_configs = store
            .list_entity_configs(&domain)
            .await?
            .into_iter()
            .map(|config| (config.entity_type.clone(), config))
            .collect();
        let singleton_configs = store
            .list_singleton_configs(&domain)
            .await?
            .into_iter()
            .map(|config| (config.kind.clone(), config))
            .collect();
        let membership_revision = store.get_membership_revision().await?;
        let placement_revision = store.get_placement_revision(&domain).await?;
        let membership_version = MembershipVersion::new(membership_term, membership_revision);
        let version = PlacementVersion::new(domain.clone(), term, placement_revision);
        let loads = LoadTable::new(config.maximum_node_loads, config.maximum_shard_loads)
            .map_err(CoordinatorRuntimeError::Coordinator)?;
        let plans = store
            .list_plans(&domain)
            .await?
            .into_iter()
            .map(|plan| (plan.plan_id, plan))
            .collect();
        let automatic_settings = store.get_automatic_settings(&domain).await?;
        let applied_admin_operations = store
            .list_admin_operations(&domain)
            .await?
            .into_iter()
            .map(|operation| (operation.operation_id.clone(), operation))
            .collect::<BTreeMap<_, _>>();
        let (operations, operation_receiver) = mpsc::channel(config.maximum_operations);
        let strategies = strategies.into_inner();
        let slot_assigned_at = slots
            .iter()
            .filter(|slot| slot.state == PlacementSlotState::Running)
            .map(|slot| (slot.key.clone(), MonotonicTime::from_millis(0)))
            .collect();
        let mut leader = Self {
            store,
            associations,
            leader,
            leader_guard,
            leader_lease_id,
            config,
            membership_version,
            version,
            sessions: BTreeMap::new(),
            claims: BTreeMap::new(),
            loads,
            plans,
            handoffs: BTreeMap::new(),
            operations,
            operation_receiver,
            entity_configs,
            singleton_configs,
            strategies,
            origin: Instant::now(),
            slot_assigned_at,
            last_automatic_move_at: None,
            node_load_received: BTreeMap::new(),
            shard_load_received: BTreeMap::new(),
            automatic_globally_paused: automatic_settings
                .as_ref()
                .is_some_and(|settings| settings.globally_paused),
            paused_entity_types: automatic_settings
                .as_ref()
                .map(|settings| settings.paused_entity_types.clone())
                .unwrap_or_default(),
            automatic_settings,
            applied_admin_operations,
            reconciliation: ReconciliationState::default(),
            leadership_loss_count: 0,
            commit_conflict_count: 0,
            unknown_outcome_count: 0,
            capacity_rejection_count: 0,
        };
        leader.reconcile_initial_inventory().await?;
        leader.recover_persisted_plans().await?;
        leader.compact_admin_operation_history().await?;
        Ok(leader)
    }

    async fn assignment_members(
        &self,
        owner: &NodeKey,
    ) -> Result<(MemberRecord, DomainMemberRecord), CoordinatorRuntimeError> {
        let global = self
            .store
            .get_member(&owner.node_id)
            .await?
            .filter(|member| member.node == *owner && member.status == MemberStatus::Up)
            .ok_or(CoordinatorRuntimeError::IneligibleTarget)?;
        let domain = self
            .store
            .get_domain_member(&self.version.domain, &owner.node_id)
            .await?
            .filter(|member| {
                member.node == *owner
                    && member.status == DomainMemberStatus::Up
                    && member.version.domain == self.version.domain
            })
            .ok_or(CoordinatorRuntimeError::IneligibleTarget)?;
        Ok((global, domain))
    }

    pub fn leader(&self) -> &LeaderRecord {
        &self.leader
    }

    pub fn handle(&self) -> CoordinatorHandle {
        CoordinatorHandle {
            domain: self.version.domain.clone(),
            operations: self.operations.clone(),
        }
    }

    pub fn register_strategy(
        &mut self,
        strategy: Arc<dyn ShardAllocationStrategy>,
    ) -> Result<(), CoordinatorRuntimeError> {
        let key = (strategy.policy_id().to_owned(), strategy.policy_version());
        if self.strategies.insert(key, strategy).is_some() {
            return Err(CoordinatorRuntimeError::DuplicateStrategy);
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum CoordinatorRuntimeError {
    #[error("Coordinator runtime configuration is invalid")]
    InvalidConfig,
    #[error("Coordinator leadership campaign lost")]
    NotLeader,
    #[error("Coordinator durable store failed")]
    Storage(#[from] StorageError),
    #[error("Coordinator reducer rejected state")]
    Coordinator(#[source] CoordinatorError),
    #[error("Coordinator control codec failed")]
    Control(#[source] PlacementControlError),
    #[error("Coordinator snapshot record codec failed")]
    Codec,
    #[error("Coordinator control stream closed")]
    ControlClosed,
    #[error("Coordinator operation stream closed")]
    OperationClosed,
    #[error("Coordinator admin operation is invalid")]
    InvalidAdminOperation,
    #[error("Coordinator admin operation ID conflicts with another command")]
    IdempotencyConflict,
    #[error("Coordinator admin operation history is full")]
    OperationCapacity,
    #[error("Coordinator received a command from an unauthorized session")]
    UnauthorizedCommand,
    #[error("Coordinator session is not registered")]
    UnknownSession,
    #[error("Coordinator member transition is stale or invalid")]
    StaleMember,
    #[error("predecessor incarnation {predecessor:?} is still leased for {remaining_ttl:?}")]
    IncarnationPending {
        predecessor: NodeIncarnation,
        remaining_ttl: Option<Duration>,
    },
    #[error("Coordinator drain operation is not ready")]
    DrainNotReady,
    #[error("Coordinator association is unavailable")]
    AssociationUnavailable,
    #[error("Coordinator claim sequence exhausted")]
    ClaimSequence,
    #[error("rebalance proposal is stale or no longer matches placement truth")]
    StaleProposal,
    #[error("rebalance plan conflicts with active work")]
    PlanConflict,
    #[error("rebalance concurrency or target reservation limit reached")]
    ConcurrencyLimit,
    #[error("rebalance target is not a live eligible host")]
    IneligibleTarget,
    #[error("placement entity configuration is unknown")]
    UnknownEntityConfig,
    #[error("shard ID is outside the configured entity key domain")]
    ShardOutOfRange,
    #[error("placement singleton configuration is unknown")]
    UnknownSingletonConfig,
    #[error("placement configuration conflicts with an existing declaration")]
    ConfigurationConflict,
    #[error("placement configuration cardinality limit reached")]
    ConfigurationCapacity,
    #[error("allocation strategy is not registered")]
    UnknownStrategy,
    #[error("allocation strategy ID/version is already registered")]
    DuplicateStrategy,
    #[error("allocation strategy rejected the placement view")]
    Allocation(#[source] AllocationError),
    #[error("placement slot does not exist")]
    UnknownSlot,
    #[error("rebalance plan does not exist")]
    UnknownPlan,
    #[error("handoff state does not exist")]
    UnknownHandoff,
    #[error("handoff state no longer matches persisted placement truth")]
    StaleHandoff,
    #[error("old claim invalidation could not be proven")]
    ClaimNotProven,
    #[error("Coordinator revision exhausted")]
    RevisionExhausted,
    #[error("rebalance plan reducer rejected a transition")]
    Plan(#[source] PlanError),
    #[error("handoff reducer rejected a transition")]
    Handoff(#[source] HandoffError),
    #[error("Coordinator Association rejected reliable control admission")]
    Association(#[from] AssociationError),
}

#[cfg(test)]
mod reconciliation_tests;
#[cfg(test)]
mod tests;
