use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use lattice_core::actor_ref::NodeIncarnation;
use lattice_remoting::association::Association;
use lattice_remoting::association::AssociationManager;
use lattice_remoting::association::AssociationState;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::allocation::{
    AllocationRequest, LoadSample, PlacedShard, PlacementNode, PlacementView, RebalanceLimits,
    RebalanceProposal, RebalanceTrigger, ShardAllocationStrategy, WeightedLeastLoad,
};
use crate::control::{
    DEFAULT_MAX_CONTROL_PAYLOAD, PlacementControlCommand, PlacementControlEvent,
    encode_control_command,
};
use crate::coordinator::{
    LeaderRecord, LoadTable, MemberChange, MemberEvent, MemberRecord, MemberRemovalReason,
    MemberStatus, NodeHello, SessionLimits, SingletonConfig, SnapshotLimits, SnapshotRecord,
    build_snapshot,
};
use crate::handoff::{HandoffEffect, HandoffEvent, HandoffMachine, HandoffPhase};
use crate::plan::{MoveProgress, PlanError, PlanReason, PlanStatus, RebalancePlan};
use crate::storage::{CoordinatorStore, StorageError};
use crate::types::{
    ClaimGrant, CoordinatorTerm, GrantSequence, NodeKey, PlacementSlot, PlacementSlotKey,
    PlacementSlotState, Revision,
};

mod admin;
mod allocation;
mod lifecycle;
mod membership;
mod rebalance;

#[derive(Debug, Clone)]
pub struct CoordinatorLeaderConfig {
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
    pub maximum_plan_moves: usize,
    pub maximum_completed_plan_history: usize,
    pub rebalance_limits: RebalanceLimits,
    pub rebalance_interval: Duration,
}

impl Default for CoordinatorLeaderConfig {
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
            maximum_plan_moves: 64,
            maximum_completed_plan_history: 64,
            rebalance_limits: RebalanceLimits {
                moves_per_round: 16,
                concurrent_cluster: 8,
                concurrent_entity: 2,
                concurrent_source: 1,
                concurrent_target: 1,
            },
            rebalance_interval: Duration::from_secs(30),
        }
    }
}

impl CoordinatorLeaderConfig {
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
            || self.maximum_plan_moves == 0
            || self.maximum_completed_plan_history == 0
            || self.rebalance_limits.validate().is_err()
            || self.rebalance_interval.is_zero()
        {
            return Err(CoordinatorRuntimeError::InvalidConfig);
        }
        Ok(())
    }
}

struct MemberSession {
    hello: NodeHello,
    record: MemberRecord,
    association: lattice_remoting::association::AssociationKey,
    lease_id: i64,
    heartbeat_sequence: u64,
    last_heartbeat: Instant,
    applied_revision: Option<Revision>,
    snapshot_revision: Option<Revision>,
    draining: bool,
    drain_operation: Option<String>,
    drain_ready: bool,
    joined_at: crate::types::MonotonicTime,
}

struct ClaimLease {
    lease_id: i64,
    grant: ClaimGrant,
}

#[derive(Debug, Clone)]
pub struct ManualRelocationRequest {
    pub operation_id: String,
    pub entity_type: lattice_core::actor_ref::EntityType,
    pub shard_id: crate::types::ShardId,
    pub expected_generation: crate::types::AssignmentGeneration,
    pub target_node_id: String,
}

#[derive(Debug, Clone)]
pub struct ForceRemoveRequest {
    pub operation_id: String,
    pub node_id: String,
    pub expected_incarnation: NodeIncarnation,
}

#[derive(Debug, Clone)]
pub struct CoordinatorInspection {
    pub term: CoordinatorTerm,
    pub revision: Revision,
    pub automatic_globally_paused: bool,
    pub paused_entity_types: Vec<lattice_core::actor_ref::EntityType>,
    pub slots: Vec<PlacementSlot>,
    pub plans: Vec<RebalancePlan>,
}

struct AppliedAdminOperation {
    fingerprint: String,
    plan_id: Option<u128>,
}

enum CoordinatorOperation {
    SubmitRebalance {
        proposal: RebalanceProposal,
        entity_type: lattice_core::actor_ref::EntityType,
        completion: tokio::sync::oneshot::Sender<Result<u128, CoordinatorRuntimeError>>,
    },
    CancelPending {
        plan_id: u128,
        shard_id: crate::types::ShardId,
        completion: tokio::sync::oneshot::Sender<Result<(), CoordinatorRuntimeError>>,
    },
    Evaluate {
        entity_type: lattice_core::actor_ref::EntityType,
        trigger: RebalanceTrigger,
        completion: tokio::sync::oneshot::Sender<Result<Option<u128>, CoordinatorRuntimeError>>,
    },
    SetAutomatic {
        operation_id: String,
        entity_type: Option<lattice_core::actor_ref::EntityType>,
        paused: bool,
        completion: tokio::sync::oneshot::Sender<Result<(), CoordinatorRuntimeError>>,
    },
    ManualRelocate {
        request: ManualRelocationRequest,
        completion: tokio::sync::oneshot::Sender<Result<u128, CoordinatorRuntimeError>>,
    },
    ForceRemove {
        request: ForceRemoveRequest,
        completion: tokio::sync::oneshot::Sender<Result<(), CoordinatorRuntimeError>>,
    },
    Inspect {
        completion:
            tokio::sync::oneshot::Sender<Result<CoordinatorInspection, CoordinatorRuntimeError>>,
    },
}

#[derive(Clone)]
pub struct CoordinatorHandle {
    operations: mpsc::Sender<CoordinatorOperation>,
}

impl CoordinatorHandle {
    pub async fn submit_rebalance(
        &self,
        proposal: RebalanceProposal,
        entity_type: lattice_core::actor_ref::EntityType,
    ) -> Result<u128, CoordinatorRuntimeError> {
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
        plan_id: u128,
        shard_id: crate::types::ShardId,
    ) -> Result<(), CoordinatorRuntimeError> {
        let (completion, result) = tokio::sync::oneshot::channel();
        self.operations
            .send(CoordinatorOperation::CancelPending {
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
        entity_type: lattice_core::actor_ref::EntityType,
        trigger: RebalanceTrigger,
    ) -> Result<Option<u128>, CoordinatorRuntimeError> {
        let (completion, result) = tokio::sync::oneshot::channel();
        self.operations
            .send(CoordinatorOperation::Evaluate {
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
        operation_id: String,
        entity_type: Option<lattice_core::actor_ref::EntityType>,
        paused: bool,
    ) -> Result<(), CoordinatorRuntimeError> {
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

    pub async fn inspect(&self) -> Result<CoordinatorInspection, CoordinatorRuntimeError> {
        let (completion, result) = tokio::sync::oneshot::channel();
        self.operations
            .send(CoordinatorOperation::Inspect { completion })
            .await
            .map_err(|_| CoordinatorRuntimeError::OperationClosed)?;
        result
            .await
            .map_err(|_| CoordinatorRuntimeError::OperationClosed)?
    }
}

pub struct CoordinatorLeader<S: CoordinatorStore> {
    store: Arc<S>,
    associations: Arc<AssociationManager>,
    leader: LeaderRecord,
    leader_lease_id: i64,
    config: CoordinatorLeaderConfig,
    revision: Revision,
    sessions: BTreeMap<NodeIncarnation, MemberSession>,
    claims: BTreeMap<PlacementSlotKey, ClaimLease>,
    loads: LoadTable,
    plans: BTreeMap<u128, RebalancePlan>,
    handoffs: BTreeMap<PlacementSlotKey, HandoffMachine>,
    operations: mpsc::Sender<CoordinatorOperation>,
    operation_receiver: mpsc::Receiver<CoordinatorOperation>,
    entity_configs: BTreeMap<lattice_core::actor_ref::EntityType, crate::region::EntityConfig>,
    singleton_configs: BTreeMap<lattice_core::actor_ref::SingletonKind, SingletonConfig>,
    strategies: BTreeMap<(String, u32), Arc<dyn ShardAllocationStrategy>>,
    origin: Instant,
    slot_assigned_at: BTreeMap<PlacementSlotKey, crate::types::MonotonicTime>,
    last_automatic_move_at: Option<crate::types::MonotonicTime>,
    node_load_received: BTreeMap<NodeIncarnation, crate::types::MonotonicTime>,
    shard_load_received: BTreeMap<
        (
            NodeIncarnation,
            lattice_core::actor_ref::EntityType,
            crate::types::ShardId,
        ),
        crate::types::MonotonicTime,
    >,
    automatic_globally_paused: bool,
    paused_entity_types: std::collections::BTreeSet<lattice_core::actor_ref::EntityType>,
    applied_admin_operations: BTreeMap<String, AppliedAdminOperation>,
}

impl<S: CoordinatorStore> CoordinatorLeader<S> {
    pub async fn elect(
        store: Arc<S>,
        associations: Arc<AssociationManager>,
        node: NodeKey,
        term: CoordinatorTerm,
        protocol_generation: u64,
        config: CoordinatorLeaderConfig,
    ) -> Result<Self, CoordinatorRuntimeError> {
        config.validate()?;
        if protocol_generation == 0 {
            return Err(CoordinatorRuntimeError::InvalidConfig);
        }
        store.ensure_schema_generation().await?;
        let leader_lease_id = store.grant_lease(config.leader_lease_ttl).await?;
        let leader = LeaderRecord {
            node,
            protocol_generation,
            term,
        };
        if !store.campaign_leader(&leader, leader_lease_id).await? {
            let _ = store.revoke_lease(leader_lease_id).await;
            return Err(CoordinatorRuntimeError::NotLeader);
        }
        let slots = store.list_slots().await?;
        let members = store.list_members().await?;
        let revision = slots
            .iter()
            .map(|slot| slot.revision)
            .chain(members.iter().map(|member| member.revision))
            .max()
            .unwrap_or(Revision::new(1).expect("one is a valid revision"));
        let loads = LoadTable::new(config.maximum_node_loads, config.maximum_shard_loads)
            .map_err(CoordinatorRuntimeError::Coordinator)?;
        let plans = store
            .list_plans()
            .await?
            .into_iter()
            .map(|plan| (plan.plan_id, plan))
            .collect();
        let (operations, operation_receiver) = mpsc::channel(config.maximum_operations);
        let default_strategy: Arc<dyn ShardAllocationStrategy> =
            Arc::new(WeightedLeastLoad::default());
        let mut strategies = BTreeMap::new();
        strategies.insert(
            (
                default_strategy.policy_id().to_owned(),
                default_strategy.policy_version(),
            ),
            default_strategy,
        );
        let slot_assigned_at = slots
            .iter()
            .filter(|slot| slot.state == PlacementSlotState::Running)
            .map(|slot| {
                (
                    slot.key.clone(),
                    crate::types::MonotonicTime::from_millis(0),
                )
            })
            .collect();
        let mut leader = Self {
            store,
            associations,
            leader,
            leader_lease_id,
            config,
            revision,
            sessions: BTreeMap::new(),
            claims: BTreeMap::new(),
            loads,
            plans,
            handoffs: BTreeMap::new(),
            operations,
            operation_receiver,
            entity_configs: BTreeMap::new(),
            singleton_configs: BTreeMap::new(),
            strategies,
            origin: Instant::now(),
            slot_assigned_at,
            last_automatic_move_at: None,
            node_load_received: BTreeMap::new(),
            shard_load_received: BTreeMap::new(),
            automatic_globally_paused: false,
            paused_entity_types: Default::default(),
            applied_admin_operations: BTreeMap::new(),
        };
        leader.recover_persisted_plans().await?;
        Ok(leader)
    }

    pub fn leader(&self) -> &LeaderRecord {
        &self.leader
    }

    pub fn handle(&self) -> CoordinatorHandle {
        CoordinatorHandle {
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
    Coordinator(#[source] crate::coordinator::CoordinatorError),
    #[error("Coordinator control codec failed")]
    Control(#[source] crate::control::PlacementControlError),
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
    #[error("placement singleton configuration is unknown")]
    UnknownSingletonConfig,
    #[error("placement configuration conflicts with an existing declaration")]
    ConfigurationConflict,
    #[error("allocation strategy is not registered")]
    UnknownStrategy,
    #[error("allocation strategy ID/version is already registered")]
    DuplicateStrategy,
    #[error("allocation strategy rejected the placement view")]
    Allocation(#[source] crate::allocation::AllocationError),
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
    Handoff(#[source] crate::handoff::HandoffError),
    #[error("Coordinator Association rejected reliable control admission")]
    Association(#[from] lattice_remoting::association::AssociationError),
}

#[cfg(test)]
mod tests;
