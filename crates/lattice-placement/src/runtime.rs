use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use lattice_core::actor_ref::NodeIncarnation;
use lattice_remoting::{Association, AssociationManager, AssociationState};
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
    LeaderRecord, LoadTable, NodeHello, SessionLimits, SingletonConfig, SnapshotLimits,
    SnapshotRecord, build_snapshot,
};
use crate::handoff::{HandoffEffect, HandoffEvent, HandoffMachine, HandoffPhase};
use crate::plan::{MoveProgress, PlanError, PlanReason, PlanStatus, RebalancePlan};
use crate::storage::{CoordinatorStore, StorageError};
use crate::types::{
    ClaimGrant, CoordinatorTerm, GrantSequence, NodeKey, PlacementSlot, PlacementSlotKey,
    PlacementSlotState, Revision,
};

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
    association: lattice_remoting::AssociationKey,
    lease_id: i64,
    heartbeat_sequence: u64,
    last_heartbeat: Instant,
    applied_revision: Option<Revision>,
    draining: bool,
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
        let revision = slots
            .iter()
            .map(|slot| slot.revision)
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

    async fn recover_persisted_plans(&mut self) -> Result<(), CoordinatorRuntimeError> {
        let plan_ids = self.plans.keys().copied().collect::<Vec<_>>();
        for plan_id in plan_ids {
            let mut plan = self
                .plans
                .get(&plan_id)
                .cloned()
                .ok_or(CoordinatorRuntimeError::UnknownPlan)?;
            let mut plan_changed = false;
            for movement in plan.moves.clone() {
                let key = PlacementSlotKey::Shard {
                    entity_type: plan.entity_type.clone(),
                    shard_id: movement.shard_id,
                };
                let Some(mut slot) = self.store.get_slot(&key).await? else {
                    if movement.progress == MoveProgress::Pending {
                        plan.cancel_pending_move(movement.shard_id)
                            .map_err(CoordinatorRuntimeError::Plan)?;
                        plan_changed = true;
                    }
                    continue;
                };
                match movement.progress {
                    MoveProgress::Pending => {
                        if slot.owner.as_ref() != Some(&movement.source)
                            || slot.assignment_generation != movement.expected_generation
                            || slot.state != PlacementSlotState::Running
                            || slot.active_move.is_some()
                        {
                            plan.cancel_pending_move(movement.shard_id)
                                .map_err(CoordinatorRuntimeError::Plan)?;
                            plan_changed = true;
                        }
                    }
                    MoveProgress::Handoff => {
                        if slot.state == PlacementSlotState::Running
                            && slot.owner.as_ref() == Some(&movement.target)
                            && slot.assignment_generation
                                == movement
                                    .expected_generation
                                    .next()
                                    .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?
                            && slot.active_move.is_none()
                        {
                            plan.complete_move(movement.shard_id)
                                .map_err(CoordinatorRuntimeError::Plan)?;
                            plan_changed = true;
                            continue;
                        }
                        let (barrier_revision, barrier_sessions) = if slot.state
                            == PlacementSlotState::Running
                            && slot.owner.as_ref() == Some(&movement.source)
                            && slot.assignment_generation == movement.expected_generation
                            && slot.active_move.is_none()
                        {
                            let barrier_revision = self
                                .revision
                                .next()
                                .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?;
                            let barrier_sessions = movement.barrier_sessions.clone();
                            if let Some(current) = plan
                                .moves
                                .iter_mut()
                                .find(|current| current.shard_id == movement.shard_id)
                            {
                                current.barrier_revision = Some(barrier_revision);
                            }
                            plan_changed = true;
                            let expected = slot.revision;
                            slot.target = Some(movement.target.clone());
                            slot.state = PlacementSlotState::BeginHandoff;
                            slot.active_move = Some(plan_id);
                            slot.barrier_sessions = barrier_sessions.clone();
                            slot.coordinator_term = self.leader.term;
                            slot.revision = barrier_revision;
                            self.store
                                .compare_and_put_slot(Some(expected), slot.clone())
                                .await?;
                            self.revision = barrier_revision;
                            (barrier_revision, barrier_sessions)
                        } else {
                            (slot.revision, slot.barrier_sessions.clone())
                        };
                        let handoff = HandoffMachine::recover(
                            &slot,
                            plan_id,
                            movement.source,
                            movement.target,
                            movement.expected_generation,
                            barrier_revision,
                            barrier_sessions,
                        )
                        .map_err(CoordinatorRuntimeError::Handoff)?;
                        self.handoffs.insert(key, handoff);
                    }
                    MoveProgress::Completed | MoveProgress::Cancelled | MoveProgress::Failed => {}
                }
            }
            if plan_changed {
                let expected = plan.revision;
                plan.revision = plan
                    .revision
                    .next()
                    .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?;
                self.store
                    .compare_and_put_plan(Some(expected), plan.clone(), plan.revision)
                    .await?;
                self.plans.insert(plan_id, plan);
            }
        }
        for slot in self.store.list_slots().await? {
            if !matches!(slot.key, PlacementSlotKey::Singleton(_))
                || slot.active_move.is_none()
                || self.handoffs.contains_key(&slot.key)
            {
                continue;
            }
            let plan_id = slot
                .active_move
                .ok_or(CoordinatorRuntimeError::StaleHandoff)?;
            let (source, target, source_generation) =
                if slot.state == PlacementSlotState::Allocating {
                    let target = slot
                        .owner
                        .clone()
                        .ok_or(CoordinatorRuntimeError::StaleHandoff)?;
                    let previous = slot
                        .assignment_generation
                        .get()
                        .checked_sub(1)
                        .and_then(|value| crate::types::AssignmentGeneration::new(value).ok())
                        .ok_or(CoordinatorRuntimeError::StaleHandoff)?;
                    (target.clone(), target, previous)
                } else {
                    (
                        slot.owner
                            .clone()
                            .ok_or(CoordinatorRuntimeError::StaleHandoff)?,
                        slot.target
                            .clone()
                            .ok_or(CoordinatorRuntimeError::StaleHandoff)?,
                        slot.assignment_generation,
                    )
                };
            let handoff = HandoffMachine::recover(
                &slot,
                plan_id,
                source,
                target,
                source_generation,
                slot.revision,
                slot.barrier_sessions.clone(),
            )
            .map_err(CoordinatorRuntimeError::Handoff)?;
            self.handoffs.insert(slot.key.clone(), handoff);
        }
        let live_members = self
            .store
            .list_members()
            .await?
            .into_iter()
            .map(|hello| hello.node.incarnation)
            .collect::<std::collections::BTreeSet<_>>();
        let keys = self.handoffs.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            let effects = {
                let handoff = self
                    .handoffs
                    .get_mut(&key)
                    .ok_or(CoordinatorRuntimeError::UnknownHandoff)?;
                let departed = handoff
                    .required_sessions()
                    .iter()
                    .filter(|session| !live_members.contains(session))
                    .copied()
                    .collect::<Vec<_>>();
                let mut effects = Vec::new();
                for session in departed {
                    effects.extend(
                        handoff
                            .transition(HandoffEvent::FenceSession(session))
                            .map_err(CoordinatorRuntimeError::Handoff)?,
                    );
                }
                effects.extend(handoff.start());
                effects
            };
            self.apply_handoff_effects(key, effects).await?;
        }
        self.compact_plan_history().await?;
        Ok(())
    }

    async fn compact_plan_history(&mut self) -> Result<(), CoordinatorRuntimeError> {
        let mut terminal = self
            .plans
            .values()
            .filter(|plan| {
                matches!(
                    plan.status,
                    PlanStatus::Completed | PlanStatus::Cancelled | PlanStatus::Failed
                )
            })
            .map(|plan| (plan.base_revision, plan.plan_id, plan.revision))
            .collect::<Vec<_>>();
        terminal.sort_unstable();
        let remove = terminal
            .len()
            .saturating_sub(self.config.maximum_completed_plan_history);
        for (_, plan_id, revision) in terminal.into_iter().take(remove) {
            self.store.delete_plan(plan_id, revision).await?;
            self.plans.remove(&plan_id);
        }
        Ok(())
    }

    pub async fn run(
        mut self,
        mut controls: mpsc::Receiver<PlacementControlEvent>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), CoordinatorRuntimeError> {
        let mut renewal = tokio::time::interval(self.config.renewal_interval);
        renewal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut rebalance = tokio::time::interval(self.config.rebalance_interval);
        rebalance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        rebalance.reset();
        loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        self.store.revoke_lease(self.leader_lease_id).await?;
                        return Ok(());
                    }
                }
                event = controls.recv() => {
                    let Some(event) = event else {
                        return Err(CoordinatorRuntimeError::ControlClosed);
                    };
                    let result = self.handle_control(event.kind).await;
                    let acknowledgement = result
                        .as_ref()
                        .map(|_| ())
                        .map_err(control_dispatch_error);
                    let _ = event.completion.send(acknowledgement);
                    result?;
                }
                operation = self.operation_receiver.recv() => {
                    let Some(operation) = operation else {
                        return Err(CoordinatorRuntimeError::OperationClosed);
                    };
                    self.handle_operation(operation).await;
                }
                _ = renewal.tick() => {
                    self.renew().await?;
                }
                _ = rebalance.tick() => {
                    let entity_types = self.entity_configs.keys().cloned().collect::<Vec<_>>();
                    for entity_type in entity_types {
                        let _ = self
                            .evaluate_rebalance(entity_type, RebalanceTrigger::Automatic)
                            .await;
                    }
                }
            }
        }
    }

    async fn handle_operation(&mut self, operation: CoordinatorOperation) {
        match operation {
            CoordinatorOperation::SubmitRebalance {
                proposal,
                entity_type,
                completion,
            } => {
                let result = self.submit_rebalance(proposal, entity_type).await;
                let _ = completion.send(result);
            }
            CoordinatorOperation::CancelPending {
                plan_id,
                shard_id,
                completion,
            } => {
                let result = self.cancel_pending(plan_id, shard_id).await;
                let _ = completion.send(result);
            }
            CoordinatorOperation::Evaluate {
                entity_type,
                trigger,
                completion,
            } => {
                let result = self.evaluate_rebalance(entity_type, trigger).await;
                let _ = completion.send(result);
            }
            CoordinatorOperation::SetAutomatic {
                operation_id,
                entity_type,
                paused,
                completion,
            } => {
                let result = self
                    .set_automatic_paused(operation_id, entity_type, paused)
                    .await;
                let _ = completion.send(result);
            }
            CoordinatorOperation::ManualRelocate {
                request,
                completion,
            } => {
                let result = self.manual_relocate(request).await;
                let _ = completion.send(result);
            }
            CoordinatorOperation::Inspect { completion } => {
                let result = self.inspect().await;
                let _ = completion.send(result);
            }
        }
    }

    async fn set_automatic_paused(
        &mut self,
        operation_id: String,
        entity_type: Option<lattice_core::actor_ref::EntityType>,
        paused: bool,
    ) -> Result<(), CoordinatorRuntimeError> {
        let fingerprint = format!(
            "automatic:{}:{}",
            entity_type.as_ref().map_or("*", |value| value.as_str()),
            paused
        );
        if self
            .prior_admin_operation(&operation_id, &fingerprint)?
            .is_some()
        {
            return Ok(());
        }
        match entity_type {
            Some(entity_type) if paused => {
                self.paused_entity_types.insert(entity_type);
            }
            Some(entity_type) => {
                self.paused_entity_types.remove(&entity_type);
            }
            None => self.automatic_globally_paused = paused,
        }
        self.record_admin_operation(operation_id, fingerprint, None)
    }

    async fn manual_relocate(
        &mut self,
        request: ManualRelocationRequest,
    ) -> Result<u128, CoordinatorRuntimeError> {
        let fingerprint = format!(
            "relocate:{}:{}:{}:{}",
            request.entity_type.as_str(),
            request.shard_id.get(),
            request.expected_generation.get(),
            request.target_node_id
        );
        if let Some(previous) = self.prior_admin_operation(&request.operation_id, &fingerprint)? {
            return previous.ok_or(CoordinatorRuntimeError::InvalidAdminOperation);
        }
        let key = PlacementSlotKey::Shard {
            entity_type: request.entity_type.clone(),
            shard_id: request.shard_id,
        };
        let slot = self
            .store
            .get_slot(&key)
            .await?
            .ok_or(CoordinatorRuntimeError::UnknownSlot)?;
        let source = slot
            .owner
            .clone()
            .ok_or(CoordinatorRuntimeError::StaleProposal)?;
        if slot.state != PlacementSlotState::Running
            || slot.assignment_generation != request.expected_generation
            || slot.active_move.is_some()
        {
            return Err(CoordinatorRuntimeError::StaleProposal);
        }
        let target = self
            .sessions
            .values()
            .find(|session| session.hello.node.node_id == request.target_node_id)
            .map(|session| session.hello.node.clone())
            .ok_or(CoordinatorRuntimeError::IneligibleTarget)?;
        let config = self
            .entity_configs
            .get(&request.entity_type)
            .ok_or(CoordinatorRuntimeError::UnknownEntityConfig)?;
        let strategy = self
            .strategies
            .get(&(
                config.allocation_policy_id.clone(),
                config.allocation_policy_version,
            ))
            .ok_or(CoordinatorRuntimeError::UnknownStrategy)?;
        let proposal = RebalanceProposal {
            policy_id: strategy.policy_id(),
            policy_version: strategy.policy_version(),
            base_revision: self.revision,
            trigger: RebalanceTrigger::Manual {
                source: Some(source.clone()),
                target: Some(target.clone()),
                bypass_improvement: true,
            },
            moves: vec![crate::allocation::ProposedMove {
                entity_type: request.entity_type.clone(),
                shard_id: request.shard_id,
                expected_generation: request.expected_generation,
                source,
                target,
                estimated_weight: 1,
            }],
        };
        let plan_id = self.submit_rebalance(proposal, request.entity_type).await?;
        self.record_admin_operation(request.operation_id, fingerprint, Some(plan_id))?;
        Ok(plan_id)
    }

    async fn inspect(&self) -> Result<CoordinatorInspection, CoordinatorRuntimeError> {
        Ok(CoordinatorInspection {
            term: self.leader.term,
            revision: self.revision,
            automatic_globally_paused: self.automatic_globally_paused,
            paused_entity_types: self.paused_entity_types.iter().cloned().collect(),
            slots: self.store.list_slots().await?,
            plans: self.store.list_plans().await?,
        })
    }

    fn prior_admin_operation(
        &self,
        operation_id: &str,
        fingerprint: &str,
    ) -> Result<Option<Option<u128>>, CoordinatorRuntimeError> {
        if operation_id.is_empty() || operation_id.len() > 256 {
            return Err(CoordinatorRuntimeError::InvalidAdminOperation);
        }
        self.applied_admin_operations
            .get(operation_id)
            .map(|previous| {
                if previous.fingerprint == fingerprint {
                    Ok(previous.plan_id)
                } else {
                    Err(CoordinatorRuntimeError::IdempotencyConflict)
                }
            })
            .transpose()
    }

    fn record_admin_operation(
        &mut self,
        operation_id: String,
        fingerprint: String,
        plan_id: Option<u128>,
    ) -> Result<(), CoordinatorRuntimeError> {
        if self.applied_admin_operations.len() == self.config.maximum_operations {
            return Err(CoordinatorRuntimeError::OperationCapacity);
        }
        self.applied_admin_operations.insert(
            operation_id,
            AppliedAdminOperation {
                fingerprint,
                plan_id,
            },
        );
        Ok(())
    }

    async fn evaluate_rebalance(
        &mut self,
        entity_type: lattice_core::actor_ref::EntityType,
        trigger: RebalanceTrigger,
    ) -> Result<Option<u128>, CoordinatorRuntimeError> {
        if trigger == RebalanceTrigger::Automatic
            && (self.automatic_globally_paused || self.paused_entity_types.contains(&entity_type))
        {
            return Err(CoordinatorRuntimeError::Allocation(
                crate::allocation::AllocationError::AutomaticPaused,
            ));
        }
        let config = self
            .entity_configs
            .get(&entity_type)
            .cloned()
            .ok_or(CoordinatorRuntimeError::UnknownEntityConfig)?;
        let strategy = self
            .strategies
            .get(&(
                config.allocation_policy_id.clone(),
                config.allocation_policy_version,
            ))
            .cloned()
            .ok_or(CoordinatorRuntimeError::UnknownStrategy)?;
        let view = self.placement_view().await?;
        let proposal = strategy
            .rebalance(
                &entity_type,
                config.protocol_id,
                trigger.clone(),
                &view,
                self.config.rebalance_limits,
            )
            .map_err(CoordinatorRuntimeError::Allocation)?;
        if proposal.moves.is_empty() {
            return Ok(None);
        }
        let plan_id = self.submit_rebalance(proposal, entity_type).await?;
        if trigger == RebalanceTrigger::Automatic {
            self.last_automatic_move_at = Some(view.now);
        }
        Ok(Some(plan_id))
    }

    async fn submit_rebalance(
        &mut self,
        proposal: RebalanceProposal,
        entity_type: lattice_core::actor_ref::EntityType,
    ) -> Result<u128, CoordinatorRuntimeError> {
        if proposal.base_revision != self.revision
            || proposal.moves.len() > self.config.rebalance_limits.moves_per_round
        {
            return Err(CoordinatorRuntimeError::StaleProposal);
        }
        let plan = RebalancePlan::from_proposal(
            proposal,
            entity_type.clone(),
            self.leader.term,
            self.config.maximum_plan_moves,
        )
        .map_err(CoordinatorRuntimeError::Plan)?;
        if plan.reason == PlanReason::Automatic
            && self.plans.values().any(|current| {
                current.entity_type == entity_type
                    && current.reason == PlanReason::Automatic
                    && matches!(current.status, PlanStatus::Planned | PlanStatus::Running)
            })
        {
            return Err(CoordinatorRuntimeError::PlanConflict);
        }
        self.preempt_lower_priority(plan.reason.clone()).await?;
        self.revalidate_plan(&plan).await?;
        self.store
            .compare_and_put_plan(None, plan.clone(), plan.revision)
            .await?;
        lattice_core::failpoint::hit(lattice_core::failpoint::Failpoint::RebalanceAfterPlanPersist);
        let plan_id = plan.plan_id;
        self.plans.insert(plan_id, plan);
        self.start_pending_moves(plan_id).await?;
        Ok(plan_id)
    }

    async fn preempt_lower_priority(
        &mut self,
        incoming: PlanReason,
    ) -> Result<(), CoordinatorRuntimeError> {
        let incoming_priority = plan_priority(&incoming);
        let candidates = self
            .plans
            .iter()
            .filter_map(|(plan_id, plan)| {
                (plan_priority(&plan.reason) > incoming_priority)
                    .then_some((*plan_id, plan.clone()))
            })
            .collect::<Vec<_>>();
        for (plan_id, mut plan) in candidates {
            let pending = plan
                .moves
                .iter()
                .filter(|movement| movement.progress == MoveProgress::Pending)
                .map(|movement| movement.shard_id)
                .collect::<Vec<_>>();
            if pending.is_empty() {
                continue;
            }
            let expected = plan.revision;
            for shard_id in pending {
                plan.cancel_pending_move(shard_id)
                    .map_err(CoordinatorRuntimeError::Plan)?;
            }
            plan.revision = plan
                .revision
                .next()
                .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?;
            self.store
                .compare_and_put_plan(Some(expected), plan.clone(), plan.revision)
                .await?;
            self.plans.insert(plan_id, plan);
        }
        self.compact_plan_history().await?;
        Ok(())
    }

    async fn revalidate_plan(&self, plan: &RebalancePlan) -> Result<(), CoordinatorRuntimeError> {
        let mut target_reservations = BTreeMap::<NodeKey, u64>::new();
        for current in self.plans.values() {
            for movement in &current.moves {
                if matches!(
                    movement.progress,
                    MoveProgress::Pending | MoveProgress::Handoff
                ) {
                    *target_reservations
                        .entry(movement.target.clone())
                        .or_default() += movement.estimated_weight;
                }
            }
        }
        for movement in &plan.moves {
            let key = PlacementSlotKey::Shard {
                entity_type: plan.entity_type.clone(),
                shard_id: movement.shard_id,
            };
            let slot = self
                .store
                .get_slot(&key)
                .await?
                .ok_or(CoordinatorRuntimeError::UnknownSlot)?;
            if slot.revision > plan.base_revision
                || slot.owner.as_ref() != Some(&movement.source)
                || slot.assignment_generation != movement.expected_generation
                || slot.state != PlacementSlotState::Running
                || slot.active_move.is_some()
            {
                return Err(CoordinatorRuntimeError::StaleProposal);
            }
            let target_session = self
                .sessions
                .get(&movement.target.incarnation)
                .filter(|session| {
                    session.hello.node == movement.target
                        && session
                            .hello
                            .hosted_entity_types
                            .contains(&plan.entity_type)
                })
                .ok_or(CoordinatorRuntimeError::IneligibleTarget)?;
            let reservation = target_reservations
                .entry(movement.target.clone())
                .or_default();
            *reservation = reservation.saturating_add(movement.estimated_weight);
            if *reservation > target_session.hello.capacity_units {
                return Err(CoordinatorRuntimeError::ConcurrencyLimit);
            }
        }
        Ok(())
    }

    async fn cancel_pending(
        &mut self,
        plan_id: u128,
        shard_id: crate::types::ShardId,
    ) -> Result<(), CoordinatorRuntimeError> {
        let mut plan = self
            .plans
            .get(&plan_id)
            .cloned()
            .ok_or(CoordinatorRuntimeError::UnknownPlan)?;
        let expected = plan.revision;
        plan.cancel_pending_move(shard_id)
            .map_err(CoordinatorRuntimeError::Plan)?;
        plan.revision = plan
            .revision
            .next()
            .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?;
        self.store
            .compare_and_put_plan(Some(expected), plan.clone(), plan.revision)
            .await?;
        self.plans.insert(plan_id, plan);
        self.compact_plan_history().await?;
        Ok(())
    }

    async fn start_pending_moves(&mut self, plan_id: u128) -> Result<(), CoordinatorRuntimeError> {
        let shards = self
            .plans
            .get(&plan_id)
            .ok_or(CoordinatorRuntimeError::UnknownPlan)?
            .moves
            .iter()
            .filter(|movement| movement.progress == MoveProgress::Pending)
            .map(|movement| movement.shard_id)
            .collect::<Vec<_>>();
        for shard_id in shards {
            if !self.can_start_move(plan_id, shard_id)? {
                continue;
            }
            self.begin_move(plan_id, shard_id).await?;
        }
        Ok(())
    }

    fn can_start_move(
        &self,
        plan_id: u128,
        shard_id: crate::types::ShardId,
    ) -> Result<bool, CoordinatorRuntimeError> {
        let plan = self
            .plans
            .get(&plan_id)
            .ok_or(CoordinatorRuntimeError::UnknownPlan)?;
        let movement = plan
            .moves
            .iter()
            .find(|movement| movement.shard_id == shard_id)
            .ok_or(CoordinatorRuntimeError::UnknownSlot)?;
        let limits = self.config.rebalance_limits;
        let active = self
            .plans
            .values()
            .flat_map(|plan| {
                plan.moves
                    .iter()
                    .filter(|movement| movement.progress == MoveProgress::Handoff)
                    .map(move |movement| (&plan.entity_type, movement))
            })
            .collect::<Vec<_>>();
        Ok(active.len() < limits.concurrent_cluster
            && active
                .iter()
                .filter(|(entity, _)| *entity == &plan.entity_type)
                .count()
                < limits.concurrent_entity
            && active
                .iter()
                .filter(|(_, active)| active.source == movement.source)
                .count()
                < limits.concurrent_source
            && active
                .iter()
                .filter(|(_, active)| active.target == movement.target)
                .count()
                < limits.concurrent_target)
    }

    async fn begin_move(
        &mut self,
        plan_id: u128,
        shard_id: crate::types::ShardId,
    ) -> Result<(), CoordinatorRuntimeError> {
        let mut plan = self
            .plans
            .get(&plan_id)
            .cloned()
            .ok_or(CoordinatorRuntimeError::UnknownPlan)?;
        let movement = plan
            .moves
            .iter()
            .find(|movement| movement.shard_id == shard_id)
            .cloned()
            .ok_or(CoordinatorRuntimeError::UnknownSlot)?;
        let key = PlacementSlotKey::Shard {
            entity_type: plan.entity_type.clone(),
            shard_id,
        };
        let mut slot = self
            .store
            .get_slot(&key)
            .await?
            .ok_or(CoordinatorRuntimeError::UnknownSlot)?;
        plan.begin_move(shard_id, slot.assignment_generation, slot.active_move)
            .map_err(CoordinatorRuntimeError::Plan)?;
        let barrier_revision = self
            .revision
            .next()
            .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?;
        let barrier_sessions: std::collections::BTreeSet<NodeIncarnation> = self
            .sessions
            .iter()
            .filter_map(|(incarnation, session)| {
                session
                    .hello
                    .subscribes_to(&plan.entity_type)
                    .then_some(*incarnation)
            })
            .collect();
        plan.install_barrier(shard_id, barrier_revision, barrier_sessions.clone())
            .map_err(CoordinatorRuntimeError::Plan)?;
        let expected_plan_revision = plan.revision;
        plan.revision = plan
            .revision
            .next()
            .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?;
        self.store
            .compare_and_put_plan(Some(expected_plan_revision), plan.clone(), plan.revision)
            .await?;
        lattice_core::failpoint::hit(lattice_core::failpoint::Failpoint::RebalanceAfterPlanPersist);
        let expected_slot_revision = slot.revision;
        slot.target = Some(movement.target.clone());
        slot.state = PlacementSlotState::BeginHandoff;
        slot.active_move = Some(plan_id);
        slot.barrier_sessions = barrier_sessions.clone();
        slot.coordinator_term = self.leader.term;
        slot.revision = barrier_revision;
        self.store
            .compare_and_put_slot(Some(expected_slot_revision), slot.clone())
            .await?;
        lattice_core::failpoint::hit(
            lattice_core::failpoint::Failpoint::RebalanceAfterReservationBeforeHandoff,
        );
        lattice_core::failpoint::hit(lattice_core::failpoint::Failpoint::HandoffAfterBeginPersist);
        self.revision = barrier_revision;
        let mut handoff = HandoffMachine::begin(
            key.clone(),
            plan_id,
            movement.source,
            movement.target,
            movement.expected_generation,
            barrier_revision,
            barrier_sessions,
        )
        .map_err(CoordinatorRuntimeError::Handoff)?;
        let effects = handoff.start();
        self.plans.insert(plan_id, plan);
        self.handoffs.insert(key.clone(), handoff);
        self.publish_slot_delta(&slot).await?;
        lattice_core::failpoint::hit(
            lattice_core::failpoint::Failpoint::HandoffAfterPartialBarrier,
        );
        Box::pin(self.apply_handoff_effects(key, effects)).await
    }

    async fn publish_slot_delta(
        &self,
        slot: &PlacementSlot,
    ) -> Result<(), CoordinatorRuntimeError> {
        lattice_core::failpoint::hit(
            lattice_core::failpoint::Failpoint::CoordinatorAfterEtcdCommitBeforeDelta,
        );
        let record = SnapshotRecord {
            key: slot_record_key(&slot.key),
            value: Bytes::from(
                serde_json::to_vec(slot).map_err(|_| CoordinatorRuntimeError::Codec)?,
            ),
        };
        for session in self.sessions.values() {
            let include = match &slot.key {
                PlacementSlotKey::Shard { entity_type, .. } => {
                    session.hello.subscribes_to(entity_type)
                }
                PlacementSlotKey::Singleton(kind) => {
                    session.hello.singleton_eligibility.contains(kind)
                        || session.hello.used_singletons.contains(kind)
                }
            };
            let delta = crate::coordinator::CoordinatorDelta {
                revision: slot.revision,
                records: include.then_some(record.clone()).into_iter().collect(),
            };
            let association = self
                .associations
                .get(&session.association)
                .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
            send_control(
                &association,
                PlacementControlCommand::StateDelta(delta),
                &self.config,
            )?;
        }
        Ok(())
    }

    async fn transition_handoff(
        &mut self,
        key: PlacementSlotKey,
        event: HandoffEvent,
    ) -> Result<(), CoordinatorRuntimeError> {
        let effects = self
            .handoffs
            .get_mut(&key)
            .ok_or(CoordinatorRuntimeError::UnknownHandoff)?
            .transition(event)
            .map_err(CoordinatorRuntimeError::Handoff)?;
        self.apply_handoff_effects(key, effects).await
    }

    async fn apply_handoff_effects(
        &mut self,
        key: PlacementSlotKey,
        effects: Vec<HandoffEffect>,
    ) -> Result<(), CoordinatorRuntimeError> {
        for effect in effects {
            match effect {
                HandoffEffect::DrainSource => self.drain_source(&key).await?,
                HandoffEffect::ReplaceAuthority => self.replace_authority(&key).await?,
                HandoffEffect::PublishActive => self.publish_active(&key).await?,
                HandoffEffect::StopFailed => self.record_stop_failed(&key).await?,
            }
        }
        Ok(())
    }

    async fn drain_source(
        &mut self,
        key: &PlacementSlotKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        let handoff = self
            .handoffs
            .get(key)
            .ok_or(CoordinatorRuntimeError::UnknownHandoff)?;
        let source = handoff.source.clone();
        let generation = handoff.source_generation;
        let plan_id = handoff.plan_id;
        let mut slot = self
            .store
            .get_slot(key)
            .await?
            .ok_or(CoordinatorRuntimeError::UnknownSlot)?;
        if slot.active_move != Some(handoff.plan_id)
            || slot.state != PlacementSlotState::BeginHandoff
        {
            return Err(CoordinatorRuntimeError::StaleHandoff);
        }
        let expected = slot.revision;
        slot.state = PlacementSlotState::Stopping;
        slot.revision = self
            .revision
            .next()
            .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?;
        self.store
            .compare_and_put_slot(Some(expected), slot.clone())
            .await?;
        self.revision = slot.revision;
        self.publish_slot_delta(&slot).await?;
        if let Some(session) = self
            .sessions
            .get(&source.incarnation)
            .filter(|session| session.hello.node == source)
        {
            let association = self
                .associations
                .get(&session.association)
                .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
            send_control(
                &association,
                PlacementControlCommand::DrainSlot {
                    slot: key.clone(),
                    generation,
                    revision: slot.revision,
                },
                &self.config,
            )?;
            lattice_core::failpoint::hit(lattice_core::failpoint::Failpoint::HandoffAfterDrainSend);
        }
        let recovery = self
            .plans
            .get(&plan_id)
            .is_some_and(|plan| plan.reason == PlanReason::Recovery)
            || (matches!(key, PlacementSlotKey::Singleton(_))
                && !self.sessions.contains_key(&source.incarnation));
        if recovery && self.store.get_claim(key).await?.is_none() {
            let effects = self
                .handoffs
                .get_mut(key)
                .ok_or(CoordinatorRuntimeError::UnknownHandoff)?
                .transition(HandoffEvent::SourceAuthorityInvalid { source, generation })
                .map_err(CoordinatorRuntimeError::Handoff)?;
            Box::pin(self.apply_handoff_effects(key.clone(), effects)).await?;
        }
        Ok(())
    }

    async fn record_stop_failed(
        &mut self,
        key: &PlacementSlotKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        let mut slot = self
            .store
            .get_slot(key)
            .await?
            .ok_or(CoordinatorRuntimeError::UnknownSlot)?;
        if slot.state != PlacementSlotState::Stopping {
            return Err(CoordinatorRuntimeError::StaleHandoff);
        }
        let expected = slot.revision;
        slot.state = PlacementSlotState::StopFailed;
        slot.revision = self
            .revision
            .next()
            .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?;
        self.store
            .compare_and_put_slot(Some(expected), slot.clone())
            .await?;
        self.revision = slot.revision;
        self.publish_slot_delta(&slot).await
    }

    async fn replace_authority(
        &mut self,
        key: &PlacementSlotKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        let handoff = self
            .handoffs
            .get(key)
            .cloned()
            .ok_or(CoordinatorRuntimeError::UnknownHandoff)?;
        let mut slot = self
            .store
            .get_slot(key)
            .await?
            .ok_or(CoordinatorRuntimeError::UnknownSlot)?;
        if slot.state != PlacementSlotState::Fenced {
            lattice_core::failpoint::hit(
                lattice_core::failpoint::Failpoint::HandoffAfterShardDrainedBeforeClaimRevoke,
            );
            if let Some(old_claim) = self.store.get_claim(key).await? {
                if old_claim.owner != handoff.source
                    || old_claim.assignment_generation != handoff.source_generation
                {
                    return Err(CoordinatorRuntimeError::ClaimNotProven);
                }
                self.store.delete_claim(&old_claim).await?;
            }
            if let Some(claim) = self.claims.remove(key) {
                let _ = self.store.revoke_lease(claim.lease_id).await;
            }
        }
        if !matches!(
            slot.state,
            PlacementSlotState::Stopping
                | PlacementSlotState::StopFailed
                | PlacementSlotState::Fenced
        ) || slot.active_move != Some(handoff.plan_id)
        {
            return Err(CoordinatorRuntimeError::StaleHandoff);
        }
        if slot.state != PlacementSlotState::Fenced {
            let expected = slot.revision;
            slot.state = PlacementSlotState::Fenced;
            slot.revision = self
                .revision
                .next()
                .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?;
            self.store
                .compare_and_put_slot(Some(expected), slot.clone())
                .await?;
            self.revision = slot.revision;
            self.publish_slot_delta(&slot).await?;
        }

        let expected = slot.revision;
        slot.owner = Some(handoff.target.clone());
        slot.target = None;
        slot.assignment_generation = handoff.target_generation;
        slot.state = PlacementSlotState::Allocating;
        slot.coordinator_term = self.leader.term;
        slot.revision = self
            .revision
            .next()
            .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?;
        self.store
            .compare_and_put_slot(Some(expected), slot.clone())
            .await?;
        self.revision = slot.revision;
        self.publish_slot_delta(&slot).await?;
        let lease_id = self.store.grant_lease(self.config.claim_ttl).await?;
        let grant = ClaimGrant {
            slot: key.clone(),
            owner: handoff.target.clone(),
            coordinator_term: self.leader.term,
            assignment_generation: handoff.target_generation,
            grant_sequence: GrantSequence::new(1).expect("one is a valid grant sequence"),
            ttl: self.config.claim_ttl,
        };
        self.store.put_claim(&grant, lease_id).await?;
        lattice_core::failpoint::hit(
            lattice_core::failpoint::Failpoint::HandoffAfterNewClaimBeforeGrantSend,
        );
        self.claims.insert(
            key.clone(),
            ClaimLease {
                lease_id,
                grant: grant.clone(),
            },
        );
        let session = self
            .sessions
            .get(&handoff.target.incarnation)
            .filter(|session| session.hello.node == handoff.target)
            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
        let association = self
            .associations
            .get(&session.association)
            .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
        send_control(
            &association,
            PlacementControlCommand::ClaimGranted(grant),
            &self.config,
        )?;
        lattice_core::failpoint::hit(
            lattice_core::failpoint::Failpoint::HandoffAfterGrantBeforeShardReady,
        );
        let effects = self
            .handoffs
            .get_mut(key)
            .ok_or(CoordinatorRuntimeError::UnknownHandoff)?
            .transition(HandoffEvent::TargetClaimInstalled {
                target: handoff.target,
                generation: handoff.target_generation,
            })
            .map_err(CoordinatorRuntimeError::Handoff)?;
        if effects.is_empty() {
            Ok(())
        } else {
            Err(CoordinatorRuntimeError::StaleHandoff)
        }
    }

    async fn publish_active(
        &mut self,
        key: &PlacementSlotKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        let handoff = self
            .handoffs
            .get(key)
            .cloned()
            .ok_or(CoordinatorRuntimeError::UnknownHandoff)?;
        let mut slot = self
            .store
            .get_slot(key)
            .await?
            .ok_or(CoordinatorRuntimeError::UnknownSlot)?;
        if slot.state != PlacementSlotState::Allocating
            || slot.owner.as_ref() != Some(&handoff.target)
            || slot.assignment_generation != handoff.target_generation
            || slot.active_move != Some(handoff.plan_id)
        {
            return Err(CoordinatorRuntimeError::StaleHandoff);
        }
        let expected = slot.revision;
        slot.state = PlacementSlotState::Running;
        slot.active_move = None;
        slot.barrier_sessions.clear();
        slot.revision = self
            .revision
            .next()
            .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?;
        self.store
            .compare_and_put_slot(Some(expected), slot.clone())
            .await?;
        lattice_core::failpoint::hit(
            lattice_core::failpoint::Failpoint::HandoffAfterActivePersistBeforeDelta,
        );
        self.revision = slot.revision;
        self.slot_assigned_at.insert(key.clone(), self.now());
        self.handoffs.remove(key);
        self.publish_slot_delta(&slot).await?;
        if let PlacementSlotKey::Shard { shard_id, .. } = key {
            let mut plan = self
                .plans
                .get(&handoff.plan_id)
                .cloned()
                .ok_or(CoordinatorRuntimeError::UnknownPlan)?;
            let expected_plan_revision = plan.revision;
            plan.complete_move(*shard_id)
                .map_err(CoordinatorRuntimeError::Plan)?;
            plan.revision = plan
                .revision
                .next()
                .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?;
            self.store
                .compare_and_put_plan(Some(expected_plan_revision), plan.clone(), plan.revision)
                .await?;
            self.plans.insert(plan.plan_id, plan);
            self.start_pending_moves(handoff.plan_id).await?;
            self.compact_plan_history().await?;
        }
        Ok(())
    }

    async fn handle_control(
        &mut self,
        event: crate::control::PlacementControlEventKind,
    ) -> Result<(), CoordinatorRuntimeError> {
        match event {
            crate::control::PlacementControlEventKind::Reconcile { association, .. } => {
                if let Some(session) = self.sessions.get(&association.remote_incarnation) {
                    self.send_snapshot(session.hello.clone(), association)
                        .await?;
                }
            }
            crate::control::PlacementControlEventKind::Command(inbound) => {
                let remote = inbound.association.remote_incarnation;
                match inbound.command {
                    PlacementControlCommand::NodeHello(hello) => {
                        self.register(hello, inbound.association).await?;
                    }
                    PlacementControlCommand::NodeHeartbeat {
                        incarnation,
                        sequence,
                    } => {
                        if incarnation != remote || sequence == 0 {
                            return Err(CoordinatorRuntimeError::UnauthorizedCommand);
                        }
                        let session = self
                            .sessions
                            .get_mut(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
                        if sequence > session.heartbeat_sequence {
                            session.heartbeat_sequence = sequence;
                            session.last_heartbeat = Instant::now();
                            self.store.keep_lease_alive(session.lease_id).await?;
                        }
                    }
                    PlacementControlCommand::AppliedRevision(revision) => {
                        let session = self
                            .sessions
                            .get_mut(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
                        if session
                            .applied_revision
                            .is_none_or(|current| revision > current)
                        {
                            session.applied_revision = Some(revision);
                        }
                        let barriers = self
                            .handoffs
                            .iter()
                            .filter_map(|(key, handoff)| {
                                (handoff.phase == HandoffPhase::Invalidating
                                    && handoff.required_sessions().contains(&remote)
                                    && revision >= handoff.barrier_revision())
                                .then_some(key.clone())
                            })
                            .collect::<Vec<_>>();
                        for key in barriers {
                            self.transition_handoff(
                                key,
                                HandoffEvent::AppliedRevision {
                                    session: remote,
                                    revision,
                                },
                            )
                            .await?;
                        }
                    }
                    PlacementControlCommand::NodeLoad(report) => {
                        if self
                            .sessions
                            .get(&remote)
                            .is_none_or(|session| session.hello.node != report.node)
                        {
                            return Err(CoordinatorRuntimeError::UnauthorizedCommand);
                        }
                        let received = self.now();
                        if self
                            .loads
                            .report_node(report)
                            .map_err(CoordinatorRuntimeError::Coordinator)?
                        {
                            self.node_load_received.insert(remote, received);
                        }
                    }
                    PlacementControlCommand::ShardLoad(report) => {
                        if self
                            .sessions
                            .get(&remote)
                            .is_none_or(|session| session.hello.node != report.node)
                        {
                            return Err(CoordinatorRuntimeError::UnauthorizedCommand);
                        }
                        let received = self.now();
                        let key = (remote, report.entity_type.clone(), report.shard_id);
                        if self
                            .loads
                            .report_shard(report)
                            .map_err(CoordinatorRuntimeError::Coordinator)?
                        {
                            self.shard_load_received.insert(key, received);
                        }
                    }
                    PlacementControlCommand::SubscribeEntity(entity_type) => {
                        let session = self
                            .sessions
                            .get_mut(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
                        session.hello.proxied_entity_types.insert(entity_type);
                        let hello = session.hello.clone();
                        let association = session.association.clone();
                        self.send_snapshot(hello, association).await?;
                    }
                    PlacementControlCommand::SubscribeSingleton(kind) => {
                        let session = self
                            .sessions
                            .get_mut(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
                        session.hello.used_singletons.insert(kind);
                        let hello = session.hello.clone();
                        let association = session.association.clone();
                        self.send_snapshot(hello, association).await?;
                    }
                    PlacementControlCommand::SlotDrained { slot, generation } => {
                        let source = self
                            .sessions
                            .get(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?
                            .hello
                            .node
                            .clone();
                        self.transition_handoff(
                            slot,
                            HandoffEvent::SourceDrained { source, generation },
                        )
                        .await?;
                    }
                    PlacementControlCommand::SlotStopFailed { slot, generation } => {
                        let source = self
                            .sessions
                            .get(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?
                            .hello
                            .node
                            .clone();
                        self.transition_handoff(
                            slot,
                            HandoffEvent::SourceStopFailed { source, generation },
                        )
                        .await?;
                    }
                    PlacementControlCommand::SlotReady { slot, generation } => {
                        let target = self
                            .sessions
                            .get(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?
                            .hello
                            .node
                            .clone();
                        if self.handoffs.contains_key(&slot) {
                            self.transition_handoff(
                                slot,
                                HandoffEvent::TargetReady { target, generation },
                            )
                            .await?;
                        } else {
                            self.complete_initial_ready(&slot, &target, generation)
                                .await?;
                        }
                    }
                    PlacementControlCommand::BeginDrain => {
                        let session = self
                            .sessions
                            .get_mut(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
                        session.draining = true;
                        let source = session.hello.node.clone();
                        let entity_types = session
                            .hello
                            .hosted_entity_types
                            .iter()
                            .cloned()
                            .collect::<Vec<_>>();
                        for entity_type in entity_types {
                            let _ = self
                                .evaluate_rebalance(
                                    entity_type,
                                    RebalanceTrigger::Drain {
                                        node: source.clone(),
                                    },
                                )
                                .await?;
                        }
                    }
                    PlacementControlCommand::DrainComplete => {
                        if !self.sessions.contains_key(&remote) {
                            return Err(CoordinatorRuntimeError::UnknownSession);
                        }
                    }
                    PlacementControlCommand::ResolveShard {
                        entity_type,
                        shard_id,
                        ..
                    } => {
                        let session = self
                            .sessions
                            .get(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
                        if !session.hello.subscribes_to(&entity_type) {
                            return Err(CoordinatorRuntimeError::UnauthorizedCommand);
                        }
                        let hello = session.hello.clone();
                        let association = session.association.clone();
                        self.ensure_shard_allocated(entity_type, shard_id).await?;
                        self.send_snapshot(hello, association).await?;
                    }
                    PlacementControlCommand::ResolveSingleton { kind, .. } => {
                        let session = self
                            .sessions
                            .get(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
                        if !session.hello.used_singletons.contains(&kind)
                            && !session.hello.singleton_eligibility.contains(&kind)
                        {
                            return Err(CoordinatorRuntimeError::UnauthorizedCommand);
                        }
                        let hello = session.hello.clone();
                        let association = session.association.clone();
                        self.ensure_singleton_allocated(kind).await?;
                        self.send_snapshot(hello, association).await?;
                    }
                    PlacementControlCommand::SnapshotBegin(_)
                    | PlacementControlCommand::SnapshotChunk(_)
                    | PlacementControlCommand::SnapshotEnd(_)
                    | PlacementControlCommand::StateDelta(_)
                    | PlacementControlCommand::ClaimGranted(_)
                    | PlacementControlCommand::NodeRemoved(_)
                    | PlacementControlCommand::DrainSlot { .. } => {
                        return Err(CoordinatorRuntimeError::UnauthorizedCommand);
                    }
                }
            }
        }
        Ok(())
    }

    async fn ensure_shard_allocated(
        &mut self,
        entity_type: lattice_core::actor_ref::EntityType,
        shard_id: crate::types::ShardId,
    ) -> Result<(), CoordinatorRuntimeError> {
        let key = PlacementSlotKey::Shard {
            entity_type: entity_type.clone(),
            shard_id,
        };
        if let Some(slot) = self.store.get_slot(&key).await? {
            return if matches!(
                slot.state,
                PlacementSlotState::Allocating | PlacementSlotState::Running
            ) {
                Ok(())
            } else {
                Err(CoordinatorRuntimeError::StaleHandoff)
            };
        }
        let config = self
            .entity_configs
            .get(&entity_type)
            .cloned()
            .ok_or(CoordinatorRuntimeError::UnknownEntityConfig)?;
        let strategy = self
            .strategies
            .get(&(
                config.allocation_policy_id.clone(),
                config.allocation_policy_version,
            ))
            .cloned()
            .ok_or(CoordinatorRuntimeError::UnknownStrategy)?;
        let view = self.placement_view().await?;
        let decision = strategy
            .allocate(
                &AllocationRequest {
                    entity_type,
                    shard_id,
                    required_protocol: config.protocol_id,
                },
                &view,
            )
            .map_err(CoordinatorRuntimeError::Allocation)?;
        let slot = PlacementSlot {
            key,
            config_fingerprint: config.fingerprint(),
            owner: Some(decision.target),
            target: None,
            assignment_generation: crate::types::AssignmentGeneration::new(1)
                .expect("one is a valid assignment generation"),
            coordinator_term: self.leader.term,
            revision: self
                .revision
                .next()
                .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?,
            state: PlacementSlotState::Allocating,
            active_move: None,
            barrier_sessions: Default::default(),
        };
        self.persist_initial_allocation(slot).await
    }

    async fn ensure_singleton_allocated(
        &mut self,
        kind: lattice_core::actor_ref::SingletonKind,
    ) -> Result<(), CoordinatorRuntimeError> {
        let key = PlacementSlotKey::Singleton(kind.clone());
        if let Some(slot) = self.store.get_slot(&key).await? {
            return if matches!(
                slot.state,
                PlacementSlotState::Allocating | PlacementSlotState::Running
            ) {
                Ok(())
            } else {
                Err(CoordinatorRuntimeError::StaleHandoff)
            };
        }
        let config = self
            .singleton_configs
            .get(&kind)
            .cloned()
            .ok_or(CoordinatorRuntimeError::UnknownSingletonConfig)?;
        let target = self.select_singleton_target(&kind, &config, None)?;
        let slot = PlacementSlot {
            key,
            config_fingerprint: config.config_fingerprint,
            owner: Some(target),
            target: None,
            assignment_generation: crate::types::AssignmentGeneration::new(1)
                .expect("one is a valid assignment generation"),
            coordinator_term: self.leader.term,
            revision: self
                .revision
                .next()
                .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?,
            state: PlacementSlotState::Allocating,
            active_move: None,
            barrier_sessions: Default::default(),
        };
        self.persist_initial_allocation(slot).await
    }

    fn select_singleton_target(
        &self,
        kind: &lattice_core::actor_ref::SingletonKind,
        config: &SingletonConfig,
        exclude: Option<&NodeKey>,
    ) -> Result<NodeKey, CoordinatorRuntimeError> {
        self.sessions
            .values()
            .filter(|session| {
                !session.draining
                    && exclude != Some(&session.hello.node)
                    && session.hello.singleton_eligibility.contains(kind)
                    && session.hello.singleton_configs.contains(config)
                    && session
                        .hello
                        .protocols
                        .iter()
                        .any(|protocol| protocol.protocol_id == config.protocol_id)
            })
            .map(|session| session.hello.node.clone())
            .min()
            .ok_or(CoordinatorRuntimeError::IneligibleTarget)
    }

    async fn begin_singleton_recovery(
        &mut self,
        mut slot: PlacementSlot,
    ) -> Result<(), CoordinatorRuntimeError> {
        let PlacementSlotKey::Singleton(kind) = &slot.key else {
            return Err(CoordinatorRuntimeError::UnknownSlot);
        };
        if slot.state != PlacementSlotState::Running || slot.active_move.is_some() {
            return Ok(());
        }
        let source = slot
            .owner
            .clone()
            .ok_or(CoordinatorRuntimeError::UnknownSlot)?;
        let config = self
            .singleton_configs
            .get(kind)
            .cloned()
            .ok_or(CoordinatorRuntimeError::UnknownSingletonConfig)?;
        let target = self.select_singleton_target(kind, &config, Some(&source))?;
        let plan_id = uuid::Uuid::new_v4().as_u128();
        let barrier_revision = self
            .revision
            .next()
            .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?;
        let barrier_sessions = self
            .sessions
            .iter()
            .filter_map(|(incarnation, session)| {
                (session.hello.used_singletons.contains(kind)
                    || session.hello.singleton_eligibility.contains(kind))
                .then_some(*incarnation)
            })
            .collect::<std::collections::BTreeSet<_>>();
        let expected = slot.revision;
        slot.target = Some(target.clone());
        slot.state = PlacementSlotState::BeginHandoff;
        slot.active_move = Some(plan_id);
        slot.barrier_sessions = barrier_sessions.clone();
        slot.coordinator_term = self.leader.term;
        slot.revision = barrier_revision;
        self.store
            .compare_and_put_slot(Some(expected), slot.clone())
            .await?;
        self.revision = barrier_revision;
        let mut handoff = HandoffMachine::begin(
            slot.key.clone(),
            plan_id,
            source,
            target,
            slot.assignment_generation,
            barrier_revision,
            barrier_sessions,
        )
        .map_err(CoordinatorRuntimeError::Handoff)?;
        let effects = handoff.start();
        self.handoffs.insert(slot.key.clone(), handoff);
        self.publish_slot_delta(&slot).await?;
        Box::pin(self.apply_handoff_effects(slot.key, effects)).await
    }

    async fn persist_initial_allocation(
        &mut self,
        slot: PlacementSlot,
    ) -> Result<(), CoordinatorRuntimeError> {
        let owner = slot
            .owner
            .clone()
            .ok_or(CoordinatorRuntimeError::IneligibleTarget)?;
        self.store.compare_and_put_slot(None, slot.clone()).await?;
        self.revision = slot.revision;
        self.publish_slot_delta(&slot).await?;
        let lease_id = self.store.grant_lease(self.config.claim_ttl).await?;
        let grant = ClaimGrant {
            slot: slot.key.clone(),
            owner: owner.clone(),
            coordinator_term: self.leader.term,
            assignment_generation: slot.assignment_generation,
            grant_sequence: GrantSequence::new(1).expect("one is a valid grant sequence"),
            ttl: self.config.claim_ttl,
        };
        self.store.put_claim(&grant, lease_id).await?;
        self.claims.insert(
            slot.key.clone(),
            ClaimLease {
                lease_id,
                grant: grant.clone(),
            },
        );
        let session = self
            .sessions
            .get(&owner.incarnation)
            .filter(|session| session.hello.node == owner)
            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
        let association = self
            .associations
            .get(&session.association)
            .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
        send_control(
            &association,
            PlacementControlCommand::ClaimGranted(grant),
            &self.config,
        )
    }

    async fn complete_initial_ready(
        &mut self,
        key: &PlacementSlotKey,
        owner: &NodeKey,
        generation: crate::types::AssignmentGeneration,
    ) -> Result<(), CoordinatorRuntimeError> {
        let mut slot = self
            .store
            .get_slot(key)
            .await?
            .ok_or(CoordinatorRuntimeError::UnknownSlot)?;
        if slot.state != PlacementSlotState::Allocating
            || slot.owner.as_ref() != Some(owner)
            || slot.assignment_generation != generation
            || slot.active_move.is_some()
        {
            return Err(CoordinatorRuntimeError::StaleHandoff);
        }
        let expected = slot.revision;
        slot.state = PlacementSlotState::Running;
        slot.revision = self
            .revision
            .next()
            .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?;
        self.store
            .compare_and_put_slot(Some(expected), slot.clone())
            .await?;
        self.revision = slot.revision;
        self.slot_assigned_at.insert(key.clone(), self.now());
        self.publish_slot_delta(&slot).await
    }

    async fn placement_view(&self) -> Result<PlacementView, CoordinatorRuntimeError> {
        let now = self.now();
        let mut reservations = BTreeMap::<NodeKey, u64>::new();
        for plan in self.plans.values() {
            for (target, weight) in plan.target_reservations() {
                *reservations.entry(target.clone()).or_default() += weight;
            }
        }
        let nodes = self
            .sessions
            .values()
            .map(|session| PlacementNode {
                key: session.hello.node.clone(),
                ready: true,
                eligible_entity_types: session.hello.hosted_entity_types.clone(),
                protocols: session
                    .hello
                    .protocols
                    .iter()
                    .map(|protocol| protocol.protocol_id)
                    .collect(),
                capacity_units: session.hello.capacity_units,
                joined_at: session.joined_at,
                load: self
                    .loads
                    .node(session.hello.node.incarnation)
                    .map(|report| LoadSample {
                        boot_incarnation: report.node.incarnation,
                        sequence: report.sequence,
                        observed_at: self
                            .node_load_received
                            .get(&report.node.incarnation)
                            .copied()
                            .unwrap_or(now),
                        weight: report.total_weight,
                    }),
                reserved_weight: reservations.get(&session.hello.node).copied().unwrap_or(0),
                draining: session.draining,
            })
            .collect();
        let shards = self
            .store
            .list_slots()
            .await?
            .into_iter()
            .filter_map(|slot| {
                let key = slot.key.clone();
                let PlacementSlotKey::Shard {
                    entity_type,
                    shard_id,
                } = slot.key
                else {
                    return None;
                };
                slot.owner.map(|owner| {
                    let measured_weight = self
                        .loads
                        .shard(owner.incarnation, &entity_type, shard_id)
                        .map(|report| report.weight);
                    PlacedShard {
                        entity_type,
                        shard_id,
                        owner,
                        generation: slot.assignment_generation,
                        measured_weight,
                        assigned_at: self.slot_assigned_at.get(&key).copied().unwrap_or(now),
                        active_move: slot.active_move.is_some(),
                    }
                })
            })
            .collect::<Vec<_>>();
        let mut active_entity_moves = BTreeMap::new();
        let mut active_source_moves = BTreeMap::new();
        let mut active_target_moves = BTreeMap::new();
        let mut active_cluster_moves = 0;
        for plan in self.plans.values() {
            for movement in &plan.moves {
                if movement.progress == MoveProgress::Handoff {
                    active_cluster_moves += 1;
                    *active_entity_moves
                        .entry(plan.entity_type.clone())
                        .or_default() += 1;
                    *active_source_moves
                        .entry(movement.source.clone())
                        .or_default() += 1;
                    *active_target_moves
                        .entry(movement.target.clone())
                        .or_default() += 1;
                }
            }
        }
        Ok(PlacementView {
            coordinator_term: self.leader.term,
            revision: self.revision,
            now,
            reconciled: true,
            degraded: false,
            nodes,
            shards,
            active_cluster_moves,
            active_entity_moves,
            active_source_moves,
            active_target_moves,
            last_automatic_move_at: self.last_automatic_move_at,
        })
    }

    fn now(&self) -> crate::types::MonotonicTime {
        crate::types::MonotonicTime::from_millis(
            u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX),
        )
    }

    async fn register(
        &mut self,
        hello: NodeHello,
        association_key: lattice_remoting::AssociationKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        hello
            .validate(&self.config.session_limits)
            .map_err(CoordinatorRuntimeError::Coordinator)?;
        if hello.node.incarnation != association_key.remote_incarnation
            || hello.node.address != association_key.remote_address
            || self.sessions.len() == self.config.maximum_sessions
                && !self.sessions.contains_key(&hello.node.incarnation)
        {
            return Err(CoordinatorRuntimeError::UnauthorizedCommand);
        }
        for config in &hello.entity_configs {
            if self
                .entity_configs
                .get(&config.entity_type)
                .is_some_and(|current| current != config)
            {
                return Err(CoordinatorRuntimeError::ConfigurationConflict);
            }
            if !self.strategies.contains_key(&(
                config.allocation_policy_id.clone(),
                config.allocation_policy_version,
            )) {
                return Err(CoordinatorRuntimeError::UnknownStrategy);
            }
            self.entity_configs
                .insert(config.entity_type.clone(), config.clone());
        }
        for config in &hello.singleton_configs {
            if self
                .singleton_configs
                .get(&config.kind)
                .is_some_and(|current| current != config)
            {
                return Err(CoordinatorRuntimeError::ConfigurationConflict);
            }
            self.singleton_configs
                .insert(config.kind.clone(), config.clone());
        }
        let lease_id = match self.sessions.get(&hello.node.incarnation) {
            Some(session) => session.lease_id,
            None => self.store.grant_lease(self.config.member_lease_ttl).await?,
        };
        let joined_at = self
            .sessions
            .get(&hello.node.incarnation)
            .map(|session| session.joined_at)
            .unwrap_or_else(|| self.now());
        self.store.register_member(&hello, lease_id).await?;
        self.sessions.insert(
            hello.node.incarnation,
            MemberSession {
                hello: hello.clone(),
                association: association_key.clone(),
                lease_id,
                heartbeat_sequence: 0,
                last_heartbeat: Instant::now(),
                applied_revision: None,
                draining: false,
                joined_at,
            },
        );
        self.send_snapshot(hello.clone(), association_key).await?;
        self.reconcile_claims_for(&hello).await?;
        self.resume_handoffs_for(&hello.node).await
    }

    async fn resume_handoffs_for(&mut self, node: &NodeKey) -> Result<(), CoordinatorRuntimeError> {
        let candidates = self
            .handoffs
            .iter()
            .filter_map(|(key, handoff)| {
                ((handoff.phase == HandoffPhase::Draining
                    && (&handoff.source == node
                        || (&handoff.target == node
                            && matches!(key, PlacementSlotKey::Singleton(_))
                            && !self.sessions.contains_key(&handoff.source.incarnation))))
                    || (handoff.phase == HandoffPhase::ReplacingAuthority
                        && &handoff.target == node))
                    .then_some((key.clone(), handoff.phase))
            })
            .collect::<Vec<_>>();
        for (key, phase) in candidates {
            match phase {
                HandoffPhase::Draining => {
                    let slot = self
                        .store
                        .get_slot(&key)
                        .await?
                        .ok_or(CoordinatorRuntimeError::UnknownSlot)?;
                    if slot.state == PlacementSlotState::Stopping {
                        let handoff = self
                            .handoffs
                            .get(&key)
                            .cloned()
                            .ok_or(CoordinatorRuntimeError::UnknownHandoff)?;
                        if &handoff.source == node {
                            let session = self
                                .sessions
                                .get(&node.incarnation)
                                .ok_or(CoordinatorRuntimeError::UnknownSession)?;
                            let association = self
                                .associations
                                .get(&session.association)
                                .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
                            send_control(
                                &association,
                                PlacementControlCommand::DrainSlot {
                                    slot: key,
                                    generation: handoff.source_generation,
                                    revision: slot.revision,
                                },
                                &self.config,
                            )?;
                        } else if self.store.get_claim(&key).await?.is_none() {
                            self.transition_handoff(
                                key,
                                HandoffEvent::SourceAuthorityInvalid {
                                    source: handoff.source,
                                    generation: handoff.source_generation,
                                },
                            )
                            .await?;
                        }
                    }
                }
                HandoffPhase::ReplacingAuthority => self.replace_authority(&key).await?,
                _ => {}
            }
        }
        Ok(())
    }

    async fn send_snapshot(
        &self,
        hello: NodeHello,
        association_key: lattice_remoting::AssociationKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        let mut records = Vec::new();
        for member in self.store.list_members().await? {
            records.push(SnapshotRecord {
                key: format!("member/{}", member.node.node_id),
                value: Bytes::from(
                    serde_json::to_vec(&member).map_err(|_| CoordinatorRuntimeError::Codec)?,
                ),
            });
        }
        for slot in self.store.list_slots().await? {
            let include = match &slot.key {
                PlacementSlotKey::Shard { entity_type, .. } => hello.subscribes_to(entity_type),
                PlacementSlotKey::Singleton(kind) => {
                    hello.singleton_eligibility.contains(kind)
                        || hello.used_singletons.contains(kind)
                }
            };
            if include {
                records.push(SnapshotRecord {
                    key: slot_record_key(&slot.key),
                    value: Bytes::from(
                        serde_json::to_vec(&slot).map_err(|_| CoordinatorRuntimeError::Codec)?,
                    ),
                });
            }
        }
        let (begin, chunks, end) =
            build_snapshot(self.revision, records, &self.config.snapshot_limits)
                .map_err(CoordinatorRuntimeError::Coordinator)?;
        let association = self
            .associations
            .get(&association_key)
            .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
        send_control(
            &association,
            PlacementControlCommand::SnapshotBegin(begin),
            &self.config,
        )?;
        for chunk in chunks {
            send_control(
                &association,
                PlacementControlCommand::SnapshotChunk(chunk),
                &self.config,
            )?;
        }
        send_control(
            &association,
            PlacementControlCommand::SnapshotEnd(end),
            &self.config,
        )
    }

    async fn reconcile_claims_for(
        &mut self,
        hello: &NodeHello,
    ) -> Result<(), CoordinatorRuntimeError> {
        let association = self
            .sessions
            .get(&hello.node.incarnation)
            .and_then(|session| self.associations.get(&session.association))
            .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
        for slot in self.store.list_slots().await? {
            if slot.owner.as_ref() != Some(&hello.node)
                || !matches!(
                    slot.state,
                    crate::types::PlacementSlotState::Allocating
                        | crate::types::PlacementSlotState::Running
                )
            {
                continue;
            }
            let previous = self.store.get_claim(&slot.key).await?;
            let sequence = previous
                .as_ref()
                .filter(|claim| claim.assignment_generation == slot.assignment_generation)
                .map(|claim| claim.grant_sequence.next())
                .transpose()
                .map_err(|_| CoordinatorRuntimeError::ClaimSequence)?
                .unwrap_or(GrantSequence::new(1).expect("one is a valid sequence"));
            let grant = ClaimGrant {
                slot: slot.key.clone(),
                owner: hello.node.clone(),
                coordinator_term: self.leader.term,
                assignment_generation: slot.assignment_generation,
                grant_sequence: sequence,
                ttl: self.config.claim_ttl,
            };
            let lease_id = match self.claims.get(&slot.key) {
                Some(claim) => claim.lease_id,
                None => self.store.grant_lease(self.config.claim_ttl).await?,
            };
            self.store.put_claim(&grant, lease_id).await?;
            self.claims.insert(
                slot.key.clone(),
                ClaimLease {
                    lease_id,
                    grant: grant.clone(),
                },
            );
            send_control(
                &association,
                PlacementControlCommand::ClaimGranted(grant),
                &self.config,
            )?;
        }
        Ok(())
    }

    async fn renew(&mut self) -> Result<(), CoordinatorRuntimeError> {
        self.store.keep_lease_alive(self.leader_lease_id).await?;
        let now = Instant::now();
        let expired = self
            .sessions
            .iter()
            .filter_map(|(incarnation, session)| {
                (now.duration_since(session.last_heartbeat) > self.config.member_heartbeat_timeout)
                    .then_some((*incarnation, session.lease_id, session.hello.node.clone()))
            })
            .collect::<Vec<_>>();
        for (incarnation, lease_id, node) in expired {
            self.store.revoke_lease(lease_id).await?;
            self.sessions.remove(&incarnation);
            for session in self.sessions.values() {
                let association = self
                    .associations
                    .get(&session.association)
                    .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
                send_control(
                    &association,
                    PlacementControlCommand::NodeRemoved(incarnation),
                    &self.config,
                )?;
            }
            let expired_claims = self
                .claims
                .iter()
                .filter_map(|(key, claim)| {
                    (claim.grant.owner.incarnation == incarnation).then_some((
                        key.clone(),
                        claim.lease_id,
                        claim.grant.clone(),
                    ))
                })
                .collect::<Vec<_>>();
            for (key, claim_lease, grant) in expired_claims {
                self.store.delete_claim(&grant).await?;
                self.store.revoke_lease(claim_lease).await?;
                self.claims.remove(&key);
            }
            let barriers = self
                .handoffs
                .iter()
                .filter_map(|(key, handoff)| {
                    (handoff.phase == HandoffPhase::Invalidating
                        && handoff.required_sessions().contains(&incarnation))
                    .then_some(key.clone())
                })
                .collect::<Vec<_>>();
            for key in barriers {
                self.transition_handoff(key, HandoffEvent::FenceSession(incarnation))
                    .await?;
            }
            let owned_slots = self
                .store
                .list_slots()
                .await?
                .into_iter()
                .filter(|slot| slot.owner.as_ref() == Some(&node))
                .collect::<Vec<_>>();
            let entity_types = owned_slots
                .iter()
                .filter_map(|slot| match slot.key {
                    PlacementSlotKey::Shard {
                        ref entity_type, ..
                    } => Some(entity_type.clone()),
                    PlacementSlotKey::Singleton(_) => None,
                })
                .collect::<std::collections::BTreeSet<_>>();
            for entity_type in entity_types {
                let _ = self
                    .evaluate_rebalance(
                        entity_type,
                        RebalanceTrigger::Recovery {
                            owner: node.clone(),
                        },
                    )
                    .await;
            }
            for slot in owned_slots {
                if matches!(slot.key, PlacementSlotKey::Singleton(_)) {
                    match self.begin_singleton_recovery(slot).await {
                        Ok(()) | Err(CoordinatorRuntimeError::IneligibleTarget) => {}
                        Err(error) => return Err(error),
                    }
                }
            }
        }
        for session in self.sessions.values() {
            self.store.keep_lease_alive(session.lease_id).await?;
        }
        for claim in self.claims.values() {
            self.store.keep_lease_alive(claim.lease_id).await?;
            if let Some(session) = self.sessions.get(&claim.grant.owner.incarnation)
                && let Some(association) = self.associations.get(&session.association)
            {
                send_control(
                    &association,
                    PlacementControlCommand::ClaimGranted(claim.grant.clone()),
                    &self.config,
                )?;
            }
        }
        Ok(())
    }
}

fn control_dispatch_error(
    error: &CoordinatorRuntimeError,
) -> lattice_remoting::ControlDispatchError {
    match error {
        CoordinatorRuntimeError::UnauthorizedCommand
        | CoordinatorRuntimeError::UnknownSession
        | CoordinatorRuntimeError::Codec
        | CoordinatorRuntimeError::Coordinator(_)
        | CoordinatorRuntimeError::Control(_)
        | CoordinatorRuntimeError::ClaimSequence => {
            lattice_remoting::ControlDispatchError::InvalidCommand
        }
        _ => lattice_remoting::ControlDispatchError::Unavailable,
    }
}

fn send_control(
    association: &Association,
    command: PlacementControlCommand,
    config: &CoordinatorLeaderConfig,
) -> Result<(), CoordinatorRuntimeError> {
    if association.state() == AssociationState::Closed {
        return Err(CoordinatorRuntimeError::AssociationUnavailable);
    }
    let payload = encode_control_command(&command, config.maximum_control_payload)
        .map_err(CoordinatorRuntimeError::Control)?;
    association.admit_control_command(payload)?;
    Ok(())
}

fn slot_record_key(key: &PlacementSlotKey) -> String {
    match key {
        PlacementSlotKey::Shard {
            entity_type,
            shard_id,
        } => format!("shard/{}/{}", entity_type.as_str(), shard_id.get()),
        PlacementSlotKey::Singleton(kind) => format!("singleton/{}", kind.as_str()),
    }
}

fn plan_priority(reason: &PlanReason) -> u8 {
    match reason {
        PlanReason::Recovery => 0,
        PlanReason::Drain => 1,
        PlanReason::Manual => 2,
        PlanReason::Automatic => 3,
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
mod tests {
    use super::*;
    use async_trait::async_trait;
    use lattice_core::actor_ref::{
        ClusterId, ConfigFingerprint, EntityType, NodeAddress, NodeIncarnation,
    };
    use lattice_remoting::{
        ExactActorTarget, InboundDispatch, NodeIdentity, OutboundMessaging, RemoteMessageError,
        RemotingConfig, RemotingEndpoint,
    };

    use crate::authority::AuthorityEffect;
    use crate::control::PlacementControlRouter;
    use crate::session::{LogicCoordinatorConfig, LogicCoordinatorSession};
    use crate::storage::{InMemoryPlacementStore, PlacementStore};
    use crate::types::{AssignmentGeneration, PlacementSlot, PlacementSlotState, ShardId};

    fn attach_test_session(
        associations: &AssociationManager,
        cluster_id: &ClusterId,
        coordinator_incarnation: NodeIncarnation,
        remote: &NodeKey,
        nonce_base: u128,
    ) -> lattice_remoting::AssociationKey {
        let association = associations
            .get_or_create(
                cluster_id.clone(),
                remote.address.clone(),
                remote.incarnation,
            )
            .unwrap();
        let key = lattice_remoting::AssociationKey {
            cluster_id: cluster_id.clone(),
            local_incarnation: coordinator_incarnation,
            remote_address: remote.address.clone(),
            remote_incarnation: remote.incarnation,
        };
        for (lane, nonce) in [
            (lattice_remoting::LaneKind::Control, nonce_base),
            (lattice_remoting::LaneKind::Interactive, nonce_base + 1),
            (lattice_remoting::LaneKind::Bulk(0), nonce_base + 2),
        ] {
            association
                .attach(lattice_remoting::LaneAttachment {
                    association_id: association.id(),
                    key: key.clone(),
                    lane,
                    connection_nonce: nonce,
                })
                .unwrap();
        }
        key
    }

    struct NoActors;

    #[async_trait]
    impl InboundDispatch for NoActors {
        async fn tell(
            &self,
            _target: ExactActorTarget,
            _message_id: u64,
            _payload: Bytes,
        ) -> Result<(), RemoteMessageError> {
            Err(RemoteMessageError::UnsupportedProtocol)
        }

        async fn ask(
            &self,
            _target: ExactActorTarget,
            _message_id: u64,
            _payload: Bytes,
            _deadline: std::time::Instant,
        ) -> Result<Bytes, RemoteMessageError> {
            Err(RemoteMessageError::UnsupportedProtocol)
        }
    }

    fn node(
        cluster_id: &ClusterId,
        node_id: &str,
        port: u16,
        incarnation: u128,
    ) -> (NodeKey, NodeIdentity) {
        let address = NodeAddress::new("127.0.0.1", port).unwrap();
        let incarnation = NodeIncarnation::new(incarnation).unwrap();
        (
            NodeKey {
                node_id: node_id.to_owned(),
                address: address.clone(),
                incarnation,
            },
            NodeIdentity {
                cluster_id: cluster_id.clone(),
                node_id: node_id.to_owned(),
                address,
                incarnation,
            },
        )
    }

    #[tokio::test]
    async fn real_control_session_installs_snapshot_and_matching_claim() {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let coordinator_port = probe.local_addr().unwrap().port();
        drop(probe);
        let logic_port = coordinator_port - 1;
        let cluster_id = ClusterId::new("coordinator-test").unwrap();
        let (logic_node, logic_identity) = node(&cluster_id, "logic", logic_port, 1);
        let (coordinator_node, coordinator_identity) =
            node(&cluster_id, "coordinator", coordinator_port, 2);
        let remoting = RemotingConfig {
            heartbeat_interval: Duration::from_millis(100),
            shutdown_timeout: Duration::from_secs(2),
            ..RemotingConfig::default()
        };
        let logic_associations = Arc::new(
            AssociationManager::new(
                logic_identity.address.clone(),
                logic_identity.incarnation,
                remoting.clone(),
            )
            .unwrap(),
        );
        let coordinator_associations = Arc::new(
            AssociationManager::new(
                coordinator_identity.address.clone(),
                coordinator_identity.incarnation,
                remoting.clone(),
            )
            .unwrap(),
        );
        let (logic_router, logic_controls) =
            PlacementControlRouter::bounded(64, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
        let (coordinator_router, coordinator_controls) =
            PlacementControlRouter::bounded(64, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
        let logic_endpoint = Arc::new(
            RemotingEndpoint::new_with_control(
                logic_identity.clone(),
                remoting.clone(),
                logic_associations.clone(),
                Arc::new(OutboundMessaging::new(32).unwrap()),
                Arc::new(NoActors),
                Arc::new(logic_router),
                Vec::new(),
            )
            .unwrap(),
        );
        let coordinator_endpoint = Arc::new(
            RemotingEndpoint::new_with_control(
                coordinator_identity.clone(),
                remoting,
                coordinator_associations.clone(),
                Arc::new(OutboundMessaging::new(32).unwrap()),
                Arc::new(NoActors),
                Arc::new(coordinator_router),
                Vec::new(),
            )
            .unwrap(),
        );
        coordinator_endpoint.bind().await.unwrap();
        let logic_to_coordinator = logic_endpoint
            .connect_peer(coordinator_identity)
            .await
            .unwrap();

        let entity_type = EntityType::new("player").unwrap();
        let slot_key = PlacementSlotKey::Shard {
            entity_type: entity_type.clone(),
            shard_id: ShardId::new(3),
        };
        let store = Arc::new(InMemoryPlacementStore::new(16, 16).unwrap());
        store
            .compare_and_put_slot(
                None,
                PlacementSlot {
                    key: slot_key.clone(),
                    config_fingerprint: ConfigFingerprint::new([7; 32]),
                    owner: Some(logic_node.clone()),
                    target: None,
                    assignment_generation: AssignmentGeneration::new(1).unwrap(),
                    coordinator_term: CoordinatorTerm::new(1).unwrap(),
                    revision: Revision::new(1).unwrap(),
                    state: PlacementSlotState::Running,
                    active_move: None,
                    barrier_sessions: Default::default(),
                },
            )
            .await
            .unwrap();
        let leader = CoordinatorLeader::elect(
            store,
            coordinator_associations,
            coordinator_node,
            CoordinatorTerm::new(1).unwrap(),
            2,
            CoordinatorLeaderConfig {
                renewal_interval: Duration::from_secs(1),
                ..CoordinatorLeaderConfig::default()
            },
        )
        .await
        .unwrap();
        let hello = NodeHello {
            node: logic_node,
            roles: ["logic".to_owned()].into_iter().collect(),
            capacity_units: 1,
            hosted_entity_types: [entity_type].into_iter().collect(),
            proxied_entity_types: Default::default(),
            singleton_eligibility: Default::default(),
            used_singletons: Default::default(),
            protocols: Vec::new(),
            entity_configs: Vec::new(),
            singleton_configs: Vec::new(),
        };
        let (logic, mut effects) = LogicCoordinatorSession::new(
            hello,
            logic_to_coordinator.key().clone(),
            logic_associations,
            LogicCoordinatorConfig::default(),
            32,
        )
        .unwrap();
        let state = logic.state();
        logic
            .register_authority(slot_key.clone(), Duration::from_secs(2))
            .unwrap();
        let (leader_shutdown_tx, leader_shutdown_rx) = watch::channel(false);
        let (logic_shutdown_tx, logic_shutdown_rx) = watch::channel(false);
        let leader_task = tokio::spawn(leader.run(coordinator_controls, leader_shutdown_rx));
        let logic_task = tokio::spawn(logic.run(logic_controls, logic_shutdown_rx));
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if state
                    .lock()
                    .expect("logic state poisoned")
                    .admission_open(&slot_key)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut observed = Vec::new();
        while let Ok(effect) = effects.try_recv() {
            if let crate::session::LogicPlacementEffect::Authority { effect, .. } = effect {
                observed.push(effect);
            }
        }
        assert!(observed.contains(&AuthorityEffect::StartSlot));
        assert!(observed.contains(&AuthorityEffect::OpenAdmission));
        assert!(observed.contains(&AuthorityEffect::PublishReady));
        logic_endpoint.shutdown().await.unwrap();
        coordinator_endpoint.shutdown().await.unwrap();
        logic_shutdown_tx.send(true).unwrap();
        leader_shutdown_tx.send(true).unwrap();
        logic_task.await.unwrap().unwrap();
        leader_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn persisted_handoff_barrier_replaces_claim_forward() {
        let cluster_id = ClusterId::new("handoff-test").unwrap();
        let (coordinator_node, _) = node(&cluster_id, "coordinator", 26100, 100);
        let (source, _) = node(&cluster_id, "source", 26101, 101);
        let (target, _) = node(&cluster_id, "target", 26102, 102);
        let associations = Arc::new(
            AssociationManager::new(
                coordinator_node.address.clone(),
                coordinator_node.incarnation,
                RemotingConfig::default(),
            )
            .unwrap(),
        );
        let source_key = attach_test_session(
            &associations,
            &cluster_id,
            coordinator_node.incarnation,
            &source,
            10,
        );
        let target_key = attach_test_session(
            &associations,
            &cluster_id,
            coordinator_node.incarnation,
            &target,
            20,
        );
        let entity_type = EntityType::new("handoff-entity").unwrap();
        let slot_key = PlacementSlotKey::Shard {
            entity_type: entity_type.clone(),
            shard_id: ShardId::new(1),
        };
        let store = Arc::new(InMemoryPlacementStore::new(16, 16).unwrap());
        store
            .compare_and_put_slot(
                None,
                PlacementSlot {
                    key: slot_key.clone(),
                    config_fingerprint: ConfigFingerprint::new([9; 32]),
                    owner: Some(source.clone()),
                    target: None,
                    assignment_generation: AssignmentGeneration::new(1).unwrap(),
                    coordinator_term: CoordinatorTerm::new(1).unwrap(),
                    revision: Revision::new(1).unwrap(),
                    state: PlacementSlotState::Running,
                    active_move: None,
                    barrier_sessions: Default::default(),
                },
            )
            .await
            .unwrap();
        let mut leader = CoordinatorLeader::elect(
            store.clone(),
            associations,
            coordinator_node,
            CoordinatorTerm::new(1).unwrap(),
            2,
            CoordinatorLeaderConfig::default(),
        )
        .await
        .unwrap();
        let protocol_id = lattice_core::actor_ref::ProtocolId::new(77).unwrap();
        let entity_config = crate::region::EntityConfig::new(
            entity_type.clone(),
            protocol_id,
            8,
            "weighted-least-load",
            1,
            Vec::new(),
        )
        .unwrap();
        let descriptor = lattice_remoting::ProtocolDescriptor {
            protocol_id,
            fingerprint: lattice_remoting::ProtocolFingerprint::new([7; 32]),
        };
        let hello = |node: NodeKey| NodeHello {
            node,
            roles: Default::default(),
            capacity_units: 10,
            hosted_entity_types: [entity_type.clone()].into_iter().collect(),
            proxied_entity_types: Default::default(),
            singleton_eligibility: Default::default(),
            used_singletons: Default::default(),
            protocols: vec![descriptor.clone()],
            entity_configs: vec![entity_config.clone()],
            singleton_configs: Vec::new(),
        };
        leader
            .register(hello(source.clone()), source_key)
            .await
            .unwrap();
        leader
            .register(hello(target.clone()), target_key)
            .await
            .unwrap();
        let relocation = ManualRelocationRequest {
            operation_id: "manual-1".to_owned(),
            entity_type: entity_type.clone(),
            shard_id: ShardId::new(1),
            expected_generation: AssignmentGeneration::new(1).unwrap(),
            target_node_id: target.node_id.clone(),
        };
        let plan_id = leader.manual_relocate(relocation.clone()).await.unwrap();
        assert_eq!(
            leader.manual_relocate(relocation.clone()).await.unwrap(),
            plan_id
        );
        assert!(matches!(
            leader
                .manual_relocate(ManualRelocationRequest {
                    target_node_id: source.node_id.clone(),
                    ..relocation
                })
                .await,
            Err(CoordinatorRuntimeError::IdempotencyConflict)
        ));
        let barrier_revision = leader.handoffs[&slot_key].barrier_revision();
        leader
            .transition_handoff(
                slot_key.clone(),
                HandoffEvent::AppliedRevision {
                    session: source.incarnation,
                    revision: barrier_revision,
                },
            )
            .await
            .unwrap();
        leader
            .transition_handoff(
                slot_key.clone(),
                HandoffEvent::AppliedRevision {
                    session: target.incarnation,
                    revision: barrier_revision,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            store.get_slot(&slot_key).await.unwrap().unwrap().state,
            PlacementSlotState::Stopping
        );
        leader
            .transition_handoff(
                slot_key.clone(),
                HandoffEvent::SourceDrained {
                    source,
                    generation: AssignmentGeneration::new(1).unwrap(),
                },
            )
            .await
            .unwrap();
        let allocating = store.get_slot(&slot_key).await.unwrap().unwrap();
        assert_eq!(allocating.state, PlacementSlotState::Allocating);
        assert_eq!(allocating.owner.as_ref(), Some(&target));
        assert_eq!(
            store.get_claim(&slot_key).await.unwrap().unwrap().owner,
            target
        );
        leader
            .transition_handoff(
                slot_key.clone(),
                HandoffEvent::TargetReady {
                    target: allocating.owner.unwrap(),
                    generation: allocating.assignment_generation,
                },
            )
            .await
            .unwrap();
        let active = store.get_slot(&slot_key).await.unwrap().unwrap();
        assert_eq!(active.state, PlacementSlotState::Running);
        assert!(active.active_move.is_none());
        let plan = store.get_plan(plan_id).await.unwrap().unwrap();
        assert_eq!(plan.status, PlanStatus::Completed);
    }

    #[tokio::test]
    async fn first_resolution_allocates_shard_and_singleton_to_declared_host() {
        let cluster_id = ClusterId::new("allocation-test").unwrap();
        let (coordinator_node, _) = node(&cluster_id, "coordinator", 26200, 200);
        let (proxy, _) = node(&cluster_id, "proxy", 26201, 201);
        let (host, _) = node(&cluster_id, "host", 26202, 202);
        let associations = Arc::new(
            AssociationManager::new(
                coordinator_node.address.clone(),
                coordinator_node.incarnation,
                RemotingConfig::default(),
            )
            .unwrap(),
        );
        let proxy_key = attach_test_session(
            &associations,
            &cluster_id,
            coordinator_node.incarnation,
            &proxy,
            30,
        );
        let host_key = attach_test_session(
            &associations,
            &cluster_id,
            coordinator_node.incarnation,
            &host,
            40,
        );
        let store = Arc::new(InMemoryPlacementStore::new(16, 16).unwrap());
        let mut leader = CoordinatorLeader::elect(
            store.clone(),
            associations,
            coordinator_node,
            CoordinatorTerm::new(1).unwrap(),
            2,
            CoordinatorLeaderConfig::default(),
        )
        .await
        .unwrap();
        let entity_type = EntityType::new("allocated-entity").unwrap();
        let protocol_id = lattice_core::actor_ref::ProtocolId::new(55).unwrap();
        let entity_config = crate::region::EntityConfig::new(
            entity_type.clone(),
            protocol_id,
            8,
            "weighted-least-load",
            1,
            Vec::new(),
        )
        .unwrap();
        let singleton_kind =
            lattice_core::actor_ref::SingletonKind::new("allocated-singleton").unwrap();
        let singleton_config = SingletonConfig {
            kind: singleton_kind.clone(),
            protocol_id,
            config_fingerprint: ConfigFingerprint::new([6; 32]),
        };
        let descriptor = lattice_remoting::ProtocolDescriptor {
            protocol_id,
            fingerprint: lattice_remoting::ProtocolFingerprint::new([8; 32]),
        };
        leader
            .register(
                NodeHello {
                    node: proxy,
                    roles: Default::default(),
                    capacity_units: 1,
                    hosted_entity_types: Default::default(),
                    proxied_entity_types: [entity_type.clone()].into_iter().collect(),
                    singleton_eligibility: Default::default(),
                    used_singletons: [singleton_kind.clone()].into_iter().collect(),
                    protocols: vec![descriptor.clone()],
                    entity_configs: Vec::new(),
                    singleton_configs: Vec::new(),
                },
                proxy_key,
            )
            .await
            .unwrap();
        leader
            .register(
                NodeHello {
                    node: host.clone(),
                    roles: Default::default(),
                    capacity_units: 10,
                    hosted_entity_types: [entity_type.clone()].into_iter().collect(),
                    proxied_entity_types: Default::default(),
                    singleton_eligibility: [singleton_kind.clone()].into_iter().collect(),
                    used_singletons: Default::default(),
                    protocols: vec![descriptor],
                    entity_configs: vec![entity_config],
                    singleton_configs: vec![singleton_config],
                },
                host_key,
            )
            .await
            .unwrap();
        leader
            .ensure_shard_allocated(entity_type.clone(), ShardId::new(3))
            .await
            .unwrap();
        let shard_key = PlacementSlotKey::Shard {
            entity_type,
            shard_id: ShardId::new(3),
        };
        let shard = store.get_slot(&shard_key).await.unwrap().unwrap();
        assert_eq!(shard.owner.as_ref(), Some(&host));
        assert_eq!(shard.state, PlacementSlotState::Allocating);
        leader
            .complete_initial_ready(&shard_key, &host, shard.assignment_generation)
            .await
            .unwrap();
        leader
            .ensure_singleton_allocated(singleton_kind.clone())
            .await
            .unwrap();
        let singleton_key = PlacementSlotKey::Singleton(singleton_kind);
        let singleton = store.get_slot(&singleton_key).await.unwrap().unwrap();
        assert_eq!(singleton.owner.as_ref(), Some(&host));
        leader
            .complete_initial_ready(&singleton_key, &host, singleton.assignment_generation)
            .await
            .unwrap();
        assert_eq!(
            store.get_slot(&shard_key).await.unwrap().unwrap().state,
            PlacementSlotState::Running
        );
        assert_eq!(
            store.get_slot(&singleton_key).await.unwrap().unwrap().state,
            PlacementSlotState::Running
        );
    }

    #[tokio::test]
    async fn admin_pause_is_idempotent_fingerprinted_and_inspectable() {
        let cluster_id = ClusterId::new("admin-test").unwrap();
        let (coordinator, _) = node(&cluster_id, "coordinator", 26300, 300);
        let associations = Arc::new(
            AssociationManager::new(
                coordinator.address.clone(),
                coordinator.incarnation,
                RemotingConfig::default(),
            )
            .unwrap(),
        );
        let store = Arc::new(InMemoryPlacementStore::new(8, 8).unwrap());
        let mut leader = CoordinatorLeader::elect(
            store,
            associations,
            coordinator,
            CoordinatorTerm::new(1).unwrap(),
            2,
            CoordinatorLeaderConfig::default(),
        )
        .await
        .unwrap();
        let entity_type = EntityType::new("admin-entity").unwrap();
        leader
            .set_automatic_paused("pause-1".to_owned(), Some(entity_type.clone()), true)
            .await
            .unwrap();
        leader
            .set_automatic_paused("pause-1".to_owned(), Some(entity_type.clone()), true)
            .await
            .unwrap();
        assert!(matches!(
            leader
                .set_automatic_paused("pause-1".to_owned(), Some(entity_type.clone()), false)
                .await,
            Err(CoordinatorRuntimeError::IdempotencyConflict)
        ));
        let inspection = leader.inspect().await.unwrap();
        assert_eq!(inspection.term, CoordinatorTerm::new(1).unwrap());
        assert_eq!(inspection.paused_entity_types, vec![entity_type]);

        leader
            .record_admin_operation("relocate-1".to_owned(), "move:a".to_owned(), Some(42))
            .unwrap();
        assert_eq!(
            leader
                .prior_admin_operation("relocate-1", "move:a")
                .unwrap(),
            Some(Some(42))
        );
        assert!(matches!(
            leader.prior_admin_operation("relocate-1", "move:b"),
            Err(CoordinatorRuntimeError::IdempotencyConflict)
        ));
    }

    #[tokio::test]
    async fn terminal_plan_history_compacts_oldest_persisted_record() {
        let cluster_id = ClusterId::new("history-test").unwrap();
        let (coordinator, _) = node(&cluster_id, "coordinator", 26310, 310);
        let associations = Arc::new(
            AssociationManager::new(
                coordinator.address.clone(),
                coordinator.incarnation,
                RemotingConfig::default(),
            )
            .unwrap(),
        );
        let store = Arc::new(InMemoryPlacementStore::new(8, 8).unwrap());
        let mut leader = CoordinatorLeader::elect(
            store.clone(),
            associations,
            coordinator,
            CoordinatorTerm::new(1).unwrap(),
            2,
            CoordinatorLeaderConfig {
                maximum_completed_plan_history: 2,
                ..CoordinatorLeaderConfig::default()
            },
        )
        .await
        .unwrap();
        let entity_type = EntityType::new("history-entity").unwrap();
        for id in 1..=3_u128 {
            let plan = RebalancePlan {
                plan_id: id,
                entity_type: entity_type.clone(),
                reason: PlanReason::Manual,
                coordinator_term: CoordinatorTerm::new(1).unwrap(),
                base_revision: Revision::new(id as u64).unwrap(),
                revision: Revision::new(1).unwrap(),
                policy_id: "test".to_owned(),
                policy_version: 1,
                status: PlanStatus::Completed,
                moves: Vec::new(),
            };
            store
                .compare_and_put_plan(None, plan.clone(), plan.revision)
                .await
                .unwrap();
            leader.plans.insert(id, plan);
        }
        leader.compact_plan_history().await.unwrap();
        assert!(store.get_plan(1).await.unwrap().is_none());
        assert!(store.get_plan(2).await.unwrap().is_some());
        assert!(store.get_plan(3).await.unwrap().is_some());
        assert_eq!(leader.plans.len(), 2);
    }

    #[tokio::test]
    async fn leader_recovery_resumes_handoff_and_cancels_stale_pending_move() {
        use crate::allocation::{ProposedMove, RebalanceProposal, RebalanceTrigger};

        let cluster_id = ClusterId::new("recovery-test").unwrap();
        let (coordinator, _) = node(&cluster_id, "coordinator", 26300, 300);
        let (source, _) = node(&cluster_id, "source", 26301, 301);
        let (target, _) = node(&cluster_id, "target", 26302, 302);
        let entity_type = EntityType::new("recovery-entity").unwrap();
        let shard_id = ShardId::new(4);
        let proposal = |expected_generation| RebalanceProposal {
            policy_id: "test",
            policy_version: 1,
            base_revision: Revision::new(1).unwrap(),
            trigger: RebalanceTrigger::Manual {
                source: Some(source.clone()),
                target: Some(target.clone()),
                bypass_improvement: true,
            },
            moves: vec![ProposedMove {
                entity_type: entity_type.clone(),
                shard_id,
                expected_generation,
                source: source.clone(),
                target: target.clone(),
                estimated_weight: 1,
            }],
        };
        let mut started = RebalancePlan::from_proposal(
            proposal(AssignmentGeneration::new(1).unwrap()),
            entity_type.clone(),
            CoordinatorTerm::new(1).unwrap(),
            4,
        )
        .unwrap();
        started
            .begin_move(shard_id, AssignmentGeneration::new(1).unwrap(), None)
            .unwrap();
        started
            .install_barrier(shard_id, Revision::new(2).unwrap(), Default::default())
            .unwrap();
        let stale = RebalancePlan::from_proposal(
            proposal(AssignmentGeneration::new(9).unwrap()),
            entity_type.clone(),
            CoordinatorTerm::new(1).unwrap(),
            4,
        )
        .unwrap();
        let slot_key = PlacementSlotKey::Shard {
            entity_type,
            shard_id,
        };
        let store = Arc::new(InMemoryPlacementStore::new(8, 8).unwrap());
        store
            .compare_and_put_slot(
                None,
                PlacementSlot {
                    key: slot_key.clone(),
                    config_fingerprint: ConfigFingerprint::new([7; 32]),
                    owner: Some(source),
                    target: Some(target),
                    assignment_generation: AssignmentGeneration::new(1).unwrap(),
                    coordinator_term: CoordinatorTerm::new(1).unwrap(),
                    revision: Revision::new(2).unwrap(),
                    state: PlacementSlotState::BeginHandoff,
                    active_move: Some(started.plan_id),
                    barrier_sessions: Default::default(),
                },
            )
            .await
            .unwrap();
        store
            .compare_and_put_plan(None, started.clone(), started.revision)
            .await
            .unwrap();
        store
            .compare_and_put_plan(None, stale.clone(), stale.revision)
            .await
            .unwrap();
        let associations = Arc::new(
            AssociationManager::new(
                coordinator.address.clone(),
                coordinator.incarnation,
                RemotingConfig::default(),
            )
            .unwrap(),
        );
        let leader = CoordinatorLeader::elect(
            store.clone(),
            associations,
            coordinator,
            CoordinatorTerm::new(1).unwrap(),
            2,
            CoordinatorLeaderConfig::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            store.get_slot(&slot_key).await.unwrap().unwrap().state,
            PlacementSlotState::Stopping
        );
        assert_eq!(leader.handoffs[&slot_key].phase, HandoffPhase::Draining);
        assert_eq!(
            store.get_plan(stale.plan_id).await.unwrap().unwrap().status,
            PlanStatus::Cancelled
        );
    }

    #[tokio::test]
    async fn singleton_owner_loss_recovers_forward_after_leader_restart() {
        let cluster_id = ClusterId::new("singleton-recovery-test").unwrap();
        let (coordinator, _) = node(&cluster_id, "coordinator", 26400, 400);
        let (source, _) = node(&cluster_id, "source", 26401, 401);
        let (target, _) = node(&cluster_id, "target", 26402, 402);
        let associations = Arc::new(
            AssociationManager::new(
                coordinator.address.clone(),
                coordinator.incarnation,
                RemotingConfig::default(),
            )
            .unwrap(),
        );
        let source_key = attach_test_session(
            &associations,
            &cluster_id,
            coordinator.incarnation,
            &source,
            50,
        );
        let target_key = attach_test_session(
            &associations,
            &cluster_id,
            coordinator.incarnation,
            &target,
            60,
        );
        let store = Arc::new(InMemoryPlacementStore::new(8, 8).unwrap());
        let mut leader = CoordinatorLeader::elect(
            store.clone(),
            associations,
            coordinator,
            CoordinatorTerm::new(1).unwrap(),
            2,
            CoordinatorLeaderConfig {
                member_heartbeat_timeout: Duration::from_millis(10),
                ..CoordinatorLeaderConfig::default()
            },
        )
        .await
        .unwrap();
        let kind = lattice_core::actor_ref::SingletonKind::new("recovering-singleton").unwrap();
        let protocol_id = lattice_core::actor_ref::ProtocolId::new(77).unwrap();
        let singleton_config = SingletonConfig {
            kind: kind.clone(),
            protocol_id,
            config_fingerprint: ConfigFingerprint::new([4; 32]),
        };
        let descriptor = lattice_remoting::ProtocolDescriptor {
            protocol_id,
            fingerprint: lattice_remoting::ProtocolFingerprint::new([5; 32]),
        };
        let hello = |node: NodeKey| NodeHello {
            node,
            roles: Default::default(),
            capacity_units: 1,
            hosted_entity_types: Default::default(),
            proxied_entity_types: Default::default(),
            singleton_eligibility: [kind.clone()].into_iter().collect(),
            used_singletons: [kind.clone()].into_iter().collect(),
            protocols: vec![descriptor.clone()],
            entity_configs: Vec::new(),
            singleton_configs: vec![singleton_config.clone()],
        };
        leader
            .register(hello(source.clone()), source_key)
            .await
            .unwrap();
        leader
            .register(hello(target.clone()), target_key)
            .await
            .unwrap();
        leader
            .ensure_singleton_allocated(kind.clone())
            .await
            .unwrap();
        let slot_key = PlacementSlotKey::Singleton(kind);
        let initial = store.get_slot(&slot_key).await.unwrap().unwrap();
        assert_eq!(initial.owner.as_ref(), Some(&source));
        leader
            .complete_initial_ready(&slot_key, &source, initial.assignment_generation)
            .await
            .unwrap();

        leader
            .sessions
            .get_mut(&source.incarnation)
            .unwrap()
            .last_heartbeat = Instant::now() - Duration::from_secs(1);
        leader.renew().await.unwrap();
        let persisted = store.get_slot(&slot_key).await.unwrap().unwrap();
        assert_eq!(persisted.state, PlacementSlotState::BeginHandoff);
        assert_eq!(persisted.target.as_ref(), Some(&target));
        assert!(store.get_claim(&slot_key).await.unwrap().is_none());

        leader.handoffs.clear();
        leader.recover_persisted_plans().await.unwrap();
        assert_eq!(leader.handoffs[&slot_key].phase, HandoffPhase::Invalidating);
        leader
            .transition_handoff(
                slot_key.clone(),
                HandoffEvent::AppliedRevision {
                    session: target.incarnation,
                    revision: persisted.revision,
                },
            )
            .await
            .unwrap();
        let allocating = store.get_slot(&slot_key).await.unwrap().unwrap();
        assert_eq!(allocating.state, PlacementSlotState::Allocating);
        assert_eq!(allocating.owner.as_ref(), Some(&target));
        assert_eq!(allocating.assignment_generation.get(), 2);
        assert_eq!(
            store.get_claim(&slot_key).await.unwrap().unwrap().owner,
            target
        );

        leader
            .transition_handoff(
                slot_key.clone(),
                HandoffEvent::TargetReady {
                    target: allocating.owner.unwrap(),
                    generation: allocating.assignment_generation,
                },
            )
            .await
            .unwrap();
        let active = store.get_slot(&slot_key).await.unwrap().unwrap();
        assert_eq!(active.state, PlacementSlotState::Running);
        assert!(active.active_move.is_none());
        assert!(active.barrier_sessions.is_empty());
    }
}
