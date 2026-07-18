use std::time::SystemTime;

use lattice_core::{actor_ref::EntityType, failpoint::Failpoint};

use super::{
    BTreeMap, CoordinatorInspection, CoordinatorLeaseStore, CoordinatorOperation,
    CoordinatorRuntimeError, ForceRemoveRequest, Instant, ManualRelocationRequest, MembershipStore,
    MoveProgress, NodeKey, PlacementDomainLeader, PlacementDomainStore, PlacementSlotKey,
    PlacementSlotState, PlanReason, PlanStatus, RebalancePlan, RebalanceProposal, RebalanceTrigger,
    ScopedElectionStore, membership::plan_priority,
};
use crate::{
    allocation::{AllocationError, ProposedMove},
    coordinator::MemberRemovalReason,
    storage::{
        StorageError,
        domain::{
            AdminOperationRecord, AdminOperationResult, AdminOperationStatus,
            AutomaticBalanceSettings, CommitAutomaticSettings, CompactAdminOperations, CreatePlan,
            CreatePlanWithOperation, RecordAdminOperation, UpdatePlan, UpdatePlanWithOperation,
        },
    },
    types::{PlacementVersion, ShardId},
};

struct PlanAdminContext {
    operation_id: String,
    fingerprint: String,
    evaluation: bool,
}

impl<S> PlacementDomainLeader<S>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + PlacementDomainStore,
{
    pub(super) async fn handle_operation(
        &mut self,
        operation: CoordinatorOperation,
    ) -> Result<(), CoordinatorRuntimeError> {
        let leadership_lost = match operation {
            CoordinatorOperation::SubmitRebalance {
                proposal,
                entity_type,
                completion,
            } => {
                let result = self.submit_rebalance(proposal, entity_type).await;
                self.observe_operation_result("submit_rebalance", &result);
                let lost = operation_lost_leadership(&result);
                let _ = completion.send(result);
                lost
            }
            CoordinatorOperation::CancelPending {
                operation_id,
                plan_id,
                shard_id,
                completion,
            } => {
                let result = self.cancel_pending(operation_id, plan_id, shard_id).await;
                self.observe_operation_result("cancel_pending", &result);
                let lost = operation_lost_leadership(&result);
                let _ = completion.send(result);
                lost
            }
            CoordinatorOperation::Evaluate {
                operation_id,
                entity_type,
                trigger,
                completion,
            } => {
                let result = self
                    .evaluate_rebalance_operation(operation_id, entity_type, trigger)
                    .await;
                self.observe_operation_result("evaluate_rebalance", &result);
                let lost = operation_lost_leadership(&result);
                let _ = completion.send(result);
                lost
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
                self.observe_operation_result("set_automatic", &result);
                let lost = operation_lost_leadership(&result);
                let _ = completion.send(result);
                lost
            }
            CoordinatorOperation::ManualRelocate {
                request,
                completion,
            } => {
                let result = self.manual_relocate(request).await;
                self.observe_operation_result("manual_relocate", &result);
                let lost = operation_lost_leadership(&result);
                let _ = completion.send(result);
                lost
            }
            CoordinatorOperation::ForceRemove {
                request,
                completion,
            } => {
                let result = self.force_remove(request).await;
                self.observe_operation_result("force_remove", &result);
                let lost = operation_lost_leadership(&result);
                let _ = completion.send(result);
                lost
            }
            CoordinatorOperation::Inspect { completion } => {
                let result = self.inspect().await;
                let lost = operation_lost_leadership(&result);
                let _ = completion.send(result);
                lost
            }
        };
        if leadership_lost {
            Err(StorageError::LeadershipLost.into())
        } else {
            Ok(())
        }
    }

    fn observe_operation_result<T>(
        &mut self,
        family: &'static str,
        result: &Result<T, CoordinatorRuntimeError>,
    ) {
        let Err(CoordinatorRuntimeError::Storage(error)) = result else {
            return;
        };
        match error {
            StorageError::LeadershipLost => {
                self.leadership_loss_count = self.leadership_loss_count.saturating_add(1);
                tracing::warn!(
                    operation_family = family,
                    "Coordinator leadership was fenced"
                );
            }
            StorageError::CompareFailed => {
                self.commit_conflict_count = self.commit_conflict_count.saturating_add(1);
            }
            StorageError::OutcomeUnknown => {
                self.unknown_outcome_count = self.unknown_outcome_count.saturating_add(1);
            }
            StorageError::Capacity => {
                self.capacity_rejection_count = self.capacity_rejection_count.saturating_add(1);
            }
            _ => {}
        }
    }

    pub(super) async fn set_automatic_paused(
        &mut self,
        operation_id: String,
        entity_type: Option<EntityType>,
        paused: bool,
    ) -> Result<(), CoordinatorRuntimeError> {
        let fingerprint = format!(
            "automatic:{}:{}",
            entity_type.as_ref().map_or("*", |value| value.as_str()),
            paused
        );
        if let Some(result) = self.prior_admin_operation(&operation_id, &fingerprint)? {
            return if result == AdminOperationResult::AutomaticBalanceUpdated {
                Ok(())
            } else {
                Err(CoordinatorRuntimeError::IdempotencyConflict)
            };
        }
        self.reserve_admin_operation_capacity().await?;
        let mut settings = self
            .automatic_settings
            .clone()
            .unwrap_or(AutomaticBalanceSettings {
                globally_paused: false,
                paused_entity_types: Default::default(),
                version: self.version.clone(),
            });
        match entity_type {
            Some(entity_type) if paused => {
                settings.paused_entity_types.insert(entity_type);
            }
            Some(entity_type) => {
                settings.paused_entity_types.remove(&entity_type);
            }
            None => settings.globally_paused = paused,
        }
        settings.version = self.version.clone();
        let operation = self.new_admin_operation(
            operation_id,
            fingerprint,
            AdminOperationResult::AutomaticBalanceUpdated,
            self.version.clone(),
        )?;
        lattice_core::failpoint::hit(Failpoint::AdminBeforeGuardedCommit);
        let settings = self
            .store
            .commit_automatic_settings(
                &self.leader_guard,
                CommitAutomaticSettings {
                    expected: self.automatic_settings.clone(),
                    settings,
                    operation: operation.clone(),
                },
            )
            .await?;
        lattice_core::failpoint::hit(Failpoint::AdminAfterCommitBeforeResponse);
        self.automatic_globally_paused = settings.globally_paused;
        self.paused_entity_types = settings.paused_entity_types.clone();
        self.automatic_settings = Some(settings);
        self.applied_admin_operations
            .insert(operation.operation_id.clone(), operation);
        self.compact_admin_operation_history().await
    }

    pub(super) async fn manual_relocate(
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
            return match previous {
                AdminOperationResult::PlanCreated { plan_id } => Ok(plan_id),
                _ => Err(CoordinatorRuntimeError::IdempotencyConflict),
            };
        }
        let config = self
            .entity_configs
            .get(&request.entity_type)
            .ok_or(CoordinatorRuntimeError::UnknownEntityConfig)?;
        if request.shard_id.get() >= config.shard_count {
            return Err(CoordinatorRuntimeError::ShardOutOfRange);
        }
        let key = PlacementSlotKey::Shard {
            domain: config.domain.clone(),
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
            .find(|session| {
                session.placement_up() && session.hello.node.node_id == request.target_node_id
            })
            .map(|session| session.hello.node.clone())
            .ok_or(CoordinatorRuntimeError::IneligibleTarget)?;
        let strategy = self
            .strategies
            .get(&(
                config.allocation_policy_id.clone(),
                config.allocation_policy_version,
            ))
            .ok_or(CoordinatorRuntimeError::UnknownStrategy)?;
        let proposal = RebalanceProposal {
            domain: config.domain.clone(),
            policy_id: strategy.policy_id(),
            policy_version: strategy.policy_version(),
            base_version: self.version.clone(),
            trigger: RebalanceTrigger::Manual {
                source: Some(source.clone()),
                target: Some(target.clone()),
                bypass_improvement: true,
            },
            moves: vec![ProposedMove {
                domain: config.domain.clone(),
                entity_type: request.entity_type.clone(),
                shard_id: request.shard_id,
                expected_generation: request.expected_generation,
                source,
                target,
                estimated_weight: 1,
            }],
        };
        self.submit_rebalance_inner(
            proposal,
            request.entity_type,
            Some(PlanAdminContext {
                operation_id: request.operation_id,
                fingerprint,
                evaluation: false,
            }),
        )
        .await
    }

    pub(super) async fn force_remove(
        &mut self,
        request: ForceRemoveRequest,
    ) -> Result<(), CoordinatorRuntimeError> {
        let fingerprint = format!(
            "force-remove:{}:{:032x}",
            request.node_id,
            request.expected_incarnation.get()
        );
        if let Some(previous) = self.prior_admin_operation(&request.operation_id, &fingerprint)? {
            return match previous {
                AdminOperationResult::MemberRemoved {
                    ref node_id,
                    incarnation,
                } if node_id == &request.node_id && incarnation == request.expected_incarnation => {
                    Ok(())
                }
                _ => Err(CoordinatorRuntimeError::IdempotencyConflict),
            };
        }
        let member = self
            .store
            .get_member(&request.node_id)
            .await?
            .ok_or(CoordinatorRuntimeError::StaleMember)?;
        if member.node.incarnation != request.expected_incarnation {
            return Err(CoordinatorRuntimeError::StaleMember);
        }
        self.reserve_admin_operation_capacity().await?;
        self.remove_member(member, MemberRemovalReason::ForceRemoved)
            .await?;
        let version = self.next_version()?;
        let operation = self.new_admin_operation(
            request.operation_id,
            fingerprint,
            AdminOperationResult::MemberRemoved {
                node_id: request.node_id,
                incarnation: request.expected_incarnation,
            },
            version.clone(),
        )?;
        self.store
            .record_admin_operation(
                &self.leader_guard,
                RecordAdminOperation {
                    operation: operation.clone(),
                },
            )
            .await?;
        self.version = version;
        self.applied_admin_operations
            .insert(operation.operation_id.clone(), operation);
        self.compact_admin_operation_history().await
    }

    pub(super) async fn inspect(&self) -> Result<CoordinatorInspection, CoordinatorRuntimeError> {
        let now = Instant::now();
        Ok(CoordinatorInspection {
            version: self.version.clone(),
            automatic_globally_paused: self.automatic_globally_paused,
            paused_entity_types: self.paused_entity_types.iter().cloned().collect(),
            slots: self.store.list_slots(&self.version.domain).await?,
            plans: self.store.list_plans(&self.version.domain).await?,
            reconciliation_backlog: self.reconciliation.backlog,
            reconciliation_oldest_pending_millis: self.reconciliation.oldest_pending.map(
                |started| {
                    u64::try_from(now.duration_since(started).as_millis()).unwrap_or(u64::MAX)
                },
            ),
            reconciliation_last_success_age_millis: self.reconciliation.last_success.map(
                |finished| {
                    u64::try_from(now.duration_since(finished).as_millis()).unwrap_or(u64::MAX)
                },
            ),
            quarantined_records: self
                .reconciliation
                .quarantined
                .iter()
                .map(|(key, reason)| (key.clone(), reason.clone()))
                .collect(),
            durable_limits: self.store.durable_limits(&self.version.domain),
            retained_admin_operations: self.applied_admin_operations.len(),
            leadership_loss_count: self.leadership_loss_count,
            commit_conflict_count: self.commit_conflict_count,
            unknown_outcome_count: self.unknown_outcome_count,
            capacity_rejection_count: self.capacity_rejection_count,
        })
    }

    pub(super) fn prior_admin_operation(
        &self,
        operation_id: &str,
        fingerprint: &str,
    ) -> Result<Option<AdminOperationResult>, CoordinatorRuntimeError> {
        if operation_id.is_empty() || operation_id.len() > 256 {
            return Err(CoordinatorRuntimeError::InvalidAdminOperation);
        }
        self.applied_admin_operations
            .get(operation_id)
            .map(|previous| {
                if previous.fingerprint == fingerprint {
                    Ok(previous.result.clone())
                } else {
                    Err(CoordinatorRuntimeError::IdempotencyConflict)
                }
            })
            .transpose()
    }

    fn new_admin_operation(
        &self,
        operation_id: String,
        fingerprint: String,
        result: AdminOperationResult,
        version: PlacementVersion,
    ) -> Result<AdminOperationRecord, CoordinatorRuntimeError> {
        if self.applied_admin_operations.len() >= self.config.maximum_admin_operation_records {
            return Err(CoordinatorRuntimeError::OperationCapacity);
        }
        let created_unix_millis = unix_millis()?;
        let retention =
            u64::try_from(self.config.admin_operation_retention.as_millis()).unwrap_or(u64::MAX);
        Ok(AdminOperationRecord {
            operation_id,
            fingerprint,
            status: AdminOperationStatus::Completed,
            result,
            version,
            created_unix_millis,
            expires_unix_millis: created_unix_millis.saturating_add(retention),
        })
    }

    async fn persist_admin_operation(
        &mut self,
        operation: AdminOperationRecord,
    ) -> Result<(), CoordinatorRuntimeError> {
        let operation = self
            .store
            .record_admin_operation(
                &self.leader_guard,
                RecordAdminOperation {
                    operation: operation.clone(),
                },
            )
            .await?;
        self.applied_admin_operations
            .insert(operation.operation_id.clone(), operation);
        self.compact_admin_operation_history().await
    }

    pub(super) async fn compact_admin_operation_history(
        &mut self,
    ) -> Result<(), CoordinatorRuntimeError> {
        self.compact_admin_operation_history_for(0).await
    }

    async fn reserve_admin_operation_capacity(&mut self) -> Result<(), CoordinatorRuntimeError> {
        self.compact_admin_operation_history_for(1).await?;
        if self.applied_admin_operations.len() >= self.config.maximum_admin_operation_records {
            return Err(CoordinatorRuntimeError::OperationCapacity);
        }
        Ok(())
    }

    async fn compact_admin_operation_history_for(
        &mut self,
        additional: usize,
    ) -> Result<(), CoordinatorRuntimeError> {
        let now = unix_millis()?;
        let mut records = self
            .applied_admin_operations
            .values()
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by_key(|record| (record.created_unix_millis, record.operation_id.clone()));
        let excess = records
            .len()
            .saturating_add(additional)
            .saturating_sub(self.config.maximum_admin_operation_records);
        let expected = records
            .into_iter()
            .enumerate()
            .filter_map(|(index, record)| {
                (record.expires_unix_millis <= now || index < excess).then_some(record)
            })
            .collect::<Vec<_>>();
        if expected.is_empty() {
            return Ok(());
        }
        self.store
            .compact_admin_operations(
                &self.leader_guard,
                CompactAdminOperations {
                    expected: expected.clone(),
                },
            )
            .await?;
        for record in expected {
            self.applied_admin_operations.remove(&record.operation_id);
        }
        Ok(())
    }

    pub(super) async fn evaluate_rebalance(
        &mut self,
        entity_type: EntityType,
        trigger: RebalanceTrigger,
    ) -> Result<Option<u128>, CoordinatorRuntimeError> {
        if trigger == RebalanceTrigger::Automatic
            && (self.automatic_globally_paused || self.paused_entity_types.contains(&entity_type))
        {
            return Err(CoordinatorRuntimeError::Allocation(
                AllocationError::AutomaticPaused,
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
        let view = self.placement_view(&config.domain).await?;
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

    async fn evaluate_rebalance_operation(
        &mut self,
        operation_id: String,
        entity_type: EntityType,
        trigger: RebalanceTrigger,
    ) -> Result<Option<u128>, CoordinatorRuntimeError> {
        let fingerprint = format!("evaluate:{}:{trigger:?}", entity_type.as_str());
        if let Some(previous) = self.prior_admin_operation(&operation_id, &fingerprint)? {
            return match previous {
                AdminOperationResult::EvaluationCompleted { plan_id } => Ok(plan_id),
                _ => Err(CoordinatorRuntimeError::IdempotencyConflict),
            };
        }
        if trigger == RebalanceTrigger::Automatic
            && (self.automatic_globally_paused || self.paused_entity_types.contains(&entity_type))
        {
            return Err(CoordinatorRuntimeError::Allocation(
                AllocationError::AutomaticPaused,
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
        let view = self.placement_view(&config.domain).await?;
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
            self.reserve_admin_operation_capacity().await?;
            let operation = self.new_admin_operation(
                operation_id,
                fingerprint,
                AdminOperationResult::EvaluationCompleted { plan_id: None },
                self.version.clone(),
            )?;
            self.persist_admin_operation(operation).await?;
            return Ok(None);
        }
        let plan_id = self
            .submit_rebalance_inner(
                proposal,
                entity_type,
                Some(PlanAdminContext {
                    operation_id,
                    fingerprint,
                    evaluation: true,
                }),
            )
            .await?;
        if trigger == RebalanceTrigger::Automatic {
            self.last_automatic_move_at = Some(view.now);
        }
        Ok(Some(plan_id))
    }

    pub(super) async fn submit_rebalance(
        &mut self,
        proposal: RebalanceProposal,
        entity_type: EntityType,
    ) -> Result<u128, CoordinatorRuntimeError> {
        self.submit_rebalance_inner(proposal, entity_type, None)
            .await
    }

    async fn submit_rebalance_inner(
        &mut self,
        proposal: RebalanceProposal,
        entity_type: EntityType,
        admin: Option<PlanAdminContext>,
    ) -> Result<u128, CoordinatorRuntimeError> {
        if proposal.base_version != self.version
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
        if admin.is_some() {
            self.reserve_admin_operation_capacity().await?;
        }
        self.preempt_lower_priority(plan.reason.clone()).await?;
        self.revalidate_plan(&plan).await?;
        let plan_id = plan.plan_id;
        let operation = admin
            .map(|admin| {
                let result = if admin.evaluation {
                    AdminOperationResult::EvaluationCompleted {
                        plan_id: Some(plan_id),
                    }
                } else {
                    AdminOperationResult::PlanCreated { plan_id }
                };
                self.new_admin_operation(
                    admin.operation_id,
                    admin.fingerprint,
                    result,
                    self.version.clone(),
                )
            })
            .transpose()?;
        lattice_core::failpoint::hit(Failpoint::PlanBeforeGuardedCommit);
        if let Some(operation) = operation.clone() {
            self.store
                .create_plan_with_operation(
                    &self.leader_guard,
                    CreatePlanWithOperation {
                        plan: plan.clone(),
                        operation: operation.clone(),
                    },
                )
                .await?;
            self.applied_admin_operations
                .insert(operation.operation_id.clone(), operation);
        } else {
            self.store
                .create_plan(&self.leader_guard, CreatePlan { plan: plan.clone() })
                .await?;
        }
        lattice_core::failpoint::hit(Failpoint::RebalanceAfterPlanPersist);
        self.plans.insert(plan_id, plan);
        self.start_pending_moves(plan_id).await?;
        if operation.is_some() {
            self.compact_admin_operation_history().await?;
        }
        Ok(plan_id)
    }

    pub(super) async fn preempt_lower_priority(
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
            let expected_plan = plan.clone();
            for shard_id in pending {
                plan.cancel_pending_move(shard_id)
                    .map_err(CoordinatorRuntimeError::Plan)?;
            }
            plan.record_revision = plan
                .record_revision
                .next()
                .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?;
            self.store
                .update_plan(
                    &self.leader_guard,
                    UpdatePlan {
                        expected: expected_plan,
                        plan: plan.clone(),
                    },
                )
                .await?;
            self.plans.insert(plan_id, plan);
        }
        self.compact_plan_history().await?;
        Ok(())
    }

    pub(super) async fn revalidate_plan(
        &self,
        plan: &RebalancePlan,
    ) -> Result<(), CoordinatorRuntimeError> {
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
                domain: plan.domain.clone(),
                entity_type: plan.entity_type.clone(),
                shard_id: movement.shard_id,
            };
            let slot = self
                .store
                .get_slot(&key)
                .await?
                .ok_or(CoordinatorRuntimeError::UnknownSlot)?;
            if slot.version > plan.base_version
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
                    session.placement_up()
                        && session.hello.node == movement.target
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

    pub(super) async fn cancel_pending(
        &mut self,
        operation_id: String,
        plan_id: u128,
        shard_id: ShardId,
    ) -> Result<(), CoordinatorRuntimeError> {
        let fingerprint = format!("cancel:{plan_id:032x}:{}", shard_id.get());
        if let Some(previous) = self.prior_admin_operation(&operation_id, &fingerprint)? {
            return if previous == (AdminOperationResult::PendingMoveCancelled { plan_id, shard_id })
            {
                Ok(())
            } else {
                Err(CoordinatorRuntimeError::IdempotencyConflict)
            };
        }
        let mut plan = self
            .plans
            .get(&plan_id)
            .cloned()
            .ok_or(CoordinatorRuntimeError::UnknownPlan)?;
        let expected_plan = plan.clone();
        plan.cancel_pending_move(shard_id)
            .map_err(CoordinatorRuntimeError::Plan)?;
        plan.record_revision = plan
            .record_revision
            .next()
            .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?;
        self.reserve_admin_operation_capacity().await?;
        let operation = self.new_admin_operation(
            operation_id,
            fingerprint,
            AdminOperationResult::PendingMoveCancelled { plan_id, shard_id },
            self.version.clone(),
        )?;
        self.store
            .update_plan_with_operation(
                &self.leader_guard,
                UpdatePlanWithOperation {
                    expected_plan,
                    plan: plan.clone(),
                    operation: operation.clone(),
                },
            )
            .await?;
        self.plans.insert(plan_id, plan);
        self.applied_admin_operations
            .insert(operation.operation_id.clone(), operation);
        self.compact_plan_history().await?;
        self.compact_admin_operation_history().await
    }
}

fn operation_lost_leadership<T>(result: &Result<T, CoordinatorRuntimeError>) -> bool {
    matches!(
        result,
        Err(CoordinatorRuntimeError::Storage(
            StorageError::LeadershipLost
        ))
    )
}

fn unix_millis() -> Result<u64, CoordinatorRuntimeError> {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .map_err(|_| CoordinatorRuntimeError::InvalidAdminOperation)
}
