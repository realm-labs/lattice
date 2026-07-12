use super::membership::plan_priority;
use super::{
    AppliedAdminOperation, BTreeMap, CoordinatorInspection, CoordinatorLeader,
    CoordinatorOperation, CoordinatorRuntimeError, CoordinatorStore, ManualRelocationRequest,
    MoveProgress, NodeKey, PlacementSlotKey, PlacementSlotState, PlanReason, PlanStatus,
    RebalancePlan, RebalanceProposal, RebalanceTrigger,
};

impl<S: CoordinatorStore> CoordinatorLeader<S> {
    pub(super) async fn handle_operation(&mut self, operation: CoordinatorOperation) {
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

    pub(super) async fn set_automatic_paused(
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

    pub(super) async fn inspect(&self) -> Result<CoordinatorInspection, CoordinatorRuntimeError> {
        Ok(CoordinatorInspection {
            term: self.leader.term,
            revision: self.revision,
            automatic_globally_paused: self.automatic_globally_paused,
            paused_entity_types: self.paused_entity_types.iter().cloned().collect(),
            slots: self.store.list_slots().await?,
            plans: self.store.list_plans().await?,
        })
    }

    pub(super) fn prior_admin_operation(
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

    pub(super) fn record_admin_operation(
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

    pub(super) async fn evaluate_rebalance(
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

    pub(super) async fn submit_rebalance(
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

    pub(super) async fn cancel_pending(
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
}
