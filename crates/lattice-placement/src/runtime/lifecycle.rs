use super::membership::{control_dispatch_error, send_control};
use super::{
    CoordinatorLeaseStore, CoordinatorRuntimeError, HandoffEvent, HandoffMachine, Instant,
    MembershipStore, MoveProgress, PlacementControlCommand, PlacementControlEvent,
    PlacementDomainLeader, PlacementDomainStore, PlacementSlotKey, PlacementSlotState, PlanStatus,
    RebalanceTrigger, ScopedElectionStore, mpsc, watch,
};
use crate::storage::domain::{DeletePlan, ReserveMove, UpdatePlan};

impl<S> PlacementDomainLeader<S>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + PlacementDomainStore,
{
    pub(super) async fn recover_persisted_plans(&mut self) -> Result<(), CoordinatorRuntimeError> {
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
                    domain: plan.domain.clone(),
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
                        let (barrier_version, barrier_sessions) = if slot.state
                            == PlacementSlotState::Running
                            && slot.owner.as_ref() == Some(&movement.source)
                            && slot.assignment_generation == movement.expected_generation
                            && slot.active_move.is_none()
                        {
                            let expected_plan = plan.clone();
                            let barrier_version = self.next_version()?;
                            let barrier_sessions = movement.barrier_sessions.clone();
                            if let Some(current) = plan
                                .moves
                                .iter_mut()
                                .find(|current| current.shard_id == movement.shard_id)
                            {
                                current.barrier_version = Some(barrier_version.clone());
                            }
                            plan.record_revision = plan
                                .record_revision
                                .next()
                                .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?;
                            let expected_slot = slot.clone();
                            slot.target = Some(movement.target.clone());
                            slot.state = PlacementSlotState::BeginHandoff;
                            slot.active_move = Some(plan_id);
                            slot.barrier_sessions = barrier_sessions.clone();
                            slot.version = barrier_version.clone();
                            let committed = self
                                .store
                                .reserve_move(
                                    &self.leader_guard,
                                    ReserveMove {
                                        expected_plan,
                                        plan,
                                        expected_slot,
                                        slot,
                                    },
                                )
                                .await?;
                            plan = committed.plan;
                            slot = committed.slot;
                            self.version = barrier_version.clone();
                            (barrier_version, barrier_sessions)
                        } else {
                            (slot.version.clone(), slot.barrier_sessions.clone())
                        };
                        let handoff = HandoffMachine::recover(
                            &slot,
                            plan_id,
                            movement.source,
                            movement.target,
                            movement.expected_generation,
                            barrier_version,
                            barrier_sessions,
                        )
                        .map_err(CoordinatorRuntimeError::Handoff)?;
                        self.handoffs.insert(key, handoff);
                    }
                    MoveProgress::Completed | MoveProgress::Cancelled | MoveProgress::Failed => {}
                }
            }
            if plan_changed {
                let expected_plan = plan.clone();
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
        }
        for slot in self.store.list_slots(&self.version.domain).await? {
            if !matches!(slot.key, PlacementSlotKey::Singleton { .. })
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
                slot.version.clone(),
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

    pub(super) async fn compact_plan_history(&mut self) -> Result<(), CoordinatorRuntimeError> {
        let mut terminal = self
            .plans
            .values()
            .filter(|plan| {
                matches!(
                    plan.status,
                    PlanStatus::Completed | PlanStatus::Cancelled | PlanStatus::Failed
                )
            })
            .map(|plan| {
                (
                    plan.base_version.clone(),
                    plan.plan_id,
                    plan.record_revision,
                )
            })
            .collect::<Vec<_>>();
        terminal.sort_unstable();
        let remove = terminal
            .len()
            .saturating_sub(self.config.maximum_completed_plan_history);
        for (_, plan_id, _) in terminal.into_iter().take(remove) {
            let expected = self
                .plans
                .get(&plan_id)
                .cloned()
                .ok_or(CoordinatorRuntimeError::UnknownPlan)?;
            self.store
                .delete_plan(&self.leader_guard, DeletePlan { expected })
                .await?;
            self.plans.remove(&plan_id);
        }
        Ok(())
    }

    pub async fn run(
        mut self,
        controls: mpsc::Receiver<PlacementControlEvent>,
        shutdown: watch::Receiver<bool>,
    ) -> Result<(), CoordinatorRuntimeError> {
        let result = self.run_loop(controls, shutdown).await;
        let revoke = self.store.revoke_lease(self.leader_lease_id).await;
        match (result, revoke) {
            (Err(error), _) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
            (Ok(()), Err(error)) => Err(error.into()),
        }
    }

    async fn run_loop(
        &mut self,
        mut controls: mpsc::Receiver<PlacementControlEvent>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), CoordinatorRuntimeError> {
        let mut renewal = tokio::time::interval(self.config.renewal_interval);
        renewal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut rebalance = tokio::time::interval(self.config.rebalance_interval);
        rebalance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        rebalance.reset();
        let reconciliation_millis =
            u64::try_from(self.config.reconciliation_interval.as_millis()).unwrap_or(u64::MAX);
        let jitter_bound = (reconciliation_millis / 4).max(1);
        let jitter = std::time::Duration::from_millis(self.leader.term.get() % jitter_bound);
        let mut reconciliation =
            tokio::time::interval_at(Instant::now() + jitter, self.config.reconciliation_interval);
        reconciliation.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                _ = renewal.tick() => {
                    self.renew().await?;
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
                    if let Err(error) = result {
                        tracing::warn!(
                            target: "lattice.cluster.coordinator",
                            %error,
                            "Coordinator rejected a member control command"
                        );
                    }
                }
                operation = self.operation_receiver.recv() => {
                    let Some(operation) = operation else {
                        return Err(CoordinatorRuntimeError::OperationClosed);
                    };
                    self.handle_operation(operation).await?;
                }
                _ = reconciliation.tick() => {
                    self.reconcile_bounded_pass().await?;
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

    pub(super) async fn renew(&mut self) -> Result<(), CoordinatorRuntimeError> {
        self.store.keep_lease_alive(self.leader_lease_id).await?;
        let now = Instant::now();
        let expired = self
            .sessions
            .iter()
            .filter_map(|(incarnation, session)| {
                (now.duration_since(session.last_heartbeat) > self.config.member_heartbeat_timeout)
                    .then_some((*incarnation, session.record.clone()))
            })
            .collect::<Vec<_>>();
        for (_incarnation, member) in expired {
            self.remove_member(
                member,
                crate::coordinator::MemberRemovalReason::FailureDetected,
            )
            .await?;
        }
        let leaving = self
            .sessions
            .iter()
            .filter_map(|(incarnation, session)| session.draining.then_some(*incarnation))
            .collect::<Vec<_>>();
        for incarnation in leaving {
            self.maybe_send_drain_ready(incarnation).await?;
        }
        for claim in self.claims.values() {
            self.store.keep_lease_alive(claim.lease_id).await?;
            if let Some(session) = self.sessions.get(&claim.grant.owner.incarnation)
                && let Some(association) = self.associations.get(&session.association)
            {
                send_control(
                    &association,
                    &self.version.domain,
                    PlacementControlCommand::ClaimGranted(claim.grant.clone()),
                    &self.config,
                )?;
            }
        }
        Ok(())
    }
}
