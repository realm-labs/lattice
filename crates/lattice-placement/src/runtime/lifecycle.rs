use std::{collections::BTreeSet, time::Duration};

use tokio::time::MissedTickBehavior;

use lattice_core::actor_ref::{EntityType, PlacementDomainId};

use super::{
    AllocationError, CoordinatorLeaseStore, CoordinatorRuntimeError, HandoffEvent, HandoffMachine,
    Instant, MembershipStore, MoveProgress, PlacementControlEvent, PlacementDomainLeader,
    PlacementDomainStore, PlacementSlotKey, PlacementSlotState, PlanStatus, RebalanceTrigger,
    ScopedElectionStore, membership::control_dispatch_error, mpsc, watch,
};
use crate::{
    coordinator::MemberRemovalReason,
    storage::{
        StorageError,
        domain::{DeletePlan, ReserveMove, UpdatePlan},
    },
    types::AssignmentGeneration,
};

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
                            self.observe_slot(&slot);
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
            self.observe_slot(&slot);
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
                        .and_then(|value| AssignmentGeneration::new(value).ok())
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
            .collect::<BTreeSet<_>>();
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
        renewal.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut rebalance = tokio::time::interval(self.config.rebalance_interval);
        rebalance.set_missed_tick_behavior(MissedTickBehavior::Delay);
        rebalance.reset();
        let reconciliation_millis =
            u64::try_from(self.config.reconciliation_interval.as_millis()).unwrap_or(u64::MAX);
        let jitter_bound = (reconciliation_millis / 4).max(1);
        let jitter = Duration::from_millis(self.leader.term.get() % jitter_bound);
        let mut reconciliation =
            tokio::time::interval_at(Instant::now() + jitter, self.config.reconciliation_interval);
        reconciliation.set_missed_tick_behavior(MissedTickBehavior::Delay);
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
                            cause = %error_cause(&error),
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
                        if let Err(error) = self
                            .evaluate_rebalance(entity_type.clone(), RebalanceTrigger::Automatic)
                            .await
                        {
                            report_automatic_rebalance_error(
                                &self.version.domain,
                                &entity_type,
                                &error,
                            );
                        }
                    }
                }
            }
        }
    }

    pub(super) async fn renew(&mut self) -> Result<(), CoordinatorRuntimeError> {
        self.renew_leader_lease().await?;
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
            self.remove_member(member, MemberRemovalReason::FailureDetected)
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
        let claims = self
            .claims
            .values()
            .map(|claim| (claim.lease_id, claim.grant.clone()))
            .collect::<Vec<_>>();
        for (lease_id, grant) in claims {
            match self.store.keep_lease_alive(lease_id).await {
                Ok(()) => self.replay_claim_if_connected(&grant)?,
                Err(StorageError::LeadershipLost) => {
                    self.leadership_loss_count = self.leadership_loss_count.saturating_add(1);
                    return Err(StorageError::LeadershipLost.into());
                }
                Err(error) => {
                    self.focus_reconciliation(&grant.slot);
                    tracing::warn!(
                        target: "lattice.cluster.placement",
                        domain = %self.version.domain.as_str(),
                        slot = ?grant.slot,
                        %error,
                        "claim lease keep-alive failed; scheduling focused slot reconciliation"
                    );
                }
            }
        }
        Ok(())
    }

    /// Leader keep-alive is retried only inside the remaining lease budget: a single transport
    /// deadline must not surrender a domain whose lease is still valid, and an expired budget must
    /// not pretend the lease survived.
    async fn renew_leader_lease(&mut self) -> Result<(), CoordinatorRuntimeError> {
        let mut attempt = 0_u32;
        loop {
            let error = match self.store.keep_lease_alive(self.leader_lease_id).await {
                Ok(()) => {
                    self.leader_lease_deadline = Instant::now() + self.config.leader_lease_ttl;
                    return Ok(());
                }
                Err(error) => error,
            };
            let remaining = self
                .leader_lease_deadline
                .checked_duration_since(Instant::now())
                .unwrap_or_default();
            match classify_lease_renewal(&error, remaining, self.config.renewal_interval, attempt) {
                LeaseRenewal::Retry(backoff) => {
                    tracing::warn!(
                        target: "lattice.cluster.placement",
                        domain = %self.version.domain.as_str(),
                        %error,
                        remaining_millis = remaining.as_millis(),
                        "leader lease keep-alive failed; retrying inside the remaining lease budget"
                    );
                    tokio::time::sleep(backoff).await;
                    attempt = attempt.saturating_add(1);
                }
                LeaseRenewal::Surrender => {
                    if error == StorageError::LeadershipLost {
                        self.leadership_loss_count = self.leadership_loss_count.saturating_add(1);
                    }
                    return Err(error.into());
                }
            }
        }
    }

    fn focus_reconciliation(&mut self, key: &PlacementSlotKey) {
        if self.reconciliation.focus.len() >= self.config.maximum_reconciliation_work_per_pass {
            self.reconciliation.focused = true;
            return;
        }
        self.reconciliation.focus.insert(key.clone());
    }
}

/// Automatic rebalancing routinely declines a round; only an input the operator can act on is a
/// warning. Silently discarding both kinds hides a permanently stalled balancer.
fn report_automatic_rebalance_error(
    domain: &PlacementDomainId,
    entity_type: &EntityType,
    error: &CoordinatorRuntimeError,
) {
    if declined_automatic_round(error) {
        tracing::debug!(
            target: "lattice.cluster.placement",
            domain = %domain.as_str(),
            entity_type = %entity_type.as_str(),
            %error,
            cause = %error_cause(error),
            "automatic rebalance round declined"
        );
    } else {
        tracing::warn!(
            target: "lattice.cluster.placement",
            domain = %domain.as_str(),
            entity_type = %entity_type.as_str(),
            %error,
            cause = %error_cause(error),
            "automatic rebalance round failed"
        );
    }
}

/// Every runtime error variant renders only its own message, so a rejection that wraps a strategy,
/// plan, or storage verdict reaches the log as a category with the part an operator can act on
/// discarded. The wrapped chain is what names which gate refused.
fn error_cause(error: &CoordinatorRuntimeError) -> String {
    let mut chain = Vec::new();
    let mut source = std::error::Error::source(error);
    while let Some(current) = source {
        chain.push(current.to_string());
        source = current.source();
    }
    if chain.is_empty() {
        "none".to_owned()
    } else {
        chain.join(": ")
    }
}

fn declined_automatic_round(error: &CoordinatorRuntimeError) -> bool {
    matches!(
        error,
        CoordinatorRuntimeError::Allocation(
            AllocationError::AutomaticPaused
                | AllocationError::Cooldown
                | AllocationError::ConcurrencyLimit
                | AllocationError::NoEligibleNode
                | AllocationError::Unreconciled
        )
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaseRenewal {
    Retry(Duration),
    Surrender,
}

/// Losing the exact lease-backed leader record is authority loss and is always fatal. A transport
/// deadline or unavailability is not: it may be retried while the lease it renews is still valid.
fn classify_lease_renewal(
    error: &StorageError,
    remaining: Duration,
    renewal_interval: Duration,
    attempt: u32,
) -> LeaseRenewal {
    let transient = matches!(
        error,
        StorageError::Unavailable | StorageError::Deadline | StorageError::OutcomeUnknown
    );
    if !transient || remaining.is_zero() {
        return LeaseRenewal::Surrender;
    }
    LeaseRenewal::Retry(
        renewal_interval
            .checked_div(16)
            .unwrap_or(Duration::ZERO)
            .saturating_mul(1_u32 << attempt.min(4))
            .min(remaining),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_transient_lease_failures_are_retried_and_only_inside_the_lease_budget() {
        let renewal = Duration::from_secs(4);
        let budget = Duration::from_secs(4);
        assert_eq!(
            classify_lease_renewal(&StorageError::Deadline, budget, renewal, 0),
            LeaseRenewal::Retry(Duration::from_millis(250))
        );
        assert_eq!(
            classify_lease_renewal(&StorageError::Unavailable, budget, renewal, 3),
            LeaseRenewal::Retry(Duration::from_secs(2))
        );
        assert_eq!(
            classify_lease_renewal(
                &StorageError::Deadline,
                Duration::from_millis(20),
                renewal,
                4
            ),
            LeaseRenewal::Retry(Duration::from_millis(20))
        );
        assert_eq!(
            classify_lease_renewal(&StorageError::Deadline, Duration::ZERO, renewal, 0),
            LeaseRenewal::Surrender
        );
        assert_eq!(
            classify_lease_renewal(&StorageError::LeadershipLost, budget, renewal, 0),
            LeaseRenewal::Surrender
        );
        assert_eq!(
            classify_lease_renewal(&StorageError::CompareFailed, budget, renewal, 0),
            LeaseRenewal::Surrender
        );
    }

    #[test]
    fn automatic_rebalance_separates_declined_rounds_from_actionable_failures() {
        let paused = CoordinatorRuntimeError::Allocation(AllocationError::AutomaticPaused);
        let stale = CoordinatorRuntimeError::Allocation(AllocationError::StaleLoad);
        assert!(declined_automatic_round(&paused));
        assert!(!declined_automatic_round(&stale));
        assert!(!declined_automatic_round(
            &CoordinatorRuntimeError::Storage(StorageError::Unavailable)
        ));
    }

    #[test]
    fn a_rejected_command_reports_which_gate_refused_it_and_not_only_its_category() {
        let rejected = CoordinatorRuntimeError::Allocation(AllocationError::InvalidView);
        assert_eq!(
            rejected.to_string(),
            "allocation strategy rejected the placement view: placement view is invalid"
        );
        assert_eq!(error_cause(&rejected), "placement view is invalid");
        assert_eq!(error_cause(&CoordinatorRuntimeError::DrainNotReady), "none");
    }
}
