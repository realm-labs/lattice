use super::membership::{send_control, slot_record_key};
use super::{
    Bytes, ClaimGrant, ClaimLease, CoordinatorLeader, CoordinatorRuntimeError, CoordinatorStore,
    GrantSequence, HandoffEffect, HandoffEvent, HandoffMachine, MoveProgress, NodeIncarnation,
    PlacementControlCommand, PlacementSlot, PlacementSlotKey, PlacementSlotState, PlanReason,
    SnapshotRecord,
};

impl<S: CoordinatorStore> CoordinatorLeader<S> {
    pub(super) async fn start_pending_moves(
        &mut self,
        plan_id: u128,
    ) -> Result<(), CoordinatorRuntimeError> {
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

    pub(super) fn can_start_move(
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

    pub(super) async fn begin_move(
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

    pub(super) async fn publish_slot_delta(
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
            if session.record.status != crate::coordinator::MemberStatus::Up {
                continue;
            }
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

    pub(super) async fn transition_handoff(
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

    pub(super) async fn apply_handoff_effects(
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

    pub(super) async fn drain_source(
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

    pub(super) async fn record_stop_failed(
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

    pub(super) async fn replace_authority(
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

    pub(super) async fn publish_active(
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
}
