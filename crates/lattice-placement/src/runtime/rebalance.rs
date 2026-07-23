use std::collections::BTreeSet;

use lattice_core::failpoint::Failpoint;

use super::{
    Bytes, ClaimGrant, ClaimLease, CoordinatorLeaseStore, CoordinatorRuntimeError, GrantSequence,
    HandoffEffect, HandoffEvent, HandoffMachine, MembershipStore, MoveProgress, NodeIncarnation,
    PlacementControlCommand, PlacementDomainLeader, PlacementDomainStore, PlacementSlot,
    PlacementSlotKey, PlacementSlotState, PlanReason, ScopedElectionStore, SnapshotRecord,
    membership::{send_control, slot_record_key},
};
use crate::{
    coordinator::CoordinatorDelta,
    storage::{
        StorageError,
        domain::{
            ActivateAuthority, AuthorityCommit, ClaimPredicate, CompleteMove, FenceAuthority,
            InstallAuthority, LeasedClaim, ReserveMove, TransitionSlot,
        },
    },
    types::ShardId,
};

impl<S> PlacementDomainLeader<S>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + PlacementDomainStore,
{
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
        shard_id: ShardId,
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
        shard_id: ShardId,
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
            domain: plan.domain.clone(),
            entity_type: plan.entity_type.clone(),
            shard_id,
        };
        let mut slot = self
            .store
            .get_slot(&key)
            .await?
            .ok_or(CoordinatorRuntimeError::UnknownSlot)?;
        let expected_slot = slot.clone();
        let expected_plan = plan.clone();
        plan.begin_move(shard_id, slot.assignment_generation, slot.active_move)
            .map_err(CoordinatorRuntimeError::Plan)?;
        let barrier_version = self.next_version()?;
        let barrier_sessions: BTreeSet<NodeIncarnation> = self
            .sessions
            .iter()
            .filter_map(|(incarnation, session)| {
                session
                    .hello
                    .subscribes_to(&plan.entity_type)
                    .then_some(*incarnation)
            })
            .collect();
        plan.install_barrier(shard_id, barrier_version.clone(), barrier_sessions.clone())
            .map_err(CoordinatorRuntimeError::Plan)?;
        plan.record_revision = plan
            .record_revision
            .next()
            .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?;
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
        let plan = committed.plan;
        let slot = committed.slot;
        lattice_core::failpoint::hit(Failpoint::RebalanceAfterReservationBeforeHandoff);
        lattice_core::failpoint::hit(Failpoint::HandoffAfterBeginPersist);
        self.version = barrier_version.clone();
        let mut handoff = HandoffMachine::begin(
            key.clone(),
            plan_id,
            movement.source,
            movement.target,
            movement.expected_generation,
            barrier_version,
            barrier_sessions,
        )
        .map_err(CoordinatorRuntimeError::Handoff)?;
        let effects = handoff.start();
        self.plans.insert(plan_id, plan);
        self.handoffs.insert(key.clone(), handoff);
        self.publish_slot_delta(&slot).await?;
        lattice_core::failpoint::hit(Failpoint::HandoffAfterPartialBarrier);
        Box::pin(self.apply_handoff_effects(key, effects)).await
    }

    pub(super) async fn publish_slot_delta(
        &self,
        slot: &PlacementSlot,
    ) -> Result<(), CoordinatorRuntimeError> {
        lattice_core::failpoint::hit(Failpoint::CoordinatorAfterEtcdCommitBeforeDelta);
        let record = SnapshotRecord {
            key: slot_record_key(&slot.key),
            value: Bytes::from(
                serde_json::to_vec(slot).map_err(|_| CoordinatorRuntimeError::Codec)?,
            ),
        };
        for session in self.sessions.values() {
            if !session.placement_up() {
                continue;
            }
            let include = match &slot.key {
                PlacementSlotKey::Shard { entity_type, .. } => {
                    session.hello.subscribes_to(entity_type)
                }
                PlacementSlotKey::Singleton { kind, .. } => {
                    session.hello.singleton_eligibility.contains(kind)
                        || session.hello.used_singletons.contains(kind)
                }
            };
            let delta = CoordinatorDelta {
                version: slot.version.clone(),
                records: include.then_some(record.clone()).into_iter().collect(),
            };
            let association = self
                .associations
                .get(&session.association)
                .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
            send_control(
                &association,
                &self.version.domain,
                self.version.term.get(),
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
        let expected_slot = slot.clone();
        slot.state = PlacementSlotState::Stopping;
        slot.version = self.next_version()?;
        let slot = self
            .store
            .transition_slot(
                &self.leader_guard,
                TransitionSlot {
                    expected: expected_slot,
                    slot,
                },
            )
            .await?
            .slot;
        self.version = slot.version.clone();
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
                &self.version.domain,
                self.version.term.get(),
                PlacementControlCommand::DrainSlot {
                    slot: key.clone(),
                    generation,
                    version: slot.version.clone(),
                },
                &self.config,
            )?;
            lattice_core::failpoint::hit(Failpoint::HandoffAfterDrainSend);
        }
        let recovery = self
            .plans
            .get(&plan_id)
            .is_some_and(|plan| plan.reason == PlanReason::Recovery)
            || (matches!(key, PlacementSlotKey::Singleton { .. })
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
        let expected_slot = slot.clone();
        slot.state = PlacementSlotState::StopFailed;
        slot.version = self.next_version()?;
        let slot = self
            .store
            .transition_slot(
                &self.leader_guard,
                TransitionSlot {
                    expected: expected_slot,
                    slot,
                },
            )
            .await?
            .slot;
        self.version = slot.version.clone();
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
            lattice_core::failpoint::hit(Failpoint::HandoffAfterShardDrainedBeforeClaimRevoke);
            let old_claim = self.store.get_claim(key).await?;
            if let Some(old_claim) = &old_claim
                && (old_claim.grant.owner != handoff.source
                    || old_claim.grant.assignment_generation != handoff.source_generation)
            {
                return Err(CoordinatorRuntimeError::ClaimNotProven);
            }
            if !matches!(
                slot.state,
                PlacementSlotState::Stopping | PlacementSlotState::StopFailed
            ) || slot.active_move != Some(handoff.plan_id)
            {
                return Err(CoordinatorRuntimeError::StaleHandoff);
            }
            let expected_slot = slot.clone();
            slot.state = PlacementSlotState::Fenced;
            slot.version = self.next_version()?;
            slot = self
                .store
                .fence_authority(
                    &self.leader_guard,
                    FenceAuthority {
                        expected_slot,
                        expected_claim: old_claim
                            .as_ref()
                            .map(|claim| ClaimPredicate::Present(claim.grant.clone()))
                            .unwrap_or(ClaimPredicate::Absent),
                        slot,
                    },
                )
                .await?
                .slot;
            lattice_core::failpoint::hit(Failpoint::FenceAuthorityAfterCommitBeforeEffect);
            self.version = slot.version.clone();
            let old_lease = self.claims.remove(key).map(|claim| claim.lease_id);
            self.publish_slot_delta(&slot).await?;
            if let Some(lease_id) = old_lease {
                let _ = self.store.revoke_lease(lease_id).await;
            }
        }

        if slot.state != PlacementSlotState::Fenced || slot.active_move != Some(handoff.plan_id) {
            return Err(CoordinatorRuntimeError::StaleHandoff);
        }
        let lease_id = self.store.grant_lease(self.config.claim_ttl).await?;
        let expected_slot = slot.clone();
        slot.owner = Some(handoff.target.clone());
        slot.target = None;
        slot.assignment_generation = handoff.target_generation;
        slot.state = PlacementSlotState::Allocating;
        slot.version = self.next_version()?;
        let grant = ClaimGrant {
            domain: key.domain().clone(),
            slot: key.clone(),
            owner: handoff.target.clone(),
            coordinator_term: self.leader.term,
            assignment_generation: handoff.target_generation,
            grant_sequence: GrantSequence::new(1).expect("one is a valid grant sequence"),
            ttl: self.config.claim_ttl,
        };
        let (expected_global_member, expected_domain_member) =
            self.assignment_members(&handoff.target).await?;
        let request = InstallAuthority {
            expected_global_member,
            expected_domain_member,
            expected_slot,
            slot,
            claim: LeasedClaim {
                grant: grant.clone(),
                lease_id,
            },
        };
        let committed = match self
            .store
            .install_authority(&self.leader_guard, request)
            .await
        {
            Ok(committed) => committed,
            Err(StorageError::OutcomeUnknown) => {
                self.reconciliation.focused = true;
                match self.store.get_claim(key).await? {
                    Some(claim) if claim.lease_id == lease_id && claim.grant == grant => {
                        let slot = self
                            .store
                            .get_slot(key)
                            .await?
                            .ok_or(CoordinatorRuntimeError::UnknownSlot)?;
                        AuthorityCommit { slot, claim }
                    }
                    _ => {
                        let _ = self.store.revoke_lease(lease_id).await;
                        return Err(StorageError::OutcomeUnknown.into());
                    }
                }
            }
            Err(error) => {
                let _ = self.store.revoke_lease(lease_id).await;
                return Err(error.into());
            }
        };
        let slot = committed.slot;
        let leased_claim = committed.claim;
        self.version = slot.version.clone();
        lattice_core::failpoint::hit(Failpoint::HandoffAfterNewClaimBeforeGrantSend);
        self.claims.insert(
            key.clone(),
            ClaimLease {
                lease_id: leased_claim.lease_id,
                grant: leased_claim.grant.clone(),
            },
        );
        self.publish_slot_delta(&slot).await?;
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
            &self.version.domain,
            self.version.term.get(),
            PlacementControlCommand::ClaimGranted(leased_claim.grant),
            &self.config,
        )?;
        lattice_core::failpoint::hit(Failpoint::HandoffAfterGrantBeforeShardReady);
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
        let expected_slot = slot.clone();
        slot.state = PlacementSlotState::Running;
        slot.active_move = None;
        slot.barrier_sessions.clear();
        slot.version = self.next_version()?;
        let claim = self
            .store
            .get_claim(key)
            .await?
            .ok_or(CoordinatorRuntimeError::ClaimNotProven)?;
        let (slot, completed_plan) = if let PlacementSlotKey::Shard { shard_id, .. } = key {
            let mut plan = self
                .plans
                .get(&handoff.plan_id)
                .cloned()
                .ok_or(CoordinatorRuntimeError::UnknownPlan)?;
            let expected_plan = plan.clone();
            plan.complete_move(*shard_id)
                .map_err(CoordinatorRuntimeError::Plan)?;
            plan.record_revision = plan
                .record_revision
                .next()
                .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?;
            let committed = self
                .store
                .complete_move(
                    &self.leader_guard,
                    CompleteMove {
                        expected_slot,
                        slot,
                        expected_plan,
                        plan,
                        expected_claim: claim.grant,
                    },
                )
                .await?;
            (committed.slot, Some(committed.plan))
        } else {
            let committed = self
                .store
                .activate_authority(
                    &self.leader_guard,
                    ActivateAuthority {
                        expected_slot,
                        expected_claim: claim.grant,
                        slot,
                    },
                )
                .await?;
            (committed.slot, None)
        };
        lattice_core::failpoint::hit(Failpoint::HandoffAfterActivePersistBeforeDelta);
        self.version = slot.version.clone();
        self.slot_assigned_at.insert(key.clone(), self.now());
        self.handoffs.remove(key);
        self.publish_slot_delta(&slot).await?;
        if let Some(plan) = completed_plan {
            self.plans.insert(plan.plan_id, plan);
            self.start_pending_moves(handoff.plan_id).await?;
            self.compact_plan_history().await?;
        }
        Ok(())
    }
}
