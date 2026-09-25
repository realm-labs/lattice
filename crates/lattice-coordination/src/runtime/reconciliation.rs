use crate::failpoints;

use super::{
    ActorGroupStore, AllocationRequest, ClaimGrant, CoordinatorLeaseStore, CoordinatorRuntimeError,
    GrantSequence, GroupCoordinator, HandoffEvent, Instant, MemberRemovalReason, MembershipStore,
    PlacementSlot, PlacementSlotKey, PlacementSlotState, ScopedElectionStore,
};

use crate::{
    allocation::AllocationError,
    coordinator::MemberStatus,
    plan::MoveProgress,
    storage::records::{
        AdoptAuthority, FenceMissingAuthority, InstallAuthority, LeasedClaim, RemoveGroupMember,
    },
    types::PlacementVersion,
};

impl<S> GroupCoordinator<S>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + ActorGroupStore,
{
    pub(super) async fn reconcile_initial_inventory(
        &mut self,
    ) -> Result<(), CoordinatorRuntimeError> {
        self.reconcile_group_member_inventory().await?;
        let mut cursor = None;
        loop {
            let page = self
                .store
                .list_slots_page(
                    &self.version.group,
                    &[],
                    cursor.as_ref(),
                    self.config.reconciliation_page_size,
                )
                .await?;
            for slot in page.records {
                self.observe_slot(&slot);
                self.validate_slot_move_relationship(&slot);
                let claim = self.store.get_claim(&slot.key).await?;
                self.reconcile_authority_record(slot, claim).await?;
            }
            let Some(next) = page.next_cursor else {
                break;
            };
            cursor = Some(next);
        }
        self.validate_plan_move_relationships().await?;
        self.reconciliation.initial_complete = true;
        self.reconciliation.cursor = None;
        self.reconciliation.backlog = 0;
        self.reconciliation.last_success = Some(Instant::now());
        Ok(())
    }

    async fn reconcile_group_member_inventory(&mut self) -> Result<(), CoordinatorRuntimeError> {
        let members = self.store.list_group_members(&self.version.group).await?;
        for member in members {
            let global = self.store.get_member(&member.node.node_id).await?;
            let stale = global.as_ref().is_none_or(|global| {
                global.node != member.node || global.status != MemberStatus::Up
            });
            if !stale {
                if !self.sessions.contains_key(&member.node.incarnation) {
                    self.reconciliation
                        .unrecovered_members
                        .insert(member.node.incarnation, member);
                }
                continue;
            }
            let node = member.node.clone();
            self.store
                .remove_group_member(&self.leader_guard, RemoveGroupMember { expected: member })
                .await?;
            self.version = PlacementVersion::new(
                self.version.group.clone(),
                self.version.term,
                self.store
                    .get_placement_revision(&self.version.group)
                    .await?,
            );
            tracing::info!(
                target: "lattice.cluster.placement",
                group = %self.version.group.as_str(),
                node_id = %node.node_id,
                incarnation = ?node.incarnation,
                "removed orphaned actor-group member before inventory recovery"
            );
            self.finish_node_removal(node, MemberRemovalReason::FailureDetected)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn reconcile_bounded_pass(&mut self) -> Result<(), CoordinatorRuntimeError> {
        self.retire_unrecovered_members().await?;
        self.reconciliation.barrier_gc_cursor = self
            .store
            .reclaim_orphan_barriers(
                &self.leader_guard,
                self.reconciliation.barrier_gc_cursor.as_deref(),
            )
            .await?;
        if self.reconciliation.focused {
            self.reconciliation.cursor = None;
            self.reconciliation.focused = false;
        }
        let focus = std::mem::take(&mut self.reconciliation.focus);
        let focused_keys = focus.clone();
        for key in focus {
            let Some(slot) = self.store.get_slot(&key).await? else {
                continue;
            };
            self.observe_slot(&slot);
            self.validate_slot_move_relationship(&slot);
            let claim = self.store.get_claim(&key).await?;
            self.reconcile_authority_record(slot, claim).await?;
        }
        let limit = self
            .config
            .reconciliation_page_size
            .min(self.config.maximum_reconciliation_work_per_pass);
        let page = self
            .store
            .list_slots_page(
                &self.version.group,
                &[],
                self.reconciliation.cursor.as_ref(),
                limit,
            )
            .await?;
        let remaining = page.remaining;
        for slot in page.records {
            if focused_keys.contains(&slot.key) {
                continue;
            }
            self.observe_slot(&slot);
            self.validate_slot_move_relationship(&slot);
            let claim = self.store.get_claim(&slot.key).await?;
            self.reconcile_authority_record(slot, claim).await?;
        }
        match page.next_cursor {
            Some(next) => {
                self.reconciliation.cursor = Some(next);
                self.reconciliation.backlog = remaining;
                if self.reconciliation.oldest_pending.is_none() {
                    self.reconciliation.oldest_pending = Some(Instant::now());
                }
            }
            None => {
                self.reconciliation.cursor = None;
                self.reconciliation.backlog = 0;
                self.reconciliation.oldest_pending = None;
                self.reconciliation.last_success = Some(Instant::now());
            }
        }
        Ok(())
    }

    /// A recovered participation record is not a live description. Give its node
    /// the normal heartbeat grace period to reattach; otherwise remove that exact
    /// group admission, even if another group keeps global membership alive.
    /// This is only a session fence: prior grants still require the full holdoff.
    async fn retire_unrecovered_members(&mut self) -> Result<(), CoordinatorRuntimeError> {
        if self.origin.elapsed() <= self.config.member_heartbeat_timeout {
            return Ok(());
        }
        let candidates = self
            .reconciliation
            .unrecovered_members
            .values()
            .take(self.config.maximum_reconciliation_work_per_pass)
            .cloned()
            .collect::<Vec<_>>();
        for expected in candidates {
            let incarnation = expected.node.incarnation;
            if self.sessions.contains_key(&incarnation) {
                self.reconciliation.unrecovered_members.remove(&incarnation);
                continue;
            }
            let current = self
                .store
                .get_group_member(&self.version.group, &expected.node.node_id)
                .await?;
            if current.as_ref().is_some_and(|record| record != &expected) {
                self.reconciliation.unrecovered_members.remove(&incarnation);
                continue;
            }
            if current.is_some() {
                self.store
                    .remove_group_member(
                        &self.leader_guard,
                        RemoveGroupMember {
                            expected: expected.clone(),
                        },
                    )
                    .await?;
                self.version = PlacementVersion::new(
                    self.version.group.clone(),
                    self.version.term,
                    self.store
                        .get_placement_revision(&self.version.group)
                        .await?,
                );
            }
            self.reconciliation.unrecovered_members.remove(&incarnation);
            self.finish_node_removal(expected.node, MemberRemovalReason::FailureDetected)
                .await?;
        }
        Ok(())
    }

    fn validate_slot_move_relationship(&mut self, slot: &PlacementSlot) {
        let Some(plan_id) = slot.active_move else {
            return;
        };
        if let PlacementSlotKey::Shard { shard_id, .. } = &slot.key {
            let valid = self.plans.get(&plan_id).is_some_and(|plan| {
                plan.moves.iter().any(|movement| {
                    movement.shard_id == *shard_id && movement.progress == MoveProgress::Handoff
                })
            });
            if !valid {
                self.quarantine(&slot.key, "slot active_move has no matching handoff plan");
            }
        }
    }

    async fn validate_plan_move_relationships(&mut self) -> Result<(), CoordinatorRuntimeError> {
        let plan_ids = self.plans.keys().copied().collect::<Vec<_>>();
        for plan_id in plan_ids {
            let plan_moves = self
                .plans
                .get(&plan_id)
                .into_iter()
                .flat_map(|plan| {
                    plan.moves
                        .iter()
                        .filter(|movement| movement.progress == MoveProgress::Handoff)
                        .map(|movement| PlacementSlotKey::Shard {
                            group: plan.group.clone(),
                            entity_type: plan.entity_type.clone(),
                            shard_id: movement.shard_id,
                        })
                })
                .collect::<Vec<_>>();
            for key in plan_moves {
                if self
                    .store
                    .get_slot(&key)
                    .await?
                    .is_none_or(|slot| slot.active_move != Some(plan_id))
                {
                    self.quarantine(&key, "handoff plan has no matching slot active_move");
                }
            }
        }
        Ok(())
    }

    async fn reconcile_authority_record(
        &mut self,
        slot: PlacementSlot,
        claim: Option<LeasedClaim>,
    ) -> Result<(), CoordinatorRuntimeError> {
        let key = slot.key.clone();
        let active = matches!(
            slot.state,
            PlacementSlotState::Allocating | PlacementSlotState::Running
        );
        match (active, claim) {
            (true, Some(claim)) if self.claim_matches_persisted_slot(&claim, &slot) => {
                if claim.grant.coordinator_term < self.leader.term {
                    self.adopt_authority_record(slot, claim).await?;
                    self.clear_quarantine(&key);
                } else if claim.grant.coordinator_term == self.leader.term {
                    self.remember_and_replay_claim(claim)?;
                    self.clear_quarantine(&key);
                } else {
                    self.quarantine(&slot.key, "claim term is ahead of the elected leader");
                }
            }
            (true, Some(_)) => {
                self.quarantine(
                    &slot.key,
                    "active slot and claim owner/generation do not match",
                );
            }
            (true, None) => {
                self.fence_missing_claim(slot.clone()).await?;
                self.clear_quarantine(&slot.key);
            }
            (false, Some(_)) if slot.state == PlacementSlotState::Fenced => {
                self.quarantine(&slot.key, "Fenced slot still has a claim");
            }
            (false, None)
                if matches!(
                    slot.state,
                    PlacementSlotState::Stopping | PlacementSlotState::StopFailed
                ) =>
            {
                if let Some(handoff) = self.handoffs.get(&slot.key).cloned() {
                    let effects = self
                        .handoffs
                        .get_mut(&slot.key)
                        .expect("handoff was just read")
                        .transition(HandoffEvent::SourceAuthorityInvalid {
                            source: handoff.source,
                            generation: handoff.source_generation,
                        })
                        .map_err(CoordinatorRuntimeError::Handoff)?;
                    Box::pin(self.apply_handoff_effects(key.clone(), effects)).await?;
                }
                self.clear_quarantine(&slot.key);
            }
            (false, None)
                if slot.state == PlacementSlotState::Fenced && slot.active_move.is_none() =>
            {
                if !self.reinstall_fenced_authority(slot.clone()).await? {
                    return Ok(());
                }
                self.clear_quarantine(&slot.key);
            }
            (false, None)
                if slot.state == PlacementSlotState::Fenced && slot.active_move.is_some() =>
            {
                if self.handoffs.contains_key(&key) {
                    Box::pin(self.replace_authority(&key)).await?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn clear_quarantine(&mut self, key: &PlacementSlotKey) {
        self.reconciliation.quarantined.remove(&format!("{key:?}"));
    }

    fn claim_matches_persisted_slot(&self, claim: &LeasedClaim, slot: &PlacementSlot) -> bool {
        claim.grant.slot == slot.key
            && slot.owner.as_ref() == Some(&claim.grant.owner)
            && slot.assignment_generation == claim.grant.assignment_generation
            && slot.version.term <= claim.grant.coordinator_term
    }

    async fn adopt_authority_record(
        &mut self,
        slot: PlacementSlot,
        previous: LeasedClaim,
    ) -> Result<(), CoordinatorRuntimeError> {
        let members = match self.assignment_members(&previous.grant.owner).await {
            Ok(members) => members,
            Err(CoordinatorRuntimeError::IneligibleTarget) => return Ok(()),
            Err(error) => return Err(error),
        };
        let lease_id = self.store.grant_lease(self.config.claim_ttl).await?;
        let grant = ClaimGrant {
            request_id: 0,
            group: slot.key.group().clone(),
            slot: slot.key.clone(),
            owner: previous.grant.owner.clone(),
            coordinator_term: self.leader.term,
            assignment_generation: previous.grant.assignment_generation,
            grant_sequence: previous
                .grant
                .grant_sequence
                .next()
                .map_err(|_| CoordinatorRuntimeError::ClaimSequence)?,
            ttl: self.config.claim_ttl,
        };
        let (expected_global_member, expected_group_member) = members;
        let result = self
            .store
            .adopt_authority(
                &self.leader_guard,
                AdoptAuthority {
                    expected_global_member,
                    expected_group_member,
                    expected_slot: slot,
                    expected_claim: previous.grant.clone(),
                    claim: LeasedClaim {
                        grant: grant.clone(),
                        lease_id,
                    },
                },
            )
            .await;
        match result {
            Ok(committed) => {
                super::post_commit_failpoint(
                    failpoints::RECONCILIATION_AFTER_COMMIT_BEFORE_EFFECT,
                )?;
                let _ = self.store.revoke_lease(previous.lease_id).await;
                self.remember_claim(committed.lease_id, committed.grant.clone());
                self.replay_claim_if_connected(&committed.grant)?;
            }
            Err(error) => {
                let _ = self.store.revoke_lease(lease_id).await;
                return Err(error.into());
            }
        }
        Ok(())
    }

    pub(super) async fn fence_missing_claim(
        &mut self,
        slot: PlacementSlot,
    ) -> Result<(), CoordinatorRuntimeError> {
        let mut fenced = slot.clone();
        fenced.state = PlacementSlotState::Fenced;
        fenced.version = self.next_version()?;
        let committed = self
            .store
            .fence_missing_authority(
                &self.leader_guard,
                FenceMissingAuthority {
                    expected_slot: slot,
                    slot: fenced,
                },
            )
            .await?;
        self.version = committed.slot.version.clone();
        self.release_claim(&committed.slot.key);
        self.publish_slot_delta(&committed.slot).await
    }

    pub(super) async fn reinstall_fenced_authority(
        &mut self,
        slot: PlacementSlot,
    ) -> Result<bool, CoordinatorRuntimeError> {
        if slot.state != PlacementSlotState::Fenced || slot.active_move.is_some() {
            return Err(CoordinatorRuntimeError::StaleHandoff);
        }
        // Election reconciliation runs before a placement view is declared reconciled and before
        // any member session is available. Keep the fenced record durable during that pass; a
        // bounded pass or an explicit resolve will reinstall it once an eligible host has joined.
        if !self.reconciliation.initial_complete {
            return Ok(false);
        }
        if !self.replacement_wait_complete(&slot.key, slot.assignment_generation) {
            self.focus_reconciliation(&slot.key);
            return Ok(false);
        }
        let owner = match &slot.key {
            PlacementSlotKey::Shard {
                entity_type,
                shard_id,
                ..
            } => {
                let Some(config) = self.entity_configs.get(entity_type).cloned() else {
                    return Ok(false);
                };
                let strategy = self
                    .strategies
                    .get(&(
                        config.allocation_policy_id.clone(),
                        config.allocation_policy_version,
                    ))
                    .cloned()
                    .ok_or(CoordinatorRuntimeError::UnknownStrategy)?;
                let view = self.placement_view(&config.group).await?;
                match strategy.allocate(
                    &AllocationRequest {
                        group: config.group,
                        entity_type: entity_type.clone(),
                        shard_id: *shard_id,
                        required_protocol: config.protocol_id,
                    },
                    &view,
                ) {
                    Ok(decision) => decision.target,
                    Err(AllocationError::NoEligibleNode) => return Ok(false),
                    Err(error) => return Err(CoordinatorRuntimeError::Allocation(error)),
                }
            }
            PlacementSlotKey::Singleton { kind, .. } => {
                let Some(config) = self.singleton_configs.get(kind).cloned() else {
                    return Ok(false);
                };
                match self.select_singleton_target(kind, &config, None) {
                    Ok(target) => target,
                    Err(CoordinatorRuntimeError::IneligibleTarget) => return Ok(false),
                    Err(error) => return Err(error),
                }
            }
        };
        let lease_id = self.store.grant_lease(self.config.claim_ttl).await?;
        let mut allocating = slot.clone();
        allocating.owner = Some(owner.clone());
        allocating.target = None;
        allocating.assignment_generation = allocating
            .assignment_generation
            .next()
            .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?;
        allocating.state = PlacementSlotState::Allocating;
        allocating.version = self.next_version()?;
        let grant = ClaimGrant {
            request_id: 0,
            group: allocating.key.group().clone(),
            slot: allocating.key.clone(),
            owner: owner.clone(),
            coordinator_term: self.leader.term,
            assignment_generation: allocating.assignment_generation,
            grant_sequence: GrantSequence::new(1).expect("one is a valid grant sequence"),
            ttl: self.config.claim_ttl,
        };
        let (expected_global_member, expected_group_member) =
            self.assignment_members(&owner).await?;
        let result = self
            .store
            .install_authority(
                &self.leader_guard,
                InstallAuthority {
                    expected_global_member,
                    expected_group_member,
                    expected_slot: slot,
                    slot: allocating,
                    claim: LeasedClaim {
                        grant: grant.clone(),
                        lease_id,
                    },
                },
            )
            .await;
        match result {
            Ok(committed) => {
                self.version = committed.slot.version.clone();
                self.remember_claim(lease_id, grant.clone());
                self.publish_slot_delta(&committed.slot).await?;
                self.replay_claim_if_connected(&grant)?;
                Ok(true)
            }
            Err(error) => {
                let _ = self.store.revoke_lease(lease_id).await;
                Err(error.into())
            }
        }
    }

    fn remember_and_replay_claim(
        &mut self,
        claim: LeasedClaim,
    ) -> Result<(), CoordinatorRuntimeError> {
        if self.claim_is_expiring(&claim.grant.slot, claim.lease_id) {
            return Ok(());
        }
        self.remember_claim(claim.lease_id, claim.grant.clone());
        self.replay_claim_if_connected(&claim.grant)
    }

    pub(super) fn replay_claim_if_connected(
        &self,
        grant: &ClaimGrant,
    ) -> Result<(), CoordinatorRuntimeError> {
        match self.grant_authority(grant) {
            Err(
                CoordinatorRuntimeError::UnknownSession
                | CoordinatorRuntimeError::AssociationUnavailable,
            ) => Ok(()),
            result => result,
        }
    }

    pub(super) fn quarantine(&mut self, key: &PlacementSlotKey, reason: &str) {
        if self.reconciliation.quarantined.len() < self.config.maximum_quarantined_records {
            self.reconciliation
                .quarantined
                .insert(format!("{key:?}"), reason.to_owned());
        }
        if self.reconciliation.oldest_pending.is_none() {
            self.reconciliation.oldest_pending = Some(Instant::now());
        }
    }
}
