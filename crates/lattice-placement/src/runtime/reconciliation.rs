use super::membership::send_control;
use super::{
    ClaimGrant, ClaimLease, CoordinatorLeaseStore, CoordinatorRuntimeError, GrantSequence,
    HandoffEvent, Instant, MembershipStore, PlacementControlCommand, PlacementDomainLeader,
    PlacementDomainStore, PlacementSlot, PlacementSlotKey, PlacementSlotState, ScopedElectionStore,
};
use crate::storage::domain::{
    AdoptAuthority, FenceMissingAuthority, InstallAuthority, LeasedClaim,
};

impl<S> PlacementDomainLeader<S>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + PlacementDomainStore,
{
    pub(super) async fn reconcile_initial_inventory(
        &mut self,
    ) -> Result<(), CoordinatorRuntimeError> {
        let mut offset = 0;
        loop {
            let page = self
                .store
                .list_slots_page(
                    &self.version.domain,
                    &[],
                    offset,
                    self.config.reconciliation_page_size,
                )
                .await?;
            for slot in page.records {
                self.validate_slot_move_relationship(&slot);
                let claim = self.store.get_claim(&slot.key).await?;
                self.reconcile_authority_record(slot, claim).await?;
            }
            let Some(next) = page.next_offset else {
                break;
            };
            offset = next;
        }
        self.validate_plan_move_relationships().await?;
        self.reconciliation.initial_complete = true;
        self.reconciliation.cursor = 0;
        self.reconciliation.backlog = 0;
        self.reconciliation.last_success = Some(Instant::now());
        Ok(())
    }

    pub(super) async fn reconcile_bounded_pass(&mut self) -> Result<(), CoordinatorRuntimeError> {
        if self.reconciliation.focused {
            self.reconciliation.cursor = 0;
            self.reconciliation.focused = false;
        }
        let limit = self
            .config
            .reconciliation_page_size
            .min(self.config.maximum_reconciliation_work_per_pass);
        let page = self
            .store
            .list_slots_page(&self.version.domain, &[], self.reconciliation.cursor, limit)
            .await?;
        let total = page.total;
        for slot in page.records {
            self.validate_slot_move_relationship(&slot);
            let claim = self.store.get_claim(&slot.key).await?;
            self.reconcile_authority_record(slot, claim).await?;
        }
        match page.next_offset {
            Some(next) => {
                self.reconciliation.cursor = next;
                self.reconciliation.backlog = total.saturating_sub(next);
                if self.reconciliation.oldest_pending.is_none() {
                    self.reconciliation.oldest_pending = Some(Instant::now());
                }
            }
            None => {
                self.reconciliation.cursor = 0;
                self.reconciliation.backlog = 0;
                self.reconciliation.oldest_pending = None;
                self.reconciliation.last_success = Some(Instant::now());
            }
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
                    movement.shard_id == *shard_id
                        && movement.progress == crate::plan::MoveProgress::Handoff
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
                        .filter(|movement| movement.progress == crate::plan::MoveProgress::Handoff)
                        .map(|movement| PlacementSlotKey::Shard {
                            domain: plan.domain.clone(),
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
        let active = matches!(
            slot.state,
            PlacementSlotState::Allocating | PlacementSlotState::Running
        );
        match (active, claim) {
            (true, Some(claim)) if self.claim_matches_persisted_slot(&claim, &slot) => {
                if claim.grant.coordinator_term < self.leader.term {
                    self.adopt_authority_record(slot, claim).await?;
                } else if claim.grant.coordinator_term == self.leader.term {
                    self.remember_and_replay_claim(claim)?;
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
            (true, None) => self.fence_missing_claim(slot).await?,
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
                    Box::pin(self.apply_handoff_effects(slot.key, effects)).await?;
                }
            }
            (false, None)
                if slot.state == PlacementSlotState::Fenced && slot.active_move.is_none() =>
            {
                self.reinstall_same_owner(slot).await?;
            }
            _ => {}
        }
        Ok(())
    }

    fn claim_matches_persisted_slot(&self, claim: &LeasedClaim, slot: &PlacementSlot) -> bool {
        claim.grant.slot == slot.key
            && slot.owner.as_ref() == Some(&claim.grant.owner)
            && slot.assignment_generation == claim.grant.assignment_generation
            && slot.version.term == claim.grant.coordinator_term
    }

    async fn adopt_authority_record(
        &mut self,
        slot: PlacementSlot,
        previous: LeasedClaim,
    ) -> Result<(), CoordinatorRuntimeError> {
        let lease_id = self.store.grant_lease(self.config.claim_ttl).await?;
        let mut adopted = slot.clone();
        adopted.version = self.next_version()?;
        let grant = ClaimGrant {
            domain: slot.key.domain().clone(),
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
        let (expected_global_member, expected_domain_member) =
            self.assignment_members(&previous.grant.owner).await?;
        let result = self
            .store
            .adopt_authority(
                &self.leader_guard,
                AdoptAuthority {
                    expected_global_member,
                    expected_domain_member,
                    expected_slot: slot,
                    expected_claim: previous.grant.clone(),
                    slot: adopted,
                    claim: LeasedClaim {
                        grant: grant.clone(),
                        lease_id,
                    },
                },
            )
            .await;
        match result {
            Ok(committed) => {
                lattice_core::failpoint::hit(
                    lattice_core::failpoint::Failpoint::ReconciliationAfterCommitBeforeEffect,
                );
                let _ = self.store.revoke_lease(previous.lease_id).await;
                self.version = committed.slot.version.clone();
                self.claims.insert(
                    committed.slot.key.clone(),
                    ClaimLease {
                        lease_id: committed.claim.lease_id,
                        grant: committed.claim.grant.clone(),
                    },
                );
                self.publish_slot_delta(&committed.slot).await?;
                self.replay_claim_if_connected(&committed.claim.grant)?;
            }
            Err(error) => {
                let _ = self.store.revoke_lease(lease_id).await;
                return Err(error.into());
            }
        }
        Ok(())
    }

    async fn fence_missing_claim(
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
        self.claims.remove(&committed.slot.key);
        self.publish_slot_delta(&committed.slot).await
    }

    async fn reinstall_same_owner(
        &mut self,
        slot: PlacementSlot,
    ) -> Result<(), CoordinatorRuntimeError> {
        let Some(owner) = slot.owner.clone() else {
            self.quarantine(&slot.key, "Fenced slot has no prior owner");
            return Ok(());
        };
        if self
            .sessions
            .get(&owner.incarnation)
            .is_none_or(|session| session.hello.node != owner)
        {
            return Ok(());
        }
        let lease_id = self.store.grant_lease(self.config.claim_ttl).await?;
        let mut allocating = slot.clone();
        allocating.assignment_generation = allocating
            .assignment_generation
            .next()
            .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?;
        allocating.state = PlacementSlotState::Allocating;
        allocating.version = self.next_version()?;
        let grant = ClaimGrant {
            domain: allocating.key.domain().clone(),
            slot: allocating.key.clone(),
            owner: owner.clone(),
            coordinator_term: self.leader.term,
            assignment_generation: allocating.assignment_generation,
            grant_sequence: GrantSequence::new(1).expect("one is a valid grant sequence"),
            ttl: self.config.claim_ttl,
        };
        let (expected_global_member, expected_domain_member) =
            self.assignment_members(&owner).await?;
        let result = self
            .store
            .install_authority(
                &self.leader_guard,
                InstallAuthority {
                    expected_global_member,
                    expected_domain_member,
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
                self.claims.insert(
                    committed.slot.key.clone(),
                    ClaimLease {
                        lease_id,
                        grant: grant.clone(),
                    },
                );
                self.publish_slot_delta(&committed.slot).await?;
                self.replay_claim_if_connected(&grant)?;
                Ok(())
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
        self.claims.insert(
            claim.grant.slot.clone(),
            ClaimLease {
                lease_id: claim.lease_id,
                grant: claim.grant.clone(),
            },
        );
        self.replay_claim_if_connected(&claim.grant)
    }

    fn replay_claim_if_connected(&self, grant: &ClaimGrant) -> Result<(), CoordinatorRuntimeError> {
        let Some(session) = self.sessions.get(&grant.owner.incarnation) else {
            return Ok(());
        };
        let Some(association) = self.associations.get(&session.association) else {
            return Ok(());
        };
        send_control(
            &association,
            &self.version.domain,
            PlacementControlCommand::ClaimGranted(grant.clone()),
            &self.config,
        )
    }

    fn quarantine(&mut self, key: &PlacementSlotKey, reason: &str) {
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
