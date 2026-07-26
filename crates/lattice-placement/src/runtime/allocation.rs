use std::collections::BTreeSet;

use lattice_core::{
    actor_ref::{EntityType, PlacementDomainId, SingletonKind},
    failpoint::Failpoint,
};

use super::{
    AllocationRequest, BTreeMap, ClaimGrant, CoordinatorLeaseStore, CoordinatorRuntimeError,
    GrantSequence, HandoffMachine, LoadSample, MembershipStore, MoveProgress, NodeKey, PlacedShard,
    PlacementDomainLeader, PlacementDomainStore, PlacementNode, PlacementSlot, PlacementSlotKey,
    PlacementSlotState, PlacementView, ScopedElectionStore, SingletonConfig,
};
use crate::{
    storage::{
        StorageError,
        domain::{
            ActivateAuthority, AllocateInitial, AuthorityCommit, LeasedClaim, ReserveHandoff,
        },
    },
    types::{AssignmentGeneration, ShardId},
};

/// Singleton kinds share one eligibility set, so a plain node ordering would land every kind on the
/// same node. The rank is a pure function of kind and node, so re-election reproduces it.
fn singleton_placement_rank(kind: &SingletonKind, node: &NodeKey) -> [u8; 16] {
    let mut input = Vec::new();
    input.extend_from_slice(kind.as_str().as_bytes());
    input.push(0);
    input.extend_from_slice(node.node_id.as_bytes());
    input.extend_from_slice(&node.incarnation.get().to_be_bytes());
    let digest = blake3::hash(&input);
    let mut rank = [0_u8; 16];
    rank.copy_from_slice(&digest.as_bytes()[..16]);
    rank
}

impl<S> PlacementDomainLeader<S>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + PlacementDomainStore,
{
    /// Returns whether the resolution itself published the slot, so a caller only falls back to a
    /// full snapshot when the requester was not already brought current by that delta.
    pub(super) async fn ensure_shard_allocated(
        &mut self,
        entity_type: EntityType,
        shard_id: ShardId,
    ) -> Result<bool, CoordinatorRuntimeError> {
        let config = self
            .entity_configs
            .get(&entity_type)
            .cloned()
            .ok_or(CoordinatorRuntimeError::UnknownEntityConfig)?;
        if shard_id.get() >= config.shard_count {
            return Err(CoordinatorRuntimeError::ShardOutOfRange);
        }
        let key = PlacementSlotKey::Shard {
            domain: config.domain.clone(),
            entity_type: entity_type.clone(),
            shard_id,
        };
        if let Some(slot) = self.store.get_slot(&key).await? {
            return match slot.state {
                PlacementSlotState::Allocating | PlacementSlotState::Running => Ok(false),
                PlacementSlotState::Fenced if slot.active_move.is_none() => {
                    if self.reinstall_fenced_authority(slot).await? {
                        Ok(true)
                    } else {
                        Err(CoordinatorRuntimeError::IneligibleTarget)
                    }
                }
                _ => Err(CoordinatorRuntimeError::StaleHandoff),
            };
        }
        let strategy = self
            .strategies
            .get(&(
                config.allocation_policy_id.clone(),
                config.allocation_policy_version,
            ))
            .cloned()
            .ok_or(CoordinatorRuntimeError::UnknownStrategy)?;
        let view = self.placement_view(&config.domain).await?;
        let decision = strategy
            .allocate(
                &AllocationRequest {
                    domain: config.domain.clone(),
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
            assignment_generation: AssignmentGeneration::new(1)
                .expect("one is a valid assignment generation"),
            version: self.next_version()?,
            state: PlacementSlotState::Allocating,
            active_move: None,
            barrier_sessions: Default::default(),
        };
        self.persist_initial_allocation(slot).await?;
        Ok(true)
    }

    pub(super) async fn ensure_singleton_allocated(
        &mut self,
        kind: SingletonKind,
    ) -> Result<bool, CoordinatorRuntimeError> {
        let config = self
            .singleton_configs
            .get(&kind)
            .cloned()
            .ok_or(CoordinatorRuntimeError::UnknownSingletonConfig)?;
        let key = PlacementSlotKey::Singleton {
            domain: config.domain.clone(),
            kind: kind.clone(),
        };
        if let Some(slot) = self.store.get_slot(&key).await? {
            return match slot.state {
                PlacementSlotState::Allocating | PlacementSlotState::Running => Ok(false),
                PlacementSlotState::Fenced if slot.active_move.is_none() => {
                    if self.reinstall_fenced_authority(slot).await? {
                        Ok(true)
                    } else {
                        Err(CoordinatorRuntimeError::IneligibleTarget)
                    }
                }
                _ => Err(CoordinatorRuntimeError::StaleHandoff),
            };
        }
        let target = self.select_singleton_target(&kind, &config, None)?;
        let slot = PlacementSlot {
            key,
            config_fingerprint: config.fingerprint(),
            owner: Some(target),
            target: None,
            assignment_generation: AssignmentGeneration::new(1)
                .expect("one is a valid assignment generation"),
            version: self.next_version()?,
            state: PlacementSlotState::Allocating,
            active_move: None,
            barrier_sessions: Default::default(),
        };
        self.persist_initial_allocation(slot).await?;
        Ok(true)
    }

    pub(super) fn select_singleton_target(
        &self,
        kind: &SingletonKind,
        config: &SingletonConfig,
        exclude: Option<&NodeKey>,
    ) -> Result<NodeKey, CoordinatorRuntimeError> {
        let target_release = self
            .sessions
            .values()
            .filter(|session| {
                session.placement_up()
                    && !session.draining
                    && exclude != Some(&session.hello.node)
                    && session.hello.singleton_eligibility.contains(kind)
                    && session.hello.singleton_configs.contains(config)
                    && session
                        .record
                        .hello
                        .protocols
                        .iter()
                        .any(|protocol| protocol.protocol_id == config.protocol_id)
            })
            .map(|session| session.record.hello.release.release_id)
            .max();
        self.sessions
            .values()
            .filter(|session| {
                Some(session.record.hello.release.release_id) == target_release
                    && session.placement_up()
                    && !session.draining
                    && exclude != Some(&session.hello.node)
                    && session.hello.singleton_eligibility.contains(kind)
                    && session.hello.singleton_configs.contains(config)
                    && session
                        .record
                        .hello
                        .protocols
                        .iter()
                        .any(|protocol| protocol.protocol_id == config.protocol_id)
            })
            .map(|session| session.hello.node.clone())
            .min_by(|left, right| {
                singleton_placement_rank(kind, left)
                    .cmp(&singleton_placement_rank(kind, right))
                    .then_with(|| left.cmp(right))
            })
            .ok_or(CoordinatorRuntimeError::IneligibleTarget)
    }

    pub(super) async fn begin_singleton_recovery(
        &mut self,
        mut slot: PlacementSlot,
    ) -> Result<(), CoordinatorRuntimeError> {
        let PlacementSlotKey::Singleton { kind, .. } = &slot.key else {
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
        let barrier_version = self.next_version()?;
        let barrier_sessions = self
            .sessions
            .iter()
            .filter_map(|(incarnation, session)| {
                (session.hello.used_singletons.contains(kind)
                    || session.hello.singleton_eligibility.contains(kind))
                .then_some(*incarnation)
            })
            .collect::<BTreeSet<_>>();
        let expected_slot = slot.clone();
        slot.target = Some(target.clone());
        slot.state = PlacementSlotState::BeginHandoff;
        slot.active_move = Some(plan_id);
        slot.barrier_sessions = barrier_sessions.clone();
        slot.version = barrier_version.clone();
        let committed = self
            .store
            .reserve_handoff(
                &self.leader_guard,
                ReserveHandoff {
                    expected_slot,
                    slot,
                },
            )
            .await?;
        let slot = committed.slot;
        self.version = slot.version.clone();
        let mut handoff = HandoffMachine::begin(
            slot.key.clone(),
            plan_id,
            source,
            target,
            slot.assignment_generation,
            barrier_version,
            barrier_sessions,
        )
        .map_err(CoordinatorRuntimeError::Handoff)?;
        let effects = handoff.start();
        self.handoffs.insert(slot.key.clone(), handoff);
        self.publish_slot_delta(&slot).await?;
        Box::pin(self.apply_handoff_effects(slot.key, effects)).await
    }

    pub(super) async fn persist_initial_allocation(
        &mut self,
        slot: PlacementSlot,
    ) -> Result<(), CoordinatorRuntimeError> {
        let owner = slot
            .owner
            .clone()
            .ok_or(CoordinatorRuntimeError::IneligibleTarget)?;
        let (expected_global_member, expected_domain_member) =
            self.assignment_members(&owner).await?;
        let lease_id = self.store.grant_lease(self.config.claim_ttl).await?;
        let grant = ClaimGrant {
            domain: slot.key.domain().clone(),
            slot: slot.key.clone(),
            owner: owner.clone(),
            coordinator_term: self.leader.term,
            assignment_generation: slot.assignment_generation,
            grant_sequence: GrantSequence::new(1).expect("one is a valid grant sequence"),
            ttl: self.config.claim_ttl,
        };
        let request = AllocateInitial {
            expected_global_member,
            expected_domain_member,
            slot,
            claim: LeasedClaim {
                grant: grant.clone(),
                lease_id,
            },
        };
        lattice_core::failpoint::hit(Failpoint::AuthorityBeforeGuardedCommit);
        let committed = match self
            .store
            .allocate_initial(&self.leader_guard, request)
            .await
        {
            Ok(committed) => committed,
            Err(StorageError::OutcomeUnknown) => {
                self.reconciliation.focused = true;
                match self.store.get_claim(&grant.slot).await? {
                    Some(claim) if claim.lease_id == lease_id && claim.grant == grant => {
                        let slot = self
                            .store
                            .get_slot(&grant.slot)
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
        lattice_core::failpoint::hit(Failpoint::InitialAuthorityAfterCommitBeforeEffect);
        self.version = slot.version.clone();
        self.remember_claim(leased_claim.lease_id, leased_claim.grant.clone());
        self.publish_slot_delta(&slot).await?;
        self.grant_authority(&leased_claim.grant)
    }

    pub(super) async fn complete_initial_ready(
        &mut self,
        key: &PlacementSlotKey,
        owner: &NodeKey,
        generation: AssignmentGeneration,
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
        let expected_slot = slot.clone();
        slot.state = PlacementSlotState::Running;
        slot.version = self.next_version()?;
        let claim = self
            .store
            .get_claim(key)
            .await?
            .ok_or(CoordinatorRuntimeError::ClaimNotProven)?;
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
        let slot = committed.slot;
        self.version = slot.version.clone();
        self.slot_assigned_at.insert(key.clone(), self.now());
        self.publish_slot_delta(&slot).await
    }

    pub(super) async fn placement_view(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<PlacementView, CoordinatorRuntimeError> {
        let now = self.now();
        let mut reservations = BTreeMap::<NodeKey, u64>::new();
        for plan in self.plans.values().filter(|plan| &plan.domain == domain) {
            for (target, weight) in plan.target_reservations() {
                *reservations.entry(target.clone()).or_default() += weight;
            }
        }
        let nodes = self
            .sessions
            .values()
            .filter(|session| session.placement_attached())
            .map(|session| PlacementNode {
                key: session.hello.node.clone(),
                release_id: session.record.hello.release.release_id,
                ready: session.placement_up(),
                eligible_entity_types: session.hello.hosted_entity_types.clone(),
                protocols: session
                    .record
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
            .list_slots(&self.version.domain)
            .await?
            .into_iter()
            .filter_map(|slot| {
                if slot.key.domain() != domain {
                    return None;
                }
                let key = slot.key.clone();
                let PlacementSlotKey::Shard {
                    domain,
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
                        domain,
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
        for plan in self.plans.values().filter(|plan| &plan.domain == domain) {
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
            domain: domain.clone(),
            version: self.version.clone(),
            now,
            reconciled: self.reconciliation.initial_complete,
            degraded: !self.reconciliation.quarantined.is_empty(),
            nodes,
            shards,
            active_cluster_moves,
            active_entity_moves,
            active_source_moves,
            active_target_moves,
            last_automatic_move_at: self.last_automatic_move_at,
        })
    }
}
