use super::membership::send_control;
use super::{
    AllocationRequest, BTreeMap, ClaimGrant, ClaimLease, CoordinatorLeader,
    CoordinatorRuntimeError, CoordinatorStore, GrantSequence, HandoffMachine, LoadSample,
    MoveProgress, NodeKey, PlacedShard, PlacementControlCommand, PlacementNode, PlacementSlot,
    PlacementSlotKey, PlacementSlotState, PlacementView, SingletonConfig,
};

impl<S: CoordinatorStore> CoordinatorLeader<S> {
    pub(super) async fn ensure_shard_allocated(
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

    pub(super) async fn ensure_singleton_allocated(
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

    pub(super) fn select_singleton_target(
        &self,
        kind: &lattice_core::actor_ref::SingletonKind,
        config: &SingletonConfig,
        exclude: Option<&NodeKey>,
    ) -> Result<NodeKey, CoordinatorRuntimeError> {
        self.sessions
            .values()
            .filter(|session| {
                session.record.status == crate::coordinator::MemberStatus::Up
                    && !session.draining
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

    pub(super) async fn begin_singleton_recovery(
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

    pub(super) async fn persist_initial_allocation(
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

    pub(super) async fn complete_initial_ready(
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

    pub(super) async fn placement_view(&self) -> Result<PlacementView, CoordinatorRuntimeError> {
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
            .filter(|session| session.record.status == crate::coordinator::MemberStatus::Up)
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
}
