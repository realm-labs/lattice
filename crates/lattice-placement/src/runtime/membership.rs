use super::{
    Association, AssociationState, Bytes, ClaimGrant, ClaimLease, CoordinatorLeader,
    CoordinatorLeaderConfig, CoordinatorRuntimeError, CoordinatorStore, GrantSequence,
    HandoffEvent, HandoffPhase, Instant, MemberSession, NodeHello, NodeKey,
    PlacementControlCommand, PlacementSlotKey, PlacementSlotState, PlanReason, RebalanceTrigger,
    SnapshotRecord, build_snapshot, encode_control_command,
};

impl<S: CoordinatorStore> CoordinatorLeader<S> {
    pub(super) async fn handle_control(
        &mut self,
        event: crate::control::PlacementControlEventKind,
    ) -> Result<(), CoordinatorRuntimeError> {
        match event {
            crate::control::PlacementControlEventKind::Reconcile { association, .. } => {
                if let Some(session) = self.sessions.get(&association.remote_incarnation) {
                    self.send_snapshot(session.hello.clone(), association)
                        .await?;
                }
            }
            crate::control::PlacementControlEventKind::Command(inbound) => {
                let remote = inbound.association.remote_incarnation;
                match inbound.command {
                    PlacementControlCommand::NodeHello(hello) => {
                        self.register(hello, inbound.association).await?;
                    }
                    PlacementControlCommand::NodeHeartbeat {
                        incarnation,
                        sequence,
                    } => {
                        if incarnation != remote || sequence == 0 {
                            return Err(CoordinatorRuntimeError::UnauthorizedCommand);
                        }
                        let session = self
                            .sessions
                            .get_mut(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
                        if sequence > session.heartbeat_sequence {
                            session.heartbeat_sequence = sequence;
                            session.last_heartbeat = Instant::now();
                            self.store.keep_lease_alive(session.lease_id).await?;
                        }
                    }
                    PlacementControlCommand::AppliedRevision(revision) => {
                        let session = self
                            .sessions
                            .get_mut(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
                        if session
                            .applied_revision
                            .is_none_or(|current| revision > current)
                        {
                            session.applied_revision = Some(revision);
                        }
                        let barriers = self
                            .handoffs
                            .iter()
                            .filter_map(|(key, handoff)| {
                                (handoff.phase == HandoffPhase::Invalidating
                                    && handoff.required_sessions().contains(&remote)
                                    && revision >= handoff.barrier_revision())
                                .then_some(key.clone())
                            })
                            .collect::<Vec<_>>();
                        for key in barriers {
                            self.transition_handoff(
                                key,
                                HandoffEvent::AppliedRevision {
                                    session: remote,
                                    revision,
                                },
                            )
                            .await?;
                        }
                    }
                    PlacementControlCommand::NodeLoad(report) => {
                        if self
                            .sessions
                            .get(&remote)
                            .is_none_or(|session| session.hello.node != report.node)
                        {
                            return Err(CoordinatorRuntimeError::UnauthorizedCommand);
                        }
                        let received = self.now();
                        if self
                            .loads
                            .report_node(report)
                            .map_err(CoordinatorRuntimeError::Coordinator)?
                        {
                            self.node_load_received.insert(remote, received);
                        }
                    }
                    PlacementControlCommand::ShardLoad(report) => {
                        if self
                            .sessions
                            .get(&remote)
                            .is_none_or(|session| session.hello.node != report.node)
                        {
                            return Err(CoordinatorRuntimeError::UnauthorizedCommand);
                        }
                        let received = self.now();
                        let key = (remote, report.entity_type.clone(), report.shard_id);
                        if self
                            .loads
                            .report_shard(report)
                            .map_err(CoordinatorRuntimeError::Coordinator)?
                        {
                            self.shard_load_received.insert(key, received);
                        }
                    }
                    PlacementControlCommand::SubscribeEntity(entity_type) => {
                        let session = self
                            .sessions
                            .get_mut(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
                        session.hello.proxied_entity_types.insert(entity_type);
                        let hello = session.hello.clone();
                        let association = session.association.clone();
                        self.send_snapshot(hello, association).await?;
                    }
                    PlacementControlCommand::SubscribeSingleton(kind) => {
                        let session = self
                            .sessions
                            .get_mut(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
                        session.hello.used_singletons.insert(kind);
                        let hello = session.hello.clone();
                        let association = session.association.clone();
                        self.send_snapshot(hello, association).await?;
                    }
                    PlacementControlCommand::SlotDrained { slot, generation } => {
                        let source = self
                            .sessions
                            .get(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?
                            .hello
                            .node
                            .clone();
                        self.transition_handoff(
                            slot,
                            HandoffEvent::SourceDrained { source, generation },
                        )
                        .await?;
                    }
                    PlacementControlCommand::SlotStopFailed { slot, generation } => {
                        let source = self
                            .sessions
                            .get(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?
                            .hello
                            .node
                            .clone();
                        self.transition_handoff(
                            slot,
                            HandoffEvent::SourceStopFailed { source, generation },
                        )
                        .await?;
                    }
                    PlacementControlCommand::SlotReady { slot, generation } => {
                        let target = self
                            .sessions
                            .get(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?
                            .hello
                            .node
                            .clone();
                        if self.handoffs.contains_key(&slot) {
                            self.transition_handoff(
                                slot,
                                HandoffEvent::TargetReady { target, generation },
                            )
                            .await?;
                        } else {
                            self.complete_initial_ready(&slot, &target, generation)
                                .await?;
                        }
                    }
                    PlacementControlCommand::BeginDrain => {
                        let session = self
                            .sessions
                            .get_mut(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
                        session.draining = true;
                        let source = session.hello.node.clone();
                        let entity_types = session
                            .hello
                            .hosted_entity_types
                            .iter()
                            .cloned()
                            .collect::<Vec<_>>();
                        for entity_type in entity_types {
                            let _ = self
                                .evaluate_rebalance(
                                    entity_type,
                                    RebalanceTrigger::Drain {
                                        node: source.clone(),
                                    },
                                )
                                .await?;
                        }
                    }
                    PlacementControlCommand::DrainComplete => {
                        if !self.sessions.contains_key(&remote) {
                            return Err(CoordinatorRuntimeError::UnknownSession);
                        }
                    }
                    PlacementControlCommand::ResolveShard {
                        entity_type,
                        shard_id,
                        ..
                    } => {
                        let session = self
                            .sessions
                            .get(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
                        if !session.hello.subscribes_to(&entity_type) {
                            return Err(CoordinatorRuntimeError::UnauthorizedCommand);
                        }
                        let hello = session.hello.clone();
                        let association = session.association.clone();
                        self.ensure_shard_allocated(entity_type, shard_id).await?;
                        self.send_snapshot(hello, association).await?;
                    }
                    PlacementControlCommand::ResolveSingleton { kind, .. } => {
                        let session = self
                            .sessions
                            .get(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
                        if !session.hello.used_singletons.contains(&kind)
                            && !session.hello.singleton_eligibility.contains(&kind)
                        {
                            return Err(CoordinatorRuntimeError::UnauthorizedCommand);
                        }
                        let hello = session.hello.clone();
                        let association = session.association.clone();
                        self.ensure_singleton_allocated(kind).await?;
                        self.send_snapshot(hello, association).await?;
                    }
                    PlacementControlCommand::SnapshotBegin(_)
                    | PlacementControlCommand::SnapshotChunk(_)
                    | PlacementControlCommand::SnapshotEnd(_)
                    | PlacementControlCommand::StateDelta(_)
                    | PlacementControlCommand::ClaimGranted(_)
                    | PlacementControlCommand::NodeRemoved(_)
                    | PlacementControlCommand::DrainSlot { .. } => {
                        return Err(CoordinatorRuntimeError::UnauthorizedCommand);
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn now(&self) -> crate::types::MonotonicTime {
        crate::types::MonotonicTime::from_millis(
            u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX),
        )
    }

    pub(super) async fn register(
        &mut self,
        hello: NodeHello,
        association_key: lattice_remoting::association::AssociationKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        hello
            .validate(&self.config.session_limits)
            .map_err(CoordinatorRuntimeError::Coordinator)?;
        if hello.node.incarnation != association_key.remote_incarnation
            || hello.node.address != association_key.remote_address
            || self.sessions.len() == self.config.maximum_sessions
                && !self.sessions.contains_key(&hello.node.incarnation)
        {
            return Err(CoordinatorRuntimeError::UnauthorizedCommand);
        }
        for config in &hello.entity_configs {
            if self
                .entity_configs
                .get(&config.entity_type)
                .is_some_and(|current| current != config)
            {
                return Err(CoordinatorRuntimeError::ConfigurationConflict);
            }
            if !self.strategies.contains_key(&(
                config.allocation_policy_id.clone(),
                config.allocation_policy_version,
            )) {
                return Err(CoordinatorRuntimeError::UnknownStrategy);
            }
            self.entity_configs
                .insert(config.entity_type.clone(), config.clone());
        }
        for config in &hello.singleton_configs {
            if self
                .singleton_configs
                .get(&config.kind)
                .is_some_and(|current| current != config)
            {
                return Err(CoordinatorRuntimeError::ConfigurationConflict);
            }
            self.singleton_configs
                .insert(config.kind.clone(), config.clone());
        }
        let lease_id = match self.sessions.get(&hello.node.incarnation) {
            Some(session) => session.lease_id,
            None => self.store.grant_lease(self.config.member_lease_ttl).await?,
        };
        let joined_at = self
            .sessions
            .get(&hello.node.incarnation)
            .map(|session| session.joined_at)
            .unwrap_or_else(|| self.now());
        self.store.register_member(&hello, lease_id).await?;
        self.sessions.insert(
            hello.node.incarnation,
            MemberSession {
                hello: hello.clone(),
                association: association_key.clone(),
                lease_id,
                heartbeat_sequence: 0,
                last_heartbeat: Instant::now(),
                applied_revision: None,
                draining: false,
                joined_at,
            },
        );
        self.send_snapshot(hello.clone(), association_key).await?;
        self.reconcile_claims_for(&hello).await?;
        self.resume_handoffs_for(&hello.node).await
    }

    pub(super) async fn resume_handoffs_for(
        &mut self,
        node: &NodeKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        let candidates = self
            .handoffs
            .iter()
            .filter_map(|(key, handoff)| {
                ((handoff.phase == HandoffPhase::Draining
                    && (&handoff.source == node
                        || (&handoff.target == node
                            && matches!(key, PlacementSlotKey::Singleton(_))
                            && !self.sessions.contains_key(&handoff.source.incarnation))))
                    || (handoff.phase == HandoffPhase::ReplacingAuthority
                        && &handoff.target == node))
                    .then_some((key.clone(), handoff.phase))
            })
            .collect::<Vec<_>>();
        for (key, phase) in candidates {
            match phase {
                HandoffPhase::Draining => {
                    let slot = self
                        .store
                        .get_slot(&key)
                        .await?
                        .ok_or(CoordinatorRuntimeError::UnknownSlot)?;
                    if slot.state == PlacementSlotState::Stopping {
                        let handoff = self
                            .handoffs
                            .get(&key)
                            .cloned()
                            .ok_or(CoordinatorRuntimeError::UnknownHandoff)?;
                        if &handoff.source == node {
                            let session = self
                                .sessions
                                .get(&node.incarnation)
                                .ok_or(CoordinatorRuntimeError::UnknownSession)?;
                            let association = self
                                .associations
                                .get(&session.association)
                                .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
                            send_control(
                                &association,
                                PlacementControlCommand::DrainSlot {
                                    slot: key,
                                    generation: handoff.source_generation,
                                    revision: slot.revision,
                                },
                                &self.config,
                            )?;
                        } else if self.store.get_claim(&key).await?.is_none() {
                            self.transition_handoff(
                                key,
                                HandoffEvent::SourceAuthorityInvalid {
                                    source: handoff.source,
                                    generation: handoff.source_generation,
                                },
                            )
                            .await?;
                        }
                    }
                }
                HandoffPhase::ReplacingAuthority => self.replace_authority(&key).await?,
                _ => {}
            }
        }
        Ok(())
    }

    pub(super) async fn send_snapshot(
        &self,
        hello: NodeHello,
        association_key: lattice_remoting::association::AssociationKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        let mut records = Vec::new();
        for member in self.store.list_members().await? {
            records.push(SnapshotRecord {
                key: format!("member/{}", member.node.node_id),
                value: Bytes::from(
                    serde_json::to_vec(&member).map_err(|_| CoordinatorRuntimeError::Codec)?,
                ),
            });
        }
        for slot in self.store.list_slots().await? {
            let include = match &slot.key {
                PlacementSlotKey::Shard { entity_type, .. } => hello.subscribes_to(entity_type),
                PlacementSlotKey::Singleton(kind) => {
                    hello.singleton_eligibility.contains(kind)
                        || hello.used_singletons.contains(kind)
                }
            };
            if include {
                records.push(SnapshotRecord {
                    key: slot_record_key(&slot.key),
                    value: Bytes::from(
                        serde_json::to_vec(&slot).map_err(|_| CoordinatorRuntimeError::Codec)?,
                    ),
                });
            }
        }
        let (begin, chunks, end) =
            build_snapshot(self.revision, records, &self.config.snapshot_limits)
                .map_err(CoordinatorRuntimeError::Coordinator)?;
        let association = self
            .associations
            .get(&association_key)
            .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
        send_control(
            &association,
            PlacementControlCommand::SnapshotBegin(begin),
            &self.config,
        )?;
        for chunk in chunks {
            send_control(
                &association,
                PlacementControlCommand::SnapshotChunk(chunk),
                &self.config,
            )?;
        }
        send_control(
            &association,
            PlacementControlCommand::SnapshotEnd(end),
            &self.config,
        )
    }

    pub(super) async fn reconcile_claims_for(
        &mut self,
        hello: &NodeHello,
    ) -> Result<(), CoordinatorRuntimeError> {
        let association = self
            .sessions
            .get(&hello.node.incarnation)
            .and_then(|session| self.associations.get(&session.association))
            .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
        for slot in self.store.list_slots().await? {
            if slot.owner.as_ref() != Some(&hello.node)
                || !matches!(
                    slot.state,
                    crate::types::PlacementSlotState::Allocating
                        | crate::types::PlacementSlotState::Running
                )
            {
                continue;
            }
            let previous = self.store.get_claim(&slot.key).await?;
            let sequence = previous
                .as_ref()
                .filter(|claim| claim.assignment_generation == slot.assignment_generation)
                .map(|claim| claim.grant_sequence.next())
                .transpose()
                .map_err(|_| CoordinatorRuntimeError::ClaimSequence)?
                .unwrap_or(GrantSequence::new(1).expect("one is a valid sequence"));
            let grant = ClaimGrant {
                slot: slot.key.clone(),
                owner: hello.node.clone(),
                coordinator_term: self.leader.term,
                assignment_generation: slot.assignment_generation,
                grant_sequence: sequence,
                ttl: self.config.claim_ttl,
            };
            let lease_id = match self.claims.get(&slot.key) {
                Some(claim) => claim.lease_id,
                None => self.store.grant_lease(self.config.claim_ttl).await?,
            };
            self.store.put_claim(&grant, lease_id).await?;
            self.claims.insert(
                slot.key.clone(),
                ClaimLease {
                    lease_id,
                    grant: grant.clone(),
                },
            );
            send_control(
                &association,
                PlacementControlCommand::ClaimGranted(grant),
                &self.config,
            )?;
        }
        Ok(())
    }
}

pub(super) fn control_dispatch_error(
    error: &CoordinatorRuntimeError,
) -> lattice_remoting::control::ControlDispatchError {
    match error {
        CoordinatorRuntimeError::UnauthorizedCommand
        | CoordinatorRuntimeError::UnknownSession
        | CoordinatorRuntimeError::Codec
        | CoordinatorRuntimeError::Coordinator(_)
        | CoordinatorRuntimeError::Control(_)
        | CoordinatorRuntimeError::ClaimSequence => {
            lattice_remoting::control::ControlDispatchError::InvalidCommand
        }
        _ => lattice_remoting::control::ControlDispatchError::Unavailable,
    }
}

pub(super) fn send_control(
    association: &Association,
    command: PlacementControlCommand,
    config: &CoordinatorLeaderConfig,
) -> Result<(), CoordinatorRuntimeError> {
    if association.state() == AssociationState::Closed {
        return Err(CoordinatorRuntimeError::AssociationUnavailable);
    }
    let payload = encode_control_command(&command, config.maximum_control_payload)
        .map_err(CoordinatorRuntimeError::Control)?;
    association.admit_control_command(payload)?;
    Ok(())
}

pub(super) fn slot_record_key(key: &PlacementSlotKey) -> String {
    match key {
        PlacementSlotKey::Shard {
            entity_type,
            shard_id,
        } => format!("shard/{}/{}", entity_type.as_str(), shard_id.get()),
        PlacementSlotKey::Singleton(kind) => format!("singleton/{}", kind.as_str()),
    }
}

pub(super) fn plan_priority(reason: &PlanReason) -> u8 {
    match reason {
        PlanReason::Recovery => 0,
        PlanReason::Drain => 1,
        PlanReason::Manual => 2,
        PlanReason::Automatic => 3,
    }
}
