use super::{
    Association, AssociationState, Bytes, ClaimGrant, ClaimLease, CoordinatorLeader,
    CoordinatorLeaderConfig, CoordinatorRuntimeError, CoordinatorStore, GrantSequence,
    HandoffEvent, HandoffPhase, Instant, MemberChange, MemberEvent, MemberRecord,
    MemberRemovalReason, MemberSession, MemberStatus, NodeHello, NodeKey, PlacementControlCommand,
    PlacementSlotKey, PlacementSlotState, PlanReason, RebalanceTrigger, SnapshotRecord,
    build_snapshot, encode_control_command,
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
                    PlacementControlCommand::JoinReady { snapshot_revision } => {
                        self.mark_member_up(remote, snapshot_revision, &inbound.association)
                            .await?;
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
                        let mut hello = self
                            .sessions
                            .get(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?
                            .hello
                            .clone();
                        hello.proxied_entity_types.insert(entity_type);
                        let association = self.persist_member_hello(remote, hello.clone()).await?;
                        self.send_snapshot(hello, association).await?;
                    }
                    PlacementControlCommand::SubscribeSingleton(kind) => {
                        let mut hello = self
                            .sessions
                            .get(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?
                            .hello
                            .clone();
                        hello.used_singletons.insert(kind);
                        let association = self.persist_member_hello(remote, hello.clone()).await?;
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
                    PlacementControlCommand::BeginDrain {
                        operation_id,
                        expected_incarnation,
                    } => {
                        self.begin_member_drain(remote, operation_id, expected_incarnation)
                            .await?;
                    }
                    PlacementControlCommand::DrainComplete {
                        operation_id,
                        expected_incarnation,
                    } => {
                        self.complete_member_drain(remote, &operation_id, expected_incarnation)
                            .await?;
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
                    | PlacementControlCommand::MemberUp(_)
                    | PlacementControlCommand::MemberDelta(_)
                    | PlacementControlCommand::DrainReady { .. }
                    | PlacementControlCommand::ForceRemove { .. }
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
        if let Some(session) = self.sessions.get_mut(&hello.node.incarnation) {
            if session.hello != hello || session.association != association_key {
                return Err(CoordinatorRuntimeError::UnauthorizedCommand);
            }
            session.last_heartbeat = Instant::now();
            session.snapshot_revision = Some(self.revision);
            let status = session.record.status;
            let record = session.record.clone();
            self.send_snapshot(hello, association_key.clone()).await?;
            if status == MemberStatus::Up {
                let association = self
                    .associations
                    .get(&association_key)
                    .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
                send_control(
                    &association,
                    PlacementControlCommand::MemberUp(record),
                    &self.config,
                )?;
            }
            return Ok(());
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
        let mut existing = self.store.get_member(&hello.node.node_id).await?;
        if let Some(current) = existing.as_ref()
            && current.node.incarnation != hello.node.incarnation
        {
            let expired = self
                .sessions
                .get(&current.node.incarnation)
                .is_some_and(|session| {
                    Instant::now().duration_since(session.last_heartbeat)
                        > self.config.member_heartbeat_timeout
                });
            if !expired {
                return Err(CoordinatorRuntimeError::StaleMember);
            }
            self.remove_member(current.clone(), MemberRemovalReason::IncarnationReplaced)
                .await?;
            existing = None;
        }
        let (record, joined_at) = match existing {
            Some(record)
                if record.node.incarnation == hello.node.incarnation && record.hello == hello =>
            {
                (record, self.now())
            }
            Some(_) => return Err(CoordinatorRuntimeError::StaleMember),
            None => {
                let lease_id = self.store.grant_lease(self.config.member_lease_ttl).await?;
                let revision = self.next_revision()?;
                let record = MemberRecord {
                    node: hello.node.clone(),
                    hello: hello.clone(),
                    status: MemberStatus::Joining,
                    revision,
                    lease_id,
                };
                if let Err(error) = self.store.create_member(&record).await {
                    let _ = self.store.revoke_lease(lease_id).await;
                    return Err(error.into());
                }
                self.revision = revision;
                (record, self.now())
            }
        };
        self.sessions.insert(
            hello.node.incarnation,
            MemberSession {
                hello: hello.clone(),
                record: record.clone(),
                association: association_key.clone(),
                lease_id: record.lease_id,
                heartbeat_sequence: 0,
                last_heartbeat: Instant::now(),
                applied_revision: None,
                snapshot_revision: Some(self.revision),
                draining: record.status == MemberStatus::Leaving,
                drain_operation: None,
                drain_ready: false,
                joined_at,
            },
        );
        self.send_snapshot(hello.clone(), association_key).await?;
        if record.status == MemberStatus::Up {
            let session = self
                .sessions
                .get(&hello.node.incarnation)
                .ok_or(CoordinatorRuntimeError::UnknownSession)?;
            let association = self
                .associations
                .get(&session.association)
                .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
            send_control(
                &association,
                PlacementControlCommand::MemberUp(record),
                &self.config,
            )?;
        }
        Ok(())
    }

    pub(super) fn next_revision(&self) -> Result<crate::types::Revision, CoordinatorRuntimeError> {
        self.revision
            .next()
            .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)
    }

    pub(super) async fn persist_member_hello(
        &mut self,
        incarnation: lattice_core::actor_ref::NodeIncarnation,
        hello: NodeHello,
    ) -> Result<lattice_remoting::association::AssociationKey, CoordinatorRuntimeError> {
        hello
            .validate(&self.config.session_limits)
            .map_err(CoordinatorRuntimeError::Coordinator)?;
        let session = self
            .sessions
            .get(&incarnation)
            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
        if session.record.status != MemberStatus::Up || hello.node != session.record.node {
            return Err(CoordinatorRuntimeError::StaleMember);
        }
        if hello == session.hello {
            return Ok(session.association.clone());
        }
        let expected = session.record.clone();
        let association = session.association.clone();
        let revision = self.next_revision()?;
        let mut member = expected.clone();
        member.hello = hello.clone();
        member.revision = revision;
        self.store
            .compare_and_put_member(&expected, &member)
            .await?;
        let session = self
            .sessions
            .get_mut(&incarnation)
            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
        session.hello = hello;
        session.record = member.clone();
        self.revision = revision;
        self.publish_member_event(MemberEvent {
            revision,
            change: MemberChange::Upsert(Box::new(member)),
        })?;
        Ok(association)
    }

    pub(super) async fn mark_member_up(
        &mut self,
        incarnation: lattice_core::actor_ref::NodeIncarnation,
        snapshot_revision: crate::types::Revision,
        association_key: &lattice_remoting::association::AssociationKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        let session = self
            .sessions
            .get(&incarnation)
            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
        if &session.association != association_key
            || session.snapshot_revision != Some(snapshot_revision)
            || session.record.node.incarnation != incarnation
        {
            return Err(CoordinatorRuntimeError::StaleMember);
        }
        if session.record.status == MemberStatus::Up {
            let association = self
                .associations
                .get(association_key)
                .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
            return send_control(
                &association,
                PlacementControlCommand::MemberUp(session.record.clone()),
                &self.config,
            );
        }
        if session.record.status == MemberStatus::Leaving {
            return Ok(());
        }
        if session.record.status != MemberStatus::Joining {
            return Err(CoordinatorRuntimeError::StaleMember);
        }
        let expected = session.record.clone();
        let hello = session.hello.clone();
        let revision = self.next_revision()?;
        let mut member = expected.clone();
        member.status = MemberStatus::Up;
        member.revision = revision;
        self.store
            .compare_and_put_member(&expected, &member)
            .await?;
        let session = self
            .sessions
            .get_mut(&incarnation)
            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
        session.record = member.clone();
        session.applied_revision = Some(revision);
        self.revision = revision;
        self.publish_member_event(MemberEvent {
            revision,
            change: MemberChange::Upsert(Box::new(member.clone())),
        })?;
        let association = self
            .associations
            .get(association_key)
            .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
        send_control(
            &association,
            PlacementControlCommand::MemberUp(member),
            &self.config,
        )?;
        self.reconcile_claims_for(&hello).await?;
        self.resume_handoffs_for(&hello.node).await
    }

    pub(super) async fn begin_member_drain(
        &mut self,
        incarnation: lattice_core::actor_ref::NodeIncarnation,
        operation_id: String,
        expected_incarnation: lattice_core::actor_ref::NodeIncarnation,
    ) -> Result<(), CoordinatorRuntimeError> {
        if operation_id.is_empty()
            || operation_id.len() > 256
            || expected_incarnation != incarnation
        {
            return Err(CoordinatorRuntimeError::StaleMember);
        }
        let session = self
            .sessions
            .get(&incarnation)
            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
        if session.record.status == MemberStatus::Leaving {
            if session
                .drain_operation
                .as_ref()
                .is_some_and(|current| current != &operation_id)
            {
                return Err(CoordinatorRuntimeError::IdempotencyConflict);
            }
            let session = self
                .sessions
                .get_mut(&incarnation)
                .ok_or(CoordinatorRuntimeError::UnknownSession)?;
            session.drain_operation = Some(operation_id);
            return self.maybe_send_drain_ready(incarnation).await;
        }
        if session.record.status != MemberStatus::Up {
            return Err(CoordinatorRuntimeError::StaleMember);
        }
        let expected = session.record.clone();
        let source = session.hello.node.clone();
        let entity_types = session
            .hello
            .hosted_entity_types
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let revision = self.next_revision()?;
        let mut member = expected.clone();
        member.status = MemberStatus::Leaving;
        member.revision = revision;
        self.store
            .compare_and_put_member(&expected, &member)
            .await?;
        let session = self
            .sessions
            .get_mut(&incarnation)
            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
        session.record = member.clone();
        session.draining = true;
        session.drain_operation = Some(operation_id);
        session.drain_ready = false;
        self.revision = revision;
        self.publish_member_event(MemberEvent {
            revision,
            change: MemberChange::Upsert(Box::new(member)),
        })?;
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
        let singletons = self
            .store
            .list_slots()
            .await?
            .into_iter()
            .filter(|slot| {
                slot.owner.as_ref() == Some(&source)
                    && matches!(slot.key, PlacementSlotKey::Singleton(_))
            })
            .collect::<Vec<_>>();
        for slot in singletons {
            match self.begin_singleton_recovery(slot).await {
                Ok(()) | Err(CoordinatorRuntimeError::IneligibleTarget) => {}
                Err(error) => return Err(error),
            }
        }
        self.maybe_send_drain_ready(incarnation).await
    }

    pub(super) async fn maybe_send_drain_ready(
        &mut self,
        incarnation: lattice_core::actor_ref::NodeIncarnation,
    ) -> Result<(), CoordinatorRuntimeError> {
        let session = self
            .sessions
            .get(&incarnation)
            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
        if session.record.status != MemberStatus::Leaving || session.drain_ready {
            return Ok(());
        }
        let node = session.record.node.clone();
        if self
            .store
            .list_slots()
            .await?
            .iter()
            .any(|slot| slot.owner.as_ref() == Some(&node))
        {
            return Ok(());
        }
        let operation_id = session
            .drain_operation
            .clone()
            .ok_or(CoordinatorRuntimeError::DrainNotReady)?;
        let association_key = session.association.clone();
        let association = self
            .associations
            .get(&association_key)
            .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
        send_control(
            &association,
            PlacementControlCommand::DrainReady {
                operation_id,
                expected_incarnation: incarnation,
            },
            &self.config,
        )?;
        self.sessions
            .get_mut(&incarnation)
            .ok_or(CoordinatorRuntimeError::UnknownSession)?
            .drain_ready = true;
        Ok(())
    }

    pub(super) async fn complete_member_drain(
        &mut self,
        incarnation: lattice_core::actor_ref::NodeIncarnation,
        operation_id: &str,
        expected_incarnation: lattice_core::actor_ref::NodeIncarnation,
    ) -> Result<(), CoordinatorRuntimeError> {
        let session = self
            .sessions
            .get(&incarnation)
            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
        if expected_incarnation != incarnation
            || session.record.status != MemberStatus::Leaving
            || !session.drain_ready
            || session.drain_operation.as_deref() != Some(operation_id)
        {
            return Err(CoordinatorRuntimeError::DrainNotReady);
        }
        let member = session.record.clone();
        self.remove_member(member, MemberRemovalReason::GracefulLeave)
            .await
    }

    pub(super) async fn remove_member(
        &mut self,
        member: MemberRecord,
        reason: MemberRemovalReason,
    ) -> Result<(), CoordinatorRuntimeError> {
        let incarnation = member.node.incarnation;
        let node = member.node.clone();
        let revision = self.next_revision()?;
        self.store.compare_and_delete_member(&member).await?;
        self.store.revoke_lease(member.lease_id).await?;
        self.sessions.remove(&incarnation);
        self.revision = revision;
        self.publish_member_event(MemberEvent {
            revision,
            change: MemberChange::Removed {
                node: node.clone(),
                reason,
            },
        })?;
        let expired_claims = self
            .claims
            .iter()
            .filter_map(|(key, claim)| {
                (claim.grant.owner.incarnation == incarnation).then_some((
                    key.clone(),
                    claim.lease_id,
                    claim.grant.clone(),
                ))
            })
            .collect::<Vec<_>>();
        for (key, claim_lease, grant) in expired_claims {
            self.store.delete_claim(&grant).await?;
            self.store.revoke_lease(claim_lease).await?;
            self.claims.remove(&key);
        }
        let barriers = self
            .handoffs
            .iter()
            .filter_map(|(key, handoff)| {
                (handoff.phase == HandoffPhase::Invalidating
                    && handoff.required_sessions().contains(&incarnation))
                .then_some(key.clone())
            })
            .collect::<Vec<_>>();
        for key in barriers {
            self.transition_handoff(key, HandoffEvent::FenceSession(incarnation))
                .await?;
        }
        let owned_slots = self
            .store
            .list_slots()
            .await?
            .into_iter()
            .filter(|slot| slot.owner.as_ref() == Some(&node))
            .collect::<Vec<_>>();
        let entity_types = owned_slots
            .iter()
            .filter_map(|slot| match slot.key {
                PlacementSlotKey::Shard {
                    ref entity_type, ..
                } => Some(entity_type.clone()),
                PlacementSlotKey::Singleton(_) => None,
            })
            .collect::<std::collections::BTreeSet<_>>();
        for entity_type in entity_types {
            let _ = self
                .evaluate_rebalance(
                    entity_type,
                    RebalanceTrigger::Recovery {
                        owner: node.clone(),
                    },
                )
                .await;
        }
        for slot in owned_slots {
            if matches!(slot.key, PlacementSlotKey::Singleton(_)) {
                match self.begin_singleton_recovery(slot).await {
                    Ok(()) | Err(CoordinatorRuntimeError::IneligibleTarget) => {}
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }

    pub(super) fn publish_member_event(
        &self,
        event: MemberEvent,
    ) -> Result<(), CoordinatorRuntimeError> {
        lattice_core::failpoint::hit(
            lattice_core::failpoint::Failpoint::CoordinatorAfterEtcdCommitBeforeDelta,
        );
        for session in self.sessions.values() {
            let association = self
                .associations
                .get(&session.association)
                .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
            send_control(
                &association,
                PlacementControlCommand::MemberDelta(event.clone()),
                &self.config,
            )?;
        }
        Ok(())
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
