use lattice_core::{
    actor_ref::{NodeIncarnation, PlacementDomainId},
    coordinator::CoordinatorScope,
};
use lattice_remoting::{association::AssociationKey, control::ControlDispatchError};

use super::{
    AllocationError, Association, AssociationState, Bytes, ClaimGrant, ClaimLease,
    CoordinatorLeaseStore, CoordinatorRuntimeError, HandoffEvent, HandoffPhase, Instant,
    MemberRecord, MemberRemovalReason, MemberSession, MemberStatus, MembershipStore,
    MembershipVersion, NodeKey, PlacementControlCommand, PlacementDomainHello,
    PlacementDomainLeader, PlacementDomainLeaderConfig, PlacementDomainStore, PlacementSlotKey,
    PlacementSlotState, PlacementVersion, PlanReason, RebalanceTrigger, ScopedElectionStore,
    SnapshotRecord, build_snapshot, encode_control_command,
};
use crate::{
    control::PlacementControlEventKind,
    coordinator::{CoordinatorDelta, DomainMemberRecord, DomainMemberStatus, SnapshotVersion},
    storage::domain::{
        CreateDomainMember, LeasedClaim, PutEntityConfig, PutSingletonConfig, RemoveDomainMember,
        UpdateDomainMember,
    },
    types::MonotonicTime,
};

impl<S> PlacementDomainLeader<S>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + PlacementDomainStore,
{
    pub(super) async fn handle_control(
        &mut self,
        event: PlacementControlEventKind,
    ) -> Result<(), CoordinatorRuntimeError> {
        match event {
            PlacementControlEventKind::GlobalMemberRemoved { node, reason } => {
                self.remove_global_member_participation(node, reason)
                    .await?;
            }
            PlacementControlEventKind::Reconcile { association, .. } => {
                self.reconciliation.focused = true;
                if let Some(hello) = self
                    .sessions
                    .get(&association.remote_incarnation)
                    .map(|session| session.hello.clone())
                {
                    self.send_snapshot(hello, association).await?;
                }
            }
            PlacementControlEventKind::Command(inbound) => {
                let remote = inbound.association.remote_incarnation;
                let expected_scope = CoordinatorScope::Placement(self.version.domain.clone());
                if inbound.scope != expected_scope {
                    return Err(CoordinatorRuntimeError::UnauthorizedCommand);
                }
                match inbound.command {
                    PlacementControlCommand::MemberHello(_) => {
                        return Err(CoordinatorRuntimeError::UnauthorizedCommand);
                    }
                    PlacementControlCommand::PlacementDomainHello(hello) => {
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
                    PlacementControlCommand::JoinReady { snapshot_version } => {
                        self.mark_member_up(remote, snapshot_version, &inbound.association)
                            .await?;
                    }
                    PlacementControlCommand::AppliedRevision(version) => {
                        let session = self
                            .sessions
                            .get_mut(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
                        if session
                            .applied_version
                            .as_ref()
                            .is_none_or(|current| &version > current)
                        {
                            session.applied_version = Some(version.clone());
                        }
                        let barriers = self
                            .handoffs
                            .iter()
                            .filter_map(|(key, handoff)| {
                                (handoff.phase == HandoffPhase::Invalidating
                                    && handoff.required_sessions().contains(&remote)
                                    && version.satisfies(&handoff.barrier_version()))
                                .then_some(key.clone())
                            })
                            .collect::<Vec<_>>();
                        for key in barriers {
                            self.transition_handoff(
                                key,
                                HandoffEvent::AppliedRevision {
                                    session: remote,
                                    version: version.clone(),
                                },
                            )
                            .await?;
                        }
                        let ready_member = self.sessions.get(&remote).and_then(|session| {
                            (session.placement_up()
                                && session
                                    .applied_version
                                    .as_ref()
                                    .is_some_and(|applied| applied.satisfies(&self.version)))
                            .then(|| session.hello.clone())
                        });
                        if let Some(hello) = ready_member {
                            self.reconcile_claims_for(&hello).await?;
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
                        let association = self.persist_domain_hello(remote, hello.clone()).await?;
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
                        let association = self.persist_domain_hello(remote, hello.clone()).await?;
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
                        domain,
                        entity_type,
                        shard_id,
                        ..
                    } => {
                        if domain != self.version.domain {
                            return Err(CoordinatorRuntimeError::UnauthorizedCommand);
                        }
                        let session = self
                            .sessions
                            .get(&remote)
                            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
                        if !session.hello.subscribes_to(&entity_type) {
                            return Err(CoordinatorRuntimeError::UnauthorizedCommand);
                        }
                        let hello = session.hello.clone();
                        let association = session.association.clone();
                        match self.ensure_shard_allocated(entity_type, shard_id).await {
                            Ok(()) => self.send_snapshot(hello, association).await?,
                            Err(CoordinatorRuntimeError::Allocation(
                                AllocationError::NoEligibleNode,
                            ))
                            | Err(CoordinatorRuntimeError::IneligibleTarget) => {}
                            Err(error) => return Err(error),
                        }
                    }
                    PlacementControlCommand::ResolveSingleton { domain, kind, .. } => {
                        if domain != self.version.domain {
                            return Err(CoordinatorRuntimeError::UnauthorizedCommand);
                        }
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
                        match self.ensure_singleton_allocated(kind).await {
                            Ok(()) => self.send_snapshot(hello, association).await?,
                            Err(CoordinatorRuntimeError::IneligibleTarget) => {}
                            Err(error) => return Err(error),
                        }
                    }
                    PlacementControlCommand::SnapshotBegin(_)
                    | PlacementControlCommand::SnapshotChunk(_)
                    | PlacementControlCommand::SnapshotEnd(_)
                    | PlacementControlCommand::StateDelta(_)
                    | PlacementControlCommand::ClaimGranted(_)
                    | PlacementControlCommand::MemberUp(_)
                    | PlacementControlCommand::MemberDelta(_)
                    | PlacementControlCommand::DrainReady { .. }
                    | PlacementControlCommand::MembershipDrainComplete { .. }
                    | PlacementControlCommand::ForceRemove { .. }
                    | PlacementControlCommand::DrainSlot { .. } => {
                        return Err(CoordinatorRuntimeError::UnauthorizedCommand);
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn now(&self) -> MonotonicTime {
        MonotonicTime::from_millis(
            u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX),
        )
    }

    pub(super) async fn register(
        &mut self,
        hello: PlacementDomainHello,
        association_key: AssociationKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        hello
            .validate(&self.config.session_limits)
            .map_err(CoordinatorRuntimeError::Coordinator)?;
        if hello.domain != self.version.domain
            || hello.node.incarnation != association_key.remote_incarnation
            || hello.node.address != association_key.remote_address
            || self.sessions.len() == self.config.maximum_sessions
                && !self.sessions.contains_key(&hello.node.incarnation)
        {
            return Err(CoordinatorRuntimeError::UnauthorizedCommand);
        }
        let record = self
            .store
            .get_member(&hello.node.node_id)
            .await?
            .filter(|record| record.node == hello.node && record.status == MemberStatus::Up)
            .ok_or(CoordinatorRuntimeError::StaleMember)?;
        if let Some(session) = self.sessions.get_mut(&hello.node.incarnation) {
            if session.record != record
                || session.hello != hello
                || session.association != association_key
            {
                return Err(CoordinatorRuntimeError::UnauthorizedCommand);
            }
            session.last_heartbeat = Instant::now();
            session.snapshot_version = Some(self.membership_version);
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
                    &self.version.domain,
                    PlacementControlCommand::MemberUp(record),
                    &self.config,
                )?;
            }
            return Ok(());
        }
        self.persist_hello_configs(&hello).await?;
        let record = self
            .store
            .get_member(&hello.node.node_id)
            .await?
            .filter(|current| current == &record)
            .ok_or(CoordinatorRuntimeError::StaleMember)?;
        if record.version > self.membership_version {
            self.membership_version = record.version;
        }
        let joined_at = self.now();
        self.sessions.insert(
            hello.node.incarnation,
            MemberSession {
                hello: hello.clone(),
                record: record.clone(),
                domain_record: None,
                association: association_key.clone(),
                lease_id: record.lease_id,
                heartbeat_sequence: 0,
                last_heartbeat: Instant::now(),
                applied_version: None,
                snapshot_version: Some(self.membership_version),
                draining: record.status == MemberStatus::Leaving,
                drain_operation: None,
                drain_ready: false,
                joined_at,
            },
        );
        // Domain participation is durable before the placement snapshot is cut.
        // Otherwise a failover can advance the domain revision after the member
        // applies its snapshot, leaving authority replay permanently gated on a
        // revision the member was never sent.
        self.ensure_domain_member_up(hello.node.incarnation).await?;
        // Persisting the new domain member advances the shared placement
        // revision. Existing sessions must observe that revision before any
        // later slot delta, otherwise their strict reducer sees a gap and
        // tears down the Coordinator association.
        self.synchronize_sessions().await?;
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
                &self.version.domain,
                PlacementControlCommand::MemberUp(record),
                &self.config,
            )?;
        }
        Ok(())
    }

    async fn synchronize_sessions(&mut self) -> Result<(), CoordinatorRuntimeError> {
        let version = self.version.clone();
        let sessions = self
            .sessions
            .values()
            .filter(|session| session.placement_up())
            .map(|session| {
                (
                    session.hello.clone(),
                    session.association.clone(),
                    session
                        .applied_version
                        .as_ref()
                        .is_some_and(|current| current.accepts_delta_after(&version)),
                )
            })
            .collect::<Vec<_>>();
        for (hello, association_key, accepts_delta) in sessions {
            if accepts_delta {
                let association = self
                    .associations
                    .get(&association_key)
                    .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
                send_control(
                    &association,
                    &self.version.domain,
                    PlacementControlCommand::StateDelta(CoordinatorDelta {
                        version: version.clone(),
                        records: Vec::new(),
                    }),
                    &self.config,
                )?;
            } else {
                self.send_snapshot(hello, association_key).await?;
            }
        }
        Ok(())
    }

    async fn persist_hello_configs(
        &mut self,
        hello: &PlacementDomainHello,
    ) -> Result<(), CoordinatorRuntimeError> {
        for config in &hello.entity_configs {
            if self.entity_configs.len() == self.config.maximum_entity_configs
                && !self.entity_configs.contains_key(&config.entity_type)
            {
                return Err(CoordinatorRuntimeError::ConfigurationCapacity);
            }
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
            if !self.entity_configs.contains_key(&config.entity_type) {
                let committed = self
                    .store
                    .put_entity_config(
                        &self.leader_guard,
                        PutEntityConfig {
                            expected: None,
                            config: config.clone(),
                        },
                    )
                    .await?;
                self.version = committed.version;
            }
            self.entity_configs
                .insert(config.entity_type.clone(), config.clone());
        }
        for config in &hello.singleton_configs {
            if self.singleton_configs.len() == self.config.maximum_singleton_configs
                && !self.singleton_configs.contains_key(&config.kind)
            {
                return Err(CoordinatorRuntimeError::ConfigurationCapacity);
            }
            if self
                .singleton_configs
                .get(&config.kind)
                .is_some_and(|current| current != config)
            {
                return Err(CoordinatorRuntimeError::ConfigurationConflict);
            }
            if !self.singleton_configs.contains_key(&config.kind) {
                let committed = self
                    .store
                    .put_singleton_config(
                        &self.leader_guard,
                        PutSingletonConfig {
                            expected: None,
                            config: config.clone(),
                        },
                    )
                    .await?;
                self.version = committed.version;
            }
            self.singleton_configs
                .insert(config.kind.clone(), config.clone());
        }
        Ok(())
    }

    pub(super) fn next_version(&self) -> Result<PlacementVersion, CoordinatorRuntimeError> {
        self.version
            .next_revision()
            .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)
    }

    pub(super) async fn persist_domain_hello(
        &mut self,
        incarnation: NodeIncarnation,
        hello: PlacementDomainHello,
    ) -> Result<AssociationKey, CoordinatorRuntimeError> {
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
        self.persist_hello_configs(&hello).await?;
        let session = self
            .sessions
            .get(&incarnation)
            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
        let association = session.association.clone();
        let global_member = session.record.clone();
        let expected = session
            .domain_record
            .clone()
            .ok_or(CoordinatorRuntimeError::StaleMember)?;
        let mut member = expected.clone();
        member.hello = hello.clone();
        member.version = self.next_version()?;
        let member = self
            .store
            .update_domain_member(
                &self.leader_guard,
                UpdateDomainMember {
                    expected_global_member: global_member,
                    expected,
                    member,
                },
            )
            .await?
            .member;
        self.version = member.version.clone();
        let session = self
            .sessions
            .get_mut(&incarnation)
            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
        session.hello = hello;
        session.domain_record = Some(member);
        Ok(association)
    }

    pub(super) async fn mark_member_up(
        &mut self,
        incarnation: NodeIncarnation,
        snapshot_version: MembershipVersion,
        association_key: &AssociationKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        let session = self
            .sessions
            .get(&incarnation)
            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
        if &session.association != association_key || session.record.node.incarnation != incarnation
        {
            return Err(CoordinatorRuntimeError::StaleMember);
        }
        if session.snapshot_version != Some(snapshot_version) {
            if session
                .snapshot_version
                .is_some_and(|current| snapshot_version < current)
            {
                return Ok(());
            }
            return Err(CoordinatorRuntimeError::StaleMember);
        }
        if session.record.status != MemberStatus::Up {
            return Err(CoordinatorRuntimeError::StaleMember);
        }
        let hello = session.hello.clone();
        let member = session.record.clone();
        self.ensure_domain_member_up(incarnation).await?;
        let association = self
            .associations
            .get(association_key)
            .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
        send_control(
            &association,
            &self.version.domain,
            PlacementControlCommand::MemberUp(member),
            &self.config,
        )?;
        let placement_ready = self
            .sessions
            .get(&incarnation)
            .and_then(|session| session.applied_version.as_ref())
            .is_some_and(|applied| applied.satisfies(&self.version));
        if placement_ready {
            self.reconcile_claims_for(&hello).await?;
        }
        self.resume_handoffs_for(&hello.node).await
    }

    async fn ensure_domain_member_up(
        &mut self,
        incarnation: NodeIncarnation,
    ) -> Result<(), CoordinatorRuntimeError> {
        let session = self
            .sessions
            .get(&incarnation)
            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
        if session.record.status != MemberStatus::Up {
            return Err(CoordinatorRuntimeError::StaleMember);
        }
        let global_member = session.record.clone();
        let hello = session.hello.clone();
        let existing = self
            .store
            .get_domain_member(&hello.domain, &hello.node.node_id)
            .await?;
        let member = match existing {
            Some(current)
                if current.node == hello.node
                    && current.hello == hello
                    && current.status == DomainMemberStatus::Up
                    && current.version.term == self.version.term =>
            {
                current
            }
            Some(expected) => {
                if expected.node != hello.node {
                    let predecessor = expected.node.clone();
                    self.store
                        .remove_domain_member(&self.leader_guard, RemoveDomainMember { expected })
                        .await?;
                    self.version = PlacementVersion::new(
                        self.version.domain.clone(),
                        self.version.term,
                        self.store
                            .get_placement_revision(&self.version.domain)
                            .await?,
                    );
                    self.finish_node_removal(predecessor).await?;
                    let member = DomainMemberRecord {
                        node: hello.node.clone(),
                        hello,
                        status: DomainMemberStatus::Up,
                        version: self.next_version()?,
                    };
                    self.store
                        .create_domain_member(
                            &self.leader_guard,
                            CreateDomainMember {
                                expected_global_member: global_member,
                                member,
                            },
                        )
                        .await?
                        .member
                } else {
                    let member = DomainMemberRecord {
                        node: hello.node.clone(),
                        hello,
                        status: DomainMemberStatus::Up,
                        version: self.next_version()?,
                    };
                    self.store
                        .update_domain_member(
                            &self.leader_guard,
                            UpdateDomainMember {
                                expected_global_member: global_member,
                                expected,
                                member,
                            },
                        )
                        .await?
                        .member
                }
            }
            None => {
                let member = DomainMemberRecord {
                    node: hello.node.clone(),
                    hello,
                    status: DomainMemberStatus::Up,
                    version: self.next_version()?,
                };
                self.store
                    .create_domain_member(
                        &self.leader_guard,
                        CreateDomainMember {
                            expected_global_member: global_member,
                            member,
                        },
                    )
                    .await?
                    .member
            }
        };
        if member.version > self.version {
            self.version = member.version.clone();
        }
        self.sessions
            .get_mut(&incarnation)
            .ok_or(CoordinatorRuntimeError::UnknownSession)?
            .domain_record = Some(member);
        Ok(())
    }
}

include!("membership_domain_ops.rs");

pub(super) fn control_dispatch_error(error: &CoordinatorRuntimeError) -> ControlDispatchError {
    match error {
        CoordinatorRuntimeError::UnauthorizedCommand
        | CoordinatorRuntimeError::UnknownSession
        | CoordinatorRuntimeError::Codec
        | CoordinatorRuntimeError::Coordinator(_)
        | CoordinatorRuntimeError::Control(_)
        | CoordinatorRuntimeError::ClaimSequence => ControlDispatchError::InvalidCommand,
        _ => ControlDispatchError::Unavailable,
    }
}

pub(super) fn send_control(
    association: &Association,
    domain: &PlacementDomainId,
    command: PlacementControlCommand,
    config: &PlacementDomainLeaderConfig,
) -> Result<(), CoordinatorRuntimeError> {
    if association.state() == AssociationState::Closed {
        return Err(CoordinatorRuntimeError::AssociationUnavailable);
    }
    let payload = encode_control_command(
        &CoordinatorScope::Placement(domain.clone()),
        &command,
        config.maximum_control_payload,
    )
    .map_err(CoordinatorRuntimeError::Control)?;
    association.admit_control_command(payload)?;
    Ok(())
}

pub(super) fn slot_record_key(key: &PlacementSlotKey) -> String {
    match key {
        PlacementSlotKey::Shard {
            domain,
            entity_type,
            shard_id,
        } => format!(
            "domain/{}/shard/{}/{}",
            domain.as_str(),
            entity_type.as_str(),
            shard_id.get()
        ),
        PlacementSlotKey::Singleton { domain, kind } => {
            format!("domain/{}/singleton/{}", domain.as_str(), kind.as_str())
        }
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
