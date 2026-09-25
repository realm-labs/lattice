use std::collections::BTreeMap;
use tokio::time::Instant;

use super::{
    GroupSession, GroupSessionError, LocalAuthorityEvent, LogicPlacementEffect,
    snapshot::decode_slots,
};
use crate::{
    authority::{AuthorityEffect, AuthorityError, AuthorityEvent, PlacementAuthority},
    control::{PlacementControlCommand, PlacementControlEventKind},
    coordinator::{CoordinatorDelta, MemberRecord, MemberStatus, SnapshotStager, SnapshotVersion},
    failpoints,
    types::{PlacementSlot, PlacementSlotKey},
};

impl GroupSession {
    /// At most one request per slot is outstanding. Retries use a new unpredictable
    /// ID and start time; a late response consumes no newer request or deadline.
    pub(super) fn request_claim(&self, key: &PlacementSlotKey) -> Result<(), GroupSessionError> {
        let request = {
            let mut state = self.state.lock().expect("logic placement state poisoned");
            let now = self.now();
            let Some(authority) = state.authorities.get_mut(key) else {
                return Ok(());
            };
            if !authority.renewal_needed(now) {
                return Ok(());
            }
            let Some(slot) = authority.slot() else {
                return Ok(());
            };
            if slot.owner.as_ref() != Some(&self.group_hello.node) {
                return Ok(());
            }
            let generation = slot.assignment_generation;
            let request_id = uuid::Uuid::new_v4().as_u128();
            if authority
                .transition(AuthorityEvent::BeginRenewal { request_id, now })
                .is_err()
            {
                return Ok(());
            }
            PlacementControlCommand::RequestClaim {
                request_id,
                slot: key.clone(),
                generation,
            }
        };
        self.send_runtime_progress(request)
    }

    pub(super) async fn handle_local_event(
        &self,
        event: LocalAuthorityEvent,
    ) -> Result<(), GroupSessionError> {
        let effects = {
            let mut state = self.state.lock().expect("logic placement state poisoned");
            state
                .authorities
                .get_mut(&event.slot)
                .ok_or(GroupSessionError::UnknownAuthority)?
                .transition(if event.succeeded {
                    AuthorityEvent::StopSucceeded
                } else {
                    AuthorityEvent::StopFailed
                })
                .map_err(GroupSessionError::Authority)?
        };
        self.publish_effects(event.slot, effects).await
    }

    pub(super) async fn handle(
        &mut self,
        event: PlacementControlEventKind,
    ) -> Result<(), GroupSessionError> {
        match event {
            PlacementControlEventKind::GlobalMemberRemoved { .. } => {
                Err(GroupSessionError::UnauthorizedCommand)
            }
            PlacementControlEventKind::Reconcile { association, .. } => {
                self.require_coordinator(&association)?;
                if self.drain_confirmation.committed_operation().is_some() {
                    return Ok(());
                }
                self.state
                    .lock()
                    .expect("logic placement state poisoned")
                    .group_up = false;
                self.hello_pending = true;
                self.send_hello_wait().await
            }
            PlacementControlEventKind::Command(inbound) => {
                self.require_coordinator(&inbound.association)?;
                if matches!(&inbound.command, PlacementControlCommand::SnapshotBegin(_)) {
                    self.accept_snapshot_term(inbound.coordinator_term)?;
                } else {
                    self.require_coordinator_term(inbound.coordinator_term)?;
                }
                match inbound.command {
                    PlacementControlCommand::NodeHeartbeatAck { sequence } => {
                        if sequence > self.heartbeat_ack_sequence
                            && sequence <= self.heartbeat_sequence
                        {
                            self.heartbeat_ack_sequence = sequence;
                            self.last_heartbeat_ack = Instant::now();
                        }
                        Ok(())
                    }
                    PlacementControlCommand::ClusterRunning { .. }
                    | PlacementControlCommand::NodeStopCompleted { .. }
                    | PlacementControlCommand::NodeStopConfirmed { .. }
                    | PlacementControlCommand::RequestClusterShutdown { .. }
                    | PlacementControlCommand::ClusterShutdownResult { .. }
                    | PlacementControlCommand::ChangeCandidate(_)
                    | PlacementControlCommand::CandidateChanged { .. }
                    | PlacementControlCommand::ClusterClosing { .. }
                    | PlacementControlCommand::ClusterStopReport { .. }
                    | PlacementControlCommand::ClusterClosed { .. }
                    | PlacementControlCommand::RequestClaim { .. } => {
                        Err(GroupSessionError::UnauthorizedCommand)
                    }
                    PlacementControlCommand::SnapshotBegin(begin) => {
                        self.hello_pending = false;
                        self.stager = Some(
                            SnapshotStager::begin(
                                begin,
                                self.config.snapshot_limits.clone(),
                                self.now(),
                            )
                            .map_err(GroupSessionError::Coordinator)?,
                        );
                        Ok(())
                    }
                    PlacementControlCommand::SnapshotChunk(chunk) => {
                        let now = self.now();
                        self.stager
                            .as_mut()
                            .ok_or(GroupSessionError::SnapshotRequired)?
                            .push(chunk, now)
                            .map_err(GroupSessionError::Coordinator)
                    }
                    PlacementControlCommand::SnapshotEnd(end) => {
                        let install = self
                            .stager
                            .take()
                            .ok_or(GroupSessionError::SnapshotRequired)?
                            .finish(end, self.now())
                            .map_err(GroupSessionError::Coordinator)?;
                        lattice_failpoint::hit(failpoints::SNAPSHOT_AFTER_STAGE_BEFORE_INSTALL);
                        let version = install.version.clone();
                        match version {
                            SnapshotVersion::Membership(version) => {
                                let _ = version;
                                Err(GroupSessionError::UnauthorizedCommand)
                            }
                            SnapshotVersion::Placement(version) => {
                                let slots = decode_slots(&install.records)?;
                                self.install_snapshot_slots(slots).await?;
                                self.state
                                    .lock()
                                    .expect("logic placement state poisoned")
                                    .session
                                    .install(install)
                                    .map_err(GroupSessionError::PlacementState)?;
                                self.send_runtime_progress(
                                    PlacementControlCommand::AppliedRevision(version),
                                )
                            }
                        }
                    }
                    PlacementControlCommand::StateDelta(delta) => self.apply_delta(delta).await,
                    PlacementControlCommand::MemberDelta(_) => {
                        Err(GroupSessionError::UnauthorizedCommand)
                    }
                    PlacementControlCommand::MemberUp(member) => self.apply_member_up(member),
                    PlacementControlCommand::ClaimGranted(grant) => {
                        if grant.coordinator_term.get() != self.coordinator_term {
                            return Err(GroupSessionError::StaleGeneration);
                        }
                        if grant.request_id == 0 {
                            return self.request_claim(&grant.slot);
                        }
                        let effects = {
                            let mut state =
                                self.state.lock().expect("logic placement state poisoned");
                            match state
                                .authorities
                                .get_mut(&grant.slot)
                                .ok_or(GroupSessionError::UnknownAuthority)?
                                .transition(AuthorityEvent::InstallGrant {
                                    grant: grant.clone(),
                                    now: self.now(),
                                }) {
                                Ok(effects) => effects,
                                Err(
                                    AuthorityError::StaleGrant
                                    | AuthorityError::UncorrelatedGrant
                                    | AuthorityError::Retired
                                    | AuthorityError::InvalidClaimDeadline,
                                ) => Vec::new(),
                                Err(error) => return Err(GroupSessionError::Authority(error)),
                            }
                        };
                        self.publish_effects(grant.slot, effects).await
                    }
                    PlacementControlCommand::ResolutionFailed {
                        request_id,
                        slot,
                        reason,
                    } => {
                        if request_id == 0 || slot.group() != &self.group_hello.group {
                            return Err(GroupSessionError::UnauthorizedCommand);
                        }
                        let subscribed = match &slot {
                            PlacementSlotKey::Shard { entity_type, .. } => {
                                self.group_hello.subscribes_to(entity_type)
                            }
                            PlacementSlotKey::Singleton { kind, .. } => {
                                self.group_hello.used_singletons.contains(kind)
                                    || self.group_hello.singleton_eligibility.contains(kind)
                            }
                        };
                        if !subscribed {
                            return Err(GroupSessionError::UnauthorizedCommand);
                        }
                        let mut state = self.state.lock().expect("logic placement state poisoned");
                        state.resolution_failures.insert(slot, (request_id, reason));
                        state.changed.notify_waiters();
                        Ok(())
                    }
                    PlacementControlCommand::DrainReady {
                        operation_id,
                        expected_incarnation,
                    } => {
                        let local = self
                            .state
                            .lock()
                            .expect("logic placement state poisoned")
                            .local_node
                            .incarnation;
                        if expected_incarnation != local {
                            return Err(GroupSessionError::StaleGeneration);
                        }
                        self.effects
                            .send(LogicPlacementEffect::DrainReady {
                                operation_id,
                                incarnation: expected_incarnation,
                            })
                            .await
                            .map_err(|_| GroupSessionError::EffectBackpressure)
                    }
                    PlacementControlCommand::DrainCommitted {
                        operation_id,
                        expected_incarnation,
                    } => {
                        if expected_incarnation != self.group_hello.node.incarnation {
                            return Err(GroupSessionError::UnauthorizedCommand);
                        }
                        self.drain_confirmation
                            .confirm(&operation_id, self.coordinator_term)?;
                        self.effects
                            .send(LogicPlacementEffect::DrainCommitted {
                                operation_id,
                                incarnation: expected_incarnation,
                            })
                            .await
                            .map_err(|_| GroupSessionError::EffectBackpressure)
                    }
                    PlacementControlCommand::DrainSlot {
                        slot: key,
                        generation,
                        version,
                    } => {
                        let effects = {
                            let mut state =
                                self.state.lock().expect("logic placement state poisoned");
                            if state
                                .session
                                .version()
                                .is_none_or(|current| !current.satisfies(&version))
                            {
                                return Err(GroupSessionError::StaleGeneration);
                            }
                            let authority = state
                                .authorities
                                .get_mut(&key)
                                .ok_or(GroupSessionError::UnknownAuthority)?;
                            if authority
                                .slot()
                                .is_none_or(|slot| slot.assignment_generation != generation)
                            {
                                return Err(GroupSessionError::StaleGeneration);
                            }
                            authority
                                .transition(AuthorityEvent::BeginDrain)
                                .map_err(GroupSessionError::Authority)?
                        };
                        self.publish_effects(key, effects).await
                    }
                    PlacementControlCommand::MemberHello(_)
                    | PlacementControlCommand::ActorGroupHello(_)
                    | PlacementControlCommand::JoinReady { .. }
                    | PlacementControlCommand::NodeHeartbeat { .. }
                    | PlacementControlCommand::SubscribeEntity(_)
                    | PlacementControlCommand::SubscribeSingleton(_)
                    | PlacementControlCommand::AppliedRevision(_)
                    | PlacementControlCommand::NodeLoad(_)
                    | PlacementControlCommand::ShardLoad(_)
                    | PlacementControlCommand::ResolveShard { .. }
                    | PlacementControlCommand::ResolveSingleton { .. }
                    | PlacementControlCommand::SlotDrained { .. }
                    | PlacementControlCommand::SlotStopFailed { .. }
                    | PlacementControlCommand::SlotReady { .. }
                    | PlacementControlCommand::BeginDrain { .. }
                    | PlacementControlCommand::DrainComplete { .. }
                    | PlacementControlCommand::MembershipDrainComplete { .. }
                    | PlacementControlCommand::ForceRemove { .. } => {
                        Err(GroupSessionError::UnauthorizedCommand)
                    }
                }
            }
        }
    }

    async fn apply_delta(&self, delta: CoordinatorDelta) -> Result<(), GroupSessionError> {
        let slots = decode_slots(&delta.records)?;
        {
            let mut state = self.state.lock().expect("logic placement state poisoned");
            state
                .session
                .apply(delta.clone())
                .map_err(GroupSessionError::PlacementState)?;
        }
        self.install_slots(slots).await?;
        self.send_runtime_progress(PlacementControlCommand::AppliedRevision(delta.version))
    }

    fn apply_member_up(&self, member: MemberRecord) -> Result<(), GroupSessionError> {
        let mut state = self.state.lock().expect("logic placement state poisoned");
        if member.status != MemberStatus::Up || member.node != state.local_node {
            return Err(GroupSessionError::StaleGeneration);
        }
        state.group_up = true;
        state.changed.notify_waiters();
        Ok(())
    }

    async fn install_snapshot_slots(
        &self,
        slots: BTreeMap<PlacementSlotKey, PlacementSlot>,
    ) -> Result<(), GroupSessionError> {
        let existing = self
            .state
            .lock()
            .expect("logic placement state poisoned")
            .slots
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        self.install_slots(slots.clone()).await?;
        let changed = {
            let mut state = self.state.lock().expect("logic placement state poisoned");
            for key in existing {
                if !slots.contains_key(&key) {
                    state.slots.remove(&key);
                }
            }
            state.changed.clone()
        };
        changed.notify_waiters();
        Ok(())
    }

    async fn install_slots(
        &self,
        slots: BTreeMap<PlacementSlotKey, PlacementSlot>,
    ) -> Result<(), GroupSessionError> {
        let mut all_effects = Vec::new();
        {
            let mut state = self.state.lock().expect("logic placement state poisoned");
            for (key, slot) in slots {
                state.resolution_failures.remove(&key);
                if slot.owner.as_ref() == Some(&state.local_node)
                    && !state.authorities.contains_key(&key)
                {
                    if state.authorities.len() == self.config.maximum_authorities {
                        return Err(GroupSessionError::AuthorityCapacity);
                    }
                    let local = state.local_node.clone();
                    state.authorities.insert(
                        key.clone(),
                        PlacementAuthority::new(local, self.config.claim_safety_margin)
                            .map_err(GroupSessionError::Authority)?,
                    );
                }
                if let Some(authority) = state.authorities.get_mut(&key) {
                    let effects = authority
                        .transition(AuthorityEvent::ReconcileSlot(slot.clone()))
                        .map_err(GroupSessionError::Authority)?;
                    all_effects.push((key.clone(), effects));
                }
                state.slots.insert(key, slot);
            }
        }
        for (key, effects) in all_effects {
            self.publish_effects(key.clone(), effects).await?;
            self.request_claim(&key)?;
        }
        self.state
            .lock()
            .expect("logic placement state poisoned")
            .changed
            .notify_waiters();
        Ok(())
    }

    pub(super) async fn tick_authorities(&self) -> Result<(), GroupSessionError> {
        let now = self.now();
        let effects = {
            let mut state = self.state.lock().expect("logic placement state poisoned");
            state
                .authorities
                .iter_mut()
                .map(|(key, authority)| {
                    authority
                        .transition(AuthorityEvent::Tick { now })
                        .map(|effects| (key.clone(), effects))
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(GroupSessionError::Authority)?
        };
        for (key, effects) in effects {
            self.publish_effects(key, effects).await?;
        }
        Ok(())
    }

    pub(super) async fn publish_effects(
        &self,
        slot: PlacementSlotKey,
        effects: Vec<AuthorityEffect>,
    ) -> Result<(), GroupSessionError> {
        for effect in effects {
            self.effects
                .send(LogicPlacementEffect::Authority {
                    slot: slot.clone(),
                    effect,
                })
                .await
                .map_err(|_| GroupSessionError::EffectBackpressure)?;
        }
        Ok(())
    }
}
