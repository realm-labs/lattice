use std::collections::BTreeMap;

use lattice_core::failpoint::Failpoint;

use super::{
    LocalAuthorityEvent, LogicPlacementEffect, LogicSessionError, PlacementDomainSession,
    snapshot::decode_slots,
};
use crate::{
    authority::{AuthorityEffect, AuthorityEvent, PlacementAuthority},
    control::{PlacementControlCommand, PlacementControlEventKind},
    coordinator::{CoordinatorDelta, MemberRecord, MemberStatus, SnapshotStager, SnapshotVersion},
    types::{PlacementSlot, PlacementSlotKey},
};

impl PlacementDomainSession {
    pub(super) async fn handle_local_event(
        &self,
        event: LocalAuthorityEvent,
    ) -> Result<(), LogicSessionError> {
        let effects = {
            let mut state = self.state.lock().expect("logic placement state poisoned");
            state
                .authorities
                .get_mut(&event.slot)
                .ok_or(LogicSessionError::UnknownAuthority)?
                .transition(if event.succeeded {
                    AuthorityEvent::StopSucceeded
                } else {
                    AuthorityEvent::StopFailed
                })
                .map_err(LogicSessionError::Authority)?
        };
        self.publish_effects(event.slot, effects).await
    }

    pub(super) async fn handle(
        &mut self,
        event: PlacementControlEventKind,
    ) -> Result<(), LogicSessionError> {
        match event {
            PlacementControlEventKind::GlobalMemberRemoved { .. } => {
                Err(LogicSessionError::UnauthorizedCommand)
            }
            PlacementControlEventKind::Reconcile { association, .. } => {
                self.require_coordinator(&association)?;
                self.state
                    .lock()
                    .expect("logic placement state poisoned")
                    .domain_up = false;
                self.hello_pending = true;
                self.send_hello()
            }
            PlacementControlEventKind::Command(inbound) => {
                self.require_coordinator(&inbound.association)?;
                if matches!(&inbound.command, PlacementControlCommand::SnapshotBegin(_)) {
                    self.accept_snapshot_term(inbound.coordinator_term)?;
                } else {
                    self.require_coordinator_term(inbound.coordinator_term)?;
                }
                match inbound.command {
                    PlacementControlCommand::SnapshotBegin(begin) => {
                        self.hello_pending = false;
                        self.stager = Some(
                            SnapshotStager::begin(
                                begin,
                                self.config.snapshot_limits.clone(),
                                self.now(),
                            )
                            .map_err(LogicSessionError::Coordinator)?,
                        );
                        Ok(())
                    }
                    PlacementControlCommand::SnapshotChunk(chunk) => {
                        let now = self.now();
                        self.stager
                            .as_mut()
                            .ok_or(LogicSessionError::SnapshotRequired)?
                            .push(chunk, now)
                            .map_err(LogicSessionError::Coordinator)
                    }
                    PlacementControlCommand::SnapshotEnd(end) => {
                        let install = self
                            .stager
                            .take()
                            .ok_or(LogicSessionError::SnapshotRequired)?
                            .finish(end, self.now())
                            .map_err(LogicSessionError::Coordinator)?;
                        lattice_core::failpoint::hit(Failpoint::SnapshotAfterStageBeforeInstall);
                        let version = install.version.clone();
                        match version {
                            SnapshotVersion::Membership(version) => {
                                let _ = version;
                                Err(LogicSessionError::UnauthorizedCommand)
                            }
                            SnapshotVersion::Placement(version) => {
                                let slots = decode_slots(&install.records)?;
                                self.install_snapshot_slots(slots).await?;
                                self.state
                                    .lock()
                                    .expect("logic placement state poisoned")
                                    .session
                                    .install(install)
                                    .map_err(LogicSessionError::PlacementState)?;
                                self.send(PlacementControlCommand::AppliedRevision(version))
                            }
                        }
                    }
                    PlacementControlCommand::StateDelta(delta) => self.apply_delta(delta).await,
                    PlacementControlCommand::MemberDelta(_) => {
                        Err(LogicSessionError::UnauthorizedCommand)
                    }
                    PlacementControlCommand::MemberUp(member) => self.apply_member_up(member),
                    PlacementControlCommand::ClaimGranted(grant) => {
                        let effects = {
                            let mut state =
                                self.state.lock().expect("logic placement state poisoned");
                            state
                                .authorities
                                .get_mut(&grant.slot)
                                .ok_or(LogicSessionError::UnknownAuthority)?
                                .transition(AuthorityEvent::InstallGrant {
                                    grant: grant.clone(),
                                    now: self.now(),
                                })
                                .map_err(LogicSessionError::Authority)?
                        };
                        self.publish_effects(grant.slot, effects).await
                    }
                    PlacementControlCommand::ResolutionFailed {
                        request_id,
                        slot,
                        reason,
                    } => {
                        if request_id == 0 || slot.domain() != &self.domain_hello.domain {
                            return Err(LogicSessionError::UnauthorizedCommand);
                        }
                        let subscribed = match &slot {
                            PlacementSlotKey::Shard { entity_type, .. } => {
                                self.domain_hello.subscribes_to(entity_type)
                            }
                            PlacementSlotKey::Singleton { kind, .. } => {
                                self.domain_hello.used_singletons.contains(kind)
                                    || self.domain_hello.singleton_eligibility.contains(kind)
                            }
                        };
                        if !subscribed {
                            return Err(LogicSessionError::UnauthorizedCommand);
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
                            return Err(LogicSessionError::StaleGeneration);
                        }
                        self.effects
                            .send(LogicPlacementEffect::DrainReady {
                                operation_id,
                                incarnation: expected_incarnation,
                            })
                            .await
                            .map_err(|_| LogicSessionError::EffectBackpressure)
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
                                return Err(LogicSessionError::StaleGeneration);
                            }
                            let authority = state
                                .authorities
                                .get_mut(&key)
                                .ok_or(LogicSessionError::UnknownAuthority)?;
                            if authority
                                .slot()
                                .is_none_or(|slot| slot.assignment_generation != generation)
                            {
                                return Err(LogicSessionError::StaleGeneration);
                            }
                            authority
                                .transition(AuthorityEvent::BeginDrain)
                                .map_err(LogicSessionError::Authority)?
                        };
                        self.publish_effects(key, effects).await
                    }
                    PlacementControlCommand::MemberHello(_)
                    | PlacementControlCommand::PlacementDomainHello(_)
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
                        Err(LogicSessionError::UnauthorizedCommand)
                    }
                }
            }
        }
    }

    async fn apply_delta(&self, delta: CoordinatorDelta) -> Result<(), LogicSessionError> {
        let slots = decode_slots(&delta.records)?;
        {
            let mut state = self.state.lock().expect("logic placement state poisoned");
            state
                .session
                .apply(delta.clone())
                .map_err(LogicSessionError::PlacementState)?;
        }
        self.install_slots(slots).await?;
        self.send(PlacementControlCommand::AppliedRevision(delta.version))
    }

    fn apply_member_up(&self, member: MemberRecord) -> Result<(), LogicSessionError> {
        let mut state = self.state.lock().expect("logic placement state poisoned");
        if member.status != MemberStatus::Up || member.node != state.local_node {
            return Err(LogicSessionError::StaleGeneration);
        }
        state.domain_up = true;
        state.changed.notify_waiters();
        Ok(())
    }

    async fn install_snapshot_slots(
        &self,
        slots: BTreeMap<PlacementSlotKey, PlacementSlot>,
    ) -> Result<(), LogicSessionError> {
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
    ) -> Result<(), LogicSessionError> {
        let mut all_effects = Vec::new();
        {
            let mut state = self.state.lock().expect("logic placement state poisoned");
            for (key, slot) in slots {
                state.resolution_failures.remove(&key);
                if slot.owner.as_ref() == Some(&state.local_node)
                    && !state.authorities.contains_key(&key)
                {
                    if state.authorities.len() == self.config.maximum_authorities {
                        return Err(LogicSessionError::AuthorityCapacity);
                    }
                    let local = state.local_node.clone();
                    state.authorities.insert(
                        key.clone(),
                        PlacementAuthority::new(local, self.config.claim_safety_margin)
                            .map_err(LogicSessionError::Authority)?,
                    );
                }
                if let Some(authority) = state.authorities.get_mut(&key) {
                    let effects = authority
                        .transition(AuthorityEvent::ReconcileSlot(slot.clone()))
                        .map_err(LogicSessionError::Authority)?;
                    all_effects.push((key.clone(), effects));
                }
                state.slots.insert(key, slot);
            }
        }
        for (key, effects) in all_effects {
            self.publish_effects(key, effects).await?;
        }
        self.state
            .lock()
            .expect("logic placement state poisoned")
            .changed
            .notify_waiters();
        Ok(())
    }

    pub(super) async fn tick_authorities(&self) -> Result<(), LogicSessionError> {
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
                .map_err(LogicSessionError::Authority)?
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
    ) -> Result<(), LogicSessionError> {
        for effect in effects {
            self.effects
                .send(LogicPlacementEffect::Authority {
                    slot: slot.clone(),
                    effect,
                })
                .await
                .map_err(|_| LogicSessionError::EffectBackpressure)?;
        }
        Ok(())
    }
}
