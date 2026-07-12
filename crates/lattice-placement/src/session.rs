use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lattice_remoting::{AssociationManager, AssociationState, ControlDispatchError};
use thiserror::Error;
use tokio::sync::{Notify, mpsc, watch};
use tokio::time::Instant;

use crate::authority::{AuthorityEffect, AuthorityEvent, PlacementAuthority};
use crate::control::{
    DEFAULT_MAX_CONTROL_PAYLOAD, PlacementControlCommand, PlacementControlEvent,
    PlacementControlEventKind, encode_control_command,
};
use crate::coordinator::{
    CoordinatorDelta, CoordinatorSession, NodeHello, SnapshotLimits, SnapshotStager,
};
use crate::types::{MonotonicTime, NodeKey, PlacementSlot, PlacementSlotKey};

#[derive(Debug, Clone)]
pub struct LogicCoordinatorConfig {
    pub snapshot_limits: SnapshotLimits,
    pub maximum_control_payload: usize,
    pub tick_interval: Duration,
    pub heartbeat_interval: Duration,
    pub maximum_authorities: usize,
    pub claim_safety_margin: Duration,
}

impl Default for LogicCoordinatorConfig {
    fn default() -> Self {
        Self {
            snapshot_limits: SnapshotLimits {
                maximum_chunk_bytes: 192 * 1024,
                ..SnapshotLimits::default()
            },
            maximum_control_payload: DEFAULT_MAX_CONTROL_PAYLOAD,
            tick_interval: Duration::from_millis(100),
            heartbeat_interval: Duration::from_secs(5),
            maximum_authorities: 65_536,
            claim_safety_margin: Duration::from_secs(2),
        }
    }
}

impl LogicCoordinatorConfig {
    fn validate(&self) -> Result<(), LogicSessionError> {
        if self.maximum_control_payload == 0
            || self.tick_interval.is_zero()
            || self.heartbeat_interval.is_zero()
            || self.maximum_authorities == 0
            || self.claim_safety_margin.is_zero()
        {
            return Err(LogicSessionError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicPlacementEffect {
    Authority {
        slot: PlacementSlotKey,
        effect: AuthorityEffect,
    },
    NodeDown(lattice_core::actor_ref::NodeIncarnation),
}

pub struct LogicPlacementState {
    local_node: NodeKey,
    session: CoordinatorSession,
    slots: BTreeMap<PlacementSlotKey, PlacementSlot>,
    authorities: BTreeMap<PlacementSlotKey, PlacementAuthority>,
    changed: Arc<Notify>,
}

impl LogicPlacementState {
    pub fn slot(&self, key: &PlacementSlotKey) -> Option<&PlacementSlot> {
        self.slots.get(key)
    }

    pub fn admission_open(&self, key: &PlacementSlotKey) -> bool {
        self.authorities
            .get(key)
            .is_some_and(PlacementAuthority::admission_open)
    }

    pub fn ready(&self) -> bool {
        self.session.ready()
    }

    pub fn change_notifier(&self) -> Arc<Notify> {
        self.changed.clone()
    }
}

pub struct LogicCoordinatorSession {
    hello: NodeHello,
    coordinator: lattice_remoting::AssociationKey,
    associations: Arc<AssociationManager>,
    config: LogicCoordinatorConfig,
    state: Arc<Mutex<LogicPlacementState>>,
    stager: Option<SnapshotStager>,
    effects: mpsc::Sender<LogicPlacementEffect>,
    local_events: mpsc::Receiver<LocalAuthorityEvent>,
    local_event_sender: mpsc::Sender<LocalAuthorityEvent>,
    origin: Instant,
    heartbeat_sequence: u64,
}

struct LocalAuthorityEvent {
    slot: PlacementSlotKey,
    succeeded: bool,
}

#[derive(Clone)]
pub struct LogicCoordinatorHandle {
    coordinator: lattice_remoting::AssociationKey,
    associations: Arc<AssociationManager>,
    maximum_control_payload: usize,
    state: Arc<Mutex<LogicPlacementState>>,
    local_events: mpsc::Sender<LocalAuthorityEvent>,
}

impl LogicCoordinatorHandle {
    pub async fn complete_drain(
        &self,
        slot: PlacementSlotKey,
        succeeded: bool,
    ) -> Result<(), LogicSessionError> {
        self.local_events
            .send(LocalAuthorityEvent { slot, succeeded })
            .await
            .map_err(|_| LogicSessionError::ControlClosed)
    }

    pub fn publish_ready(&self, slot: &PlacementSlotKey) -> Result<(), LogicSessionError> {
        self.send_slot_command(slot, true, false)
    }

    pub fn publish_drained(&self, slot: &PlacementSlotKey) -> Result<(), LogicSessionError> {
        self.send_slot_command(slot, false, false)
    }

    pub fn publish_stop_failed(&self, slot: &PlacementSlotKey) -> Result<(), LogicSessionError> {
        self.send_slot_command(slot, false, true)
    }

    pub fn report_node_load(
        &self,
        report: crate::coordinator::NodeLoadReport,
    ) -> Result<(), LogicSessionError> {
        self.send_ephemeral(PlacementControlCommand::NodeLoad(report))
    }

    pub fn report_shard_load(
        &self,
        report: crate::coordinator::ShardLoadReport,
    ) -> Result<(), LogicSessionError> {
        self.send_ephemeral(PlacementControlCommand::ShardLoad(report))
    }

    fn send_ephemeral(&self, command: PlacementControlCommand) -> Result<(), LogicSessionError> {
        let association = self
            .associations
            .get(&self.coordinator)
            .ok_or(LogicSessionError::AssociationUnavailable)?;
        association.admit_ephemeral_control(
            encode_control_command(&command, self.maximum_control_payload)
                .map_err(LogicSessionError::Control)?,
        )?;
        Ok(())
    }

    fn send_slot_command(
        &self,
        slot: &PlacementSlotKey,
        ready: bool,
        stop_failed: bool,
    ) -> Result<(), LogicSessionError> {
        let generation = self
            .state
            .lock()
            .expect("logic placement state poisoned")
            .slot(slot)
            .ok_or(LogicSessionError::UnknownAuthority)?
            .assignment_generation;
        let command = if ready {
            PlacementControlCommand::SlotReady {
                slot: slot.clone(),
                generation,
            }
        } else if stop_failed {
            PlacementControlCommand::SlotStopFailed {
                slot: slot.clone(),
                generation,
            }
        } else {
            PlacementControlCommand::SlotDrained {
                slot: slot.clone(),
                generation,
            }
        };
        let association = self
            .associations
            .get(&self.coordinator)
            .ok_or(LogicSessionError::AssociationUnavailable)?;
        association.admit_control_command(
            encode_control_command(&command, self.maximum_control_payload)
                .map_err(LogicSessionError::Control)?,
        )?;
        Ok(())
    }
}

impl LogicCoordinatorSession {
    pub fn new(
        hello: NodeHello,
        coordinator: lattice_remoting::AssociationKey,
        associations: Arc<AssociationManager>,
        config: LogicCoordinatorConfig,
        effect_capacity: usize,
    ) -> Result<(Self, mpsc::Receiver<LogicPlacementEffect>), LogicSessionError> {
        config.validate()?;
        if effect_capacity == 0
            || hello.node.incarnation != coordinator.local_incarnation
            || hello.node.address == coordinator.remote_address
        {
            return Err(LogicSessionError::InvalidConfig);
        }
        let (effects, receiver) = mpsc::channel(effect_capacity);
        let (local_event_sender, local_events) = mpsc::channel(effect_capacity);
        let local_node = hello.node.clone();
        Ok((
            Self {
                hello,
                coordinator,
                associations,
                config,
                state: Arc::new(Mutex::new(LogicPlacementState {
                    local_node,
                    session: CoordinatorSession::default(),
                    slots: BTreeMap::new(),
                    authorities: BTreeMap::new(),
                    changed: Arc::new(Notify::new()),
                })),
                stager: None,
                effects,
                local_events,
                local_event_sender,
                origin: Instant::now(),
                heartbeat_sequence: 0,
            },
            receiver,
        ))
    }

    pub fn state(&self) -> Arc<Mutex<LogicPlacementState>> {
        self.state.clone()
    }

    pub fn control_handle(&self) -> LogicCoordinatorHandle {
        LogicCoordinatorHandle {
            coordinator: self.coordinator.clone(),
            associations: self.associations.clone(),
            maximum_control_payload: self.config.maximum_control_payload,
            state: self.state.clone(),
            local_events: self.local_event_sender.clone(),
        }
    }

    pub fn coordinator_key(&self) -> &lattice_remoting::AssociationKey {
        &self.coordinator
    }

    pub fn register_authority(
        &self,
        key: PlacementSlotKey,
        safety_margin: Duration,
    ) -> Result<(), LogicSessionError> {
        let mut state = self.state.lock().expect("logic placement state poisoned");
        if state.authorities.len() == self.config.maximum_authorities
            && !state.authorities.contains_key(&key)
        {
            return Err(LogicSessionError::AuthorityCapacity);
        }
        if state.authorities.contains_key(&key) {
            return Err(LogicSessionError::DuplicateAuthority);
        }
        let local = state.local_node.clone();
        state.authorities.insert(
            key,
            PlacementAuthority::new(local, safety_margin).map_err(LogicSessionError::Authority)?,
        );
        Ok(())
    }

    pub fn send_hello(&self) -> Result<(), LogicSessionError> {
        self.send(PlacementControlCommand::NodeHello(self.hello.clone()))
    }

    pub async fn run(
        mut self,
        mut controls: mpsc::Receiver<PlacementControlEvent>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), LogicSessionError> {
        self.send_hello()?;
        let mut tick = tokio::time::interval(self.config.tick_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut heartbeat = tokio::time::interval(self.config.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.reset();
        loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                event = controls.recv() => {
                    let Some(event) = event else {
                        return Err(LogicSessionError::ControlClosed);
                    };
                    let result = self.handle(event.kind).await;
                    let acknowledgement = result
                        .as_ref()
                        .map(|_| ())
                        .map_err(session_dispatch_error);
                    let _ = event.completion.send(acknowledgement);
                    result?;
                }
                event = self.local_events.recv() => {
                    let Some(event) = event else {
                        return Err(LogicSessionError::ControlClosed);
                    };
                    self.handle_local_event(event)?;
                }
                _ = tick.tick() => {
                    self.tick_authorities()?;
                }
                _ = heartbeat.tick() => {
                    self.heartbeat_sequence = self
                        .heartbeat_sequence
                        .checked_add(1)
                        .ok_or(LogicSessionError::HeartbeatSequenceExhausted)?;
                    self.send(PlacementControlCommand::NodeHeartbeat {
                        incarnation: self.hello.node.incarnation,
                        sequence: self.heartbeat_sequence,
                    })?;
                }
            }
        }
    }

    fn handle_local_event(&self, event: LocalAuthorityEvent) -> Result<(), LogicSessionError> {
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
        self.publish_effects(event.slot, effects)
    }

    async fn handle(&mut self, event: PlacementControlEventKind) -> Result<(), LogicSessionError> {
        match event {
            PlacementControlEventKind::Reconcile { association, .. } => {
                self.require_coordinator(&association)?;
                self.send_hello()
            }
            PlacementControlEventKind::Command(inbound) => {
                self.require_coordinator(&inbound.association)?;
                match inbound.command {
                    PlacementControlCommand::SnapshotBegin(begin) => {
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
                        let revision = install.revision;
                        let slots = decode_slots(&install.records)?;
                        self.install_snapshot_slots(slots)?;
                        self.state
                            .lock()
                            .expect("logic placement state poisoned")
                            .session
                            .install(install)
                            .map_err(LogicSessionError::Coordinator)?;
                        self.send(PlacementControlCommand::AppliedRevision(revision))
                    }
                    PlacementControlCommand::StateDelta(delta) => self.apply_delta(delta),
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
                        self.publish_effects(grant.slot, effects)
                    }
                    PlacementControlCommand::NodeRemoved(incarnation) => self
                        .effects
                        .try_send(LogicPlacementEffect::NodeDown(incarnation))
                        .map_err(|_| LogicSessionError::EffectBackpressure),
                    PlacementControlCommand::DrainSlot {
                        slot: key,
                        generation,
                        ..
                    } => {
                        let effects = {
                            let mut state =
                                self.state.lock().expect("logic placement state poisoned");
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
                        self.publish_effects(key, effects)
                    }
                    PlacementControlCommand::NodeHello(_)
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
                    | PlacementControlCommand::BeginDrain
                    | PlacementControlCommand::DrainComplete => {
                        Err(LogicSessionError::UnauthorizedCommand)
                    }
                }
            }
        }
    }

    fn apply_delta(&self, delta: CoordinatorDelta) -> Result<(), LogicSessionError> {
        let slots = decode_slots(&delta.records)?;
        {
            let mut state = self.state.lock().expect("logic placement state poisoned");
            state
                .session
                .apply_delta(delta.clone())
                .map_err(LogicSessionError::Coordinator)?;
        }
        self.install_slots(slots)?;
        self.send(PlacementControlCommand::AppliedRevision(delta.revision))
    }

    fn install_snapshot_slots(
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
        self.install_slots(slots.clone())?;
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

    fn install_slots(
        &self,
        slots: BTreeMap<PlacementSlotKey, PlacementSlot>,
    ) -> Result<(), LogicSessionError> {
        let mut all_effects = Vec::new();
        {
            let mut state = self.state.lock().expect("logic placement state poisoned");
            for (key, slot) in slots {
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
            self.publish_effects(key, effects)?;
        }
        self.state
            .lock()
            .expect("logic placement state poisoned")
            .changed
            .notify_waiters();
        Ok(())
    }

    fn tick_authorities(&self) -> Result<(), LogicSessionError> {
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
            self.publish_effects(key, effects)?;
        }
        Ok(())
    }

    fn publish_effects(
        &self,
        slot: PlacementSlotKey,
        effects: Vec<AuthorityEffect>,
    ) -> Result<(), LogicSessionError> {
        for effect in effects {
            self.effects
                .try_send(LogicPlacementEffect::Authority {
                    slot: slot.clone(),
                    effect,
                })
                .map_err(|_| LogicSessionError::EffectBackpressure)?;
        }
        Ok(())
    }

    fn send(&self, command: PlacementControlCommand) -> Result<(), LogicSessionError> {
        let association = self
            .associations
            .get(&self.coordinator)
            .ok_or(LogicSessionError::AssociationUnavailable)?;
        if association.state() == AssociationState::Closed {
            return Err(LogicSessionError::AssociationUnavailable);
        }
        association.admit_control_command(
            encode_control_command(&command, self.config.maximum_control_payload)
                .map_err(LogicSessionError::Control)?,
        )?;
        Ok(())
    }

    fn require_coordinator(
        &self,
        association: &lattice_remoting::AssociationKey,
    ) -> Result<(), LogicSessionError> {
        if association != &self.coordinator {
            return Err(LogicSessionError::UnauthorizedCommand);
        }
        Ok(())
    }

    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_millis(
            u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX),
        )
    }
}

fn decode_slots(
    records: &[crate::coordinator::SnapshotRecord],
) -> Result<BTreeMap<PlacementSlotKey, PlacementSlot>, LogicSessionError> {
    let mut slots = BTreeMap::new();
    for record in records {
        if !(record.key.starts_with("shard/") || record.key.starts_with("singleton/")) {
            continue;
        }
        let slot: PlacementSlot =
            serde_json::from_slice(&record.value).map_err(|_| LogicSessionError::Codec)?;
        slot.validate().map_err(|_| LogicSessionError::Codec)?;
        if slots.insert(slot.key.clone(), slot).is_some() {
            return Err(LogicSessionError::Codec);
        }
    }
    Ok(slots)
}

fn session_dispatch_error(error: &LogicSessionError) -> ControlDispatchError {
    match error {
        LogicSessionError::UnauthorizedCommand
        | LogicSessionError::Codec
        | LogicSessionError::SnapshotRequired
        | LogicSessionError::StaleGeneration
        | LogicSessionError::Coordinator(_)
        | LogicSessionError::Authority(_)
        | LogicSessionError::UnknownAuthority => ControlDispatchError::InvalidCommand,
        _ => ControlDispatchError::Unavailable,
    }
}

#[derive(Debug, Error)]
pub enum LogicSessionError {
    #[error("logic Coordinator session configuration is invalid")]
    InvalidConfig,
    #[error("logic Coordinator control stream closed")]
    ControlClosed,
    #[error("logic Coordinator association is unavailable")]
    AssociationUnavailable,
    #[error("logic Coordinator received a command from another peer")]
    UnauthorizedCommand,
    #[error("logic Coordinator snapshot must begin before chunks/end")]
    SnapshotRequired,
    #[error("logic Coordinator snapshot record is invalid")]
    Codec,
    #[error("logic Coordinator slot authority is not registered")]
    UnknownAuthority,
    #[error("logic Coordinator slot authority registration is full")]
    AuthorityCapacity,
    #[error("logic Coordinator slot authority is already registered")]
    DuplicateAuthority,
    #[error("logic Coordinator drain command has a stale generation")]
    StaleGeneration,
    #[error("logic Coordinator heartbeat sequence exhausted")]
    HeartbeatSequenceExhausted,
    #[error("logic Coordinator effect queue is full or closed")]
    EffectBackpressure,
    #[error("logic Coordinator state reducer rejected input")]
    Coordinator(#[source] crate::coordinator::CoordinatorError),
    #[error("logic Coordinator placement authority rejected input")]
    Authority(#[source] crate::authority::AuthorityError),
    #[error("logic Coordinator control codec failed")]
    Control(#[source] crate::control::PlacementControlError),
    #[error("logic Coordinator Association rejected control admission")]
    Association(#[from] lattice_remoting::association::AssociationError),
}
