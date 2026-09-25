use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex},
    time::Duration,
};

use lattice_model::{
    cluster::CoordinatorScope,
    cluster::{ActorGroupId, NodeIncarnation},
    run::{ClusterLifecycle, ControlOperationId, RunEpoch},
};
use lattice_remoting::{
    association::{AssociationError, AssociationKey, AssociationManager, AssociationState},
    control::ControlDispatchError,
};
use thiserror::Error;
use tokio::{
    sync::{Notify, mpsc, watch},
    time::{Instant, MissedTickBehavior},
};

use crate::{
    authority::{AuthorityEffect, AuthorityError, PlacementAuthority, clock::AuthorityClock},
    control::{
        DEFAULT_MAX_CONTROL_PAYLOAD, PlacementControlCommand, PlacementControlError,
        PlacementControlEvent, PlacementControlEventKind, PlacementResolutionFailure,
        encode_control_command_for_term,
    },
    coordinator::{
        ActorGroupHello, ActorGroupState, ActorGroupStateError, CoordinatorError, MemberEvent,
        MemberRecord, MembershipStateError, NodeLoadReport, SnapshotLimits, SnapshotStager,
    },
    types::{
        MembershipVersion, MonotonicTime, NodeKey, PlacementSlot, PlacementSlotKey,
        PlacementSlotState,
    },
};

mod dispatch;
mod handle;
mod snapshot;

pub(crate) const CONTROL_ADMISSION_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct GroupSessionConfig {
    pub snapshot_limits: SnapshotLimits,
    pub maximum_control_payload: usize,
    pub tick_interval: Duration,
    pub heartbeat_interval: Duration,
    /// Publishes a unit-weight fallback derived from the locally owned placement slots.
    ///
    /// Applications that publish their own measured `NodeLoadReport` sequence must disable this
    /// fallback so the two independent sequence sources cannot supersede one another.
    pub automatic_node_load_reporting: bool,
    pub maximum_authorities: usize,
    pub claim_safety_margin: Duration,
    pub drain_acknowledgement_timeout: Duration,
}

impl Default for GroupSessionConfig {
    fn default() -> Self {
        Self {
            snapshot_limits: SnapshotLimits {
                maximum_chunk_bytes: 192 * 1024,
                ..SnapshotLimits::default()
            },
            maximum_control_payload: DEFAULT_MAX_CONTROL_PAYLOAD,
            tick_interval: Duration::from_millis(100),
            heartbeat_interval: Duration::from_secs(5),
            automatic_node_load_reporting: true,
            maximum_authorities: 65_536,
            claim_safety_margin: Duration::from_secs(2),
            drain_acknowledgement_timeout: Duration::from_secs(30),
        }
    }
}

impl GroupSessionConfig {
    pub(crate) fn validate(&self) -> Result<(), GroupSessionError> {
        if self.maximum_control_payload == 0
            || self.tick_interval.is_zero()
            || self.heartbeat_interval.is_zero()
            || self.maximum_authorities == 0
            || self.claim_safety_margin.is_zero()
            || self.drain_acknowledgement_timeout.is_zero()
        {
            return Err(GroupSessionError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicPlacementEffect {
    ClusterClosing {
        epoch: RunEpoch,
        operation: ControlOperationId,
    },
    ClusterClosed(ClusterLifecycle),
    Authority {
        slot: PlacementSlotKey,
        effect: AuthorityEffect,
    },
    MemberEvent(Box<MemberEvent>),
    MemberSnapshot {
        version: MembershipVersion,
        members: Vec<MemberRecord>,
    },
    DrainReady {
        operation_id: String,
        incarnation: NodeIncarnation,
    },
    DrainCommitted {
        operation_id: String,
        incarnation: NodeIncarnation,
    },
}

pub struct LogicPlacementState {
    local_node: NodeKey,
    coordinator_term: u64,
    session: ActorGroupState,
    slots: BTreeMap<PlacementSlotKey, PlacementSlot>,
    authorities: BTreeMap<PlacementSlotKey, PlacementAuthority>,
    resolution_failures: BTreeMap<PlacementSlotKey, (u128, PlacementResolutionFailure)>,
    group_up: bool,
    origin: AuthorityClock,
    changed: Arc<Notify>,
}

impl LogicPlacementState {
    /// Test coordinators must answer the request actually emitted by the owner,
    /// not inject receipt-relative grants. This accessor does not install authority.
    #[cfg(any(test, feature = "test-harness"))]
    #[doc(hidden)]
    pub fn pending_claim_request(&self, slot: &PlacementSlotKey) -> Option<u128> {
        self.authorities
            .get(slot)
            .and_then(PlacementAuthority::pending_request_id)
    }
    pub fn slot(&self, key: &PlacementSlotKey) -> Option<&PlacementSlot> {
        self.slots.get(key)
    }

    /// The monotonic base the session installs grants against. Admission reads it here rather than
    /// accepting a caller-supplied instant so no delivery path can prove authority with a clock the
    /// grant deadline was never computed from.
    pub fn now(&self) -> MonotonicTime {
        monotonic_since(self.origin)
    }

    pub fn admission_open(&self, key: &PlacementSlotKey) -> bool {
        let now = self.now();
        self.authorities
            .get(key)
            .is_some_and(|authority| authority.admission_open_at(now))
    }

    pub fn resolution_failure(
        &self,
        key: &PlacementSlotKey,
        request_id: u128,
    ) -> Option<PlacementResolutionFailure> {
        self.resolution_failures
            .get(key)
            .filter(|(failed_request, _)| *failed_request == request_id)
            .map(|(_, reason)| *reason)
    }

    pub fn ready(&self) -> bool {
        self.session.ready() && self.group_up
    }

    fn ready_for_admission(&self) -> bool {
        let now = self.now();
        self.ready()
            && self
                .slots
                .iter()
                .filter(|(_, slot)| slot.owner.as_ref() == Some(&self.local_node))
                .all(|(key, slot)| {
                    slot.state == PlacementSlotState::Running
                        && self
                            .authorities
                            .get(key)
                            .is_some_and(|authority| authority.admission_open_at(now))
                })
    }

    pub fn change_notifier(&self) -> Arc<Notify> {
        self.changed.clone()
    }

    pub fn coordinator_term(&self) -> Option<u64> {
        Some(self.coordinator_term)
    }

    fn baseline_node_load(&self, sequence: u64) -> NodeLoadReport {
        let total_weight = u64::try_from(
            self.slots
                .values()
                .filter(|slot| {
                    slot.owner.as_ref() == Some(&self.local_node)
                        && !matches!(
                            slot.state,
                            PlacementSlotState::Unallocated | PlacementSlotState::Fenced
                        )
                })
                .count(),
        )
        .unwrap_or(u64::MAX);
        NodeLoadReport {
            node: self.local_node.clone(),
            sequence,
            observed_at: self.now(),
            total_weight,
        }
    }
}

pub struct GroupSession {
    group_hello: ActorGroupHello,
    coordinator: AssociationKey,
    associations: Arc<AssociationManager>,
    config: GroupSessionConfig,
    state: Arc<Mutex<LogicPlacementState>>,
    stager: Option<SnapshotStager>,
    effects: mpsc::Sender<LogicPlacementEffect>,
    local_events: mpsc::Receiver<LocalAuthorityEvent>,
    local_event_sender: mpsc::Sender<LocalAuthorityEvent>,
    origin: AuthorityClock,
    heartbeat_sequence: u64,
    heartbeat_ack_sequence: u64,
    last_heartbeat_ack: Instant,
    coordinator_term: u64,
    shared_coordinator_term: Arc<AtomicU64>,
    hello_pending: bool,
    drain_confirmation: crate::drain::DrainConfirmation,
}

const MAX_READY_REPLAYS_PER_HEARTBEAT: usize = 64;

struct LocalAuthorityEvent {
    slot: PlacementSlotKey,
    succeeded: bool,
}

#[derive(Clone)]
pub struct GroupSessionHandle {
    group: ActorGroupId,
    coordinator: AssociationKey,
    associations: Arc<AssociationManager>,
    maximum_control_payload: usize,
    state: Arc<Mutex<LogicPlacementState>>,
    local_events: mpsc::Sender<LocalAuthorityEvent>,
    coordinator_term: Arc<AtomicU64>,
    drain_acknowledgement_timeout: Duration,
    drain_confirmation: crate::drain::DrainConfirmation,
}

impl GroupSession {
    pub fn new(
        group_hello: ActorGroupHello,
        coordinator: AssociationKey,
        associations: Arc<AssociationManager>,
        config: GroupSessionConfig,
        effect_capacity: usize,
        coordinator_term: u64,
    ) -> Result<(Self, mpsc::Receiver<LogicPlacementEffect>), GroupSessionError> {
        config.validate()?;
        if effect_capacity == 0
            || coordinator_term == 0
            || group_hello.node.incarnation != coordinator.local_incarnation
            || group_hello.node.address == coordinator.remote_address
        {
            return Err(GroupSessionError::InvalidConfig);
        }
        let (effects, receiver) = mpsc::channel(effect_capacity);
        let (local_event_sender, local_events) = mpsc::channel(effect_capacity);
        let local_node = group_hello.node.clone();
        let group = group_hello.group.clone();
        let origin = AuthorityClock::new().ok_or(GroupSessionError::UnsupportedClock)?;
        let shared_coordinator_term = Arc::new(AtomicU64::new(coordinator_term));
        Ok((
            Self {
                group_hello,
                coordinator,
                associations,
                config,
                state: Arc::new(Mutex::new(LogicPlacementState {
                    local_node,
                    coordinator_term,
                    session: ActorGroupState::new(group),
                    slots: BTreeMap::new(),
                    authorities: BTreeMap::new(),
                    resolution_failures: BTreeMap::new(),
                    group_up: false,
                    origin,
                    changed: Arc::new(Notify::new()),
                })),
                stager: None,
                effects,
                local_events,
                local_event_sender,
                origin,
                heartbeat_sequence: 0,
                heartbeat_ack_sequence: 0,
                last_heartbeat_ack: Instant::now(),
                coordinator_term,
                shared_coordinator_term,
                hello_pending: true,
                drain_confirmation: crate::drain::DrainConfirmation::default(),
            },
            receiver,
        ))
    }

    pub fn state(&self) -> Arc<Mutex<LogicPlacementState>> {
        self.state.clone()
    }

    pub fn control_handle(&self) -> GroupSessionHandle {
        GroupSessionHandle {
            group: self.group_hello.group.clone(),
            coordinator: self.coordinator.clone(),
            associations: self.associations.clone(),
            maximum_control_payload: self.config.maximum_control_payload,
            state: self.state.clone(),
            local_events: self.local_event_sender.clone(),
            coordinator_term: self.shared_coordinator_term.clone(),
            drain_acknowledgement_timeout: self.config.drain_acknowledgement_timeout,
            drain_confirmation: self.drain_confirmation.clone(),
        }
    }

    pub fn coordinator_key(&self) -> &AssociationKey {
        &self.coordinator
    }

    pub fn register_authority(
        &self,
        key: PlacementSlotKey,
        safety_margin: Duration,
    ) -> Result<(), GroupSessionError> {
        let mut state = self.state.lock().expect("logic placement state poisoned");
        if state.authorities.len() == self.config.maximum_authorities
            && !state.authorities.contains_key(&key)
        {
            return Err(GroupSessionError::AuthorityCapacity);
        }
        if state.authorities.contains_key(&key) {
            return Err(GroupSessionError::DuplicateAuthority);
        }
        let local = state.local_node.clone();
        state.authorities.insert(
            key,
            PlacementAuthority::new(local, safety_margin).map_err(GroupSessionError::Authority)?,
        );
        Ok(())
    }

    pub fn send_hello(&self) -> Result<(), GroupSessionError> {
        self.send_immediate(PlacementControlCommand::ActorGroupHello(
            self.group_hello.clone(),
        ))
    }

    async fn send_hello_wait(&self) -> Result<(), GroupSessionError> {
        self.send(PlacementControlCommand::ActorGroupHello(
            self.group_hello.clone(),
        ))
        .await
    }

    pub async fn run(
        self,
        controls: mpsc::Receiver<PlacementControlEvent>,
        shutdown: watch::Receiver<bool>,
    ) -> Result<(), GroupSessionError> {
        self.run_recoverable(controls, shutdown).await.0
    }

    pub async fn run_recoverable(
        mut self,
        mut controls: mpsc::Receiver<PlacementControlEvent>,
        mut shutdown: watch::Receiver<bool>,
    ) -> (
        Result<(), GroupSessionError>,
        mpsc::Receiver<PlacementControlEvent>,
    ) {
        let result = self.run_loop(&mut controls, &mut shutdown).await;
        self.drain_confirmation.close();
        if let Err(error) = &result {
            tracing::warn!(
                target: "lattice.cluster.logic",
                %error,
                "logic Coordinator session terminated"
            );
        }
        (result, controls)
    }

    async fn run_loop(
        &mut self,
        controls: &mut mpsc::Receiver<PlacementControlEvent>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<(), GroupSessionError> {
        self.send_hello_wait().await?;
        let mut tick = tokio::time::interval(self.config.tick_interval);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut heartbeat = tokio::time::interval(self.config.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        heartbeat.reset();
        let mut last_heartbeat = Instant::now();
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
                        return Err(GroupSessionError::ControlClosed);
                    };
                    let event_name = event_name(&event.kind);
                    let result = self.handle(event.kind).await;
                    let stale_generation = matches!(&result, Err(GroupSessionError::StaleGeneration));
                    let acknowledgement = result
                        .as_ref()
                        .map(|_| ())
                        .map_err(session_dispatch_error);
                    let _ = event.completion.send(acknowledgement);
                    if stale_generation {
                        self.reconcile_after_stale_control(event_name).await;
                        continue;
                    }
                    result?;
                }
                event = self.local_events.recv() => {
                    let Some(event) = event else {
                        return Err(GroupSessionError::ControlClosed);
                    };
                    self.handle_local_event(event).await?;
                }
                _ = tick.tick() => {
                    self.tick_authorities().await?;
                }
                _ = heartbeat.tick() => {
                    // Draining deliberately stops heartbeats; retain the control path
                    // for the final application-level stop proof and confirmation.
                    // Authority ticking remains independent and cannot be extended here.
                    if self.drain_confirmation.requested() {
                        continue;
                    }
                    if last_heartbeat.elapsed() > self.config.heartbeat_interval.saturating_mul(2) {
                        return Err(GroupSessionError::HeartbeatInterrupted);
                    }
                    last_heartbeat = Instant::now();
                    if self.heartbeat_sequence > 0 && self.last_heartbeat_ack.elapsed() > self.config.heartbeat_interval.saturating_mul(3) {
                        return Err(GroupSessionError::HeartbeatInterrupted);
                    }
                    if self.hello_pending {
                        // Domain registration can race global membership recovery. Retry the
                        // idempotent hello until the Coordinator starts the placement snapshot.
                        self.send_hello_wait().await?;
                        continue;
                    }
                    if self.heartbeat_sequence == 0 { self.last_heartbeat_ack = Instant::now(); }
                    self.heartbeat_sequence = self
                        .heartbeat_sequence
                        .checked_add(1)
                        .ok_or(GroupSessionError::HeartbeatSequenceExhausted)?;
                    self.send(PlacementControlCommand::NodeHeartbeat {
                        incarnation: self.group_hello.node.incarnation,
                        sequence: self.heartbeat_sequence,
                    }).await?;
                    self.replay_runtime_progress()?;
                    let owned = self.state.lock().expect("logic placement state poisoned")
                        .authorities.keys().cloned().collect::<Vec<_>>();
                    for key in owned { self.request_claim(&key)?; }
                    if self.config.automatic_node_load_reporting {
                        let report = self
                            .state
                            .lock()
                            .expect("logic placement state poisoned")
                            .baseline_node_load(self.heartbeat_sequence);
                        if let Err(error) =
                            self.send_ephemeral(PlacementControlCommand::NodeLoad(report))
                        {
                            // Load samples are intentionally latest-value and lossy. A full
                            // ephemeral lane must not terminate the reliable heartbeat session.
                            tracing::debug!(
                                target: "lattice.cluster.logic",
                                group = %self.group_hello.group.as_str(),
                                sequence = self.heartbeat_sequence,
                                %error,
                                "automatic node load sample was dropped"
                            );
                        }
                    }
                }
            }
        }
    }

    async fn send(&self, command: PlacementControlCommand) -> Result<(), GroupSessionError> {
        let association = self
            .associations
            .get(&self.coordinator)
            .ok_or(GroupSessionError::AssociationUnavailable)?;
        if association.state() == AssociationState::Closed {
            return Err(GroupSessionError::AssociationUnavailable);
        }
        let scope = CoordinatorScope::Group(self.group_hello.group.clone());
        let payload = encode_control_command_for_term(
            &scope,
            self.coordinator_term,
            &command,
            self.config.maximum_control_payload,
        )
        .map_err(GroupSessionError::Control)?;
        association
            .admit_control_command_in_wait(
                crate::control::control_stream_id(&scope),
                payload,
                CONTROL_ADMISSION_TIMEOUT,
            )
            .await?;
        Ok(())
    }

    fn send_immediate(&self, command: PlacementControlCommand) -> Result<(), GroupSessionError> {
        let association = self
            .associations
            .get(&self.coordinator)
            .ok_or(GroupSessionError::AssociationUnavailable)?;
        if association.state() == AssociationState::Closed {
            return Err(GroupSessionError::AssociationUnavailable);
        }
        let scope = CoordinatorScope::Group(self.group_hello.group.clone());
        association.admit_control_command_in(
            crate::control::control_stream_id(&scope),
            encode_control_command_for_term(
                &scope,
                self.coordinator_term,
                &command,
                self.config.maximum_control_payload,
            )
            .map_err(GroupSessionError::Control)?,
        )?;
        Ok(())
    }

    fn send_ephemeral(&self, command: PlacementControlCommand) -> Result<(), GroupSessionError> {
        let association = self
            .associations
            .get(&self.coordinator)
            .ok_or(GroupSessionError::AssociationUnavailable)?;
        if association.state() == AssociationState::Closed {
            return Err(GroupSessionError::AssociationUnavailable);
        }
        let scope = CoordinatorScope::Group(self.group_hello.group.clone());
        association.admit_ephemeral_control(
            encode_control_command_for_term(
                &scope,
                self.coordinator_term,
                &command,
                self.config.maximum_control_payload,
            )
            .map_err(GroupSessionError::Control)?,
        )?;
        Ok(())
    }

    /// Applied revisions and initial-authority readiness are level-triggered runtime state. They
    /// must never occupy the reliable outbox: a burst of newly allocated shards can otherwise
    /// fill that outbox and prevent the heartbeats needed to drain it. Transient ephemeral
    /// admission failure is safe because the current level is replayed on every heartbeat.
    pub(super) fn send_runtime_progress(
        &self,
        command: PlacementControlCommand,
    ) -> Result<(), GroupSessionError> {
        match self.send_ephemeral(command) {
            Ok(()) => Ok(()),
            Err(GroupSessionError::Association(error)) => {
                tracing::debug!(
                    target: "lattice.cluster.logic",
                    group = %self.group_hello.group.as_str(),
                    %error,
                    "runtime progress sample was dropped and will be replayed"
                );
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    fn replay_runtime_progress(&self) -> Result<(), GroupSessionError> {
        let (version, ready) = {
            let state = self.state.lock().expect("logic placement state poisoned");
            let now = state.now();
            let ready = state
                .slots
                .iter()
                .filter(|(key, slot)| {
                    slot.state == PlacementSlotState::Allocating
                        && slot.owner.as_ref() == Some(&state.local_node)
                        && state
                            .authorities
                            .get(*key)
                            .is_some_and(|authority| authority.admission_open_at(now))
                })
                .take(MAX_READY_REPLAYS_PER_HEARTBEAT)
                .map(|(key, slot)| (key.clone(), slot.assignment_generation))
                .collect::<Vec<_>>();
            (state.session.version().cloned(), ready)
        };
        if let Some(version) = version {
            self.send_runtime_progress(PlacementControlCommand::AppliedRevision(version))?;
        }
        for (slot, generation) in ready {
            self.send_runtime_progress(PlacementControlCommand::SlotReady { slot, generation })?;
        }
        Ok(())
    }

    fn require_coordinator(&self, association: &AssociationKey) -> Result<(), GroupSessionError> {
        if association != &self.coordinator {
            return Err(GroupSessionError::UnauthorizedCommand);
        }
        Ok(())
    }

    fn require_coordinator_term(&self, term: Option<u64>) -> Result<(), GroupSessionError> {
        if term == Some(self.coordinator_term) {
            Ok(())
        } else {
            Err(GroupSessionError::StaleGeneration)
        }
    }

    fn accept_snapshot_term(&mut self, term: Option<u64>) -> Result<(), GroupSessionError> {
        let Some(term) = term else {
            return Err(GroupSessionError::StaleGeneration);
        };
        if term < self.coordinator_term {
            return Err(GroupSessionError::StaleGeneration);
        }
        self.coordinator_term = term;
        self.shared_coordinator_term.store(term, Ordering::Release);
        self.state
            .lock()
            .expect("logic placement state poisoned")
            .coordinator_term = term;
        Ok(())
    }

    fn now(&self) -> MonotonicTime {
        monotonic_since(self.origin)
    }

    async fn reconcile_after_stale_control(&mut self, command: &'static str) {
        if self.drain_confirmation.committed_operation().is_some() {
            return;
        }
        self.stager = None;
        self.state
            .lock()
            .expect("logic placement state poisoned")
            .group_up = false;
        self.hello_pending = true;
        if let Err(error) = self.send_hello_wait().await {
            tracing::warn!(
                target: "lattice.cluster.logic",
                group = %self.group_hello.group.as_str(),
                command,
                coordinator_term = self.coordinator_term,
                %error,
                "stale placement control was dropped; fresh snapshot request could not be sent"
            );
        } else {
            tracing::warn!(
                target: "lattice.cluster.logic",
                group = %self.group_hello.group.as_str(),
                command,
                coordinator_term = self.coordinator_term,
                "stale placement control was dropped; requested fresh snapshot"
            );
        }
    }
}

fn event_name(event: &PlacementControlEventKind) -> &'static str {
    match event {
        PlacementControlEventKind::Command(inbound) => inbound.command.name(),
        PlacementControlEventKind::Reconcile { .. } => "Reconcile",
        PlacementControlEventKind::GlobalMemberRemoved { .. } => "GlobalMemberRemoved",
    }
}

fn monotonic_since(origin: AuthorityClock) -> MonotonicTime {
    origin.now()
}

fn session_dispatch_error(error: &GroupSessionError) -> ControlDispatchError {
    match error {
        GroupSessionError::UnauthorizedCommand
        | GroupSessionError::Codec
        | GroupSessionError::SnapshotRequired
        | GroupSessionError::StaleGeneration
        | GroupSessionError::Coordinator(_)
        | GroupSessionError::MembershipState(_)
        | GroupSessionError::PlacementState(_)
        | GroupSessionError::Authority(_)
        | GroupSessionError::UnknownAuthority => ControlDispatchError::InvalidCommand,
        _ => ControlDispatchError::RetryLater(
            lattice_remoting::control::ControlRetryReason::AssociationStarting,
        ),
    }
}

#[derive(Debug, Error)]
pub enum GroupSessionError {
    #[error("serving authority requires a validated suspension-aware clock")]
    UnsupportedClock,
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
    #[error("session heartbeat was interrupted; fresh registration is required")]
    HeartbeatInterrupted,
    #[error("logic Coordinator did not acknowledge drain completion inside its bound")]
    DrainNotAcknowledged,
    #[error("logic Coordinator effect consumer is closed")]
    EffectBackpressure,
    #[error("logic Coordinator state reducer rejected input")]
    Coordinator(#[source] CoordinatorError),
    #[error("membership state reducer rejected input")]
    MembershipState(#[source] MembershipStateError),
    #[error("actor-group state reducer rejected input")]
    PlacementState(#[source] ActorGroupStateError),
    #[error("logic Coordinator placement authority rejected input")]
    Authority(#[source] AuthorityError),
    #[error("logic Coordinator control codec failed")]
    Control(#[source] PlacementControlError),
    #[error("logic Coordinator Association rejected control admission: {0}")]
    Association(#[from] AssociationError),
}

#[cfg(test)]
mod tests;
