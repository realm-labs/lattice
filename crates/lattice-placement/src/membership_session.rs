use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use lattice_core::{actor_ref::NodeIncarnation, coordinator::CoordinatorScope};
use lattice_remoting::{
    association::{AssociationKey, AssociationManager, AssociationState},
    control::ControlDispatchError,
};
use tokio::{
    sync::{Notify, mpsc, watch},
    time::MissedTickBehavior,
};

use crate::{
    control::{
        PlacementControlCommand, PlacementControlEvent, PlacementControlEventKind,
        encode_control_command_for_term,
    },
    coordinator::{
        MemberEvent, MemberHello, MemberRecord, MemberStatus, MembershipState, SnapshotRecord,
        SnapshotStager, SnapshotVersion,
    },
    session::{LogicCoordinatorConfig, LogicPlacementEffect, LogicSessionError},
    types::{MonotonicTime, NodeKey},
};

pub struct MembershipSessionState {
    session: MembershipState,
    local_node: NodeKey,
    changed: Arc<Notify>,
}

impl MembershipSessionState {
    pub fn ready(&self) -> bool {
        self.session.ready()
            && self
                .session
                .member(&self.local_node)
                .is_some_and(|member| member.status == MemberStatus::Up)
    }

    pub fn change_notifier(&self) -> Arc<Notify> {
        self.changed.clone()
    }
}

pub struct MembershipSession {
    hello: MemberHello,
    coordinator: AssociationKey,
    associations: Arc<AssociationManager>,
    config: LogicCoordinatorConfig,
    state: Arc<Mutex<MembershipSessionState>>,
    stager: Option<SnapshotStager>,
    effects: mpsc::Sender<LogicPlacementEffect>,
    heartbeat_sequence: u64,
    coordinator_term: u64,
}

#[derive(Clone)]
pub struct MembershipCoordinatorHandle {
    local_incarnation: NodeIncarnation,
    coordinator: AssociationKey,
    associations: Arc<AssociationManager>,
    maximum_control_payload: usize,
    coordinator_term: u64,
}

impl MembershipCoordinatorHandle {
    pub async fn complete_drain(&self, operation_id: String) -> Result<(), LogicSessionError> {
        let association = self
            .associations
            .get(&self.coordinator)
            .ok_or(LogicSessionError::AssociationUnavailable)?;
        let command_id = association.admit_control_command(
            encode_control_command_for_term(
                &CoordinatorScope::Membership,
                self.coordinator_term,
                &PlacementControlCommand::MembershipDrainComplete {
                    operation_id,
                    expected_incarnation: self.local_incarnation,
                },
                self.maximum_control_payload,
            )
            .map_err(LogicSessionError::Control)?,
        )?;
        while association.control_command_pending(command_id) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }
}

impl MembershipSession {
    pub fn new(
        hello: MemberHello,
        coordinator: AssociationKey,
        associations: Arc<AssociationManager>,
        config: LogicCoordinatorConfig,
        effect_capacity: usize,
        coordinator_term: u64,
    ) -> Result<
        (
            Self,
            MembershipCoordinatorHandle,
            mpsc::Receiver<LogicPlacementEffect>,
        ),
        LogicSessionError,
    > {
        if effect_capacity == 0
            || coordinator_term == 0
            || hello.node.incarnation != coordinator.local_incarnation
            || hello.node.address == coordinator.remote_address
        {
            return Err(LogicSessionError::InvalidConfig);
        }
        let (effects, receiver) = mpsc::channel(effect_capacity);
        let handle = MembershipCoordinatorHandle {
            local_incarnation: hello.node.incarnation,
            coordinator: coordinator.clone(),
            associations: associations.clone(),
            maximum_control_payload: config.maximum_control_payload,
            coordinator_term,
        };
        let local_node = hello.node.clone();
        Ok((
            Self {
                hello,
                coordinator,
                associations,
                config,
                state: Arc::new(Mutex::new(MembershipSessionState {
                    session: MembershipState::default(),
                    local_node,
                    changed: Arc::new(Notify::new()),
                })),
                stager: None,
                effects,
                heartbeat_sequence: 0,
                coordinator_term,
            },
            handle,
            receiver,
        ))
    }

    pub fn state(&self) -> Arc<Mutex<MembershipSessionState>> {
        self.state.clone()
    }

    pub async fn run_recoverable(
        mut self,
        mut controls: mpsc::Receiver<PlacementControlEvent>,
        mut shutdown: watch::Receiver<bool>,
    ) -> (
        Result<(), LogicSessionError>,
        mpsc::Receiver<PlacementControlEvent>,
    ) {
        let result = self.run_loop(&mut controls, &mut shutdown).await;
        (result, controls)
    }

    async fn run_loop(
        &mut self,
        controls: &mut mpsc::Receiver<PlacementControlEvent>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<(), LogicSessionError> {
        self.send(PlacementControlCommand::MemberHello(self.hello.clone()))?;
        let mut heartbeat = tokio::time::interval(self.config.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        heartbeat.reset();
        loop {
            tokio::select! {
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
                        .map_err(membership_dispatch_error);
                    let _ = event.completion.send(acknowledgement);
                    result?;
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

    async fn handle(&mut self, event: PlacementControlEventKind) -> Result<(), LogicSessionError> {
        match event {
            PlacementControlEventKind::GlobalMemberRemoved { .. } => {
                Err(LogicSessionError::UnauthorizedCommand)
            }
            PlacementControlEventKind::Reconcile { association, .. } => {
                self.require_coordinator(&association)?;
                self.state
                    .lock()
                    .expect("membership session state poisoned")
                    .session = MembershipState::default();
                self.send(PlacementControlCommand::MemberHello(self.hello.clone()))
            }
            PlacementControlEventKind::Command(inbound) => {
                self.require_coordinator(&inbound.association)?;
                self.require_coordinator_term(inbound.coordinator_term)?;
                match inbound.command {
                    PlacementControlCommand::SnapshotBegin(begin) => {
                        if !matches!(begin.version, SnapshotVersion::Membership(_)) {
                            return Err(LogicSessionError::UnauthorizedCommand);
                        }
                        self.stager = Some(
                            SnapshotStager::begin(
                                begin,
                                self.config.snapshot_limits.clone(),
                                MonotonicTime::from_millis(0),
                            )
                            .map_err(LogicSessionError::Coordinator)?,
                        );
                        Ok(())
                    }
                    PlacementControlCommand::SnapshotChunk(chunk) => self
                        .stager
                        .as_mut()
                        .ok_or(LogicSessionError::SnapshotRequired)?
                        .push(chunk, MonotonicTime::from_millis(0))
                        .map_err(LogicSessionError::Coordinator),
                    PlacementControlCommand::SnapshotEnd(end) => {
                        let install = self
                            .stager
                            .take()
                            .ok_or(LogicSessionError::SnapshotRequired)?
                            .finish(end, MonotonicTime::from_millis(0))
                            .map_err(LogicSessionError::Coordinator)?;
                        let SnapshotVersion::Membership(version) = install.version.clone() else {
                            return Err(LogicSessionError::UnauthorizedCommand);
                        };
                        let members = decode_members(&install.records)?;
                        let changed = {
                            let mut state = self
                                .state
                                .lock()
                                .expect("membership session state poisoned");
                            state
                                .session
                                .install(install)
                                .map_err(LogicSessionError::MembershipState)?;
                            state.changed.clone()
                        };
                        self.effects
                            .try_send(LogicPlacementEffect::MemberSnapshot { version, members })
                            .map_err(|_| LogicSessionError::EffectBackpressure)?;
                        changed.notify_waiters();
                        self.send(PlacementControlCommand::JoinReady {
                            snapshot_version: version,
                        })
                    }
                    PlacementControlCommand::MemberDelta(event) => self.apply_member_event(event),
                    _ => Err(LogicSessionError::UnauthorizedCommand),
                }
            }
        }
    }

    fn apply_member_event(&self, event: MemberEvent) -> Result<(), LogicSessionError> {
        let changed = {
            let mut state = self
                .state
                .lock()
                .expect("membership session state poisoned");
            if state
                .session
                .version()
                .is_some_and(|current| current.satisfies(event.version))
            {
                return Ok(());
            }
            state
                .session
                .apply(event.clone())
                .map_err(LogicSessionError::MembershipState)?;
            state.changed.clone()
        };
        self.effects
            .try_send(LogicPlacementEffect::MemberEvent(Box::new(event)))
            .map_err(|_| LogicSessionError::EffectBackpressure)?;
        changed.notify_waiters();
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
            encode_control_command_for_term(
                &CoordinatorScope::Membership,
                self.coordinator_term,
                &command,
                self.config.maximum_control_payload,
            )
            .map_err(LogicSessionError::Control)?,
        )?;
        Ok(())
    }

    fn require_coordinator(&self, association: &AssociationKey) -> Result<(), LogicSessionError> {
        if association == &self.coordinator {
            Ok(())
        } else {
            Err(LogicSessionError::UnauthorizedCommand)
        }
    }

    fn require_coordinator_term(&self, term: Option<u64>) -> Result<(), LogicSessionError> {
        if term == Some(self.coordinator_term) {
            Ok(())
        } else {
            Err(LogicSessionError::StaleGeneration)
        }
    }
}

fn decode_members(records: &[SnapshotRecord]) -> Result<Vec<MemberRecord>, LogicSessionError> {
    let mut members = BTreeMap::new();
    for record in records {
        if !record.key.starts_with("member/") {
            continue;
        }
        let member: MemberRecord =
            serde_json::from_slice(&record.value).map_err(|_| LogicSessionError::Codec)?;
        if member.node != member.hello.node
            || members
                .insert(
                    (member.node.node_id.clone(), member.node.incarnation),
                    member,
                )
                .is_some()
        {
            return Err(LogicSessionError::Codec);
        }
    }
    Ok(members.into_values().collect())
}

fn membership_dispatch_error(error: &LogicSessionError) -> ControlDispatchError {
    match error {
        LogicSessionError::UnauthorizedCommand
        | LogicSessionError::Codec
        | LogicSessionError::SnapshotRequired
        | LogicSessionError::StaleGeneration
        | LogicSessionError::Coordinator(_)
        | LogicSessionError::MembershipState(_) => ControlDispatchError::InvalidCommand,
        _ => ControlDispatchError::Unavailable,
    }
}
