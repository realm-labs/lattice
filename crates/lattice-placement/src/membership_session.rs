use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex},
    time::Duration,
};

use lattice_core::{actor_address::NodeIncarnation, coordinator::CoordinatorScope};
use lattice_remoting::{
    association::{AssociationKey, AssociationManager, AssociationState},
    control::ControlDispatchError,
};
use tokio::{
    sync::{Notify, mpsc, watch},
    time::{Instant, MissedTickBehavior},
};

use crate::{
    control::{
        PlacementControlCommand, PlacementControlEvent, PlacementControlEventKind,
        control_stream_id, encode_control_command_for_term,
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
    shared_coordinator_term: Arc<AtomicU64>,
    hello_pending: bool,
    origin: Instant,
    drain_confirmation: crate::drain::DrainConfirmation,
}

#[derive(Clone)]
pub struct MembershipCoordinatorHandle {
    local_node_id: String,
    local_incarnation: NodeIncarnation,
    coordinator: AssociationKey,
    associations: Arc<AssociationManager>,
    maximum_control_payload: usize,
    coordinator_term: Arc<AtomicU64>,
    drain_acknowledgement_timeout: Duration,
    drain_confirmation: crate::drain::DrainConfirmation,
}

impl MembershipCoordinatorHandle {
    pub async fn complete_drain(&self, operation_id: String) -> Result<(), LogicSessionError> {
        self.complete_drain_until(
            operation_id,
            Instant::now() + self.drain_acknowledgement_timeout,
        )
        .await
    }

    pub async fn complete_drain_until(
        &self,
        operation_id: String,
        deadline: Instant,
    ) -> Result<(), LogicSessionError> {
        if Instant::now() >= deadline {
            return Err(LogicSessionError::DrainNotAcknowledged);
        }
        if self.drain_confirmation.committed_operation().as_deref() == Some(&operation_id) {
            return Ok(());
        }
        let term = self.coordinator_term.load(Ordering::Acquire);
        self.drain_confirmation.request(&operation_id, term)?;
        let association = self
            .associations
            .get(&self.coordinator)
            .ok_or(LogicSessionError::AssociationUnavailable)?;
        tokio::time::timeout_at(
            deadline,
            association.admit_control_command_in_wait(
                control_stream_id(&CoordinatorScope::Membership),
                encode_control_command_for_term(
                    &CoordinatorScope::Membership,
                    term,
                    &PlacementControlCommand::MembershipDrainComplete {
                        operation_id: operation_id.clone(),
                        node_id: self.local_node_id.clone(),
                        expected_incarnation: self.local_incarnation,
                    },
                    self.maximum_control_payload,
                )
                .map_err(LogicSessionError::Control)?,
                super::session::CONTROL_ADMISSION_TIMEOUT,
            ),
        )
        .await
        .map_err(|_| LogicSessionError::DrainNotAcknowledged)??;
        self.drain_confirmation
            .wait(&operation_id, term, deadline)
            .await
    }

    /// An authenticated completion remains valid after the session carrying it closes.
    pub fn committed_drain(&self) -> Option<String> {
        self.drain_confirmation.committed_operation()
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
        config.validate()?;
        if effect_capacity == 0
            || coordinator_term == 0
            || hello.node.incarnation != coordinator.local_incarnation
            || hello.node.address == coordinator.remote_address
        {
            return Err(LogicSessionError::InvalidConfig);
        }
        let (effects, receiver) = mpsc::channel(effect_capacity);
        let shared_coordinator_term = Arc::new(AtomicU64::new(coordinator_term));
        let drain_confirmation = crate::drain::DrainConfirmation::default();
        let handle = MembershipCoordinatorHandle {
            local_node_id: hello.node.node_id.clone(),
            local_incarnation: hello.node.incarnation,
            coordinator: coordinator.clone(),
            associations: associations.clone(),
            maximum_control_payload: config.maximum_control_payload,
            coordinator_term: shared_coordinator_term.clone(),
            drain_acknowledgement_timeout: config.drain_acknowledgement_timeout,
            drain_confirmation: drain_confirmation.clone(),
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
                shared_coordinator_term,
                hello_pending: true,
                origin: Instant::now(),
                drain_confirmation,
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
        self.drain_confirmation.close();
        (result, controls)
    }

    async fn run_loop(
        &mut self,
        controls: &mut mpsc::Receiver<PlacementControlEvent>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<(), LogicSessionError> {
        self.send(PlacementControlCommand::MemberHello(self.hello.clone()))
            .await?;
        let mut heartbeat = tokio::time::interval(self.config.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        heartbeat.reset();
        let mut last_heartbeat = Instant::now();
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
                    // A paused process may resume on an active TCP association after its
                    // application session expired. Re-bootstrap instead of trusting that socket.
                    if last_heartbeat.elapsed() > self.config.heartbeat_interval.saturating_mul(2) {
                        return Err(LogicSessionError::HeartbeatInterrupted);
                    }
                    last_heartbeat = Instant::now();
                    if self.drain_confirmation.requested() {
                        continue;
                    }
                    if self.hello_pending {
                        // The initial hello may race Coordinator election or membership
                        // recovery. Retry until a membership snapshot proves it was accepted.
                        self.send(PlacementControlCommand::MemberHello(self.hello.clone()))
                            .await?;
                        continue;
                    }
                    self.heartbeat_sequence = self
                        .heartbeat_sequence
                        .checked_add(1)
                        .ok_or(LogicSessionError::HeartbeatSequenceExhausted)?;
                    self.send(PlacementControlCommand::NodeHeartbeat {
                        incarnation: self.hello.node.incarnation,
                        sequence: self.heartbeat_sequence,
                    }).await?;
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
                if self.drain_confirmation.committed_operation().is_some() {
                    return Ok(());
                }
                self.state
                    .lock()
                    .expect("membership session state poisoned")
                    .session = MembershipState::default();
                self.hello_pending = true;
                self.send(PlacementControlCommand::MemberHello(self.hello.clone()))
                    .await
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
                        if !matches!(begin.version, SnapshotVersion::Membership(_)) {
                            return Err(LogicSessionError::UnauthorizedCommand);
                        }
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
                            .send(LogicPlacementEffect::MemberSnapshot { version, members })
                            .await
                            .map_err(|_| LogicSessionError::EffectBackpressure)?;
                        changed.notify_waiters();
                        self.send(PlacementControlCommand::JoinReady {
                            snapshot_version: version,
                        })
                        .await
                    }
                    PlacementControlCommand::MemberDelta(event) => {
                        self.apply_member_event(event).await
                    }
                    PlacementControlCommand::DrainCommitted {
                        operation_id,
                        expected_incarnation,
                    } => {
                        if expected_incarnation != self.hello.node.incarnation {
                            return Err(LogicSessionError::UnauthorizedCommand);
                        }
                        self.drain_confirmation
                            .confirm(&operation_id, self.coordinator_term)
                    }
                    _ => Err(LogicSessionError::UnauthorizedCommand),
                }
            }
        }
    }

    async fn apply_member_event(&self, event: MemberEvent) -> Result<(), LogicSessionError> {
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
            .send(LogicPlacementEffect::MemberEvent(Box::new(event)))
            .await
            .map_err(|_| LogicSessionError::EffectBackpressure)?;
        changed.notify_waiters();
        Ok(())
    }

    async fn send(&self, command: PlacementControlCommand) -> Result<(), LogicSessionError> {
        let association = self
            .associations
            .get(&self.coordinator)
            .ok_or(LogicSessionError::AssociationUnavailable)?;
        if association.state() == AssociationState::Closed {
            return Err(LogicSessionError::AssociationUnavailable);
        }
        let payload = encode_control_command_for_term(
            &CoordinatorScope::Membership,
            self.coordinator_term,
            &command,
            self.config.maximum_control_payload,
        )
        .map_err(LogicSessionError::Control)?;
        association
            .admit_control_command_in_wait(
                control_stream_id(&CoordinatorScope::Membership),
                payload,
                super::session::CONTROL_ADMISSION_TIMEOUT,
            )
            .await?;
        Ok(())
    }

    fn require_coordinator(&self, association: &AssociationKey) -> Result<(), LogicSessionError> {
        if association == &self.coordinator {
            Ok(())
        } else {
            Err(LogicSessionError::UnauthorizedCommand)
        }
    }

    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_millis(
            u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX),
        )
    }

    fn require_coordinator_term(&self, term: Option<u64>) -> Result<(), LogicSessionError> {
        if term == Some(self.coordinator_term) {
            Ok(())
        } else {
            Err(LogicSessionError::StaleGeneration)
        }
    }

    fn accept_snapshot_term(&mut self, term: Option<u64>) -> Result<(), LogicSessionError> {
        let Some(term) = term else {
            return Err(LogicSessionError::StaleGeneration);
        };
        if term < self.coordinator_term {
            return Err(LogicSessionError::StaleGeneration);
        }
        self.coordinator_term = term;
        self.shared_coordinator_term.store(term, Ordering::Release);
        Ok(())
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
        _ => ControlDispatchError::RetryLater(
            lattice_remoting::control::ControlRetryReason::AssociationStarting,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        control::InboundPlacementControl,
        coordinator::build_snapshot,
        types::{CoordinatorTerm, MembershipVersion, Revision},
    };
    use lattice_core::{
        actor_address::{ClusterId, NodeAddress},
        release::ReleaseManifest,
    };
    use lattice_remoting::{config::RemotingConfig, control::CommandId};

    #[tokio::test(start_paused = true)]
    async fn paused_sessions_require_registration_even_when_the_association_stays_active() {
        use lattice_remoting::association::{LaneAttachment, LaneKind};
        for placement in [false, true] {
            let local = NodeKey {
                node_id: "paused".to_owned(),
                address: NodeAddress::new("127.0.0.1", 34611).unwrap(),
                incarnation: NodeIncarnation::new(1).unwrap(),
            };
            let associations = Arc::new(
                AssociationManager::new(
                    local.address.clone(),
                    local.incarnation,
                    RemotingConfig::default(),
                )
                .unwrap(),
            );
            let association = associations
                .get_or_create(
                    ClusterId::new("paused-session").unwrap(),
                    NodeAddress::new("127.0.0.1", 34612).unwrap(),
                    NodeIncarnation::new(2).unwrap(),
                )
                .unwrap();
            for (lane, nonce) in [
                (LaneKind::Control, 1),
                (LaneKind::Interactive, 2),
                (LaneKind::Bulk(0), 3),
            ] {
                association
                    .attach(LaneAttachment {
                        association_id: association.id(),
                        key: association.key().clone(),
                        lane,
                        connection_nonce: nonce,
                    })
                    .unwrap();
            }
            let config = LogicCoordinatorConfig::default();
            let pause = config.heartbeat_interval.saturating_mul(3);
            let (_controls_tx, controls) = mpsc::channel(8);
            let (_shutdown_tx, shutdown) = watch::channel(false);
            let mut run: std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<(), LogicSessionError>>>,
            > = if placement {
                let (session, _effects) = crate::session::PlacementDomainSession::new(
                    crate::coordinator::PlacementDomainHello::builder(
                        local,
                        lattice_core::actor_address::PlacementDomainId::new("paused").unwrap(),
                        1,
                    )
                    .build(),
                    association.key().clone(),
                    associations,
                    config,
                    8,
                    1,
                )
                .unwrap();
                Box::pin(session.run(controls, shutdown))
            } else {
                let hello = MemberHello {
                    node: local,
                    release: ReleaseManifest::development(1),
                    rollout_participant: true,
                    roles: Default::default(),
                    failure_domains: Default::default(),
                    protocols: Vec::new(),
                    remoting_capabilities: Default::default(),
                };
                let (session, _, _effects) = MembershipSession::new(
                    hello,
                    association.key().clone(),
                    associations,
                    config,
                    8,
                    1,
                )
                .unwrap();
                Box::pin(async move { session.run_recoverable(controls, shutdown).await.0 })
            };
            let mut context = std::task::Context::from_waker(std::task::Waker::noop());
            assert!(run.as_mut().poll(&mut context).is_pending());
            tokio::time::advance(pause).await;
            assert_eq!(association.state(), AssociationState::Active);
            assert!(matches!(
                run.as_mut().poll(&mut context),
                std::task::Poll::Ready(Err(LogicSessionError::HeartbeatInterrupted))
            ));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn membership_snapshot_staging_expires_on_the_real_monotonic_clock() {
        let local = NodeKey {
            node_id: "joining".to_owned(),
            address: NodeAddress::new("127.0.0.1", 34601).unwrap(),
            incarnation: NodeIncarnation::new(1).unwrap(),
        };
        let associations = Arc::new(
            AssociationManager::new(
                local.address.clone(),
                local.incarnation,
                RemotingConfig::default(),
            )
            .unwrap(),
        );
        let association = associations
            .get_or_create(
                ClusterId::new("snapshot-timeout").unwrap(),
                NodeAddress::new("127.0.0.1", 34602).unwrap(),
                NodeIncarnation::new(2).unwrap(),
            )
            .unwrap();
        let hello = MemberHello {
            node: local,
            release: ReleaseManifest::development(1),
            rollout_participant: true,
            roles: Default::default(),
            failure_domains: Default::default(),
            protocols: Vec::new(),
            remoting_capabilities: Default::default(),
        };
        let config = LogicCoordinatorConfig::default();
        let (begin, _, end) = build_snapshot(
            &CoordinatorScope::Membership,
            1,
            config.maximum_control_payload,
            SnapshotVersion::Membership(MembershipVersion::new(
                CoordinatorTerm::new(1).unwrap(),
                Revision::new(1).unwrap(),
            )),
            Vec::new(),
            &config.snapshot_limits,
        )
        .unwrap();
        let timeout = Duration::from_millis(config.snapshot_limits.staging_timeout_millis);
        let (mut session, _, _effects) =
            MembershipSession::new(hello, association.key().clone(), associations, config, 8, 1)
                .unwrap();
        let event = |command| {
            PlacementControlEventKind::Command(Box::new(InboundPlacementControl {
                association: association.key().clone(),
                scope: CoordinatorScope::Membership,
                coordinator_term: Some(1),
                command_id: CommandId::generate(),
                command,
            }))
        };
        session
            .handle(event(PlacementControlCommand::SnapshotBegin(begin)))
            .await
            .unwrap();
        tokio::time::advance(timeout + Duration::from_millis(1)).await;
        assert!(matches!(
            session
                .handle(event(PlacementControlCommand::SnapshotEnd(end)))
                .await,
            Err(LogicSessionError::Coordinator(
                crate::coordinator::CoordinatorError::SnapshotIntegrity
            ))
        ));
    }
}
