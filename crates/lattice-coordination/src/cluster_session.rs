use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{
    drain::DrainConfirmation,
    shutdown::{ShutdownReport, ShutdownRequestRejection},
};
use lattice_model::{
    cluster::CoordinatorScope,
    cluster::NodeIncarnation,
    run::{ClusterLifecycle, ControlOperationId, RunEpoch, RunPhase},
};
use lattice_remoting::{
    association::{AssociationKey, AssociationManager, AssociationState},
    control::ControlDispatchError,
};
use tokio::{
    sync::{Notify, mpsc, oneshot, watch},
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
    session::{GroupSessionConfig, GroupSessionError, LogicPlacementEffect},
    types::{MonotonicTime, NodeKey},
};

pub struct ClusterSessionState {
    session: MembershipState,
    local_node: NodeKey,
    changed: Arc<Notify>,
}

impl ClusterSessionState {
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

type PendingShutdownRequests = Arc<
    Mutex<
        BTreeMap<
            ControlOperationId,
            Option<oneshot::Sender<Result<ClusterLifecycle, ShutdownRequestRejection>>>,
        >,
    >,
>;

#[derive(Debug, thiserror::Error)]
pub enum ShutdownRequestError {
    #[error("shutdown request transport failed: {0}")]
    Transport(#[from] GroupSessionError),
    #[error(transparent)]
    Rejected(ShutdownRequestRejection),
    #[error(
        "shutdown acceptance wait timed out; the request may still commit; retry the same operation"
    )]
    Timeout,
    #[error("shutdown acceptance session ended; retry the same operation")]
    Interrupted,
    #[error("shutdown request is already pending or the pending-request limit is reached")]
    PendingLimit,
}

struct ShutdownRequestWaiter {
    operation: ControlOperationId,
    pending: PendingShutdownRequests,
}
impl Drop for ShutdownRequestWaiter {
    fn drop(&mut self) {
        self.pending
            .lock()
            .expect("shutdown requests poisoned")
            .remove(&self.operation);
    }
}

pub struct ClusterSession {
    shutdown_requests: PendingShutdownRequests,
    shared_epoch: Arc<AtomicU64>,
    stop_confirmation: DrainConfirmation,
    hello: MemberHello,
    coordinator: AssociationKey,
    associations: Arc<AssociationManager>,
    config: GroupSessionConfig,
    state: Arc<Mutex<ClusterSessionState>>,
    stager: Option<SnapshotStager>,
    effects: mpsc::Sender<LogicPlacementEffect>,
    heartbeat_sequence: u64,
    acknowledged_sequence: u64,
    last_acknowledgement: Instant,
    closing: bool,
    coordinator_term: u64,
    shared_coordinator_term: Arc<AtomicU64>,
    hello_pending: bool,
    origin: Instant,
    drain_confirmation: DrainConfirmation,
}

#[derive(Clone)]
pub struct ClusterSessionHandle {
    shutdown_requests: PendingShutdownRequests,
    shared_epoch: Arc<AtomicU64>,
    stop_confirmation: DrainConfirmation,
    local_node_id: String,
    local_incarnation: NodeIncarnation,
    coordinator: AssociationKey,
    associations: Arc<AssociationManager>,
    maximum_control_payload: usize,
    coordinator_term: Arc<AtomicU64>,
    drain_acknowledgement_timeout: Duration,
    drain_confirmation: DrainConfirmation,
}

impl Drop for ClusterSession {
    fn drop(&mut self) {
        self.drain_confirmation.close();
        self.stop_confirmation.close();
        self.shutdown_requests
            .lock()
            .expect("shutdown requests poisoned")
            .clear();
    }
}

impl ClusterSessionHandle {
    pub async fn request_cluster_shutdown(
        &self,
        operation: ControlOperationId,
    ) -> Result<ClusterLifecycle, ShutdownRequestError> {
        let epoch = RunEpoch::new(self.shared_epoch.load(Ordering::Acquire))
            .ok_or(GroupSessionError::StaleGeneration)?;
        let association = self
            .associations
            .get(&self.coordinator)
            .ok_or(GroupSessionError::AssociationUnavailable)?;
        let payload = encode_control_command_for_term(
            &CoordinatorScope::Cluster,
            self.coordinator_term.load(Ordering::Acquire),
            &PlacementControlCommand::RequestClusterShutdown {
                epoch,
                operation: operation.clone(),
            },
            self.maximum_control_payload,
        )
        .map_err(GroupSessionError::Control)?;
        let (sender, response) = oneshot::channel();
        {
            let mut pending = self
                .shutdown_requests
                .lock()
                .expect("shutdown requests poisoned");
            if pending.contains_key(&operation) || pending.len() >= 32 {
                return Err(ShutdownRequestError::PendingLimit);
            }
            pending.insert(operation.clone(), Some(sender));
        }
        let _waiter = ShutdownRequestWaiter {
            operation,
            pending: self.shutdown_requests.clone(),
        };
        association
            .admit_control_command_in_wait(
                control_stream_id(&CoordinatorScope::Cluster),
                payload,
                super::session::CONTROL_ADMISSION_TIMEOUT,
            )
            .await
            .map_err(GroupSessionError::from)?;
        let accepted = tokio::time::timeout(self.drain_acknowledgement_timeout, response)
            .await
            .map_err(|_| ShutdownRequestError::Timeout)?
            .map_err(|_| ShutdownRequestError::Interrupted)?
            .map_err(ShutdownRequestError::Rejected)?;
        if accepted.epoch != epoch
            || !matches!(
                accepted.phase,
                RunPhase::Closing { .. } | RunPhase::Closed { .. }
            )
        {
            return Err(GroupSessionError::StaleGeneration.into());
        }
        Ok(accepted)
    }
    /// Confirms positive post-drain evidence before ordinary node teardown.
    pub async fn confirm_node_stopped(&self, node: NodeKey) -> Result<(), GroupSessionError> {
        let epoch = RunEpoch::new(self.shared_epoch.load(Ordering::Acquire))
            .ok_or(GroupSessionError::StaleGeneration)?;
        if node.node_id != self.local_node_id || node.incarnation != self.local_incarnation {
            return Err(GroupSessionError::UnauthorizedCommand);
        }
        let operation = format!("stopped-{}", node.incarnation.get());
        let term = self.coordinator_term.load(Ordering::Acquire);
        self.stop_confirmation.request(&operation, term)?;
        let association = self
            .associations
            .get(&self.coordinator)
            .ok_or(GroupSessionError::AssociationUnavailable)?;
        let payload = encode_control_command_for_term(
            &CoordinatorScope::Cluster,
            term,
            &PlacementControlCommand::NodeStopCompleted { epoch, node },
            self.maximum_control_payload,
        )
        .map_err(GroupSessionError::Control)?;
        association
            .admit_control_command_in_wait(
                control_stream_id(&CoordinatorScope::Cluster),
                payload,
                super::session::CONTROL_ADMISSION_TIMEOUT,
            )
            .await?;
        self.stop_confirmation
            .wait(
                &operation,
                term,
                Instant::now() + self.drain_acknowledgement_timeout,
            )
            .await
    }
    pub async fn report_cluster_stop(
        &self,
        report: ShutdownReport,
    ) -> Result<(), GroupSessionError> {
        if report.node.node_id != self.local_node_id
            || report.node.incarnation != self.local_incarnation
        {
            return Err(GroupSessionError::UnauthorizedCommand);
        }
        let association = self
            .associations
            .get(&self.coordinator)
            .ok_or(GroupSessionError::AssociationUnavailable)?;
        let payload = encode_control_command_for_term(
            &CoordinatorScope::Cluster,
            self.coordinator_term.load(Ordering::Acquire),
            &PlacementControlCommand::ClusterStopReport {
                epoch: report.epoch,
                operation: report.operation,
                node: report.node,
                outcome: report.outcome,
            },
            self.maximum_control_payload,
        )
        .map_err(GroupSessionError::Control)?;
        association
            .admit_control_command_in_wait(
                control_stream_id(&CoordinatorScope::Cluster),
                payload,
                super::session::CONTROL_ADMISSION_TIMEOUT,
            )
            .await?;
        Ok(())
    }
    pub async fn complete_drain(&self, operation_id: String) -> Result<(), GroupSessionError> {
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
    ) -> Result<(), GroupSessionError> {
        if Instant::now() >= deadline {
            return Err(GroupSessionError::DrainNotAcknowledged);
        }
        if self.drain_confirmation.committed_operation().as_deref() == Some(&operation_id) {
            return Ok(());
        }
        let term = self.coordinator_term.load(Ordering::Acquire);
        self.drain_confirmation.request(&operation_id, term)?;
        let association = self
            .associations
            .get(&self.coordinator)
            .ok_or(GroupSessionError::AssociationUnavailable)?;
        tokio::time::timeout_at(
            deadline,
            association.admit_control_command_in_wait(
                control_stream_id(&CoordinatorScope::Cluster),
                encode_control_command_for_term(
                    &CoordinatorScope::Cluster,
                    term,
                    &PlacementControlCommand::MembershipDrainComplete {
                        operation_id: operation_id.clone(),
                        node_id: self.local_node_id.clone(),
                        expected_incarnation: self.local_incarnation,
                    },
                    self.maximum_control_payload,
                )
                .map_err(GroupSessionError::Control)?,
                super::session::CONTROL_ADMISSION_TIMEOUT,
            ),
        )
        .await
        .map_err(|_| GroupSessionError::DrainNotAcknowledged)??;
        self.drain_confirmation
            .wait(&operation_id, term, deadline)
            .await
    }

    /// An authenticated completion remains valid after the session carrying it closes.
    pub fn committed_drain(&self) -> Option<String> {
        self.drain_confirmation.committed_operation()
    }
}

impl ClusterSession {
    pub fn new(
        hello: MemberHello,
        coordinator: AssociationKey,
        associations: Arc<AssociationManager>,
        config: GroupSessionConfig,
        effect_capacity: usize,
        coordinator_term: u64,
    ) -> Result<
        (
            Self,
            ClusterSessionHandle,
            mpsc::Receiver<LogicPlacementEffect>,
        ),
        GroupSessionError,
    > {
        config.validate()?;
        if effect_capacity == 0
            || coordinator_term == 0
            || hello.node.incarnation != coordinator.local_incarnation
            || hello.node.address == coordinator.remote_address
        {
            return Err(GroupSessionError::InvalidConfig);
        }
        let (effects, receiver) = mpsc::channel(effect_capacity);
        let shared_coordinator_term = Arc::new(AtomicU64::new(coordinator_term));
        let drain_confirmation = DrainConfirmation::default();
        let stop_confirmation = DrainConfirmation::default();
        let shared_epoch = Arc::new(AtomicU64::new(0));
        let shutdown_requests = Arc::new(Mutex::new(BTreeMap::new()));
        let handle = ClusterSessionHandle {
            shutdown_requests: shutdown_requests.clone(),
            shared_epoch: shared_epoch.clone(),
            stop_confirmation: stop_confirmation.clone(),
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
                shutdown_requests,
                shared_epoch,
                stop_confirmation,
                hello,
                coordinator,
                associations,
                config,
                state: Arc::new(Mutex::new(ClusterSessionState {
                    session: MembershipState::default(),
                    local_node,
                    changed: Arc::new(Notify::new()),
                })),
                stager: None,
                effects,
                heartbeat_sequence: 0,
                acknowledged_sequence: 0,
                last_acknowledgement: Instant::now(),
                closing: false,
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

    pub fn state(&self) -> Arc<Mutex<ClusterSessionState>> {
        self.state.clone()
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
        self.stop_confirmation.close();
        self.shutdown_requests
            .lock()
            .expect("shutdown requests poisoned")
            .clear();
        (result, controls)
    }

    async fn run_loop(
        &mut self,
        controls: &mut mpsc::Receiver<PlacementControlEvent>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<(), GroupSessionError> {
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
                        return Err(GroupSessionError::ControlClosed);
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
                        return Err(GroupSessionError::HeartbeatInterrupted);
                    }
                    last_heartbeat = Instant::now();
                    if self.closing || self.drain_confirmation.requested() {
                        continue;
                    }
                    if !self.hello_pending && self.last_acknowledgement.elapsed() > self.config.heartbeat_interval.saturating_mul(3) {
                        return Err(GroupSessionError::HeartbeatInterrupted);
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
                        .ok_or(GroupSessionError::HeartbeatSequenceExhausted)?;
                    self.send(PlacementControlCommand::NodeHeartbeat {
                        incarnation: self.hello.node.incarnation,
                        sequence: self.heartbeat_sequence,
                    }).await?;
                }
            }
        }
    }

    async fn handle(&mut self, event: PlacementControlEventKind) -> Result<(), GroupSessionError> {
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
                    .expect("membership session state poisoned")
                    .session = MembershipState::default();
                self.hello_pending = true;
                self.send(PlacementControlCommand::MemberHello(self.hello.clone()))
                    .await
            }
            PlacementControlEventKind::Command(inbound) => {
                self.require_coordinator(&inbound.association)?;
                if matches!(
                    &inbound.command,
                    PlacementControlCommand::SnapshotBegin(_)
                        | PlacementControlCommand::ClusterRunning { .. }
                        | PlacementControlCommand::ClusterShutdownResult { .. }
                        | PlacementControlCommand::ClusterClosing { .. }
                        | PlacementControlCommand::ClusterClosed { .. }
                ) {
                    self.accept_snapshot_term(inbound.coordinator_term)?;
                } else {
                    self.require_coordinator_term(inbound.coordinator_term)?;
                }
                match inbound.command {
                    PlacementControlCommand::ClusterShutdownResult { request, outcome } => {
                        if let Some(waiter) = self
                            .shutdown_requests
                            .lock()
                            .expect("shutdown requests poisoned")
                            .get_mut(&request)
                            .and_then(Option::take)
                        {
                            let _ = waiter.send(outcome);
                        }
                        Ok(())
                    }
                    PlacementControlCommand::ClusterRunning { epoch } => {
                        let previous = self.shared_epoch.load(Ordering::Acquire);
                        if previous != 0 && previous != epoch.get() {
                            return Err(GroupSessionError::StaleGeneration);
                        }
                        self.shared_epoch.store(epoch.get(), Ordering::Release);
                        Ok(())
                    }
                    PlacementControlCommand::NodeHeartbeatAck { sequence } => {
                        if sequence > self.heartbeat_sequence || sequence == 0 {
                            return Err(GroupSessionError::UnauthorizedCommand);
                        }
                        if sequence > self.acknowledged_sequence {
                            self.acknowledged_sequence = sequence;
                            self.last_acknowledgement = Instant::now();
                        }
                        Ok(())
                    }
                    PlacementControlCommand::NodeStopConfirmed { epoch, node } => {
                        if epoch.get() != self.shared_epoch.load(Ordering::Acquire)
                            || node != self.hello.node
                        {
                            return Err(GroupSessionError::StaleGeneration);
                        }
                        self.stop_confirmation.confirm(
                            &format!("stopped-{}", node.incarnation.get()),
                            self.coordinator_term,
                        )
                    }
                    PlacementControlCommand::SnapshotBegin(begin) => {
                        if !matches!(begin.version, SnapshotVersion::Membership(_)) {
                            return Err(GroupSessionError::UnauthorizedCommand);
                        }
                        self.hello_pending = false;
                        self.last_acknowledgement = Instant::now();
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
                        let SnapshotVersion::Membership(version) = install.version.clone() else {
                            return Err(GroupSessionError::UnauthorizedCommand);
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
                                .map_err(GroupSessionError::MembershipState)?;
                            state.changed.clone()
                        };
                        self.effects
                            .send(LogicPlacementEffect::MemberSnapshot { version, members })
                            .await
                            .map_err(|_| GroupSessionError::EffectBackpressure)?;
                        changed.notify_waiters();
                        self.send(PlacementControlCommand::JoinReady {
                            snapshot_version: version,
                        })
                        .await
                    }
                    PlacementControlCommand::MemberDelta(event) => {
                        self.apply_member_event(event).await
                    }
                    PlacementControlCommand::ClusterClosing { epoch, operation } => {
                        self.closing = true;
                        let previous = self.shared_epoch.load(Ordering::Acquire);
                        if previous != 0 && previous != epoch.get() {
                            return Err(GroupSessionError::StaleGeneration);
                        }
                        self.shared_epoch.store(epoch.get(), Ordering::Release);
                        self.effects
                            .send(LogicPlacementEffect::ClusterClosing { epoch, operation })
                            .await
                            .map_err(|_| GroupSessionError::EffectBackpressure)
                    }
                    PlacementControlCommand::ClusterClosed { lifecycle } => {
                        let previous = self.shared_epoch.load(Ordering::Acquire);
                        if previous != 0 && previous != lifecycle.epoch.get() {
                            return Err(GroupSessionError::StaleGeneration);
                        }
                        self.shared_epoch
                            .store(lifecycle.epoch.get(), Ordering::Release);
                        self.effects
                            .send(LogicPlacementEffect::ClusterClosed(lifecycle))
                            .await
                            .map_err(|_| GroupSessionError::EffectBackpressure)
                    }
                    PlacementControlCommand::DrainCommitted {
                        operation_id,
                        expected_incarnation,
                    } => {
                        if expected_incarnation != self.hello.node.incarnation {
                            return Err(GroupSessionError::UnauthorizedCommand);
                        }
                        self.drain_confirmation
                            .confirm(&operation_id, self.coordinator_term)
                    }
                    _ => Err(GroupSessionError::UnauthorizedCommand),
                }
            }
        }
    }

    async fn apply_member_event(&self, event: MemberEvent) -> Result<(), GroupSessionError> {
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
                .map_err(GroupSessionError::MembershipState)?;
            state.changed.clone()
        };
        self.effects
            .send(LogicPlacementEffect::MemberEvent(Box::new(event)))
            .await
            .map_err(|_| GroupSessionError::EffectBackpressure)?;
        changed.notify_waiters();
        Ok(())
    }

    async fn send(&self, command: PlacementControlCommand) -> Result<(), GroupSessionError> {
        let association = self
            .associations
            .get(&self.coordinator)
            .ok_or(GroupSessionError::AssociationUnavailable)?;
        if association.state() == AssociationState::Closed {
            return Err(GroupSessionError::AssociationUnavailable);
        }
        let payload = encode_control_command_for_term(
            &CoordinatorScope::Cluster,
            self.coordinator_term,
            &command,
            self.config.maximum_control_payload,
        )
        .map_err(GroupSessionError::Control)?;
        association
            .admit_control_command_in_wait(
                control_stream_id(&CoordinatorScope::Cluster),
                payload,
                super::session::CONTROL_ADMISSION_TIMEOUT,
            )
            .await?;
        Ok(())
    }

    fn require_coordinator(&self, association: &AssociationKey) -> Result<(), GroupSessionError> {
        if association == &self.coordinator {
            Ok(())
        } else {
            Err(GroupSessionError::UnauthorizedCommand)
        }
    }

    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_millis(
            u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX),
        )
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
        Ok(())
    }
}

fn decode_members(records: &[SnapshotRecord]) -> Result<Vec<MemberRecord>, GroupSessionError> {
    let mut members = BTreeMap::new();
    for record in records {
        if !record.key.starts_with("member/") {
            continue;
        }
        let member: MemberRecord =
            serde_json::from_slice(&record.value).map_err(|_| GroupSessionError::Codec)?;
        if member.node.validate().is_err()
            || members
                .insert(
                    (member.node.node_id.clone(), member.node.incarnation),
                    member,
                )
                .is_some()
        {
            return Err(GroupSessionError::Codec);
        }
    }
    Ok(members.into_values().collect())
}

fn membership_dispatch_error(error: &GroupSessionError) -> ControlDispatchError {
    match error {
        GroupSessionError::UnauthorizedCommand
        | GroupSessionError::Codec
        | GroupSessionError::SnapshotRequired
        | GroupSessionError::StaleGeneration
        | GroupSessionError::Coordinator(_)
        | GroupSessionError::MembershipState(_) => ControlDispatchError::InvalidCommand,
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
    use lattice_model::cluster::{ClusterId, NodeEndpoint};
    use lattice_remoting::{config::RemotingConfig, control::CommandId};

    #[tokio::test(start_paused = true)]
    async fn paused_sessions_require_registration_even_when_the_association_stays_active() {
        use lattice_remoting::association::{LaneAttachment, LaneKind};
        for placement in [false, true] {
            let local = NodeKey {
                node_id: "paused".to_owned(),
                address: NodeEndpoint::new("127.0.0.1", 34611).unwrap(),
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
                    NodeEndpoint::new("127.0.0.1", 34612).unwrap(),
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
            let config = GroupSessionConfig::default();
            let pause = config.heartbeat_interval.saturating_mul(3);
            let (_controls_tx, controls) = mpsc::channel(8);
            let (_shutdown_tx, shutdown) = watch::channel(false);
            let mut run: std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<(), GroupSessionError>>>,
            > = if placement {
                let (session, _effects) = crate::session::GroupSession::new(
                    crate::coordinator::ActorGroupHello::builder(
                        local,
                        lattice_model::cluster::ActorGroupId::new("paused").unwrap(),
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
                    roles: Default::default(),
                    failure_domains: Default::default(),
                    protocols: Vec::new(),
                    remoting_capabilities: Default::default(),
                };
                let (session, _, _effects) = ClusterSession::new(
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
                std::task::Poll::Ready(Err(GroupSessionError::HeartbeatInterrupted))
            ));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn membership_snapshot_staging_expires_on_the_real_monotonic_clock() {
        let local = NodeKey {
            node_id: "joining".to_owned(),
            address: NodeEndpoint::new("127.0.0.1", 34601).unwrap(),
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
                NodeEndpoint::new("127.0.0.1", 34602).unwrap(),
                NodeIncarnation::new(2).unwrap(),
            )
            .unwrap();
        let hello = MemberHello {
            node: local,
            roles: Default::default(),
            failure_domains: Default::default(),
            protocols: Vec::new(),
            remoting_capabilities: Default::default(),
        };
        let config = GroupSessionConfig::default();
        let (begin, _, end) = build_snapshot(
            &CoordinatorScope::Cluster,
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
            ClusterSession::new(hello, association.key().clone(), associations, config, 8, 1)
                .unwrap();
        let event = |command| {
            PlacementControlEventKind::Command(Box::new(InboundPlacementControl {
                association: association.key().clone(),
                scope: CoordinatorScope::Cluster,
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
            Err(GroupSessionError::Coordinator(
                crate::coordinator::CoordinatorError::SnapshotIntegrity
            ))
        ));
    }
}
