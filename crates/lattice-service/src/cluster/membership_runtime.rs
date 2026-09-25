use lattice_model::run::{ClosingStage, ClusterLifecycle, RunPhase};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use lattice_coordination::{
    cluster_session::{ClusterSession, ClusterSessionHandle, ClusterSessionState},
    control::PlacementControlEvent,
    coordinator::{MemberChange, MemberEvent, MemberHello},
    session::{GroupSessionConfig, LogicPlacementEffect},
};
use lattice_remoting::{
    association::AssociationManager, bootstrap::BootstrapLeader, watch::WatchRegistry,
};
use tokio::sync::{mpsc, watch};

use crate::shutdown::ClusterStopHook;
use lattice_actor_distributed::host::ProtocolHostRegistry;
use lattice_coordination::shutdown::{NodeStopOutcome, ShutdownReport};

use super::{
    join::{BootstrapView, JoinController, JoinEvent},
    peers::PeerReconciler,
};
use crate::lifecycle::{
    ActorGroupState, NodeLifecycle, NodeLifecycleState, ProductionLifecycleDriver,
    ServiceHealthSnapshot, ServiceLifecycleEvent,
};

pub(crate) struct MembershipJoinRuntime {
    pub cluster_run: watch::Sender<Option<ClusterLifecycle>>,
    pub stop_result: Mutex<Option<ShutdownReport>>,
    pub hosts: Arc<ProtocolHostRegistry>,
    pub stop_hook: Option<Arc<dyn ClusterStopHook>>,
    pub stop_timeout: Duration,
    pub controller: Arc<JoinController>,
    pub hello: MemberHello,
    pub associations: Arc<AssociationManager>,
    pub controls: Option<mpsc::Receiver<PlacementControlEvent>>,
    pub config: GroupSessionConfig,
    pub effect_capacity: usize,
    pub peers: Arc<PeerReconciler>,
    pub watches: Arc<Mutex<WatchRegistry>>,
    pub lifecycle: Arc<Mutex<NodeLifecycle>>,
    pub lifecycle_driver: ProductionLifecycleDriver,
    pub health: Arc<Mutex<ServiceHealthSnapshot>>,
    pub bootstrap_view: Arc<BootstrapView>,
    pub ready: watch::Sender<bool>,
    pub handle: Arc<Mutex<Option<ClusterSessionHandle>>>,
}

struct ClusterSessionRun {
    leader: BootstrapLeader,
    session: ClusterSession,
    state: Arc<Mutex<ClusterSessionState>>,
    controls: mpsc::Receiver<PlacementControlEvent>,
    effects: mpsc::Receiver<LogicPlacementEffect>,
}

struct ClusterSessionReturn {
    controls: mpsc::Receiver<PlacementControlEvent>,
    retry: bool,
}

impl MembershipJoinRuntime {
    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        let terminal_monitor = {
            let controller = self.controller.clone();
            let cluster_run = self.cluster_run.clone();
            let driver = self.lifecycle_driver.clone();
            let mut cancelled = shutdown.clone();
            tokio::spawn(async move {
                loop {
                    let observed = cluster_run.borrow().clone();
                    if let Some(run) = observed {
                        if matches!(run.phase, RunPhase::Closing { .. }) {
                            if let Some(closed) = controller.query_completion(run.epoch).await {
                                cluster_run.send_replace(Some(closed));
                                if driver.state() != NodeLifecycleState::Stopping
                                    && driver.state() != NodeLifecycleState::Terminated
                                {
                                    let _ = driver.transition(ServiceLifecycleEvent::ForceStop);
                                }
                                return;
                            }
                        }
                    }
                    tokio::select! {
                        changed=cancelled.changed()=>{if changed.is_err()||*cancelled.borrow(){return;}}
                        ()=tokio::time::sleep(Duration::from_secs(1))=>{}
                    }
                }
            })
        };
        let (join_events_tx, mut join_events) = mpsc::channel(8);
        let controller = tokio::spawn(
            self.controller
                .clone()
                .run(join_events_tx, shutdown.clone()),
        );
        let mut controls = self.controls.take();
        'joining: while let Some(event) = next_join_event(&mut join_events, &mut shutdown).await {
            match event {
                JoinEvent::Coordinator {
                    leader,
                    association,
                } => {
                    self.bootstrap_view.install(leader.clone());
                    let Some(mut receiver) = controls.take() else {
                        continue;
                    };
                    let mut retry_backoff = Duration::from_millis(100);
                    loop {
                        if association.state()
                            != lattice_remoting::association::AssociationState::Active
                        {
                            controls = Some(receiver);
                            break;
                        }
                        let Ok((session, handle, effects)) = ClusterSession::new(
                            self.hello.clone(),
                            association.key().clone(),
                            self.associations.clone(),
                            self.config.clone(),
                            self.effect_capacity,
                            leader.term,
                        ) else {
                            controls = Some(receiver);
                            break;
                        };
                        *self.handle.lock().expect("membership handle poisoned") =
                            Some(handle.clone());
                        let state = session.state();
                        let session_started = Instant::now();
                        let returned = self
                            .run_session(
                                ClusterSessionRun {
                                    leader: leader.clone(),
                                    session,
                                    state,
                                    controls: receiver,
                                    effects,
                                },
                                &mut join_events,
                                &mut shutdown,
                            )
                            .await;
                        if handle.committed_drain().is_some() {
                            // Retain the authority proof for leave's retry even if the session
                            // closed before its caller observed the response. Rejoining would
                            // recreate the very incarnation whose removal was confirmed.
                            break 'joining;
                        }
                        *self.handle.lock().expect("membership handle poisoned") = None;
                        receiver = returned.controls;
                        if !returned.retry {
                            controls = Some(receiver);
                            break;
                        }
                        if session_started.elapsed() >= self.config.heartbeat_interval {
                            retry_backoff = Duration::from_millis(100);
                        }
                        tokio::select! {
                            changed = shutdown.changed() => {
                                if changed.is_err() || *shutdown.borrow() {
                                    controls = Some(receiver);
                                    break;
                                }
                            }
                            () = tokio::time::sleep(retry_backoff) => {}
                        }
                        retry_backoff = retry_backoff.saturating_mul(2).min(Duration::from_secs(5));
                    }
                }
                JoinEvent::CoordinatorLost { .. } => {
                    self.mark_membership_lost();
                    *self.handle.lock().expect("membership handle poisoned") = None;
                }
                JoinEvent::TerminalFailure(_) => {
                    self.ready.send_replace(false);
                    *self.handle.lock().expect("membership handle poisoned") = None;
                    let _ = self
                        .lifecycle_driver
                        .transition(ServiceLifecycleEvent::StartupFailed);
                    break;
                }
            }
        }
        controller.abort();
        terminal_monitor.abort();
        let _ = terminal_monitor.await;
        {
            let mut handle = self.handle.lock().expect("membership handle poisoned");
            if handle
                .as_ref()
                .is_none_or(|handle| handle.committed_drain().is_none())
            {
                *handle = None;
            }
        }
        let _ = controller.await;
    }

    async fn run_session(
        &self,
        run: ClusterSessionRun,
        join_events: &mut mpsc::Receiver<JoinEvent>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> ClusterSessionReturn {
        let ClusterSessionRun {
            leader,
            session,
            state,
            controls,
            mut effects,
        } = run;
        let (session_shutdown, session_shutdown_rx) = watch::channel(false);
        let mut task = tokio::spawn(session.run_recoverable(controls, session_shutdown_rx));
        let changed = state
            .lock()
            .expect("membership session state poisoned")
            .change_notifier();
        loop {
            if !self
                .cluster_run
                .borrow()
                .as_ref()
                .is_some_and(|run| !run.is_running())
                && state
                    .lock()
                    .expect("membership session state poisoned")
                    .ready()
            {
                self.controller.set_session_healthy(true);
                // Placement sessions re-bootstrap on readiness transitions, not every heartbeat.
                self.ready.send_if_modified(|ready| {
                    let changed = !*ready;
                    *ready = true;
                    changed
                });
                let recovering = self
                    .lifecycle
                    .lock()
                    .expect("service lifecycle poisoned")
                    .recovering_membership();
                let node_state = self.lifecycle_driver.state();
                if node_state == NodeLifecycleState::JoiningMembership
                    && (recovering || self.all_groups_ready())
                {
                    let _ = self
                        .lifecycle_driver
                        .transition(ServiceLifecycleEvent::SnapshotInstalled);
                }
            }
            tokio::select! {
                result = &mut task => {
                    self.mark_membership_lost();
                    return match result {
                        Ok((Ok(()), controls)) => ClusterSessionReturn {
                            controls,
                            retry: false,
                        },
                        Ok((Err(error), controls)) => {
                            let retry = !controls.is_closed();
                            tracing::warn!(
                                target: "lattice.cluster.membership",
                                error = ?error,
                                "membership session stopped; reconciliation required"
                            );
                            ClusterSessionReturn {
                                controls,
                                retry,
                            }
                        }
                        Err(error) => {
                            tracing::warn!(
                                target: "lattice.cluster.membership",
                                %error,
                                "membership session task failed; reconciliation required"
                            );
                            ClusterSessionReturn {
                                controls: closed_controls(),
                                retry: false,
                            }
                        }
                    };
                }
                event = join_events.recv() => {
                    match event {
                        Some(JoinEvent::CoordinatorLost { leader: lost })
                            if lost.identity == leader.identity && lost.term == leader.term =>
                        {
                            self.mark_membership_lost();
                            let _ = session_shutdown.send(true);
                            return ClusterSessionReturn {
                                controls: task.await
                                .map(|(_, controls)| controls)
                                .unwrap_or_else(|_| closed_controls()),
                                retry: false,
                            };
                        }
                        Some(JoinEvent::TerminalFailure(_)) | None => {
                            self.mark_membership_lost();
                            let _ = session_shutdown.send(true);
                            return ClusterSessionReturn {
                                controls: task.await
                                .map(|(_, controls)| controls)
                                .unwrap_or_else(|_| closed_controls()),
                                retry: false,
                            };
                        }
                        Some(JoinEvent::Coordinator { .. })
                        | Some(JoinEvent::CoordinatorLost { .. }) => {}
                    }
                }
                effect = effects.recv() => {
                    let Some(effect) = effect else {
                        self.mark_membership_lost();
                        let _ = session_shutdown.send(true);
                        let controls = task.await
                            .map(|(_, controls)| controls)
                            .unwrap_or_else(|_| closed_controls());
                        let retry = !controls.is_closed();
                        return ClusterSessionReturn {
                            controls,
                            retry,
                        };
                    };
                    if self.apply_effect(effect).await.is_err() {
                        self.mark_membership_lost();
                        let _ = session_shutdown.send(true);
                        let controls = task.await
                            .map(|(_, controls)| controls)
                            .unwrap_or_else(|_| closed_controls());
                        let retry = !controls.is_closed();
                        tracing::warn!(
                            target: "lattice.cluster.membership",
                            retry,
                            "membership session effect failed; reconciliation required"
                        );
                        return ClusterSessionReturn {
                            controls,
                            retry,
                        };
                    }
                }
                _ = changed.notified() => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        let _ = session_shutdown.send(true);
                        return ClusterSessionReturn {
                            controls: task.await
                            .map(|(_, controls)| controls)
                            .unwrap_or_else(|_| closed_controls()),
                            retry: false,
                        };
                    }
                }
            }
        }
    }

    async fn apply_effect(&self, effect: LogicPlacementEffect) -> Result<(), ()> {
        match effect {
            LogicPlacementEffect::ClusterClosing { epoch, operation } => {
                if self.cluster_run.borrow().as_ref().is_some_and(|run| {
                    run.epoch == epoch && matches!(run.phase, RunPhase::Closed { .. })
                }) {
                    return Ok(());
                }
                self.cluster_run.send_replace(Some(ClusterLifecycle {
                    epoch,
                    phase: RunPhase::Closing {
                        operation: operation.clone(),
                        stage: ClosingStage::Draining,
                    },
                }));
                if matches!(
                    self.lifecycle_driver.state(),
                    NodeLifecycleState::JoiningMembership | NodeLifecycleState::Ready
                ) {
                    self.lifecycle_driver
                        .transition(ServiceLifecycleEvent::BeginDrain)
                        .map_err(|_| ())?;
                }
                self.ready.send_replace(false);
                self.hosts.close_business_admission();
                let previous = self
                    .stop_result
                    .lock()
                    .expect("stop result poisoned")
                    .clone()
                    .filter(|report| {
                        report.epoch == epoch
                            && report.operation == operation
                            && report.outcome == NodeStopOutcome::Stopped
                    });
                let stop = async {
                    if let Some(hook) = &self.stop_hook {
                        hook.stop().await?;
                    }
                    let remaining = self.hosts.drain_all().await;
                    if remaining.is_empty() {
                        Ok(())
                    } else {
                        Err(format!(
                            "{} Actor instances require stop intervention",
                            remaining.len()
                        ))
                    }
                };
                let outcome = if previous.is_some() {
                    NodeStopOutcome::Stopped
                } else {
                    match tokio::time::timeout(self.stop_timeout, stop).await {
                        Ok(Ok(())) => NodeStopOutcome::Stopped,
                        Ok(Err(reason)) => NodeStopOutcome::Blocked {
                            reason: reason.chars().take(256).collect(),
                        },
                        Err(_) => NodeStopOutcome::Blocked {
                            reason: "local cluster-stop deadline elapsed; admission remains closed"
                                .to_owned(),
                        },
                    }
                };
                let handle = self
                    .handle
                    .lock()
                    .expect("membership handle poisoned")
                    .clone()
                    .ok_or(())?;
                let report = ShutdownReport {
                    epoch,
                    operation,
                    node: self.hello.node.clone(),
                    outcome,
                };
                *self.stop_result.lock().expect("stop result poisoned") = Some(report.clone());
                handle.report_cluster_stop(report).await.map_err(|_| ())
            }
            LogicPlacementEffect::ClusterClosed(lifecycle) => {
                self.cluster_run.send_replace(Some(lifecycle));
                // Stop reports were sent while reply/control channels remained
                // alive. Only a confirmable Closed result tears down runtimes.
                if self.lifecycle_driver.state() != NodeLifecycleState::Stopping
                    && self.lifecycle_driver.state() != NodeLifecycleState::Terminated
                {
                    self.lifecycle_driver
                        .transition(ServiceLifecycleEvent::ForceStop)
                        .map_err(|_| ())?;
                }
                Ok(())
            }
            LogicPlacementEffect::MemberSnapshot { version, members } => self
                .peers
                .install_snapshot(version, members)
                .await
                .map_err(|_| ()),
            LogicPlacementEffect::MemberEvent(event) => {
                if let MemberEvent {
                    change: MemberChange::Removed { node, .. },
                    ..
                } = event.as_ref()
                {
                    self.watches
                        .lock()
                        .expect("watch registry poisoned")
                        .node_down(node.incarnation);
                }
                self.peers.apply(*event).await.map_err(|_| ())
            }
            LogicPlacementEffect::Authority { .. }
            | LogicPlacementEffect::DrainReady { .. }
            | LogicPlacementEffect::DrainCommitted { .. } => Err(()),
        }
    }

    fn all_groups_ready(&self) -> bool {
        self.health
            .lock()
            .expect("service health poisoned")
            .groups
            .values()
            .all(|state| *state == ActorGroupState::Ready)
    }

    fn mark_membership_lost(&self) {
        self.controller.set_session_healthy(false);
        self.ready.send_replace(false);
        let _ = self
            .lifecycle_driver
            .transition(ServiceLifecycleEvent::MembershipLost);
    }
}

async fn next_join_event(
    events: &mut mpsc::Receiver<JoinEvent>,
    shutdown: &mut watch::Receiver<bool>,
) -> Option<JoinEvent> {
    tokio::select! {
        event = events.recv() => event,
        changed = shutdown.changed() => {
            if changed.is_err() || *shutdown.borrow() { None } else { events.recv().await }
        }
    }
}

fn closed_controls() -> mpsc::Receiver<PlacementControlEvent> {
    let (_, receiver) = mpsc::channel(1);
    receiver
}
