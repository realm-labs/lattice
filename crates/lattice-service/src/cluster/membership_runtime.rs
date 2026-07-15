use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use lattice_placement::control::PlacementControlEvent;
use lattice_placement::coordinator::{MemberChange, MemberEvent, MemberHello};
use lattice_placement::membership_session::{MembershipCoordinatorHandle, MembershipSession};
use lattice_placement::session::{LogicCoordinatorConfig, LogicPlacementEffect};
use lattice_remoting::association::AssociationManager;
use lattice_remoting::watch::WatchRegistry;
use tokio::sync::{mpsc, watch};

use crate::lifecycle::{
    NodeLifecycle, NodeLifecycleState, PlacementDomainState, ServiceHealthSnapshot,
    ServiceLifecycleEvent,
};

use super::join::{BootstrapView, JoinController, JoinEvent};
use super::peers::PeerReconciler;

pub(crate) struct MembershipJoinRuntime {
    pub controller: Arc<JoinController>,
    pub hello: MemberHello,
    pub associations: Arc<AssociationManager>,
    pub controls: Option<mpsc::Receiver<PlacementControlEvent>>,
    pub config: LogicCoordinatorConfig,
    pub effect_capacity: usize,
    pub peers: Arc<PeerReconciler>,
    pub watches: Arc<Mutex<WatchRegistry>>,
    pub lifecycle: Arc<Mutex<NodeLifecycle>>,
    pub lifecycle_events: watch::Sender<NodeLifecycleState>,
    pub health: Arc<Mutex<ServiceHealthSnapshot>>,
    pub health_events: watch::Sender<ServiceHealthSnapshot>,
    pub bootstrap_view: Arc<BootstrapView>,
    pub ready: Arc<AtomicBool>,
    pub handle: Arc<Mutex<Option<MembershipCoordinatorHandle>>>,
}

impl MembershipJoinRuntime {
    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        let (join_events_tx, mut join_events) = mpsc::channel(8);
        let controller = tokio::spawn(
            self.controller
                .clone()
                .run(join_events_tx, shutdown.clone()),
        );
        let mut controls = self.controls.take();
        while let Some(event) = next_join_event(&mut join_events, &mut shutdown).await {
            match event {
                JoinEvent::Coordinator {
                    leader,
                    association,
                } => {
                    self.bootstrap_view.install(leader.clone());
                    let Some(receiver) = controls.take() else {
                        continue;
                    };
                    let Ok((session, handle, effects)) = MembershipSession::new(
                        self.hello.clone(),
                        association.key().clone(),
                        self.associations.clone(),
                        self.config.clone(),
                        self.effect_capacity,
                    ) else {
                        break;
                    };
                    *self.handle.lock().expect("membership handle poisoned") = Some(handle);
                    let state = session.state();
                    let returned = self
                        .run_session(
                            leader,
                            session,
                            state,
                            receiver,
                            effects,
                            &mut join_events,
                            &mut shutdown,
                        )
                        .await;
                    *self.handle.lock().expect("membership handle poisoned") = None;
                    controls = Some(returned);
                }
                JoinEvent::CoordinatorLost { .. } => {
                    self.mark_membership_lost();
                    *self.handle.lock().expect("membership handle poisoned") = None;
                }
                JoinEvent::TerminalFailure(_) => {
                    self.ready.store(false, Ordering::Release);
                    *self.handle.lock().expect("membership handle poisoned") = None;
                    transition(
                        &self.lifecycle,
                        &self.lifecycle_events,
                        ServiceLifecycleEvent::StartupFailed,
                    );
                    break;
                }
            }
        }
        controller.abort();
        *self.handle.lock().expect("membership handle poisoned") = None;
        let _ = controller.await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_session(
        &self,
        leader: lattice_remoting::bootstrap::BootstrapLeader,
        session: MembershipSession,
        state: Arc<Mutex<lattice_placement::membership_session::MembershipSessionState>>,
        controls: mpsc::Receiver<PlacementControlEvent>,
        mut effects: mpsc::Receiver<LogicPlacementEffect>,
        join_events: &mut mpsc::Receiver<JoinEvent>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> mpsc::Receiver<PlacementControlEvent> {
        let (session_shutdown, session_shutdown_rx) = watch::channel(false);
        let mut task = tokio::spawn(session.run_recoverable(controls, session_shutdown_rx));
        let changed = state
            .lock()
            .expect("membership session state poisoned")
            .change_notifier();
        loop {
            if state
                .lock()
                .expect("membership session state poisoned")
                .ready()
            {
                self.ready.store(true, Ordering::Release);
                let recovering = self
                    .lifecycle
                    .lock()
                    .expect("service lifecycle poisoned")
                    .recovering_membership();
                if recovering || self.all_domains_ready() {
                    transition(
                        &self.lifecycle,
                        &self.lifecycle_events,
                        ServiceLifecycleEvent::SnapshotInstalled,
                    );
                    self.sync_node_health();
                }
            }
            tokio::select! {
                result = &mut task => {
                    self.mark_membership_lost();
                    return result
                        .map(|(_, controls)| controls)
                        .unwrap_or_else(|_| closed_controls());
                }
                event = join_events.recv() => {
                    match event {
                        Some(JoinEvent::CoordinatorLost { leader: lost })
                            if lost.identity == leader.identity && lost.term == leader.term =>
                        {
                            self.mark_membership_lost();
                            let _ = session_shutdown.send(true);
                            return task.await
                                .map(|(_, controls)| controls)
                                .unwrap_or_else(|_| closed_controls());
                        }
                        Some(JoinEvent::TerminalFailure(_)) | None => {
                            self.mark_membership_lost();
                            let _ = session_shutdown.send(true);
                            return task.await
                                .map(|(_, controls)| controls)
                                .unwrap_or_else(|_| closed_controls());
                        }
                        Some(JoinEvent::Coordinator { .. })
                        | Some(JoinEvent::CoordinatorLost { .. }) => {}
                    }
                }
                effect = effects.recv() => {
                    let Some(effect) = effect else {
                        self.mark_membership_lost();
                        let _ = session_shutdown.send(true);
                        return task.await
                            .map(|(_, controls)| controls)
                            .unwrap_or_else(|_| closed_controls());
                    };
                    if self.apply_effect(effect).await.is_err() {
                        self.mark_membership_lost();
                        let _ = session_shutdown.send(true);
                        return task.await
                            .map(|(_, controls)| controls)
                            .unwrap_or_else(|_| closed_controls());
                    }
                }
                _ = changed.notified() => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        let _ = session_shutdown.send(true);
                        return task.await
                            .map(|(_, controls)| controls)
                            .unwrap_or_else(|_| closed_controls());
                    }
                }
            }
        }
    }

    async fn apply_effect(&self, effect: LogicPlacementEffect) -> Result<(), ()> {
        match effect {
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
            LogicPlacementEffect::Authority { .. } | LogicPlacementEffect::DrainReady { .. } => {
                Err(())
            }
        }
    }

    fn sync_node_health(&self) {
        let node = self
            .lifecycle
            .lock()
            .expect("service lifecycle poisoned")
            .state();
        let mut health = self.health.lock().expect("service health poisoned");
        health.node = node;
        self.health_events.send_replace(health.clone());
    }

    fn all_domains_ready(&self) -> bool {
        self.health
            .lock()
            .expect("service health poisoned")
            .domains
            .values()
            .all(|state| *state == PlacementDomainState::Ready)
    }

    fn mark_membership_lost(&self) {
        self.ready.store(false, Ordering::Release);
        transition(
            &self.lifecycle,
            &self.lifecycle_events,
            ServiceLifecycleEvent::MembershipLost,
        );
        self.sync_node_health();
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

fn transition(
    lifecycle: &Arc<Mutex<NodeLifecycle>>,
    events: &watch::Sender<NodeLifecycleState>,
    event: ServiceLifecycleEvent,
) {
    let mut lifecycle = lifecycle.lock().expect("service lifecycle poisoned");
    if lifecycle.transition(event).is_ok() {
        events.send_replace(lifecycle.state());
    }
}

fn closed_controls() -> mpsc::Receiver<PlacementControlEvent> {
    let (_, receiver) = mpsc::channel(1);
    receiver
}
