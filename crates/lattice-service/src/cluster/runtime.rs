use std::sync::{Arc, Mutex};

use lattice_placement::authority::AuthorityEffect;
use lattice_placement::control::PlacementControlEvent;
use lattice_placement::coordinator::{MemberChange, MemberEvent, NodeHello};
use lattice_placement::session::{
    LogicCoordinatorConfig, LogicCoordinatorHandle, LogicCoordinatorSession, LogicPlacementEffect,
};
use lattice_remoting::association::AssociationManager;
use lattice_remoting::watch::WatchRegistry;
use tokio::sync::{mpsc, watch};

use crate::backend::LogicalRouter;
use crate::lifecycle::{ServiceLifecycle, ServiceLifecycleEvent, ServiceLifecycleState};

use super::join::{BootstrapView, JoinController, JoinEvent};
use super::members::MemberDirectory;
use super::peers::PeerReconciler;

pub(crate) struct LogicJoinRuntime {
    pub controller: Arc<JoinController>,
    pub hello: NodeHello,
    pub associations: Arc<AssociationManager>,
    pub controls: Option<mpsc::Receiver<PlacementControlEvent>>,
    pub config: LogicCoordinatorConfig,
    pub effect_capacity: usize,
    pub router: Option<Arc<dyn LogicalRouter>>,
    pub members: Arc<MemberDirectory>,
    pub peers: Arc<PeerReconciler>,
    pub watches: Arc<Mutex<WatchRegistry>>,
    pub lifecycle: Arc<Mutex<ServiceLifecycle>>,
    pub lifecycle_events: watch::Sender<ServiceLifecycleState>,
    pub logic_handle: Arc<Mutex<Option<LogicCoordinatorHandle>>>,
    pub drain_ready: watch::Sender<Option<String>>,
    pub bootstrap_view: Arc<BootstrapView>,
}

impl LogicJoinRuntime {
    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        let (join_events_tx, mut join_events) = mpsc::channel(8);
        let controller_shutdown = shutdown.clone();
        let controller = tokio::spawn(
            self.controller
                .clone()
                .run(join_events_tx, controller_shutdown),
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
                    let key = association.key().clone();
                    let Ok((session, effects)) = LogicCoordinatorSession::new(
                        self.hello.clone(),
                        key,
                        self.associations.clone(),
                        self.config.clone(),
                        self.effect_capacity,
                    ) else {
                        break;
                    };
                    let handle = session.control_handle();
                    *self.logic_handle.lock().expect("logic handle poisoned") =
                        Some(handle.clone());
                    controls = Some(
                        self.run_session(
                            leader,
                            session,
                            receiver,
                            effects,
                            handle,
                            &mut join_events,
                            &mut shutdown,
                        )
                        .await,
                    );
                    *self.logic_handle.lock().expect("logic handle poisoned") = None;
                }
                JoinEvent::CoordinatorLost { .. } => {
                    transition(
                        &self.lifecycle,
                        &self.lifecycle_events,
                        ServiceLifecycleEvent::CoordinatorLost,
                    );
                }
                JoinEvent::TerminalFailure(_) => {
                    let event = if self
                        .lifecycle
                        .lock()
                        .expect("service lifecycle poisoned")
                        .state()
                        == ServiceLifecycleState::Joining
                    {
                        ServiceLifecycleEvent::StartupFailed
                    } else {
                        ServiceLifecycleEvent::ForceStop
                    };
                    transition(&self.lifecycle, &self.lifecycle_events, event);
                    break;
                }
            }
        }
        controller.abort();
        let _ = controller.await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_session(
        &self,
        leader: lattice_remoting::bootstrap::BootstrapLeader,
        session: LogicCoordinatorSession,
        controls: mpsc::Receiver<PlacementControlEvent>,
        mut effects: mpsc::Receiver<LogicPlacementEffect>,
        handle: LogicCoordinatorHandle,
        join_events: &mut mpsc::Receiver<JoinEvent>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> mpsc::Receiver<PlacementControlEvent> {
        let (session_shutdown, session_shutdown_rx) = watch::channel(false);
        let mut task = tokio::spawn(session.run_recoverable(controls, session_shutdown_rx));
        let changed = handle.change_notifier();
        loop {
            if handle.ready() {
                let state = self
                    .lifecycle
                    .lock()
                    .expect("service lifecycle poisoned")
                    .state();
                let event = match state {
                    ServiceLifecycleState::Joining => {
                        Some(ServiceLifecycleEvent::SnapshotInstalled)
                    }
                    ServiceLifecycleState::Degraded => Some(ServiceLifecycleEvent::Reconciled),
                    _ => None,
                };
                if let Some(event) = event {
                    transition(&self.lifecycle, &self.lifecycle_events, event);
                }
            }
            tokio::select! {
                result = &mut task => {
                    transition(
                        &self.lifecycle,
                        &self.lifecycle_events,
                        ServiceLifecycleEvent::CoordinatorLost,
                    );
                    return result
                        .map(|(_, controls)| controls)
                        .unwrap_or_else(|_| closed_controls());
                }
                event = join_events.recv() => {
                    match event {
                        Some(JoinEvent::CoordinatorLost { leader: lost })
                            if lost.identity == leader.identity && lost.term == leader.term =>
                        {
                            transition(
                                &self.lifecycle,
                                &self.lifecycle_events,
                                ServiceLifecycleEvent::CoordinatorLost,
                            );
                            let _ = session_shutdown.send(true);
                            return task.await
                                .map(|(_, controls)| controls)
                                .unwrap_or_else(|_| closed_controls());
                        }
                        Some(JoinEvent::TerminalFailure(_)) | None => {
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
                        let _ = session_shutdown.send(true);
                        return task.await
                            .map(|(_, controls)| controls)
                            .unwrap_or_else(|_| closed_controls());
                    };
                    if self.apply_effect(effect, &handle).await.is_err() {
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

    async fn apply_effect(
        &self,
        effect: LogicPlacementEffect,
        handle: &LogicCoordinatorHandle,
    ) -> Result<(), ()> {
        match effect {
            LogicPlacementEffect::MemberSnapshot { revision, members } => self
                .members
                .install_snapshot(revision, members)
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
                self.peers.apply(*event).map_err(|_| ())
            }
            LogicPlacementEffect::DrainReady {
                operation_id,
                incarnation,
            } => {
                if incarnation != self.hello.node.incarnation {
                    return Err(());
                }
                handle
                    .complete_member_drain(operation_id.clone())
                    .map_err(|_| ())?;
                self.drain_ready.send_replace(Some(operation_id));
                Ok(())
            }
            LogicPlacementEffect::Authority { slot, effect } => {
                let router = self.router.as_ref().ok_or(())?;
                match effect {
                    AuthorityEffect::DrainSlot => {
                        let succeeded = router.drain_slot(slot.clone()).await.unwrap_or(false);
                        handle.complete_drain(slot, succeeded).await.map_err(|_| ())
                    }
                    AuthorityEffect::PublishReady => handle.publish_ready(&slot).map_err(|_| ()),
                    AuthorityEffect::PublishDrained => {
                        handle.publish_drained(&slot).map_err(|_| ())
                    }
                    AuthorityEffect::PublishStopFailed => {
                        handle.publish_stop_failed(&slot).map_err(|_| ())
                    }
                    AuthorityEffect::StopSlot => {
                        router.stop_fenced_slot(slot).await.map_err(|_| ())
                    }
                    AuthorityEffect::FenceAdmission
                    | AuthorityEffect::OpenAdmission
                    | AuthorityEffect::StartSlot
                    | AuthorityEffect::StateLossPossible => Ok(()),
                }
            }
        }
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
    lifecycle: &Arc<Mutex<ServiceLifecycle>>,
    events: &watch::Sender<ServiceLifecycleState>,
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
