use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use lattice_placement::authority::AuthorityEffect;
use lattice_placement::control::PlacementControlEvent;
use lattice_placement::coordinator::{MemberChange, MemberEvent, PlacementDomainHello};
use lattice_placement::session::{
    LogicCoordinatorConfig, LogicCoordinatorHandle, LogicPlacementEffect, PlacementDomainSession,
};
use lattice_remoting::association::AssociationManager;
use lattice_remoting::messaging::outbound::OutboundMessaging;
use lattice_remoting::watch::WatchRegistry;
use tokio::sync::{mpsc, watch};

use crate::backend::{DomainRouterDirectory, LogicalRouter};
use crate::builder::LogicalEntityInstaller;
use crate::lifecycle::{
    NodeLifecycle, NodeLifecycleState, PlacementDomainState, ProductionLifecycleDriver,
    ServiceHealthSnapshot, ServiceLifecycleEvent,
};

use super::join::{BootstrapView, JoinController, JoinEvent};
use super::peers::PeerReconciler;
use super::{DomainLogicalRouter, LogicalBufferConfig};

pub(crate) struct LogicJoinRuntime {
    pub controller: Arc<JoinController>,
    pub domain_hello: PlacementDomainHello,
    pub associations: Arc<AssociationManager>,
    pub controls: Option<mpsc::Receiver<PlacementControlEvent>>,
    pub config: LogicCoordinatorConfig,
    pub effect_capacity: usize,
    pub router: Arc<DomainRouterDirectory>,
    pub entity_installers: Vec<LogicalEntityInstaller>,
    pub messaging: Arc<OutboundMessaging>,
    pub buffer_config: LogicalBufferConfig,
    pub maximum_registrations: usize,
    pub peers: Arc<PeerReconciler>,
    pub watches: Arc<Mutex<WatchRegistry>>,
    pub lifecycle: Arc<Mutex<NodeLifecycle>>,
    pub lifecycle_driver: ProductionLifecycleDriver,
    pub health: Arc<Mutex<ServiceHealthSnapshot>>,
    pub health_events: watch::Sender<ServiceHealthSnapshot>,
    pub logic_handles:
        Arc<Mutex<BTreeMap<lattice_core::actor_ref::PlacementDomainId, LogicCoordinatorHandle>>>,
    pub drain_ready: watch::Sender<BTreeMap<lattice_core::actor_ref::PlacementDomainId, String>>,
    pub drain_blockers: watch::Sender<
        BTreeMap<
            lattice_core::actor_ref::PlacementDomainId,
            std::collections::BTreeSet<lattice_placement::types::PlacementSlotKey>,
        >,
    >,
    pub bootstrap_view: Arc<BootstrapView>,
    pub membership_ready: Arc<AtomicBool>,
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
                    if wait_for_membership(&self.membership_ready, &mut shutdown)
                        .await
                        .is_err()
                    {
                        break;
                    }
                    self.set_domain_state(PlacementDomainState::Joining);
                    self.bootstrap_view.install(leader.clone());
                    let Some(receiver) = controls.take() else {
                        continue;
                    };
                    let key = association.key().clone();
                    let Ok((session, effects)) = PlacementDomainSession::new(
                        self.domain_hello.clone(),
                        key,
                        self.associations.clone(),
                        self.config.clone(),
                        self.effect_capacity,
                    ) else {
                        break;
                    };
                    let Ok(mut router) = DomainLogicalRouter::new(
                        self.domain_hello.node.clone(),
                        session.state(),
                        self.associations.clone(),
                        self.messaging.clone(),
                        association.key().clone(),
                        self.buffer_config.clone(),
                        self.maximum_registrations,
                    )
                    .map(|router| router.with_peer_reconciler(self.peers.clone())) else {
                        let _ = self
                            .lifecycle_driver
                            .transition(ServiceLifecycleEvent::CoordinatorLost);
                        self.set_domain_state(PlacementDomainState::Degraded);
                        break;
                    };
                    if self
                        .entity_installers
                        .iter()
                        .filter(|install| install.domain == self.domain_hello.domain)
                        .any(|install| (install.install)(&mut router).is_err())
                    {
                        let _ = self
                            .lifecycle_driver
                            .transition(ServiceLifecycleEvent::CoordinatorLost);
                        self.set_domain_state(PlacementDomainState::Degraded);
                        break;
                    }
                    let domain = self.domain_hello.domain.clone();
                    if self.router.install(&domain, Arc::new(router)).is_err() {
                        let _ = self
                            .lifecycle_driver
                            .transition(ServiceLifecycleEvent::CoordinatorLost);
                        self.set_domain_state(PlacementDomainState::Degraded);
                        break;
                    }
                    let handle = session.control_handle();
                    self.logic_handles
                        .lock()
                        .expect("logic handles poisoned")
                        .insert(self.domain_hello.domain.clone(), handle.clone());
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
                    self.logic_handles
                        .lock()
                        .expect("logic handles poisoned")
                        .remove(&self.domain_hello.domain);
                    self.router.clear(&self.domain_hello.domain);
                }
                JoinEvent::CoordinatorLost { .. } => {
                    self.router.clear(&self.domain_hello.domain);
                    self.set_domain_state(PlacementDomainState::Degraded);
                    let _ = self
                        .lifecycle_driver
                        .transition(ServiceLifecycleEvent::CoordinatorLost);
                }
                JoinEvent::TerminalFailure(_) => {
                    self.set_domain_state(PlacementDomainState::Terminated);
                    let event = if self
                        .lifecycle
                        .lock()
                        .expect("service lifecycle poisoned")
                        .state()
                        == NodeLifecycleState::JoiningMembership
                    {
                        ServiceLifecycleEvent::StartupFailed
                    } else {
                        ServiceLifecycleEvent::ForceStop
                    };
                    let _ = self.lifecycle_driver.transition(event);
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
        session: PlacementDomainSession,
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
                self.set_domain_state(PlacementDomainState::Ready);
                let state = self
                    .lifecycle
                    .lock()
                    .expect("service lifecycle poisoned")
                    .state();
                let event = match state {
                    NodeLifecycleState::JoiningMembership
                        if self.membership_ready.load(Ordering::Acquire)
                            && self.all_domains_ready() =>
                    {
                        Some(ServiceLifecycleEvent::SnapshotInstalled)
                    }
                    NodeLifecycleState::Ready => None,
                    _ => None,
                };
                if let Some(event) = event {
                    let _ = self.lifecycle_driver.transition(event);
                    self.sync_node_health();
                }
            }
            tokio::select! {
                result = &mut task => {
                    self.set_domain_state(PlacementDomainState::Degraded);
                    let _ = self
                        .lifecycle_driver
                        .transition(ServiceLifecycleEvent::CoordinatorLost);
                    return result
                        .map(|(_, controls)| controls)
                        .unwrap_or_else(|_| closed_controls());
                }
                event = join_events.recv() => {
                    match event {
                        Some(JoinEvent::CoordinatorLost { leader: lost })
                            if lost.identity == leader.identity && lost.term == leader.term =>
                        {
                            self.set_domain_state(PlacementDomainState::Degraded);
                            let _ = self
                                .lifecycle_driver
                                .transition(ServiceLifecycleEvent::CoordinatorLost);
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
            LogicPlacementEffect::DrainReady {
                operation_id,
                incarnation,
            } => {
                if incarnation != self.domain_hello.node.incarnation {
                    return Err(());
                }
                handle
                    .complete_member_drain(operation_id.clone())
                    .await
                    .map_err(|_| ())?;
                let mut ready = self.drain_ready.borrow().clone();
                ready.insert(self.domain_hello.domain.clone(), operation_id);
                self.drain_ready.send_replace(ready);
                Ok(())
            }
            LogicPlacementEffect::Authority { slot, effect } => match effect {
                AuthorityEffect::DrainSlot => {
                    let succeeded = self.router.drain_slot(slot.clone()).await.unwrap_or(false);
                    handle.complete_drain(slot, succeeded).await.map_err(|_| ())
                }
                AuthorityEffect::PublishReady => handle.publish_ready(&slot).map_err(|_| ()),
                AuthorityEffect::PublishDrained => {
                    let result = handle.publish_drained(&slot).map_err(|_| ());
                    let mut blockers = self.drain_blockers.borrow().clone();
                    if let Some(slots) = blockers.get_mut(&self.domain_hello.domain) {
                        slots.remove(&slot);
                    }
                    self.drain_blockers.send_replace(blockers);
                    result
                }
                AuthorityEffect::PublishStopFailed => {
                    let result = handle.publish_stop_failed(&slot).map_err(|_| ());
                    let mut blockers = self.drain_blockers.borrow().clone();
                    let inserted = blockers
                        .entry(self.domain_hello.domain.clone())
                        .or_default()
                        .insert(slot.clone());
                    self.drain_blockers.send_replace(blockers);
                    if result.is_ok() && inserted {
                        let router = self.router.clone();
                        let handle = handle.clone();
                        tokio::spawn(async move {
                            if router.wait_slot_drained(slot.clone()).await.is_ok() {
                                let _ = handle.complete_drain(slot, true).await;
                            }
                        });
                    }
                    result
                }
                AuthorityEffect::StopSlot => {
                    let result = self
                        .router
                        .stop_fenced_slot(slot.clone())
                        .await
                        .map_err(|_| ());
                    let mut blockers = self.drain_blockers.borrow().clone();
                    if let Some(slots) = blockers.get_mut(&self.domain_hello.domain) {
                        slots.remove(&slot);
                    }
                    self.drain_blockers.send_replace(blockers);
                    result
                }
                AuthorityEffect::FenceAdmission
                | AuthorityEffect::OpenAdmission
                | AuthorityEffect::StartSlot
                | AuthorityEffect::StateLossPossible => Ok(()),
            },
        }
    }

    fn set_domain_state(&self, state: PlacementDomainState) {
        self.lifecycle_driver
            .set_domain_state(self.domain_hello.domain.clone(), state);
    }

    fn all_domains_ready(&self) -> bool {
        self.health
            .lock()
            .expect("service health poisoned")
            .domains
            .values()
            .all(|state| *state == PlacementDomainState::Ready)
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

async fn wait_for_membership(
    ready: &AtomicBool,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), ()> {
    while !ready.load(Ordering::Acquire) {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Err(());
                }
            }
        }
    }
    Ok(())
}
