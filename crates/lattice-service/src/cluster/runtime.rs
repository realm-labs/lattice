use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use lattice_core::actor_ref::{NodeIncarnation, PlacementDomainId};
use lattice_placement::{
    authority::AuthorityEffect,
    control::PlacementControlEvent,
    coordinator::{MemberChange, MemberEvent, PlacementDomainHello},
    session::{
        LogicCoordinatorConfig, LogicCoordinatorHandle, LogicPlacementEffect,
        PlacementDomainSession,
    },
    types::PlacementSlotKey,
};
use lattice_remoting::{
    association::AssociationManager, bootstrap::BootstrapLeader,
    messaging::outbound::OutboundMessaging, watch::WatchRegistry,
};
use tokio::sync::{mpsc, watch};

use super::{
    DomainLogicalRouter, LogicalBufferConfig,
    join::{BootstrapView, JoinController, JoinEvent},
    peers::PeerReconciler,
};
use crate::{
    backend::{DomainRouterDirectory, LogicalRouter},
    builder::LogicalEntityInstaller,
    lifecycle::{
        NodeLifecycle, NodeLifecycleState, PlacementDomainState, ProductionLifecycleDriver,
        ServiceHealthSnapshot, ServiceLifecycleEvent,
    },
    supervisor::TaskSupervisor,
};

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
    pub logic_handles: Arc<Mutex<BTreeMap<PlacementDomainId, LogicCoordinatorHandle>>>,
    pub drain_ready: watch::Sender<BTreeMap<PlacementDomainId, String>>,
    pub drain_blockers: watch::Sender<BTreeMap<PlacementDomainId, BTreeSet<PlacementSlotKey>>>,
    pub bootstrap_view: Arc<BootstrapView>,
    pub membership_ready: watch::Receiver<bool>,
    pub supervisor: Arc<TaskSupervisor>,
}

/// Applies the placement effects produced by one logic domain session.
///
/// Both the discovery-driven join runtime and the pre-assembled cluster runtime share this
/// applier so authority effects cannot diverge between the two assembly paths.
pub(crate) struct LogicEffectApplier {
    pub domain: PlacementDomainId,
    pub incarnation: NodeIncarnation,
    pub router: Arc<dyn LogicalRouter>,
    pub peers: Arc<PeerReconciler>,
    pub watches: Arc<Mutex<WatchRegistry>>,
    pub drain_ready: watch::Sender<BTreeMap<PlacementDomainId, String>>,
    pub drain_blockers: watch::Sender<BTreeMap<PlacementDomainId, BTreeSet<PlacementSlotKey>>>,
    pub supervisor: Arc<TaskSupervisor>,
}

impl LogicEffectApplier {
    pub async fn apply(
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
                let MemberEvent { version, change } = event.as_ref();
                match change {
                    MemberChange::Removed { node, reason } => {
                        tracing::info!(
                            target: "lattice.cluster.members",
                            node_id = %node.node_id,
                            incarnation = node.incarnation.get(),
                            term = version.term.get(),
                            revision = version.revision.get(),
                            ?reason,
                            "authoritative member removed"
                        );
                        self.watches
                            .lock()
                            .expect("watch registry poisoned")
                            .node_down(node.incarnation);
                    }
                    MemberChange::Upsert(record) => tracing::info!(
                        target: "lattice.cluster.members",
                        node_id = %record.node.node_id,
                        incarnation = record.node.incarnation.get(),
                        term = version.term.get(),
                        revision = version.revision.get(),
                        status = ?record.status,
                        "authoritative member upserted"
                    ),
                }
                self.peers.apply(*event).await.map_err(|_| ())
            }
            LogicPlacementEffect::DrainReady {
                operation_id,
                incarnation,
            } => {
                if incarnation != self.incarnation {
                    return Err(());
                }
                handle
                    .complete_member_drain(operation_id.clone())
                    .await
                    .map_err(|_| ())?;
                self.drain_ready.send_modify(|ready| {
                    ready.insert(self.domain.clone(), operation_id);
                });
                Ok(())
            }
            LogicPlacementEffect::Authority { slot, effect } => {
                self.apply_authority(slot, effect, handle).await
            }
        }
    }

    async fn apply_authority(
        &self,
        slot: PlacementSlotKey,
        effect: AuthorityEffect,
        handle: &LogicCoordinatorHandle,
    ) -> Result<(), ()> {
        match effect {
            AuthorityEffect::DrainSlot => {
                let succeeded = self.router.drain_slot(slot.clone()).await.unwrap_or(false);
                handle.complete_drain(slot, succeeded).await.map_err(|_| ())
            }
            AuthorityEffect::PublishReady => handle.publish_ready(&slot).map_err(|_| ()),
            AuthorityEffect::PublishDrained => {
                let result = handle.publish_drained(&slot).map_err(|_| ());
                self.release_blocker(&slot);
                result
            }
            AuthorityEffect::PublishStopFailed => {
                let result = handle.publish_stop_failed(&slot).map_err(|_| ());
                let mut inserted = false;
                self.drain_blockers.send_modify(|blockers| {
                    inserted = blockers
                        .entry(self.domain.clone())
                        .or_default()
                        .insert(slot.clone());
                });
                if result.is_ok() && inserted {
                    self.watch_stop_failed_slot(slot, handle);
                }
                result
            }
            AuthorityEffect::StopSlot => {
                let result = self
                    .router
                    .stop_fenced_slot(slot.clone())
                    .await
                    .map_err(|_| ());
                self.release_blocker(&slot);
                result
            }
            AuthorityEffect::FenceAdmission
            | AuthorityEffect::OpenAdmission
            | AuthorityEffect::StartSlot
            | AuthorityEffect::StateLossPossible => Ok(()),
        }
    }

    fn release_blocker(&self, slot: &PlacementSlotKey) {
        self.drain_blockers.send_modify(|blockers| {
            if let Some(slots) = blockers.get_mut(&self.domain) {
                slots.remove(slot);
            }
        });
    }

    pub(super) fn watch_stop_failed_slot(
        &self,
        slot: PlacementSlotKey,
        handle: &LogicCoordinatorHandle,
    ) {
        let router = self.router.clone();
        let handle = handle.clone();
        let watched = slot.clone();
        if self
            .supervisor
            .spawn(async move {
                if router.wait_slot_drained(watched.clone()).await.is_ok() {
                    let _ = handle.complete_drain(watched, true).await;
                }
            })
            .is_err()
        {
            tracing::warn!(
                target: "lattice.cluster.logic",
                domain = %self.domain.as_str(),
                ?slot,
                "stop-failed slot keeps blocking the drain because no supervised task was available"
            );
        }
    }
}

struct LogicSessionRun {
    leader: BootstrapLeader,
    session: PlacementDomainSession,
    controls: mpsc::Receiver<PlacementControlEvent>,
    effects: mpsc::Receiver<LogicPlacementEffect>,
    handle: LogicCoordinatorHandle,
}

struct LogicSessionReturn {
    controls: mpsc::Receiver<PlacementControlEvent>,
    retry: bool,
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
                    if wait_for_membership(&mut self.membership_ready, &mut shutdown)
                        .await
                        .is_err()
                    {
                        break;
                    }
                    self.set_domain_state(PlacementDomainState::Joining);
                    self.bootstrap_view.install(leader.clone());
                    let Some(mut receiver) = controls.take() else {
                        continue;
                    };
                    loop {
                        if association.state()
                            != lattice_remoting::association::AssociationState::Active
                        {
                            controls = Some(receiver);
                            break;
                        }
                        let key = association.key().clone();
                        let Ok((session, effects)) = PlacementDomainSession::new(
                            self.domain_hello.clone(),
                            key,
                            self.associations.clone(),
                            self.config.clone(),
                            self.effect_capacity,
                            leader.term,
                        ) else {
                            controls = Some(receiver);
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
                            controls = Some(receiver);
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
                            controls = Some(receiver);
                            break;
                        }
                        let domain = self.domain_hello.domain.clone();
                        if self.router.install(&domain, Arc::new(router)).is_err() {
                            let _ = self
                                .lifecycle_driver
                                .transition(ServiceLifecycleEvent::CoordinatorLost);
                            self.set_domain_state(PlacementDomainState::Degraded);
                            controls = Some(receiver);
                            break;
                        }
                        let handle = session.control_handle();
                        self.logic_handles
                            .lock()
                            .expect("logic handles poisoned")
                            .insert(self.domain_hello.domain.clone(), handle.clone());
                        let returned = self
                            .run_session(
                                LogicSessionRun {
                                    leader: leader.clone(),
                                    session,
                                    controls: receiver,
                                    effects,
                                    handle,
                                },
                                &mut join_events,
                                &mut shutdown,
                            )
                            .await;
                        self.logic_handles
                            .lock()
                            .expect("logic handles poisoned")
                            .remove(&self.domain_hello.domain);
                        self.router.clear(&self.domain_hello.domain);
                        receiver = returned.controls;
                        if !returned.retry {
                            controls = Some(receiver);
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
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

    async fn run_session(
        &self,
        run: LogicSessionRun,
        join_events: &mut mpsc::Receiver<JoinEvent>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> LogicSessionReturn {
        let LogicSessionRun {
            leader,
            session,
            controls,
            mut effects,
            handle,
        } = run;
        let (session_shutdown, session_shutdown_rx) = watch::channel(false);
        let mut task = tokio::spawn(session.run_recoverable(controls, session_shutdown_rx));
        let applier = self.effect_applier();
        let changed = handle.change_notifier();
        loop {
            // The placement state can become ready while authority effects produced by the
            // snapshot are still queued. Publishing domain readiness before those effects are
            // applied exposes a transient Ready state in which logical messages are rejected as
            // stale authority.
            if handle.ready_for_admission() && effects.is_empty() {
                self.set_domain_state(PlacementDomainState::Ready);
                let state = self
                    .lifecycle
                    .lock()
                    .expect("service lifecycle poisoned")
                    .state();
                let event = match state {
                    NodeLifecycleState::JoiningMembership
                        if *self.membership_ready.borrow() && self.all_domains_ready() =>
                    {
                        Some(ServiceLifecycleEvent::SnapshotInstalled)
                    }
                    NodeLifecycleState::Ready => None,
                    _ => None,
                };
                if let Some(event) = event {
                    let _ = self.lifecycle_driver.transition(event);
                }
            }
            tokio::select! {
                result = &mut task => {
                    self.set_domain_state(PlacementDomainState::Degraded);
                    let _ = self
                        .lifecycle_driver
                        .transition(ServiceLifecycleEvent::CoordinatorLost);
                    return match result {
                        Ok((Ok(()), controls)) => LogicSessionReturn {
                            controls,
                            retry: false,
                        },
                        Ok((Err(error), controls)) => {
                            let retry = !controls.is_closed();
                            tracing::warn!(
                                target: "lattice.cluster.logic",
                                %error,
                                domain = %self.domain_hello.domain.as_str(),
                                "logic session stopped; reconciliation required"
                            );
                            LogicSessionReturn {
                                controls,
                                retry,
                            }
                        }
                        Err(error) => {
                            tracing::warn!(
                                target: "lattice.cluster.logic",
                                %error,
                                domain = %self.domain_hello.domain.as_str(),
                                "logic session task failed; reconciliation required"
                            );
                            LogicSessionReturn {
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
                            self.set_domain_state(PlacementDomainState::Degraded);
                            let _ = self
                                .lifecycle_driver
                                .transition(ServiceLifecycleEvent::CoordinatorLost);
                            let _ = session_shutdown.send(true);
                            return LogicSessionReturn {
                                controls: task.await
                                .map(|(_, controls)| controls)
                                .unwrap_or_else(|_| closed_controls()),
                                retry: false,
                            };
                        }
                        Some(JoinEvent::TerminalFailure(_)) | None => {
                            let _ = session_shutdown.send(true);
                            return LogicSessionReturn {
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
                        self.set_domain_state(PlacementDomainState::Degraded);
                        let _ = self
                            .lifecycle_driver
                            .transition(ServiceLifecycleEvent::CoordinatorLost);
                        let _ = session_shutdown.send(true);
                        let controls = task.await
                            .map(|(_, controls)| controls)
                            .unwrap_or_else(|_| closed_controls());
                        let retry = !controls.is_closed();
                        return LogicSessionReturn {
                            controls,
                            retry,
                        };
                    };
                    if applier.apply(effect, &handle).await.is_err() {
                        self.set_domain_state(PlacementDomainState::Degraded);
                        let _ = self
                            .lifecycle_driver
                            .transition(ServiceLifecycleEvent::CoordinatorLost);
                        let _ = session_shutdown.send(true);
                        let controls = task.await
                            .map(|(_, controls)| controls)
                            .unwrap_or_else(|_| closed_controls());
                        let retry = !controls.is_closed();
                        tracing::warn!(
                            target: "lattice.cluster.logic",
                            domain = %self.domain_hello.domain.as_str(),
                            retry,
                            "logic session effect failed; reconciliation required"
                        );
                        return LogicSessionReturn {
                            controls,
                            retry,
                        };
                    }
                }
                _ = changed.notified() => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        let _ = session_shutdown.send(true);
                        return LogicSessionReturn {
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

    fn effect_applier(&self) -> LogicEffectApplier {
        LogicEffectApplier {
            domain: self.domain_hello.domain.clone(),
            incarnation: self.domain_hello.node.incarnation,
            router: self.router.clone(),
            peers: self.peers.clone(),
            watches: self.watches.clone(),
            drain_ready: self.drain_ready.clone(),
            drain_blockers: self.drain_blockers.clone(),
            supervisor: self.supervisor.clone(),
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
    ready: &mut watch::Receiver<bool>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), ()> {
    while !*ready.borrow_and_update() {
        tokio::select! {
            changed = ready.changed() => {
                if changed.is_err() {
                    return Err(());
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Err(());
                }
            }
        }
    }
    Ok(())
}
