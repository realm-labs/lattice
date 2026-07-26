use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::Instant,
};

use lattice_core::{actor_ref::PlacementDomainId, coordinator::CoordinatorScope};
use lattice_placement::types::PlacementSlotKey;
use thiserror::Error;
use tokio::sync::watch::Sender;

/// Lifecycle metrics for one service component.
///
/// A process can host several `LatticeService` instances, so the counters belong to the
/// component that owns them instead of to the crate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceLifecycleMetricsSnapshot {
    pub component: String,
    pub lifecycle_transition_failures_total: u64,
    pub termination_completed_total: u64,
    pub latest_termination_latency_millis: u64,
    pub blocked_drain_reports_total: u64,
    pub active_blocked_drain_slots: u64,
}

#[derive(Debug, Default)]
struct ServiceLifecycleMetrics {
    transition_failures_total: AtomicU64,
    termination_completed_total: AtomicU64,
    latest_termination_latency_millis: AtomicU64,
    blocked_drain_reports_total: AtomicU64,
    active_blocked_drain_slots: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeLifecycleState {
    Booting,
    JoiningMembership,
    Ready,
    Draining,
    Stopping,
    Terminated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlacementDomainState {
    Joining,
    Ready,
    Degraded,
    Draining,
    Terminated,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceHealthSnapshot {
    pub node: NodeLifecycleState,
    pub domains: BTreeMap<PlacementDomainId, PlacementDomainState>,
    pub coordinator_scopes: BTreeMap<CoordinatorScope, CoordinatorScopeState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorScopeState {
    Active,
    Standby,
    Failed,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LifecycleInterventionReport {
    pub blocked_slots: BTreeMap<PlacementDomainId, Vec<PlacementSlotKey>>,
    pub retained_actor_cells: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceLifecycleEvent {
    RemotingReady,
    SnapshotInstalled,
    MembershipLost,
    CoordinatorLost,
    Reconciled,
    BeginDrain,
    DrainComplete,
    ForceStop,
    StartupFailed,
    RuntimeTerminated,
    ShutdownComplete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceLifecycleEffect {
    /// Opens every admission scope. Only a node that installed a membership snapshot may do this.
    OpenAdmission,
    /// Closes external ingress only, leaving cluster-internal routes that govern themselves.
    CloseExternalAdmission,
    /// Closes every admission scope. Every termination path must use this and never the narrow one.
    CloseAllAdmission,
    BeginPlacementDrain,
    FenceClaimsAndStopRuntime,
    ReleaseRuntimeIdentity,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("event {event:?} is invalid while service is {state:?}")]
pub struct ServiceLifecycleError {
    pub state: NodeLifecycleState,
    pub event: ServiceLifecycleEvent,
}

#[derive(Debug, Clone)]
pub struct NodeLifecycle {
    state: NodeLifecycleState,
    recovering_membership: bool,
}

/// The class of traffic one admission decision covers.
///
/// A single node-wide gate cannot express the cluster's failure model: losing the membership
/// session says nothing about whether an exact activation still exists or whether a placement
/// claim is still valid, so a gate that covers all three at once turns one coordinator failure
/// into a total traffic outage. Each scope names the mechanism that actually decides whether its
/// traffic is safe, which is what lets the lifecycle close only the scope a failure invalidates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AdmissionScope {
    /// Traffic entering the actor world from outside the cluster mesh (gateways, HTTP edges).
    ///
    /// Nothing outside the node vouches for this traffic, so membership is the only thing that
    /// says the node should still be taking new work on the cluster's behalf.
    External,
    /// Logical destinations (`EntityRef`, `SingletonRef`) resolved through placement.
    ///
    /// Governed by the placement domain session plus the installed claim deadline, which fences
    /// itself without any membership input.
    Logical,
    /// Exact activations addressed by a fully bound `ActorRef`.
    ///
    /// Governed by the `(node incarnation, actor path, ActivationId)` binding: a stale reference
    /// resolves to nothing rather than to a replacement.
    Exact,
}

impl AdmissionScope {
    pub const ALL: [Self; 3] = [Self::External, Self::Logical, Self::Exact];

    const fn bit(self) -> u8 {
        match self {
            Self::External => 0b001,
            Self::Logical => 0b010,
            Self::Exact => 0b100,
        }
    }
}

const ALL_ADMISSION_SCOPES: u8 = 0b111;

/// Which admission scopes are currently open, for health reporting and edge admission checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeAdmissionSnapshot {
    pub external: bool,
    pub logical: bool,
    pub exact: bool,
}

impl NodeAdmissionSnapshot {
    /// True while the node still serves traffic that other cluster members address to it.
    pub fn serves_cluster_traffic(&self) -> bool {
        self.logical || self.exact
    }

    /// True only when every scope is open, which is the node's fully participating state.
    pub fn fully_open(&self) -> bool {
        self.external && self.logical && self.exact
    }
}

/// Per-scope admission control for one node.
///
/// The scopes share one atomic word so that "close everything" is a single store: a termination
/// path must never be able to interleave with a narrower close and leave one scope open.
#[derive(Debug, Clone)]
pub struct NodeAdmissionGate {
    open: Arc<AtomicU8>,
}

impl NodeAdmissionGate {
    pub fn closed() -> Self {
        Self {
            open: Arc::new(AtomicU8::new(0)),
        }
    }

    pub fn is_open(&self, scope: AdmissionScope) -> bool {
        self.open.load(Ordering::Acquire) & scope.bit() != 0
    }

    pub fn snapshot(&self) -> NodeAdmissionSnapshot {
        let open = self.open.load(Ordering::Acquire);
        NodeAdmissionSnapshot {
            external: open & AdmissionScope::External.bit() != 0,
            logical: open & AdmissionScope::Logical.bit() != 0,
            exact: open & AdmissionScope::Exact.bit() != 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn opened() -> Self {
        Self {
            open: Arc::new(AtomicU8::new(ALL_ADMISSION_SCOPES)),
        }
    }

    fn open_all(&self) {
        self.open.store(ALL_ADMISSION_SCOPES, Ordering::Release);
    }

    fn close_all(&self) {
        self.open.store(0, Ordering::Release);
    }

    fn close(&self, scope: AdmissionScope) {
        self.open.fetch_and(!scope.bit(), Ordering::AcqRel);
    }
}

#[derive(Clone)]
pub struct ProductionLifecycleDriver {
    component: Arc<str>,
    lifecycle: Arc<Mutex<NodeLifecycle>>,
    lifecycle_events: Sender<NodeLifecycleState>,
    health: Arc<Mutex<ServiceHealthSnapshot>>,
    health_events: Sender<ServiceHealthSnapshot>,
    admission: NodeAdmissionGate,
    runtime_stop_requested: Arc<AtomicBool>,
    identity_released: Arc<AtomicBool>,
    runtime_shutdowns: Arc<Mutex<Vec<Sender<bool>>>>,
    termination_started_at: Arc<Mutex<Option<Instant>>>,
    metrics: Arc<ServiceLifecycleMetrics>,
}

impl ProductionLifecycleDriver {
    pub fn new(
        component: impl Into<Arc<str>>,
        lifecycle: Arc<Mutex<NodeLifecycle>>,
        lifecycle_events: Sender<NodeLifecycleState>,
        health: Arc<Mutex<ServiceHealthSnapshot>>,
        health_events: Sender<ServiceHealthSnapshot>,
        admission: NodeAdmissionGate,
    ) -> Self {
        Self {
            component: component.into(),
            lifecycle,
            lifecycle_events,
            health,
            health_events,
            admission,
            runtime_stop_requested: Arc::new(AtomicBool::new(false)),
            identity_released: Arc::new(AtomicBool::new(false)),
            runtime_shutdowns: Arc::new(Mutex::new(Vec::new())),
            termination_started_at: Arc::new(Mutex::new(None)),
            metrics: Arc::new(ServiceLifecycleMetrics::default()),
        }
    }

    pub fn metrics(&self) -> ServiceLifecycleMetricsSnapshot {
        ServiceLifecycleMetricsSnapshot {
            component: self.component.to_string(),
            lifecycle_transition_failures_total: self
                .metrics
                .transition_failures_total
                .load(Ordering::Relaxed),
            termination_completed_total: self
                .metrics
                .termination_completed_total
                .load(Ordering::Relaxed),
            latest_termination_latency_millis: self
                .metrics
                .latest_termination_latency_millis
                .load(Ordering::Relaxed),
            blocked_drain_reports_total: self
                .metrics
                .blocked_drain_reports_total
                .load(Ordering::Relaxed),
            active_blocked_drain_slots: self
                .metrics
                .active_blocked_drain_slots
                .load(Ordering::Relaxed),
        }
    }

    pub(crate) fn record_blocked_drain_slots(&self, count: usize) {
        let count = u64::try_from(count).unwrap_or(u64::MAX);
        self.metrics
            .active_blocked_drain_slots
            .store(count, Ordering::Relaxed);
        if count > 0 {
            self.metrics
                .blocked_drain_reports_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn state(&self) -> NodeLifecycleState {
        self.lifecycle
            .lock()
            .expect("service lifecycle poisoned")
            .state()
    }

    pub fn admission_gate(&self) -> NodeAdmissionGate {
        self.admission.clone()
    }

    /// True while the node is re-joining after losing a membership session it had already used.
    ///
    /// This is what separates a node that never served traffic from one that is still serving
    /// exact and logical traffic while its membership session recovers; both report
    /// `JoiningMembership`.
    pub fn recovering_membership(&self) -> bool {
        self.lifecycle
            .lock()
            .expect("service lifecycle poisoned")
            .recovering_membership()
    }

    pub fn runtime_stop_requested(&self) -> bool {
        self.runtime_stop_requested.load(Ordering::Acquire)
    }

    pub fn identity_released(&self) -> bool {
        self.identity_released.load(Ordering::Acquire)
    }

    pub(crate) fn register_runtime_shutdown(&self, shutdown: Sender<bool>) {
        let mut shutdowns = self
            .runtime_shutdowns
            .lock()
            .expect("service runtime shutdown registry poisoned");
        if self.runtime_stop_requested() {
            let _ = shutdown.send(true);
        }
        shutdowns.push(shutdown);
    }

    pub fn transition(
        &self,
        event: ServiceLifecycleEvent,
    ) -> Result<NodeLifecycleState, ServiceLifecycleError> {
        let mut lifecycle = self.lifecycle.lock().expect("service lifecycle poisoned");
        let previous = lifecycle.state();
        let effects = match lifecycle.transition(event) {
            Ok(effects) => effects,
            Err(error) => {
                self.metrics
                    .transition_failures_total
                    .fetch_add(1, Ordering::Relaxed);
                tracing::error!(
                    target: "lattice.cluster.lifecycle",
                    component = %self.component,
                    ?event,
                    ?previous,
                    error = %error,
                    "production lifecycle driver rejected transition"
                );
                return Err(error);
            }
        };
        if matches!(
            event,
            ServiceLifecycleEvent::BeginDrain
                | ServiceLifecycleEvent::ForceStop
                | ServiceLifecycleEvent::StartupFailed
                | ServiceLifecycleEvent::RuntimeTerminated
        ) {
            let mut started = self
                .termination_started_at
                .lock()
                .expect("service termination timer poisoned");
            started.get_or_insert_with(Instant::now);
        }
        for effect in effects {
            self.apply_effect(effect);
        }
        let next = lifecycle.state();
        {
            let mut health = self.health.lock().expect("service health poisoned");
            if health.node != next {
                health.node = next;
                self.health_events.send_replace(health.clone());
            }
        }
        tracing::info!(
            target: "lattice.cluster.lifecycle",
            component = %self.component,
            ?event,
            ?previous,
            ?next,
            "production lifecycle driver committed transition"
        );
        self.lifecycle_events.send_if_modified(|current| {
            let changed = *current != next;
            *current = next;
            changed
        });
        if event == ServiceLifecycleEvent::ShutdownComplete {
            if let Some(started) = self
                .termination_started_at
                .lock()
                .expect("service termination timer poisoned")
                .take()
            {
                let millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
                self.metrics
                    .latest_termination_latency_millis
                    .store(millis, Ordering::Relaxed);
            }
            self.metrics
                .termination_completed_total
                .fetch_add(1, Ordering::Relaxed);
            self.metrics
                .active_blocked_drain_slots
                .store(0, Ordering::Relaxed);
        }
        Ok(next)
    }

    pub fn set_domain_state(&self, domain: PlacementDomainId, state: PlacementDomainState) {
        let node = self.state();
        let valid = match node {
            NodeLifecycleState::Terminated => state == PlacementDomainState::Terminated,
            NodeLifecycleState::Draining | NodeLifecycleState::Stopping => matches!(
                state,
                PlacementDomainState::Draining | PlacementDomainState::Terminated
            ),
            NodeLifecycleState::Booting
            | NodeLifecycleState::JoiningMembership
            | NodeLifecycleState::Ready => true,
        };
        if !valid {
            tracing::debug!(?node, ?state, %domain, "ignored late domain health transition during termination");
            return;
        }
        let mut health = self.health.lock().expect("service health poisoned");
        if health.domains.get(&domain) == Some(&state) {
            return;
        }
        health.domains.insert(domain, state);
        self.health_events.send_replace(health.clone());
    }

    fn apply_effect(&self, effect: ServiceLifecycleEffect) {
        match effect {
            ServiceLifecycleEffect::OpenAdmission => self.admission.open_all(),
            ServiceLifecycleEffect::CloseExternalAdmission => {
                self.admission.close(AdmissionScope::External);
            }
            ServiceLifecycleEffect::CloseAllAdmission => self.admission.close_all(),
            ServiceLifecycleEffect::BeginPlacementDrain => {
                let mut health = self.health.lock().expect("service health poisoned");
                let mut changed = false;
                for state in health.domains.values_mut() {
                    if !matches!(
                        state,
                        PlacementDomainState::Terminated | PlacementDomainState::Draining
                    ) {
                        *state = PlacementDomainState::Draining;
                        changed = true;
                    }
                }
                if changed {
                    self.health_events.send_replace(health.clone());
                }
            }
            ServiceLifecycleEffect::FenceClaimsAndStopRuntime => {
                self.admission.close_all();
                self.runtime_stop_requested.store(true, Ordering::Release);
                let mut shutdowns = self
                    .runtime_shutdowns
                    .lock()
                    .expect("service runtime shutdown registry poisoned");
                shutdowns.retain(|shutdown| shutdown.send(true).is_ok());
            }
            ServiceLifecycleEffect::ReleaseRuntimeIdentity => {
                self.identity_released.store(true, Ordering::Release);
            }
        }
    }
}

impl Default for NodeLifecycle {
    fn default() -> Self {
        Self {
            state: NodeLifecycleState::Booting,
            recovering_membership: false,
        }
    }
}

impl NodeLifecycle {
    pub fn state(&self) -> NodeLifecycleState {
        self.state
    }

    pub fn recovering_membership(&self) -> bool {
        self.recovering_membership
    }

    pub fn transition(
        &mut self,
        event: ServiceLifecycleEvent,
    ) -> Result<Vec<ServiceLifecycleEffect>, ServiceLifecycleError> {
        use NodeLifecycleState as State;
        use ServiceLifecycleEffect as Effect;
        use ServiceLifecycleEvent as Event;

        let (next, effects): (State, &[Effect]) = match (self.state, event) {
            (State::Booting, Event::RemotingReady) => (State::JoiningMembership, &[]),
            (State::JoiningMembership, Event::SnapshotInstalled) => {
                (State::Ready, &[Effect::OpenAdmission])
            }
            // Losing the membership session revokes nothing that exact-activation binding or a
            // placement claim deadline proves on its own, so only the scope membership actually
            // vouches for is closed. Closing more would turn one coordinator failover into a
            // node-wide outage for traffic that stayed provably safe throughout.
            (State::Ready, Event::MembershipLost) => {
                (State::JoiningMembership, &[Effect::CloseExternalAdmission])
            }
            (State::JoiningMembership, Event::MembershipLost) => (State::JoiningMembership, &[]),
            (
                State::Draining,
                Event::MembershipLost
                | Event::CoordinatorLost
                | Event::Reconciled
                | Event::SnapshotInstalled,
            ) => (State::Draining, &[]),
            (
                State::JoiningMembership | State::Ready,
                Event::CoordinatorLost | Event::Reconciled,
            ) => (self.state, &[]),
            (State::JoiningMembership | State::Ready, Event::BeginDrain) => (
                State::Draining,
                &[Effect::CloseAllAdmission, Effect::BeginPlacementDrain],
            ),
            (State::Draining, Event::DrainComplete) => {
                (State::Stopping, &[Effect::FenceClaimsAndStopRuntime])
            }
            (
                State::Booting | State::JoiningMembership | State::Ready | State::Draining,
                Event::ForceStop,
            ) => (
                State::Stopping,
                &[Effect::CloseAllAdmission, Effect::FenceClaimsAndStopRuntime],
            ),
            (State::Booting | State::JoiningMembership, Event::StartupFailed) => {
                (State::Stopping, &[Effect::CloseAllAdmission])
            }
            (
                State::JoiningMembership | State::Ready | State::Draining,
                Event::RuntimeTerminated,
            ) => (
                State::Stopping,
                &[Effect::CloseAllAdmission, Effect::FenceClaimsAndStopRuntime],
            ),
            (
                State::Stopping,
                Event::MembershipLost
                | Event::CoordinatorLost
                | Event::Reconciled
                | Event::SnapshotInstalled
                | Event::RuntimeTerminated,
            ) => (State::Stopping, &[]),
            (State::Stopping, Event::ShutdownComplete) => {
                (State::Terminated, &[Effect::ReleaseRuntimeIdentity])
            }
            _ => {
                return Err(ServiceLifecycleError {
                    state: self.state,
                    event,
                });
            }
        };
        if event == Event::MembershipLost && self.state == State::Ready {
            self.recovering_membership = true;
        } else if event == Event::SnapshotInstalled || next == State::Terminated {
            self.recovering_membership = false;
        }
        self.state = next;
        Ok(effects.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn production_driver(
        domains: impl IntoIterator<Item = PlacementDomainId>,
    ) -> ProductionLifecycleDriver {
        let lifecycle = Arc::new(Mutex::new(NodeLifecycle::default()));
        let (lifecycle_events, _) = tokio::sync::watch::channel(NodeLifecycleState::Booting);
        let health = Arc::new(Mutex::new(ServiceHealthSnapshot {
            node: NodeLifecycleState::Booting,
            domains: domains
                .into_iter()
                .map(|domain| (domain, PlacementDomainState::Joining))
                .collect(),
            coordinator_scopes: BTreeMap::new(),
        }));
        let (health_events, _) = tokio::sync::watch::channel(health.lock().unwrap().clone());
        ProductionLifecycleDriver::new(
            "driver-test-node",
            lifecycle,
            lifecycle_events,
            health,
            health_events,
            NodeAdmissionGate::closed(),
        )
    }

    fn observed_driver(
        domain: &PlacementDomainId,
    ) -> (
        ProductionLifecycleDriver,
        tokio::sync::watch::Receiver<ServiceHealthSnapshot>,
    ) {
        let lifecycle = Arc::new(Mutex::new(NodeLifecycle::default()));
        let (lifecycle_events, _) = tokio::sync::watch::channel(NodeLifecycleState::Booting);
        let health = Arc::new(Mutex::new(ServiceHealthSnapshot {
            node: NodeLifecycleState::Booting,
            domains: [(domain.clone(), PlacementDomainState::Joining)]
                .into_iter()
                .collect(),
            coordinator_scopes: BTreeMap::new(),
        }));
        let (health_events, health_rx) =
            tokio::sync::watch::channel(health.lock().unwrap().clone());
        let driver = ProductionLifecycleDriver::new(
            "observed-node",
            lifecycle,
            lifecycle_events,
            health,
            health_events,
            NodeAdmissionGate::closed(),
        );
        (driver, health_rx)
    }

    #[test]
    fn repeated_domain_and_node_states_do_not_republish_health() {
        let domain = PlacementDomainId::new("republish-test").unwrap();
        let (driver, mut health) = observed_driver(&domain);
        driver
            .transition(ServiceLifecycleEvent::RemotingReady)
            .unwrap();
        driver.set_domain_state(domain.clone(), PlacementDomainState::Ready);
        assert!(health.has_changed().unwrap());
        health.mark_unchanged();

        for _ in 0..8 {
            driver.set_domain_state(domain.clone(), PlacementDomainState::Ready);
        }
        driver
            .transition(ServiceLifecycleEvent::MembershipLost)
            .unwrap();

        assert!(!health.has_changed().unwrap());
    }

    #[test]
    fn lifecycle_metrics_are_scoped_to_one_component() {
        let logic = production_driver([]);
        let coordinator = production_driver([]);
        assert!(
            logic
                .transition(ServiceLifecycleEvent::DrainComplete)
                .is_err()
        );

        assert_eq!(logic.metrics().lifecycle_transition_failures_total, 1);
        assert_eq!(coordinator.metrics().lifecycle_transition_failures_total, 0);

        logic.transition(ServiceLifecycleEvent::ForceStop).unwrap();
        logic.record_blocked_drain_slots(3);
        logic
            .transition(ServiceLifecycleEvent::ShutdownComplete)
            .unwrap();

        assert_eq!(logic.metrics().termination_completed_total, 1);
        assert_eq!(logic.metrics().blocked_drain_reports_total, 1);
        assert_eq!(logic.metrics().active_blocked_drain_slots, 0);
        assert_eq!(coordinator.metrics().termination_completed_total, 0);
        assert_eq!(coordinator.metrics().blocked_drain_reports_total, 0);
    }

    #[test]
    fn lifecycle_follows_ready_degraded_drain_and_shutdown() {
        let mut lifecycle = NodeLifecycle::default();
        lifecycle
            .transition(ServiceLifecycleEvent::RemotingReady)
            .unwrap();
        lifecycle
            .transition(ServiceLifecycleEvent::SnapshotInstalled)
            .unwrap();
        lifecycle
            .transition(ServiceLifecycleEvent::CoordinatorLost)
            .unwrap();
        lifecycle
            .transition(ServiceLifecycleEvent::Reconciled)
            .unwrap();
        lifecycle
            .transition(ServiceLifecycleEvent::BeginDrain)
            .unwrap();
        lifecycle
            .transition(ServiceLifecycleEvent::DrainComplete)
            .unwrap();
        lifecycle
            .transition(ServiceLifecycleEvent::ShutdownComplete)
            .unwrap();
        assert_eq!(lifecycle.state(), NodeLifecycleState::Terminated);
    }

    #[test]
    fn illegal_transition_has_no_state_change_or_effects() {
        let mut lifecycle = NodeLifecycle::default();
        assert!(
            lifecycle
                .transition(ServiceLifecycleEvent::DrainComplete)
                .is_err()
        );
        assert_eq!(lifecycle.state(), NodeLifecycleState::Booting);
    }

    #[test]
    fn coordinator_loss_during_join_still_requires_snapshot() {
        let mut lifecycle = NodeLifecycle::default();
        lifecycle
            .transition(ServiceLifecycleEvent::RemotingReady)
            .unwrap();
        assert!(
            lifecycle
                .transition(ServiceLifecycleEvent::CoordinatorLost)
                .unwrap()
                .is_empty()
        );
        assert!(
            lifecycle
                .transition(ServiceLifecycleEvent::Reconciled)
                .unwrap()
                .is_empty()
        );
        assert_eq!(lifecycle.state(), NodeLifecycleState::JoiningMembership);
        assert_eq!(
            lifecycle
                .transition(ServiceLifecycleEvent::SnapshotInstalled)
                .unwrap(),
            vec![ServiceLifecycleEffect::OpenAdmission]
        );
    }

    #[test]
    fn membership_loss_revokes_node_readiness_until_a_new_snapshot() {
        let mut lifecycle = NodeLifecycle::default();
        lifecycle
            .transition(ServiceLifecycleEvent::RemotingReady)
            .unwrap();
        lifecycle
            .transition(ServiceLifecycleEvent::SnapshotInstalled)
            .unwrap();
        assert_eq!(
            lifecycle
                .transition(ServiceLifecycleEvent::MembershipLost)
                .unwrap(),
            vec![ServiceLifecycleEffect::CloseExternalAdmission]
        );
        assert_eq!(lifecycle.state(), NodeLifecycleState::JoiningMembership);
        lifecycle
            .transition(ServiceLifecycleEvent::SnapshotInstalled)
            .unwrap();
        assert_eq!(lifecycle.state(), NodeLifecycleState::Ready);
    }

    /// Every path that gives up this node's cluster authority has to close every scope. The
    /// narrow close exists for one event only, so the table is asserted exhaustively rather than
    /// per-path: a future transition that reaches for `CloseExternalAdmission` instead of
    /// `CloseAllAdmission` would leave a terminating node still serving traffic.
    #[test]
    fn only_membership_loss_narrows_admission_and_every_termination_closes_all() {
        use ServiceLifecycleEvent as Event;

        let ready = || {
            let mut lifecycle = NodeLifecycle::default();
            lifecycle.transition(Event::RemotingReady).unwrap();
            lifecycle.transition(Event::SnapshotInstalled).unwrap();
            lifecycle
        };

        for event in [
            Event::BeginDrain,
            Event::ForceStop,
            Event::RuntimeTerminated,
        ] {
            let effects = ready().transition(event).unwrap();
            assert!(
                effects.contains(&ServiceLifecycleEffect::CloseAllAdmission),
                "{event:?} must close every admission scope"
            );
            assert!(
                !effects.contains(&ServiceLifecycleEffect::CloseExternalAdmission),
                "{event:?} must not settle for closing external admission only"
            );
        }

        let mut booting = NodeLifecycle::default();
        booting.transition(Event::RemotingReady).unwrap();
        assert_eq!(
            booting.transition(Event::StartupFailed).unwrap(),
            vec![ServiceLifecycleEffect::CloseAllAdmission]
        );

        let mut draining = ready();
        draining.transition(Event::BeginDrain).unwrap();
        assert_eq!(
            draining.transition(Event::DrainComplete).unwrap(),
            vec![ServiceLifecycleEffect::FenceClaimsAndStopRuntime]
        );

        assert_eq!(
            ready().transition(Event::MembershipLost).unwrap(),
            vec![ServiceLifecycleEffect::CloseExternalAdmission]
        );
    }

    /// The availability property the split exists for: a membership session this node already
    /// used can be lost and regained without the traffic that governs itself ever being refused.
    #[test]
    fn membership_loss_keeps_cluster_traffic_admitted_and_closes_only_the_edge() {
        let driver = production_driver([]);
        driver
            .transition(ServiceLifecycleEvent::RemotingReady)
            .unwrap();
        driver
            .transition(ServiceLifecycleEvent::SnapshotInstalled)
            .unwrap();
        assert!(driver.admission_gate().snapshot().fully_open());
        assert!(!driver.recovering_membership());

        driver
            .transition(ServiceLifecycleEvent::MembershipLost)
            .unwrap();
        let admission = driver.admission_gate().snapshot();
        assert!(!admission.external, "external admission must be shed");
        assert!(
            admission.logical,
            "placement claims fence logical traffic on their own deadline"
        );
        assert!(
            admission.exact,
            "an ActorRef is fenced by the incarnation and activation it names"
        );
        assert!(admission.serves_cluster_traffic());
        assert!(
            driver.recovering_membership(),
            "recovery must be distinguishable from a first join"
        );

        driver
            .transition(ServiceLifecycleEvent::SnapshotInstalled)
            .unwrap();
        assert_eq!(driver.state(), NodeLifecycleState::Ready);
        assert!(driver.admission_gate().snapshot().fully_open());
        assert!(!driver.recovering_membership());
    }

    /// A first join admits nothing. Membership recovery is the only reason `JoiningMembership`
    /// serves traffic, so the two must not be conflated by anything reading the gate.
    #[test]
    fn a_node_that_never_joined_admits_nothing_while_joining() {
        let driver = production_driver([]);
        driver
            .transition(ServiceLifecycleEvent::RemotingReady)
            .unwrap();
        assert_eq!(driver.state(), NodeLifecycleState::JoiningMembership);
        let admission = driver.admission_gate().snapshot();
        assert!(!admission.external);
        assert!(!admission.serves_cluster_traffic());
        assert!(!driver.recovering_membership());
    }

    #[test]
    fn production_driver_consumes_admission_drain_and_identity_effects() {
        let domain = PlacementDomainId::new("driver-test").unwrap();
        let driver = production_driver([domain.clone()]);
        let (runtime_shutdown, runtime_shutdown_rx) = tokio::sync::watch::channel(false);
        driver.register_runtime_shutdown(runtime_shutdown);
        driver
            .transition(ServiceLifecycleEvent::RemotingReady)
            .unwrap();
        driver
            .transition(ServiceLifecycleEvent::SnapshotInstalled)
            .unwrap();
        assert!(driver.admission_gate().snapshot().fully_open());

        driver
            .transition(ServiceLifecycleEvent::MembershipLost)
            .unwrap();
        assert!(!driver.admission_gate().is_open(AdmissionScope::External));
        assert_eq!(driver.state(), NodeLifecycleState::JoiningMembership);
        driver
            .transition(ServiceLifecycleEvent::SnapshotInstalled)
            .unwrap();
        driver
            .transition(ServiceLifecycleEvent::BeginDrain)
            .unwrap();
        assert_eq!(
            driver.admission_gate().snapshot(),
            NodeAdmissionSnapshot {
                external: false,
                logical: false,
                exact: false,
            },
            "a draining node must admit nothing"
        );
        driver
            .transition(ServiceLifecycleEvent::DrainComplete)
            .unwrap();
        assert_eq!(driver.state(), NodeLifecycleState::Stopping);
        assert!(driver.runtime_stop_requested());
        assert!(*runtime_shutdown_rx.borrow());
        assert!(!driver.identity_released());
        driver
            .transition(ServiceLifecycleEvent::ShutdownComplete)
            .unwrap();
        assert_eq!(driver.state(), NodeLifecycleState::Terminated);
        assert!(driver.identity_released());
    }

    #[test]
    fn force_stop_is_not_observably_terminated_before_shutdown_complete() {
        let driver = production_driver([]);
        driver
            .transition(ServiceLifecycleEvent::RemotingReady)
            .unwrap();
        driver
            .transition(ServiceLifecycleEvent::SnapshotInstalled)
            .unwrap();
        driver.transition(ServiceLifecycleEvent::ForceStop).unwrap();
        assert_eq!(driver.state(), NodeLifecycleState::Stopping);
        assert!(
            AdmissionScope::ALL
                .iter()
                .all(|scope| !driver.admission_gate().is_open(*scope)),
            "a force-stopped node must admit nothing"
        );
        assert!(driver.runtime_stop_requested());
        driver
            .transition(ServiceLifecycleEvent::ShutdownComplete)
            .unwrap();
        assert_eq!(driver.state(), NodeLifecycleState::Terminated);
    }

    #[test]
    fn late_cluster_events_do_not_reopen_a_terminating_node() {
        let mut lifecycle = NodeLifecycle::default();
        lifecycle
            .transition(ServiceLifecycleEvent::RemotingReady)
            .unwrap();
        lifecycle
            .transition(ServiceLifecycleEvent::SnapshotInstalled)
            .unwrap();
        lifecycle
            .transition(ServiceLifecycleEvent::BeginDrain)
            .unwrap();

        for event in [
            ServiceLifecycleEvent::CoordinatorLost,
            ServiceLifecycleEvent::SnapshotInstalled,
            ServiceLifecycleEvent::MembershipLost,
        ] {
            assert!(lifecycle.transition(event).unwrap().is_empty());
            assert_eq!(lifecycle.state(), NodeLifecycleState::Draining);
        }

        lifecycle
            .transition(ServiceLifecycleEvent::DrainComplete)
            .unwrap();
        assert!(
            lifecycle
                .transition(ServiceLifecycleEvent::RuntimeTerminated)
                .unwrap()
                .is_empty()
        );
        assert_eq!(lifecycle.state(), NodeLifecycleState::Stopping);
    }
}
