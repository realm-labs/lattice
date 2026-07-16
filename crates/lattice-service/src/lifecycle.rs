use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use lattice_core::actor_ref::PlacementDomainId;
use lattice_core::coordinator::CoordinatorScope;
use lattice_placement::types::PlacementSlotKey;
use thiserror::Error;

static LIFECYCLE_TRANSITION_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
static TERMINATION_COMPLETED_TOTAL: AtomicU64 = AtomicU64::new(0);
static LATEST_TERMINATION_LATENCY_MILLIS: AtomicU64 = AtomicU64::new(0);
static BLOCKED_DRAIN_REPORTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static ACTIVE_BLOCKED_DRAIN_SLOTS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServiceLifecycleMetricsSnapshot {
    pub lifecycle_transition_failures_total: u64,
    pub termination_completed_total: u64,
    pub latest_termination_latency_millis: u64,
    pub blocked_drain_reports_total: u64,
    pub active_blocked_drain_slots: u64,
}

pub fn service_lifecycle_metrics() -> ServiceLifecycleMetricsSnapshot {
    ServiceLifecycleMetricsSnapshot {
        lifecycle_transition_failures_total: LIFECYCLE_TRANSITION_FAILURES_TOTAL
            .load(Ordering::Relaxed),
        termination_completed_total: TERMINATION_COMPLETED_TOTAL.load(Ordering::Relaxed),
        latest_termination_latency_millis: LATEST_TERMINATION_LATENCY_MILLIS
            .load(Ordering::Relaxed),
        blocked_drain_reports_total: BLOCKED_DRAIN_REPORTS_TOTAL.load(Ordering::Relaxed),
        active_blocked_drain_slots: ACTIVE_BLOCKED_DRAIN_SLOTS.load(Ordering::Relaxed),
    }
}

pub(crate) fn record_blocked_drain_slots(count: usize) {
    let count = u64::try_from(count).unwrap_or(u64::MAX);
    ACTIVE_BLOCKED_DRAIN_SLOTS.store(count, Ordering::Relaxed);
    if count > 0 {
        BLOCKED_DRAIN_REPORTS_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
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
    OpenExternalAdmission,
    CloseExternalAdmission,
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

#[derive(Debug, Clone)]
pub struct NodeAdmissionGate {
    open: Arc<AtomicBool>,
}

impl NodeAdmissionGate {
    pub fn closed() -> Self {
        Self {
            open: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn is_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn opened() -> Self {
        Self {
            open: Arc::new(AtomicBool::new(true)),
        }
    }

    fn open(&self) {
        self.open.store(true, Ordering::Release);
    }

    fn close(&self) {
        self.open.store(false, Ordering::Release);
    }
}

#[derive(Clone)]
pub struct ProductionLifecycleDriver {
    lifecycle: Arc<Mutex<NodeLifecycle>>,
    lifecycle_events: tokio::sync::watch::Sender<NodeLifecycleState>,
    health: Arc<Mutex<ServiceHealthSnapshot>>,
    health_events: tokio::sync::watch::Sender<ServiceHealthSnapshot>,
    admission: NodeAdmissionGate,
    runtime_stop_requested: Arc<AtomicBool>,
    identity_released: Arc<AtomicBool>,
    termination_started_at: Arc<Mutex<Option<Instant>>>,
}

impl ProductionLifecycleDriver {
    pub fn new(
        lifecycle: Arc<Mutex<NodeLifecycle>>,
        lifecycle_events: tokio::sync::watch::Sender<NodeLifecycleState>,
        health: Arc<Mutex<ServiceHealthSnapshot>>,
        health_events: tokio::sync::watch::Sender<ServiceHealthSnapshot>,
        admission: NodeAdmissionGate,
    ) -> Self {
        Self {
            lifecycle,
            lifecycle_events,
            health,
            health_events,
            admission,
            runtime_stop_requested: Arc::new(AtomicBool::new(false)),
            identity_released: Arc::new(AtomicBool::new(false)),
            termination_started_at: Arc::new(Mutex::new(None)),
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

    pub fn runtime_stop_requested(&self) -> bool {
        self.runtime_stop_requested.load(Ordering::Acquire)
    }

    pub fn identity_released(&self) -> bool {
        self.identity_released.load(Ordering::Acquire)
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
                LIFECYCLE_TRANSITION_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
                tracing::error!(
                    target: "lattice.cluster.lifecycle",
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
            health.node = next;
            self.health_events.send_replace(health.clone());
        }
        tracing::info!(
            target: "lattice.cluster.lifecycle",
            ?event,
            ?previous,
            ?next,
            "production lifecycle driver committed transition"
        );
        self.lifecycle_events.send_replace(next);
        if event == ServiceLifecycleEvent::ShutdownComplete {
            if let Some(started) = self
                .termination_started_at
                .lock()
                .expect("service termination timer poisoned")
                .take()
            {
                let millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
                LATEST_TERMINATION_LATENCY_MILLIS.store(millis, Ordering::Relaxed);
            }
            TERMINATION_COMPLETED_TOTAL.fetch_add(1, Ordering::Relaxed);
            ACTIVE_BLOCKED_DRAIN_SLOTS.store(0, Ordering::Relaxed);
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
            tracing::error!(?node, ?state, %domain, "rejected domain health transition that violates node postconditions");
            return;
        }
        let mut health = self.health.lock().expect("service health poisoned");
        health.domains.insert(domain, state);
        self.health_events.send_replace(health.clone());
    }

    fn apply_effect(&self, effect: ServiceLifecycleEffect) {
        match effect {
            ServiceLifecycleEffect::OpenExternalAdmission => self.admission.open(),
            ServiceLifecycleEffect::CloseExternalAdmission => self.admission.close(),
            ServiceLifecycleEffect::BeginPlacementDrain => {
                let mut health = self.health.lock().expect("service health poisoned");
                for state in health.domains.values_mut() {
                    if *state != PlacementDomainState::Terminated {
                        *state = PlacementDomainState::Draining;
                    }
                }
                self.health_events.send_replace(health.clone());
            }
            ServiceLifecycleEffect::FenceClaimsAndStopRuntime => {
                self.admission.close();
                self.runtime_stop_requested.store(true, Ordering::Release);
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
                (State::Ready, &[Effect::OpenExternalAdmission])
            }
            (State::Ready, Event::MembershipLost) => {
                (State::JoiningMembership, &[Effect::CloseExternalAdmission])
            }
            (State::JoiningMembership, Event::MembershipLost) => (State::JoiningMembership, &[]),
            (State::Draining, Event::MembershipLost) => (State::Draining, &[]),
            (
                State::JoiningMembership | State::Ready,
                Event::CoordinatorLost | Event::Reconciled,
            ) => (self.state, &[]),
            (State::JoiningMembership | State::Ready, Event::BeginDrain) => (
                State::Draining,
                &[Effect::CloseExternalAdmission, Effect::BeginPlacementDrain],
            ),
            (State::Draining, Event::DrainComplete) => {
                (State::Stopping, &[Effect::FenceClaimsAndStopRuntime])
            }
            (
                State::Booting | State::JoiningMembership | State::Ready | State::Draining,
                Event::ForceStop,
            ) => (
                State::Stopping,
                &[
                    Effect::CloseExternalAdmission,
                    Effect::FenceClaimsAndStopRuntime,
                ],
            ),
            (State::Booting | State::JoiningMembership, Event::StartupFailed) => {
                (State::Stopping, &[Effect::CloseExternalAdmission])
            }
            (
                State::JoiningMembership | State::Ready | State::Draining,
                Event::RuntimeTerminated,
            ) => (
                State::Stopping,
                &[
                    Effect::CloseExternalAdmission,
                    Effect::FenceClaimsAndStopRuntime,
                ],
            ),
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
            lifecycle,
            lifecycle_events,
            health,
            health_events,
            NodeAdmissionGate::closed(),
        )
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
            vec![ServiceLifecycleEffect::OpenExternalAdmission]
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

    #[test]
    fn production_driver_consumes_admission_drain_and_identity_effects() {
        let domain = PlacementDomainId::new("driver-test").unwrap();
        let driver = production_driver([domain.clone()]);
        driver
            .transition(ServiceLifecycleEvent::RemotingReady)
            .unwrap();
        driver
            .transition(ServiceLifecycleEvent::SnapshotInstalled)
            .unwrap();
        assert!(driver.admission_gate().is_open());

        driver
            .transition(ServiceLifecycleEvent::MembershipLost)
            .unwrap();
        assert!(!driver.admission_gate().is_open());
        assert_eq!(driver.state(), NodeLifecycleState::JoiningMembership);
        driver
            .transition(ServiceLifecycleEvent::SnapshotInstalled)
            .unwrap();
        driver
            .transition(ServiceLifecycleEvent::BeginDrain)
            .unwrap();
        assert!(!driver.admission_gate().is_open());
        driver
            .transition(ServiceLifecycleEvent::DrainComplete)
            .unwrap();
        assert_eq!(driver.state(), NodeLifecycleState::Stopping);
        assert!(driver.runtime_stop_requested());
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
        assert!(!driver.admission_gate().is_open());
        assert!(driver.runtime_stop_requested());
        driver
            .transition(ServiceLifecycleEvent::ShutdownComplete)
            .unwrap();
        assert_eq!(driver.state(), NodeLifecycleState::Terminated);
    }
}
