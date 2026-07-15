use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceLifecycleState {
    Booting,
    Joining,
    Ready,
    Degraded,
    Draining,
    Stopping,
    Terminated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceLifecycleEvent {
    RemotingReady,
    SnapshotInstalled,
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
    pub state: ServiceLifecycleState,
    pub event: ServiceLifecycleEvent,
}

#[derive(Debug, Clone)]
pub struct ServiceLifecycle {
    state: ServiceLifecycleState,
}

impl Default for ServiceLifecycle {
    fn default() -> Self {
        Self {
            state: ServiceLifecycleState::Booting,
        }
    }
}

impl ServiceLifecycle {
    pub fn state(&self) -> ServiceLifecycleState {
        self.state
    }

    pub fn transition(
        &mut self,
        event: ServiceLifecycleEvent,
    ) -> Result<Vec<ServiceLifecycleEffect>, ServiceLifecycleError> {
        use ServiceLifecycleEffect as Effect;
        use ServiceLifecycleEvent as Event;
        use ServiceLifecycleState as State;

        let (next, effects): (State, &[Effect]) = match (self.state, event) {
            (State::Booting, Event::RemotingReady) => (State::Joining, &[]),
            (State::Joining, Event::SnapshotInstalled) => {
                (State::Ready, &[Effect::OpenExternalAdmission])
            }
            // A join has no external admission to close. Stay in Joining so a
            // fresh snapshot, not a mere reconnect, remains the readiness gate.
            (State::Joining, Event::CoordinatorLost | Event::Reconciled) => (State::Joining, &[]),
            (State::Ready, Event::CoordinatorLost) => {
                (State::Degraded, &[Effect::CloseExternalAdmission])
            }
            (State::Degraded, Event::Reconciled) => {
                (State::Ready, &[Effect::OpenExternalAdmission])
            }
            (State::Ready, Event::Reconciled) => (State::Ready, &[]),
            (State::Joining | State::Ready | State::Degraded, Event::BeginDrain) => (
                State::Draining,
                &[Effect::CloseExternalAdmission, Effect::BeginPlacementDrain],
            ),
            (State::Draining, Event::DrainComplete) => {
                (State::Stopping, &[Effect::FenceClaimsAndStopRuntime])
            }
            (
                State::Booting | State::Joining | State::Ready | State::Degraded | State::Draining,
                Event::ForceStop,
            ) => (State::Stopping, &[Effect::FenceClaimsAndStopRuntime]),
            (State::Booting | State::Joining, Event::StartupFailed) => {
                (State::Terminated, &[Effect::ReleaseRuntimeIdentity])
            }
            (
                State::Joining | State::Ready | State::Degraded | State::Draining,
                Event::RuntimeTerminated,
            ) => (
                State::Terminated,
                &[
                    Effect::CloseExternalAdmission,
                    Effect::ReleaseRuntimeIdentity,
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
        self.state = next;
        Ok(effects.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_follows_ready_degraded_drain_and_shutdown() {
        let mut lifecycle = ServiceLifecycle::default();
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
        assert_eq!(lifecycle.state(), ServiceLifecycleState::Terminated);
    }

    #[test]
    fn illegal_transition_has_no_state_change_or_effects() {
        let mut lifecycle = ServiceLifecycle::default();
        assert!(
            lifecycle
                .transition(ServiceLifecycleEvent::DrainComplete)
                .is_err()
        );
        assert_eq!(lifecycle.state(), ServiceLifecycleState::Booting);
    }

    #[test]
    fn coordinator_loss_during_join_still_requires_snapshot() {
        let mut lifecycle = ServiceLifecycle::default();
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
        assert_eq!(lifecycle.state(), ServiceLifecycleState::Joining);
        assert_eq!(
            lifecycle
                .transition(ServiceLifecycleEvent::SnapshotInstalled)
                .unwrap(),
            vec![ServiceLifecycleEffect::OpenExternalAdmission]
        );
    }
}
