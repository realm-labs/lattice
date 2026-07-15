use std::collections::BTreeMap;

use lattice_core::actor_ref::PlacementDomainId;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeLifecycleState {
    Booting,
    JoiningMembership,
    Ready,
    Draining,
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
                (State::Draining, &[Effect::FenceClaimsAndStopRuntime])
            }
            (
                State::Booting | State::JoiningMembership | State::Ready | State::Draining,
                Event::ForceStop,
            ) => (
                State::Terminated,
                &[
                    Effect::FenceClaimsAndStopRuntime,
                    Effect::ReleaseRuntimeIdentity,
                ],
            ),
            (State::Booting | State::JoiningMembership, Event::StartupFailed) => {
                (State::Terminated, &[Effect::ReleaseRuntimeIdentity])
            }
            (
                State::JoiningMembership | State::Ready | State::Draining,
                Event::RuntimeTerminated,
            ) => (
                State::Terminated,
                &[
                    Effect::CloseExternalAdmission,
                    Effect::ReleaseRuntimeIdentity,
                ],
            ),
            (State::Draining, Event::ShutdownComplete) => {
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
}
