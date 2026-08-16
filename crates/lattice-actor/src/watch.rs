use crate::traits::{PassivationReason, StopReason};
#[cfg(feature = "distributed")]
use lattice_core::actor_ref::ActorRef;

pub use lattice_core::watch::{TerminatedReason, WatchId, WatchStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LocalActorRef {
    id: u64,
}

impl LocalActorRef {
    pub(crate) fn new(id: u64) -> Self {
        Self { id }
    }

    pub fn id(self) -> u64 {
        self.id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TerminatedTarget {
    Local(LocalActorRef),
    #[cfg(feature = "distributed")]
    Exact(ActorRef),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorTermination {
    pub target: LocalActorRef,
    pub reason: TerminatedReason,
}

#[derive(Debug, Clone, PartialEq, Eq, crate::Message)]
pub struct ActorTerminated {
    pub watch_id: WatchId,
    pub target: TerminatedTarget,
    pub reason: TerminatedReason,
}

impl From<StopReason> for TerminatedReason {
    fn from(value: StopReason) -> Self {
        match value {
            StopReason::Passivated(PassivationReason::BusinessIdle)
            | StopReason::Passivated(PassivationReason::IdleTimeout)
            | StopReason::Passivated(PassivationReason::Drain) => Self::Passivated,
            StopReason::Passivated(PassivationReason::Migrate) => Self::Migrated,
            StopReason::AuthorityLost => Self::Fenced,
            StopReason::Requested | StopReason::MailboxClosed | StopReason::StartFailed => {
                Self::Stopped
            }
        }
    }
}
