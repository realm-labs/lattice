use crate::traits::{Message, PassivationReason, StopReason};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WatchId(u64);

impl WatchId {
    pub(crate) fn new(value: u64) -> Self {
        Self(value)
    }
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ActorIncarnation(u64);

impl ActorIncarnation {
    pub(crate) fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn value(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorTerminated {
    pub target: LocalActorRef,
    pub incarnation: ActorIncarnation,
    pub reason: TerminatedReason,
}

impl Message for ActorTerminated {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminatedReason {
    Stopped,
    Passivated,
    Migrated,
    Fenced,
    NodeDown,
}

impl From<StopReason> for TerminatedReason {
    fn from(value: StopReason) -> Self {
        match value {
            StopReason::Passivated(PassivationReason::BusinessIdle)
            | StopReason::Passivated(PassivationReason::IdleTimeout)
            | StopReason::Passivated(PassivationReason::Drain) => Self::Passivated,
            StopReason::Passivated(PassivationReason::Migrate) => Self::Migrated,
            StopReason::Requested | StopReason::MailboxClosed | StopReason::StartFailed => {
                Self::Stopped
            }
        }
    }
}
