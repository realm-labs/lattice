use std::{convert::Infallible, future::Future};

use crate::{
    handle::{ActorHandle, ActorTerminationSubscription},
    traits::{Actor, PassivationReason, StopReason},
};

pub use lattice_model::actor::{TerminatedReason, WatchId, WatchStatus};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorTermination {
    pub target: LocalActorRef,
    pub reason: TerminatedReason,
}

#[derive(Debug, Clone, PartialEq, Eq, crate::Message)]
pub struct ActorTerminated {
    pub watch_id: WatchId,
    pub reason: TerminatedReason,
}

/// A target whose lifetime can be observed without exposing how it is reached.
pub trait WatchTarget: Send + Sync {
    type Error: std::fmt::Display;
    type Subscription: TerminationSubscription;

    fn watch(&self) -> impl Future<Output = Result<Self::Subscription, Self::Error>> + Send;
}

/// One DeathWatch subscription normalized across local and distributed targets.
pub trait TerminationSubscription: Send + 'static {
    fn id(&self) -> WatchId;

    fn recv(&mut self) -> impl Future<Output = Option<TerminatedReason>> + Send;
}

pub struct LocalWatchSubscription {
    id: WatchId,
    inner: ActorTerminationSubscription,
}

impl LocalWatchSubscription {
    pub(crate) fn new(inner: ActorTerminationSubscription) -> Self {
        Self {
            id: WatchId::random(),
            inner,
        }
    }
}

impl TerminationSubscription for LocalWatchSubscription {
    fn id(&self) -> WatchId {
        self.id
    }

    async fn recv(&mut self) -> Option<TerminatedReason> {
        self.inner.recv().await.ok().map(|event| event.reason)
    }
}

impl<A: Actor> WatchTarget for ActorHandle<A> {
    type Error = Infallible;
    type Subscription = LocalWatchSubscription;

    fn watch(&self) -> impl Future<Output = Result<Self::Subscription, Self::Error>> + Send {
        ActorHandle::watch(self)
    }
}

impl From<StopReason> for TerminatedReason {
    fn from(value: StopReason) -> Self {
        match value {
            StopReason::Passivated(PassivationReason::BusinessIdle)
            | StopReason::Passivated(PassivationReason::IdleTimeout)
            | StopReason::Passivated(PassivationReason::Drain) => Self::Passivated,
            StopReason::Requested | StopReason::MailboxClosed | StopReason::StartFailed => {
                Self::Stopped
            }
        }
    }
}
