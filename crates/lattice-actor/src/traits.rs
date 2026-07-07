use async_trait::async_trait;
use lattice_core::{
    LinkBackpressure, LinkClosed, LinkDirectionClosed, LinkOpened, LinkProtocolError, Linked,
};
use std::error::Error as StdError;

use crate::{ActorContext, ActorStopError, MailboxConfig};

pub trait Message: Send + 'static {
    type Reply: Send + 'static;
}

impl<T, M> Message for Linked<T, M>
where
    T: Send + 'static,
    M: Send + 'static,
{
    type Reply = ();
}

impl Message for LinkOpened {
    type Reply = ();
}

impl Message for LinkDirectionClosed {
    type Reply = ();
}

impl Message for LinkClosed {
    type Reply = ();
}

impl Message for LinkBackpressure {
    type Reply = ();
}

impl Message for LinkProtocolError {
    type Reply = ();
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandlerErrorAction<Reply, Error> {
    Reply(Reply),
    Propagate(Error),
}

#[async_trait]
pub trait Actor: Sized + Send + 'static {
    type Error: StdError + Send + Sync + 'static;

    async fn started(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn stopping(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _reason: StopReason,
    ) -> Result<(), ActorStopError> {
        Ok(())
    }

    async fn on_error<M>(&mut self, _ctx: &mut ActorContext<Self>, _error: &Self::Error)
    where
        M: Message,
    {
    }
}

#[async_trait]
pub trait Handler<M>: Actor
where
    M: Message,
{
    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        msg: M,
    ) -> Result<M::Reply, Self::Error>;

    async fn handle_error(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        error: Self::Error,
    ) -> HandlerErrorAction<M::Reply, Self::Error> {
        HandlerErrorAction::Propagate(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    Requested,
    Passivated(PassivationReason),
    MailboxClosed,
    StartFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorLifecycleState {
    Empty,
    Activating,
    Loading,
    Running,
    Passivating,
    Stopping,
    StopFailed,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassivationReason {
    BusinessIdle,
    IdleTimeout,
    Drain,
    Migrate,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChildActorKey(String);

impl ChildActorKey {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ChildActorOptions {
    pub mailbox: MailboxConfig,
    pub supervision: ChildSupervision,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ChildSupervision {
    #[default]
    StopChild,
    StopParent,
    RestartChild,
}
