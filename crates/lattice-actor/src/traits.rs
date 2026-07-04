use async_trait::async_trait;

use crate::{ActorContext, ActorError, ActorStopError, MailboxConfig};

#[async_trait]
pub trait Actor: Sized + Send + 'static {
    async fn started(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        Ok(())
    }

    async fn stopping(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _reason: StopReason,
    ) -> Result<(), ActorStopError> {
        Ok(())
    }
}

pub trait Message: Send + 'static {
    type Reply: Send + 'static;
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
    ) -> Result<M::Reply, ActorError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    Requested,
    Passivated(PassivationReason),
    MailboxClosed,
    StartFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassivationReason {
    BusinessIdle,
    IdleTimeout,
    Drain,
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
}
