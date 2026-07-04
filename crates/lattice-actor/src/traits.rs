use async_trait::async_trait;

use crate::{ActorContext, ActorError, ActorStopError};

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
    Passivated,
    MailboxClosed,
    StartFailed,
}
