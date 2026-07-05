use async_trait::async_trait;
use tokio::sync::oneshot;

use crate::traits::HandlerErrorAction;
use crate::{Actor, ActorCallError, ActorContext, ActorError, Handler, Message, StopReason};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MailboxConfig {
    normal_capacity: usize,
    system_capacity: usize,
}

impl MailboxConfig {
    pub fn bounded(capacity: usize) -> Self {
        Self {
            normal_capacity: capacity,
            system_capacity: capacity,
        }
    }

    pub fn with_lanes(normal_capacity: usize, system_capacity: usize) -> Self {
        Self {
            normal_capacity,
            system_capacity,
        }
    }

    pub(crate) fn normal_capacity(&self) -> usize {
        self.normal_capacity
    }

    pub(crate) fn system_capacity(&self) -> usize {
        self.system_capacity
    }
}

impl Default for MailboxConfig {
    fn default() -> Self {
        Self::bounded(1024)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MailboxLane {
    Normal,
    System,
}

impl MailboxLane {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::System => "system",
        }
    }
}

pub(crate) enum ActorCommand<A: Actor> {
    Envelope(Box<dyn ActorEnvelope<A>>),
    Stop(StopReason),
}

#[async_trait]
pub(crate) trait ActorEnvelope<A: Actor>: Send {
    fn message_type(&self) -> &'static str;

    async fn handle(self: Box<Self>, actor: &mut A, ctx: &mut ActorContext<A>);
}

pub(crate) struct EnvelopeMessage<M: Message> {
    msg: M,
    reply_tx: oneshot::Sender<Result<M::Reply, ActorCallError>>,
}

impl<M: Message> EnvelopeMessage<M> {
    pub(crate) fn new(msg: M, reply_tx: oneshot::Sender<Result<M::Reply, ActorCallError>>) -> Self {
        Self { msg, reply_tx }
    }
}

#[async_trait]
impl<A, M> ActorEnvelope<A> for EnvelopeMessage<M>
where
    A: Handler<M>,
    M: Message,
{
    fn message_type(&self) -> &'static str {
        std::any::type_name::<M>()
    }

    async fn handle(self: Box<Self>, actor: &mut A, ctx: &mut ActorContext<A>) {
        let result = match actor.handle(ctx, self.msg).await {
            Ok(reply) => Ok(reply),
            Err(error) => {
                actor.on_error::<M>(ctx, &error).await;
                match actor.handle_error(ctx, error).await {
                    HandlerErrorAction::Reply(reply) => Ok(reply),
                    HandlerErrorAction::Propagate(error) => {
                        Err(ActorCallError::Handler(ActorError::from_error(error)))
                    }
                }
            }
        };
        let _ = self.reply_tx.send(result);
    }
}
