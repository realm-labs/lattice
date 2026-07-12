use async_trait::async_trait;
use std::error::Error as StdError;

use crate::context::ActorContext;
use crate::error::ActorStopError;
use crate::mailbox::MailboxConfig;
use lattice_core::actor_ref::{EntityId, ProtocolId};
use thiserror::Error;

pub trait Message: Send + 'static {
    type Reply: Send + 'static;
}

pub trait EntityKey: Clone + Send + Sync + 'static {
    fn to_entity_id(&self) -> Result<EntityId, EntityKeyDecodeError>;
    fn try_from_entity_id(entity_id: &EntityId) -> Result<Self, EntityKeyDecodeError>;
}

pub trait ShardedActor: Actor {
    type Key: EntityKey;
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("entity key encoding is invalid: {reason}")]
pub struct EntityKeyDecodeError {
    pub reason: String,
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
    pub protocol_id: Option<ProtocolId>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ChildSupervision {
    #[default]
    StopChild,
    StopParent,
    RestartChild,
}
