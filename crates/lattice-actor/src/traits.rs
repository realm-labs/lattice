use async_trait::async_trait;
use std::error::Error as StdError;

use crate::context::ActorContext;
use crate::error::ActorStopError;
use crate::mailbox::MailboxConfig;
use crate::reply::ReplyTo;
use lattice_core::actor_ref::{EntityId, ProtocolId};
use thiserror::Error;

/// A one-way message handled without a reply channel.
pub trait Message: Send + 'static {}

/// A request whose caller waits for a typed response.
pub trait Request: Send + 'static {
    type Response: Send + 'static;
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
pub enum ResponderErrorAction<Response, Error> {
    Respond(Response),
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
        M: Send + 'static,
    {
    }
}

#[async_trait]
pub trait Handler<M>: Actor
where
    M: Message,
{
    async fn handle(&mut self, ctx: &mut ActorContext<Self>, msg: M) -> Result<(), Self::Error>;
}

#[async_trait]
pub trait Responder<R>: Actor
where
    R: Request,
{
    async fn respond(
        &mut self,
        ctx: &mut ActorContext<Self>,
        request: R,
        reply_to: ReplyTo<R::Response>,
    ) -> Result<(), Self::Error>;

    async fn respond_error(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        error: Self::Error,
    ) -> ResponderErrorAction<R::Response, Self::Error> {
        ResponderErrorAction::Propagate(error)
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
