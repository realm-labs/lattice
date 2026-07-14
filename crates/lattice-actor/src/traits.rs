use async_trait::async_trait;
use std::any::Any;
use std::error::Error as StdError;
use std::time::Instant;

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

/// Whether a delivered actor message is a one-way tell or a request with a reply channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageKind {
    Tell,
    Request,
}

/// The mailbox lane from which a message was delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageLane {
    Normal,
    System,
}

/// Immutable information shared by the before/after message hooks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageMetadata {
    type_name: &'static str,
    kind: MessageKind,
    lane: MessageLane,
    enqueued_at: Instant,
    deadline: Option<Instant>,
}

impl MessageMetadata {
    pub(crate) fn new(
        type_name: &'static str,
        kind: MessageKind,
        lane: MessageLane,
        enqueued_at: Instant,
        deadline: Option<Instant>,
    ) -> Self {
        Self {
            type_name,
            kind,
            lane,
            enqueued_at,
            deadline,
        }
    }

    pub fn type_name(&self) -> &'static str {
        self.type_name
    }

    pub fn kind(&self) -> MessageKind {
        self.kind
    }

    pub fn lane(&self) -> MessageLane {
        self.lane
    }

    pub fn enqueued_at(&self) -> Instant {
        self.enqueued_at
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }
}

/// Immutable access to the concrete payload before its typed handler consumes it.
#[derive(Clone, Copy)]
pub struct MessageView<'a> {
    metadata: &'a MessageMetadata,
    payload: &'a dyn Any,
}

impl<'a> MessageView<'a> {
    pub(crate) fn new(metadata: &'a MessageMetadata, payload: &'a dyn Any) -> Self {
        Self { metadata, payload }
    }

    pub fn metadata(&self) -> &MessageMetadata {
        self.metadata
    }

    pub fn is<T: 'static>(&self) -> bool {
        self.payload.is::<T>()
    }

    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.payload.downcast_ref::<T>()
    }
}

/// Why a dequeued request was not passed to its typed responder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageRejection {
    DeadlineExceeded,
    DeferredReplyCapacityExceeded,
}

/// The result of dispatching a dequeued message.
///
/// `Handled` means the typed handler returned successfully. A request may still have a deferred
/// reply outstanding after that point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageOutcome {
    Handled,
    HandlerFailed,
    HandlerErrorRecovered,
    Rejected(MessageRejection),
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

    /// Observes every dequeued tell and request before typed dispatch.
    ///
    /// The view exposes the immutable concrete payload through `downcast_ref`. Returning from this
    /// hook always continues normal dispatch; rejection and transformation belong in typed handlers.
    fn before_message(&mut self, _ctx: &mut ActorContext<Self>, _message: MessageView<'_>) {}

    /// Observes the dispatch result after normal error/recovery handling has completed.
    fn after_message(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _metadata: &MessageMetadata,
        _outcome: MessageOutcome,
    ) {
    }

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

    async fn on_error<M>(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _metadata: &MessageMetadata,
        _error: &Self::Error,
    ) where
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
