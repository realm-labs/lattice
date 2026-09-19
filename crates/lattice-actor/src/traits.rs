use std::{any::Any, error::Error as StdError, future::Future, time::Instant};

use crate::{
    context::{ActorContext, HandlerContext},
    error::ActorStopError,
    mailbox::MailboxConfig,
    reply::ReplyTo,
    runtime::{ActorExecutionPolicy, SchedulerKey},
    state_machine::Behavior,
};

/// A one-way message handled without a reply channel.
pub trait Message: Send + 'static {}

/// A request whose caller waits for a typed response.
pub trait Request: Send + 'static {
    type Response: Send + 'static;
}

/// The kind of work delivered through an actor mailbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageKind {
    Tell,
    Request,
    /// Internal actor work resumed after an asynchronous operation completed.
    Continuation,
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
    deadline: Option<Instant>,
}

impl MessageMetadata {
    pub(crate) fn new(
        type_name: &'static str,
        kind: MessageKind,
        lane: MessageLane,
        deadline: Option<Instant>,
    ) -> Self {
        Self {
            type_name,
            kind,
            lane,
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

    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }
}

/// Immutable access to the concrete payload before its typed handler or continuation consumes it.
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

/// Why a dequeued message was not passed to its typed handler or responder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageRejection {
    DeadlineExceeded,
    DeferredReplyCapacityExceeded,
    /// The actor's current behavior does not accept this message type.
    UnhandledInCurrentState,
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
    Panicked,
    Rejected(MessageRejection),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponderErrorAction<Response, Error> {
    Respond(Response),
    Propagate(Error),
}

pub trait Actor: Sized + Send + 'static {
    /// The concrete error handled inside this Actor.
    ///
    /// Prefer a domain-specific `thiserror` enum and include
    /// [`ActorContextError`](crate::error::ActorContextError) with `#[from]` when the Actor uses
    /// fallible context operations. An unrecovered value is converted to
    /// [`ActorFailure`](crate::error::ActorFailure) only after it crosses the Actor boundary.
    type Error: StdError + Send + Sync + 'static;
    type Behavior: Behavior;

    fn initial_behavior(&self) -> Self::Behavior {
        Self::Behavior::default()
    }

    /// Observes every dequeued tell, request, and continuation before dispatch.
    ///
    /// The view exposes the immutable concrete payload through `downcast_ref`. This observational
    /// hook cannot reject or transform mailbox work. Typed behavior admission follows for tells and
    /// requests; internal continuations bypass behavior admission.
    fn before_message(&mut self, _ctx: &mut ActorContext<Self>, _message: MessageView<'_>) {}

    /// Observes the dispatch result after normal error/recovery handling has completed.
    fn after_message(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _metadata: &MessageMetadata,
        _outcome: MessageOutcome,
    ) {
    }

    fn started(
        &mut self,
        _ctx: &mut ActorContext<Self>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    fn stopping(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _reason: StopReason,
    ) -> impl Future<Output = Result<(), ActorStopError>> + Send {
        async { Ok(()) }
    }

    fn on_error<M>(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _metadata: &MessageMetadata,
        _error: &Self::Error,
    ) -> impl Future<Output = ()> + Send
    where
        M: Send + 'static,
    {
        async {}
    }

    /// Dispatches one admitted tell to its authored [`Handler`] implementation.
    ///
    /// The default is a direct trait call. Actor integrations may override this
    /// generic boundary to select generated middleware or hot-patch roots
    /// without changing individual `Handler<M>` bodies. An override must call
    /// the authored handler at most once when it selects the default path.
    fn dispatch_handler<M>(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        msg: M,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        Self: Handler<M>,
        M: Message,
    {
        <Self as Handler<M>>::handle(self, ctx, msg)
    }

    /// Dispatches one admitted request to its authored [`Responder`]
    /// implementation.
    ///
    /// The default is a direct trait call. Actor integrations may override this
    /// generic boundary to select generated middleware or hot-patch roots
    /// without changing individual `Responder<R>` bodies.
    fn dispatch_responder<R>(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        request: R,
        reply_to: ReplyTo<R::Response>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        Self: Responder<R>,
        R: Request,
    {
        <Self as Responder<R>>::respond(self, ctx, request, reply_to)
    }
}

pub trait Handler<M>: Actor
where
    M: Message,
{
    fn handle(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        msg: M,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait Responder<R>: Actor
where
    R: Request,
{
    fn respond(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        request: R,
        reply_to: ReplyTo<R::Response>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn respond_error(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        error: Self::Error,
    ) -> impl Future<Output = ResponderErrorAction<R::Response, Self::Error>> + Send {
        async { ResponderErrorAction::Propagate(error) }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    Requested,
    Passivated(PassivationReason),
    MailboxClosed,
    StartFailed,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorLifecycleState {
    Starting,
    Running,
    Passivating,
    Stopping,
    StopFailed,
    Stopped,
}

impl TryFrom<u8> for ActorLifecycleState {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            value if value == Self::Starting as u8 => Ok(Self::Starting),
            value if value == Self::Running as u8 => Ok(Self::Running),
            value if value == Self::Passivating as u8 => Ok(Self::Passivating),
            value if value == Self::Stopping as u8 => Ok(Self::Stopping),
            value if value == Self::StopFailed as u8 => Ok(Self::StopFailed),
            value if value == Self::Stopped as u8 => Ok(Self::Stopped),
            _ => Err(()),
        }
    }
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

#[derive(Debug, Clone)]
pub struct ChildActorOptions {
    pub mailbox: MailboxConfig,
    pub supervision: ChildSupervision,
    /// Execution policy used to run this child.
    pub execution: ActorExecutionPolicy,
    /// Affinity key required by [`crate::runtime::WorkerPlacement::Affinity`].
    pub scheduler_key: Option<SchedulerKey>,
}

impl Default for ChildActorOptions {
    fn default() -> Self {
        Self {
            mailbox: MailboxConfig::default(),
            supervision: ChildSupervision::default(),
            execution: ActorExecutionPolicy::TaskPerActor,
            scheduler_key: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ChildSupervision {
    #[default]
    StopChild,
    StopParent,
    RestartChild,
}
