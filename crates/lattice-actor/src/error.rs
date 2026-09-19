use std::{error::Error as StdError, fmt};

use thiserror::Error;

use crate::{runtime::ActorExecutionPolicy, traits::ActorLifecycleState};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{message}")]
/// Diagnostic summary of an Actor error that crossed a runtime boundary.
///
/// The concrete [`crate::traits::Actor::Error`] remains available to Actor error hooks. Once an
/// unrecovered error is sent to a caller or activation coordinator, only this stable textual
/// representation is retained.
pub struct ActorFailure {
    message: String,
}

impl ActorFailure {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn from_error(error: impl StdError + Send + Sync + 'static) -> Self {
        Self::new(error.to_string())
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

/// A structured failure produced by an operation on [`crate::context::ActorContext`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ActorContextError {
    #[error("RestartChild supervision requires spawn_child_with_factory")]
    RestartChildRequiresFactory,
    #[error("child actor {key} already exists")]
    DuplicateChild { key: String },
    #[error("actor watch capacity {capacity} is exhausted")]
    WatchCapacity { capacity: usize },
    #[error("actor watch registration failed: {reason}")]
    WatchRegistration { reason: String },
    #[error(transparent)]
    Spawn(#[from] ActorSpawnError),
    #[error(transparent)]
    Reply(#[from] ReplyError),
    #[error(transparent)]
    Deferred(#[from] PipeToSelfError),
    #[error("actor mailbox is full")]
    TellMailboxFull,
    #[error("actor mailbox is closed")]
    TellMailboxClosed,
    #[error("actor does not admit business traffic while lifecycle state is {state:?}")]
    TellLifecycleUnavailable { state: ActorLifecycleState },
}

impl From<ActorContextError> for ActorFailure {
    fn from(value: ActorContextError) -> Self {
        Self::from_error(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct ActorStopError {
    message: String,
}

impl ActorStopError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ActorCallError {
    #[error("actor ask timeout cannot be represented as a deadline")]
    InvalidTimeout,
    #[error("actor mailbox is full")]
    MailboxFull,
    #[error("actor mailbox is closed")]
    MailboxClosed,
    #[error("actor panicked while processing its execution callback")]
    ActorPanicked,
    #[error("actor does not admit business traffic while lifecycle state is {state:?}")]
    LifecycleUnavailable { state: ActorLifecycleState },
    #[error("actor dropped the response before replying")]
    ResponseDropped,
    #[error("actor ask deadline elapsed before a response completed")]
    DeadlineExceeded,
    #[error("actor does not handle the message in its current state")]
    UnhandledInCurrentState,
    #[error("actor handler failed: {0}")]
    Handler(ActorFailure),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ReplyError {
    #[error("reply has already been completed or invalidated")]
    AlreadyCompleted,
    #[error("ask caller is no longer waiting for a response")]
    ResponseDropped,
    #[error("ask deadline elapsed before the response was sent")]
    DeadlineExceeded,
}

impl From<ReplyError> for ActorFailure {
    fn from(value: ReplyError) -> Self {
        Self::new(value.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum PipeToSelfError {
    #[error("actor deferred-operation capacity {capacity} is exhausted")]
    Capacity { capacity: usize },
}

impl From<PipeToSelfError> for ActorFailure {
    fn from(value: PipeToSelfError) -> Self {
        Self::new(value.to_string())
    }
}

/// A one-way message that could not be admitted to an Actor mailbox.
///
/// Every variant retains the original message so callers can retry, reroute,
/// or handle it without requiring `M: Clone`.
pub enum ActorTellError<M> {
    MailboxFull(M),
    MailboxClosed(M),
    LifecycleUnavailable {
        state: ActorLifecycleState,
        message: M,
    },
}

impl<M> ActorTellError<M> {
    /// Borrows the message that was not delivered.
    pub fn message(&self) -> &M {
        match self {
            Self::MailboxFull(message)
            | Self::MailboxClosed(message)
            | Self::LifecycleUnavailable { message, .. } => message,
        }
    }

    /// Returns ownership of the message that was not delivered.
    pub fn into_message(self) -> M {
        match self {
            Self::MailboxFull(message)
            | Self::MailboxClosed(message)
            | Self::LifecycleUnavailable { message, .. } => message,
        }
    }
}

impl<M> fmt::Debug for ActorTellError<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MailboxFull(_) => formatter.write_str("MailboxFull(..)"),
            Self::MailboxClosed(_) => formatter.write_str("MailboxClosed(..)"),
            Self::LifecycleUnavailable { state, .. } => formatter
                .debug_struct("LifecycleUnavailable")
                .field("state", state)
                .field("message", &"..")
                .finish(),
        }
    }
}

impl<M> fmt::Display for ActorTellError<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MailboxFull(_) => formatter.write_str("actor mailbox is full"),
            Self::MailboxClosed(_) => formatter.write_str("actor mailbox is closed"),
            Self::LifecycleUnavailable { state, .. } => write!(
                formatter,
                "actor does not admit business traffic while lifecycle state is {state:?}"
            ),
        }
    }
}

impl<M> StdError for ActorTellError<M> {}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ActorAdminError {
    #[error("actor admin operation {operation} is invalid while lifecycle state is {state:?}")]
    InvalidState {
        operation: &'static str,
        state: ActorLifecycleState,
    },
    #[error("actor system mailbox is full")]
    MailboxFull,
    #[error("actor system mailbox is closed")]
    MailboxClosed,
    #[error("actor stopping persistence failed: {0}")]
    StopFailed(ActorStopError),
    #[error("actor admin operation response was dropped")]
    ResponseDropped,
}

impl<M> From<ActorTellError<M>> for ActorFailure {
    fn from(value: ActorTellError<M>) -> Self {
        Self::new(value.to_string())
    }
}

impl<M> From<ActorTellError<M>> for ActorContextError {
    fn from(value: ActorTellError<M>) -> Self {
        match value {
            ActorTellError::MailboxFull(_) => Self::TellMailboxFull,
            ActorTellError::MailboxClosed(_) => Self::TellMailboxClosed,
            ActorTellError::LifecycleUnavailable { state, .. } => {
                Self::TellLifecycleUnavailable { state }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ActorSpawnError {
    #[error("unsupported actor execution policy: {policy:?}")]
    UnsupportedExecutionPolicy { policy: ActorExecutionPolicy },
    #[error("invalid actor execution policy: {reason}")]
    InvalidExecutionPolicy { reason: &'static str },
    #[error("actor executor failed to start: {reason}")]
    ExecutorStartFailed { reason: &'static str },
}
