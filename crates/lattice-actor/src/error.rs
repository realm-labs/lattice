use std::{error::Error as StdError, fmt, time::Duration};

use thiserror::Error;

use crate::{runtime::ActorExecutionPolicy, traits::ActorLifecycleState};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct ActorError {
    message: String,
}

impl ActorError {
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
    Handler(ActorError),
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

impl From<ReplyError> for ActorError {
    fn from(value: ReplyError) -> Self {
        Self::new(value.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum PipeToSelfError {
    #[error("actor deferred-operation capacity {capacity} is exhausted")]
    Capacity { capacity: usize },
}

impl From<PipeToSelfError> for ActorError {
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

impl<M> From<ActorTellError<M>> for ActorError {
    fn from(value: ActorTellError<M>) -> Self {
        Self::new(value.to_string())
    }
}

#[derive(Debug, Clone, Error)]
pub enum ActorActivationError {
    #[error("actor is already running or activating")]
    AlreadyExists,
    #[error("activation waiter capacity exceeded")]
    WaiterCapacityExceeded,
    #[error("timed out waiting {timeout:?} for actor activation")]
    WaiterTimeout { timeout: Duration },
    #[error("actor activation failed: {0}")]
    ActivationFailed(ActorError),
    #[error("actor activation is retained after stopping persistence failed")]
    RetainedStopFailure,
    #[error("actor activation is quarantined after external authority loss")]
    Quarantined,
}

#[derive(Debug, Clone, Error)]
pub enum ActorSpawnError {
    #[error("unsupported actor execution policy: {policy:?}")]
    UnsupportedExecutionPolicy { policy: ActorExecutionPolicy },
    #[error("invalid actor execution policy: {reason}")]
    InvalidExecutionPolicy { reason: &'static str },
    #[error("actor executor failed to start: {reason}")]
    ExecutorStartFailed { reason: &'static str },
}
