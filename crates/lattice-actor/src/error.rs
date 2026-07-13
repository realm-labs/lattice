use std::error::Error as StdError;
use std::time::Duration;

use thiserror::Error;

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
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ActorCallError {
    #[error("actor mailbox is full")]
    MailboxFull,
    #[error("actor mailbox is closed")]
    MailboxClosed,
    #[error("actor dropped the response before replying")]
    ResponseDropped,
    #[error("actor ask deadline elapsed before a response completed")]
    DeadlineExceeded,
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

#[derive(Debug, Error)]
pub enum ActorTellError {
    #[error("actor mailbox is full")]
    MailboxFull,
    #[error("actor mailbox is closed")]
    MailboxClosed,
}

impl From<ActorTellError> for ActorError {
    fn from(value: ActorTellError) -> Self {
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
}

#[derive(Debug, Clone, Error)]
pub enum ActorSpawnError {
    #[error("unsupported actor execution policy: {policy:?}")]
    UnsupportedExecutionPolicy {
        policy: crate::runtime::ActorExecutionPolicy,
    },
    #[error("invalid actor execution policy: {reason}")]
    InvalidExecutionPolicy { reason: &'static str },
    #[error("actor executor failed to start: {reason}")]
    ExecutorStartFailed { reason: &'static str },
}
