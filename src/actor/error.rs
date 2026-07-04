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

#[derive(Debug, Error)]
pub enum ActorCallError {
    #[error("actor mailbox is full")]
    MailboxFull,
    #[error("actor mailbox is closed")]
    MailboxClosed,
    #[error("actor dropped the response before replying")]
    ResponseDropped,
    #[error("actor handler failed: {0}")]
    Handler(ActorError),
}

#[derive(Debug, Error)]
pub enum ActorTellError {
    #[error("actor mailbox is full")]
    MailboxFull,
    #[error("actor mailbox is closed")]
    MailboxClosed,
    #[error("actor dropped the response before acknowledging tell")]
    ResponseDropped,
    #[error("actor handler failed: {0}")]
    Handler(ActorError),
}
