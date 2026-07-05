use lattice_core::{Epoch, RequestId};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RpcError {
    #[error("target owner not found")]
    NoOwner,
    #[error("route target is not owner")]
    NotOwner { expected_epoch: Option<Epoch> },
    #[error("request was fenced by newer owner")]
    Fenced { current_epoch: Epoch },
    #[error("actor is unavailable")]
    ActorUnavailable,
    #[error("mailbox is full")]
    MailboxFull,
    #[error("rpc result is unknown for {method} request {request_id}: {message}")]
    UnknownResult {
        method: &'static str,
        request_id: RequestId,
        message: String,
    },
    #[error("business error: {0}")]
    Business(String),
}
