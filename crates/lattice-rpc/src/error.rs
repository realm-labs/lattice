use lattice_core::Epoch;

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
    #[error("rpc timed out; result may be unknown")]
    TimeoutUnknown,
    #[error("business error: {0}")]
    Business(String),
}
