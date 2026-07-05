use lattice_rpc::RpcError;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EventBusError {
    #[error("event handler failed: {0}")]
    Handler(String),
    #[error("failed to decode event payload as {message_type}: {reason}")]
    Decode {
        message_type: &'static str,
        reason: String,
    },
    #[error("event is missing actor routing field {field}")]
    MissingActorTarget { field: &'static str },
    #[error("event actor delivery failed: {0}")]
    ActorDelivery(String),
}

impl EventBusError {
    pub(crate) fn from_rpc(error: RpcError) -> Self {
        Self::ActorDelivery(error.to_string())
    }
}
