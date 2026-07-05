use lattice_rpc::RpcError;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EventBusError {
    #[error("event handler failed: {0}")]
    Handler(String),
    #[error("event actor delivery failed: {0}")]
    ActorDelivery(String),
}

impl EventBusError {
    pub(crate) fn from_rpc(error: RpcError) -> Self {
        Self::ActorDelivery(error.to_string())
    }
}
