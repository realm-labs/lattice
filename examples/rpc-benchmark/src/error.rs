use std::time::Duration;
use thiserror::Error;

pub type BenchmarkResult<T> = Result<T, BenchmarkError>;

#[derive(Debug, Error)]
pub enum BenchmarkError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Service(#[from] lattice_service::LatticeServiceError),
    #[error(transparent)]
    Placement(#[from] lattice_placement::PlacementError),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
    #[error("service readiness signal was dropped")]
    ReadyDropped,
    #[error("missing generated client in service context: {client_type}")]
    MissingClient { client_type: &'static str },
    #[error("actor factory expected u64 actor id, got {actual:?}")]
    InvalidActorId { actual: lattice_core::ActorId },
    #[error("rpc failed: {message}")]
    Rpc { message: String },
    #[error("{operation} timed out after {timeout:?}")]
    Timeout {
        operation: &'static str,
        timeout: Duration,
    },
    #[error("benchmark child process exited early: {status}")]
    ChildExited { status: String },
}

impl From<lattice_rpc::RpcError> for BenchmarkError {
    fn from(error: lattice_rpc::RpcError) -> Self {
        Self::Rpc {
            message: error.to_string(),
        }
    }
}
