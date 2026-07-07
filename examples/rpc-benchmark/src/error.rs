use std::time::Duration;

use lattice_core::id::ActorId;
use lattice_placement::error::PlacementError;
use lattice_rpc::error::RpcError;
use lattice_service::error::LatticeServiceError;
use thiserror::Error;

pub type BenchmarkResult<T> = Result<T, BenchmarkError>;

#[derive(Debug, Error)]
pub enum BenchmarkError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Service(#[from] LatticeServiceError),
    #[error(transparent)]
    Placement(#[from] PlacementError),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
    #[error("service readiness signal was dropped")]
    ReadyDropped,
    #[error("missing generated client in service context: {client_type}")]
    MissingClient { client_type: &'static str },
    #[error("actor factory expected u64 actor id, got {actual:?}")]
    InvalidActorId { actual: ActorId },
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

impl From<RpcError> for BenchmarkError {
    fn from(error: RpcError) -> Self {
        Self::Rpc {
            message: error.to_string(),
        }
    }
}
