use lattice_rpc::RpcError;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum GatewayError {
    #[error("client frame is too short")]
    FrameTooShort,
    #[error("duplicate gateway route for msg_id {msg_id}")]
    DuplicateRoute { msg_id: u32 },
    #[error("unexpected msg_id: expected {expected}, got {actual}")]
    UnexpectedMessageId { expected: u32, actual: u32 },
    #[error("unknown gateway msg_id {msg_id}")]
    UnknownMessageId { msg_id: u32 },
    #[error("failed to decode client payload: {0}")]
    DecodePayload(String),
    #[error("rpc failed: {0}")]
    Rpc(RpcError),
    #[error("unknown gateway session {session_id}")]
    UnknownSession { session_id: String },
    #[error(
        "stale gateway session {session_id}: expected epoch {expected_epoch}, got {actual_epoch}"
    )]
    StaleSession {
        session_id: String,
        expected_epoch: u64,
        actual_epoch: u64,
    },
    #[error("gateway rate limit exceeded")]
    RateLimited,
    #[error("gateway load shed: concurrency limit exceeded")]
    LoadShed,
}
