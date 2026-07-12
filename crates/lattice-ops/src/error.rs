#[derive(Debug, thiserror::Error)]
pub enum OpsError {
    #[error("duplicate operation {operation_id}")]
    DuplicateOperation { operation_id: String },
    #[error("unknown operation {operation_id}")]
    UnknownOperation { operation_id: String },
    #[error("duplicate outbox event")]
    DuplicateOutboxEvent,
    #[error("unknown outbox event")]
    UnknownOutboxEvent,
    #[error("metric label {label} is too high-cardinality")]
    HighCardinalityMetricLabel { label: String },
    #[error("admin query failed: {message}")]
    Admin { message: String },
    #[error("drain failed: {message}")]
    Drain { message: String },
}
