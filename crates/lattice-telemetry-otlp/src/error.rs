#[derive(Debug, thiserror::Error)]
pub enum TelemetryInitError {
    #[error("invalid telemetry filter: {message}")]
    Filter { message: String },
    #[error("failed to build telemetry exporter: {message}")]
    Exporter { message: String },
    #[error("failed to install telemetry subscriber: {message}")]
    Subscriber { message: String },
}
