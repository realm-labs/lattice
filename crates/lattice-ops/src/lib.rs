pub mod admin;
pub mod error;
pub mod operation;
pub mod ops_config;
pub mod outbox;
pub mod scheduler;
pub mod shutdown;
pub mod telemetry;

pub use error::OpsError;
pub use ops_config::{AdminHttpConfig, TelemetryConfig};
pub use scheduler::ServiceScheduler;
pub use shutdown::{GracefulShutdown, GracefulShutdownReport, ShutdownTrigger};

#[cfg(test)]
mod tests;
