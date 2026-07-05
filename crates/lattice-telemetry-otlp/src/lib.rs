mod config;
mod error;
mod guard;
mod resource;
mod telemetry;

pub use config::{OtlpTraceConfig, TelemetryConfig};
pub use error::TelemetryInitError;
pub use guard::TelemetryGuard;
pub use resource::TelemetryResource;
pub use telemetry::LatticeTelemetry;

#[cfg(test)]
mod tests;
