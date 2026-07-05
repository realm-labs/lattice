pub mod config;
pub mod error;
pub mod guard;
pub mod resource;
pub mod telemetry;

pub use config::TelemetryConfig;
pub use error::TelemetryInitError;
pub use guard::TelemetryGuard;
pub use telemetry::LatticeTelemetry;

#[cfg(test)]
mod tests;
