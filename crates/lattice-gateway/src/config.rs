use std::time::Duration;

use thiserror::Error;

use crate::server::{DEFAULT_MAX_CLIENT_FRAME_SIZE, DEFAULT_MAX_FRAME_READ_CHUNK};

pub const ABSOLUTE_MAX_CLIENT_FRAME_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayServerConfig {
    /// Largest client frame body accepted before the connection is failed.
    pub max_client_frame_size: usize,
    /// Bytes reserved per incremental read while assembling a client frame body.
    pub max_frame_read_chunk: usize,
    /// Client connections served concurrently before new ones are dropped.
    pub max_connections: usize,
    /// Time a connection may wait for the next frame header before it is closed.
    pub idle_timeout: Duration,
    /// Time budget for reading a frame body whose header already arrived.
    pub read_timeout: Duration,
    /// Time budget for writing a single reply frame.
    pub write_timeout: Duration,
    /// Delay before the first retry after a recoverable accept failure.
    pub accept_backoff_min: Duration,
    /// Upper bound of the exponential accept backoff.
    pub accept_backoff_max: Duration,
    /// Time in-flight connections are given to finish once accept stops.
    pub shutdown_drain_timeout: Duration,
}

impl Default for GatewayServerConfig {
    fn default() -> Self {
        Self {
            max_client_frame_size: DEFAULT_MAX_CLIENT_FRAME_SIZE,
            max_frame_read_chunk: DEFAULT_MAX_FRAME_READ_CHUNK,
            max_connections: 1024,
            idle_timeout: Duration::from_secs(60),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            accept_backoff_min: Duration::from_millis(10),
            accept_backoff_max: Duration::from_secs(1),
            shutdown_drain_timeout: Duration::from_secs(10),
        }
    }
}

impl GatewayServerConfig {
    pub fn validate(&self) -> Result<(), GatewayServerConfigError> {
        for (name, value) in [
            ("max_client_frame_size", self.max_client_frame_size),
            ("max_frame_read_chunk", self.max_frame_read_chunk),
            ("max_connections", self.max_connections),
        ] {
            if value == 0 {
                return Err(GatewayServerConfigError::Zero { name });
            }
        }
        if self.max_client_frame_size > ABSOLUTE_MAX_CLIENT_FRAME_SIZE {
            return Err(GatewayServerConfigError::FrameSize {
                actual: self.max_client_frame_size,
                maximum: ABSOLUTE_MAX_CLIENT_FRAME_SIZE,
            });
        }
        for (name, value) in [
            ("idle_timeout", self.idle_timeout),
            ("read_timeout", self.read_timeout),
            ("write_timeout", self.write_timeout),
            ("accept_backoff_min", self.accept_backoff_min),
            ("accept_backoff_max", self.accept_backoff_max),
            ("shutdown_drain_timeout", self.shutdown_drain_timeout),
        ] {
            if value.is_zero() {
                return Err(GatewayServerConfigError::ZeroDuration { name });
            }
        }
        if self.accept_backoff_min > self.accept_backoff_max {
            return Err(GatewayServerConfigError::AcceptBackoffOrder);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum GatewayServerConfigError {
    #[error("gateway limit {name} must be nonzero")]
    Zero { name: &'static str },
    #[error("gateway duration {name} must be nonzero")]
    ZeroDuration { name: &'static str },
    #[error("client frame size {actual} exceeds absolute maximum {maximum}")]
    FrameSize { actual: usize, maximum: usize },
    #[error("minimum accept backoff exceeds maximum accept backoff")]
    AcceptBackoffOrder,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_limits_are_finite_and_nonzero() {
        GatewayServerConfig::default().validate().unwrap();
    }

    #[test]
    fn zero_connection_limit_is_rejected() {
        let config = GatewayServerConfig {
            max_connections: 0,
            ..GatewayServerConfig::default()
        };
        assert_eq!(
            config.validate(),
            Err(GatewayServerConfigError::Zero {
                name: "max_connections"
            })
        );
    }

    #[test]
    fn oversized_client_frame_limit_is_rejected() {
        let config = GatewayServerConfig {
            max_client_frame_size: ABSOLUTE_MAX_CLIENT_FRAME_SIZE + 1,
            ..GatewayServerConfig::default()
        };
        assert_eq!(
            config.validate(),
            Err(GatewayServerConfigError::FrameSize {
                actual: ABSOLUTE_MAX_CLIENT_FRAME_SIZE + 1,
                maximum: ABSOLUTE_MAX_CLIENT_FRAME_SIZE,
            })
        );
    }

    #[test]
    fn inverted_accept_backoff_is_rejected() {
        let config = GatewayServerConfig {
            accept_backoff_min: Duration::from_secs(2),
            accept_backoff_max: Duration::from_secs(1),
            ..GatewayServerConfig::default()
        };
        assert_eq!(
            config.validate(),
            Err(GatewayServerConfigError::AcceptBackoffOrder)
        );
    }

    #[test]
    fn zero_idle_timeout_is_rejected() {
        let config = GatewayServerConfig {
            idle_timeout: Duration::ZERO,
            ..GatewayServerConfig::default()
        };
        assert_eq!(
            config.validate(),
            Err(GatewayServerConfigError::ZeroDuration {
                name: "idle_timeout"
            })
        );
    }
}
