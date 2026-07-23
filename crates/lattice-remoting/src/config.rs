use std::time::Duration;

use thiserror::Error;

pub const ABSOLUTE_MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;
pub const ABSOLUTE_MAX_READY_WRITE_BATCH_FRAMES: usize = 512;
pub const ABSOLUTE_MAX_READY_READ_BATCH_FRAMES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemotingConfig {
    pub max_associations: usize,
    pub bulk_stripes: usize,
    pub max_frame_size: usize,
    pub control_queue_frames: usize,
    pub interactive_queue_frames: usize,
    pub bulk_queue_frames_per_stripe: usize,
    pub max_outbound_bytes_per_association: usize,
    pub max_outbound_bytes_per_node: usize,
    pub max_pending_asks: usize,
    pub max_control_outbox_frames: usize,
    pub max_control_outbox_bytes: usize,
    pub max_protocols_per_peer: usize,
    pub max_cached_exact_targets_per_lane: usize,
    pub max_prepared_exact_tell_routes: usize,
    pub socket_read_ahead_bytes: usize,
    pub max_ready_write_batch_frames: usize,
    pub max_ready_read_batch_frames: usize,
    pub max_coalesced_write_batch_bytes: usize,
    pub connect_timeout: Duration,
    pub reconnect_backoff_min: Duration,
    pub reconnect_backoff_max: Duration,
    pub heartbeat_interval: Duration,
    pub heartbeat_miss_limit: u32,
    pub idle_data_connection_timeout: Duration,
    pub shutdown_timeout: Duration,
}

impl Default for RemotingConfig {
    fn default() -> Self {
        Self {
            max_associations: 256,
            bulk_stripes: 1,
            max_frame_size: 256 * 1024,
            control_queue_frames: 1024,
            interactive_queue_frames: 4096,
            bulk_queue_frames_per_stripe: 8192,
            max_outbound_bytes_per_association: 16 * 1024 * 1024,
            max_outbound_bytes_per_node: 256 * 1024 * 1024,
            max_pending_asks: 4096,
            max_control_outbox_frames: 1024,
            max_control_outbox_bytes: 4 * 1024 * 1024,
            max_protocols_per_peer: 1024,
            max_cached_exact_targets_per_lane: 1024,
            max_prepared_exact_tell_routes: 16 * 1024,
            socket_read_ahead_bytes: 64 * 1024,
            max_ready_write_batch_frames: 256,
            max_ready_read_batch_frames: 1,
            max_coalesced_write_batch_bytes: 128 * 1024,
            connect_timeout: Duration::from_secs(3),
            reconnect_backoff_min: Duration::from_millis(100),
            reconnect_backoff_max: Duration::from_secs(5),
            heartbeat_interval: Duration::from_secs(2),
            heartbeat_miss_limit: 3,
            idle_data_connection_timeout: Duration::from_secs(60),
            shutdown_timeout: Duration::from_secs(10),
        }
    }
}

impl RemotingConfig {
    pub fn validate(&self) -> Result<(), RemotingConfigError> {
        for (name, value) in [
            ("max_associations", self.max_associations),
            ("max_frame_size", self.max_frame_size),
            ("control_queue_frames", self.control_queue_frames),
            ("interactive_queue_frames", self.interactive_queue_frames),
            (
                "bulk_queue_frames_per_stripe",
                self.bulk_queue_frames_per_stripe,
            ),
            (
                "max_outbound_bytes_per_association",
                self.max_outbound_bytes_per_association,
            ),
            (
                "max_outbound_bytes_per_node",
                self.max_outbound_bytes_per_node,
            ),
            ("max_pending_asks", self.max_pending_asks),
            ("max_control_outbox_frames", self.max_control_outbox_frames),
            ("max_control_outbox_bytes", self.max_control_outbox_bytes),
            ("max_protocols_per_peer", self.max_protocols_per_peer),
            (
                "max_cached_exact_targets_per_lane",
                self.max_cached_exact_targets_per_lane,
            ),
            ("socket_read_ahead_bytes", self.socket_read_ahead_bytes),
            (
                "max_ready_write_batch_frames",
                self.max_ready_write_batch_frames,
            ),
            (
                "max_ready_read_batch_frames",
                self.max_ready_read_batch_frames,
            ),
            (
                "max_coalesced_write_batch_bytes",
                self.max_coalesced_write_batch_bytes,
            ),
        ] {
            if value == 0 {
                return Err(RemotingConfigError::Zero { name });
            }
        }
        if !(1..=4).contains(&self.bulk_stripes) {
            return Err(RemotingConfigError::BulkStripeCount {
                actual: self.bulk_stripes,
            });
        }
        if self.max_frame_size > ABSOLUTE_MAX_FRAME_SIZE {
            return Err(RemotingConfigError::FrameSize {
                actual: self.max_frame_size,
                maximum: ABSOLUTE_MAX_FRAME_SIZE,
            });
        }
        if self.max_ready_write_batch_frames > ABSOLUTE_MAX_READY_WRITE_BATCH_FRAMES {
            return Err(RemotingConfigError::WriteBatchFrames {
                actual: self.max_ready_write_batch_frames,
                maximum: ABSOLUTE_MAX_READY_WRITE_BATCH_FRAMES,
            });
        }
        if self.max_ready_read_batch_frames > ABSOLUTE_MAX_READY_READ_BATCH_FRAMES {
            return Err(RemotingConfigError::ReadBatchFrames {
                actual: self.max_ready_read_batch_frames,
                maximum: ABSOLUTE_MAX_READY_READ_BATCH_FRAMES,
            });
        }
        if self.max_outbound_bytes_per_association > self.max_outbound_bytes_per_node {
            return Err(RemotingConfigError::AssociationBytesExceedNodeBytes);
        }
        if self.reconnect_backoff_min > self.reconnect_backoff_max {
            return Err(RemotingConfigError::ReconnectBackoffOrder);
        }
        for (name, value) in [
            ("connect_timeout", self.connect_timeout),
            ("reconnect_backoff_min", self.reconnect_backoff_min),
            ("reconnect_backoff_max", self.reconnect_backoff_max),
            ("heartbeat_interval", self.heartbeat_interval),
            (
                "idle_data_connection_timeout",
                self.idle_data_connection_timeout,
            ),
            ("shutdown_timeout", self.shutdown_timeout),
        ] {
            if value.is_zero() {
                return Err(RemotingConfigError::ZeroDuration { name });
            }
        }
        if self.heartbeat_miss_limit == 0 {
            return Err(RemotingConfigError::Zero {
                name: "heartbeat_miss_limit",
            });
        }
        Ok(())
    }

    pub fn physical_connections_per_association(&self) -> usize {
        2 + self.bulk_stripes
    }

    pub fn required_socket_budget(&self) -> usize {
        self.max_associations
            .saturating_mul(self.physical_connections_per_association())
            .saturating_add(1)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RemotingConfigError {
    #[error("remoting limit {name} must be nonzero")]
    Zero { name: &'static str },
    #[error("remoting duration {name} must be nonzero")]
    ZeroDuration { name: &'static str },
    #[error("bulk stripe count must be in 1..=4, got {actual}")]
    BulkStripeCount { actual: usize },
    #[error("frame size {actual} exceeds absolute maximum {maximum}")]
    FrameSize { actual: usize, maximum: usize },
    #[error("ready write batch frame count {actual} exceeds maximum {maximum}")]
    WriteBatchFrames { actual: usize, maximum: usize },
    #[error("ready read batch frame count {actual} exceeds maximum {maximum}")]
    ReadBatchFrames { actual: usize, maximum: usize },
    #[error("per-association outbound bytes exceed the node-wide bound")]
    AssociationBytesExceedNodeBytes,
    #[error("minimum reconnect backoff exceeds maximum reconnect backoff")]
    ReconnectBackoffOrder,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_limits_are_finite_and_nonzero() {
        let config = RemotingConfig::default();
        config.validate().unwrap();
        assert_eq!(config.physical_connections_per_association(), 3);
        assert_eq!(config.required_socket_budget(), 769);
    }
}
