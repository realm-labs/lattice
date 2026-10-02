//! Resource limits and timeouts for remoting.
//!
//! Each association has one Control lane, one Interactive lane and a configurable number
//! of Bulk lanes. Queue limits bound frame counts; byte budgets independently bound retained
//! outbound payloads. The endpoint shares one connection semaphore across inbound setup,
//! outbound dialing and established lanes.
//!
//! Data lanes may release their sockets after an idle timeout while Control remains connected.
//! The initial establishment timeout retires a generation that has never become active;
//! individual dial attempts use the shorter connect timeout. Neither timeout is an idle lifetime
//! for an already active association.

#![deny(missing_docs)]

use std::time::Duration;

use thiserror::Error;

/// Largest encoded frame body accepted by the remoting implementation, in bytes.
pub const ABSOLUTE_MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;
/// Largest number of ready frames that may be collected in one outbound batch.
pub const ABSOLUTE_MAX_READY_WRITE_BATCH_FRAMES: usize = 512;
/// Largest number of already buffered frames processed in one inbound batch.
pub const ABSOLUTE_MAX_READY_READ_BATCH_FRAMES: usize = 128;

/// Minimum number of Bulk lanes in an association's configured lane group.
pub const MIN_BULK_STRIPES: usize = 1;
/// Maximum number of Bulk lanes supported per association.
pub const ABSOLUTE_MAX_BULK_STRIPES: usize = 4;
/// Number of Bulk lanes used by the default remoting configuration.
pub const DEFAULT_BULK_STRIPES: usize = 1;

/// One Control connection and one Interactive connection per association.
const FIXED_CONNECTIONS_PER_ASSOCIATION: usize = 2;

/// Bounds the resources and waiting time used by an endpoint and its associations.
///
/// Start with [`Self::default`] and override the limits needed by the application.
/// Constructors validate these settings; changing a value does not resize an existing endpoint.
///
/// # Examples
///
/// ```
/// use lattice_remoting::config::RemotingConfig;
///
/// let config = RemotingConfig {
///     bulk_stripes: 2,
///     ..RemotingConfig::default()
/// };
/// config.validate()?;
/// assert_eq!(config.physical_connections_per_association(), 4);
/// # Ok::<(), lattice_remoting::config::RemotingConfigError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemotingConfig {
    /// Maximum number of registered association generations. Defaults to 256.
    pub max_associations: usize,
    /// Number of Bulk lanes per association, from 1 through 4. Defaults to 1.
    pub bulk_stripes: usize,
    /// Maximum encoded frame body size, including its header. Defaults to 256 KiB.
    pub max_frame_size: usize,
    /// Number of frames retained in each Control lane queue. Defaults to 1,024.
    pub control_queue_frames: usize,
    /// Number of frames retained in each Interactive lane queue. Defaults to 4,096.
    pub interactive_queue_frames: usize,
    /// Number of frames retained in each Bulk lane queue. Defaults to 8,192.
    pub bulk_queue_frames_per_stripe: usize,
    /// Maximum queued outbound payload bytes per association. Defaults to 16 MiB.
    pub max_outbound_bytes_per_association: usize,
    /// Maximum queued outbound payload bytes shared by a manager's associations. Defaults to 256 MiB.
    pub max_outbound_bytes_per_node: usize,
    /// Maximum pending outbound asks; also supplies the endpoint's per-lane inbound ask limit.
    pub max_pending_asks: usize,
    /// Maximum unacknowledged reliable control commands per association. Defaults to 1,024.
    pub max_control_outbox_frames: usize,
    /// Maximum reliable control payload bytes retained per association. Defaults to 4 MiB.
    pub max_control_outbox_bytes: usize,
    /// Maximum tracked reliable control streams per association. Defaults to 1,024.
    pub max_control_streams: usize,
    /// Maximum unacknowledged commands in one reliable control stream. Defaults to 512.
    pub max_control_outbox_frames_per_stream: usize,
    /// Maximum payload bytes retained in one reliable control stream. Defaults to 2 MiB.
    pub max_control_outbox_bytes_per_stream: usize,
    /// Maximum protocol descriptors accepted in a peer catalogue. Defaults to 1,024.
    pub max_protocols_per_peer: usize,
    /// Maximum cached inbound exact-target resolutions per lane. Defaults to 1,024.
    pub max_cached_exact_targets_per_lane: usize,
    /// Maximum cached prepared outbound exact-Tell routes; zero disables caching.
    pub max_prepared_exact_tell_routes: usize,
    /// Bytes reserved for buffered socket reads per connection. Defaults to 64 KiB.
    pub socket_read_ahead_bytes: usize,
    /// Maximum ready frames gathered in one outbound batch. Defaults to 256.
    pub max_ready_write_batch_frames: usize,
    /// Maximum ready frames dispatched in one inbound batch. Defaults to 1.
    pub max_ready_read_batch_frames: usize,
    /// Byte limit used to coalesce outbound writes. Defaults to 128 KiB.
    pub max_coalesced_write_batch_bytes: usize,
    /// Deadline for an outbound connection attempt or bootstrap exchange. Defaults to 3 seconds.
    pub connect_timeout: Duration,
    /// Maximum initial association establishment time, and the absolute inbound
    /// setup budget from TCP accept through TLS, handshake and catalogue/bootstrap.
    /// Defaults to 30 seconds.
    pub establishing_timeout: Duration,
    /// Initial delay before retrying a failed lane connection. Defaults to 100 milliseconds.
    pub reconnect_backoff_min: Duration,
    /// Upper bound on exponential connection retry delays. Defaults to 5 seconds.
    pub reconnect_backoff_max: Duration,
    /// Interval between Control heartbeats. Defaults to 2 seconds.
    pub heartbeat_interval: Duration,
    /// Number of heartbeat intervals of silence tolerated on Control. Defaults to 3.
    pub heartbeat_miss_limit: u32,
    /// Maximum retry age after a reliable command's first transient application failure.
    ///
    /// Also bounds configured reliable outbox waits. Defaults to 30 seconds.
    pub control_apply_retry_timeout: Duration,
    /// Idle time before an Interactive or Bulk socket sleeps. Defaults to 60 seconds.
    ///
    /// Control does not sleep. Interactive also stays awake while inbound asks or pending
    /// outbound asks for the association remain unfinished.
    pub idle_data_connection_timeout: Duration,
    /// Time allowed for endpoint shutdown before remaining tasks are cancelled.
    ///
    /// Cancellation is followed by resource cleanup, so this is not a strict bound on the
    /// entire shutdown call. Defaults to 10 seconds.
    pub shutdown_timeout: Duration,
}

impl Default for RemotingConfig {
    fn default() -> Self {
        Self {
            max_associations: 256,
            bulk_stripes: DEFAULT_BULK_STRIPES,
            max_frame_size: 256 * 1024,
            control_queue_frames: 1024,
            interactive_queue_frames: 4096,
            bulk_queue_frames_per_stripe: 8192,
            max_outbound_bytes_per_association: 16 * 1024 * 1024,
            max_outbound_bytes_per_node: 256 * 1024 * 1024,
            max_pending_asks: 4096,
            max_control_outbox_frames: 1024,
            max_control_outbox_bytes: 4 * 1024 * 1024,
            max_control_streams: 1024,
            max_control_outbox_frames_per_stream: 512,
            max_control_outbox_bytes_per_stream: 2 * 1024 * 1024,
            max_protocols_per_peer: 1024,
            max_cached_exact_targets_per_lane: 1024,
            max_prepared_exact_tell_routes: 16 * 1024,
            socket_read_ahead_bytes: 64 * 1024,
            max_ready_write_batch_frames: 256,
            max_ready_read_batch_frames: 1,
            max_coalesced_write_batch_bytes: 128 * 1024,
            connect_timeout: Duration::from_secs(3),
            establishing_timeout: Duration::from_secs(30),
            reconnect_backoff_min: Duration::from_millis(100),
            reconnect_backoff_max: Duration::from_secs(5),
            heartbeat_interval: Duration::from_secs(2),
            heartbeat_miss_limit: 3,
            control_apply_retry_timeout: Duration::from_secs(30),
            idle_data_connection_timeout: Duration::from_secs(60),
            shutdown_timeout: Duration::from_secs(10),
        }
    }
}

impl RemotingConfig {
    /// Checks that resource bounds and timeout relationships are supported.
    ///
    /// # Errors
    ///
    /// Returns an error for a required zero limit or timeout, an unsupported stripe or batch
    /// count, or a per-association/per-stream budget exceeding its enclosing budget.
    pub fn validate(&self) -> Result<(), RemotingConfigError> {
        let nonzero_limits = [
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
            ("max_control_streams", self.max_control_streams),
            (
                "max_control_outbox_frames_per_stream",
                self.max_control_outbox_frames_per_stream,
            ),
            (
                "max_control_outbox_bytes_per_stream",
                self.max_control_outbox_bytes_per_stream,
            ),
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
        ];
        for (name, value) in nonzero_limits {
            if value == 0 {
                return Err(RemotingConfigError::Zero { name });
            }
        }
        if !(MIN_BULK_STRIPES..=ABSOLUTE_MAX_BULK_STRIPES).contains(&self.bulk_stripes) {
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
        if self.max_control_outbox_frames_per_stream > self.max_control_outbox_frames
            || self.max_control_outbox_bytes_per_stream > self.max_control_outbox_bytes
        {
            return Err(RemotingConfigError::StreamOutboxExceedsAssociation);
        }
        if self.reconnect_backoff_min > self.reconnect_backoff_max {
            return Err(RemotingConfigError::ReconnectBackoffOrder);
        }
        let nonzero_durations = [
            ("connect_timeout", self.connect_timeout),
            ("establishing_timeout", self.establishing_timeout),
            ("reconnect_backoff_min", self.reconnect_backoff_min),
            ("reconnect_backoff_max", self.reconnect_backoff_max),
            ("heartbeat_interval", self.heartbeat_interval),
            (
                "control_apply_retry_timeout",
                self.control_apply_retry_timeout,
            ),
            (
                "idle_data_connection_timeout",
                self.idle_data_connection_timeout,
            ),
            ("shutdown_timeout", self.shutdown_timeout),
        ];
        for (name, value) in nonzero_durations {
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

    /// Returns the socket count of a fully connected association: `2 + bulk_stripes`.
    ///
    /// Sleeping data lanes use fewer sockets without changing this configured count.
    pub fn physical_connections_per_association(&self) -> usize {
        FIXED_CONNECTIONS_PER_ASSOCIATION + self.bulk_stripes
    }

    /// Maximum concurrent connections admitted by the endpoint's semaphore.
    pub fn connection_capacity(&self) -> usize {
        self.max_associations
            .saturating_mul(self.physical_connections_per_association())
    }

    /// Socket count for full connection capacity plus one TCP listener.
    ///
    /// This reports resource requirements; admission is enforced separately using
    /// [`Self::connection_capacity`].
    pub fn required_socket_budget(&self) -> usize {
        self.connection_capacity().saturating_add(1)
    }

    /// The longest silence a healthy peer may produce before its own control lane fails.
    ///
    /// Every control lane both sends a heartbeat each `heartbeat_interval` and gives up
    /// once `heartbeat_miss_limit` of them go unanswered, so an association that has seen
    /// no peer bytes for this long is one whose lane bookkeeping can no longer be trusted.
    pub fn peer_liveness_window(&self) -> Duration {
        self.heartbeat_interval
            .saturating_mul(self.heartbeat_miss_limit)
    }
}

/// An unsupported resource limit or timeout in [`RemotingConfig`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RemotingConfigError {
    /// A required count or byte limit is zero.
    #[error("remoting limit {name} must be nonzero")]
    Zero {
        /// Name of the configuration field.
        name: &'static str,
    },
    /// A required timeout is zero.
    #[error("remoting duration {name} must be nonzero")]
    ZeroDuration {
        /// Name of the configuration field.
        name: &'static str,
    },
    /// The number of Bulk lanes is outside the supported range.
    #[error(
        "bulk stripe count must be in {minimum}..={maximum}, got {actual}",
        minimum = MIN_BULK_STRIPES,
        maximum = ABSOLUTE_MAX_BULK_STRIPES
    )]
    BulkStripeCount {
        /// Requested number of Bulk lanes.
        actual: usize,
    },
    /// The maximum frame body exceeds the implementation limit.
    #[error("frame size {actual} exceeds absolute maximum {maximum}")]
    FrameSize {
        /// Requested frame body limit in bytes.
        actual: usize,
        /// Largest supported frame body in bytes.
        maximum: usize,
    },
    /// An outbound batch contains more frames than supported.
    #[error("ready write batch frame count {actual} exceeds maximum {maximum}")]
    WriteBatchFrames {
        /// Requested frame count.
        actual: usize,
        /// Largest supported frame count.
        maximum: usize,
    },
    /// An inbound batch contains more frames than supported.
    #[error("ready read batch frame count {actual} exceeds maximum {maximum}")]
    ReadBatchFrames {
        /// Requested frame count.
        actual: usize,
        /// Largest supported frame count.
        maximum: usize,
    },
    /// One association's outbound byte budget exceeds the shared node budget.
    #[error("per-association outbound bytes exceed the node-wide bound")]
    AssociationBytesExceedNodeBytes,
    /// A reliable control stream's budget exceeds the association's outbox budget.
    #[error("per-stream reliable control outbox exceeds its association-wide bound")]
    StreamOutboxExceedsAssociation,
    /// The initial retry delay exceeds the maximum retry delay.
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
        assert_eq!(config.connection_capacity(), 768);
        assert_eq!(config.required_socket_budget(), 769);
    }
}
