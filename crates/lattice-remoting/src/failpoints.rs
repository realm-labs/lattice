//! Failpoint IDs owned by the remoting subsystem.

use lattice_failpoint::FailpointId;

pub const ASSOCIATION_AFTER_HANDSHAKE_BEFORE_CATALOGUE: FailpointId =
    FailpointId::new("association_after_handshake_before_catalogue");
pub const CONTROL_AFTER_OUTBOX_BEFORE_SOCKET_WRITE: FailpointId =
    FailpointId::new("control_after_outbox_before_socket_write");
pub const CONTROL_AFTER_REMOTE_APPLY_BEFORE_ACK: FailpointId =
    FailpointId::new("control_after_remote_apply_before_ack");
pub const WATCH_AFTER_INSTALL_BEFORE_ACK: FailpointId =
    FailpointId::new("watch_after_install_before_ack");
pub const WATCH_AFTER_TERMINATED_BEFORE_ACK: FailpointId =
    FailpointId::new("watch_after_terminated_before_ack");
pub const SHUTDOWN_AFTER_FENCE_BEFORE_TASK_JOIN: FailpointId =
    FailpointId::new("shutdown_after_fence_before_task_join");
