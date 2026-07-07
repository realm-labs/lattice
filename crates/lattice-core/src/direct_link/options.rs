use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DirectLinkMode {
    Unidirectional,
    Bidirectional,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReconnectPolicy {
    BusinessOwned,
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CoalesceKey(pub String);

impl CoalesceKey {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackpressurePolicy {
    Block {
        max_pending: usize,
    },
    FailFast {
        max_pending: usize,
    },
    DropNewest {
        max_pending: usize,
    },
    DropOldest {
        max_pending: usize,
    },
    Coalesce {
        max_pending: usize,
        key: CoalesceKey,
    },
    Disconnect {
        max_pending: usize,
    },
}

impl BackpressurePolicy {
    pub fn max_pending(&self) -> usize {
        match self {
            Self::Block { max_pending }
            | Self::FailFast { max_pending }
            | Self::DropNewest { max_pending }
            | Self::DropOldest { max_pending }
            | Self::Coalesce { max_pending, .. }
            | Self::Disconnect { max_pending } => *max_pending,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectLinkOptions {
    pub mode: DirectLinkMode,
    pub reconnect: ReconnectPolicy,
    pub backpressure: BackpressurePolicy,
    pub heartbeat_interval: Duration,
    pub idle_timeout: Duration,
    pub max_frame_size: usize,
}

impl DirectLinkOptions {
    pub fn unidirectional() -> Self {
        Self::default()
    }

    pub fn bidirectional() -> Self {
        Self {
            mode: DirectLinkMode::Bidirectional,
            ..Self::default()
        }
    }
}

impl Default for DirectLinkOptions {
    fn default() -> Self {
        Self {
            mode: DirectLinkMode::Unidirectional,
            reconnect: ReconnectPolicy::BusinessOwned,
            backpressure: BackpressurePolicy::FailFast { max_pending: 1024 },
            heartbeat_interval: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(30),
            max_frame_size: 256 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LinkCloseReason {
    Done,
    LocalClose,
    RemoteClose,
    HeartbeatTimeout,
    BackpressureExceeded,
    ProtocolError(String),
    Unauthorized,
    TargetPassivated,
    TargetMigrating,
    NodeDraining,
    ConnectionLost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LinkDirection {
    SourceToTarget,
    TargetToSource,
}
