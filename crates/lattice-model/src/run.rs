//! Cluster-run identity and durable lifecycle. These are safety identities, not
//! application or framework release versions.

use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Monotonic cluster-run identity retained across runtime cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunEpoch(NonZeroU64);

impl RunEpoch {
    pub const INITIAL: Self = Self(NonZeroU64::MIN);

    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }

    pub fn next(self) -> Result<Self, LifecycleError> {
        self.get()
            .checked_add(1)
            .and_then(Self::new)
            .ok_or(LifecycleError::EpochExhausted)
    }
}

/// A caller-supplied idempotency key. The same ID must be retained when retrying
/// an operation whose result is unknown. It never confers permission itself.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ControlOperationId(String);

impl ControlOperationId {
    pub fn new(value: impl Into<String>) -> Result<Self, LifecycleError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(LifecycleError::InvalidOperationId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ControlOperationId {
    type Error = LifecycleError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ControlOperationId> for String {
    fn from(value: ControlOperationId) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClosingStage {
    Draining,
    Cleaning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunCompletion {
    Graceful,
    Reset,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunPhase {
    Running,
    Closing {
        operation: ControlOperationId,
        stage: ClosingStage,
    },
    /// A privileged offline reset. Unlike graceful closing, this never records
    /// proof that application stop callbacks completed.
    Resetting {
        operation: ControlOperationId,
    },
    Closed {
        operation: ControlOperationId,
        completion: RunCompletion,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterLifecycle {
    pub epoch: RunEpoch,
    pub phase: RunPhase,
}

impl ClusterLifecycle {
    pub fn initial() -> Self {
        Self {
            epoch: RunEpoch::INITIAL,
            phase: RunPhase::Running,
        }
    }

    /// Produces the canonical close operation. Concurrent requests for the same
    /// run converge on the first committed operation rather than replacing it.
    /// Publication still requires the store's exact-state CAS and authorization.
    pub fn closing(&self, operation: ControlOperationId) -> Result<Self, LifecycleError> {
        match self.phase {
            RunPhase::Running => Ok(Self {
                epoch: self.epoch,
                phase: RunPhase::Closing {
                    operation,
                    stage: ClosingStage::Draining,
                },
            }),
            RunPhase::Closing { .. } => Ok(self.clone()),
            RunPhase::Closed { .. } | RunPhase::Resetting { .. } => Err(LifecycleError::Closed),
        }
    }

    pub fn permits_election(&self) -> bool {
        matches!(self.phase, RunPhase::Running | RunPhase::Closing { .. })
    }
    pub fn is_running(&self) -> bool {
        self.phase == RunPhase::Running
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum LifecycleError {
    #[error("cluster run epoch exhausted")]
    EpochExhausted,
    #[error("operation ID must contain 1..=128 ASCII letters, digits, '.', '-' or '_'")]
    InvalidOperationId,
    #[error("cluster run is already closed")]
    Closed,
}

#[cfg(test)]
mod tests {
    use super::{ClusterLifecycle, ControlOperationId, LifecycleError, RunEpoch, RunPhase};

    #[test]
    fn rejects_zero_epochs_and_invalid_operation_ids_on_decode() {
        assert!(serde_json::from_str::<RunEpoch>("0").is_err());
        assert!(serde_json::from_str::<ControlOperationId>("\"../bad\"").is_err());
        assert!(ControlOperationId::new("x".repeat(129)).is_err());
        assert_eq!(
            RunEpoch::new(u64::MAX).unwrap().next(),
            Err(LifecycleError::EpochExhausted)
        );
    }

    #[test]
    fn closing_is_irreversible_and_retries_return_the_first_operation() {
        let original = ControlOperationId::new("shutdown-1").unwrap();
        let lifecycle = ClusterLifecycle::initial()
            .closing(original.clone())
            .unwrap();
        assert!(!lifecycle.is_running());
        assert!(lifecycle.permits_election());
        let retried = lifecycle
            .closing(ControlOperationId::new("shutdown-2").unwrap())
            .unwrap();
        assert_eq!(retried, lifecycle);
        assert!(
            matches!(retried.phase, RunPhase::Closing { operation, .. } if operation == original)
        );
    }
}
