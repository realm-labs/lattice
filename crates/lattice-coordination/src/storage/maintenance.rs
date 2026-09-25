//! Privileged offline maintenance contracts. These APIs do not belong to the
//! ordinary node credential boundary and are not graceful-shutdown shortcuts.

use async_trait::async_trait;
use lattice_model::run::{ClusterLifecycle, ControlOperationId, RunEpoch};

use super::{ClusterLifecycleStore, StorageError};

/// Explicit operator acknowledgement, bound to an exact deployment and run.
/// This records intent, not remotely verifiable proof of process termination.
/// A management tool must show the selected namespace and require confirmation
/// before constructing this value; automatic recovery must never construct it.
#[derive(Debug, Clone)]
pub struct StoppedDeployment {
    pub namespace: String,
    pub epoch: RunEpoch,
}

/// A bounded cleanup result. Repeating a batch is safe after an ambiguous result:
/// the durable maintenance phase remains fenced, and scanning resumes at the
/// first remaining key. No process-local cursor is required for recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResetProgress {
    pub operation: ControlOperationId,
    pub epoch: RunEpoch,
    pub deleted_keys: usize,
    pub complete: bool,
}

#[async_trait]
pub trait ClusterResetStore: ClusterLifecycleStore {
    /// Freezes all ordinary writers and elections before any deletion. Conflicting
    /// reset IDs are rejected. The caller must use separate administrative credentials.
    async fn begin_reset(
        &self,
        expected: &ClusterLifecycle,
        operation: ControlOperationId,
        confirmation: &StoppedDeployment,
    ) -> Result<ClusterLifecycle, StorageError>;

    /// Deletes only the exact run subtree, in bounded transactions. Live leased
    /// records block progress even after operator confirmation. Never deletes
    /// framework metadata, terms, definitions, or unrelated application data.
    async fn reset_batch(
        &self,
        epoch: RunEpoch,
        operation: &ControlOperationId,
        maximum_keys: usize,
    ) -> Result<ResetProgress, StorageError>;

    /// Starts a fresh run only after a confirmed, completed cleanup. Existing
    /// store handles remain bound to the old epoch; open a new handle afterward.
    async fn start_new_run(&self, expected: &ClusterLifecycle) -> Result<RunEpoch, StorageError>;
}
