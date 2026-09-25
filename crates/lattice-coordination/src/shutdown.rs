//! Durable, resumable cluster-stop operations. Lease disappearance is never stop evidence.

use async_trait::async_trait;
use lattice_model::run::{ClosingStage, ClusterLifecycle, ControlOperationId, RunEpoch, RunPhase};
use serde::{Deserialize, Serialize};
use std::{fmt::Write, sync::Arc, time::Duration};

use crate::{
    coordinator::ClusterLeaderGuard,
    storage::{ClusterLifecycleStore, StorageError},
    types::NodeKey,
};

/// A report covers managed Actors, children/background work, and the application's stop hook.
/// `Stopped` must only be emitted after those operations have actually completed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStopOutcome {
    Stopped,
    Blocked { reason: String },
}

/// Administrative rejection is distinct from an uncertain request timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum ShutdownRequestRejection {
    #[error("the authenticated peer is not authorized to shut down this cluster")]
    Unauthorized,
    #[error("the request refers to a different cluster run")]
    StaleRun,
    #[error("the coordinator cannot accept shutdown; rediscover and retry the same operation")]
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShutdownManifest {
    pub epoch: RunEpoch,
    pub operation: ControlOperationId,
    pub source_revision: i64,
    pub cursor: Option<String>,
    pub participant_count: u64,
    pub digest: [u8; 32],
    pub sealed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShutdownReport {
    pub epoch: RunEpoch,
    pub operation: ControlOperationId,
    pub node: NodeKey,
    pub outcome: NodeStopOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShutdownProgress {
    pub lifecycle: ClusterLifecycle,
    pub manifest: Option<ShutdownManifest>,
    pub complete: bool,
}

/// Store-backed administrative operations. Transport adapters must authenticate
/// requests and stop reports before entering this privileged interface.
#[async_trait]
pub trait ClusterShutdownStore: ClusterLifecycleStore {
    /// Positive local-stop proof after ordinary node leave. Running removes the
    /// retired incarnation's obligation; Closing retains proof for its manifest.
    async fn record_node_stopped(
        &self,
        guard: &ClusterLeaderGuard,
        node: &NodeKey,
    ) -> Result<(), StorageError>;
    /// Advances a fixed-revision scan by at most 32 identities. No stop command
    /// may be emitted until the returned manifest is sealed.
    async fn build_shutdown_manifest(
        &self,
        guard: &ClusterLeaderGuard,
        operation: &ControlOperationId,
    ) -> Result<ShutdownManifest, StorageError>;
    async fn shutdown_participants(
        &self,
        operation: &ControlOperationId,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, NodeKey)>, StorageError>;
    async fn record_shutdown_report(
        &self,
        guard: &ClusterLeaderGuard,
        report: ShutdownReport,
    ) -> Result<(), StorageError>;
    /// Verifies every sealed identity and its exact positive report before
    /// freezing progress writers. Missing, blocked or stale reports fail closed.
    async fn begin_shutdown_cleanup(
        &self,
        guard: &ClusterLeaderGuard,
        operation: &ControlOperationId,
    ) -> Result<(), StorageError>;
    async fn shutdown_cleanup_batch(
        &self,
        guard: &ClusterLeaderGuard,
        operation: &ControlOperationId,
        maximum_keys: usize,
    ) -> Result<ShutdownProgress, StorageError>;
}

pub(crate) fn participant_key(node: &NodeKey) -> String {
    // Stable identity encoding, independent of JSON field order and endpoint changes.
    let mut key = String::with_capacity(node.node_id.len() * 2 + 34);
    for byte in node.node_id.as_bytes() {
        write!(&mut key, "{byte:02x}").expect("string write");
    }
    key.push('-');
    key.push_str(&node.incarnation.get().to_string());
    key
}

/// Resumable privileged coordinator driver. Dropping a waiter never cancels the
/// durable operation; the next authorized leader can construct another driver.
pub struct ClusterShutdown<S> {
    store: Arc<S>,
    guard: ClusterLeaderGuard,
}

impl<S: ClusterShutdownStore> ClusterShutdown<S> {
    pub fn new(store: Arc<S>, guard: ClusterLeaderGuard) -> Self {
        Self { store, guard }
    }

    pub async fn request_shutdown(
        &self,
        operation: ControlOperationId,
    ) -> Result<ClusterLifecycle, StorageError> {
        self.store.begin_shutdown(&self.guard, operation).await
    }

    pub async fn shutdown_status(&self) -> Result<ClusterLifecycle, StorageError> {
        self.store.lifecycle().await
    }

    pub async fn participants(
        &self,
        operation: &ControlOperationId,
        after: Option<&str>,
    ) -> Result<Vec<(String, NodeKey)>, StorageError> {
        self.store.shutdown_participants(operation, after, 32).await
    }

    pub async fn report(&self, report: ShutdownReport) -> Result<(), StorageError> {
        self.store.record_shutdown_report(&self.guard, report).await
    }

    /// Runs one bounded manifest/cleanup step. A missing report returns
    /// `ShutdownBlocked`, not success, so a driver can notify participants and retry.
    pub async fn advance(
        &self,
        operation: &ControlOperationId,
    ) -> Result<ShutdownProgress, StorageError> {
        let lifecycle = self.store.lifecycle().await?;
        if lifecycle.epoch != self.guard.record().epoch {
            return Err(StorageError::RunMismatch);
        }
        match &lifecycle.phase {
            RunPhase::Closing {
                operation: active,
                stage: ClosingStage::Draining,
            } if active == operation => {
                let manifest = self
                    .store
                    .build_shutdown_manifest(&self.guard, operation)
                    .await?;
                if manifest.sealed {
                    self.store
                        .begin_shutdown_cleanup(&self.guard, operation)
                        .await?;
                }
                Ok(ShutdownProgress {
                    lifecycle: self.store.lifecycle().await?,
                    manifest: Some(manifest),
                    complete: false,
                })
            }
            RunPhase::Closing {
                operation: active,
                stage: ClosingStage::Cleaning,
            } if active == operation => {
                self.store
                    .shutdown_cleanup_batch(&self.guard, operation, 32)
                    .await
            }
            RunPhase::Closed {
                operation: active, ..
            } if active == operation => Ok(ShutdownProgress {
                lifecycle,
                manifest: None,
                complete: true,
            }),
            _ => Err(StorageError::RunNotRunning),
        }
    }

    pub async fn wait_shutdown(
        &self,
        operation: &ControlOperationId,
        timeout: Duration,
    ) -> Result<ClusterLifecycle, ShutdownWaitError> {
        tokio::time::timeout(timeout, async {
            loop {
                let state = self
                    .shutdown_status()
                    .await
                    .map_err(ShutdownWaitError::Storage)?;
                if state.epoch != self.guard.record().epoch {
                    return Err(ShutdownWaitError::Storage(StorageError::RunMismatch));
                }
                if matches!(&state.phase,RunPhase::Closed{operation:active,..}if active==operation)
                {
                    return Ok(state);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .map_err(|_| ShutdownWaitError::Timeout)?
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ShutdownWaitError {
    #[error("shutdown wait timed out; the durable operation continues")]
    Timeout,
    #[error(transparent)]
    Storage(StorageError),
}

pub(crate) fn extend_digest(
    previous: [u8; 32],
    key: &str,
    node: &NodeKey,
) -> Result<[u8; 32], StorageError> {
    let mut hash = blake3::Hasher::new();
    hash.update(&previous);
    hash.update(&(key.len() as u64).to_be_bytes());
    hash.update(key.as_bytes());
    hash.update(&serde_json::to_vec(node).map_err(|_| StorageError::InvalidRecord)?);
    Ok(*hash.finalize().as_bytes())
}
