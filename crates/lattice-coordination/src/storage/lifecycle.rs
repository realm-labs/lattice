use async_trait::async_trait;
use lattice_model::run::{ClusterLifecycle, ControlOperationId};

use super::{ClusterLifecycleStore, InMemoryCoordinationStore, StorageError, validate_leader};
use crate::coordinator::ClusterLeaderGuard;

#[async_trait]
impl ClusterLifecycleStore for InMemoryCoordinationStore {
    async fn lifecycle(&self) -> Result<ClusterLifecycle, StorageError> {
        self.inner
            .lock()
            .expect("coordination memory store poisoned")
            .lifecycle
            .clone()
            .ok_or(StorageError::StorageMetadataMismatch)
    }

    async fn begin_shutdown(
        &self,
        guard: &ClusterLeaderGuard,
        operation: ControlOperationId,
    ) -> Result<ClusterLifecycle, StorageError> {
        let mut state = self
            .inner
            .lock()
            .expect("coordination memory store poisoned");
        validate_leader(&state, guard)?;
        let next = state
            .lifecycle
            .as_ref()
            .ok_or(StorageError::StorageMetadataMismatch)?
            .closing(operation)
            .map_err(|_| StorageError::RunNotRunning)?;
        state.lifecycle = Some(next.clone());
        Ok(next)
    }
}
