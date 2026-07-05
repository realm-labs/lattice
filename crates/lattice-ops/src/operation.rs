use std::collections::HashMap;
use std::sync::Arc;

use serde::Serialize;
use tokio::sync::Mutex;

use crate::OpsError;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct OperationId(String);

impl OperationId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum OperationStatus {
    Pending,
    Retrying { attempts: u32 },
    Completed,
    CompensationRequired { reason: String },
    ManualRequired { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PendingOperation {
    pub operation_id: OperationId,
    pub status: OperationStatus,
}

#[derive(Debug, Default, Clone)]
pub struct OperationTracker {
    operations: Arc<Mutex<HashMap<OperationId, PendingOperation>>>,
}

impl OperationTracker {
    pub async fn start(&self, operation_id: OperationId) -> Result<(), OpsError> {
        let mut operations = self.operations.lock().await;
        if operations.contains_key(&operation_id) {
            return Err(OpsError::DuplicateOperation {
                operation_id: operation_id.as_str().to_string(),
            });
        }
        operations.insert(
            operation_id.clone(),
            PendingOperation {
                operation_id,
                status: OperationStatus::Pending,
            },
        );
        Ok(())
    }

    pub async fn mark_retrying(
        &self,
        operation_id: &OperationId,
        attempts: u32,
    ) -> Result<(), OpsError> {
        self.update(operation_id, OperationStatus::Retrying { attempts })
            .await
    }

    pub async fn mark_compensation_required(
        &self,
        operation_id: &OperationId,
        reason: impl Into<String>,
    ) -> Result<(), OpsError> {
        self.update(
            operation_id,
            OperationStatus::CompensationRequired {
                reason: reason.into(),
            },
        )
        .await
    }

    pub async fn mark_manual_required(
        &self,
        operation_id: &OperationId,
        reason: impl Into<String>,
    ) -> Result<(), OpsError> {
        self.update(
            operation_id,
            OperationStatus::ManualRequired {
                reason: reason.into(),
            },
        )
        .await
    }

    pub async fn complete(&self, operation_id: &OperationId) -> Result<(), OpsError> {
        self.update(operation_id, OperationStatus::Completed).await
    }

    pub async fn get(&self, operation_id: &OperationId) -> Option<PendingOperation> {
        self.operations.lock().await.get(operation_id).cloned()
    }

    async fn update(
        &self,
        operation_id: &OperationId,
        status: OperationStatus,
    ) -> Result<(), OpsError> {
        let mut operations = self.operations.lock().await;
        let operation =
            operations
                .get_mut(operation_id)
                .ok_or_else(|| OpsError::UnknownOperation {
                    operation_id: operation_id.as_str().to_string(),
                })?;
        operation.status = status;
        Ok(())
    }
}
