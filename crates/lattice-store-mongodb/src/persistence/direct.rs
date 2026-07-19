//! Explicit whole-document persistence for non-scanned data.

use crate::error::MongoStoreError;
use async_trait::async_trait;

use crate::document::{LoadedDocument, MongoDocument};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DirectWriteToken(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirectWriteOperation<D>
where
    D: MongoDocument,
{
    Insert { value: D },
    Replace { expected_version: i64, value: D },
    Delete { id: D::Id, expected_version: i64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectWriteRequest<D>
where
    D: MongoDocument,
{
    pub token: DirectWriteToken,
    pub operation: DirectWriteOperation<D>,
}

#[derive(Debug)]
pub enum DirectWriteOutcome {
    Insert(InsertOutcome),
    Replace(ReplaceOutcome),
    Delete(DeleteOutcome),
    Failed(MongoStoreError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    Inserted { version: i64 },
    AlreadyExists,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplaceOutcome {
    Replaced { new_version: i64 },
    VersionConflict,
    NotFound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOutcome {
    Deleted,
    VersionConflict,
    NotFound,
}

#[async_trait]
pub trait DirectDocumentStore<D>: Send + Sync + 'static
where
    D: MongoDocument + Sync,
    D::Id: Sync,
{
    async fn load(&self, id: &D::Id) -> Result<Option<LoadedDocument<D>>, MongoStoreError>;

    async fn insert(&self, value: &D) -> Result<InsertOutcome, MongoStoreError>;

    async fn replace(
        &self,
        expected_version: i64,
        value: &D,
    ) -> Result<ReplaceOutcome, MongoStoreError>;

    async fn delete(
        &self,
        id: &D::Id,
        expected_version: i64,
    ) -> Result<DeleteOutcome, MongoStoreError>;

    async fn delete_document(
        &self,
        value: &D,
        expected_version: i64,
    ) -> Result<DeleteOutcome, MongoStoreError> {
        self.delete(value.id(), expected_version).await
    }
}
