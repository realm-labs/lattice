//! Direct and prepared MongoDB write execution.

use std::collections::BTreeMap;

use mongodb::bson::{Bson, Document, doc};

use crate::document::{MongoDocument, decode_flat_document, encode_flat_document};
use crate::error::MongoStoreError;
use crate::persistence::direct::{
    DeleteOutcome, DirectDocumentStore, InsertOutcome, ReplaceOutcome,
};
use crate::persistence::request::{
    CreateMode, DocumentOperation, DocumentWriteOutcome, FlushOutcome, PreparedDocumentWrite,
    PreparedWriteStore,
};

use super::{MongoStore, mongo_timeout, store_error};

impl MongoStore {
    pub(super) async fn flush_prepared_writes(
        &self,
        writes: Vec<PreparedDocumentWrite>,
    ) -> Result<FlushOutcome, MongoStoreError> {
        let mut documents = BTreeMap::new();
        for write in writes {
            let token = write.token;
            let outcome = match self.apply_prepared_write(write).await {
                Ok(outcome) => outcome,
                Err(error) => DocumentWriteOutcome::Failed { error },
            };
            documents.insert(token, outcome);
        }
        Ok(FlushOutcome { documents })
    }

    async fn apply_prepared_write(
        &self,
        write: PreparedDocumentWrite,
    ) -> Result<DocumentWriteOutcome, MongoStoreError> {
        let collection = self.database.collection::<Document>(write.key.collection);
        let document_id = write.document_id.clone();
        let expected_version = write.expected_version;
        let new_version = expected_version
            .checked_add(1)
            .ok_or_else(|| MongoStoreError::new("prepared document version overflow"))?;
        match write.operation {
            DocumentOperation::Update { sets, unsets } => {
                let updated_at_ms = unix_time_ms()?;
                let mut set = Document::new();
                for (path, value) in sets {
                    set.insert(path.0, value);
                }
                set.insert("version", new_version);
                set.insert("updated_at_ms", updated_at_ms);
                let mut update = doc! { "$set": set };
                if !unsets.is_empty() {
                    let unset = unsets
                        .into_iter()
                        .map(|path| (path.0, Bson::String(String::new())))
                        .collect::<Document>();
                    update.insert("$unset", unset);
                }
                let result = mongo_timeout(
                    self.operation_timeout,
                    "update prepared document",
                    collection.update_one(
                        doc! { "_id": document_id.clone(), "version": expected_version },
                        update,
                    ),
                )
                .await?;
                if result.matched_count == 1 {
                    Ok(DocumentWriteOutcome::Applied {
                        previous_version: expected_version,
                        new_version,
                        updated_at_ms,
                    })
                } else if direct_document_exists(self, write.key.collection, document_id).await? {
                    Ok(DocumentWriteOutcome::VersionConflict { expected_version })
                } else {
                    Ok(unmatched_prepared_outcome(expected_version, false))
                }
            }
            DocumentOperation::Create { mut document, mode } => {
                let updated_at_ms = unix_time_ms()?;
                document.insert("_id", document_id.clone());
                document.insert("version", new_version);
                document.insert("updated_at_ms", updated_at_ms);
                match mode {
                    CreateMode::InsertOnly => match mongo_timeout(
                        self.operation_timeout,
                        "insert prepared document",
                        collection.insert_one(document),
                    )
                    .await
                    {
                        Ok(_) => Ok(DocumentWriteOutcome::Applied {
                            previous_version: expected_version,
                            new_version,
                            updated_at_ms,
                        }),
                        Err(error) if is_duplicate_key_message(error.message()) => {
                            Ok(DocumentWriteOutcome::VersionConflict { expected_version })
                        }
                        Err(error) => Err(error),
                    },
                    CreateMode::UpsertAllowed => {
                        let result = mongo_timeout(
                            self.operation_timeout,
                            "upsert prepared document",
                            collection
                                .replace_one(doc! { "_id": document_id }, document)
                                .upsert(true),
                        )
                        .await?;
                        if result.matched_count == 1 || result.upserted_id.is_some() {
                            Ok(DocumentWriteOutcome::Applied {
                                previous_version: expected_version,
                                new_version,
                                updated_at_ms,
                            })
                        } else {
                            Ok(DocumentWriteOutcome::VersionConflict { expected_version })
                        }
                    }
                }
            }
            DocumentOperation::Delete => {
                let updated_at_ms = unix_time_ms()?;
                let result = mongo_timeout(
                    self.operation_timeout,
                    "delete prepared document",
                    collection.delete_one(
                        doc! { "_id": document_id.clone(), "version": expected_version },
                    ),
                )
                .await?;
                if result.deleted_count == 1 {
                    Ok(DocumentWriteOutcome::Applied {
                        previous_version: expected_version,
                        new_version,
                        updated_at_ms,
                    })
                } else if direct_document_exists(self, write.key.collection, document_id).await? {
                    Ok(DocumentWriteOutcome::VersionConflict { expected_version })
                } else {
                    Ok(unmatched_prepared_outcome(expected_version, false))
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl PreparedWriteStore for MongoStore {
    async fn flush(
        &self,
        writes: Vec<PreparedDocumentWrite>,
    ) -> Result<FlushOutcome, MongoStoreError> {
        self.flush_prepared_writes(writes).await
    }
}

#[async_trait::async_trait]
impl<D> DirectDocumentStore<D> for MongoStore
where
    D: MongoDocument + Sync,
    D::Id: Sync,
{
    async fn load(
        &self,
        id: &D::Id,
    ) -> Result<Option<crate::document::LoadedDocument<D>>, MongoStoreError> {
        let collection = self.database.collection::<Document>(D::COLLECTION);
        let id = mongodb::bson::to_bson(id).map_err(store_error("encode direct document id"))?;
        let document = mongo_timeout(
            self.operation_timeout,
            "load direct document",
            collection.find_one(doc! { "_id": id }),
        )
        .await?;
        document.map(decode_flat_document::<D>).transpose()
    }

    async fn insert(&self, value: &D) -> Result<InsertOutcome, MongoStoreError> {
        const INITIAL_VERSION: i64 = 1;
        let collection = self.database.collection::<Document>(D::COLLECTION);
        let document = encode_flat_document(value, INITIAL_VERSION, unix_time_ms()?)?;
        match mongo_timeout(
            self.operation_timeout,
            "insert direct document",
            collection.insert_one(document),
        )
        .await
        {
            Ok(_) => Ok(InsertOutcome::Inserted {
                version: INITIAL_VERSION,
            }),
            Err(error) if is_duplicate_key_message(error.message()) => {
                Ok(InsertOutcome::AlreadyExists)
            }
            Err(error) => Err(error),
        }
    }

    async fn replace(
        &self,
        expected_version: i64,
        value: &D,
    ) -> Result<ReplaceOutcome, MongoStoreError> {
        let new_version = expected_version
            .checked_add(1)
            .ok_or_else(|| MongoStoreError::new("direct document version overflow"))?;
        let collection = self.database.collection::<Document>(D::COLLECTION);
        let id_bson =
            mongodb::bson::to_bson(value.id()).map_err(store_error("encode direct document id"))?;
        let replacement = encode_flat_document(value, new_version, unix_time_ms()?)?;
        let result = mongo_timeout(
            self.operation_timeout,
            "replace direct document",
            collection.replace_one(
                doc! { "_id": id_bson.clone(), "version": expected_version },
                replacement,
            ),
        )
        .await?;
        if result.matched_count == 1 {
            return Ok(ReplaceOutcome::Replaced { new_version });
        }
        if direct_document_exists(self, D::COLLECTION, id_bson).await? {
            Ok(ReplaceOutcome::VersionConflict)
        } else {
            Ok(ReplaceOutcome::NotFound)
        }
    }

    async fn delete(
        &self,
        id: &D::Id,
        expected_version: i64,
    ) -> Result<DeleteOutcome, MongoStoreError> {
        let collection = self.database.collection::<Document>(D::COLLECTION);
        let id = mongodb::bson::to_bson(id).map_err(store_error("encode direct document id"))?;
        let result = mongo_timeout(
            self.operation_timeout,
            "delete direct document",
            collection.delete_one(doc! { "_id": id.clone(), "version": expected_version }),
        )
        .await?;
        if result.deleted_count == 1 {
            return Ok(DeleteOutcome::Deleted);
        }
        if direct_document_exists(self, D::COLLECTION, id).await? {
            Ok(DeleteOutcome::VersionConflict)
        } else {
            Ok(DeleteOutcome::NotFound)
        }
    }
}

async fn direct_document_exists(
    store: &MongoStore,
    collection: &'static str,
    id: Bson,
) -> Result<bool, MongoStoreError> {
    let collection = store.database.collection::<Document>(collection);
    Ok(mongo_timeout(
        store.operation_timeout,
        "check direct document existence",
        collection
            .find_one(doc! { "_id": id })
            .projection(doc! { "_id": 1 }),
    )
    .await?
    .is_some())
}

fn unix_time_ms() -> Result<i64, MongoStoreError> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| {
            MongoStoreError::clock(format!("system clock before Unix epoch: {error}"))
        })?;
    i64::try_from(duration.as_millis())
        .map_err(|_| MongoStoreError::clock("system time exceeds persisted i64 milliseconds"))
}

fn is_duplicate_key_message(message: &str) -> bool {
    message.contains("E11000") || message.contains("duplicate key")
}

pub(super) fn unmatched_prepared_outcome(
    expected_version: i64,
    exists: bool,
) -> DocumentWriteOutcome {
    if exists {
        DocumentWriteOutcome::VersionConflict { expected_version }
    } else {
        DocumentWriteOutcome::NotFound { expected_version }
    }
}
