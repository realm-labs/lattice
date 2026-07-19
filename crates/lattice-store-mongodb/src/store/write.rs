//! Direct and prepared MongoDB write execution.

use std::collections::BTreeMap;

use mongodb::bson::{Bson, Document, doc};

use crate::document::{MongoDocument, WRITE_ID_FIELD, decode_flat_document, encode_flat_document};
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
        let operation_id = write.operation_id.clone();
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
                set.insert(WRITE_ID_FIELD, operation_id.clone());
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
                .await;
                match result {
                    Ok(result) if result.matched_count == 1 => Ok(DocumentWriteOutcome::Applied {
                        previous_version: expected_version,
                        new_version,
                        updated_at_ms,
                    }),
                    Ok(_) => {
                        resolve_prepared_outcome(
                            self,
                            write.key.collection,
                            document_id,
                            expected_version,
                            new_version,
                            &operation_id,
                        )
                        .await
                    }
                    Err(error) => {
                        if let Ok(Some(applied)) = reconcile_prepared_write(
                            self,
                            write.key.collection,
                            document_id,
                            expected_version,
                            new_version,
                            &operation_id,
                        )
                        .await
                        {
                            Ok(applied)
                        } else {
                            Err(error)
                        }
                    }
                }
            }
            DocumentOperation::Create { mut document, mode } => {
                let updated_at_ms = unix_time_ms()?;
                document.insert("_id", document_id.clone());
                document.insert("version", new_version);
                document.insert("updated_at_ms", updated_at_ms);
                document.insert(WRITE_ID_FIELD, operation_id.clone());
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
                        Err(error) if error.is_write_rejection() => {
                            match reconcile_insert_rejection(
                                self,
                                write.key.collection,
                                document_id,
                                expected_version,
                                new_version,
                                &operation_id,
                            )
                            .await?
                            {
                                Some(outcome) => Ok(outcome),
                                None => Err(error),
                            }
                        }
                        Err(error) => {
                            if let Ok(Some(applied)) = reconcile_prepared_write(
                                self,
                                write.key.collection,
                                document_id,
                                expected_version,
                                new_version,
                                &operation_id,
                            )
                            .await
                            {
                                Ok(applied)
                            } else {
                                Err(error)
                            }
                        }
                    },
                    CreateMode::UpsertAllowed => {
                        let result = mongo_timeout(
                            self.operation_timeout,
                            "upsert prepared document",
                            collection
                                .replace_one(
                                    doc! {
                                        "_id": document_id.clone(),
                                        "version": expected_version,
                                    },
                                    document,
                                )
                                .upsert(true),
                        )
                        .await;
                        match result {
                            Ok(result)
                                if result.matched_count == 1 || result.upserted_id.is_some() =>
                            {
                                Ok(DocumentWriteOutcome::Applied {
                                    previous_version: expected_version,
                                    new_version,
                                    updated_at_ms,
                                })
                            }
                            Ok(_) => {
                                resolve_prepared_outcome(
                                    self,
                                    write.key.collection,
                                    document_id,
                                    expected_version,
                                    new_version,
                                    &operation_id,
                                )
                                .await
                            }
                            Err(error) if error.is_write_rejection() => {
                                match reconcile_upsert_rejection(
                                    self,
                                    write.key.collection,
                                    document_id,
                                    expected_version,
                                    new_version,
                                    &operation_id,
                                )
                                .await?
                                {
                                    Some(outcome) => Ok(outcome),
                                    None => Err(error),
                                }
                            }
                            Err(error) => {
                                if let Ok(Some(applied)) = reconcile_prepared_write(
                                    self,
                                    write.key.collection,
                                    document_id,
                                    expected_version,
                                    new_version,
                                    &operation_id,
                                )
                                .await
                                {
                                    Ok(applied)
                                } else {
                                    Err(error)
                                }
                            }
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
    D: MongoDocument,
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
        let id =
            mongodb::bson::to_bson(value.id()).map_err(store_error("encode direct document id"))?;
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
            Err(error) if error.is_write_rejection() => {
                if direct_document_exists(self, D::COLLECTION, id).await? {
                    Ok(InsertOutcome::AlreadyExists)
                } else {
                    Err(error)
                }
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

#[derive(Debug)]
struct PreparedWriteState {
    version: i64,
    updated_at_ms: i64,
    operation_id: Option<String>,
}

async fn resolve_prepared_outcome(
    store: &MongoStore,
    collection: &'static str,
    id: Bson,
    expected_version: i64,
    new_version: i64,
    operation_id: &str,
) -> Result<DocumentWriteOutcome, MongoStoreError> {
    let Some(state) = prepared_write_state(store, collection, id).await? else {
        return Ok(DocumentWriteOutcome::NotFound { expected_version });
    };
    if state.version == new_version && state.operation_id.as_deref() == Some(operation_id) {
        Ok(DocumentWriteOutcome::Applied {
            previous_version: expected_version,
            new_version,
            updated_at_ms: state.updated_at_ms,
        })
    } else {
        Ok(DocumentWriteOutcome::VersionConflict { expected_version })
    }
}

async fn reconcile_prepared_write(
    store: &MongoStore,
    collection: &'static str,
    id: Bson,
    expected_version: i64,
    new_version: i64,
    operation_id: &str,
) -> Result<Option<DocumentWriteOutcome>, MongoStoreError> {
    let Some(state) = prepared_write_state(store, collection, id).await? else {
        return Ok(None);
    };
    if state.version != new_version || state.operation_id.as_deref() != Some(operation_id) {
        return Ok(None);
    }
    Ok(Some(DocumentWriteOutcome::Applied {
        previous_version: expected_version,
        new_version,
        updated_at_ms: state.updated_at_ms,
    }))
}

async fn reconcile_insert_rejection(
    store: &MongoStore,
    collection: &'static str,
    id: Bson,
    expected_version: i64,
    new_version: i64,
    operation_id: &str,
) -> Result<Option<DocumentWriteOutcome>, MongoStoreError> {
    let Some(state) = prepared_write_state(store, collection, id).await? else {
        return Ok(None);
    };
    if state.version == new_version && state.operation_id.as_deref() == Some(operation_id) {
        return Ok(Some(DocumentWriteOutcome::Applied {
            previous_version: expected_version,
            new_version,
            updated_at_ms: state.updated_at_ms,
        }));
    }
    Ok(Some(DocumentWriteOutcome::VersionConflict {
        expected_version,
    }))
}

async fn reconcile_upsert_rejection(
    store: &MongoStore,
    collection: &'static str,
    id: Bson,
    expected_version: i64,
    new_version: i64,
    operation_id: &str,
) -> Result<Option<DocumentWriteOutcome>, MongoStoreError> {
    let Some(state) = prepared_write_state(store, collection, id).await? else {
        return Ok(None);
    };
    if state.version == new_version && state.operation_id.as_deref() == Some(operation_id) {
        return Ok(Some(DocumentWriteOutcome::Applied {
            previous_version: expected_version,
            new_version,
            updated_at_ms: state.updated_at_ms,
        }));
    }
    if state.version != expected_version {
        return Ok(Some(DocumentWriteOutcome::VersionConflict {
            expected_version,
        }));
    }
    Ok(None)
}

async fn prepared_write_state(
    store: &MongoStore,
    collection: &'static str,
    id: Bson,
) -> Result<Option<PreparedWriteState>, MongoStoreError> {
    let collection = store.database.collection::<Document>(collection);
    let mut projection = doc! {
        "version": 1,
        "updated_at_ms": 1,
    };
    projection.insert(WRITE_ID_FIELD, 1);
    let document = mongo_timeout(
        store.operation_timeout,
        "reconcile prepared document write",
        collection
            .find_one(doc! { "_id": id })
            .projection(projection),
    )
    .await?;
    document
        .map(|document| {
            Ok(PreparedWriteState {
                version: document_i64(&document, "version")?,
                updated_at_ms: document_i64(&document, "updated_at_ms")?,
                operation_id: document.get_str(WRITE_ID_FIELD).ok().map(str::to_owned),
            })
        })
        .transpose()
}

fn document_i64(document: &Document, field: &'static str) -> Result<i64, MongoStoreError> {
    match document.get(field) {
        Some(Bson::Int64(value)) => Ok(*value),
        Some(Bson::Int32(value)) => Ok(i64::from(*value)),
        Some(value) => Err(MongoStoreError::new(format!(
            "Mongo `{field}` must be an integer, got {value:?}"
        ))),
        None => Err(MongoStoreError::new(format!(
            "Mongo document missing `{field}`"
        ))),
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
