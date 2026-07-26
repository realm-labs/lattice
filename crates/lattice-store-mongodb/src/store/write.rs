//! Direct and prepared MongoDB write execution.

use std::collections::BTreeMap;

use mongodb::bson::{Bson, Document, doc};

use crate::document::{
    ACTIVATION_EPOCH_FIELD, MongoDocument, WRITE_ID_FIELD, decode_flat_document,
    encode_flat_document,
};
use crate::error::MongoStoreError;
use crate::persistence::direct::{
    DeleteOutcome, DirectDocumentStore, InsertOutcome, ReplaceOutcome,
};
use crate::persistence::request::{
    CreateMode, DocumentOperation, DocumentWriteOutcome, FlushOutcome, PreparedDocumentWrite,
    PreparedWriteStore,
};

use super::{MongoStore, mongo_timeout, store_error};

/// Result of claiming storage-level ownership of one document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivationFence {
    /// The document now records the claiming activation epoch. Writes from
    /// every older activation are refused from this point on.
    Claimed,
    /// No document exists yet. Nothing was written, and the first prepared
    /// write of this activation establishes the fence.
    Absent,
    /// A strictly newer activation already owns the document. This activation
    /// must not serve the entity.
    Superseded { observed_epoch: i64 },
}

/// Result of claiming storage-level ownership of an owner collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActivationFenceSummary {
    pub claimed: u64,
    /// Documents left behind because a strictly newer activation owns them.
    pub superseded: u64,
}

impl MongoStore {
    /// Claims storage-level ownership of one document for `activation_epoch`
    /// without touching its version, business fields, or write identity.
    ///
    /// A new activation calls this after loading and before serving writes so
    /// that a partitioned older activation is refused by storage even while it
    /// still holds a matching `expected_version`. Ordinary prepared writes also
    /// raise the fence, so this is only needed to take ownership eagerly.
    pub async fn fence_document<D>(
        &self,
        id: &D::Id,
        activation_epoch: u64,
    ) -> Result<ActivationFence, MongoStoreError>
    where
        D: MongoDocument,
    {
        let epoch = stored_activation_epoch(activation_epoch)?;
        let id = mongodb::bson::to_bson(id).map_err(store_error("encode fenced document id"))?;
        let collection = self.database.collection::<Document>(D::COLLECTION);
        let mut filter = doc! { "_id": id.clone() };
        filter.insert(ACTIVATION_EPOCH_FIELD, doc! { "$not": { "$gt": epoch } });
        let result = mongo_timeout(
            self.operation_timeout,
            "claim document activation fence",
            collection.update_one(filter, doc! { "$set": { ACTIVATION_EPOCH_FIELD: epoch } }),
        )
        .await?;
        if result.matched_count == 1 {
            return Ok(ActivationFence::Claimed);
        }
        match prepared_write_state(self, D::COLLECTION, id).await? {
            None => Ok(ActivationFence::Absent),
            Some(state) => Ok(ActivationFence::Superseded {
                observed_epoch: state.activation_epoch.unwrap_or(epoch),
            }),
        }
    }

    /// Claims storage-level ownership of every document matching `filter`.
    ///
    /// Owner collections are claimed as one unit; a non-zero `superseded` count
    /// means a newer activation already owns part of the collection and this
    /// activation must not serve it.
    pub async fn fence_documents<D>(
        &self,
        filter: Document,
        activation_epoch: u64,
    ) -> Result<ActivationFenceSummary, MongoStoreError>
    where
        D: MongoDocument,
    {
        let epoch = stored_activation_epoch(activation_epoch)?;
        let collection = self.database.collection::<Document>(D::COLLECTION);
        let claimable = doc! {
            "$and": [
                filter.clone(),
                { ACTIVATION_EPOCH_FIELD: { "$not": { "$gt": epoch } } },
            ]
        };
        let claimed = mongo_timeout(
            self.operation_timeout,
            "claim collection activation fence",
            collection.update_many(
                claimable,
                doc! { "$set": { ACTIVATION_EPOCH_FIELD: epoch } },
            ),
        )
        .await?
        .matched_count;
        let superseded = mongo_timeout(
            self.operation_timeout,
            "count superseded collection documents",
            collection.count_documents(doc! {
                "$and": [filter, { ACTIVATION_EPOCH_FIELD: { "$gt": epoch } }]
            }),
        )
        .await?;
        Ok(ActivationFenceSummary {
            claimed,
            superseded,
        })
    }

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
        let activation_epoch = stored_activation_epoch(write.activation_epoch)?;
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
                set.insert(ACTIVATION_EPOCH_FIELD, activation_epoch);
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
                        fenced_filter(document_id.clone(), expected_version, activation_epoch),
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
                            activation_epoch,
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
                document.insert(ACTIVATION_EPOCH_FIELD, activation_epoch);
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
                                activation_epoch,
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
                                    fenced_filter(
                                        document_id.clone(),
                                        expected_version,
                                        activation_epoch,
                                    ),
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
                                    activation_epoch,
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
                                    activation_epoch,
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
                    collection.delete_one(fenced_filter(
                        document_id.clone(),
                        expected_version,
                        activation_epoch,
                    )),
                )
                .await?;
                if result.deleted_count == 1 {
                    return Ok(DocumentWriteOutcome::Applied {
                        previous_version: expected_version,
                        new_version,
                        updated_at_ms,
                    });
                }
                let Some(state) =
                    prepared_write_state(self, write.key.collection, document_id).await?
                else {
                    return Ok(unmatched_prepared_outcome(expected_version, false));
                };
                Ok(fenced_outcome(&state, expected_version, activation_epoch)
                    .unwrap_or(DocumentWriteOutcome::VersionConflict { expected_version }))
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
    activation_epoch: Option<i64>,
}

impl PreparedWriteState {
    fn replays(&self, new_version: i64, operation_id: &str) -> bool {
        self.version == new_version && self.operation_id.as_deref() == Some(operation_id)
    }
}

async fn resolve_prepared_outcome(
    store: &MongoStore,
    collection: &'static str,
    id: Bson,
    expected_version: i64,
    new_version: i64,
    operation_id: &str,
    activation_epoch: i64,
) -> Result<DocumentWriteOutcome, MongoStoreError> {
    let Some(state) = prepared_write_state(store, collection, id).await? else {
        return Ok(DocumentWriteOutcome::NotFound { expected_version });
    };
    if state.replays(new_version, operation_id) {
        return Ok(DocumentWriteOutcome::Applied {
            previous_version: expected_version,
            new_version,
            updated_at_ms: state.updated_at_ms,
        });
    }
    Ok(fenced_outcome(&state, expected_version, activation_epoch)
        .unwrap_or(DocumentWriteOutcome::VersionConflict { expected_version }))
}

/// Distinguishes a superseded activation from an ordinary optimistic-lock
/// conflict. It is only consulted after an exact replay has been excluded, so
/// an acknowledged ambiguous write is still reported as applied even when a
/// newer activation has claimed the document since.
fn fenced_outcome(
    state: &PreparedWriteState,
    expected_version: i64,
    activation_epoch: i64,
) -> Option<DocumentWriteOutcome> {
    state
        .activation_epoch
        .filter(|observed| *observed > activation_epoch)
        .map(|observed_epoch| DocumentWriteOutcome::Fenced {
            expected_version,
            observed_epoch,
        })
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
    if !state.replays(new_version, operation_id) {
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
    activation_epoch: i64,
) -> Result<Option<DocumentWriteOutcome>, MongoStoreError> {
    let Some(state) = prepared_write_state(store, collection, id).await? else {
        return Ok(None);
    };
    if state.replays(new_version, operation_id) {
        return Ok(Some(DocumentWriteOutcome::Applied {
            previous_version: expected_version,
            new_version,
            updated_at_ms: state.updated_at_ms,
        }));
    }
    Ok(Some(
        fenced_outcome(&state, expected_version, activation_epoch)
            .unwrap_or(DocumentWriteOutcome::VersionConflict { expected_version }),
    ))
}

async fn reconcile_upsert_rejection(
    store: &MongoStore,
    collection: &'static str,
    id: Bson,
    expected_version: i64,
    new_version: i64,
    operation_id: &str,
    activation_epoch: i64,
) -> Result<Option<DocumentWriteOutcome>, MongoStoreError> {
    let Some(state) = prepared_write_state(store, collection, id).await? else {
        return Ok(None);
    };
    if state.replays(new_version, operation_id) {
        return Ok(Some(DocumentWriteOutcome::Applied {
            previous_version: expected_version,
            new_version,
            updated_at_ms: state.updated_at_ms,
        }));
    }
    if let Some(fenced) = fenced_outcome(&state, expected_version, activation_epoch) {
        return Ok(Some(fenced));
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
    projection.insert(ACTIVATION_EPOCH_FIELD, 1);
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
                activation_epoch: stored_epoch(&document),
            })
        })
        .transpose()
}

/// Reads the recorded activation epoch. Documents written before storage-level
/// fencing existed carry no field and are claimed by the next prepared write.
/// A value this crate never writes degrades to an ordinary conflict, which
/// still refuses the write that storage already refused.
fn stored_epoch(document: &Document) -> Option<i64> {
    match document.get(ACTIVATION_EPOCH_FIELD) {
        Some(Bson::Int64(value)) => Some(*value),
        Some(Bson::Int32(value)) => Some(i64::from(*value)),
        _ => None,
    }
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

/// Builds the optimistic write filter for one prepared document.
///
/// `$not` deliberately also matches documents that carry no epoch at all, so
/// state written before storage-level fencing existed needs no migration: the
/// first coordinated write stamps the field.
pub(super) fn fenced_filter(id: Bson, expected_version: i64, activation_epoch: i64) -> Document {
    let mut filter = doc! { "_id": id, "version": expected_version };
    filter.insert(
        ACTIVATION_EPOCH_FIELD,
        doc! { "$not": { "$gt": activation_epoch } },
    );
    filter
}

fn stored_activation_epoch(activation_epoch: u64) -> Result<i64, MongoStoreError> {
    i64::try_from(activation_epoch).map_err(|_| {
        MongoStoreError::new(format!(
            "activation epoch {activation_epoch} exceeds the persisted i64 range"
        ))
    })
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
