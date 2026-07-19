use std::collections::BTreeMap;
use std::future::IntoFuture;
use std::time::Duration;

use crate::error::MongoStoreError;
use mongodb::bson::{Bson, Document, doc};
use mongodb::options::ClientOptions;
use mongodb::{Client, Database};
use tracing::info;

use crate::direct::{DeleteOutcome, DirectDocumentStore, InsertOutcome, ReplaceOutcome};
use crate::document::{MongoDocument, decode_flat_document, encode_flat_document};
use crate::prepared::{
    CreateMode, DocumentOperation, DocumentWriteOutcome, FlushOutcome, PreparedDocumentWrite,
    PreparedWriteStore,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MongoStoreConfig {
    pub uri: String,
    pub database: String,
    pub connect_timeout: Duration,
    pub operation_timeout: Duration,
}

#[derive(Clone)]
pub struct MongoStore {
    database: Database,
    operation_timeout: Duration,
}

impl MongoStore {
    pub async fn connect(config: MongoStoreConfig) -> Result<Self, MongoStoreError> {
        if config.uri.trim().is_empty() {
            return Err(MongoStoreError::invalid_config("uri", "cannot be empty"));
        }
        if config.database.trim().is_empty() {
            return Err(MongoStoreError::invalid_config(
                "database",
                "cannot be empty",
            ));
        }
        if config.connect_timeout.is_zero() {
            return Err(MongoStoreError::invalid_config(
                "connect_timeout",
                "must be positive",
            ));
        }
        if config.operation_timeout.is_zero() {
            return Err(MongoStoreError::invalid_config(
                "operation_timeout",
                "must be positive",
            ));
        }

        info!(
            database = %config.database,
            uri = %redact_mongo_uri(&config.uri),
            "mongo.connect.start"
        );

        let mut options = ClientOptions::parse(&config.uri)
            .await
            .map_err(store_error("parse mongo uri"))?;
        options.connect_timeout = Some(config.connect_timeout);
        options.server_selection_timeout = Some(config.connect_timeout);
        let client = Client::with_options(options).map_err(store_error("create mongo client"))?;
        let database = client.database(&config.database);
        mongo_timeout(
            config.operation_timeout,
            "ping mongo",
            database.run_command(doc! { "ping": 1 }),
        )
        .await?;

        info!(database = %config.database, "mongo.connect.success");
        Ok(Self {
            database,
            operation_timeout: config.operation_timeout,
        })
    }

    pub fn database(&self) -> &Database {
        &self.database
    }

    pub fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }

    pub async fn find_one<D>(
        &self,
        id: D::Id,
    ) -> Result<Option<crate::document::LoadedDocument<D>>, MongoStoreError>
    where
        D: MongoDocument,
    {
        let collection = self.database.collection::<Document>(D::COLLECTION);
        let id = mongodb::bson::to_bson(&id).map_err(store_error("encode document id"))?;
        let document = mongo_timeout(
            self.operation_timeout,
            "find typed document",
            collection.find_one(doc! { "_id": id }),
        )
        .await?;
        document.map(decode_flat_document::<D>).transpose()
    }

    pub async fn find_many<D>(
        &self,
        filter: Document,
    ) -> Result<Vec<crate::document::LoadedDocument<D>>, MongoStoreError>
    where
        D: MongoDocument,
    {
        let collection = self.database.collection::<Document>(D::COLLECTION);
        let mut cursor = mongo_timeout(
            self.operation_timeout,
            "find typed documents",
            collection.find(filter),
        )
        .await?;
        let mut documents = Vec::new();
        while mongo_timeout(
            self.operation_timeout,
            "advance typed document cursor",
            cursor.advance(),
        )
        .await?
        {
            let document = cursor
                .deserialize_current()
                .map_err(store_error("decode typed document"))?;
            documents.push(decode_flat_document::<D>(document)?);
        }
        Ok(documents)
    }

    pub async fn find_page<D>(
        &self,
        filter: Document,
        sort: Document,
        limit: u32,
    ) -> Result<Vec<crate::document::LoadedDocument<D>>, MongoStoreError>
    where
        D: MongoDocument,
    {
        if limit == 0 {
            return Err(MongoStoreError::invalid_config(
                "page limit",
                "must be positive",
            ));
        }
        let collection = self.database.collection::<Document>(D::COLLECTION);
        let mut cursor = mongo_timeout(
            self.operation_timeout,
            "find typed document page",
            collection.find(filter).sort(sort).limit(i64::from(limit)),
        )
        .await?;
        let mut documents = Vec::with_capacity(limit as usize);
        while mongo_timeout(
            self.operation_timeout,
            "advance typed document page cursor",
            cursor.advance(),
        )
        .await?
        {
            let document = cursor
                .deserialize_current()
                .map_err(store_error("decode typed document page"))?;
            documents.push(decode_flat_document::<D>(document)?);
        }
        Ok(documents)
    }

    async fn flush_prepared_writes(
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

fn unmatched_prepared_outcome(expected_version: i64, exists: bool) -> DocumentWriteOutcome {
    if exists {
        DocumentWriteOutcome::VersionConflict { expected_version }
    } else {
        DocumentWriteOutcome::NotFound { expected_version }
    }
}

async fn mongo_timeout<F, T, E>(
    duration: Duration,
    context: &'static str,
    future: F,
) -> Result<T, MongoStoreError>
where
    F: IntoFuture<Output = Result<T, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    tokio::time::timeout(duration, future.into_future())
        .await
        .map_err(|_| MongoStoreError::timeout(context, duration))?
        .map_err(store_error(context))
}

fn store_error<E>(context: &'static str) -> impl FnOnce(E) -> MongoStoreError
where
    E: std::error::Error + Send + Sync + 'static,
{
    move |error| MongoStoreError::operation(context, error)
}

pub fn redact_mongo_uri(uri: &str) -> String {
    if let Some((scheme, rest)) = uri.split_once("://")
        && let Some((_, host_and_path)) = rest.rsplit_once('@')
    {
        return format!("{scheme}://<redacted>@{host_and_path}");
    }
    uri.to_owned()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use serde::{Deserialize, Serialize};

    use crate::direct::{DeleteOutcome, DirectDocumentStore, InsertOutcome, ReplaceOutcome};
    use crate::document::MongoDocument;
    use crate::mongo::{MongoDocumentKey, MongoFieldPath};
    use crate::mongo_store::{redact_mongo_uri, unmatched_prepared_outcome};
    use crate::prepared::{
        DocumentOperation, DocumentWriteOutcome, PreparedDocumentWrite, WriteToken,
    };

    use super::{MongoStore, MongoStoreConfig};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct IntegrationDocument {
        id: u64,
        name: String,
        score: i32,
    }

    impl MongoDocument for IntegrationDocument {
        type Id = u64;

        const COLLECTION: &'static str = "persistence_scan_integration";
        const ID_FIELD: &'static str = "id";

        fn id(&self) -> &Self::Id {
            &self.id
        }
    }

    #[tokio::test]
    async fn configured_mongo_verifies_direct_and_prepared_version_semantics() {
        let Ok(uri) = std::env::var("LATTICE_MONGODB_TEST_URI") else {
            return;
        };
        let database = std::env::var("LATTICE_MONGODB_TEST_DATABASE")
            .unwrap_or_else(|_| "lattice_store_mongodb_integration".to_owned());
        let store = MongoStore::connect(MongoStoreConfig {
            uri,
            database,
            connect_timeout: std::time::Duration::from_secs(2),
            operation_timeout: std::time::Duration::from_secs(2),
        })
        .await
        .expect("local MongoDB should connect");
        store
            .database()
            .collection::<mongodb::bson::Document>(IntegrationDocument::COLLECTION)
            .drop()
            .await
            .ok();

        let id = 42_u64;
        let initial = IntegrationDocument {
            id,
            name: "initial".to_owned(),
            score: 1,
        };
        assert_eq!(
            DirectDocumentStore::<IntegrationDocument>::insert(&store, &initial)
                .await
                .expect("direct insert should execute"),
            InsertOutcome::Inserted { version: 1 }
        );
        assert_eq!(
            DirectDocumentStore::<IntegrationDocument>::replace(&store, 9, &initial)
                .await
                .expect("conflicting replace should resolve"),
            ReplaceOutcome::VersionConflict
        );

        let outcome = store
            .flush_prepared_writes(vec![PreparedDocumentWrite {
                token: WriteToken(1),
                key: MongoDocumentKey::new(IntegrationDocument::COLLECTION, id.to_string()),
                document_id: crate::document::encode_document_id::<IntegrationDocument>(&id)
                    .expect("numeric integration id should encode"),
                expected_version: 1,
                operation: DocumentOperation::Update {
                    sets: BTreeMap::from([
                        (
                            MongoFieldPath::new("name"),
                            mongodb::bson::Bson::String("prepared".to_owned()),
                        ),
                        (MongoFieldPath::new("score"), mongodb::bson::Bson::Int32(2)),
                    ]),
                    unsets: BTreeSet::new(),
                },
            }])
            .await
            .expect("prepared update should execute");
        assert!(matches!(
            outcome.documents[&WriteToken(1)],
            DocumentWriteOutcome::Applied {
                previous_version: 1,
                new_version: 2,
                ..
            }
        ));
        let loaded = DirectDocumentStore::<IntegrationDocument>::load(&store, &id)
            .await
            .expect("direct reload should execute")
            .expect("updated document should exist");
        assert_eq!(loaded.version, 2);
        assert_eq!(loaded.value.name, "prepared");
        assert_eq!(loaded.value.score, 2);

        let missing = store
            .flush_prepared_writes(vec![PreparedDocumentWrite {
                token: WriteToken(2),
                key: MongoDocumentKey::new(IntegrationDocument::COLLECTION, "999"),
                document_id: crate::document::encode_document_id::<IntegrationDocument>(&999)
                    .expect("numeric missing id should encode"),
                expected_version: 1,
                operation: DocumentOperation::Delete,
            }])
            .await
            .expect("missing delete should resolve");
        assert!(matches!(
            missing.documents[&WriteToken(2)],
            DocumentWriteOutcome::NotFound {
                expected_version: 1
            }
        ));
        assert_eq!(
            DirectDocumentStore::<IntegrationDocument>::delete(&store, &id, 7)
                .await
                .expect("conflicting direct delete should resolve"),
            DeleteOutcome::VersionConflict
        );
        assert_eq!(
            DirectDocumentStore::<IntegrationDocument>::delete(&store, &id, 2)
                .await
                .expect("direct delete should execute"),
            DeleteOutcome::Deleted
        );
        assert!(
            DirectDocumentStore::<IntegrationDocument>::load(&store, &id)
                .await
                .expect("post-delete load should execute")
                .is_none()
        );

        store
            .database()
            .collection::<mongodb::bson::Document>(IntegrationDocument::COLLECTION)
            .drop()
            .await
            .ok();
    }

    #[test]
    fn unmatched_prepared_write_distinguishes_missing_from_conflict() {
        assert!(matches!(
            unmatched_prepared_outcome(7, true),
            DocumentWriteOutcome::VersionConflict {
                expected_version: 7
            }
        ));
        assert!(matches!(
            unmatched_prepared_outcome(7, false),
            DocumentWriteOutcome::NotFound {
                expected_version: 7
            }
        ));
    }

    #[test]
    fn mongo_uri_redaction_hides_credentials() {
        assert_eq!(
            redact_mongo_uri("mongodb://user:secret@localhost:27017/p9"),
            "mongodb://<redacted>@localhost:27017/p9"
        );
        assert_eq!(
            redact_mongo_uri("mongodb://localhost:27017/p9"),
            "mongodb://localhost:27017/p9"
        );
    }
}
