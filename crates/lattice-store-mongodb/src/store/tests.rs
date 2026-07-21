use std::collections::{BTreeMap, BTreeSet};

use mongodb::bson::doc;
use serde::{Deserialize, Serialize};

use crate::document::MongoDocument;
use crate::document::set::{MongoDefaultDocument, MongoDocumentSet as _};
use crate::document::tracked::Tracked;
use crate::persistence::coordinator::{MongoPersistenceCoordinator, PersistenceError};
use crate::persistence::direct::{
    DeleteOutcome, DirectDocumentStore, InsertOutcome, ReplaceOutcome,
};
use crate::persistence::request::{
    DocumentOperation, DocumentWriteOutcome, PreparedDocumentWrite, WriteToken,
};
use crate::persistence::types::{MongoDocumentKey, MongoFieldPath};

use super::write::unmatched_prepared_outcome;
use super::{MongoStore, MongoStoreConfig, redact_mongo_uri};

#[derive(Debug, Clone, Serialize, Deserialize, crate::MongoDocument, crate::MongoScan)]
#[mongo(collection = "document_set_required_load")]
struct RequiredLoadDocument {
    #[mongo(id)]
    id: u64,
    value: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, crate::MongoDocument, crate::MongoScan)]
#[mongo(collection = "document_set_default_load")]
struct DefaultLoadDocument {
    #[mongo(id)]
    id: u64,
    value: i32,
}

impl MongoDefaultDocument<u64> for DefaultLoadDocument {
    fn default_for(owner_id: &u64) -> Self {
        Self {
            id: *owner_id,
            value: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, crate::MongoDocument, crate::MongoScan)]
#[mongo(collection = "document_set_broken_load")]
struct BrokenLoadDocument {
    #[mongo(id)]
    id: u64,
    value: i32,
}

#[derive(Debug, crate::MongoDocumentSet)]
#[mongo(id = u64)]
struct RequiredOnlyDocuments {
    core: Tracked<RequiredLoadDocument>,
}

#[derive(Debug, crate::MongoDocumentSet)]
#[mongo(id = u64)]
struct DefaultLoadDocuments {
    core: Tracked<RequiredLoadDocument>,
    #[mongo(default)]
    optional: Tracked<DefaultLoadDocument>,
}

#[derive(Debug, crate::MongoDocumentSet)]
#[mongo(id = u64)]
struct QueryFailureDocuments {
    core: Tracked<RequiredLoadDocument>,
    broken: Tracked<BrokenLoadDocument>,
}

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

    let insert_only_conflict = store
        .flush_prepared_writes(vec![PreparedDocumentWrite {
            token: WriteToken(4),
            key: MongoDocumentKey::new(IntegrationDocument::COLLECTION, id.to_string()),
            document_id: crate::document::encode_document_id::<IntegrationDocument>(&id)
                .expect("numeric integration id should encode"),
            expected_version: 0,
            operation_id: "insert-only-conflict".to_owned(),
            operation: DocumentOperation::Create {
                document: doc! { "name": "must not overwrite", "score": 99 },
                mode: crate::persistence::request::CreateMode::InsertOnly,
            },
        }])
        .await
        .expect("insert-only create conflict should resolve");
    assert!(matches!(
        insert_only_conflict.documents[&WriteToken(4)],
        DocumentWriteOutcome::VersionConflict {
            expected_version: 0
        }
    ));
    let after_insert_only = DirectDocumentStore::<IntegrationDocument>::load(&store, &id)
        .await
        .expect("conflicted insert-only document should load")
        .expect("conflicted insert-only document should remain present");
    assert_eq!(after_insert_only.value.name, "initial");
    assert_eq!(after_insert_only.value.score, 1);

    let prepared_update = PreparedDocumentWrite {
        token: WriteToken(1),
        key: MongoDocumentKey::new(IntegrationDocument::COLLECTION, id.to_string()),
        document_id: crate::document::encode_document_id::<IntegrationDocument>(&id)
            .expect("numeric integration id should encode"),
        expected_version: 1,
        operation_id: "integration-update-1".to_owned(),
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
    };
    let outcome = store
        .flush_prepared_writes(vec![prepared_update.clone()])
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

    let replayed = store
        .flush_prepared_writes(vec![prepared_update])
        .await
        .expect("prepared update replay should reconcile");
    assert!(matches!(
        replayed.documents[&WriteToken(1)],
        DocumentWriteOutcome::Applied {
            previous_version: 1,
            new_version: 2,
            ..
        }
    ));
    let after_replay = DirectDocumentStore::<IntegrationDocument>::load(&store, &id)
        .await
        .expect("replayed document should load")
        .expect("replayed document should remain present");
    assert_eq!(after_replay.version, 2);

    let stale_upsert = store
        .flush_prepared_writes(vec![PreparedDocumentWrite {
            token: WriteToken(3),
            key: MongoDocumentKey::new(IntegrationDocument::COLLECTION, id.to_string()),
            document_id: crate::document::encode_document_id::<IntegrationDocument>(&id)
                .expect("numeric integration id should encode"),
            expected_version: 0,
            operation_id: "stale-upsert".to_owned(),
            operation: DocumentOperation::Create {
                document: doc! { "name": "overwritten", "score": 99 },
                mode: crate::persistence::request::CreateMode::UpsertAllowed,
            },
        }])
        .await
        .expect("stale upsert should resolve as a conflict");
    assert!(matches!(
        stale_upsert.documents[&WriteToken(3)],
        DocumentWriteOutcome::VersionConflict {
            expected_version: 0
        }
    ));
    let after_upsert = DirectDocumentStore::<IntegrationDocument>::load(&store, &id)
        .await
        .expect("conflicted upsert document should load")
        .expect("conflicted upsert document should remain present");
    assert_eq!(after_upsert.version, 2);
    assert_eq!(after_upsert.value.name, "prepared");

    let missing = store
        .flush_prepared_writes(vec![PreparedDocumentWrite {
            token: WriteToken(2),
            key: MongoDocumentKey::new(IntegrationDocument::COLLECTION, "999"),
            document_id: crate::document::encode_document_id::<IntegrationDocument>(&999)
                .expect("numeric missing id should encode"),
            expected_version: 1,
            operation_id: "integration-delete-1".to_owned(),
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

#[tokio::test]
async fn configured_mongo_document_sets_distinguish_required_and_default_missing() {
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
    for collection in [
        RequiredLoadDocument::COLLECTION,
        DefaultLoadDocument::COLLECTION,
        BrokenLoadDocument::COLLECTION,
    ] {
        store
            .database()
            .collection::<mongodb::bson::Document>(collection)
            .drop()
            .await
            .ok();
    }

    let id = 42;
    let mut missing_coordinator = MongoPersistenceCoordinator::new(1);
    let error = RequiredOnlyDocuments::load(&store, &id, &mut missing_coordinator)
        .await
        .expect_err("required singleton must not default");
    assert!(matches!(
        error,
        PersistenceError::RequiredDocumentMissing { .. }
    ));

    DirectDocumentStore::<RequiredLoadDocument>::insert(
        &store,
        &RequiredLoadDocument { id, value: 7 },
    )
    .await
    .expect("required test document should insert");
    let mut default_coordinator = MongoPersistenceCoordinator::new(2);
    let documents = DefaultLoadDocuments::load(&store, &id, &mut default_coordinator)
        .await
        .expect("missing opted-in singleton should default");
    assert_eq!(documents.optional.id, id);
    assert_eq!(documents.optional.value, 0);
    let untouched = default_coordinator
        .prepare_set(crate::scan::ScanBudget::generous(), &documents)
        .expect("untouched default should prepare");
    assert!(untouched.request.is_none());

    store
        .database()
        .collection::<mongodb::bson::Document>(BrokenLoadDocument::COLLECTION)
        .insert_one(doc! { "_id": id as i64, "value": "wrong type" })
        .await
        .expect("malformed test document should insert");
    let mut atomic_coordinator = MongoPersistenceCoordinator::new(3);
    QueryFailureDocuments::load(&store, &id, &mut atomic_coordinator)
        .await
        .expect_err("later eager query should fail decoding");
    RequiredOnlyDocuments::load(&store, &id, &mut atomic_coordinator)
        .await
        .expect("failed multi-field load must not register its earlier query");

    for collection in [
        RequiredLoadDocument::COLLECTION,
        DefaultLoadDocument::COLLECTION,
        BrokenLoadDocument::COLLECTION,
    ] {
        store
            .database()
            .collection::<mongodb::bson::Document>(collection)
            .drop()
            .await
            .ok();
    }
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
