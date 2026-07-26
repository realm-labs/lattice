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

use super::read::owner_load_is_large;
use super::write::{fenced_filter, unmatched_prepared_outcome};
use super::{ActivationFence, MongoStore, MongoStoreConfig, redact_mongo_uri};

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

/// Connects to the MongoDB instance named by `LATTICE_MONGODB_TEST_URI`.
/// Tests that need real storage are skipped when it is unset.
async fn configured_store() -> Option<MongoStore> {
    let uri = std::env::var("LATTICE_MONGODB_TEST_URI").ok()?;
    let database = std::env::var("LATTICE_MONGODB_TEST_DATABASE")
        .unwrap_or_else(|_| "lattice_store_mongodb_integration".to_owned());
    Some(
        MongoStore::connect(MongoStoreConfig {
            uri,
            database,
            connect_timeout: std::time::Duration::from_secs(2),
            operation_timeout: std::time::Duration::from_secs(2),
            max_pool_size: None,
            min_pool_size: None,
        })
        .await
        .expect("configured MongoDB should connect"),
    )
}

#[tokio::test]
async fn configured_mongo_verifies_direct_and_prepared_version_semantics() {
    let Some(store) = configured_store().await else {
        return;
    };
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
            activation_epoch: 5,
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
        activation_epoch: 5,
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
            activation_epoch: 5,
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
            activation_epoch: 5,
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
    let Some(store) = configured_store().await else {
        return;
    };
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

#[derive(Debug, Clone, Serialize, Deserialize, crate::MongoDocument, crate::MongoScan)]
#[mongo(collection = "activation_fence_test")]
struct FenceDocument {
    #[mongo(id)]
    id: u64,
    value: String,
}

/// Fence tests share one collection but never the same document, so they clean
/// up by identity instead of dropping state a parallel test still needs.
async fn fence_collection(
    store: &MongoStore,
    id: u64,
) -> mongodb::Collection<mongodb::bson::Document> {
    let collection = store
        .database()
        .collection::<mongodb::bson::Document>(FenceDocument::COLLECTION);
    drop_fence_document(&collection, id).await;
    collection
}

async fn drop_fence_document(collection: &mongodb::Collection<mongodb::bson::Document>, id: u64) {
    collection
        .delete_one(doc! { "_id": id as i64 })
        .await
        .expect("fence test document should be removable");
}

fn fence_write(
    token: u64,
    id: u64,
    expected_version: i64,
    activation_epoch: u64,
    operation_id: &str,
    operation: DocumentOperation,
) -> PreparedDocumentWrite {
    PreparedDocumentWrite {
        token: WriteToken(token),
        key: MongoDocumentKey::new(FenceDocument::COLLECTION, id.to_string()),
        document_id: crate::document::encode_document_id::<FenceDocument>(&id)
            .expect("numeric fence id should encode"),
        expected_version,
        operation_id: operation_id.to_owned(),
        activation_epoch,
        operation,
    }
}

fn fence_update(value: &str) -> DocumentOperation {
    DocumentOperation::Update {
        sets: BTreeMap::from([(
            MongoFieldPath::new("value"),
            mongodb::bson::Bson::String(value.to_owned()),
        )]),
        unsets: BTreeSet::new(),
    }
}

#[tokio::test]
async fn configured_mongo_refuses_writes_from_an_activation_a_newer_one_fenced() {
    let Some(store) = configured_store().await else {
        return;
    };
    let id = 7_u64;
    let collection = fence_collection(&store, id).await;
    let created = store
        .flush_prepared_writes(vec![fence_write(
            1,
            id,
            0,
            5,
            "old-create",
            DocumentOperation::Create {
                document: doc! { "value": "first" },
                mode: crate::persistence::request::CreateMode::InsertOnly,
            },
        )])
        .await
        .expect("create should execute");
    assert!(matches!(
        created.documents[&WriteToken(1)],
        DocumentWriteOutcome::Applied { new_version: 1, .. }
    ));

    let owned = store
        .flush_prepared_writes(vec![fence_write(
            2,
            id,
            1,
            5,
            "old-update",
            fence_update("second"),
        )])
        .await
        .expect("update from the owning activation should execute");
    assert!(matches!(
        owned.documents[&WriteToken(2)],
        DocumentWriteOutcome::Applied { new_version: 2, .. }
    ));

    assert_eq!(
        store
            .fence_document::<FenceDocument>(&id, 9)
            .await
            .expect("a newer activation should claim the document"),
        ActivationFence::Claimed
    );

    let fenced = store
        .flush_prepared_writes(vec![fence_write(
            3,
            id,
            2,
            5,
            "old-fenced-update",
            fence_update("must not land"),
        )])
        .await
        .expect("the superseded write should resolve");
    assert!(
        matches!(
            fenced.documents[&WriteToken(3)],
            DocumentWriteOutcome::Fenced {
                expected_version: 2,
                observed_epoch: 9,
            }
        ),
        "a matching version must not let a fenced activation write: {:?}",
        fenced.documents[&WriteToken(3)]
    );
    let untouched = DirectDocumentStore::<FenceDocument>::load(&store, &id)
        .await
        .expect("fenced document should load")
        .expect("fenced document should remain present");
    assert_eq!(untouched.version, 2);
    assert_eq!(untouched.value.value, "second");

    let taken_over = store
        .flush_prepared_writes(vec![fence_write(
            4,
            id,
            2,
            9,
            "new-update",
            fence_update("third"),
        )])
        .await
        .expect("the owning activation should execute");
    assert!(matches!(
        taken_over.documents[&WriteToken(4)],
        DocumentWriteOutcome::Applied {
            previous_version: 2,
            new_version: 3,
            ..
        }
    ));

    assert_eq!(
        store
            .fence_document::<FenceDocument>(&id, 4)
            .await
            .expect("an older claim should resolve"),
        ActivationFence::Superseded { observed_epoch: 9 }
    );
    assert_eq!(
        store
            .fence_document::<FenceDocument>(&999, 9)
            .await
            .expect("claiming a missing document should resolve"),
        ActivationFence::Absent
    );

    drop_fence_document(&collection, id).await;
}

#[tokio::test]
async fn configured_mongo_claims_an_owner_collection_and_reports_rows_it_lost() {
    let Some(store) = configured_store().await else {
        return;
    };
    let ids = [41_u64, 42, 43];
    let collection = fence_collection(&store, ids[0]).await;
    for id in ids {
        drop_fence_document(&collection, id).await;
        DirectDocumentStore::<FenceDocument>::insert(
            &store,
            &FenceDocument {
                id,
                value: "owned".to_owned(),
            },
        )
        .await
        .expect("collection row should insert");
    }
    let owner_filter = doc! { "_id": { "$in": ids.map(|id| id as i64).to_vec() } };

    assert_eq!(
        store
            .fence_documents::<FenceDocument>(owner_filter.clone(), 5)
            .await
            .expect("the collection should be claimed as one unit"),
        super::ActivationFenceSummary {
            claimed: 3,
            superseded: 0,
        }
    );
    store
        .fence_document::<FenceDocument>(&ids[1], 9)
        .await
        .expect("a newer activation should claim one row");
    assert_eq!(
        store
            .fence_documents::<FenceDocument>(owner_filter, 5)
            .await
            .expect("a partially lost collection should be reported"),
        super::ActivationFenceSummary {
            claimed: 2,
            superseded: 1,
        }
    );

    for id in ids {
        drop_fence_document(&collection, id).await;
    }
}

#[tokio::test]
async fn configured_mongo_replays_an_acknowledged_write_even_after_being_fenced() {
    let Some(store) = configured_store().await else {
        return;
    };
    let id = 11_u64;
    let collection = fence_collection(&store, id).await;
    store
        .flush_prepared_writes(vec![fence_write(
            1,
            id,
            0,
            5,
            "create",
            DocumentOperation::Create {
                document: doc! { "value": "first" },
                mode: crate::persistence::request::CreateMode::InsertOnly,
            },
        )])
        .await
        .expect("create should execute");
    let ambiguous = fence_write(2, id, 1, 5, "ambiguous-update", fence_update("second"));
    store
        .flush_prepared_writes(vec![ambiguous.clone()])
        .await
        .expect("the first attempt should execute");

    assert_eq!(
        store
            .fence_document::<FenceDocument>(&id, 9)
            .await
            .expect("a newer activation should claim the document"),
        ActivationFence::Claimed
    );

    let replayed = store
        .flush_prepared_writes(vec![ambiguous])
        .await
        .expect("the exact replay should reconcile");
    assert!(
        matches!(
            replayed.documents[&WriteToken(2)],
            DocumentWriteOutcome::Applied {
                previous_version: 1,
                new_version: 2,
                ..
            }
        ),
        "fencing must not reclassify an already acknowledged write: {:?}",
        replayed.documents[&WriteToken(2)]
    );

    drop_fence_document(&collection, id).await;
}

#[tokio::test]
async fn configured_mongo_claims_documents_written_before_fencing_existed() {
    let Some(store) = configured_store().await else {
        return;
    };
    let id = 21_u64;
    let collection = fence_collection(&store, id).await;
    collection
        .insert_one(doc! {
            "_id": id as i64,
            "version": 4_i64,
            "updated_at_ms": 1_i64,
            "value": "legacy",
        })
        .await
        .expect("legacy document should insert");

    let claimed = store
        .flush_prepared_writes(vec![fence_write(
            1,
            id,
            4,
            3,
            "claiming-update",
            fence_update("migrated"),
        )])
        .await
        .expect("a document without an epoch should be writable");
    assert!(matches!(
        claimed.documents[&WriteToken(1)],
        DocumentWriteOutcome::Applied { new_version: 5, .. }
    ));
    let stored = collection
        .find_one(doc! { "_id": id as i64 })
        .await
        .expect("claimed document should load")
        .expect("claimed document should remain present");
    assert_eq!(stored.get_i64("_lattice_epoch").ok(), Some(3));

    drop_fence_document(&collection, id).await;
}

#[test]
fn prepared_write_filters_pin_the_version_and_the_activation_epoch() {
    let filter = fenced_filter(mongodb::bson::Bson::Int64(3), 7, 5);
    assert_eq!(filter.get_i64("version").ok(), Some(7));
    assert_eq!(
        filter.get_document("_lattice_epoch").ok(),
        Some(&doc! { "$not": { "$gt": 5_i64 } }),
        "a prepared write must refuse documents owned by a newer activation"
    );
}

#[test]
fn owner_collection_loads_warn_once_they_stop_being_activation_sized() {
    assert!(!owner_load_is_large(999));
    assert!(owner_load_is_large(1_000));
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

fn pool_config(max_pool_size: Option<u32>, min_pool_size: Option<u32>) -> MongoStoreConfig {
    MongoStoreConfig {
        uri: "mongodb://127.0.0.1:27017".to_owned(),
        database: "lattice_pool_config".to_owned(),
        connect_timeout: std::time::Duration::from_secs(3),
        operation_timeout: std::time::Duration::from_secs(4),
        max_pool_size,
        min_pool_size,
    }
}

#[test]
fn configured_pool_bounds_reach_the_driver_without_erasing_uri_defaults() {
    let mut options = mongodb::options::ClientOptions::default();
    options.max_pool_size = Some(99);
    options.min_pool_size = Some(7);
    super::apply_client_options(&mut options, &pool_config(None, None));
    assert_eq!(options.max_pool_size, Some(99));
    assert_eq!(options.min_pool_size, Some(7));
    assert_eq!(
        options.connect_timeout,
        Some(std::time::Duration::from_secs(3))
    );

    super::apply_client_options(&mut options, &pool_config(Some(32), Some(4)));
    assert_eq!(options.max_pool_size, Some(32));
    assert_eq!(options.min_pool_size, Some(4));
}

#[tokio::test]
async fn invalid_pool_bounds_are_rejected_before_any_client_is_built() {
    let Err(zero) = MongoStore::connect(pool_config(Some(0), None)).await else {
        panic!("a zero-sized pool cannot serve any operation");
    };
    assert!(zero.message().contains("max_pool_size"));
    let Err(inverted) = MongoStore::connect(pool_config(Some(2), Some(8))).await else {
        panic!("a minimum above the maximum cannot be satisfied");
    };
    assert!(inverted.message().contains("min_pool_size"));
}

#[tokio::test]
async fn configured_mongo_serves_several_stores_from_one_client_pool() {
    let Some(store) = configured_store().await else {
        return;
    };
    let shared =
        MongoStore::from_database(store.database().clone(), std::time::Duration::from_secs(2))
            .expect("an existing database should build a store");
    let id = 31_u64;
    let collection = fence_collection(&store, id).await;
    DirectDocumentStore::<FenceDocument>::insert(
        &store,
        &FenceDocument {
            id,
            value: "shared".to_owned(),
        },
    )
    .await
    .expect("the first store should write");
    let loaded = DirectDocumentStore::<FenceDocument>::load(&shared, &id)
        .await
        .expect("the shared store should read")
        .expect("the document written through the shared client should exist");
    assert_eq!(loaded.value.value, "shared");

    assert!(
        MongoStore::from_database(store.database().clone(), std::time::Duration::ZERO).is_err(),
        "a store must not be built without an operation timeout"
    );
    drop_fence_document(&collection, id).await;
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
