use std::collections::BTreeMap;
use std::time::Duration;

use mongodb::bson::Bson;
use serde::{Deserialize, Serialize};

use super::{MongoPersistenceCoordinator, PersistenceError, RetryPolicy};
use crate::document::{LoadedDocument, LoadedDocumentMeta};
use crate::error::MongoStoreError;
use crate::persistence::request::{
    CreateMode, DocumentOperation, DocumentWriteOutcome, FlushOutcome,
};
use crate::persistence::types::MongoDocumentKey;
use crate::scan::ScanBudget;
use crate::{MongoDocument as MongoDocumentDerive, MongoScan};

#[derive(Debug, Clone, Serialize, Deserialize, MongoDocumentDerive, MongoScan)]
#[mongo(collection = "coordinator_test")]
struct TestDocument {
    #[mongo(id)]
    id: u64,
    name: String,
    #[mongo(scan = "map")]
    items: BTreeMap<String, i32>,
}

fn document(name: &str) -> TestDocument {
    TestDocument {
        id: 42,
        name: name.to_owned(),
        items: BTreeMap::from([("one".to_owned(), 1)]),
    }
}

fn loaded(value: &TestDocument, mutation_epoch: Option<u64>) -> MongoPersistenceCoordinator {
    let mut coordinator = MongoPersistenceCoordinator::new(7);
    let meta = LoadedDocumentMeta {
        version: 3,
        updated_at_ms: 10,
    };
    if let Some(epoch) = mutation_epoch {
        coordinator
            .attach_loaded_tracked(value, epoch, meta)
            .expect("tracked document should attach");
    } else {
        coordinator
            .attach_loaded(value, meta)
            .expect("document should attach");
    }
    coordinator
}

#[test]
fn batch_registration_rejects_duplicates_without_partial_attachment() {
    let first = document("first");
    let duplicate = document("duplicate");
    let mut coordinator = MongoPersistenceCoordinator::new(7);
    let error = coordinator
        .track_loaded_many(vec![
            LoadedDocument {
                version: 1,
                updated_at_ms: 1,
                value: first.clone(),
            },
            LoadedDocument {
                version: 2,
                updated_at_ms: 2,
                value: duplicate,
            },
        ])
        .expect_err("duplicate batch IDs must be rejected");
    assert!(matches!(error, PersistenceError::DuplicateDocument(_)));

    coordinator
        .track_loaded(LoadedDocument {
            version: 1,
            updated_at_ms: 1,
            value: first,
        })
        .expect("failed batch must not leave the first document attached");
}

#[test]
fn tracked_documents_skip_unchanged_epochs_and_commit_metadata_after_ack() {
    let mut value = crate::document::tracked::Tracked::clean(document("old"));
    let mut coordinator = loaded(value.read(), Some(0));
    let clean = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&value)
        })
        .expect("unchanged preparation should succeed");
    assert!(clean.request.is_none());
    assert_eq!(coordinator.counters().scans, 0);

    value.write().name = "new".to_owned();
    let prepared = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&value)
        })
        .expect("changed preparation should succeed");
    let request = prepared.request.as_ref().expect("write should be prepared");
    let write = &request.writes[0];
    let DocumentOperation::Update { sets, .. } = &write.operation else {
        panic!("loaded document should update");
    };
    assert_eq!(sets.values().next(), Some(&Bson::String("new".to_owned())));
    let generation = request.generation;
    let token = write.token;
    coordinator
        .begin_flush(prepared.commit)
        .expect("flush should begin");
    coordinator
        .complete(
            generation,
            FlushOutcome {
                documents: BTreeMap::from([(
                    token,
                    DocumentWriteOutcome::Applied {
                        previous_version: 3,
                        new_version: 4,
                        updated_at_ms: 99,
                    },
                )]),
            },
        )
        .expect("flush should complete");

    let key = MongoDocumentKey::for_document::<TestDocument>(&42)
        .expect("test document ID should encode");
    assert_eq!(coordinator.document_meta(&key), Some((4, 99)));
    let clean = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&value)
        })
        .expect("acknowledged epoch should prepare");
    assert!(clean.request.is_none());
    assert_eq!(coordinator.counters().scans, 1);
}

#[test]
fn mutable_access_without_a_change_causes_only_a_false_positive_scan() {
    let mut value = crate::document::tracked::Tracked::clean(document("old"));
    let mut coordinator = loaded(value.read(), Some(0));
    let _ = value.write();

    let prepared = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&value)
        })
        .expect("false-positive dirty epoch should scan normally");

    assert!(prepared.request.is_none());
    assert_eq!(coordinator.counters().scans, 1);
    assert_eq!(coordinator.scan_metrics().encoded_values, 2);
    assert!(coordinator.scan_metrics().estimated_encoded_bytes > 0);
    assert_eq!(coordinator.scan_metrics().map_entries_hashed, 1);
    coordinator
        .complete_clean(prepared.commit)
        .expect("clean scan should acknowledge the newer epoch");
    assert_eq!(coordinator.scan_metrics().false_positive_scans, 1);
    let skipped = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&value)
        })
        .expect("acknowledged false positive should be skipped");
    assert!(skipped.request.is_none());
    assert_eq!(coordinator.counters().scans, 1);
}

#[test]
fn failed_write_preserves_baseline_and_schedules_retry() {
    let old = document("old");
    let mut value = old.clone();
    value.name = "new".to_owned();
    let mut coordinator = MongoPersistenceCoordinator::with_retry_policy(
        7,
        RetryPolicy {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(2),
            max_exponent: 6,
        },
    );
    coordinator
        .attach_loaded(
            &old,
            LoadedDocumentMeta {
                version: 3,
                updated_at_ms: 10,
            },
        )
        .expect("document should attach");
    let prepared = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan(&value)
        })
        .expect("write should prepare");
    let request = prepared.request.as_ref().expect("write should exist");
    let generation = request.generation;
    let token = request.writes[0].token;
    let operation_id = request.writes[0].operation_id.clone();
    coordinator.begin_flush(prepared.commit).unwrap();
    let report = coordinator
        .complete(
            generation,
            FlushOutcome {
                documents: BTreeMap::from([(
                    token,
                    DocumentWriteOutcome::Failed {
                        error: MongoStoreError::new("offline"),
                    },
                )]),
            },
        )
        .unwrap();
    assert_eq!(report.failed, 1);
    assert_eq!(coordinator.retry_attempt(), 1);
    assert!(coordinator.retry_delay().is_some());

    let retry = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan(&value)
        })
        .expect("retry should preserve the exact ambiguous write");
    let retry_write = &retry.request.as_ref().expect("retry should exist").writes[0];
    assert_eq!(retry_write.operation_id, operation_id);
    assert_eq!(retry_write.token, token);
    assert_eq!(retry.commit.generation, generation);
}

#[test]
fn rejected_write_waits_for_mutation_then_reprepares_current_state() {
    let old = document("old");
    let mut value = crate::document::tracked::Tracked::clean(old.clone());
    let mut coordinator = loaded(value.read(), Some(0));
    value.write().items.insert("oversized".to_owned(), 2);

    let rejected = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&value)
        })
        .expect("oversized state should prepare once");
    let rejected_request = rejected.request.as_ref().expect("write should exist");
    let rejected_generation = rejected_request.generation;
    let rejected_token = rejected_request.writes[0].token;
    let rejected_operation_id = rejected_request.writes[0].operation_id.clone();
    coordinator.begin_flush(rejected.commit).unwrap();
    let report = coordinator
        .complete(
            rejected_generation,
            FlushOutcome {
                documents: BTreeMap::from([(
                    rejected_token,
                    DocumentWriteOutcome::Failed {
                        error: MongoStoreError::rejected(
                            "document exceeds MongoDB's maximum BSON size",
                        ),
                    },
                )]),
            },
        )
        .expect("definitive rejection should be recorded");
    assert_eq!(report.failed, 1);
    assert_eq!(coordinator.retry_attempt(), 0);
    assert!(coordinator.retry_delay().is_none());
    let key = MongoDocumentKey::for_document::<TestDocument>(&42).unwrap();
    assert!(
        coordinator
            .document_rejection(&key)
            .is_some_and(|error| error.contains("maximum BSON size"))
    );

    let unchanged = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&value)
        })
        .expect("same rejected epoch should remain locally blocked");
    assert!(unchanged.request.is_none());
    assert!(!unchanged.scan_complete);

    {
        let current = value.write();
        current.items.remove("oversized");
        current.name = "small".to_owned();
    }
    let recovered = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&value)
        })
        .expect("new mutation epoch should be reprepared");
    let recovered_request = recovered
        .request
        .as_ref()
        .expect("fresh write should exist");
    assert_ne!(recovered_request.generation, rejected_generation);
    assert_ne!(
        recovered_request.writes[0].operation_id,
        rejected_operation_id
    );
    let DocumentOperation::Update { sets, .. } = &recovered_request.writes[0].operation else {
        panic!("loaded document should update");
    };
    assert_eq!(
        sets.get(&crate::persistence::types::MongoFieldPath::new("name")),
        Some(&Bson::String("small".to_owned())),
    );
    assert!(!sets.keys().any(|path| path.0.starts_with("items.")));

    let generation = recovered_request.generation;
    let token = recovered_request.writes[0].token;
    coordinator.begin_flush(recovered.commit).unwrap();
    coordinator
        .complete(
            generation,
            FlushOutcome {
                documents: BTreeMap::from([(
                    token,
                    DocumentWriteOutcome::Applied {
                        previous_version: 3,
                        new_version: 4,
                        updated_at_ms: 22,
                    },
                )]),
            },
        )
        .expect("smaller current state should apply");
    assert!(coordinator.document_rejection(&key).is_none());
    assert!(coordinator.last_error().is_none());
}

#[test]
fn new_document_cannot_detach_until_create_is_acknowledged() {
    let mut value = document("new");
    value.id = 9;
    let mut coordinator = MongoPersistenceCoordinator::new(4);
    let value = coordinator
        .track_new(value, CreateMode::InsertOnly)
        .unwrap();
    assert!(matches!(
        coordinator.detach::<TestDocument>(&9),
        Err(PersistenceError::CreatePending(_))
    ));
    let prepared = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&value)
        })
        .unwrap();
    let request = prepared.request.as_ref().unwrap();
    assert!(matches!(
        request.writes[0].operation,
        DocumentOperation::Create {
            mode: CreateMode::InsertOnly,
            ..
        }
    ));
    let generation = request.generation;
    let token = request.writes[0].token;
    coordinator.begin_flush(prepared.commit).unwrap();
    coordinator
        .complete(
            generation,
            FlushOutcome {
                documents: BTreeMap::from([(
                    token,
                    DocumentWriteOutcome::Applied {
                        previous_version: 0,
                        new_version: 1,
                        updated_at_ms: 55,
                    },
                )]),
            },
        )
        .unwrap();
    coordinator.detach::<TestDocument>(&9).unwrap();
}

#[test]
fn version_conflict_blocks_further_preparation() {
    let old = document("old");
    let mut value = old.clone();
    value.name = "new".to_owned();
    let mut coordinator = loaded(&old, None);
    let prepared = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan(&value)
        })
        .unwrap();
    let request = prepared.request.as_ref().unwrap();
    let generation = request.generation;
    let token = request.writes[0].token;
    coordinator.begin_flush(prepared.commit).unwrap();
    coordinator
        .complete(
            generation,
            FlushOutcome {
                documents: BTreeMap::from([(
                    token,
                    DocumentWriteOutcome::VersionConflict {
                        expected_version: 3,
                    },
                )]),
            },
        )
        .unwrap();
    assert_eq!(coordinator.conflict().unwrap().expected_version, 3);
    assert!(matches!(
        coordinator.prepare(ScanBudget::generous(), |_| Ok(())),
        Err(PersistenceError::ConflictBlocked)
    ));
}

#[test]
fn budgeted_scan_commits_progress_and_resumes_at_next_field() {
    let old = document("old");
    let mut value = old.clone();
    value.name = "new".to_owned();
    value.items.insert("two".to_owned(), 2);
    let mut coordinator = loaded(&old, None);
    let partial = coordinator
        .prepare(
            ScanBudget::new(1, 1, Duration::from_secs(1)),
            |preparation| preparation.scan(&value),
        )
        .unwrap();
    assert!(!partial.scan_complete);
    let request = partial.request.as_ref().unwrap();
    let DocumentOperation::Update { sets, .. } = &request.writes[0].operation else {
        panic!("partial document should update");
    };
    assert!(sets.keys().any(|path| path.0 == "name"));
    assert!(!sets.keys().any(|path| path.0 == "items.two"));
    let generation = request.generation;
    let token = request.writes[0].token;
    coordinator.begin_flush(partial.commit).unwrap();
    coordinator
        .complete(
            generation,
            FlushOutcome {
                documents: BTreeMap::from([(
                    token,
                    DocumentWriteOutcome::Applied {
                        previous_version: 3,
                        new_version: 4,
                        updated_at_ms: 20,
                    },
                )]),
            },
        )
        .unwrap();

    let resumed = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan(&value)
        })
        .unwrap();
    assert!(resumed.scan_complete);
    let DocumentOperation::Update { sets, .. } = &resumed.request.unwrap().writes[0].operation
    else {
        panic!("resumed document should update");
    };
    assert!(sets.keys().any(|path| path.0 == "items.two"));
    assert!(!sets.keys().any(|path| path.0 == "name"));
}

#[test]
fn mutation_during_a_field_sweep_finishes_the_sweep_then_rescans_from_start() {
    let old = document("old");
    let mut value = crate::document::tracked::Tracked::clean(old.clone());
    let mut coordinator = loaded(&old, Some(0));
    value.write().name = "first".to_owned();

    let first = coordinator
        .prepare(
            ScanBudget::new(1, 1, Duration::from_secs(1)),
            |preparation| preparation.scan_tracked(&value),
        )
        .expect("first field should prepare");
    assert!(!first.scan_complete);
    let first_request = first.request.as_ref().expect("name should change");
    let first_token = first_request.writes[0].token;
    let first_generation = first_request.generation;
    coordinator.begin_flush(first.commit).unwrap();
    coordinator
        .complete(
            first_generation,
            FlushOutcome {
                documents: BTreeMap::from([(
                    first_token,
                    DocumentWriteOutcome::Applied {
                        previous_version: 3,
                        new_version: 4,
                        updated_at_ms: 20,
                    },
                )]),
            },
        )
        .unwrap();

    {
        let write = value.write();
        write.name = "second".to_owned();
        write.items.insert("two".to_owned(), 2);
    }
    let second = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&value)
        })
        .expect("remaining field should prepare");
    assert!(!second.scan_complete);
    let second_request = second.request.as_ref().expect("map should change");
    let DocumentOperation::Update { sets, .. } = &second_request.writes[0].operation else {
        panic!("map field should update");
    };
    assert!(sets.keys().any(|path| path.0 == "items.two"));
    assert!(!sets.keys().any(|path| path.0 == "name"));
    let second_token = second_request.writes[0].token;
    let second_generation = second_request.generation;
    coordinator.begin_flush(second.commit).unwrap();
    coordinator
        .complete(
            second_generation,
            FlushOutcome {
                documents: BTreeMap::from([(
                    second_token,
                    DocumentWriteOutcome::Applied {
                        previous_version: 4,
                        new_version: 5,
                        updated_at_ms: 30,
                    },
                )]),
            },
        )
        .unwrap();

    let third = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&value)
        })
        .expect("new epoch should receive a complete follow-up sweep");
    assert!(third.scan_complete);
    let third_request = third
        .request
        .as_ref()
        .expect("earlier field should be rescanned");
    let DocumentOperation::Update { sets, .. } = &third_request.writes[0].operation else {
        panic!("name field should update");
    };
    assert!(sets.keys().any(|path| path.0 == "name"));
    assert!(!sets.keys().any(|path| path.0 == "items.two"));
}

#[test]
fn token_mismatch_is_rejected_without_consuming_in_flight_commit() {
    let old = document("old");
    let mut value = old.clone();
    value.name = "new".to_owned();
    let mut coordinator = loaded(&old, None);
    let prepared = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan(&value)
        })
        .unwrap();
    let generation = prepared.request.as_ref().unwrap().generation;
    coordinator.begin_flush(prepared.commit).unwrap();
    assert!(matches!(
        coordinator.complete(generation, FlushOutcome::default()),
        Err(PersistenceError::OutcomeTokenMismatch)
    ));
    assert!(coordinator.has_in_flight());
}
