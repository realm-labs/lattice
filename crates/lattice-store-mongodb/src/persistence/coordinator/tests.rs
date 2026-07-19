use std::collections::BTreeMap;
use std::time::Duration;

use mongodb::bson::Bson;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{
    ConflictPolicy, MongoPersistenceCoordinator, PersistenceConflictKind, PersistenceError,
    RetryPolicy,
};
use crate::document::{LoadedDocument, LoadedDocumentMeta};
use crate::error::MongoStoreError;
use crate::persistence::actor::{CompletionStatus, MongoFlushCompleted};
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

#[derive(Debug, Clone)]
struct RejectingString {
    value: String,
    reject: bool,
}

impl Serialize for RejectingString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.reject {
            return Err(serde::ser::Error::custom(
                "intentional test encoding failure",
            ));
        }
        self.value.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RejectingString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self {
            value: String::deserialize(deserializer)?,
            reject: false,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, MongoDocumentDerive, MongoScan)]
#[mongo(collection = "coordinator_rejecting_test", conflict = "quarantine")]
struct RejectingDocument {
    #[mongo(id)]
    id: u64,
    payload: RejectingString,
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
fn preparation_rejection_isolated_to_one_document_and_recovers_after_mutation() {
    let mut rejected = crate::document::tracked::Tracked::clean(RejectingDocument {
        id: 42,
        payload: RejectingString {
            value: "initial".to_owned(),
            reject: false,
        },
    });
    let mut healthy = crate::document::tracked::Tracked::clean(document("old"));
    let mut coordinator = MongoPersistenceCoordinator::new(7);
    let meta = LoadedDocumentMeta {
        version: 3,
        updated_at_ms: 10,
    };
    coordinator
        .attach_loaded_tracked(rejected.read(), 0, meta.clone())
        .unwrap();
    coordinator
        .attach_loaded_tracked(healthy.read(), 0, meta)
        .unwrap();

    rejected.write().payload.reject = true;
    healthy.write().name = "healthy".to_owned();
    let prepared = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&rejected)?;
            preparation.scan_tracked(&healthy)
        })
        .expect("a document-local encoding failure must not abort preparation");

    let request = prepared
        .request
        .as_ref()
        .expect("the healthy document should still be written");
    assert_eq!(request.writes.len(), 1);
    assert_eq!(request.writes[0].key.collection, "coordinator_test");
    assert!(!prepared.scan_complete);
    let rejected_key = MongoDocumentKey::for_document::<RejectingDocument>(&42).unwrap();
    assert!(
        coordinator
            .document_rejection(&rejected_key)
            .is_some_and(|error| error.contains("intentional test encoding failure"))
    );
    assert_eq!(coordinator.counters().scans, 2);
    assert_eq!(coordinator.counters().failed_documents, 1);
    assert_eq!(coordinator.counters().attempted_documents, 0);

    let generation = request.generation;
    let token = request.writes[0].token;
    coordinator.begin_flush(prepared.commit).unwrap();
    let report = coordinator
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
        .unwrap();
    assert_eq!(report.applied, 1);
    assert!(coordinator.document_rejection(&rejected_key).is_some());

    {
        let payload = &mut rejected.write().payload;
        payload.reject = false;
        payload.value = "recovered".to_owned();
    }
    let recovered = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&rejected)?;
            preparation.scan_tracked(&healthy)
        })
        .expect("a newer mutation epoch should retry the rejected document");
    let recovered_request = recovered
        .request
        .as_ref()
        .expect("retry should be prepared");
    assert_eq!(recovered_request.writes.len(), 1);
    assert_eq!(
        recovered_request.writes[0].key.collection,
        "coordinator_rejecting_test"
    );
    assert!(coordinator.document_rejection(&rejected_key).is_some());

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
                        updated_at_ms: 23,
                    },
                )]),
            },
        )
        .unwrap();
    assert!(coordinator.document_rejection(&rejected_key).is_none());
    assert!(coordinator.last_error().is_none());
}

#[test]
fn untracked_rejection_can_be_forced_to_rescan_without_an_epoch() {
    let original = RejectingDocument {
        id: 42,
        payload: RejectingString {
            value: "initial".to_owned(),
            reject: false,
        },
    };
    let mut value = original.clone();
    let mut coordinator = MongoPersistenceCoordinator::new(7);
    coordinator
        .attach_loaded(
            &original,
            LoadedDocumentMeta {
                version: 3,
                updated_at_ms: 10,
            },
        )
        .unwrap();
    value.payload.reject = true;
    let rejected = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan(&value)
        })
        .unwrap();
    assert!(rejected.request.is_none());

    value.payload.reject = false;
    value.payload.value = "fixed".to_owned();
    let still_blocked = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan(&value)
        })
        .unwrap();
    assert!(still_blocked.request.is_none());
    assert!(!still_blocked.scan_complete);

    coordinator
        .retry_rejected::<RejectingDocument>(&42)
        .unwrap();
    let retried = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan(&value)
        })
        .unwrap();
    assert!(retried.request.is_some());
}

#[test]
fn rejected_create_can_be_explicitly_detached() {
    let mut coordinator = MongoPersistenceCoordinator::new(7);
    let mut value = coordinator
        .track_new(
            RejectingDocument {
                id: 42,
                payload: RejectingString {
                    value: "initial".to_owned(),
                    reject: false,
                },
            },
            CreateMode::InsertOnly,
        )
        .unwrap();
    value.write().payload.reject = true;
    let rejected = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&value)
        })
        .unwrap();
    assert!(rejected.request.is_none());
    assert!(matches!(
        coordinator.detach::<RejectingDocument>(&42),
        Err(PersistenceError::CreatePending(_))
    ));
    coordinator
        .detach_rejected::<RejectingDocument>(&42)
        .unwrap();
    assert!(matches!(
        coordinator.retry_rejected::<RejectingDocument>(&42),
        Err(PersistenceError::UnknownDocument(_))
    ));
}

#[test]
fn rejected_document_can_be_replaced_with_loaded_remote_state() {
    let original = RejectingDocument {
        id: 42,
        payload: RejectingString {
            value: "initial".to_owned(),
            reject: false,
        },
    };
    let mut value = original.clone();
    let mut coordinator = MongoPersistenceCoordinator::new(7);
    coordinator
        .attach_loaded(
            &original,
            LoadedDocumentMeta {
                version: 3,
                updated_at_ms: 10,
            },
        )
        .unwrap();
    value.payload.reject = true;
    coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan(&value)
        })
        .unwrap();

    let replacement = coordinator
        .replace_rejected_with_loaded(LoadedDocument {
            version: 8,
            updated_at_ms: 30,
            value: RejectingDocument {
                id: 42,
                payload: RejectingString {
                    value: "remote".to_owned(),
                    reject: false,
                },
            },
        })
        .unwrap();
    let key = MongoDocumentKey::for_document::<RejectingDocument>(&42).unwrap();
    assert_eq!(replacement.payload.value, "remote");
    assert_eq!(coordinator.document_meta(&key), Some((8, 30)));
    assert!(coordinator.document_rejection(&key).is_none());
}

#[test]
fn definitive_write_rejection_does_not_block_other_documents() {
    let mut rejected = crate::document::tracked::Tracked::clean(RejectingDocument {
        id: 42,
        payload: RejectingString {
            value: "initial".to_owned(),
            reject: false,
        },
    });
    let mut healthy = crate::document::tracked::Tracked::clean(document("old"));
    let mut coordinator = MongoPersistenceCoordinator::new(7);
    let meta = LoadedDocumentMeta {
        version: 3,
        updated_at_ms: 10,
    };
    coordinator
        .attach_loaded_tracked(rejected.read(), 0, meta.clone())
        .unwrap();
    coordinator
        .attach_loaded_tracked(healthy.read(), 0, meta)
        .unwrap();
    rejected.write().payload.value = "too large".to_owned();
    healthy.write().name = "first healthy change".to_owned();

    let prepared = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&rejected)?;
            preparation.scan_tracked(&healthy)
        })
        .unwrap();
    let request = prepared.request.as_ref().unwrap();
    assert_eq!(request.writes.len(), 2);
    let rejected_write = request
        .writes
        .iter()
        .find(|write| write.key.collection == "coordinator_rejecting_test")
        .unwrap();
    let healthy_write = request
        .writes
        .iter()
        .find(|write| write.key.collection == "coordinator_test")
        .unwrap();
    let generation = request.generation;
    let rejected_token = rejected_write.token;
    let healthy_token = healthy_write.token;
    coordinator.begin_flush(prepared.commit).unwrap();
    let report = coordinator
        .complete(
            generation,
            FlushOutcome {
                documents: BTreeMap::from([
                    (
                        rejected_token,
                        DocumentWriteOutcome::Failed {
                            error: MongoStoreError::rejected(
                                "document exceeds MongoDB's maximum BSON size",
                            ),
                        },
                    ),
                    (
                        healthy_token,
                        DocumentWriteOutcome::Applied {
                            previous_version: 3,
                            new_version: 4,
                            updated_at_ms: 22,
                        },
                    ),
                ]),
            },
        )
        .unwrap();
    assert_eq!(report.failed, 1);
    assert_eq!(report.applied, 1);

    healthy.write().name = "second healthy change".to_owned();
    let next = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&rejected)?;
            preparation.scan_tracked(&healthy)
        })
        .expect("an unchanged rejected document must not block later healthy writes");
    let next_request = next.request.as_ref().unwrap();
    assert_eq!(next_request.writes.len(), 1);
    assert_eq!(next_request.writes[0].key.collection, "coordinator_test");
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
fn exact_retry_can_be_converted_to_outcome_unknown_for_manual_recovery() {
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
                    DocumentWriteOutcome::Failed {
                        error: MongoStoreError::new("ambiguous transport failure"),
                    },
                )]),
            },
        )
        .unwrap();
    assert_eq!(coordinator.retry_attempt(), 1);

    let report = coordinator
        .abort_retry_as_unknown("operator stopped exact retries")
        .unwrap();
    assert_eq!(report.conflicts, 1);
    assert_eq!(coordinator.retry_attempt(), 0);
    assert!(coordinator.retry_delay().is_none());
    let conflict = coordinator.conflict().unwrap();
    assert_eq!(conflict.kind, PersistenceConflictKind::OutcomeUnknown);
    assert!(matches!(
        coordinator.prepare(ScanBudget::generous(), |_| Ok(())),
        Err(PersistenceError::ConflictBlocked)
    ));

    let replacement = coordinator
        .resolve_conflict_with_loaded(LoadedDocument {
            version: 4,
            updated_at_ms: 50,
            value: value.clone(),
        })
        .unwrap();
    assert_eq!(replacement.name, "new");
    assert!(coordinator.conflict().is_none());
    assert!(coordinator.last_error().is_none());
}

#[test]
fn abandoned_in_flight_generation_ignores_its_late_completion() {
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

    let report = coordinator
        .abort_in_flight_as_unknown(generation, "operator abandoned a hung write")
        .unwrap();
    assert_eq!(report.conflicts, 1);
    assert!(!coordinator.has_in_flight());
    assert_eq!(
        coordinator.conflict().unwrap().kind,
        PersistenceConflictKind::OutcomeUnknown
    );

    let status = coordinator
        .apply_completion(MongoFlushCompleted {
            generation,
            outcome: Ok(FlushOutcome {
                documents: BTreeMap::from([(
                    token,
                    DocumentWriteOutcome::Applied {
                        previous_version: 3,
                        new_version: 4,
                        updated_at_ms: 60,
                    },
                )]),
            }),
        })
        .unwrap();
    assert!(matches!(status, CompletionStatus::IgnoredAbandoned));
    assert_eq!(
        coordinator.conflict().unwrap().kind,
        PersistenceConflictKind::OutcomeUnknown
    );
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
    let conflict = coordinator.conflict().unwrap();
    assert_eq!(conflict.expected_version, 3);
    assert_eq!(conflict.kind, PersistenceConflictKind::VersionConflict);
    assert_eq!(conflict.policy, ConflictPolicy::BlockCoordinator);
    assert!(matches!(
        coordinator.prepare(ScanBudget::generous(), |_| Ok(())),
        Err(PersistenceError::ConflictBlocked)
    ));
}

#[test]
fn quarantined_conflicts_preserve_all_documents_and_allow_healthy_progress() {
    let mut version_conflict = crate::document::tracked::Tracked::clean(RejectingDocument {
        id: 42,
        payload: RejectingString {
            value: "old version".to_owned(),
            reject: false,
        },
    });
    let mut missing = crate::document::tracked::Tracked::clean(RejectingDocument {
        id: 43,
        payload: RejectingString {
            value: "present".to_owned(),
            reject: false,
        },
    });
    let mut healthy = crate::document::tracked::Tracked::clean(document("old"));
    let mut coordinator = MongoPersistenceCoordinator::new(7);
    let meta = LoadedDocumentMeta {
        version: 3,
        updated_at_ms: 10,
    };
    coordinator
        .attach_loaded_tracked(version_conflict.read(), 0, meta.clone())
        .unwrap();
    coordinator
        .attach_loaded_tracked(missing.read(), 0, meta.clone())
        .unwrap();
    coordinator
        .attach_loaded_tracked(healthy.read(), 0, meta)
        .unwrap();
    version_conflict.write().payload.value = "local version".to_owned();
    missing.write().payload.value = "locally retained".to_owned();
    healthy.write().name = "first healthy change".to_owned();

    let prepared = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&version_conflict)?;
            preparation.scan_tracked(&missing)?;
            preparation.scan_tracked(&healthy)
        })
        .unwrap();
    let request = prepared.request.as_ref().unwrap();
    assert_eq!(request.writes.len(), 3);
    let generation = request.generation;
    let version_token = request.writes[0].token;
    let missing_token = request.writes[1].token;
    let healthy_token = request.writes[2].token;
    coordinator.begin_flush(prepared.commit).unwrap();
    let report = coordinator
        .complete(
            generation,
            FlushOutcome {
                documents: BTreeMap::from([
                    (
                        version_token,
                        DocumentWriteOutcome::VersionConflict {
                            expected_version: 3,
                        },
                    ),
                    (
                        missing_token,
                        DocumentWriteOutcome::NotFound {
                            expected_version: 3,
                        },
                    ),
                    (
                        healthy_token,
                        DocumentWriteOutcome::Applied {
                            previous_version: 3,
                            new_version: 4,
                            updated_at_ms: 22,
                        },
                    ),
                ]),
            },
        )
        .unwrap();
    assert_eq!(report.conflicts, 2);
    assert_eq!(report.applied, 1);
    assert_eq!(coordinator.conflicts().count(), 2);

    let version_key = MongoDocumentKey::for_document::<RejectingDocument>(&42).unwrap();
    let missing_key = MongoDocumentKey::for_document::<RejectingDocument>(&43).unwrap();
    assert_eq!(
        coordinator.document_conflict(&version_key).unwrap().kind,
        PersistenceConflictKind::VersionConflict
    );
    assert_eq!(
        coordinator.document_conflict(&missing_key).unwrap().kind,
        PersistenceConflictKind::NotFound
    );
    assert!(
        coordinator
            .conflicts()
            .all(|conflict| conflict.policy == ConflictPolicy::QuarantineDocument)
    );

    healthy.write().name = "second healthy change".to_owned();
    let next = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&version_conflict)?;
            preparation.scan_tracked(&missing)?;
            preparation.scan_tracked(&healthy)
        })
        .expect("quarantined documents must not block healthy writes");
    assert!(!next.scan_complete);
    let next_request = next.request.as_ref().unwrap();
    assert_eq!(next_request.writes.len(), 1);
    assert_eq!(next_request.writes[0].key.collection, "coordinator_test");
    let generation = next_request.generation;
    let token = next_request.writes[0].token;
    coordinator.begin_flush(next.commit).unwrap();
    coordinator
        .complete(
            generation,
            FlushOutcome {
                documents: BTreeMap::from([(
                    token,
                    DocumentWriteOutcome::Applied {
                        previous_version: 4,
                        new_version: 5,
                        updated_at_ms: 23,
                    },
                )]),
            },
        )
        .unwrap();

    assert!(matches!(
        coordinator.detach::<RejectingDocument>(&43),
        Err(PersistenceError::DocumentConflictPending(_))
    ));
    version_conflict = coordinator
        .resolve_conflict_with_loaded(LoadedDocument {
            version: 8,
            updated_at_ms: 30,
            value: RejectingDocument {
                id: 42,
                payload: RejectingString {
                    value: "remote version".to_owned(),
                    reject: false,
                },
            },
        })
        .unwrap();
    assert_eq!(version_conflict.payload.value, "remote version");
    coordinator
        .detach_conflicted::<RejectingDocument>(&43)
        .unwrap();
    assert!(coordinator.conflicts().next().is_none());
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
