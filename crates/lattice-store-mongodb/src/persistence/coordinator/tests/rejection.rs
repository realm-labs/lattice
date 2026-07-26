use super::*;

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
