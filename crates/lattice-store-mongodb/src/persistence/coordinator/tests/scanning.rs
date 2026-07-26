use super::*;

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
