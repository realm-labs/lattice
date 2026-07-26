use super::*;

#[test]
fn absent_default_stays_write_free_until_bson_really_changes() {
    let mut coordinator = MongoPersistenceCoordinator::new(7);
    let mut value = coordinator
        .track_absent(document("default"))
        .expect("absent default should attach");

    let untouched = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&value)
        })
        .expect("untouched default should prepare");
    assert!(untouched.request.is_none());
    coordinator.complete_clean(untouched.commit).unwrap();

    let _ = value.write();
    let false_positive = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&value)
        })
        .expect("mutable access without a change should prepare");
    assert!(false_positive.request.is_none());
    coordinator.complete_clean(false_positive.commit).unwrap();

    value.write().name = "changed".to_owned();
    value.write().name = "default".to_owned();
    let restored = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&value)
        })
        .expect("restored default should prepare");
    assert!(restored.request.is_none());
    coordinator.complete_clean(restored.commit).unwrap();

    assert_eq!(coordinator.pending_document_count(), 0);
    assert!(coordinator.tracked_is_clean(&value).unwrap());
    assert!(
        coordinator
            .detach_tracked_if_clean(&value)
            .expect("clean absent default may detach")
    );
}

#[test]
fn absent_default_preserves_a_bounded_cursor_until_change_is_found() {
    let mut coordinator = MongoPersistenceCoordinator::new(7);
    let mut value = coordinator.track_absent(document("default")).unwrap();
    value.write().items.insert("two".to_owned(), 2);

    let partial = coordinator
        .prepare(
            ScanBudget::new(1, 1, Duration::from_secs(1)),
            |preparation| preparation.scan_tracked(&value),
        )
        .unwrap();
    assert!(partial.request.is_none());
    assert!(!partial.scan_complete);
    coordinator.complete_clean(partial.commit).unwrap();

    let changed = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&value)
        })
        .unwrap();
    assert!(changed.scan_complete);
    let request = changed.request.as_ref().expect("map change should create");
    assert_eq!(request.writes.len(), 1);
    let DocumentOperation::Create { document, mode } = &request.writes[0].operation else {
        panic!("first real absent change should create");
    };
    assert_eq!(*mode, CreateMode::InsertOnly);
    assert_eq!(document.get_str("name"), Ok("default"));
    assert!(document.get_document("items").unwrap().contains_key("two"));
}

#[test]
fn acknowledged_absent_create_becomes_an_incrementally_updated_document() {
    let mut coordinator = MongoPersistenceCoordinator::new(7);
    let mut value = coordinator.track_absent(document("default")).unwrap();
    value.write().name = "created".to_owned();

    let created = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&value)
        })
        .unwrap();
    let request = created.request.as_ref().unwrap();
    assert_eq!(request.writes.len(), 1);
    assert!(matches!(
        request.writes[0].operation,
        DocumentOperation::Create {
            mode: CreateMode::InsertOnly,
            ..
        }
    ));
    let generation = request.generation;
    let token = request.writes[0].token;
    coordinator.begin_flush(created.commit).unwrap();
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

    let clean = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&value)
        })
        .unwrap();
    assert!(
        clean.request.is_none(),
        "Create installed a complete baseline"
    );

    value.write().name = "updated".to_owned();
    let updated = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&value)
        })
        .unwrap();
    let operation = &updated.request.unwrap().writes[0].operation;
    assert!(matches!(operation, DocumentOperation::Update { .. }));
}

#[test]
fn absent_create_conflict_retains_presence_and_baseline() {
    let mut coordinator = MongoPersistenceCoordinator::new(7);
    let mut value = coordinator.track_absent(document("default")).unwrap();
    value.write().name = "local".to_owned();
    let key = MongoDocumentKey::for_document::<TestDocument>(&42).unwrap();
    let baseline = coordinator.documents[&key].baseline.clone();

    let prepared = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan_tracked(&value)
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
                        expected_version: 0,
                    },
                )]),
            },
        )
        .unwrap();

    let state = &coordinator.documents[&key];
    assert_eq!(state.baseline, baseline);
    assert!(matches!(
        state.presence,
        DocumentPresence::Absent {
            mode: CreateMode::InsertOnly
        }
    ));
    assert_eq!(
        coordinator.conflict().unwrap().kind,
        PersistenceConflictKind::VersionConflict
    );
}
