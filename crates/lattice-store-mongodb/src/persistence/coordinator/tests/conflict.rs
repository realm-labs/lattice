use super::*;

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
