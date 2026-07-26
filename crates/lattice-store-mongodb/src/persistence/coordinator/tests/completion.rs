use super::*;

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
