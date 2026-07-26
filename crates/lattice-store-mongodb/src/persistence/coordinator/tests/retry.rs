use super::*;

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
            jitter_percent: 0,
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
fn document_cannot_detach_while_an_exact_retry_still_replays_it() {
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
                        error: MongoStoreError::new("replica set failover"),
                    },
                )]),
            },
        )
        .unwrap();
    assert!(matches!(
        coordinator.detach::<TestDocument>(&42),
        Err(PersistenceError::DocumentRetryPending(_))
    ));

    let retry = coordinator
        .prepare(ScanBudget::generous(), |preparation| {
            preparation.scan(&value)
        })
        .unwrap();
    let retry_request = retry.request.as_ref().unwrap();
    let retry_generation = retry_request.generation;
    let retry_token = retry_request.writes[0].token;
    coordinator.begin_flush(retry.commit).unwrap();
    coordinator
        .complete(
            retry_generation,
            FlushOutcome {
                documents: BTreeMap::from([(
                    retry_token,
                    DocumentWriteOutcome::Applied {
                        previous_version: 3,
                        new_version: 4,
                        updated_at_ms: 60,
                    },
                )]),
            },
        )
        .unwrap();
    coordinator.detach::<TestDocument>(&42).unwrap();
}

#[test]
fn retry_backoff_spreads_inside_its_exponential_step() {
    let policy = RetryPolicy::default();
    let deterministic = RetryPolicy {
        jitter_percent: 0,
        ..policy
    };
    for attempt in 1..=8 {
        let step = deterministic.delay(attempt, u64::MAX);
        assert_eq!(policy.delay(attempt, 0), step);
        for entropy in [1, u64::MAX / 3, u64::MAX] {
            let delay = policy.delay(attempt, entropy);
            assert!(delay <= step, "attempt {attempt} must not exceed its step");
            assert!(
                delay >= step / 2,
                "attempt {attempt} must keep half of its step"
            );
        }
    }
    assert_eq!(deterministic.delay(9, 0), policy.max_delay);
    assert_ne!(policy.delay(3, 0), policy.delay(3, u64::MAX));
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
