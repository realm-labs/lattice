use super::*;

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
