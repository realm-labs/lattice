use mongodb::bson::{Bson, doc};

use super::{FieldChange, ScanBudget, ScanBuilder, ScanCursor, ScanError, ScanSnapshot};

fn baseline() -> ScanSnapshot {
    ScanSnapshot::empty()
        .capture_whole("profile", &Bson::Document(doc! { "name": "old" }))
        .expect("profile baseline should capture")
        .capture_map("items", &doc! { "1": { "count": 1 }, "3": { "count": 3 } })
        .expect("item baseline should capture")
}

#[test]
fn unchanged_scan_is_clean_and_preparation_does_not_mutate_baseline() {
    let baseline = baseline();
    let before = baseline.clone();
    let mut budget = ScanBudget::generous();
    let mut scan = ScanBuilder::new(&baseline, ScanCursor::default(), &mut budget);
    scan.whole(0, "profile", Bson::Document(doc! { "name": "old" }))
        .expect("profile scan should encode");
    scan.map(
        1,
        "items",
        doc! { "3": { "count": 3 }, "1": { "count": 1 } },
    )
    .expect("item scan should encode");
    let delta = scan.finish();

    assert!(delta.complete);
    assert!(delta.changes.is_empty());
    assert_eq!(baseline, before);
}

#[test]
fn whole_and_map_changes_are_deterministic() {
    let baseline = baseline();
    let mut budget = ScanBudget::generous();
    let mut scan = ScanBuilder::new(&baseline, ScanCursor::default(), &mut budget);
    scan.whole(0, "profile", Bson::Document(doc! { "name": "new" }))
        .expect("changed profile should encode");
    scan.map(
        1,
        "items",
        doc! { "1": { "count": 2 }, "2": { "count": 1 } },
    )
    .expect("changed items should encode");
    let paths = scan
        .finish()
        .changes
        .into_iter()
        .map(|change| match change {
            FieldChange::Set { path, .. } => format!("set:{}", path.0),
            FieldChange::Unset { path } => format!("unset:{}", path.0),
        })
        .collect::<Vec<_>>();

    assert_eq!(
        paths,
        ["set:profile", "set:items.1", "set:items.2", "unset:items.3"]
    );
}

#[test]
fn field_budget_defers_whole_map_without_splitting_its_entry_diff() {
    let mut baseline = baseline();
    let current = doc! { "1": { "count": 2 }, "2": { "count": 1 }, "3": { "count": 4 } };
    let mut budget = ScanBudget::new(1, 1, Duration::from_secs(1));
    let mut scan = ScanBuilder::new(&baseline, ScanCursor::default(), &mut budget);
    scan.whole(0, "profile", Bson::Document(doc! { "name": "old" }))
        .expect("profile scan should encode");
    scan.map(1, "items", current.clone())
        .expect("deferred item scan should not encode");
    let first = scan.finish();
    assert!(!first.complete);
    assert!(first.changes.is_empty());
    let cursor = first.next_cursor.clone();
    assert_eq!(
        baseline
            .apply(first.commit)
            .expect("partial commit should apply"),
        cursor
    );

    let mut budget = ScanBudget::generous();
    let mut scan = ScanBuilder::new(&baseline, cursor, &mut budget);
    scan.map(1, "items", current)
        .expect("resumed item scan should encode");
    let second = scan.finish();
    assert!(second.complete);
    assert_eq!(second.changes.len(), 3);
}

#[test]
fn foreign_and_duplicate_commits_are_rejected() {
    let mut baseline = baseline();
    let foreign = self::baseline();
    let mut foreign_budget = ScanBudget::generous();
    let foreign_commit = ScanBuilder::new(&foreign, ScanCursor::default(), &mut foreign_budget)
        .finish()
        .commit;
    assert!(matches!(
        baseline.apply(foreign_commit),
        Err(ScanError::ForeignCommit { .. })
    ));

    let mut budget = ScanBudget::generous();
    let scan = ScanBuilder::new(&baseline, ScanCursor::default(), &mut budget).finish();
    baseline
        .apply(scan.commit)
        .expect("matching commit should apply");

    let mut budget = ScanBudget::generous();
    let old_revision_commit = ScanBuilder::new(
        &ScanSnapshot {
            identity: baseline.identity,
            revision: 0,
            fields: baseline.fields.clone(),
            field_groups: baseline.field_groups.clone(),
        },
        ScanCursor::default(),
        &mut budget,
    )
    .finish()
    .commit;
    assert!(matches!(
        baseline.apply(old_revision_commit),
        Err(ScanError::ForeignCommit { .. })
    ));
}

#[test]
fn canonical_document_hash_ignores_document_insertion_order() {
    let left = ScanSnapshot::empty()
        .capture_whole("value", &Bson::Document(doc! { "a": 1, "b": 2 }))
        .expect("left document should hash");
    let right = ScanSnapshot::empty()
        .capture_whole("value", &Bson::Document(doc! { "b": 2, "a": 1 }))
        .expect("right document should hash");
    assert_eq!(left.fields, right.fields);
}

#[test]
fn document_budget_defers_scan_without_creating_changes() {
    let baseline = baseline();
    let mut budget = ScanBudget::new(0, usize::MAX, Duration::from_secs(1));
    let mut scan = ScanBuilder::new(&baseline, ScanCursor::default(), &mut budget);
    scan.whole(0, "profile", Bson::Document(doc! { "name": "new" }))
        .expect("profile scan should encode");
    let delta = scan.finish();
    assert!(!delta.complete);
    assert!(delta.changes.is_empty());
    assert_eq!(delta.next_cursor, ScanCursor::default());
}

use std::time::Duration;
