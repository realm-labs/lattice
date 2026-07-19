//! Pure, actor-local persistence scan primitives.
//!
//! A scan compares current BSON values with an acknowledged baseline. Preparing
//! a delta never mutates that baseline. Only the matching, consuming
//! [`ScanCommit`] can advance it after storage acknowledgement.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mongodb::bson::{Bson, Document};
use serde::Serialize;

use crate::document::MongoDocument;
use crate::mongo::MongoFieldPath;

pub trait MongoMapKey {
    fn mongo_map_key(&self) -> String;
}

macro_rules! integer_map_keys {
    ($($ty:ty),* $(,)?) => {
        $(impl MongoMapKey for $ty {
            fn mongo_map_key(&self) -> String { self.to_string() }
        })*
    };
}

integer_map_keys!(i8, i16, i32, i64, u8, u16, u32, u64);

impl MongoMapKey for String {
    fn mongo_map_key(&self) -> String {
        self.clone()
    }
}

impl MongoMapKey for str {
    fn mongo_map_key(&self) -> String {
        self.to_owned()
    }
}

pub trait MongoScan: MongoDocument {
    fn capture(&self) -> Result<ScanSnapshot, ScanError>;

    fn diff(
        &self,
        baseline: &ScanSnapshot,
        cursor: ScanCursor,
        budget: &mut ScanBudget,
    ) -> Result<ScanDelta, ScanError>;
}

#[derive(Debug, Clone, PartialEq)]
pub enum FieldChange {
    Set { path: MongoFieldPath, value: Bson },
    Unset { path: MongoFieldPath },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FieldSnapshot {
    Whole(StableHash),
    Map(BTreeMap<String, StableHash>),
}

/// The last state acknowledged by storage for one document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanSnapshot {
    identity: u64,
    revision: u64,
    fields: BTreeMap<String, FieldSnapshot>,
}

impl ScanSnapshot {
    pub fn empty() -> Self {
        static NEXT_IDENTITY: AtomicU64 = AtomicU64::new(1);
        Self {
            identity: NEXT_IDENTITY.fetch_add(1, Ordering::Relaxed),
            revision: 0,
            fields: BTreeMap::new(),
        }
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn capture_whole(
        mut self,
        field: impl Into<String>,
        value: &Bson,
    ) -> Result<Self, ScanError> {
        self.fields
            .insert(field.into(), FieldSnapshot::Whole(stable_hash(value)?));
        Ok(self)
    }

    pub fn capture_value<T>(self, field: impl Into<String>, value: &T) -> Result<Self, ScanError>
    where
        T: Serialize,
    {
        self.capture_whole(field, &encode_value(value)?)
    }

    pub fn capture_map(
        mut self,
        field: impl Into<String>,
        value: &Document,
    ) -> Result<Self, ScanError> {
        self.fields.insert(
            field.into(),
            FieldSnapshot::Map(hash_document_entries(value)?),
        );
        Ok(self)
    }

    pub fn capture_map_value<T>(
        self,
        field: impl Into<String>,
        value: &T,
    ) -> Result<Self, ScanError>
    where
        T: Serialize,
    {
        let bson = encode_value(value)?;
        let Bson::Document(document) = bson else {
            return Err(ScanError::ExpectedMapDocument);
        };
        self.capture_map(field, &document)
    }

    pub fn capture_map_entries<'a, K, V>(
        mut self,
        field: impl Into<String>,
        entries: impl IntoIterator<Item = (&'a K, &'a V)>,
    ) -> Result<Self, ScanError>
    where
        K: MongoMapKey + 'a,
        V: Serialize + 'a,
    {
        let encoded = encode_map_entries(entries)?;
        self.fields.insert(
            field.into(),
            FieldSnapshot::Map(hash_encoded_map(&encoded)?),
        );
        Ok(self)
    }

    pub fn apply(&mut self, commit: ScanCommit) -> Result<ScanCursor, ScanError> {
        if commit.baseline_identity != self.identity || commit.baseline_revision != self.revision {
            return Err(ScanError::ForeignCommit {
                expected_identity: self.identity,
                actual_identity: commit.baseline_identity,
                expected_revision: self.revision,
                actual_revision: commit.baseline_revision,
            });
        }
        for (field, snapshot) in commit.fields {
            self.fields.insert(field, snapshot);
        }
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or(ScanError::RevisionOverflow)?;
        Ok(commit.next_cursor)
    }
}

impl Default for ScanSnapshot {
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ScanCursor {
    pub field_index: usize,
    pub map_key: Option<String>,
}

#[derive(Debug)]
pub struct ScanBudget {
    remaining_documents: usize,
    remaining_fields: usize,
    remaining_map_entries: usize,
    deadline: Instant,
}

impl ScanBudget {
    pub fn new(
        max_documents: usize,
        max_fields: usize,
        max_map_entries: usize,
        max_duration: Duration,
    ) -> Self {
        Self {
            remaining_documents: max_documents,
            remaining_fields: max_fields,
            remaining_map_entries: max_map_entries,
            deadline: Instant::now() + max_duration,
        }
    }

    pub fn generous() -> Self {
        Self::new(usize::MAX, usize::MAX, usize::MAX, Duration::from_secs(60))
    }

    pub fn begin_document(&mut self) -> bool {
        consume(&mut self.remaining_documents) && self.has_time()
    }

    fn field(&mut self) -> bool {
        consume(&mut self.remaining_fields) && self.has_time()
    }

    fn map_entry(&mut self) -> bool {
        consume(&mut self.remaining_map_entries) && self.has_time()
    }

    fn has_time(&self) -> bool {
        Instant::now() <= self.deadline
    }
}

fn consume(value: &mut usize) -> bool {
    if *value == 0 {
        false
    } else {
        *value -= 1;
        true
    }
}

/// Opaque baseline replacement. It is consumed when applied and is valid only
/// for the exact baseline revision used during preparation.
#[derive(Debug)]
pub struct ScanCommit {
    baseline_identity: u64,
    baseline_revision: u64,
    fields: BTreeMap<String, FieldSnapshot>,
    next_cursor: ScanCursor,
}

#[derive(Debug)]
pub struct ScanDelta {
    pub changes: Vec<FieldChange>,
    pub commit: ScanCommit,
    pub next_cursor: ScanCursor,
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanError {
    MissingBaselineField(String),
    BaselineKindMismatch(String),
    InvalidMapKey(String),
    Encoding(String),
    ExpectedMapDocument,
    ForeignCommit {
        expected_identity: u64,
        actual_identity: u64,
        expected_revision: u64,
        actual_revision: u64,
    },
    RevisionOverflow,
}

impl std::fmt::Display for ScanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingBaselineField(field) => write!(f, "missing scan baseline field: {field}"),
            Self::BaselineKindMismatch(field) => {
                write!(f, "scan baseline kind mismatch for field: {field}")
            }
            Self::InvalidMapKey(key) => write!(f, "invalid MongoDB map key: {key}"),
            Self::Encoding(error) => write!(f, "failed to encode BSON scan value: {error}"),
            Self::ExpectedMapDocument => {
                f.write_str("map scan value did not encode as a BSON document")
            }
            Self::ForeignCommit {
                expected_identity,
                actual_identity,
                expected_revision,
                actual_revision,
            } => write!(
                f,
                "scan commit targets baseline {actual_identity} revision {actual_revision}, current baseline is {expected_identity} revision {expected_revision}"
            ),
            Self::RevisionOverflow => f.write_str("scan baseline revision overflow"),
        }
    }
}

impl std::error::Error for ScanError {}

/// Builder used by generated `MongoScan` implementations.
pub struct ScanBuilder<'a> {
    baseline: &'a ScanSnapshot,
    cursor: ScanCursor,
    budget: &'a mut ScanBudget,
    changes: Vec<FieldChange>,
    fields: BTreeMap<String, FieldSnapshot>,
    next_cursor: ScanCursor,
    complete: bool,
    active: bool,
}

impl<'a> ScanBuilder<'a> {
    pub fn new(baseline: &'a ScanSnapshot, cursor: ScanCursor, budget: &'a mut ScanBudget) -> Self {
        let active = budget.begin_document();
        Self {
            baseline,
            next_cursor: cursor.clone(),
            cursor,
            budget,
            changes: Vec::new(),
            fields: BTreeMap::new(),
            complete: active,
            active,
        }
    }

    pub fn whole(
        &mut self,
        field_index: usize,
        field: &'static str,
        value: Bson,
    ) -> Result<(), ScanError> {
        if !self.active {
            return Ok(());
        }
        if field_index < self.cursor.field_index {
            return Ok(());
        }
        if !self.budget.field() {
            self.pause(field_index, None);
            return Ok(());
        }
        let hash = stable_hash(&value)?;
        match self.baseline.fields.get(field) {
            Some(FieldSnapshot::Whole(old)) => {
                if old != &hash {
                    self.changes.push(FieldChange::Set {
                        path: MongoFieldPath::new(field),
                        value,
                    });
                }
            }
            Some(FieldSnapshot::Map(_)) => {
                return Err(ScanError::BaselineKindMismatch(field.to_owned()));
            }
            None => return Err(ScanError::MissingBaselineField(field.to_owned())),
        }
        self.fields
            .insert(field.to_owned(), FieldSnapshot::Whole(hash));
        self.next_cursor = ScanCursor {
            field_index: field_index + 1,
            map_key: None,
        };
        Ok(())
    }

    pub fn whole_value<T>(
        &mut self,
        field_index: usize,
        field: &'static str,
        value: &T,
    ) -> Result<(), ScanError>
    where
        T: Serialize,
    {
        self.whole(field_index, field, encode_value(value)?)
    }

    pub fn map(
        &mut self,
        field_index: usize,
        field: &'static str,
        value: Document,
    ) -> Result<(), ScanError> {
        if !self.active {
            return Ok(());
        }
        if field_index < self.cursor.field_index {
            return Ok(());
        }
        if !self.budget.field() {
            self.pause(field_index, self.cursor.map_key.clone());
            return Ok(());
        }
        let Some(FieldSnapshot::Map(old)) = self.baseline.fields.get(field) else {
            return match self.baseline.fields.get(field) {
                Some(_) => Err(ScanError::BaselineKindMismatch(field.to_owned())),
                None => Err(ScanError::MissingBaselineField(field.to_owned())),
            };
        };
        let current = hash_document_entries(&value)?;
        let keys = old
            .keys()
            .chain(current.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        let start_after = if field_index == self.cursor.field_index {
            self.cursor.map_key.as_deref()
        } else {
            None
        };
        let mut processed = old.clone();
        for key in keys {
            if start_after.is_some_and(|cursor| key.as_str() <= cursor) {
                continue;
            }
            if !self.budget.map_entry() {
                self.fields
                    .insert(field.to_owned(), FieldSnapshot::Map(processed));
                self.pause(field_index, self.next_cursor.map_key.clone());
                return Ok(());
            }
            validate_map_key(&key)?;
            match (old.get(&key), current.get(&key)) {
                (Some(previous), Some(now)) if previous == now => {}
                (_, Some(now)) => {
                    self.changes.push(FieldChange::Set {
                        path: MongoFieldPath::new(field).child(&key),
                        value: value.get(&key).cloned().expect("hashed key exists"),
                    });
                    processed.insert(key.clone(), *now);
                }
                (Some(_), None) => {
                    self.changes.push(FieldChange::Unset {
                        path: MongoFieldPath::new(field).child(&key),
                    });
                    processed.remove(&key);
                }
                (None, None) => unreachable!(),
            }
            self.next_cursor = ScanCursor {
                field_index,
                map_key: Some(key),
            };
        }
        self.fields
            .insert(field.to_owned(), FieldSnapshot::Map(current));
        self.next_cursor = ScanCursor {
            field_index: field_index + 1,
            map_key: None,
        };
        Ok(())
    }

    pub fn map_value<T>(
        &mut self,
        field_index: usize,
        field: &'static str,
        value: &T,
    ) -> Result<(), ScanError>
    where
        T: Serialize,
    {
        let bson = encode_value(value)?;
        let Bson::Document(document) = bson else {
            return Err(ScanError::ExpectedMapDocument);
        };
        self.map(field_index, field, document)
    }

    pub fn map_entries<'entry, K, V>(
        &mut self,
        field_index: usize,
        field: &'static str,
        entries: impl IntoIterator<Item = (&'entry K, &'entry V)>,
    ) -> Result<(), ScanError>
    where
        K: MongoMapKey + 'entry,
        V: Serialize + 'entry,
    {
        self.map_encoded(field_index, field, encode_map_entries(entries)?)
    }

    fn map_encoded(
        &mut self,
        field_index: usize,
        field: &'static str,
        value: BTreeMap<String, Bson>,
    ) -> Result<(), ScanError> {
        if !self.active {
            return Ok(());
        }
        if field_index < self.cursor.field_index {
            return Ok(());
        }
        if !self.budget.field() {
            self.pause(field_index, self.cursor.map_key.clone());
            return Ok(());
        }
        let Some(FieldSnapshot::Map(old)) = self.baseline.fields.get(field) else {
            return match self.baseline.fields.get(field) {
                Some(_) => Err(ScanError::BaselineKindMismatch(field.to_owned())),
                None => Err(ScanError::MissingBaselineField(field.to_owned())),
            };
        };
        let current = hash_encoded_map(&value)?;
        let keys = old
            .keys()
            .chain(current.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        let start_after = if field_index == self.cursor.field_index {
            self.cursor.map_key.as_deref()
        } else {
            None
        };
        let mut processed = old.clone();
        for key in keys {
            if start_after.is_some_and(|cursor| key.as_str() <= cursor) {
                continue;
            }
            if !self.budget.map_entry() {
                self.fields
                    .insert(field.to_owned(), FieldSnapshot::Map(processed));
                self.pause(field_index, self.next_cursor.map_key.clone());
                return Ok(());
            }
            validate_map_key(&key)?;
            match (old.get(&key), current.get(&key)) {
                (Some(previous), Some(now)) if previous == now => {}
                (_, Some(now)) => {
                    self.changes.push(FieldChange::Set {
                        path: MongoFieldPath::new(field).child(&key),
                        value: value.get(&key).cloned().expect("hashed key exists"),
                    });
                    processed.insert(key.clone(), *now);
                }
                (Some(_), None) => {
                    self.changes.push(FieldChange::Unset {
                        path: MongoFieldPath::new(field).child(&key),
                    });
                    processed.remove(&key);
                }
                (None, None) => unreachable!(),
            }
            self.next_cursor = ScanCursor {
                field_index,
                map_key: Some(key),
            };
        }
        self.fields
            .insert(field.to_owned(), FieldSnapshot::Map(current));
        self.next_cursor = ScanCursor {
            field_index: field_index + 1,
            map_key: None,
        };
        Ok(())
    }

    pub fn finish(mut self) -> ScanDelta {
        if self.complete {
            self.next_cursor = ScanCursor::default();
        }
        ScanDelta {
            changes: self.changes,
            commit: ScanCommit {
                baseline_identity: self.baseline.identity,
                baseline_revision: self.baseline.revision,
                fields: self.fields,
                next_cursor: self.next_cursor.clone(),
            },
            next_cursor: self.next_cursor,
            complete: self.complete,
        }
    }

    fn pause(&mut self, field_index: usize, map_key: Option<String>) {
        self.complete = false;
        self.next_cursor = ScanCursor {
            field_index,
            map_key,
        };
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct StableHash(u128);

fn encode_value<T>(value: &T) -> Result<Bson, ScanError>
where
    T: Serialize,
{
    mongodb::bson::to_bson(value).map_err(|error| ScanError::Encoding(error.to_string()))
}

fn encode_map_entries<'a, K, V>(
    entries: impl IntoIterator<Item = (&'a K, &'a V)>,
) -> Result<BTreeMap<String, Bson>, ScanError>
where
    K: MongoMapKey + 'a,
    V: Serialize + 'a,
{
    entries
        .into_iter()
        .map(|(key, value)| {
            let key = key.mongo_map_key();
            validate_map_key(&key)?;
            Ok((key, encode_value(value)?))
        })
        .collect()
}

fn stable_hash(value: &Bson) -> Result<StableHash, ScanError> {
    let mut hasher = StableHasher::new();
    hash_bson(value, &mut hasher)?;
    Ok(StableHash(hasher.finish()))
}

fn hash_document_entries(value: &Document) -> Result<BTreeMap<String, StableHash>, ScanError> {
    value
        .iter()
        .map(|(key, value)| {
            validate_map_key(key)?;
            Ok((key.clone(), stable_hash(value)?))
        })
        .collect()
}

fn hash_encoded_map(
    value: &BTreeMap<String, Bson>,
) -> Result<BTreeMap<String, StableHash>, ScanError> {
    value
        .iter()
        .map(|(key, value)| {
            validate_map_key(key)?;
            Ok((key.clone(), stable_hash(value)?))
        })
        .collect()
}

fn validate_map_key(key: &str) -> Result<(), ScanError> {
    if key.is_empty() || key.starts_with('$') || key.contains('.') {
        Err(ScanError::InvalidMapKey(key.to_owned()))
    } else {
        Ok(())
    }
}

fn hash_bson(value: &Bson, hasher: &mut StableHasher) -> Result<(), ScanError> {
    match value {
        Bson::Double(value) => tagged(hasher, 1, &value.to_bits().to_le_bytes()),
        Bson::String(value) => tagged(hasher, 2, value.as_bytes()),
        Bson::Array(values) => {
            tagged(hasher, 3, &(values.len() as u64).to_le_bytes());
            for value in values {
                hash_bson(value, hasher)?;
            }
        }
        Bson::Document(value) => {
            tagged(hasher, 4, &(value.len() as u64).to_le_bytes());
            let mut entries = value.iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            for (key, value) in entries {
                tagged(hasher, 5, key.as_bytes());
                hash_bson(value, hasher)?;
            }
        }
        Bson::Boolean(value) => tagged(hasher, 8, &[*value as u8]),
        Bson::Null => tagged(hasher, 10, &[]),
        Bson::Int32(value) => tagged(hasher, 16, &value.to_le_bytes()),
        Bson::Int64(value) => tagged(hasher, 18, &value.to_le_bytes()),
        other => {
            // The driver's canonical raw BSON encoding covers less common
            // scalar variants while preserving their BSON type identity.
            let wrapper = Document::from_iter([("v".to_owned(), other.clone())]);
            let bytes = mongodb::bson::to_vec(&wrapper)
                .map_err(|error| ScanError::Encoding(error.to_string()))?;
            tagged(hasher, 255, &bytes);
        }
    }
    Ok(())
}

fn tagged(hasher: &mut StableHasher, tag: u8, bytes: &[u8]) {
    hasher.write(&[tag]);
    hasher.write(&(bytes.len() as u64).to_le_bytes());
    hasher.write(bytes);
}

struct StableHasher(u128);

impl StableHasher {
    fn new() -> Self {
        Self(0x6c62_272e_07bb_0142_62b8_2175_6295_c58d)
    }

    fn write(&mut self, bytes: &[u8]) {
        const PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;
        for byte in bytes {
            self.0 ^= u128::from(*byte);
            self.0 = self.0.wrapping_mul(PRIME);
        }
    }

    fn finish(self) -> u128 {
        self.0
    }
}

#[cfg(test)]
mod tests {
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
    fn partial_map_scan_resumes_without_skips_or_duplicates() {
        let mut baseline = baseline();
        let current = doc! { "1": { "count": 2 }, "2": { "count": 1 }, "3": { "count": 4 } };
        let mut budget = ScanBudget::new(1, 2, 1, Duration::from_secs(1));
        let mut scan = ScanBuilder::new(&baseline, ScanCursor::default(), &mut budget);
        scan.whole(0, "profile", Bson::Document(doc! { "name": "old" }))
            .expect("profile scan should encode");
        scan.map(1, "items", current.clone())
            .expect("partial item scan should encode");
        let first = scan.finish();
        assert!(!first.complete);
        assert_eq!(first.changes.len(), 1);
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
        assert_eq!(second.changes.len(), 2);
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
        let mut budget = ScanBudget::new(0, usize::MAX, usize::MAX, Duration::from_secs(1));
        let mut scan = ScanBuilder::new(&baseline, ScanCursor::default(), &mut budget);
        scan.whole(0, "profile", Bson::Document(doc! { "name": "new" }))
            .expect("profile scan should encode");
        let delta = scan.finish();
        assert!(!delta.complete);
        assert!(delta.changes.is_empty());
        assert_eq!(delta.next_cursor, ScanCursor::default());
    }

    use std::time::Duration;
}
