//! Pure, actor-local persistence scan primitives.
//!
//! A scan compares current BSON values with an acknowledged baseline. Preparing
//! a delta never mutates that baseline. Only the matching, consuming
//! [`ScanCommit`] can advance it after storage acknowledgement.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mongodb::bson::{Bson, Document};
pub type BsonDocument = mongodb::bson::Document;
use serde::Serialize;

use crate::document::{MongoDocument, bson_serde::encode_path_key};
use crate::persistence::types::MongoFieldPath;

pub trait MongoMapKey {
    fn mongo_map_key(&self) -> String;
}

/// Entry-level encoding used by an opt-in Map scan.
///
/// The adapter must produce the same BSON key and value representation as the
/// field's Serde serializer. It is called one entry at a time, so a custom Map
/// serializer does not need to materialize the complete Map during a diff.
pub trait MongoMapScanAdapter<K: ?Sized, V: ?Sized> {
    fn encode_key(key: &K) -> Result<String, ScanError>;

    fn encode_value(value: &V) -> Result<Bson, ScanError>;
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

    /// Captures a baseline from an already serialized business document. The
    /// derive overrides this to retain declared Map policies.
    fn capture_bson(document: &Document) -> Result<ScanSnapshot, ScanError> {
        ScanSnapshot::empty().capture_bson_document(document, &[], 0)
    }

    fn diff(
        &self,
        baseline: &ScanSnapshot,
        cursor: ScanCursor,
        budget: &mut ScanBudget,
    ) -> Result<ScanDelta, ScanError>;
}

/// Description of one generated business-field scan unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanFieldStrategy {
    Whole,
    Map,
    Flatten,
    Ignore,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanFieldPolicy {
    pub field_index: Option<usize>,
    pub path: &'static str,
    pub strategy: ScanFieldStrategy,
}

impl ScanFieldPolicy {
    pub const fn whole(field_index: usize, path: &'static str) -> Self {
        Self {
            field_index: Some(field_index),
            path,
            strategy: ScanFieldStrategy::Whole,
        }
    }

    pub const fn map(field_index: usize, path: &'static str) -> Self {
        Self {
            field_index: Some(field_index),
            path,
            strategy: ScanFieldStrategy::Map,
        }
    }

    pub const fn flatten(field_index: usize) -> Self {
        Self {
            field_index: Some(field_index),
            path: "",
            strategy: ScanFieldStrategy::Flatten,
        }
    }

    pub const fn ignore(path: &'static str) -> Self {
        Self {
            field_index: None,
            path,
            strategy: ScanFieldStrategy::Ignore,
        }
    }
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
    field_groups: BTreeMap<usize, BTreeSet<String>>,
}

impl ScanSnapshot {
    pub fn empty() -> Self {
        static NEXT_IDENTITY: AtomicU64 = AtomicU64::new(1);
        Self {
            identity: NEXT_IDENTITY.fetch_add(1, Ordering::Relaxed),
            revision: 0,
            fields: BTreeMap::new(),
            field_groups: BTreeMap::new(),
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
        let field = field.into();
        let field_index = self.field_groups.len();
        self.capture_whole_field(field_index, &field, value)?;
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
        let field = field.into();
        let field_index = self.field_groups.len();
        self.capture_map_document_field(field_index, &field, value)?;
        Ok(self)
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
        let field = field.into();
        let field_index = self.field_groups.len();
        self.capture_map_entries_field(field_index, &field, entries)?;
        Ok(self)
    }

    #[doc(hidden)]
    pub fn capture_absent_field(&mut self, field_index: usize) {
        self.field_groups.entry(field_index).or_default();
    }

    #[doc(hidden)]
    pub fn capture_value_field<T>(
        &mut self,
        field_index: usize,
        field: &str,
        value: &T,
    ) -> Result<(), ScanError>
    where
        T: Serialize,
    {
        self.capture_whole_field(field_index, field, &encode_value(value)?)
    }

    #[doc(hidden)]
    pub fn capture_flattened_value_field<T>(
        &mut self,
        field_index: usize,
        value: &T,
    ) -> Result<(), ScanError>
    where
        T: Serialize,
    {
        let bson = encode_value(value)?;
        let Bson::Document(document) = bson else {
            return Err(ScanError::ExpectedDocumentFragment);
        };
        self.capture_fragment_field(field_index, &document)
    }

    #[doc(hidden)]
    pub fn capture_map_entries_field<'entry, K, V>(
        &mut self,
        field_index: usize,
        field: &str,
        entries: impl IntoIterator<Item = (&'entry K, &'entry V)>,
    ) -> Result<(), ScanError>
    where
        K: MongoMapKey + 'entry,
        V: Serialize + 'entry,
    {
        self.capture_map_entries_with_field(
            field_index,
            field,
            entries,
            |key| Ok(key.mongo_map_key()),
            |value| encode_value(value),
        )
    }

    #[doc(hidden)]
    pub fn capture_path_key_map_entries_field<'entry, K, V>(
        &mut self,
        field_index: usize,
        field: &str,
        entries: impl IntoIterator<Item = (&'entry K, &'entry V)>,
    ) -> Result<(), ScanError>
    where
        K: std::fmt::Display + 'entry,
        V: Serialize + 'entry,
    {
        self.capture_map_entries_with_field(
            field_index,
            field,
            entries,
            |key| Ok(encode_path_key(&key.to_string())),
            |value| encode_value(value),
        )
    }

    #[doc(hidden)]
    pub fn capture_map_entries_with_adapter_field<'entry, A, K, V>(
        &mut self,
        field_index: usize,
        field: &str,
        entries: impl IntoIterator<Item = (&'entry K, &'entry V)>,
    ) -> Result<(), ScanError>
    where
        A: MongoMapScanAdapter<K, V>,
        K: 'entry,
        V: 'entry,
    {
        self.capture_map_entries_with_field(
            field_index,
            field,
            entries,
            A::encode_key,
            A::encode_value,
        )
    }

    fn capture_whole_field(
        &mut self,
        field_index: usize,
        field: &str,
        value: &Bson,
    ) -> Result<(), ScanError> {
        validate_field_path(field)?;
        self.fields
            .insert(field.to_owned(), FieldSnapshot::Whole(stable_hash(value)?));
        self.set_group(field_index, [field.to_owned()])
    }

    fn capture_map_document_field(
        &mut self,
        field_index: usize,
        field: &str,
        value: &Document,
    ) -> Result<(), ScanError> {
        validate_field_path(field)?;
        self.fields.insert(
            field.to_owned(),
            FieldSnapshot::Map(hash_document_entries(value)?),
        );
        self.set_group(field_index, [field.to_owned()])
    }

    fn capture_map_entries_with_field<'entry, K, V, FK, FV>(
        &mut self,
        field_index: usize,
        field: &str,
        entries: impl IntoIterator<Item = (&'entry K, &'entry V)>,
        mut encode_key: FK,
        mut encode_entry_value: FV,
    ) -> Result<(), ScanError>
    where
        K: 'entry,
        V: 'entry,
        FK: FnMut(&K) -> Result<String, ScanError>,
        FV: FnMut(&V) -> Result<Bson, ScanError>,
    {
        validate_field_path(field)?;
        let mut hashes = BTreeMap::new();
        for (key, value) in entries {
            let key = encode_key(key)?;
            validate_map_key(&key)?;
            let hash = stable_hash(&encode_entry_value(value)?)?;
            if hashes.insert(key.clone(), hash).is_some() {
                return Err(ScanError::DuplicateMapKey(key));
            }
        }
        self.fields
            .insert(field.to_owned(), FieldSnapshot::Map(hashes));
        self.set_group(field_index, [field.to_owned()])
    }

    fn capture_fragment_field(
        &mut self,
        field_index: usize,
        document: &Document,
    ) -> Result<(), ScanError> {
        let mut paths = BTreeSet::new();
        for (field, value) in document {
            validate_field_path(field)?;
            self.fields
                .insert(field.clone(), FieldSnapshot::Whole(stable_hash(value)?));
            paths.insert(field.clone());
        }
        self.set_group(field_index, paths)
    }

    fn set_group(
        &mut self,
        field_index: usize,
        paths: impl IntoIterator<Item = String>,
    ) -> Result<(), ScanError> {
        let paths = paths.into_iter().collect::<BTreeSet<_>>();
        for path in &paths {
            if self
                .field_groups
                .iter()
                .any(|(index, fields)| *index != field_index && fields.contains(path))
            {
                return Err(ScanError::DuplicateFieldPath(path.clone()));
            }
        }
        self.field_groups.insert(field_index, paths);
        Ok(())
    }

    pub fn capture_bson_document(
        mut self,
        document: &Document,
        policies: &[ScanFieldPolicy],
        field_count: usize,
    ) -> Result<Self, ScanError> {
        for field_index in 0..field_count {
            self.capture_absent_field(field_index);
        }
        let flatten_index = policies.iter().find_map(|policy| {
            (policy.strategy == ScanFieldStrategy::Flatten)
                .then_some(policy.field_index)
                .flatten()
        });
        let mut extra_paths = BTreeSet::new();
        for (field, value) in document {
            validate_field_path(field)?;
            match field_policy(field, policies) {
                Some(ScanFieldPolicy {
                    strategy: ScanFieldStrategy::Ignore,
                    ..
                }) => {}
                Some(ScanFieldPolicy {
                    field_index: Some(field_index),
                    strategy: ScanFieldStrategy::Map,
                    ..
                }) => {
                    if let Bson::Document(document) = value {
                        self.capture_map_document_field(*field_index, field, document)?;
                    } else {
                        self.capture_whole_field(*field_index, field, value)?;
                    }
                }
                Some(ScanFieldPolicy {
                    field_index: Some(field_index),
                    strategy: ScanFieldStrategy::Whole,
                    ..
                }) => self.capture_whole_field(*field_index, field, value)?,
                None if flatten_index.is_some() => {
                    self.fields
                        .insert(field.clone(), FieldSnapshot::Whole(stable_hash(value)?));
                    extra_paths.insert(field.clone());
                }
                None => {}
                Some(_) => unreachable!("flatten policies have no concrete BSON path"),
            }
        }
        if let Some(flatten_index) = flatten_index {
            self.set_group(flatten_index, extra_paths)?;
        }
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
        for field in commit.removed_fields {
            self.fields.remove(&field);
        }
        for (field_index, paths) in commit.field_groups {
            self.field_groups.insert(field_index, paths);
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
}

/// Cooperative document/field scan budget.
///
/// A business field is the smallest resumable unit. Map fields consume one
/// field and compare all entries in that call; their values are encoded one at
/// a time and only changed BSON values are retained for `$set` operations.
#[derive(Debug)]
pub struct ScanBudget {
    remaining_documents: usize,
    remaining_fields: usize,
    deadline: Instant,
    work: ScanWorkMetrics,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ScanWorkMetrics {
    pub encoded_values: u64,
    pub estimated_encoded_bytes: u64,
    pub encoding_nanos: u64,
    pub map_entries_hashed: u64,
}

impl ScanBudget {
    pub fn new(max_documents: usize, max_fields: usize, max_duration: Duration) -> Self {
        Self {
            remaining_documents: max_documents,
            remaining_fields: max_fields,
            deadline: Instant::now() + max_duration,
            work: ScanWorkMetrics::default(),
        }
    }

    pub fn generous() -> Self {
        Self::new(usize::MAX, usize::MAX, Duration::from_secs(60))
    }

    pub fn begin_document(&mut self) -> bool {
        consume(&mut self.remaining_documents) && self.has_time()
    }

    fn field(&mut self) -> bool {
        consume(&mut self.remaining_fields) && self.has_time()
    }

    fn has_time(&self) -> bool {
        Instant::now() <= self.deadline
    }

    fn record_encoding(&mut self, duration: Duration, estimated_encoded_bytes: usize) {
        self.work.encoded_values = self.work.encoded_values.saturating_add(1);
        self.work.estimated_encoded_bytes = self
            .work
            .estimated_encoded_bytes
            .saturating_add(u64::try_from(estimated_encoded_bytes).unwrap_or(u64::MAX));
        self.work.encoding_nanos = self
            .work
            .encoding_nanos
            .saturating_add(u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX));
    }

    fn record_map_hashes(&mut self, entries: usize) {
        self.work.map_entries_hashed = self
            .work
            .map_entries_hashed
            .saturating_add(u64::try_from(entries).unwrap_or(u64::MAX));
    }

    pub(crate) const fn work_metrics(&self) -> ScanWorkMetrics {
        self.work
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
#[derive(Debug, Clone)]
pub struct ScanCommit {
    baseline_identity: u64,
    baseline_revision: u64,
    fields: BTreeMap<String, FieldSnapshot>,
    removed_fields: BTreeSet<String>,
    field_groups: BTreeMap<usize, BTreeSet<String>>,
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
    BaselineKindMismatch(String),
    InvalidMapKey(String),
    DuplicateMapKey(String),
    DuplicateFieldPath(String),
    InvalidFieldPath(String),
    Encoding(String),
    ExpectedDocumentFragment,
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
            Self::BaselineKindMismatch(field) => {
                write!(f, "scan baseline kind mismatch for field: {field}")
            }
            Self::InvalidMapKey(key) => write!(
                f,
                "invalid MongoDB map update-path key: {key}; encode dynamic keys with document::bson_serde::path_key_map"
            ),
            Self::DuplicateMapKey(key) => {
                write!(
                    f,
                    "multiple logical map keys encode as MongoDB path key: {key}"
                )
            }
            Self::DuplicateFieldPath(path) => {
                write!(
                    f,
                    "multiple business fields serialize as MongoDB field path: {path}"
                )
            }
            Self::InvalidFieldPath(path) => {
                write!(f, "invalid MongoDB top-level scan field: {path}")
            }
            Self::Encoding(error) => write!(f, "failed to encode BSON scan value: {error}"),
            Self::ExpectedDocumentFragment => {
                f.write_str("flattened scan field did not encode as a BSON document")
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
    removed_fields: BTreeSet<String>,
    field_groups: BTreeMap<usize, BTreeSet<String>>,
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
            removed_fields: BTreeSet::new(),
            field_groups: BTreeMap::new(),
            complete: active,
            active,
        }
    }

    pub fn whole(&mut self, field_index: usize, field: &str, value: Bson) -> Result<(), ScanError> {
        if !self.begin_field(field_index) {
            return Ok(());
        }
        self.scan_whole(field_index, field, value)
    }

    fn scan_whole(
        &mut self,
        field_index: usize,
        field: &str,
        value: Bson,
    ) -> Result<(), ScanError> {
        validate_field_path(field)?;
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
            None => self.changes.push(FieldChange::Set {
                path: MongoFieldPath::new(field),
                value,
            }),
        }
        self.fields
            .insert(field.to_owned(), FieldSnapshot::Whole(hash));
        self.complete_field(field_index, [field.to_owned()])?;
        Ok(())
    }

    pub fn whole_value<T>(
        &mut self,
        field_index: usize,
        field: &str,
        value: &T,
    ) -> Result<(), ScanError>
    where
        T: Serialize,
    {
        if !self.begin_field(field_index) {
            return Ok(());
        }
        let started = Instant::now();
        let value = encode_value(value)?;
        self.budget
            .record_encoding(started.elapsed(), estimated_bson_value_size(&value));
        self.scan_whole(field_index, field, value)
    }

    pub fn map(
        &mut self,
        field_index: usize,
        field: &str,
        value: Document,
    ) -> Result<(), ScanError> {
        if !self.begin_field(field_index) {
            return Ok(());
        }
        let entries = value.into_iter().map(|(key, value)| Ok((key, value, None)));
        self.scan_encoded_map(field_index, field, entries)
    }

    pub fn map_entries<'entry, K, V>(
        &mut self,
        field_index: usize,
        field: &str,
        entries: impl IntoIterator<Item = (&'entry K, &'entry V)>,
    ) -> Result<(), ScanError>
    where
        K: MongoMapKey + 'entry,
        V: Serialize + 'entry,
    {
        self.map_entries_with(
            field_index,
            field,
            entries,
            |key| Ok(key.mongo_map_key()),
            |value| encode_value(value),
        )
    }

    #[doc(hidden)]
    pub fn path_key_map_entries<'entry, K, V>(
        &mut self,
        field_index: usize,
        field: &str,
        entries: impl IntoIterator<Item = (&'entry K, &'entry V)>,
    ) -> Result<(), ScanError>
    where
        K: std::fmt::Display + 'entry,
        V: Serialize + 'entry,
    {
        self.map_entries_with(
            field_index,
            field,
            entries,
            |key| Ok(encode_path_key(&key.to_string())),
            |value| encode_value(value),
        )
    }

    #[doc(hidden)]
    pub fn map_entries_with_adapter<'entry, A, K, V>(
        &mut self,
        field_index: usize,
        field: &str,
        entries: impl IntoIterator<Item = (&'entry K, &'entry V)>,
    ) -> Result<(), ScanError>
    where
        A: MongoMapScanAdapter<K, V>,
        K: 'entry,
        V: 'entry,
    {
        self.map_entries_with(field_index, field, entries, A::encode_key, A::encode_value)
    }

    fn map_entries_with<'entry, K, V, FK, FV>(
        &mut self,
        field_index: usize,
        field: &str,
        entries: impl IntoIterator<Item = (&'entry K, &'entry V)>,
        mut encode_key: FK,
        mut encode_entry_value: FV,
    ) -> Result<(), ScanError>
    where
        K: 'entry,
        V: 'entry,
        FK: FnMut(&K) -> Result<String, ScanError>,
        FV: FnMut(&V) -> Result<Bson, ScanError>,
    {
        if !self.begin_field(field_index) {
            return Ok(());
        }
        let entries = entries.into_iter().map(|(key, value)| {
            let key = encode_key(key)?;
            validate_map_key(&key)?;
            let started = Instant::now();
            let value = encode_entry_value(value)?;
            Ok((key, value, Some(started.elapsed())))
        });
        self.scan_encoded_map(field_index, field, entries)
    }

    fn scan_encoded_map(
        &mut self,
        field_index: usize,
        field: &str,
        entries: impl IntoIterator<Item = Result<(String, Bson, Option<Duration>), ScanError>>,
    ) -> Result<(), ScanError> {
        validate_field_path(field)?;
        let old = match self.baseline.fields.get(field) {
            Some(FieldSnapshot::Map(old)) => Some(old),
            Some(FieldSnapshot::Whole(_)) => {
                return Err(ScanError::BaselineKindMismatch(field.to_owned()));
            }
            None => None,
        };
        let mut current = BTreeMap::new();
        let mut changes = BTreeMap::<String, Option<Bson>>::new();
        for entry in entries {
            let (key, value, encoding_duration) = entry?;
            validate_map_key(&key)?;
            if let Some(duration) = encoding_duration {
                self.budget
                    .record_encoding(duration, estimated_bson_value_size(&value));
            }
            self.budget.record_map_hashes(1);
            let hash = stable_hash(&value)?;
            if current.insert(key.clone(), hash).is_some() {
                return Err(ScanError::DuplicateMapKey(key));
            }
            if old.and_then(|old| old.get(&key)) != Some(&hash) {
                changes.insert(key, Some(value));
            }
        }
        if let Some(old) = old {
            for key in old.keys() {
                if !current.contains_key(key) {
                    changes.insert(key.clone(), None);
                }
            }
        } else if current.is_empty() {
            self.changes.push(FieldChange::Set {
                path: MongoFieldPath::new(field),
                value: Bson::Document(Document::new()),
            });
        }
        for (key, value) in changes {
            self.changes.push(match value {
                Some(value) => FieldChange::Set {
                    path: MongoFieldPath::new(field).child(&key),
                    value,
                },
                None => FieldChange::Unset {
                    path: MongoFieldPath::new(field).child(&key),
                },
            });
        }
        self.fields
            .insert(field.to_owned(), FieldSnapshot::Map(current));
        self.complete_field(field_index, [field.to_owned()])?;
        Ok(())
    }

    #[doc(hidden)]
    pub fn absent(&mut self, field_index: usize, field: &str) -> Result<(), ScanError> {
        if !self.begin_field(field_index) {
            return Ok(());
        }
        if self.baseline.fields.contains_key(field) {
            self.changes.push(FieldChange::Unset {
                path: MongoFieldPath::new(field),
            });
            self.removed_fields.insert(field.to_owned());
        }
        self.complete_field(field_index, [])?;
        Ok(())
    }

    #[doc(hidden)]
    pub fn flattened_absent(&mut self, field_index: usize) -> Result<(), ScanError> {
        if !self.begin_field(field_index) {
            return Ok(());
        }
        self.remove_group_fields(field_index, &BTreeSet::new());
        self.complete_field(field_index, [])?;
        Ok(())
    }

    #[doc(hidden)]
    pub fn flattened_value<T>(&mut self, field_index: usize, value: &T) -> Result<(), ScanError>
    where
        T: Serialize,
    {
        if !self.begin_field(field_index) {
            return Ok(());
        }
        let started = Instant::now();
        let value = encode_value(value)?;
        self.budget
            .record_encoding(started.elapsed(), estimated_bson_value_size(&value));
        let Bson::Document(document) = value else {
            return Err(ScanError::ExpectedDocumentFragment);
        };
        let mut paths = BTreeSet::new();
        for (field, value) in document {
            validate_field_path(&field)?;
            let hash = stable_hash(&value)?;
            match self.baseline.fields.get(&field) {
                Some(FieldSnapshot::Whole(old)) if old == &hash => {}
                Some(FieldSnapshot::Whole(_)) | None => {
                    self.changes.push(FieldChange::Set {
                        path: MongoFieldPath::new(&field),
                        value,
                    });
                }
                Some(FieldSnapshot::Map(_)) => {
                    return Err(ScanError::BaselineKindMismatch(field));
                }
            }
            self.fields
                .insert(field.clone(), FieldSnapshot::Whole(hash));
            paths.insert(field);
        }
        self.remove_group_fields(field_index, &paths);
        self.complete_field(field_index, paths)?;
        Ok(())
    }

    fn remove_group_fields(&mut self, field_index: usize, current: &BTreeSet<String>) {
        if let Some(previous) = self.baseline.field_groups.get(&field_index) {
            for field in previous.difference(current) {
                self.changes.push(FieldChange::Unset {
                    path: MongoFieldPath::new(field),
                });
                self.removed_fields.insert(field.clone());
            }
        }
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
                removed_fields: self.removed_fields,
                field_groups: self.field_groups,
                next_cursor: self.next_cursor.clone(),
            },
            next_cursor: self.next_cursor,
            complete: self.complete,
        }
    }

    fn begin_field(&mut self, field_index: usize) -> bool {
        if !self.active || field_index < self.cursor.field_index {
            return false;
        }
        if !self.budget.field() {
            self.pause(field_index);
            return false;
        }
        true
    }

    fn complete_field(
        &mut self,
        field_index: usize,
        paths: impl IntoIterator<Item = String>,
    ) -> Result<(), ScanError> {
        let paths = paths.into_iter().collect::<BTreeSet<_>>();
        for path in &paths {
            let conflicts_with_current = self
                .field_groups
                .iter()
                .any(|(index, fields)| *index != field_index && fields.contains(path));
            let conflicts_with_baseline = self
                .baseline
                .field_groups
                .iter()
                .any(|(index, fields)| *index != field_index && fields.contains(path));
            if conflicts_with_current || conflicts_with_baseline {
                return Err(ScanError::DuplicateFieldPath(path.clone()));
            }
        }
        self.field_groups.insert(field_index, paths);
        self.next_cursor = ScanCursor {
            field_index: field_index + 1,
        };
        Ok(())
    }

    fn pause(&mut self, field_index: usize) {
        self.complete = false;
        self.active = false;
        self.next_cursor = ScanCursor { field_index };
    }
}

mod encoding;

use encoding::{
    StableHash, encode_value, estimated_bson_value_size, field_policy, hash_document_entries,
    stable_hash, validate_field_path, validate_map_key,
};
#[cfg(test)]
mod tests;
