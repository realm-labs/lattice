//! Reusable actor-local coordination for scanned MongoDB documents.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use crate::document::tracked::Tracked;
use crate::document::{
    LoadedDocument, LoadedDocumentMeta, encode_business_document, encode_document_id,
};
use crate::error::MongoStoreError;
use crate::scan::{FieldChange, MongoScan, ScanBudget, ScanCursor, ScanError, ScanSnapshot};

use super::request::{
    CreateMode, DocumentCommit, DocumentOperation, DocumentWriteOutcome, FlushGeneration,
    FlushOutcome, FlushRequest, InFlightCommit, PreparedDocumentWrite, PreparedFlush, WriteToken,
};
use super::types::MongoDocumentKey;

#[derive(Debug)]
struct DocumentState {
    baseline: ScanSnapshot,
    cursor: ScanCursor,
    acknowledged_mutation_epoch: Option<u64>,
    scanning_mutation_epoch: Option<u64>,
    version: i64,
    updated_at_ms: i64,
    create_mode: Option<CreateMode>,
}

impl DocumentState {
    fn needs_tracked_scan(&self, mutation_epoch: u64) -> bool {
        self.acknowledged_mutation_epoch != Some(mutation_epoch)
            || self.scanning_mutation_epoch.is_some()
            || self.create_mode.is_some()
    }

    fn scan_cursor(&self, mutation_epoch: Option<u64>) -> ScanCursor {
        if mutation_epoch.is_some()
            && self
                .scanning_mutation_epoch
                .is_some_and(|scanning| Some(scanning) != mutation_epoch)
        {
            ScanCursor::default()
        } else {
            self.cursor.clone()
        }
    }

    fn apply_commit_metadata(&mut self, mutation_epoch: Option<u64>, scan_complete: bool) {
        let Some(mutation_epoch) = mutation_epoch else {
            return;
        };
        if scan_complete {
            self.acknowledged_mutation_epoch = Some(mutation_epoch);
            self.scanning_mutation_epoch = None;
        } else {
            self.scanning_mutation_epoch = Some(mutation_epoch);
        }
    }
}

/// Retry timing for failed persistence dispatches and document writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub max_exponent: u32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(2),
            max_exponent: 6,
        }
    }
}

impl RetryPolicy {
    fn delay(self, attempt: u32) -> Duration {
        let exponent = attempt.saturating_sub(1).min(self.max_exponent);
        self.initial_delay
            .saturating_mul(1_u32.checked_shl(exponent).unwrap_or(u32::MAX))
            .min(self.max_delay)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistenceConflict {
    pub key: MongoDocumentKey,
    pub expected_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PersistenceCounters {
    pub scans: u64,
    pub changed_paths: u64,
    pub attempted_documents: u64,
    pub applied_documents: u64,
    pub failed_documents: u64,
    pub conflicts: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PersistenceReport {
    pub clean: usize,
    pub applied: usize,
    pub failed: usize,
    pub conflicts: usize,
}

/// Owns acknowledgement baselines and the exact in-flight generation for a
/// heterogeneous set of MongoDB documents belonging to one actor activation.
#[derive(Debug)]
pub struct MongoPersistenceCoordinator {
    documents: BTreeMap<MongoDocumentKey, DocumentState>,
    activation_epoch: u64,
    next_sequence: u64,
    in_flight: Option<InFlightCommit>,
    conflict: Option<PersistenceConflict>,
    last_error: Option<String>,
    retry_attempt: u32,
    retry_not_before: Option<Instant>,
    retry_policy: RetryPolicy,
    counters: PersistenceCounters,
}

impl MongoPersistenceCoordinator {
    pub fn new(activation_epoch: u64) -> Self {
        Self::with_retry_policy(activation_epoch, RetryPolicy::default())
    }

    pub fn with_retry_policy(activation_epoch: u64, retry_policy: RetryPolicy) -> Self {
        Self {
            documents: BTreeMap::new(),
            activation_epoch,
            next_sequence: 1,
            in_flight: None,
            conflict: None,
            last_error: None,
            retry_attempt: 0,
            retry_not_before: None,
            retry_policy,
            counters: PersistenceCounters::default(),
        }
    }

    pub fn attach_loaded<D>(
        &mut self,
        value: &D,
        meta: LoadedDocumentMeta,
    ) -> Result<(), PersistenceError>
    where
        D: MongoScan,
    {
        self.attach(value, meta, None, None)
    }

    pub fn attach_loaded_tracked<D>(
        &mut self,
        value: &D,
        mutation_epoch: u64,
        meta: LoadedDocumentMeta,
    ) -> Result<(), PersistenceError>
    where
        D: MongoScan,
    {
        self.attach(value, meta, Some(mutation_epoch), None)
    }

    pub fn track_loaded<D>(
        &mut self,
        loaded: LoadedDocument<D>,
    ) -> Result<Tracked<D>, PersistenceError>
    where
        D: MongoScan,
    {
        let (value, meta) = loaded.split();
        self.attach_loaded_tracked(&value, 0, meta)?;
        Ok(Tracked::clean(value))
    }

    /// Atomically registers a runtime-sized batch of loaded documents of one
    /// type and returns actor-local tracked values in input order.
    pub fn track_loaded_many<D>(
        &mut self,
        loaded: Vec<LoadedDocument<D>>,
    ) -> Result<Vec<Tracked<D>>, PersistenceError>
    where
        D: MongoScan,
    {
        let mut pending = Vec::with_capacity(loaded.len());
        let mut keys = BTreeSet::new();

        for loaded in loaded {
            let (value, meta) = loaded.split();
            let key = MongoDocumentKey::for_document::<D>(value.id())?;
            if self.documents.contains_key(&key) || !keys.insert(key.clone()) {
                return Err(PersistenceError::DuplicateDocument(key));
            }
            pending.push((
                key,
                DocumentState {
                    baseline: value.capture()?,
                    cursor: ScanCursor::default(),
                    acknowledged_mutation_epoch: Some(0),
                    scanning_mutation_epoch: None,
                    version: meta.version,
                    updated_at_ms: meta.updated_at_ms,
                    create_mode: None,
                },
                value,
            ));
        }

        let mut tracked = Vec::with_capacity(pending.len());
        for (key, state, value) in pending {
            self.documents.insert(key, state);
            tracked.push(Tracked::clean(value));
        }
        Ok(tracked)
    }

    pub fn attach_new<D>(&mut self, value: &D, mode: CreateMode) -> Result<(), PersistenceError>
    where
        D: MongoScan,
    {
        self.attach(
            value,
            LoadedDocumentMeta {
                version: 0,
                updated_at_ms: 0,
            },
            None,
            Some(mode),
        )
    }

    pub fn track_new<D>(
        &mut self,
        value: D,
        mode: CreateMode,
    ) -> Result<Tracked<D>, PersistenceError>
    where
        D: MongoScan,
    {
        self.attach_new(&value, mode)?;
        Ok(Tracked::clean(value))
    }

    fn attach<D>(
        &mut self,
        value: &D,
        meta: LoadedDocumentMeta,
        mutation_epoch: Option<u64>,
        create_mode: Option<CreateMode>,
    ) -> Result<(), PersistenceError>
    where
        D: MongoScan,
    {
        let key = MongoDocumentKey::for_document::<D>(value.id())?;
        if self.documents.contains_key(&key) {
            return Err(PersistenceError::DuplicateDocument(key));
        }
        self.documents.insert(
            key,
            DocumentState {
                baseline: value.capture()?,
                cursor: ScanCursor::default(),
                acknowledged_mutation_epoch: mutation_epoch,
                scanning_mutation_epoch: None,
                version: meta.version,
                updated_at_ms: meta.updated_at_ms,
                create_mode,
            },
        );
        Ok(())
    }

    /// Unregisters a document without deleting it from MongoDB.
    pub fn detach<D>(&mut self, id: &D::Id) -> Result<(), PersistenceError>
    where
        D: MongoScan,
    {
        if self.in_flight.is_some() {
            return Err(PersistenceError::FlushInFlight);
        }
        if self.conflict.is_some() {
            return Err(PersistenceError::ConflictBlocked);
        }
        let key = MongoDocumentKey::for_document::<D>(id)?;
        let document = self
            .documents
            .get(&key)
            .ok_or_else(|| PersistenceError::UnknownDocument(key.clone()))?;
        if document.create_mode.is_some() {
            return Err(PersistenceError::CreatePending(key));
        }
        self.documents.remove(&key);
        Ok(())
    }

    /// Returns whether a tracked document is durably clean and can be
    /// detached without losing actor-local state.
    pub fn tracked_is_clean<D>(&self, tracked: &Tracked<D>) -> Result<bool, PersistenceError>
    where
        D: MongoScan,
    {
        let value = tracked.read();
        let key = MongoDocumentKey::for_document::<D>(value.id())?;
        let state = self
            .documents
            .get(&key)
            .ok_or_else(|| PersistenceError::UnknownDocument(key.clone()))?;
        let in_flight = self.in_flight.as_ref().is_some_and(|commit| {
            commit
                .document_commits
                .values()
                .chain(commit.clean_commits.iter())
                .any(|document| document.key == key)
        });
        let conflicted = self
            .conflict
            .as_ref()
            .is_some_and(|conflict| conflict.key == key);
        Ok(!in_flight
            && !conflicted
            && state.create_mode.is_none()
            && state.scanning_mutation_epoch.is_none()
            && state.cursor == ScanCursor::default()
            && state.acknowledged_mutation_epoch == Some(tracked.mutation_epoch()))
    }

    /// Detaches a tracked document only when its current mutation epoch has
    /// already been acknowledged by storage.
    pub fn detach_tracked_if_clean<D>(
        &mut self,
        tracked: &Tracked<D>,
    ) -> Result<bool, PersistenceError>
    where
        D: MongoScan,
    {
        if !self.tracked_is_clean(tracked)? {
            return Ok(false);
        }
        let key = MongoDocumentKey::for_document::<D>(tracked.read().id())?;
        self.documents.remove(&key);
        Ok(true)
    }

    pub fn prepare<F>(
        &mut self,
        budget: ScanBudget,
        visit: F,
    ) -> Result<PreparedFlush, PersistenceError>
    where
        F: FnOnce(&mut MongoPreparation<'_>) -> Result<(), PersistenceError>,
    {
        if self.in_flight.is_some() {
            return Err(PersistenceError::FlushInFlight);
        }
        if self.conflict.is_some() {
            return Err(PersistenceError::ConflictBlocked);
        }
        let generation = FlushGeneration {
            activation_epoch: self.activation_epoch,
            sequence: self.next_sequence,
        };
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(PersistenceError::GenerationOverflow)?;
        let mut preparation = MongoPreparation::new(&self.documents, generation, budget);
        visit(&mut preparation)?;
        self.counters.scans = self.counters.scans.saturating_add(preparation.scans);
        self.counters.changed_paths = self
            .counters
            .changed_paths
            .saturating_add(preparation.changed_paths);
        Ok(preparation.finish())
    }

    pub fn begin_flush(&mut self, commit: InFlightCommit) -> Result<(), PersistenceError> {
        self.validate_generation(commit.generation)?;
        if self.in_flight.is_some() {
            return Err(PersistenceError::FlushInFlight);
        }
        self.in_flight = Some(commit);
        Ok(())
    }

    pub fn complete_clean(
        &mut self,
        commit: InFlightCommit,
    ) -> Result<PersistenceReport, PersistenceError> {
        self.validate_generation(commit.generation)?;
        if !commit.document_commits.is_empty() {
            return Err(PersistenceError::ExpectedCleanCommit);
        }
        let mut report = PersistenceReport::default();
        self.apply_clean_commits(commit.clean_commits, &mut report)?;
        Ok(report)
    }

    pub fn complete(
        &mut self,
        generation: FlushGeneration,
        outcome: FlushOutcome,
    ) -> Result<PersistenceReport, PersistenceError> {
        self.validate_generation(generation)?;
        let expected = self
            .in_flight
            .as_ref()
            .ok_or(PersistenceError::NoFlushInFlight)?;
        if expected.generation != generation {
            return Err(PersistenceError::ForeignGeneration {
                expected: expected.generation,
                actual: generation,
            });
        }
        let expected_tokens = expected
            .document_commits
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        let actual_tokens = outcome.documents.keys().copied().collect::<BTreeSet<_>>();
        if expected_tokens != actual_tokens {
            return Err(PersistenceError::OutcomeTokenMismatch);
        }
        for (token, commit) in &expected.document_commits {
            let state = self
                .documents
                .get(&commit.key)
                .ok_or_else(|| PersistenceError::UnknownDocument(commit.key.clone()))?;
            if let DocumentWriteOutcome::Applied {
                previous_version,
                new_version,
                ..
            } = outcome.documents.get(token).expect("validated token set")
                && (*previous_version != state.version || *new_version != state.version + 1)
            {
                return Err(PersistenceError::InvalidAppliedVersion(commit.key.clone()));
            }
        }

        let commit = self.in_flight.take().expect("checked in-flight commit");
        let mut report = PersistenceReport::default();
        self.apply_clean_commits(commit.clean_commits, &mut report)?;
        self.counters.attempted_documents = self
            .counters
            .attempted_documents
            .saturating_add(commit.document_commits.len() as u64);

        for (token, document_commit) in commit.document_commits {
            let outcome = outcome.documents.get(&token).expect("validated token set");
            let state = self
                .documents
                .get_mut(&document_commit.key)
                .ok_or_else(|| PersistenceError::UnknownDocument(document_commit.key.clone()))?;
            match outcome {
                DocumentWriteOutcome::Applied {
                    new_version,
                    updated_at_ms,
                    ..
                } => {
                    state.cursor = state.baseline.apply(document_commit.scan)?;
                    state.apply_commit_metadata(
                        document_commit.mutation_epoch,
                        document_commit.scan_complete,
                    );
                    state.version = *new_version;
                    state.updated_at_ms = *updated_at_ms;
                    state.create_mode = None;
                    report.applied += 1;
                    self.counters.applied_documents =
                        self.counters.applied_documents.saturating_add(1);
                }
                DocumentWriteOutcome::VersionConflict { expected_version } => {
                    self.conflict = Some(PersistenceConflict {
                        key: document_commit.key,
                        expected_version: *expected_version,
                    });
                    report.conflicts += 1;
                    self.counters.conflicts = self.counters.conflicts.saturating_add(1);
                }
                DocumentWriteOutcome::NotFound { expected_version } => {
                    self.last_error = Some(format!(
                        "document {} was missing at expected version {expected_version}",
                        document_commit.key.id
                    ));
                    report.failed += 1;
                    self.counters.failed_documents =
                        self.counters.failed_documents.saturating_add(1);
                }
                DocumentWriteOutcome::Failed { error } => {
                    self.last_error = Some(error.to_string());
                    report.failed += 1;
                    self.counters.failed_documents =
                        self.counters.failed_documents.saturating_add(1);
                }
                DocumentWriteOutcome::NotAttempted => {
                    self.last_error = Some("document write was not attempted".to_owned());
                    report.failed += 1;
                    self.counters.failed_documents =
                        self.counters.failed_documents.saturating_add(1);
                }
            }
        }
        if report.failed > 0 {
            self.schedule_retry();
        } else if report.conflicts == 0 {
            self.retry_attempt = 0;
            self.retry_not_before = None;
            self.last_error = None;
        }
        Ok(report)
    }

    pub fn dispatch_failed(
        &mut self,
        generation: FlushGeneration,
        error: impl Into<String>,
    ) -> Result<(), PersistenceError> {
        self.validate_generation(generation)?;
        let expected = self
            .in_flight
            .as_ref()
            .ok_or(PersistenceError::NoFlushInFlight)?;
        if expected.generation != generation {
            return Err(PersistenceError::ForeignGeneration {
                expected: expected.generation,
                actual: generation,
            });
        }
        self.in_flight = None;
        self.last_error = Some(error.into());
        self.schedule_retry();
        Ok(())
    }

    pub fn has_in_flight(&self) -> bool {
        self.in_flight.is_some()
    }

    pub fn pending_document_count(&self) -> usize {
        self.in_flight
            .as_ref()
            .map_or(0, |commit| commit.document_commits.len())
            + self
                .documents
                .values()
                .filter(|document| document.create_mode.is_some())
                .count()
    }

    pub fn conflict(&self) -> Option<&PersistenceConflict> {
        self.conflict.as_ref()
    }

    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    pub fn retry_delay(&self) -> Option<Duration> {
        self.retry_not_before
            .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
    }

    pub const fn retry_attempt(&self) -> u32 {
        self.retry_attempt
    }

    pub const fn counters(&self) -> &PersistenceCounters {
        &self.counters
    }

    pub fn document_meta(&self, key: &MongoDocumentKey) -> Option<(i64, i64)> {
        self.documents
            .get(key)
            .map(|state| (state.version, state.updated_at_ms))
    }

    fn schedule_retry(&mut self) {
        self.retry_attempt = self.retry_attempt.saturating_add(1);
        self.retry_not_before = Some(Instant::now() + self.retry_policy.delay(self.retry_attempt));
    }

    fn validate_generation(&self, generation: FlushGeneration) -> Result<(), PersistenceError> {
        if generation.activation_epoch == self.activation_epoch {
            Ok(())
        } else {
            Err(PersistenceError::StaleActivation {
                expected: self.activation_epoch,
                actual: generation.activation_epoch,
            })
        }
    }

    fn apply_clean_commits(
        &mut self,
        commits: Vec<DocumentCommit>,
        report: &mut PersistenceReport,
    ) -> Result<(), PersistenceError> {
        for commit in commits {
            let state = self
                .documents
                .get_mut(&commit.key)
                .ok_or_else(|| PersistenceError::UnknownDocument(commit.key.clone()))?;
            state.cursor = state.baseline.apply(commit.scan)?;
            state.apply_commit_metadata(commit.mutation_epoch, commit.scan_complete);
            report.clean += 1;
        }
        Ok(())
    }
}

/// A single synchronous preparation pass over business-owned document values.
pub struct MongoPreparation<'a> {
    documents: &'a BTreeMap<MongoDocumentKey, DocumentState>,
    generation: FlushGeneration,
    budget: ScanBudget,
    next_token: u64,
    writes: Vec<PreparedDocumentWrite>,
    document_commits: BTreeMap<WriteToken, DocumentCommit>,
    clean_commits: Vec<DocumentCommit>,
    scans: u64,
    changed_paths: u64,
    scan_complete: bool,
}

impl<'a> MongoPreparation<'a> {
    fn new(
        documents: &'a BTreeMap<MongoDocumentKey, DocumentState>,
        generation: FlushGeneration,
        budget: ScanBudget,
    ) -> Self {
        Self {
            documents,
            generation,
            budget,
            next_token: 1,
            writes: Vec::new(),
            document_commits: BTreeMap::new(),
            clean_commits: Vec::new(),
            scans: 0,
            changed_paths: 0,
            scan_complete: true,
        }
    }

    pub fn scan<D>(&mut self, value: &D) -> Result<(), PersistenceError>
    where
        D: MongoScan,
    {
        self.scan_inner(value, None)
    }

    pub fn scan_tracked<D>(&mut self, tracked: &Tracked<D>) -> Result<(), PersistenceError>
    where
        D: MongoScan,
    {
        let value = tracked.read();
        let mutation_epoch = tracked.mutation_epoch();
        let key = MongoDocumentKey::for_document::<D>(value.id())?;
        let state = self
            .documents
            .get(&key)
            .ok_or_else(|| PersistenceError::UnknownDocument(key))?;
        if !state.needs_tracked_scan(mutation_epoch) {
            return Ok(());
        }
        self.scan_inner(value, Some(mutation_epoch))
    }

    fn scan_inner<D>(
        &mut self,
        value: &D,
        mutation_epoch: Option<u64>,
    ) -> Result<(), PersistenceError>
    where
        D: MongoScan,
    {
        let key = MongoDocumentKey::for_document::<D>(value.id())?;
        let state = self
            .documents
            .get(&key)
            .ok_or_else(|| PersistenceError::UnknownDocument(key.clone()))?;
        let cursor = state.scan_cursor(mutation_epoch);
        let delta = value.diff(&state.baseline, cursor.clone(), &mut self.budget)?;
        self.scan_complete &= delta.complete;
        self.scans = self.scans.saturating_add(1);
        self.changed_paths = self
            .changed_paths
            .saturating_add(delta.changes.len() as u64);
        if delta.changes.is_empty() && !delta.complete && delta.next_cursor == cursor {
            return Ok(());
        }
        let commit = DocumentCommit {
            key: key.clone(),
            scan: delta.commit,
            mutation_epoch,
            scan_complete: delta.complete,
        };
        if delta.changes.is_empty() && state.create_mode.is_none() {
            self.clean_commits.push(commit);
            return Ok(());
        }

        let token = WriteToken(self.next_token);
        self.next_token = self
            .next_token
            .checked_add(1)
            .ok_or(PersistenceError::WriteTokenOverflow)?;
        let operation = if let Some(mode) = state.create_mode {
            DocumentOperation::Create {
                document: encode_business_document(value)?,
                mode,
            }
        } else {
            let mut sets = BTreeMap::new();
            let mut unsets = BTreeSet::new();
            for change in delta.changes {
                match change {
                    FieldChange::Set { path, value } => {
                        sets.insert(path, value);
                    }
                    FieldChange::Unset { path } => {
                        unsets.insert(path);
                    }
                }
            }
            DocumentOperation::Update { sets, unsets }
        };
        self.writes.push(PreparedDocumentWrite {
            token,
            key,
            document_id: encode_document_id::<D>(value.id())?,
            expected_version: state.version,
            operation,
        });
        self.document_commits.insert(token, commit);
        Ok(())
    }

    fn finish(self) -> PreparedFlush {
        let commit = InFlightCommit {
            generation: self.generation,
            document_commits: self.document_commits,
            clean_commits: self.clean_commits,
        };
        let request = (!self.writes.is_empty()).then_some(FlushRequest {
            generation: self.generation,
            writes: self.writes,
        });
        PreparedFlush {
            request,
            commit,
            scan_complete: self.scan_complete,
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum PersistenceError {
    #[error("required document in collection {collection} with ID {id} was not found")]
    RequiredDocumentMissing {
        collection: &'static str,
        id: String,
    },
    #[error(
        "loaded document in collection {collection} has ID {actual}, expected aggregate ID {expected}"
    )]
    DocumentIdMismatch {
        collection: &'static str,
        expected: String,
        actual: String,
    },
    #[error("document is already registered: {0:?}")]
    DuplicateDocument(MongoDocumentKey),
    #[error("document is not registered: {0:?}")]
    UnknownDocument(MongoDocumentKey),
    #[error("a persistence flush is already in flight")]
    FlushInFlight,
    #[error("no persistence flush is in flight")]
    NoFlushInFlight,
    #[error("a version conflict blocks persistence")]
    ConflictBlocked,
    #[error("a clean completion contained document writes")]
    ExpectedCleanCommit,
    #[error("persistence generation overflow")]
    GenerationOverflow,
    #[error("persistence write token overflow")]
    WriteTokenOverflow,
    #[error("flush outcome tokens do not match the in-flight request")]
    OutcomeTokenMismatch,
    #[error("applied version did not advance exactly once: {0:?}")]
    InvalidAppliedVersion(MongoDocumentKey),
    #[error("new document has not been durably created: {0:?}")]
    CreatePending(MongoDocumentKey),
    #[error("stale activation epoch: expected {expected}, got {actual}")]
    StaleActivation { expected: u64, actual: u64 },
    #[error("foreign flush generation: expected {expected:?}, got {actual:?}")]
    ForeignGeneration {
        expected: FlushGeneration,
        actual: FlushGeneration,
    },
    #[error(transparent)]
    Scan(#[from] ScanError),
    #[error(transparent)]
    Store(#[from] MongoStoreError),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use mongodb::bson::Bson;
    use serde::{Deserialize, Serialize};

    use super::{MongoPersistenceCoordinator, PersistenceError, RetryPolicy};
    use crate::document::{LoadedDocument, LoadedDocumentMeta};
    use crate::error::MongoStoreError;
    use crate::persistence::request::{
        CreateMode, DocumentOperation, DocumentWriteOutcome, FlushOutcome,
    };
    use crate::persistence::types::MongoDocumentKey;
    use crate::scan::ScanBudget;
    use crate::{MongoDocument as MongoDocumentDerive, MongoScan};

    #[derive(Debug, Clone, Serialize, Deserialize, MongoDocumentDerive, MongoScan)]
    #[mongo(collection = "coordinator_test")]
    struct TestDocument {
        #[mongo(id)]
        id: u64,
        name: String,
        items: BTreeMap<String, i32>,
    }

    fn document(name: &str) -> TestDocument {
        TestDocument {
            id: 42,
            name: name.to_owned(),
            items: BTreeMap::from([("one".to_owned(), 1)]),
        }
    }

    fn loaded(value: &TestDocument, mutation_epoch: Option<u64>) -> MongoPersistenceCoordinator {
        let mut coordinator = MongoPersistenceCoordinator::new(7);
        let meta = LoadedDocumentMeta {
            version: 3,
            updated_at_ms: 10,
        };
        if let Some(epoch) = mutation_epoch {
            coordinator
                .attach_loaded_tracked(value, epoch, meta)
                .expect("tracked document should attach");
        } else {
            coordinator
                .attach_loaded(value, meta)
                .expect("document should attach");
        }
        coordinator
    }

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
            .expect("retry should regenerate from old baseline");
        assert!(retry.request.is_some());
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
        assert_eq!(coordinator.conflict().unwrap().expected_version, 3);
        assert!(matches!(
            coordinator.prepare(ScanBudget::generous(), |_| Ok(())),
            Err(PersistenceError::ConflictBlocked)
        ));
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
                ScanBudget::new(1, 1, 8, Duration::from_secs(1)),
                |preparation| preparation.scan(&value),
            )
            .unwrap();
        assert!(!partial.scan_complete);
        let request = partial.request.as_ref().unwrap();
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
}
