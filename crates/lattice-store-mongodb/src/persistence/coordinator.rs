//! Reusable actor-local coordination for scanned MongoDB documents.

pub mod drain;
mod recovery;

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use lattice_actor::context::PipeTaskHandle;

use crate::document::tracked::Tracked;
use crate::document::{LoadedDocument, LoadedDocumentMeta, LoadedScannedDocument};
use crate::error::{MongoStoreError, MongoStoreErrorRecovery};
use crate::scan::{MongoScan, ScanBudget, ScanCursor, ScanError, ScanSnapshot, ScanWorkMetrics};

use super::request::{
    CreateMode, DocumentCommit, DocumentWriteOutcome, FlushGeneration, FlushOutcome, FlushRequest,
    InFlightCommit, PreparedDocumentWrite, PreparedFlush, WriteToken,
};
use super::types::MongoDocumentKey;

#[derive(Debug)]
struct DocumentState {
    baseline: ScanSnapshot,
    cursor: ScanCursor,
    acknowledged_mutation_epoch: Option<u64>,
    scanning_mutation_epoch: Option<u64>,
    scanning_changed: bool,
    version: i64,
    updated_at_ms: i64,
    create_mode: Option<CreateMode>,
    rejection: Option<DocumentRejection>,
    conflict_policy: ConflictPolicy,
    conflict: Option<PersistenceConflict>,
}

#[derive(Debug)]
struct DocumentRejection {
    mutation_epoch: Option<u64>,
    error: String,
}

impl DocumentState {
    fn needs_tracked_scan(&self, mutation_epoch: u64) -> bool {
        self.acknowledged_mutation_epoch != Some(mutation_epoch)
            || self.scanning_mutation_epoch.is_some()
            || self.create_mode.is_some()
    }

    fn scan_cursor(&self) -> ScanCursor {
        self.cursor.clone()
    }

    fn sweep_is_current(&self, mutation_epoch: Option<u64>) -> bool {
        mutation_epoch.is_none()
            || self.scanning_mutation_epoch.is_none()
            || self.scanning_mutation_epoch == mutation_epoch
    }

    fn apply_commit_metadata(
        &mut self,
        mutation_epoch: Option<u64>,
        scan_complete: bool,
        sweep_complete: bool,
        changed: bool,
    ) -> bool {
        let Some(mutation_epoch) = mutation_epoch else {
            return false;
        };
        let changed = self.scanning_changed || changed;
        if scan_complete {
            let false_positive =
                self.acknowledged_mutation_epoch != Some(mutation_epoch) && !changed;
            self.acknowledged_mutation_epoch = Some(mutation_epoch);
            self.scanning_mutation_epoch = None;
            self.scanning_changed = false;
            false_positive
        } else {
            if sweep_complete || self.scanning_mutation_epoch.is_none() {
                self.scanning_mutation_epoch = Some(mutation_epoch);
            }
            self.scanning_changed = changed;
            false
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
    pub kind: PersistenceConflictKind,
    pub policy: ConflictPolicy,
}

/// How one optimistic-lock conflict affects the other documents registered by
/// the same actor activation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConflictPolicy {
    /// Stop all later preparation until the application reloads or explicitly
    /// removes the conflicted document. This is the safe aggregate default.
    #[default]
    BlockCoordinator,
    /// Quarantine only the conflicted document and keep preparing unrelated
    /// documents owned by the same actor activation.
    QuarantineDocument,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceConflictKind {
    VersionConflict,
    NotFound,
    /// A previously dispatched operation may or may not have reached MongoDB.
    OutcomeUnknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PersistenceCounters {
    pub scans: u64,
    pub changed_paths: u64,
    /// Documents handed to the backing store for an actual write attempt.
    pub attempted_documents: u64,
    pub applied_documents: u64,
    /// Documents rejected during preparation or failed by the backing store.
    pub failed_documents: u64,
    pub conflicts: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PersistenceScanMetrics {
    /// Number of individual business values encoded for diff scans.
    pub encoded_values: u64,
    /// Estimated BSON bytes produced while encoding individual business values.
    ///
    /// This is calculated from the encoded BSON values and deliberately avoids
    /// a second BSON serialization solely for metrics.
    pub estimated_encoded_bytes: u64,
    /// Nanoseconds spent encoding Rust business values into BSON.
    pub encoding_nanos: u64,
    /// Number of map entries hashed while preparing field-level diffs.
    pub map_entries_hashed: u64,
    /// Completed tracked scans triggered by a new mutation epoch that found no
    /// serialized business change.
    pub false_positive_scans: u64,
}

impl PersistenceScanMetrics {
    fn record_work(&mut self, work: ScanWorkMetrics) {
        self.encoded_values = self.encoded_values.saturating_add(work.encoded_values);
        self.estimated_encoded_bytes = self
            .estimated_encoded_bytes
            .saturating_add(work.estimated_encoded_bytes);
        self.encoding_nanos = self.encoding_nanos.saturating_add(work.encoding_nanos);
        self.map_entries_hashed = self
            .map_entries_hashed
            .saturating_add(work.map_entries_hashed);
    }
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
    in_flight_task: Option<(FlushGeneration, PipeTaskHandle)>,
    retry_pending: Option<PreparedFlush>,
    abandoned_generations: BTreeSet<FlushGeneration>,
    last_error: Option<String>,
    retry_attempt: u32,
    retry_not_before: Option<Instant>,
    retry_policy: RetryPolicy,
    counters: PersistenceCounters,
    scan_metrics: PersistenceScanMetrics,
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
            in_flight_task: None,
            retry_pending: None,
            abandoned_generations: BTreeSet::new(),
            last_error: None,
            retry_attempt: 0,
            retry_not_before: None,
            retry_policy,
            counters: PersistenceCounters::default(),
            scan_metrics: PersistenceScanMetrics::default(),
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

    #[doc(hidden)]
    pub fn track_loaded_scanned<D>(
        &mut self,
        loaded: LoadedScannedDocument<D>,
    ) -> Result<Tracked<D>, PersistenceError>
    where
        D: MongoScan,
    {
        let (loaded, baseline) = loaded.into_parts();
        let LoadedDocument {
            version,
            updated_at_ms,
            value,
        } = loaded;
        let key = MongoDocumentKey::for_document::<D>(value.id())?;
        if self.documents.contains_key(&key) {
            return Err(PersistenceError::DuplicateDocument(key));
        }
        self.documents.insert(
            key,
            DocumentState {
                baseline,
                cursor: ScanCursor::default(),
                acknowledged_mutation_epoch: Some(0),
                scanning_mutation_epoch: None,
                scanning_changed: false,
                version,
                updated_at_ms,
                create_mode: None,
                rejection: None,
                conflict_policy: D::CONFLICT_POLICY,
                conflict: None,
            },
        );
        Ok(Tracked::clean(value))
    }

    #[doc(hidden)]
    pub fn track_loaded_scanned_many<D>(
        &mut self,
        loaded: Vec<LoadedScannedDocument<D>>,
    ) -> Result<Vec<Tracked<D>>, PersistenceError>
    where
        D: MongoScan,
    {
        let mut keys = BTreeSet::new();
        let mut pending = Vec::with_capacity(loaded.len());
        for loaded in loaded {
            let (loaded, baseline) = loaded.into_parts();
            let LoadedDocument {
                version,
                updated_at_ms,
                value,
            } = loaded;
            let key = MongoDocumentKey::for_document::<D>(value.id())?;
            if self.documents.contains_key(&key) || !keys.insert(key.clone()) {
                return Err(PersistenceError::DuplicateDocument(key));
            }
            pending.push((
                key,
                DocumentState {
                    baseline,
                    cursor: ScanCursor::default(),
                    acknowledged_mutation_epoch: Some(0),
                    scanning_mutation_epoch: None,
                    scanning_changed: false,
                    version,
                    updated_at_ms,
                    create_mode: None,
                    rejection: None,
                    conflict_policy: D::CONFLICT_POLICY,
                    conflict: None,
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
                    scanning_changed: false,
                    version: meta.version,
                    updated_at_ms: meta.updated_at_ms,
                    create_mode: None,
                    rejection: None,
                    conflict_policy: D::CONFLICT_POLICY,
                    conflict: None,
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
                scanning_changed: false,
                version: meta.version,
                updated_at_ms: meta.updated_at_ms,
                create_mode,
                rejection: None,
                conflict_policy: D::CONFLICT_POLICY,
                conflict: None,
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
        if self.has_blocking_conflict() {
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
        if document.conflict.is_some() {
            return Err(PersistenceError::DocumentConflictPending(key));
        }
        if document.rejection.is_some() {
            return Err(PersistenceError::DocumentRejectionPending(key));
        }
        self.documents.remove(&key);
        self.clear_last_error_if_recovered();
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
        let conflicted = state.conflict.is_some();
        Ok(!in_flight
            && !conflicted
            && state.create_mode.is_none()
            && state.rejection.is_none()
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

    /// Scans registered documents and prepares the next two-phase flush.
    ///
    /// A BSON encoding or diff error is isolated to the document that caused
    /// it. The document keeps its acknowledged baseline, records a rejection,
    /// and is retried after its tracked mutation epoch changes; other documents
    /// visited in the same pass can still be flushed. Coordinator invariants
    /// and errors returned directly by `visit` remain fail-fast.
    pub fn prepare<F>(
        &mut self,
        budget: ScanBudget,
        visit: F,
    ) -> Result<PreparedFlush, PersistenceError>
    where
        F: FnOnce(&mut MongoPreparation<'_>) -> Result<(), PersistenceError>,
    {
        self.prepare_with_document_failure_mode(budget, visit, false)
    }

    fn prepare_with_document_failure_mode<F>(
        &mut self,
        budget: ScanBudget,
        visit: F,
        continue_after_document_failures: bool,
    ) -> Result<PreparedFlush, PersistenceError>
    where
        F: FnOnce(&mut MongoPreparation<'_>) -> Result<(), PersistenceError>,
    {
        if self.in_flight.is_some() {
            return Err(PersistenceError::FlushInFlight);
        }
        if !continue_after_document_failures && self.has_blocking_conflict() {
            return Err(PersistenceError::ConflictBlocked);
        }
        if let Some(prepared) = &self.retry_pending {
            return Ok(prepared.clone());
        }
        let generation = FlushGeneration {
            activation_epoch: self.activation_epoch,
            sequence: self.next_sequence,
        };
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(PersistenceError::GenerationOverflow)?;
        let mut preparation = MongoPreparation::new(
            &self.documents,
            generation,
            budget,
            continue_after_document_failures,
        );
        visit(&mut preparation)?;
        self.counters.scans = self.counters.scans.saturating_add(preparation.scans);
        self.counters.changed_paths = self
            .counters
            .changed_paths
            .saturating_add(preparation.changed_paths);
        self.scan_metrics
            .record_work(preparation.budget.work_metrics());
        let (prepared, rejections) = preparation.finish();
        if !rejections.is_empty() {
            self.counters.failed_documents = self
                .counters
                .failed_documents
                .saturating_add(rejections.len() as u64);
            for (key, rejection) in rejections {
                self.last_error = Some(rejection.error.clone());
                self.documents
                    .get_mut(&key)
                    .ok_or_else(|| PersistenceError::UnknownDocument(key))?
                    .rejection = Some(rejection);
            }
        }
        Ok(prepared)
    }

    pub fn begin_flush(&mut self, commit: InFlightCommit) -> Result<(), PersistenceError> {
        self.validate_generation(commit.generation)?;
        if self.in_flight.is_some() {
            return Err(PersistenceError::FlushInFlight);
        }
        debug_assert!(self.in_flight_task.is_none());
        if self
            .retry_pending
            .as_ref()
            .is_some_and(|pending| pending.commit.generation == commit.generation)
        {
            self.retry_pending = None;
        }
        self.in_flight = Some(commit);
        Ok(())
    }

    pub(super) fn register_in_flight_task(
        &mut self,
        generation: FlushGeneration,
        task: PipeTaskHandle,
    ) -> Result<(), PersistenceError> {
        let Some(expected) = self.in_flight.as_ref() else {
            task.abort();
            return Err(PersistenceError::NoFlushInFlight);
        };
        if expected.generation != generation {
            task.abort();
            return Err(PersistenceError::ForeignGeneration {
                expected: expected.generation,
                actual: generation,
            });
        }
        self.in_flight_task = Some((generation, task));
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
        self.clear_last_error_if_recovered();
        Ok(report)
    }

    pub fn complete(
        &mut self,
        generation: FlushGeneration,
        outcome: FlushOutcome,
    ) -> Result<PersistenceReport, PersistenceError> {
        self.validate_generation(generation)?;
        if self.abandoned_generations.contains(&generation) {
            return Err(PersistenceError::AbandonedGeneration(generation));
        }
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

        self.clear_in_flight_task(generation);
        let commit = self.in_flight.take().expect("checked in-flight commit");
        let InFlightCommit {
            generation: _,
            document_commits,
            clean_commits,
            mut writes,
        } = commit;
        let mut report = PersistenceReport::default();
        self.apply_clean_commits(clean_commits, &mut report)?;
        self.counters.attempted_documents = self
            .counters
            .attempted_documents
            .saturating_add(document_commits.len() as u64);
        let mut retry_commits = BTreeMap::new();
        let mut retry_writes = BTreeMap::new();

        for (token, document_commit) in document_commits {
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
                    let false_positive = state.apply_commit_metadata(
                        document_commit.mutation_epoch,
                        document_commit.scan_complete,
                        document_commit.sweep_complete,
                        document_commit.changed,
                    );
                    if false_positive {
                        self.scan_metrics.false_positive_scans =
                            self.scan_metrics.false_positive_scans.saturating_add(1);
                    }
                    state.version = *new_version;
                    state.updated_at_ms = *updated_at_ms;
                    state.create_mode = None;
                    state.rejection = None;
                    state.conflict = None;
                    report.applied += 1;
                    self.counters.applied_documents =
                        self.counters.applied_documents.saturating_add(1);
                }
                DocumentWriteOutcome::VersionConflict { expected_version } => {
                    state.conflict = Some(PersistenceConflict {
                        key: document_commit.key,
                        expected_version: *expected_version,
                        kind: PersistenceConflictKind::VersionConflict,
                        policy: state.conflict_policy,
                    });
                    report.conflicts += 1;
                    self.counters.conflicts = self.counters.conflicts.saturating_add(1);
                }
                DocumentWriteOutcome::NotFound { expected_version } => {
                    state.conflict = Some(PersistenceConflict {
                        key: document_commit.key,
                        expected_version: *expected_version,
                        kind: PersistenceConflictKind::NotFound,
                        policy: state.conflict_policy,
                    });
                    report.conflicts += 1;
                    self.counters.conflicts = self.counters.conflicts.saturating_add(1);
                }
                DocumentWriteOutcome::Failed { error }
                    if error.recovery() == MongoStoreErrorRecovery::ReprepareAfterMutation =>
                {
                    let error = error.to_string();
                    self.last_error = Some(error.clone());
                    state.rejection = Some(DocumentRejection {
                        mutation_epoch: document_commit.mutation_epoch,
                        error,
                    });
                    report.failed += 1;
                    self.counters.failed_documents =
                        self.counters.failed_documents.saturating_add(1);
                }
                DocumentWriteOutcome::Failed { error } => {
                    self.last_error = Some(error.to_string());
                    report.failed += 1;
                    self.counters.failed_documents =
                        self.counters.failed_documents.saturating_add(1);
                    retry_writes.insert(
                        token,
                        writes
                            .remove(&token)
                            .expect("in-flight write matches commit"),
                    );
                    retry_commits.insert(token, document_commit);
                }
                DocumentWriteOutcome::NotAttempted => {
                    self.last_error = Some("document write was not attempted".to_owned());
                    report.failed += 1;
                    self.counters.failed_documents =
                        self.counters.failed_documents.saturating_add(1);
                    retry_writes.insert(
                        token,
                        writes
                            .remove(&token)
                            .expect("in-flight write matches commit"),
                    );
                    retry_commits.insert(token, document_commit);
                }
            }
        }
        if !retry_commits.is_empty() {
            let scan_complete = retry_commits
                .values()
                .all(|document| document.scan_complete);
            let request_writes = retry_writes.values().cloned().collect();
            self.retry_pending = Some(PreparedFlush {
                request: Some(FlushRequest {
                    generation,
                    writes: request_writes,
                }),
                commit: InFlightCommit {
                    generation,
                    document_commits: retry_commits,
                    clean_commits: Vec::new(),
                    writes: retry_writes,
                },
                scan_complete,
            });
            self.schedule_retry();
        } else {
            self.retry_attempt = 0;
            self.retry_not_before = None;
            self.clear_last_error_if_recovered();
        }
        Ok(report)
    }

    pub fn dispatch_failed(
        &mut self,
        generation: FlushGeneration,
        error: impl Into<String>,
    ) -> Result<(), PersistenceError> {
        self.validate_generation(generation)?;
        if self.abandoned_generations.contains(&generation) {
            return Err(PersistenceError::AbandonedGeneration(generation));
        }
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
        self.clear_in_flight_task(generation);
        let commit = self.in_flight.take().expect("checked in-flight commit");
        let writes = commit.writes.values().cloned().collect::<Vec<_>>();
        let scan_complete = commit
            .document_commits
            .values()
            .chain(commit.clean_commits.iter())
            .all(|document| document.scan_complete);
        self.retry_pending = Some(PreparedFlush {
            request: (!writes.is_empty()).then_some(FlushRequest { generation, writes }),
            commit,
            scan_complete,
        });
        self.last_error = Some(error.into());
        self.schedule_retry();
        Ok(())
    }

    pub fn dispatch_rejected(
        &mut self,
        generation: FlushGeneration,
        error: impl Into<String>,
    ) -> Result<PersistenceReport, PersistenceError> {
        self.validate_generation(generation)?;
        if self.abandoned_generations.contains(&generation) {
            return Err(PersistenceError::AbandonedGeneration(generation));
        }
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
        self.clear_in_flight_task(generation);
        let commit = self.in_flight.take().expect("checked in-flight commit");
        let error = error.into();
        let mut report = PersistenceReport::default();
        self.apply_clean_commits(commit.clean_commits, &mut report)?;
        report.failed = commit.document_commits.len();
        self.counters.attempted_documents = self
            .counters
            .attempted_documents
            .saturating_add(commit.document_commits.len() as u64);
        self.counters.failed_documents = self
            .counters
            .failed_documents
            .saturating_add(commit.document_commits.len() as u64);
        for document_commit in commit.document_commits.into_values() {
            let state = self
                .documents
                .get_mut(&document_commit.key)
                .ok_or_else(|| PersistenceError::UnknownDocument(document_commit.key.clone()))?;
            state.rejection = Some(DocumentRejection {
                mutation_epoch: document_commit.mutation_epoch,
                error: error.clone(),
            });
        }
        self.retry_attempt = 0;
        self.retry_not_before = None;
        self.last_error = Some(error);
        Ok(report)
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
                .filter(|document| {
                    document.create_mode.is_some()
                        || document.rejection.is_some()
                        || document.conflict.is_some()
                })
                .count()
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

    pub const fn scan_metrics(&self) -> &PersistenceScanMetrics {
        &self.scan_metrics
    }

    pub fn document_meta(&self, key: &MongoDocumentKey) -> Option<(i64, i64)> {
        self.documents
            .get(key)
            .map(|state| (state.version, state.updated_at_ms))
    }

    fn take_in_flight_task(
        &mut self,
        generation: FlushGeneration,
    ) -> Option<(FlushGeneration, PipeTaskHandle)> {
        if self
            .in_flight_task
            .as_ref()
            .is_some_and(|(registered, _)| *registered == generation)
        {
            self.in_flight_task.take()
        } else {
            None
        }
    }

    fn clear_in_flight_task(&mut self, generation: FlushGeneration) {
        drop(self.take_in_flight_task(generation));
    }

    pub(super) fn consume_abandoned_generation(&mut self, generation: FlushGeneration) -> bool {
        self.abandoned_generations.remove(&generation)
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
            let false_positive = state.apply_commit_metadata(
                commit.mutation_epoch,
                commit.scan_complete,
                commit.sweep_complete,
                commit.changed,
            );
            if false_positive {
                self.scan_metrics.false_positive_scans =
                    self.scan_metrics.false_positive_scans.saturating_add(1);
            }
            if commit.scan_complete {
                state.rejection = None;
            }
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
    rejections: BTreeMap<MongoDocumentKey, DocumentRejection>,
    scans: u64,
    changed_paths: u64,
    scan_complete: bool,
    continue_after_document_failures: bool,
}

mod preparation;

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
    #[error("no exact persistence retry is pending")]
    NoRetryPending,
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
    #[error("document is not conflicted: {0:?}")]
    DocumentNotConflicted(MongoDocumentKey),
    #[error("document conflict must be resolved explicitly before detaching: {0:?}")]
    DocumentConflictPending(MongoDocumentKey),
    #[error("document is not rejected: {0:?}")]
    DocumentNotRejected(MongoDocumentKey),
    #[error("document rejection must be resolved explicitly before detaching: {0:?}")]
    DocumentRejectionPending(MongoDocumentKey),
    #[error("persistence generation was explicitly abandoned: {0:?}")]
    AbandonedGeneration(FlushGeneration),
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
mod tests;
