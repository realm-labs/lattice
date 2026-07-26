//! Phase two of the two-phase protocol: reconciling storage outcomes.

use std::collections::{BTreeMap, BTreeSet};

use crate::error::MongoStoreErrorRecovery;
use crate::persistence::request::{
    DocumentCommit, DocumentWriteOutcome, FlushGeneration, FlushOutcome, FlushRequest,
    InFlightCommit, PreparedFlush,
};
use crate::scan::ScanCursor;

use super::{
    ConflictPolicy, DocumentPresence, DocumentRejection, MongoPersistenceCoordinator,
    PersistenceConflict, PersistenceConflictKind, PersistenceError, PersistenceReport,
};

impl MongoPersistenceCoordinator {
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

        for (token, mut document_commit) in document_commits {
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
                    if let Some(baseline) = document_commit.replacement_baseline.take() {
                        state.baseline = baseline;
                        state.cursor = ScanCursor::default();
                    } else {
                        state.cursor = state.baseline.apply(document_commit.scan)?;
                    }
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
                    state.presence = DocumentPresence::Persisted;
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
                DocumentWriteOutcome::Fenced {
                    expected_version,
                    observed_epoch,
                } => {
                    state.conflict = Some(PersistenceConflict {
                        key: document_commit.key,
                        expected_version: *expected_version,
                        kind: PersistenceConflictKind::Fenced {
                            observed_epoch: *observed_epoch,
                        },
                        policy: ConflictPolicy::BlockCoordinator,
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
            self.retry_wakeup_armed = None;
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
        self.retry_wakeup_armed = None;
        self.last_error = Some(error);
        Ok(report)
    }

    pub(super) fn apply_clean_commits(
        &mut self,
        commits: Vec<DocumentCommit>,
        report: &mut PersistenceReport,
    ) -> Result<(), PersistenceError> {
        for mut commit in commits {
            let state = self
                .documents
                .get_mut(&commit.key)
                .ok_or_else(|| PersistenceError::UnknownDocument(commit.key.clone()))?;
            if let Some(baseline) = commit.replacement_baseline.take() {
                state.baseline = baseline;
                state.cursor = ScanCursor::default();
            } else {
                state.cursor = state.baseline.apply(commit.scan)?;
            }
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
