//! Bounded, mailbox-independent persistence draining.

use std::time::{Duration, Instant};

use lattice_actor::error::ActorStopError;

use crate::document::set::MongoDocumentSet;
use crate::error::MongoStoreErrorRecovery;
use crate::persistence::request::{FlushRequest, PreparedFlush, PreparedWriteStore};
use crate::scan::ScanBudget;

use super::{MongoPersistenceCoordinator, MongoPreparation, PersistenceError, PersistenceReport};

/// Bounds synchronous scanning and asynchronous retries while draining an
/// actor's resident persistence state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MongoDrainOptions {
    /// Total time available to scan, retry, and acknowledge every dirty
    /// resident document.
    pub timeout: Duration,
    /// Maximum resident documents visited by one preparation pass.
    pub max_documents_per_pass: usize,
    /// Maximum complete business fields visited by one preparation pass.
    pub max_fields_per_pass: usize,
    /// Cooperative wall-clock bound for one preparation pass.
    pub max_scan_duration: Duration,
}

impl Default for MongoDrainOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            max_documents_per_pass: 1_024,
            max_fields_per_pass: 8_192,
            max_scan_duration: Duration::from_millis(50),
        }
    }
}

impl MongoDrainOptions {
    fn validate(self) -> Result<Self, MongoDrainError> {
        if self.timeout.is_zero() {
            return Err(MongoDrainError::InvalidOptions(
                "drain timeout must be greater than zero",
            ));
        }
        if self.max_documents_per_pass == 0 {
            return Err(MongoDrainError::InvalidOptions(
                "max_documents_per_pass must be greater than zero",
            ));
        }
        if self.max_fields_per_pass == 0 {
            return Err(MongoDrainError::InvalidOptions(
                "max_fields_per_pass must be greater than zero",
            ));
        }
        if self.max_scan_duration.is_zero() {
            return Err(MongoDrainError::InvalidOptions(
                "max_scan_duration must be greater than zero",
            ));
        }
        Ok(self)
    }

    fn scan_budget(self) -> ScanBudget {
        ScanBudget::new(
            self.max_documents_per_pass,
            self.max_fields_per_pass,
            self.max_scan_duration,
        )
    }
}

/// Successful work performed while bringing an actor's resident documents to
/// a fully scanned and acknowledged state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MongoDrainReport {
    pub preparation_passes: u64,
    pub flush_attempts: u64,
    pub retry_waits: u64,
    /// Number of actor-dispatched generations cancelled and replayed directly
    /// by this drain.
    pub recovered_in_flight: u64,
    pub persistence: PersistenceReport,
}

impl MongoDrainReport {
    fn record(&mut self, report: PersistenceReport) {
        self.persistence.clean = self.persistence.clean.saturating_add(report.clean);
        self.persistence.applied = self.persistence.applied.saturating_add(report.applied);
        self.persistence.failed = self.persistence.failed.saturating_add(report.failed);
        self.persistence.conflicts = self.persistence.conflicts.saturating_add(report.conflicts);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MongoDrainError {
    #[error("invalid MongoDB drain options: {0}")]
    InvalidOptions(&'static str),
    #[error("MongoDB persistence drain timed out after {timeout:?}: {last_error}")]
    Timeout {
        timeout: Duration,
        last_error: String,
    },
    #[error(
        "MongoDB persistence drain completed healthy writes but retained {conflicts} conflicted and {rejected} rejected document(s): {last_error}"
    )]
    DocumentFailures {
        conflicts: usize,
        rejected: usize,
        last_error: String,
        report: MongoDrainReport,
    },
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
}

impl From<MongoDrainError> for ActorStopError {
    fn from(error: MongoDrainError) -> Self {
        Self::new(error.to_string())
    }
}

impl MongoPersistenceCoordinator {
    /// Drains a generated document set without posting completion messages to
    /// the actor mailbox.
    ///
    /// This is the shutdown counterpart of `dispatch_prepared`: it awaits each
    /// store operation directly, applies the matching completion immediately,
    /// and repeats until every resident document has completed a field sweep.
    /// Lazy state that is already unloaded and clean is not loaded merely for
    /// shutdown. Terminal failures are quarantined per document while all
    /// healthy resident documents continue draining; the final error includes
    /// counts and the report for work that did succeed.
    pub async fn drain_set<S>(
        &mut self,
        store: &dyn PreparedWriteStore,
        documents: &S,
        options: MongoDrainOptions,
    ) -> Result<MongoDrainReport, MongoDrainError>
    where
        S: MongoDocumentSet + Sync,
    {
        self.drain(store, options, |preparation| {
            documents.scan_all(preparation)
        })
        .await
    }

    /// Drains documents enumerated by `visit` without using an actor mailbox.
    ///
    /// Callers that do not use [`MongoDocumentSet`] can provide the same
    /// scanning closure they pass to [`MongoPersistenceCoordinator::prepare`].
    pub async fn drain<F>(
        &mut self,
        store: &dyn PreparedWriteStore,
        options: MongoDrainOptions,
        mut visit: F,
    ) -> Result<MongoDrainReport, MongoDrainError>
    where
        F: FnMut(&mut MongoPreparation<'_>) -> Result<(), PersistenceError> + Send,
    {
        let options = options.validate()?;
        let started = Instant::now();
        let deadline =
            started
                .checked_add(options.timeout)
                .ok_or(MongoDrainError::InvalidOptions(
                    "drain timeout cannot be represented by the monotonic clock",
                ))?;
        let mut report = MongoDrainReport::default();
        // Work prepared before entering the drain may have enumerated only a
        // subset of the document set. Finish it exactly, then require one
        // drain-owned scan before declaring the actor clean.
        let mut require_fresh_scan = self.retry_pending.is_some();
        let mut recovered = self.take_over_in_flight_for_drain()?;
        if recovered.is_some() {
            report.recovered_in_flight = 1;
            require_fresh_scan = true;
        }

        loop {
            if recovered.is_none()
                && let Some(delay) = self.retry_delay()
            {
                let remaining = self.drain_remaining(deadline, options.timeout)?;
                if delay >= remaining {
                    tokio::time::sleep(remaining).await;
                    return Err(self.drain_timeout(options.timeout));
                }
                tokio::time::sleep(delay).await;
                report.retry_waits = report.retry_waits.saturating_add(1);
            }

            let prepared = match recovered.take() {
                Some(prepared) => prepared,
                None => self.prepare_with_document_failure_mode(
                    options.scan_budget(),
                    |preparation| visit(preparation),
                    true,
                )?,
            };
            report.preparation_passes = report.preparation_passes.saturating_add(1);
            let scan_complete = prepared.scan_complete;

            let Some(request) = prepared.request else {
                let batch = self.complete_clean(prepared.commit)?;
                report.record(batch);
                if scan_complete {
                    return if self.has_document_failures() {
                        Err(self.drain_document_failures(report))
                    } else {
                        Ok(report)
                    };
                }
                self.drain_remaining(deadline, options.timeout)?;
                tokio::task::yield_now().await;
                continue;
            };

            let generation = request.generation;
            self.begin_flush(prepared.commit)?;
            report.flush_attempts = report.flush_attempts.saturating_add(1);
            let remaining = self.drain_remaining(deadline, options.timeout)?;
            let outcome = match tokio::time::timeout(remaining, store.flush(request.writes)).await {
                Ok(outcome) => outcome,
                Err(_) => return Err(self.drain_timeout(options.timeout)),
            };

            match outcome {
                Ok(outcome) => {
                    let batch = self.complete(generation, outcome)?;
                    let has_failures = batch.failed > 0;
                    report.record(batch);
                    if !has_failures
                        && self.retry_pending.is_none()
                        && self.rejected_document_count() == 0
                        && self.conflict().is_none()
                    {
                        if require_fresh_scan {
                            require_fresh_scan = false;
                        } else if scan_complete {
                            return Ok(report);
                        }
                    }
                }
                Err(error)
                    if error.recovery() == MongoStoreErrorRecovery::ReprepareAfterMutation =>
                {
                    let batch = self.dispatch_rejected(generation, error.to_string())?;
                    report.record(batch);
                }
                Err(error) => {
                    self.dispatch_failed(generation, error.to_string())?;
                }
            }
        }
    }

    fn take_over_in_flight_for_drain(&mut self) -> Result<Option<PreparedFlush>, PersistenceError> {
        if self.in_flight.is_none() {
            return Ok(None);
        }
        let next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(PersistenceError::GenerationOverflow)?;
        let mut commit = self.in_flight.take().expect("checked in-flight commit");
        let abandoned = commit.generation;
        if let Some((_, task)) = self.take_in_flight_task(abandoned) {
            task.abort();
        }
        self.abandoned_generations.insert(abandoned);

        let generation = super::FlushGeneration {
            activation_epoch: self.activation_epoch,
            sequence: self.next_sequence,
        };
        self.next_sequence = next_sequence;
        commit.generation = generation;
        let writes = commit.writes.values().cloned().collect::<Vec<_>>();
        let scan_complete = commit
            .document_commits
            .values()
            .chain(commit.clean_commits.iter())
            .all(|document| document.scan_complete);
        Ok(Some(PreparedFlush {
            request: Some(FlushRequest { generation, writes }),
            commit,
            scan_complete,
        }))
    }

    fn rejected_document_count(&self) -> usize {
        self.documents
            .values()
            .filter(|document| document.rejection.is_some())
            .count()
    }

    fn drain_remaining(
        &self,
        deadline: Instant,
        timeout: Duration,
    ) -> Result<Duration, MongoDrainError> {
        deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| self.drain_timeout(timeout))
    }

    fn drain_timeout(&self, timeout: Duration) -> MongoDrainError {
        MongoDrainError::Timeout {
            timeout,
            last_error: self
                .last_error()
                .unwrap_or("drain deadline elapsed before persistence became clean")
                .to_owned(),
        }
    }

    fn has_document_failures(&self) -> bool {
        self.conflict().is_some() || self.rejected_document_count() > 0
    }

    fn drain_document_failures(&self, report: MongoDrainReport) -> MongoDrainError {
        MongoDrainError::DocumentFailures {
            conflicts: self.conflicts().count(),
            rejected: self.rejected_document_count(),
            last_error: self
                .last_error()
                .unwrap_or("one or more documents require explicit intervention")
                .to_owned(),
            report,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};

    use crate::document::LoadedDocument;
    use crate::document::tracked::Tracked;
    use crate::error::MongoStoreError;
    use crate::persistence::request::{
        DocumentWriteOutcome, FlushOutcome, PreparedDocumentWrite, PreparedWriteStore,
    };
    use crate::scan::ScanBudget;
    use crate::{MongoDocument, MongoDocumentSet, MongoScan};

    use super::{MongoDrainError, MongoDrainOptions};
    use crate::persistence::coordinator::{MongoPersistenceCoordinator, RetryPolicy};

    #[derive(Debug, Clone, Serialize, Deserialize, MongoDocument, MongoScan)]
    #[mongo(collection = "drain_documents")]
    struct DrainDocument {
        #[mongo(id)]
        id: u64,
        first: i64,
        second: i64,
    }

    #[derive(Debug, MongoDocumentSet)]
    #[mongo(id = u64)]
    struct DrainDocuments {
        document: Tracked<DrainDocument>,
    }

    #[derive(Clone, Copy)]
    enum StoreBehavior {
        Applied,
        Conflict,
        Rejected,
        FailFirst,
        Pending,
        MixedFailures,
        ConflictFirst,
        RejectedFirst,
    }

    #[derive(Clone)]
    struct TestStore {
        behavior: StoreBehavior,
        attempts: Arc<AtomicUsize>,
        operation_ids: Arc<Mutex<Vec<String>>>,
    }

    impl TestStore {
        fn new(behavior: StoreBehavior) -> Self {
            Self {
                behavior,
                attempts: Arc::new(AtomicUsize::new(0)),
                operation_ids: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl PreparedWriteStore for TestStore {
        async fn flush(
            &self,
            writes: Vec<PreparedDocumentWrite>,
        ) -> Result<FlushOutcome, MongoStoreError> {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            self.operation_ids
                .lock()
                .expect("operation ID mutex poisoned")
                .extend(writes.iter().map(|write| write.operation_id.clone()));
            match self.behavior {
                StoreBehavior::Rejected => Err(MongoStoreError::rejected("document rejected")),
                StoreBehavior::RejectedFirst if attempt == 0 => {
                    Err(MongoStoreError::rejected("first document rejected"))
                }
                StoreBehavior::Pending => std::future::pending().await,
                StoreBehavior::MixedFailures if attempt == 0 => Ok(FlushOutcome {
                    documents: writes
                        .into_iter()
                        .enumerate()
                        .map(|(index, write)| {
                            let error = if index == 0 {
                                MongoStoreError::rejected("first document rejected")
                            } else {
                                MongoStoreError::new("second document temporarily failed")
                            };
                            (write.token, DocumentWriteOutcome::Failed { error })
                        })
                        .collect(),
                }),
                StoreBehavior::FailFirst if attempt == 0 => {
                    Err(MongoStoreError::new("temporary failure"))
                }
                StoreBehavior::ConflictFirst if attempt == 0 => Ok(FlushOutcome {
                    documents: writes
                        .into_iter()
                        .map(|write| {
                            (
                                write.token,
                                DocumentWriteOutcome::VersionConflict {
                                    expected_version: write.expected_version,
                                },
                            )
                        })
                        .collect::<BTreeMap<_, _>>(),
                }),
                StoreBehavior::Applied
                | StoreBehavior::FailFirst
                | StoreBehavior::MixedFailures
                | StoreBehavior::RejectedFirst => Ok(FlushOutcome {
                    documents: writes
                        .into_iter()
                        .map(|write| {
                            (
                                write.token,
                                DocumentWriteOutcome::Applied {
                                    previous_version: write.expected_version,
                                    new_version: write.expected_version + 1,
                                    updated_at_ms: 99,
                                },
                            )
                        })
                        .collect::<BTreeMap<_, _>>(),
                }),
                StoreBehavior::Conflict => Ok(FlushOutcome {
                    documents: writes
                        .into_iter()
                        .map(|write| {
                            (
                                write.token,
                                DocumentWriteOutcome::VersionConflict {
                                    expected_version: write.expected_version,
                                },
                            )
                        })
                        .collect::<BTreeMap<_, _>>(),
                }),
                StoreBehavior::ConflictFirst => Ok(FlushOutcome {
                    documents: writes
                        .into_iter()
                        .map(|write| {
                            (
                                write.token,
                                DocumentWriteOutcome::Applied {
                                    previous_version: write.expected_version,
                                    new_version: write.expected_version + 1,
                                    updated_at_ms: 99,
                                },
                            )
                        })
                        .collect::<BTreeMap<_, _>>(),
                }),
            }
        }
    }

    fn fixture(retry_policy: RetryPolicy) -> (MongoPersistenceCoordinator, DrainDocuments) {
        let mut coordinator = MongoPersistenceCoordinator::with_retry_policy(7, retry_policy);
        let document = coordinator
            .track_loaded(LoadedDocument {
                value: DrainDocument {
                    id: 42,
                    first: 1,
                    second: 2,
                },
                version: 1,
                updated_at_ms: 10,
            })
            .expect("fixture should attach");
        (coordinator, DrainDocuments { document })
    }

    fn options() -> MongoDrainOptions {
        MongoDrainOptions {
            timeout: Duration::from_secs(1),
            max_documents_per_pass: 1,
            max_fields_per_pass: 1,
            max_scan_duration: Duration::from_secs(1),
        }
    }

    #[tokio::test]
    async fn drain_set_repeats_budgeted_scans_until_every_field_is_acknowledged() {
        let (mut coordinator, mut documents) = fixture(RetryPolicy::default());
        documents.document.write().first = 10;
        documents.document.write().second = 20;
        let store = TestStore::new(StoreBehavior::Applied);

        let report = coordinator
            .drain_set(&store, &documents, options())
            .await
            .expect("drain should complete");

        assert_eq!(report.preparation_passes, 2);
        assert_eq!(report.flush_attempts, 2);
        assert_eq!(report.persistence.applied, 2);
        assert_eq!(store.attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn drain_retries_the_exact_operation_after_a_transient_failure() {
        let retry_policy = RetryPolicy {
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            max_exponent: 0,
        };
        let (mut coordinator, mut documents) = fixture(retry_policy);
        documents.document.write().first = 10;
        let store = TestStore::new(StoreBehavior::FailFirst);

        let report = coordinator
            .drain_set(&store, &documents, MongoDrainOptions::default())
            .await
            .expect("transient failure should be retried");

        assert_eq!(report.flush_attempts, 2);
        assert_eq!(report.retry_waits, 1);
        let operation_ids = store
            .operation_ids
            .lock()
            .expect("operation ID mutex poisoned");
        assert_eq!(operation_ids.len(), 2);
        assert_eq!(operation_ids[0], operation_ids[1]);
    }

    #[tokio::test]
    async fn drain_retains_conflicts_and_definitive_rejections() {
        let (mut conflicted, mut conflict_documents) = fixture(RetryPolicy::default());
        conflict_documents.document.write().first = 10;
        let conflict = conflicted
            .drain_set(
                &TestStore::new(StoreBehavior::Conflict),
                &conflict_documents,
                MongoDrainOptions::default(),
            )
            .await
            .expect_err("conflict must fail the drain");
        assert!(matches!(
            conflict,
            MongoDrainError::DocumentFailures {
                conflicts: 1,
                rejected: 0,
                ..
            }
        ));
        assert_eq!(conflicted.conflicts().count(), 1);

        let (mut rejected, mut rejected_documents) = fixture(RetryPolicy::default());
        rejected_documents.document.write().first = 10;
        let rejection = rejected
            .drain_set(
                &TestStore::new(StoreBehavior::Rejected),
                &rejected_documents,
                MongoDrainOptions::default(),
            )
            .await
            .expect_err("definitive rejection must fail the drain");
        assert!(matches!(
            rejection,
            MongoDrainError::DocumentFailures {
                conflicts: 0,
                rejected: 1,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn timed_out_drain_retains_in_flight_work_for_the_next_stop_attempt() {
        let (mut coordinator, mut documents) = fixture(RetryPolicy::default());
        documents.document.write().first = 10;
        let timeout = coordinator
            .drain_set(
                &TestStore::new(StoreBehavior::Pending),
                &documents,
                MongoDrainOptions {
                    timeout: Duration::from_millis(10),
                    ..MongoDrainOptions::default()
                },
            )
            .await
            .expect_err("pending store should exhaust the drain deadline");
        assert!(matches!(timeout, MongoDrainError::Timeout { .. }));
        assert!(coordinator.has_in_flight());

        let report = coordinator
            .drain_set(
                &TestStore::new(StoreBehavior::Applied),
                &documents,
                MongoDrainOptions::default(),
            )
            .await
            .expect("the next stop attempt should replay retained work");
        assert_eq!(report.recovered_in_flight, 1);
        assert_eq!(report.persistence.applied, 1);
        assert!(!coordinator.has_in_flight());
    }

    #[tokio::test]
    async fn recovered_subset_is_followed_by_a_fresh_full_drain_scan() {
        let mut coordinator = MongoPersistenceCoordinator::new(9);
        let mut first = coordinator
            .track_loaded(LoadedDocument {
                value: DrainDocument {
                    id: 1,
                    first: 1,
                    second: 1,
                },
                version: 1,
                updated_at_ms: 1,
            })
            .unwrap();
        let mut second = coordinator
            .track_loaded(LoadedDocument {
                value: DrainDocument {
                    id: 2,
                    first: 2,
                    second: 2,
                },
                version: 1,
                updated_at_ms: 1,
            })
            .unwrap();
        first.write().first = 10;
        second.write().first = 20;

        let partial = coordinator
            .prepare(ScanBudget::generous(), |preparation| {
                preparation.scan_tracked(&first)
            })
            .unwrap();
        coordinator.begin_flush(partial.commit).unwrap();

        let report = coordinator
            .drain(
                &TestStore::new(StoreBehavior::Applied),
                MongoDrainOptions::default(),
                |preparation| {
                    preparation.scan_tracked(&first)?;
                    preparation.scan_tracked(&second)
                },
            )
            .await
            .expect("drain should scan documents omitted by inherited work");

        assert_eq!(report.recovered_in_flight, 1);
        assert_eq!(report.flush_attempts, 2);
        assert_eq!(report.persistence.applied, 2);
    }

    #[tokio::test]
    async fn successful_retry_does_not_hide_a_rejection_from_the_same_batch() {
        let retry_policy = RetryPolicy {
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            max_exponent: 0,
        };
        let mut coordinator = MongoPersistenceCoordinator::with_retry_policy(10, retry_policy);
        let mut first = coordinator
            .track_loaded(LoadedDocument {
                value: DrainDocument {
                    id: 1,
                    first: 1,
                    second: 1,
                },
                version: 1,
                updated_at_ms: 1,
            })
            .unwrap();
        let mut second = coordinator
            .track_loaded(LoadedDocument {
                value: DrainDocument {
                    id: 2,
                    first: 2,
                    second: 2,
                },
                version: 1,
                updated_at_ms: 1,
            })
            .unwrap();
        first.write().first = 10;
        second.write().first = 20;

        let error = coordinator
            .drain(
                &TestStore::new(StoreBehavior::MixedFailures),
                MongoDrainOptions::default(),
                |preparation| {
                    preparation.scan_tracked(&first)?;
                    preparation.scan_tracked(&second)
                },
            )
            .await
            .expect_err("one definitive rejection must keep the drain failed");

        assert!(matches!(
            error,
            MongoDrainError::DocumentFailures { rejected: 1, .. }
        ));
    }

    #[tokio::test]
    async fn terminal_document_failure_does_not_block_later_budgeted_documents() {
        for (behavior, expected_conflicts, expected_rejected) in [
            (StoreBehavior::ConflictFirst, 1, 0),
            (StoreBehavior::RejectedFirst, 0, 1),
        ] {
            let mut coordinator = MongoPersistenceCoordinator::new(12);
            let mut first = coordinator
                .track_loaded(LoadedDocument {
                    value: DrainDocument {
                        id: 1,
                        first: 1,
                        second: 1,
                    },
                    version: 1,
                    updated_at_ms: 1,
                })
                .unwrap();
            let mut second = coordinator
                .track_loaded(LoadedDocument {
                    value: DrainDocument {
                        id: 2,
                        first: 2,
                        second: 2,
                    },
                    version: 1,
                    updated_at_ms: 1,
                })
                .unwrap();
            first.write().first = 10;
            second.write().first = 20;
            let store = TestStore::new(behavior);

            let error = coordinator
                .drain(
                    &store,
                    MongoDrainOptions {
                        max_documents_per_pass: 1,
                        ..MongoDrainOptions::default()
                    },
                    |preparation| {
                        preparation.scan_tracked(&first)?;
                        preparation.scan_tracked(&second)
                    },
                )
                .await
                .expect_err("terminal document failure should be reported after healthy writes");
            let MongoDrainError::DocumentFailures {
                conflicts,
                rejected,
                report,
                ..
            } = error
            else {
                panic!("expected aggregated document failures");
            };

            assert_eq!(conflicts, expected_conflicts);
            assert_eq!(rejected, expected_rejected);
            assert_eq!(report.persistence.applied, 1);
            assert_eq!(store.attempts.load(Ordering::SeqCst), 2);
        }
    }
}
