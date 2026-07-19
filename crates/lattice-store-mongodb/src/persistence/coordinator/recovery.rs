//! Conflict, rejection, and ambiguous-outcome intervention.

use crate::document::{LoadedDocument, tracked::Tracked};
use crate::persistence::request::{FlushGeneration, InFlightCommit};
use crate::persistence::types::MongoDocumentKey;
use crate::scan::{MongoScan, ScanCursor};

use super::{
    ConflictPolicy, DocumentState, MongoPersistenceCoordinator, PersistenceConflict,
    PersistenceConflictKind, PersistenceError, PersistenceReport,
};

impl MongoPersistenceCoordinator {
    /// Explicitly discards a conflicted registration after the application has
    /// decided the missing or remotely changed document is no longer part of
    /// this actor's persistent state.
    pub fn detach_conflicted<D>(&mut self, id: &D::Id) -> Result<(), PersistenceError>
    where
        D: MongoScan,
    {
        if self.in_flight.is_some() {
            return Err(PersistenceError::FlushInFlight);
        }
        let key = MongoDocumentKey::for_document::<D>(id)?;
        let state = self
            .documents
            .get(&key)
            .ok_or_else(|| PersistenceError::UnknownDocument(key.clone()))?;
        if state.conflict.is_none() {
            return Err(PersistenceError::DocumentNotConflicted(key));
        }
        self.documents.remove(&key);
        self.clear_last_error_if_recovered();
        Ok(())
    }

    /// Replaces a conflicted registration with freshly loaded remote state.
    ///
    /// The returned tracked value starts at mutation epoch zero and must
    /// replace the stale value held by the actor. The old baseline, cursor,
    /// pending mutation metadata, and conflict are discarded atomically only
    /// after the replacement baseline has been captured successfully.
    pub fn resolve_conflict_with_loaded<D>(
        &mut self,
        loaded: LoadedDocument<D>,
    ) -> Result<Tracked<D>, PersistenceError>
    where
        D: MongoScan,
    {
        if self.in_flight.is_some() {
            return Err(PersistenceError::FlushInFlight);
        }
        let (value, meta) = loaded.split();
        let key = MongoDocumentKey::for_document::<D>(value.id())?;
        let state = self
            .documents
            .get(&key)
            .ok_or_else(|| PersistenceError::UnknownDocument(key.clone()))?;
        if state.conflict.is_none() {
            return Err(PersistenceError::DocumentNotConflicted(key.clone()));
        }
        let baseline = value.capture()?;
        self.documents.insert(
            key,
            DocumentState {
                baseline,
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
        );
        self.clear_last_error_if_recovered();
        Ok(Tracked::clean(value))
    }

    /// Clears a definitive document rejection so the current business value is
    /// scanned again even when its mutation epoch did not change.
    pub fn retry_rejected<D>(&mut self, id: &D::Id) -> Result<(), PersistenceError>
    where
        D: MongoScan,
    {
        if self.in_flight.is_some() {
            return Err(PersistenceError::FlushInFlight);
        }
        let key = MongoDocumentKey::for_document::<D>(id)?;
        let state = self
            .documents
            .get_mut(&key)
            .ok_or_else(|| PersistenceError::UnknownDocument(key.clone()))?;
        if state.rejection.take().is_none() {
            return Err(PersistenceError::DocumentNotRejected(key));
        }
        self.clear_last_error_if_recovered();
        Ok(())
    }

    /// Explicitly discards a rejected registration, including a rejected
    /// create that would otherwise remain `CreatePending`.
    pub fn detach_rejected<D>(&mut self, id: &D::Id) -> Result<(), PersistenceError>
    where
        D: MongoScan,
    {
        if self.in_flight.is_some() {
            return Err(PersistenceError::FlushInFlight);
        }
        let key = MongoDocumentKey::for_document::<D>(id)?;
        let state = self
            .documents
            .get(&key)
            .ok_or_else(|| PersistenceError::UnknownDocument(key.clone()))?;
        if state.rejection.is_none() {
            return Err(PersistenceError::DocumentNotRejected(key));
        }
        self.documents.remove(&key);
        self.clear_last_error_if_recovered();
        Ok(())
    }

    /// Replaces a rejected registration with freshly loaded remote state.
    pub fn replace_rejected_with_loaded<D>(
        &mut self,
        loaded: LoadedDocument<D>,
    ) -> Result<Tracked<D>, PersistenceError>
    where
        D: MongoScan,
    {
        if self.in_flight.is_some() {
            return Err(PersistenceError::FlushInFlight);
        }
        let (value, meta) = loaded.split();
        let key = MongoDocumentKey::for_document::<D>(value.id())?;
        let state = self
            .documents
            .get(&key)
            .ok_or_else(|| PersistenceError::UnknownDocument(key.clone()))?;
        if state.rejection.is_none() {
            return Err(PersistenceError::DocumentNotRejected(key.clone()));
        }
        let baseline = value.capture()?;
        self.documents.insert(
            key,
            DocumentState {
                baseline,
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
        );
        self.clear_last_error_if_recovered();
        Ok(Tracked::clean(value))
    }

    /// Stops retrying an exact write whose outcome can no longer be resolved
    /// automatically and converts every affected document into an explicit
    /// `OutcomeUnknown` conflict.
    pub fn abort_retry_as_unknown(
        &mut self,
        reason: impl Into<String>,
    ) -> Result<PersistenceReport, PersistenceError> {
        if self.in_flight.is_some() {
            return Err(PersistenceError::FlushInFlight);
        }
        let prepared = self
            .retry_pending
            .take()
            .ok_or(PersistenceError::NoRetryPending)?;
        self.mark_commit_outcome_unknown(prepared.commit, reason.into())
    }

    /// Cancels actor-dispatched work and marks its outcomes as unknown.
    pub fn abort_in_flight_as_unknown(
        &mut self,
        generation: FlushGeneration,
        reason: impl Into<String>,
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
        if let Some((_, task)) = self.take_in_flight_task(generation) {
            task.abort();
        }
        self.abandoned_generations.insert(generation);
        let commit = self.in_flight.take().expect("checked in-flight commit");
        self.mark_commit_outcome_unknown(commit, reason.into())
    }

    /// Returns the first conflict in stable document-key order.
    pub fn conflict(&self) -> Option<&PersistenceConflict> {
        self.conflicts().next()
    }

    pub fn conflicts(&self) -> impl Iterator<Item = &PersistenceConflict> {
        self.documents
            .values()
            .filter_map(|document| document.conflict.as_ref())
    }

    pub fn blocking_conflict(&self) -> Option<&PersistenceConflict> {
        self.conflicts()
            .find(|conflict| conflict.policy == ConflictPolicy::BlockCoordinator)
    }

    pub fn document_conflict(&self, key: &MongoDocumentKey) -> Option<&PersistenceConflict> {
        self.documents
            .get(key)
            .and_then(|document| document.conflict.as_ref())
    }

    pub fn document_rejection(&self, key: &MongoDocumentKey) -> Option<&str> {
        self.documents
            .get(key)
            .and_then(|state| state.rejection.as_ref())
            .map(|rejection| rejection.error.as_str())
    }

    pub(super) fn has_blocking_conflict(&self) -> bool {
        self.blocking_conflict().is_some()
    }

    pub(super) fn clear_last_error_if_recovered(&mut self) {
        if self.retry_pending.is_none()
            && self.in_flight.is_none()
            && !self
                .documents
                .values()
                .any(|document| document.rejection.is_some() || document.conflict.is_some())
        {
            self.last_error = None;
        }
    }

    fn mark_commit_outcome_unknown(
        &mut self,
        commit: InFlightCommit,
        reason: String,
    ) -> Result<PersistenceReport, PersistenceError> {
        let InFlightCommit {
            generation: _,
            document_commits,
            clean_commits,
            writes,
        } = commit;
        let mut report = PersistenceReport::default();
        self.apply_clean_commits(clean_commits, &mut report)?;
        for (token, document_commit) in document_commits {
            let expected_version = writes
                .get(&token)
                .expect("prepared write matches document commit")
                .expected_version;
            let state = self
                .documents
                .get_mut(&document_commit.key)
                .ok_or_else(|| PersistenceError::UnknownDocument(document_commit.key.clone()))?;
            state.conflict = Some(PersistenceConflict {
                key: document_commit.key,
                expected_version,
                kind: PersistenceConflictKind::OutcomeUnknown,
                policy: state.conflict_policy,
            });
            report.conflicts += 1;
        }
        self.counters.conflicts = self
            .counters
            .conflicts
            .saturating_add(report.conflicts as u64);
        self.retry_attempt = 0;
        self.retry_not_before = None;
        self.last_error = Some(reason);
        Ok(report)
    }
}
