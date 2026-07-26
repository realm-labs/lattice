//! Phase one of the two-phase protocol: preparation and flush dispatch.

use lattice_actor::context::PipeTaskHandle;

use crate::persistence::request::{FlushGeneration, InFlightCommit, PreparedFlush};
use crate::scan::ScanBudget;

use super::{MongoPersistenceCoordinator, MongoPreparation, PersistenceError};

impl MongoPersistenceCoordinator {
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

    pub(super) fn prepare_with_document_failure_mode<F>(
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

    pub(in crate::persistence) fn register_in_flight_task(
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

    pub(super) fn take_in_flight_task(
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

    pub(super) fn clear_in_flight_task(&mut self, generation: FlushGeneration) {
        drop(self.take_in_flight_task(generation));
    }

    pub(in crate::persistence) fn consume_abandoned_generation(
        &mut self,
        generation: FlushGeneration,
    ) -> bool {
        self.abandoned_generations.remove(&generation)
    }

    pub(super) fn validate_generation(
        &self,
        generation: FlushGeneration,
    ) -> Result<(), PersistenceError> {
        if generation.activation_epoch == self.activation_epoch {
            Ok(())
        } else {
            Err(PersistenceError::StaleActivation {
                expected: self.activation_epoch,
                actual: generation.activation_epoch,
            })
        }
    }
}
