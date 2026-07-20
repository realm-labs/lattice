//! Lattice actor integration for prepared MongoDB flushes.

use std::sync::Arc;
use std::time::Duration;

use lattice_actor::context::ActorContext;
use lattice_actor::error::PipeToSelfError;
use lattice_actor::traits::{Actor, Handler, Message};

use crate::error::{MongoStoreError, MongoStoreErrorRecovery};

use super::coordinator::{MongoPersistenceCoordinator, PersistenceError, PersistenceReport};
use super::request::{FlushGeneration, FlushOutcome, PreparedFlush, PreparedWriteStore};

/// Completion posted back to the owning actor after a prepared flush.
#[derive(Debug)]
pub struct MongoFlushCompleted {
    pub generation: FlushGeneration,
    pub outcome: Result<FlushOutcome, MongoStoreError>,
}

impl Message for MongoFlushCompleted {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceStatus {
    Clean,
    Incomplete,
    InFlight,
    Backoff(Duration),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionStatus {
    Applied(PersistenceReport),
    RetryScheduled,
    /// The generation was explicitly converted to `OutcomeUnknown`; its late
    /// asynchronous completion must not mutate coordinator state.
    IgnoredAbandoned,
}

#[derive(Debug, thiserror::Error)]
pub enum ActorPersistenceError {
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
    #[error(transparent)]
    Pipe(#[from] PipeToSelfError),
}

impl MongoPersistenceCoordinator {
    /// Dispatches an already prepared two-phase flush through the actor's
    /// bounded `pipe_to_self` facility.
    pub fn dispatch_prepared<A>(
        &mut self,
        context: &mut ActorContext<A>,
        store: Arc<dyn PreparedWriteStore>,
        prepared: PreparedFlush,
    ) -> Result<PersistenceStatus, ActorPersistenceError>
    where
        A: Actor + Handler<MongoFlushCompleted>,
        <A as lattice_actor::traits::Actor>::Behavior:
            lattice_actor::state_machine::Accepts<MongoFlushCompleted>,
    {
        if let Some(delay) = self.retry_delay() {
            return Ok(PersistenceStatus::Backoff(delay));
        }
        let scan_complete = prepared.scan_complete;
        let Some(request) = prepared.request else {
            self.complete_clean(prepared.commit)?;
            return Ok(if scan_complete {
                PersistenceStatus::Clean
            } else {
                PersistenceStatus::Incomplete
            });
        };

        let generation = request.generation;
        self.begin_flush(prepared.commit)?;
        let future = async move {
            let outcome = store.flush(request.writes).await;
            MongoFlushCompleted {
                generation,
                outcome,
            }
        };
        let task = match context.pipe_to_self(future, |completion| completion) {
            Ok(task) => task,
            Err(error) => {
                self.dispatch_failed(generation, error.to_string())?;
                return Err(error.into());
            }
        };
        self.register_in_flight_task(generation, task)?;
        Ok(PersistenceStatus::InFlight)
    }

    /// Applies a completion in a later actor turn. Ambiguous transport
    /// failures retain the exact write for retry; definitive storage
    /// rejections retain the baseline but wait for a new mutation epoch.
    pub fn apply_completion(
        &mut self,
        completion: MongoFlushCompleted,
    ) -> Result<CompletionStatus, PersistenceError> {
        if self.consume_abandoned_generation(completion.generation) {
            return Ok(CompletionStatus::IgnoredAbandoned);
        }
        match completion.outcome {
            Ok(outcome) => self
                .complete(completion.generation, outcome)
                .map(CompletionStatus::Applied),
            Err(error) if error.recovery() == MongoStoreErrorRecovery::ReprepareAfterMutation => {
                self.dispatch_rejected(completion.generation, error.to_string())
                    .map(CompletionStatus::Applied)
            }
            Err(error) => {
                self.dispatch_failed(completion.generation, error.to_string())?;
                Ok(CompletionStatus::RetryScheduled)
            }
        }
    }
}
