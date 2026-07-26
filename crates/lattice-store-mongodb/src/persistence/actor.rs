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

    /// Dispatches a prepared flush and arms a self-addressed wakeup whenever
    /// the attempt enters a new backoff.
    ///
    /// Without a wakeup, a failed flush is only retried when the actor happens
    /// to receive another message, so the dirty state it retains is the actor's
    /// only copy and can remain unwritten indefinitely. `retry_message` is the
    /// actor's own "prepare and flush" message; handling it must repeat the
    /// preparation and dispatch that produced `prepared`.
    ///
    /// One wakeup is outstanding per backoff deadline, so repeated dispatches
    /// during one backoff window do not accumulate timers. Delivery uses the
    /// normal mailbox lane and is therefore best effort: a saturated mailbox
    /// drops the notification, and the next dispatch after the deadline has
    /// passed arms a replacement.
    pub fn dispatch_prepared_with_retry<A, M>(
        &mut self,
        context: &mut ActorContext<A>,
        store: Arc<dyn PreparedWriteStore>,
        prepared: PreparedFlush,
        retry_message: M,
    ) -> Result<PersistenceStatus, ActorPersistenceError>
    where
        A: Actor + Handler<MongoFlushCompleted> + Handler<M>,
        <A as lattice_actor::traits::Actor>::Behavior: lattice_actor::state_machine::Accepts<MongoFlushCompleted>
            + lattice_actor::state_machine::Accepts<M>,
        M: Message,
    {
        let status = self.dispatch_prepared(context, store, prepared);
        self.schedule_retry_wakeup(context, retry_message);
        status
    }

    /// Applies a completion and arms a self-addressed wakeup whenever the
    /// outcome retained work for a later retry. See
    /// [`Self::dispatch_prepared_with_retry`] for the delivery contract.
    pub fn apply_completion_with_retry<A, M>(
        &mut self,
        context: &mut ActorContext<A>,
        completion: MongoFlushCompleted,
        retry_message: M,
    ) -> Result<CompletionStatus, PersistenceError>
    where
        A: Actor + Handler<M>,
        <A as lattice_actor::traits::Actor>::Behavior: lattice_actor::state_machine::Accepts<M>,
        M: Message,
    {
        let status = self.apply_completion(completion);
        self.schedule_retry_wakeup(context, retry_message);
        status
    }

    /// Posts `retry_message` back to the owning actor when the retained retry
    /// backoff expires. Returns whether a wakeup was armed.
    pub fn schedule_retry_wakeup<A, M>(
        &mut self,
        context: &mut ActorContext<A>,
        retry_message: M,
    ) -> bool
    where
        A: Actor + Handler<M>,
        <A as lattice_actor::traits::Actor>::Behavior: lattice_actor::state_machine::Accepts<M>,
        M: Message,
    {
        let Some(delay) = self.arm_retry_wakeup() else {
            return false;
        };
        context.notify_after(delay, retry_message);
        true
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
