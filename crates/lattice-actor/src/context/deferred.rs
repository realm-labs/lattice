//! Asynchronous work that leaves the actor turn and resumes through the mailbox.
//!
//! `pipe_to_self`, `continue_with`, and `defer_reply` all share one capacity budget so an actor
//! cannot accumulate unbounded off-turn work, and every reply token they own is cancelled when the
//! actor stops.

use std::{
    any::type_name,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use super::{ActorContext, HandlerContext, PipeTaskHandle};
use crate::{
    error::{ActorCallError, PipeToSelfError},
    mailbox::continuation::ContinuationEnvelope,
    reply::{ReplyControl, ReplyTo},
    traits::{Actor, Handler, Message},
};

/// Capacity reservation held for the lifetime of one off-turn operation.
struct DeferredTaskPermit {
    active: Option<Arc<AtomicUsize>>,
}

impl DeferredTaskPermit {
    fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if let Some(active) = self.active.take() {
            active.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

impl Drop for DeferredTaskPermit {
    fn drop(&mut self) {
        self.release_inner();
    }
}

impl PipeTaskHandle {
    /// Requests cancellation of the background future.
    ///
    /// Cancellation drops the future at its next cancellation point. It cannot
    /// prove that an external side effect had not already happened.
    pub fn abort(&self) {
        self.abort.abort();
    }

    /// Returns whether the background task has completed or been cancelled.
    pub fn is_finished(&self) -> bool {
        self.abort.is_finished()
    }
}

impl<A: Actor> ActorContext<A> {
    pub(crate) fn register_pending_reply<T>(&mut self, control: ReplyControl<T>) -> bool
    where
        T: Send + 'static,
    {
        self.reap_runtime_work();
        if self.pending_replies.len() >= self.deferred_capacity {
            return false;
        }
        self.pending_replies.push(Box::new(control));
        true
    }

    /// Runs asynchronous work outside the actor turn and posts its result back
    /// as a one-way message.
    ///
    /// The mapping function runs in the scoped background task. The resulting
    /// message is handled in a later actor turn, so other mailbox traffic may
    /// be processed first. The work is bounded by the deferred-operation
    /// capacity and is aborted when the actor stops. The returned handle may
    /// be discarded for fire-and-forget use or retained for explicit
    /// cancellation.
    ///
    /// Use [`Self::defer_reply`] when the continuation owns an ask reply token
    /// and must inherit that request's deadline and failure semantics.
    pub fn pipe_to_self<Fut, Map, M>(
        &mut self,
        future: Fut,
        map: Map,
    ) -> Result<PipeTaskHandle, PipeToSelfError>
    where
        A: Handler<M>,
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<M>,
        M: Message,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
        Map: FnOnce(Fut::Output) -> M + Send + 'static,
    {
        let permit = self.reserve_deferred_task()?;
        let handle = self.handle.clone();
        let abort = self.deferred_tasks.spawn(async move {
            let message = map(future.await);
            permit.release();
            if let Err(error) = handle.send_tell_internal(message).await {
                tracing::debug!(
                    actor.type = type_name::<A>(),
                    %error,
                    "actor pipe-to-self continuation was not delivered"
                );
            }
        });
        Ok(PipeTaskHandle { abort })
    }

    /// Runs asynchronous work outside the actor turn, then resumes directly
    /// against the actor in a later normal-mailbox turn.
    ///
    /// The continuation receives exclusive access to the actor, its
    /// message-scoped [`HandlerContext`], and the future output. It is
    /// intentionally synchronous: start another asynchronous step with
    /// `continue_with` instead of holding actor access across an `.await`.
    /// Other mailbox traffic may run before the continuation, and concurrently
    /// started operations have no start-order guarantee.
    /// Continuations are internal actor work and do not participate in typed
    /// behavior admission, though they remain observable as
    /// [`crate::traits::MessageKind::Continuation`].
    ///
    /// Use [`Self::pipe_to_self`] when the result should remain an explicit
    /// typed message handled through [`Handler`].
    pub fn continue_with<Fut, Continue>(
        &mut self,
        future: Fut,
        continuation: Continue,
    ) -> Result<PipeTaskHandle, PipeToSelfError>
    where
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
        Continue: FnOnce(&mut A, &mut HandlerContext<'_, A>, Fut::Output) -> Result<(), A::Error>
            + Send
            + 'static,
    {
        let permit = self.reserve_deferred_task()?;
        let handle = self.handle.clone();
        let abort = self.deferred_tasks.spawn(async move {
            let output = future.await;
            let envelope = ContinuationEnvelope::new(output, continuation);
            permit.release();
            if let Err(error) = handle.send_envelope_internal(envelope).await {
                tracing::debug!(
                    actor.type = type_name::<A>(),
                    %error,
                    "actor continuation was not delivered"
                );
            }
        });
        Ok(PipeTaskHandle { abort })
    }

    /// Defers an ask reply while asynchronous work runs outside the actor turn.
    ///
    /// Unlike [`Self::pipe_to_self`], this operation owns a [`ReplyTo`] and
    /// therefore observes the ask deadline. Capacity exhaustion, deadline
    /// expiry, and failure to post the continuation complete the request with
    /// `MailboxFull`, `DeadlineExceeded`, or `MailboxClosed`, respectively.
    /// The mapping function receives the reply token so the later actor turn
    /// can finish the request using current actor state.
    pub fn defer_reply<T, Fut, Map, M>(
        &mut self,
        reply_to: ReplyTo<T>,
        future: Fut,
        map: Map,
    ) -> Result<(), PipeToSelfError>
    where
        A: Handler<M>,
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<M>,
        M: Message,
        T: Send + 'static,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
        Map: FnOnce(Fut::Output, ReplyTo<T>) -> M + Send + 'static,
    {
        let control = reply_to.control();
        let permit = self.reserve_deferred_task().inspect_err(|_| {
            control.cancel(ActorCallError::MailboxFull);
        })?;

        let handle = self.handle.clone();
        let deadline = control.deadline();
        self.deferred_tasks.spawn(async move {
            let output = if let Some(deadline) = deadline {
                match tokio::time::timeout_at(deadline.into(), future).await {
                    Ok(output) => output,
                    Err(_) => {
                        control.cancel(ActorCallError::DeadlineExceeded);
                        return;
                    }
                }
            } else {
                future.await
            };

            if control.reap() {
                return;
            }
            let message = map(output, reply_to);
            permit.release();
            if let Some(deadline) = deadline {
                match tokio::time::timeout_at(deadline.into(), handle.send_tell_internal(message))
                    .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => control.cancel(ActorCallError::MailboxClosed),
                    Err(_) => control.cancel(ActorCallError::DeadlineExceeded),
                }
            } else if handle.send_tell_internal(message).await.is_err() {
                control.cancel(ActorCallError::MailboxClosed);
            }
        });
        Ok(())
    }

    pub(crate) fn cancel_deferred_replies(&mut self, error: ActorCallError) {
        for pending in self.pending_replies.drain(..) {
            pending.cancel(&error);
        }
        self.deferred_tasks.abort_all();
    }

    fn reserve_deferred_task(&mut self) -> Result<DeferredTaskPermit, PipeToSelfError> {
        self.reap_runtime_work();
        self.active_deferred_tasks
            .try_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < self.deferred_capacity).then_some(active + 1)
            })
            .map(|_| DeferredTaskPermit {
                active: Some(self.active_deferred_tasks.clone()),
            })
            .map_err(|_| PipeToSelfError::Capacity {
                capacity: self.deferred_capacity,
            })
    }
}
