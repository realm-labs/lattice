//! Scoped background work owned by one activation: timers, scoped tasks, and DeathWatch.
//!
//! Everything registered here is reclaimed when the actor stops, so the runtime never leaks a
//! task that outlives the actor state it observes.

use std::{any::type_name, future::Future, time::Duration};

use tokio::task::{AbortHandle, JoinSet};
use tracing::Instrument;

use super::ActorContext;
use crate::{
    error::{ActorCallError, ActorError, ActorTellError},
    handle::ActorHandle,
    traits::{Actor, Handler, Message},
    watch::{ActorTerminated, WatchId},
};

/// Upper bound on concurrently registered DeathWatch subscriptions per activation.
///
/// Watches are runtime-owned tasks, so they are bounded like every other actor resource. Completed
/// watches are reclaimed before the limit is enforced.
const MAX_ACTIVE_WATCHES: usize = 1_024;

impl<A: Actor> ActorContext<A> {
    pub fn notify_after<M>(&mut self, delay: Duration, msg: M)
    where
        A: Handler<M>,
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<M>,
        M: Message,
    {
        let handle = self.handle.clone();
        let span = tracing::info_span!(
            "actor.timer",
            otel.kind = "internal",
            actor.type = type_name::<A>(),
            message.type = type_name::<M>(),
            timer.kind = "after"
        );
        self.spawn_scoped(
            async move {
                tokio::time::sleep(delay).await;
                let _ = handle.try_tell_internal(msg);
            }
            .instrument(span),
        );
    }

    pub fn notify_interval<M, F>(&mut self, interval: Duration, mut make_msg: F)
    where
        A: Handler<M>,
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<M>,
        M: Message,
        F: FnMut() -> M + Send + 'static,
    {
        let handle = self.handle.clone();
        let span = tracing::info_span!(
            "actor.timer",
            otel.kind = "internal",
            actor.type = type_name::<A>(),
            message.type = type_name::<M>(),
            timer.kind = "interval"
        );
        self.spawn_scoped(
            async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    ticker.tick().await;
                    // A momentarily full mailbox drops one tick; only a mailbox that can never
                    // accept the message again retires the timer.
                    match handle.try_tell_internal(make_msg()) {
                        Ok(()) | Err(ActorTellError::MailboxFull(_)) => {}
                        Err(
                            ActorTellError::MailboxClosed(_)
                            | ActorTellError::LifecycleUnavailable { .. },
                        ) => break,
                    }
                }
            }
            .instrument(span),
        );
    }

    pub fn spawn_scoped<F>(&mut self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_scoped_task(future);
    }

    pub(super) fn spawn_scoped_task<F>(&mut self, future: F) -> AbortHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Self::reap_tasks(&mut self.tasks, "scoped");
        self.tasks.spawn(future)
    }

    pub fn watch<B>(&mut self, target: &ActorHandle<B>) -> Result<WatchId, ActorError>
    where
        A: Handler<ActorTerminated>,
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<ActorTerminated>,
        B: Actor,
    {
        self.watches.retain(|_watch_id, task| !task.is_finished());
        if self.watches.len() >= MAX_ACTIVE_WATCHES {
            return Err(ActorError::new(format!(
                "actor watch capacity {MAX_ACTIVE_WATCHES} is exhausted"
            )));
        }
        let watch_id = WatchId::new(self.next_watch_id);
        self.next_watch_id += 1;

        let mut terminations = target.subscribe_terminated();
        let self_handle = self.handle.clone();
        let span = tracing::info_span!(
            "actor.watch",
            otel.kind = "internal",
            watcher.type = type_name::<A>(),
            watched.type = type_name::<B>(),
            watch.id = ?watch_id
        );
        let task = tokio::spawn(
            async move {
                if let Ok(notification) = terminations.recv().await {
                    let _ = self_handle.send_system_tell_internal(notification).await;
                }
            }
            .instrument(span),
        );
        self.watches.insert(watch_id, task);
        Ok(watch_id)
    }

    pub fn unwatch(&mut self, watch_id: &WatchId) -> bool {
        if let Some(task) = self.watches.remove(watch_id) {
            task.abort();
            true
        } else {
            false
        }
    }

    pub fn cancel_all_tasks(&mut self) {
        self.cancel_deferred_replies(ActorCallError::MailboxClosed);
        self.tasks.abort_all();
        for (_watch_id, task) in self.watches.drain() {
            task.abort();
        }
    }

    pub(crate) fn reap_runtime_work(&mut self) {
        if self.tasks.is_empty()
            && self.deferred_tasks.is_empty()
            && self.pending_replies.is_empty()
        {
            return;
        }
        Self::reap_tasks(&mut self.tasks, "scoped");
        Self::reap_tasks(&mut self.deferred_tasks, "deferred");
        self.pending_replies.retain(|pending| !pending.reap());
    }

    fn reap_tasks(tasks: &mut JoinSet<()>, kind: &'static str) {
        while let Some(result) = tasks.try_join_next() {
            if let Err(error) = result
                && !error.is_cancelled()
            {
                tracing::warn!(task.kind = kind, %error, "actor scoped task failed");
            }
        }
    }
}
