//! Scoped background work owned by one activation: timers, scoped tasks, and DeathWatch.
//!
//! Everything registered here is reclaimed when the actor stops, so the runtime never leaks a
//! task that outlives the actor state it observes.

use std::{any::type_name, future::Future, time::Duration};

use tokio::task::{AbortHandle, JoinSet};
use tracing::Instrument;

#[cfg(feature = "distributed")]
use lattice_core::actor_ref::{ActorRef, EntityRef, ProtocolTag, SingletonRef};

use super::ActorContext;
use crate::{
    error::{ActorCallError, ActorError, ActorTellError},
    handle::ActorHandle,
    traits::{Actor, Handler, Message},
    watch::{ActorTerminated, TerminatedTarget, WatchId},
};

#[cfg(feature = "distributed")]
use crate::recipient::WatchSubscription;

pub struct ContextWatchTarget(ContextWatchSource);

enum ContextWatchSource {
    Local(crate::handle::ActorTerminationSubscription),
    #[cfg(feature = "distributed")]
    Exact(ActorRef),
    #[cfg(feature = "distributed")]
    EntityCurrent(EntityRef),
    #[cfg(feature = "distributed")]
    SingletonCurrent(SingletonRef),
}

impl<B: Actor> From<&ActorHandle<B>> for ContextWatchTarget {
    fn from(target: &ActorHandle<B>) -> Self {
        Self(ContextWatchSource::Local(target.subscribe_terminated()))
    }
}

#[cfg(feature = "distributed")]
impl<P: ProtocolTag> From<&ActorRef<P>> for ContextWatchTarget {
    fn from(target: &ActorRef<P>) -> Self {
        Self(ContextWatchSource::Exact(target.erase()))
    }
}

#[cfg(feature = "distributed")]
impl<P: ProtocolTag> From<&EntityRef<P>> for ContextWatchTarget {
    fn from(target: &EntityRef<P>) -> Self {
        Self(ContextWatchSource::EntityCurrent(target.erase()))
    }
}

#[cfg(feature = "distributed")]
impl<P: ProtocolTag> From<&SingletonRef<P>> for ContextWatchTarget {
    fn from(target: &SingletonRef<P>) -> Self {
        Self(ContextWatchSource::SingletonCurrent(target.erase()))
    }
}

enum ContextWatchSubscription {
    Local(crate::handle::ActorTerminationSubscription),
    #[cfg(feature = "distributed")]
    Cluster(WatchSubscription),
}

impl ContextWatchSubscription {
    async fn recv(&mut self, watch_id: WatchId) -> Option<ActorTerminated> {
        match self {
            Self::Local(subscription) => {
                let termination = subscription.recv().await.ok()?;
                Some(ActorTerminated {
                    watch_id,
                    target: TerminatedTarget::Local(termination.target),
                    reason: termination.reason,
                })
            }
            #[cfg(feature = "distributed")]
            Self::Cluster(subscription) => subscription.recv().await.ok(),
        }
    }
}

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

    pub async fn watch(
        &mut self,
        target: impl Into<ContextWatchTarget>,
    ) -> Result<WatchId, ActorError>
    where
        A: Handler<ActorTerminated>,
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<ActorTerminated>,
    {
        self.watches.retain(|_watch_id, task| !task.is_finished());
        if self.watches.len() >= MAX_ACTIVE_WATCHES {
            return Err(ActorError::new(format!(
                "actor watch capacity {MAX_ACTIVE_WATCHES} is exhausted"
            )));
        }
        let mut terminations = match target.into().0 {
            ContextWatchSource::Local(subscription) => {
                ContextWatchSubscription::Local(subscription)
            }
            #[cfg(feature = "distributed")]
            ContextWatchSource::Exact(target) => ContextWatchSubscription::Cluster(
                self.actor_system()?
                    .watch(&target)
                    .await
                    .map_err(|error| ActorError::new(error.to_string()))?,
            ),
            #[cfg(feature = "distributed")]
            ContextWatchSource::EntityCurrent(target) => ContextWatchSubscription::Cluster(
                self.actor_system()?
                    .watch(&target)
                    .await
                    .map_err(|error| ActorError::new(error.to_string()))?,
            ),
            #[cfg(feature = "distributed")]
            ContextWatchSource::SingletonCurrent(target) => ContextWatchSubscription::Cluster(
                self.actor_system()?
                    .watch(&target)
                    .await
                    .map_err(|error| ActorError::new(error.to_string()))?,
            ),
        };
        let watch_id = match &terminations {
            ContextWatchSubscription::Local(_) => WatchId::random(),
            #[cfg(feature = "distributed")]
            ContextWatchSubscription::Cluster(subscription) => subscription.id(),
        };
        let self_handle = self.handle.clone();
        let span = tracing::info_span!(
            "actor.watch",
            otel.kind = "internal",
            watcher.type = type_name::<A>(),
            watch.id = ?watch_id
        );
        let task = tokio::spawn(
            async move {
                if let Some(notification) = terminations.recv(watch_id).await {
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
