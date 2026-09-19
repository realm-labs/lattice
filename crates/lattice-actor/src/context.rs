//! The actor-facing runtime context.
//!
//! [`ActorContext`] owns every per-activation runtime facility: child actors, DeathWatch
//! subscriptions, scoped tasks, deferred replies, and integration attachments. Its surface is
//! grouped into
//! sibling modules by responsibility; the type definitions stay here so the published paths are
//! independent of that grouping.

use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use lattice_core::service_context::ServiceContext;
use tokio::task::{JoinHandle, JoinSet};

use crate::{
    attachments::ActorRuntimeAttachments,
    error::ActorError,
    handle::ActorHandle,
    reply::PendingReply,
    runtime::spawner::ActorSpawner,
    traits::{Actor, ChildActorKey, PassivationReason, StopReason},
    watch::WatchId,
};

mod children;
mod deferred;
mod messaging;
mod tasks;

pub use messaging::{AskTarget, TellTarget};

use children::ChildStop;

/// A cancellation handle for work started by [`ActorContext::pipe_to_self`] or
/// [`ActorContext::continue_with`].
///
/// Dropping the handle does not cancel the task. Call [`Self::abort`] when the
/// owning actor state has explicitly abandoned the asynchronous operation.
/// Actor shutdown still cancels every outstanding pipe task automatically.
#[derive(Debug, Clone)]
pub struct PipeTaskHandle {
    abort: tokio::task::AbortHandle,
}

pub struct ActorContext<A: Actor> {
    handle: ActorHandle<A>,
    service: ServiceContext,
    runtime_attachments: ActorRuntimeAttachments,
    spawner: ActorSpawner,
    lifecycle_request: Option<StopReason>,
    tasks: JoinSet<()>,
    deferred_tasks: JoinSet<()>,
    active_deferred_tasks: Arc<AtomicUsize>,
    pending_replies: Vec<Box<dyn PendingReply>>,
    deferred_capacity: usize,
    watches: HashMap<WatchId, JoinHandle<()>>,
    children: HashMap<ChildActorKey, Box<dyn ChildStop>>,
    current_deadline: Option<Instant>,
}

impl<A: Actor> fmt::Debug for ActorContext<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("ActorContext");
        debug.field("handle", &self.handle);
        debug
            .field("service", &self.service)
            .field("runtime_attachments", &self.runtime_attachments)
            .field("lifecycle_request", &self.lifecycle_request)
            .field("task_count", &self.tasks.len())
            .field("deferred_task_count", &self.deferred_tasks.len())
            .field(
                "active_deferred_task_count",
                &self.active_deferred_tasks.load(Ordering::Acquire),
            )
            .field("pending_reply_count", &self.pending_replies.len())
            .field("deferred_capacity", &self.deferred_capacity)
            .field("watch_count", &self.watches.len())
            .field("child_count", &self.children.len());
        debug
            .field("current_deadline", &self.current_deadline)
            .finish()
    }
}

impl<A: Actor> ActorContext<A> {
    pub(crate) fn new(
        handle: ActorHandle<A>,
        service: ServiceContext,
        runtime_attachments: ActorRuntimeAttachments,
        spawner: ActorSpawner,
        deferred_capacity: usize,
    ) -> Self {
        Self {
            handle,
            service,
            runtime_attachments,
            spawner,
            lifecycle_request: None,
            tasks: JoinSet::new(),
            deferred_tasks: JoinSet::new(),
            active_deferred_tasks: Arc::new(AtomicUsize::new(0)),
            pending_replies: Vec::new(),
            deferred_capacity,
            watches: HashMap::new(),
            children: HashMap::new(),
            current_deadline: None,
        }
    }

    pub fn self_handle(&self) -> ActorHandle<A> {
        self.handle.clone()
    }

    pub fn service(&self) -> &ServiceContext {
        &self.service
    }

    /// Returns immutable activation metadata installed by a runtime integration.
    #[doc(hidden)]
    pub fn runtime_attachment<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.runtime_attachments.get::<T>()
    }

    /// Returns the absolute deadline attached to the current request, if any.
    ///
    /// The value is message-scoped and is cleared after dispatch. Callers that
    /// need to retain deadline information outside the current turn should
    /// convert it to an owned duration or timestamp first.
    pub fn current_deadline(&self) -> Option<Instant> {
        self.current_deadline
    }

    pub(crate) fn set_current_deadline(&mut self, deadline: Option<Instant>) {
        self.current_deadline = deadline;
    }

    pub fn request_stop(&mut self) {
        self.lifecycle_request = Some(StopReason::Requested);
    }

    pub fn request_passivation(&mut self, reason: PassivationReason) -> Result<(), ActorError> {
        self.lifecycle_request = Some(StopReason::Passivated(reason));
        Ok(())
    }

    pub(crate) fn take_lifecycle_request(&mut self) -> Option<StopReason> {
        self.lifecycle_request.take()
    }
}

impl<A: Actor> Drop for ActorContext<A> {
    fn drop(&mut self) {
        self.cancel_all_tasks();
    }
}

/// Message-scoped access to the actor runtime and its typed behavior.
///
/// This wrapper is created only while a typed handler or responder is running. It dereferences to
/// [`ActorContext`], so existing context operations remain available without forwarding methods.
pub struct HandlerContext<'a, A: Actor> {
    actor: &'a mut ActorContext<A>,
    behavior: &'a mut A::Behavior,
}

impl<'a, A: Actor> HandlerContext<'a, A> {
    pub(crate) fn new(actor: &'a mut ActorContext<A>, behavior: &'a mut A::Behavior) -> Self {
        Self { actor, behavior }
    }

    pub fn behavior(&self) -> &A::Behavior {
        self.behavior
    }

    pub fn behavior_mut(&mut self) -> &mut A::Behavior {
        self.behavior
    }

    pub fn transition_to(&mut self, next: A::Behavior) {
        *self.behavior = next;
    }
}

impl<A: Actor> std::ops::Deref for HandlerContext<'_, A> {
    type Target = ActorContext<A>;

    fn deref(&self) -> &Self::Target {
        self.actor
    }
}

impl<A: Actor> std::ops::DerefMut for HandlerContext<'_, A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.actor
    }
}
