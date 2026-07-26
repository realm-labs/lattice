//! The actor-facing runtime context.
//!
//! [`ActorContext`] owns every per-activation resource: child actors, DeathWatch subscriptions,
//! scoped tasks, deferred replies, and actor-local extensions. Its surface is grouped into
//! sibling modules by responsibility; the type definitions stay here so the published paths are
//! independent of that grouping.

use std::{
    any::{Any, TypeId},
    collections::HashMap,
    fmt,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use lattice_core::{actor_ref::ActorRef, service_context::ServiceContext};
use tokio::task::{JoinHandle, JoinSet};

use crate::{
    error::ActorError,
    handle::ActorHandle,
    recipient::ActorSystem,
    reply::PendingReply,
    runtime::spawner::ActorSpawner,
    traits::{Actor, ChildActorKey, PassivationReason, StopReason},
    watch::WatchId,
};

mod children;
mod deferred;
mod extensions;
mod messaging;
mod tasks;

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

/// Owned, message-scoped capability for typed Actor messaging.
///
/// This is the narrow owned counterpart of [`ActorContext::tell`],
/// [`ActorContext::ask`], and [`ActorContext::forward`]. It snapshots only the
/// current Actor system, self/sender identity, and request deadline, so an
/// adapter may retain it across an async call without retaining or erasing an
/// [`ActorContext`] borrow. The target protocol and message types remain
/// statically checked at each call site.
#[derive(Clone, Debug)]
pub struct ActorTurnMessaging {
    actor_system: ActorSystem,
    self_ref: Option<ActorRef>,
    sender: Option<ActorRef>,
    deadline: Option<Instant>,
}

/// Type-indexed state owned by one actor activation.
///
/// Values are retained for the lifetime of the surrounding [`ActorContext`]. They are not shared,
/// serialized, persisted, or carried across passivation, termination, or supervision restart.
#[derive(Default)]
pub struct ActorLocalExtensions {
    values: HashMap<TypeId, Box<dyn Any + Send>>,
}

pub struct ActorContext<A: Actor> {
    handle: ActorHandle<A>,
    self_ref: Option<ActorRef>,
    actor_system: Option<Arc<OnceLock<ActorSystem>>>,
    service: ServiceContext,
    local_extensions: ActorLocalExtensions,
    spawner: ActorSpawner,
    lifecycle_request: Option<StopReason>,
    tasks: JoinSet<()>,
    deferred_tasks: JoinSet<()>,
    active_deferred_tasks: Arc<AtomicUsize>,
    pending_replies: Vec<Box<dyn PendingReply>>,
    deferred_capacity: usize,
    watches: HashMap<WatchId, JoinHandle<()>>,
    children: HashMap<ChildActorKey, Box<dyn ChildStop>>,
    next_watch_id: u64,
    sender: Option<ActorRef>,
    current_deadline: Option<Instant>,
}

impl<A: Actor> fmt::Debug for ActorContext<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorContext")
            .field("handle", &self.handle)
            .field(
                "self_ref",
                &self
                    .self_ref
                    .as_ref()
                    .map(|actor_ref| actor_ref.actor_path()),
            )
            .field("service", &self.service)
            .field("local_extensions", &self.local_extensions)
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
            .field("child_count", &self.children.len())
            .field("next_watch_id", &self.next_watch_id)
            .field("has_sender", &self.sender.is_some())
            .field("current_deadline", &self.current_deadline)
            .finish()
    }
}

impl<A: Actor> ActorContext<A> {
    pub(crate) fn new(
        handle: ActorHandle<A>,
        self_ref: Option<ActorRef>,
        actor_system: Option<Arc<OnceLock<ActorSystem>>>,
        service: ServiceContext,
        spawner: ActorSpawner,
        deferred_capacity: usize,
    ) -> Self {
        Self {
            handle,
            self_ref,
            actor_system,
            service,
            local_extensions: ActorLocalExtensions::new(),
            spawner,
            lifecycle_request: None,
            tasks: JoinSet::new(),
            deferred_tasks: JoinSet::new(),
            active_deferred_tasks: Arc::new(AtomicUsize::new(0)),
            pending_replies: Vec::new(),
            deferred_capacity,
            watches: HashMap::new(),
            children: HashMap::new(),
            next_watch_id: 0,
            sender: None,
            current_deadline: None,
        }
    }

    /// Returns this actor's exact activation reference when one was assigned.
    ///
    /// Clone the reference before putting it in a message or retaining it. The
    /// reference remains bound to this activation and becomes stale after the
    /// actor stops or is replaced.
    pub fn self_ref(&self) -> Option<&ActorRef> {
        self.self_ref.as_ref()
    }

    pub fn self_handle(&self) -> ActorHandle<A> {
        self.handle.clone()
    }

    pub fn service(&self) -> &ServiceContext {
        &self.service
    }

    pub fn local_extensions(&self) -> &ActorLocalExtensions {
        &self.local_extensions
    }

    pub fn local_extensions_mut(&mut self) -> &mut ActorLocalExtensions {
        &mut self.local_extensions
    }

    pub fn require_self_ref(&self) -> Result<&ActorRef, ActorError> {
        self.self_ref
            .as_ref()
            .ok_or_else(|| ActorError::new("actor self ref is not available"))
    }

    /// Returns the actor that sent the current one-way message.
    ///
    /// The value is message-scoped and read-only. Process-originated tells and
    /// asks have no actor sender; asks reply through their typed `ReplyTo`.
    pub fn sender(&self) -> Option<&ActorRef> {
        self.sender.as_ref()
    }

    /// Returns the absolute deadline attached to the current request, if any.
    ///
    /// The value is message-scoped and is cleared after dispatch. Callers that
    /// need to retain deadline information outside the current turn should
    /// convert it to an owned duration or timestamp first.
    pub fn current_deadline(&self) -> Option<Instant> {
        self.current_deadline
    }

    pub(crate) fn set_sender(&mut self, sender: ActorRef) {
        self.sender = Some(sender);
    }

    pub(crate) fn clear_sender(&mut self) {
        self.sender = None;
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
