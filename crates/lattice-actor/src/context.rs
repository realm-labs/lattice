use std::{
    any::{Any, TypeId, type_name},
    collections::HashMap,
    fmt,
    future::Future,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use lattice_core::{
    actor_ref::{ActorRef, ProtocolId, RecipientRef},
    service_context::ServiceContext,
};
use tokio::task::{JoinHandle, JoinSet};
use tracing::Instrument;

use crate::{
    directory::ActivationDirectory,
    error::{ActorCallError, ActorError, ActorTellError, PipeToSelfError},
    handle::ActorHandle,
    mailbox::continuation::ContinuationEnvelope,
    protocol::{SupportsAsk, SupportsTell},
    recipient::{ActorSystem, RecipientError, deadline_from_timeout},
    reply::{PendingReply, ReplyControl, ReplyTo},
    runtime::{ActorSpawnContext, ActorSpawnOptions, PassivationPolicy, spawner::ActorSpawner},
    traits::{
        Actor, ChildActorKey, ChildActorOptions, ChildSupervision, Handler, Message,
        PassivationReason, Request, StopReason,
    },
    watch::{ActorTerminated, WatchId},
};

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

impl ActorTurnMessaging {
    /// Sends with the current Actor as the envelope sender.
    pub async fn tell<P, M>(
        &self,
        target: impl Into<RecipientRef<P>>,
        message: M,
    ) -> Result<(), RecipientError>
    where
        P: SupportsTell<M>,
        M: Message,
    {
        self.actor_system
            .tell_with_sender(
                target.into(),
                message,
                self.self_ref.as_ref().map(ActorRef::erase),
            )
            .await
    }

    /// Sends a typed request without extending the current request deadline.
    pub async fn ask<P, R>(
        &self,
        target: impl Into<RecipientRef<P>>,
        request: R,
        timeout: Duration,
    ) -> Result<R::Response, RecipientError>
    where
        P: SupportsAsk<R>,
        R: Request,
    {
        let requested_deadline = deadline_from_timeout(timeout)?;
        let deadline = self
            .deadline
            .map_or(requested_deadline, |parent| parent.min(requested_deadline));
        self.actor_system
            .ask_until(target.into(), request, deadline)
            .await
    }

    /// Forwards while preserving the original envelope sender.
    pub async fn forward<P, M>(
        &self,
        target: impl Into<RecipientRef<P>>,
        message: M,
    ) -> Result<(), RecipientError>
    where
        P: SupportsTell<M>,
        M: Message,
    {
        self.actor_system
            .tell_with_sender(
                target.into(),
                message,
                self.sender.as_ref().map(ActorRef::erase),
            )
            .await
    }
}

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

/// Type-indexed state owned by one actor activation.
///
/// Values are retained for the lifetime of the surrounding [`ActorContext`]. They are not shared,
/// serialized, persisted, or carried across passivation, termination, or supervision restart.
#[derive(Default)]
pub struct ActorLocalExtensions {
    values: HashMap<TypeId, Box<dyn Any + Send>>,
}

impl fmt::Debug for ActorLocalExtensions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorLocalExtensions")
            .field("extension_count", &self.values.len())
            .finish()
    }
}

impl ActorLocalExtensions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get<T: Send + 'static>(&self) -> Option<&T> {
        self.values
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref::<T>())
    }

    pub fn get_mut<T: Send + 'static>(&mut self) -> Option<&mut T> {
        self.values
            .get_mut(&TypeId::of::<T>())
            .and_then(|value| value.downcast_mut::<T>())
    }

    pub fn insert<T: Send + 'static>(&mut self, value: T) -> Option<T> {
        self.values
            .insert(TypeId::of::<T>(), Box::new(value))
            .map(|previous| {
                *previous
                    .downcast::<T>()
                    .expect("actor-local extension type ID invariant violated")
            })
    }

    pub fn remove<T: Send + 'static>(&mut self) -> Option<T> {
        self.values.remove(&TypeId::of::<T>()).map(|value| {
            *value
                .downcast::<T>()
                .expect("actor-local extension type ID invariant violated")
        })
    }

    pub fn get_or_insert_with<T: Send + 'static>(&mut self, create: impl FnOnce() -> T) -> &mut T {
        self.values
            .entry(TypeId::of::<T>())
            .or_insert_with(|| Box::new(create()))
            .downcast_mut::<T>()
            .expect("actor-local extension type ID invariant violated")
    }
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

    /// Snapshots the current turn's typed messaging authority.
    ///
    /// The returned handle does not expose placement, mailbox, child, task, or
    /// extension internals. It preserves self/sender propagation and the
    /// current request deadline for adapters that must perform typed messaging
    /// after releasing the `ActorContext` borrow.
    pub fn turn_messaging(&self) -> Result<ActorTurnMessaging, RecipientError> {
        Ok(ActorTurnMessaging {
            actor_system: self.actor_system()?.clone(),
            self_ref: self.self_ref.clone(),
            sender: self.sender.clone(),
            deadline: self.current_deadline,
        })
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

    /// Sends to a process-local handle with this actor as the envelope sender.
    pub fn tell_local<B, M>(
        &self,
        target: &ActorHandle<B>,
        message: M,
    ) -> Result<(), ActorTellError<M>>
    where
        B: Actor + Handler<M>,
        <B as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<M>,
        M: Message,
    {
        let sender = self.self_ref.as_ref().map(ActorRef::erase);
        target.try_tell_from(message, sender)
    }

    /// Forwards a one-way message while preserving the current envelope sender.
    ///
    /// If the current message has no actor sender, the forwarded message also
    /// has no actor sender.
    pub fn forward_local<B, M>(
        &self,
        target: &ActorHandle<B>,
        message: M,
    ) -> Result<(), ActorTellError<M>>
    where
        B: Actor + Handler<M>,
        <B as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<M>,
        M: Message,
    {
        target.try_tell_from(message, self.sender.as_ref().map(ActorRef::erase))
    }

    /// Sends to an exact or logical actor reference with this actor as sender.
    pub async fn tell<P, M>(
        &mut self,
        target: impl Into<RecipientRef<P>>,
        message: M,
    ) -> Result<(), RecipientError>
    where
        P: SupportsTell<M>,
        M: Message,
    {
        self.actor_system()?
            .tell_with_sender(
                target.into(),
                message,
                self.self_ref.as_ref().map(ActorRef::erase),
            )
            .await
    }

    /// Sends a request using a relative timeout.
    ///
    /// While handling another request, the downstream ask cannot outlive the
    /// current request's remaining deadline.
    pub async fn ask<P, R>(
        &mut self,
        target: impl Into<RecipientRef<P>>,
        request: R,
        timeout: Duration,
    ) -> Result<R::Response, RecipientError>
    where
        P: SupportsAsk<R>,
        R: Request,
    {
        let requested_deadline = deadline_from_timeout(timeout)?;
        let deadline = self
            .current_deadline
            .map_or(requested_deadline, |parent| parent.min(requested_deadline));
        self.actor_system()?
            .ask_until(target.into(), request, deadline)
            .await
    }

    /// Forwards to an exact or logical actor reference while preserving the
    /// current envelope sender.
    pub async fn forward<P, M>(
        &mut self,
        target: impl Into<RecipientRef<P>>,
        message: M,
    ) -> Result<(), RecipientError>
    where
        P: SupportsTell<M>,
        M: Message,
    {
        self.actor_system()?
            .tell_with_sender(
                target.into(),
                message,
                self.sender.as_ref().map(ActorRef::erase),
            )
            .await
    }

    fn actor_system(&self) -> Result<&ActorSystem, RecipientError> {
        self.actor_system
            .as_ref()
            .and_then(|actor_system| actor_system.get())
            .ok_or(RecipientError::ActorSystemUnavailable)
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

    pub fn request_stop(&mut self) {
        self.lifecycle_request = Some(StopReason::Requested);
    }

    pub fn request_passivation(&mut self, reason: PassivationReason) -> Result<(), ActorError> {
        self.lifecycle_request = Some(StopReason::Passivated(reason));
        Ok(())
    }

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
                    if handle.try_tell_internal(make_msg()).is_err() {
                        break;
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
        Self::reap_tasks(&mut self.tasks, "scoped");
        self.tasks.spawn(future);
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

    pub fn watch<B>(&mut self, target: &ActorHandle<B>) -> Result<WatchId, ActorError>
    where
        A: Handler<ActorTerminated>,
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<ActorTerminated>,
        B: Actor,
    {
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

    pub fn spawn_child<C>(
        &mut self,
        key: ChildActorKey,
        actor: C,
        options: ChildActorOptions,
    ) -> Result<ActorHandle<C>, ActorError>
    where
        C: Actor,
    {
        if options.supervision == ChildSupervision::RestartChild {
            return Err(ActorError::new(
                "RestartChild supervision requires spawn_child_with_factory",
            ));
        }
        if self.children.contains_key(&key) {
            return Err(ActorError::new(format!(
                "child actor {} already exists",
                key.as_str()
            )));
        }

        let span = tracing::info_span!(
            "actor.child.spawn",
            otel.kind = "internal",
            parent.type = type_name::<A>(),
            child.type = type_name::<C>(),
            child.key = key.as_str()
        );
        let _entered = span.enter();
        let child_ref = self.child_actor_ref(&key, options.protocol_id)?;
        let handle = crate::runtime::spawn_actor_with_self_ref(
            actor,
            ActorSpawnContext {
                options: ActorSpawnOptions {
                    mailbox: options.mailbox,
                    execution: Some(options.execution),
                    scheduler_key: options.scheduler_key.clone(),
                    passivation: PassivationPolicy::Disabled,
                    self_ref: child_ref.as_ref().map(ActorRef::erase),
                    service: self.service.clone(),
                },
                actor_system: self.actor_system.clone(),
                observer: self.handle.observer().clone(),
                terminal_hook: None,
                spawner: self.spawner.clone(),
            },
        )
        .map_err(|error| ActorError::new(error.to_string()))?;
        let directory = self.service.extension::<ActivationDirectory>();
        if let Some(directory) = &directory
            && let Err(error) = directory.register(&handle)
        {
            let _ = handle.try_stop_internal(StopReason::StartFailed);
            return Err(ActorError::new(error.to_string()));
        }
        let slot = Arc::new(ChildSlot::new(handle.clone()));
        self.children.insert(
            key,
            Box::new(ChildSlotStopper {
                slot: slot.clone(),
                directory,
                reference: child_ref.map(|reference| reference.erase()),
            }),
        );
        self.spawn_supervision_task(slot, options, None::<fn() -> C>);
        Ok(handle)
    }

    pub fn spawn_child_with_factory<C, F>(
        &mut self,
        key: ChildActorKey,
        mut factory: F,
        options: ChildActorOptions,
    ) -> Result<ActorHandle<C>, ActorError>
    where
        C: Actor,
        F: FnMut() -> C + Send + 'static,
    {
        if self.children.contains_key(&key) {
            return Err(ActorError::new(format!(
                "child actor {} already exists",
                key.as_str()
            )));
        }

        let span = tracing::info_span!(
            "actor.child.spawn",
            otel.kind = "internal",
            parent.type = type_name::<A>(),
            child.type = type_name::<C>(),
            child.key = key.as_str()
        );
        let _entered = span.enter();
        let child_ref = self.child_actor_ref(&key, options.protocol_id)?;
        let handle = crate::runtime::spawn_actor_with_self_ref(
            factory(),
            ActorSpawnContext {
                options: ActorSpawnOptions {
                    mailbox: options.mailbox,
                    execution: Some(options.execution),
                    scheduler_key: options.scheduler_key.clone(),
                    passivation: PassivationPolicy::Disabled,
                    self_ref: child_ref.as_ref().map(ActorRef::erase),
                    service: self.service.clone(),
                },
                actor_system: self.actor_system.clone(),
                observer: self.handle.observer().clone(),
                terminal_hook: None,
                spawner: self.spawner.clone(),
            },
        )
        .map_err(|error| ActorError::new(error.to_string()))?;
        let directory = self.service.extension::<ActivationDirectory>();
        if let Some(directory) = &directory
            && let Err(error) = directory.register(&handle)
        {
            let _ = handle.try_stop_internal(StopReason::StartFailed);
            return Err(ActorError::new(error.to_string()));
        }
        let slot = Arc::new(ChildSlot::new(handle.clone()));
        self.children.insert(
            key,
            Box::new(ChildSlotStopper {
                slot: slot.clone(),
                directory,
                reference: child_ref.map(|reference| reference.erase()),
            }),
        );
        self.spawn_supervision_task(slot, options, Some(factory));
        Ok(handle)
    }

    pub fn stop_child(&mut self, key: &ChildActorKey) -> bool {
        if let Some(child) = self.children.remove(key) {
            let span = tracing::info_span!(
                "actor.child.stop",
                otel.kind = "internal",
                parent.type = type_name::<A>(),
                child.key = key.as_str()
            );
            let _entered = span.enter();
            child.stop(StopReason::Requested);
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

    pub(crate) fn cancel_deferred_replies(&mut self, error: ActorCallError) {
        for pending in self.pending_replies.drain(..) {
            pending.cancel(&error);
        }
        self.deferred_tasks.abort_all();
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

    pub(crate) fn stop_all_children(&mut self, reason: StopReason) {
        for (_key, child) in self.children.drain() {
            child.stop(reason);
        }
    }

    pub(crate) fn take_lifecycle_request(&mut self) -> Option<StopReason> {
        self.lifecycle_request.take()
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

    fn spawn_supervision_task<C, F>(
        &mut self,
        slot: Arc<ChildSlot<C>>,
        options: ChildActorOptions,
        mut factory: Option<F>,
    ) where
        C: Actor,
        F: FnMut() -> C + Send + 'static,
    {
        match options.supervision {
            ChildSupervision::StopChild => {}
            ChildSupervision::StopParent => {
                let parent = self.handle.clone();
                if let Some(child) = slot.current() {
                    let mut terminations = child.subscribe_terminated();
                    self.spawn_scoped(async move {
                        if terminations.recv().await.is_ok() {
                            let _ = parent.try_stop_internal(StopReason::Requested);
                        }
                    });
                }
            }
            ChildSupervision::RestartChild => {
                let (Some(mut factory), Some(child)) = (factory.take(), slot.current()) else {
                    return;
                };
                let mut terminations = child.subscribe_terminated();
                let service = self.service.clone();
                let actor_system = self.actor_system.clone();
                let child_ref = child.actor_ref().map(ActorRef::erase);
                let observer = child.observer().clone();
                let spawner = self.spawner.clone();
                self.spawn_scoped(async move {
                    loop {
                        if terminations.recv().await.is_err() {
                            break;
                        }
                        let replacement = match crate::runtime::spawn_actor_with_self_ref(
                            factory(),
                            ActorSpawnContext {
                                options: ActorSpawnOptions {
                                    mailbox: options.mailbox,
                                    execution: Some(options.execution),
                                    scheduler_key: options.scheduler_key.clone(),
                                    passivation: PassivationPolicy::Disabled,
                                    self_ref: child_ref.as_ref().map(ActorRef::erase),
                                    service: service.clone(),
                                },
                                actor_system: actor_system.clone(),
                                observer: observer.clone(),
                                terminal_hook: None,
                                spawner: spawner.clone(),
                            },
                        ) {
                            Ok(replacement) => replacement,
                            Err(error) => {
                                tracing::warn!(
                                    %error,
                                    "supervised child could not be restarted"
                                );
                                break;
                            }
                        };
                        if let Some(directory) = service.extension::<ActivationDirectory>()
                            && directory.register(&replacement).is_err()
                        {
                            let _ = replacement.try_stop_internal(StopReason::StartFailed);
                            break;
                        }
                        terminations = replacement.subscribe_terminated();
                        slot.replace(replacement);
                    }
                });
            }
        }
    }

    fn child_actor_ref(
        &self,
        key: &ChildActorKey,
        protocol_id: Option<ProtocolId>,
    ) -> Result<Option<ActorRef>, ActorError> {
        let Some(protocol_id) = protocol_id else {
            return Ok(None);
        };
        let parent = self.require_self_ref()?;
        let path = parent
            .actor_path()
            .child(key.as_str())
            .map_err(|error| ActorError::new(error.to_string()))?;
        ActorRef::new(
            parent.cluster_id().clone(),
            parent.node_address().clone(),
            parent.node_incarnation(),
            path,
            crate::runtime::next_activation_id(parent.node_incarnation()),
            protocol_id,
        )
        .map(Some)
        .map_err(|error| ActorError::new(error.to_string()))
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

impl<A: Actor> Drop for ActorContext<A> {
    fn drop(&mut self) {
        self.cancel_all_tasks();
    }
}

trait ChildStop: Send {
    fn stop(self: Box<Self>, reason: StopReason);
}

struct ChildSlot<C: Actor> {
    current: Mutex<Option<ActorHandle<C>>>,
}

impl<C: Actor> ChildSlot<C> {
    fn new(handle: ActorHandle<C>) -> Self {
        Self {
            current: Mutex::new(Some(handle)),
        }
    }

    fn current(&self) -> Option<ActorHandle<C>> {
        self.current.lock().expect("child slot poisoned").clone()
    }

    fn replace(&self, handle: ActorHandle<C>) {
        *self.current.lock().expect("child slot poisoned") = Some(handle);
    }
}

struct ChildSlotStopper<C: Actor> {
    slot: Arc<ChildSlot<C>>,
    directory: Option<Arc<ActivationDirectory>>,
    reference: Option<ActorRef>,
}

impl<C: Actor> ChildStop for ChildSlotStopper<C> {
    fn stop(self: Box<Self>, reason: StopReason) {
        if let (Some(directory), Some(reference)) = (&self.directory, &self.reference) {
            directory.remove(reference);
        }
        if let Some(handle) = self
            .slot
            .current
            .lock()
            .expect("child slot poisoned")
            .take()
        {
            let _ = handle.try_stop_internal(reason);
        }
    }
}

#[cfg(test)]
#[allow(dead_code)]
mod turn_messaging_tests {
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use async_trait::async_trait;
    use bytes::Bytes;
    use lattice_core::actor_ref::{
        ActivationId, ActorPath, ActorRef, ClusterId, EntityRef, NodeAddress, NodeIncarnation,
        ProtocolId, SingletonRef,
    };
    use lattice_remoting::messaging::error::{AskError, TellError};
    use lattice_remoting::protocol::ProtocolFingerprint;
    use lattice_remoting::watch::{WatchError, WatchId};

    use super::ActorTurnMessaging;
    use crate::{
        actor_protocol,
        protocol::ProstCodec,
        recipient::{ActorSystem, RecipientBackend, RegisteredActorProtocol},
        traits::{Message, Request},
    };

    #[derive(Clone, PartialEq, prost::Message)]
    struct Probe {
        #[prost(uint64, tag = "1")]
        value: u64,
    }

    impl Message for Probe {}

    #[derive(Clone, PartialEq, prost::Message)]
    struct Query {
        #[prost(uint64, tag = "1")]
        value: u64,
    }

    impl Request for Query {
        type Response = QueryReply;
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct QueryReply {
        #[prost(uint64, tag = "1")]
        value: u64,
    }

    actor_protocol! {
        TurnProtocol {
            protocol_id: 97;
            name: "actor-turn-messaging/v1";
            tell 1 => Probe {
                schema_version: 1,
                codec: ProstCodec,
            }
            ask 2 => Query {
                request_schema_version: 1,
                response_schema_version: 1,
                request_codec: ProstCodec,
                response_codec: ProstCodec,
            }
        }
    }

    #[derive(Default)]
    struct RecordingBackend {
        tell_senders: Mutex<Vec<Option<ActorRef>>>,
        ask_deadlines: Mutex<Vec<Instant>>,
    }

    #[async_trait]
    impl RecipientBackend for RecordingBackend {
        async fn tell(
            &self,
            sender: Option<ActorRef>,
            _target: lattice_core::actor_ref::RecipientRef,
            _protocol_fingerprint: ProtocolFingerprint,
            _message_id: u64,
            _payload: Bytes,
        ) -> Result<(), TellError> {
            self.tell_senders
                .lock()
                .expect("tell sender mutex")
                .push(sender);
            Ok(())
        }

        async fn ask(
            &self,
            _target: lattice_core::actor_ref::RecipientRef,
            _protocol_fingerprint: ProtocolFingerprint,
            _message_id: u64,
            _payload: Bytes,
            deadline: Instant,
        ) -> Result<Bytes, AskError> {
            self.ask_deadlines
                .lock()
                .expect("ask deadline mutex")
                .push(deadline);
            Ok(Bytes::from(prost::Message::encode_to_vec(&QueryReply {
                value: 41,
            })))
        }

        async fn watch_actor(&self, _target: ActorRef) -> Result<WatchId, WatchError> {
            unimplemented!("watching is outside this fixture")
        }

        async fn watch_entity_current(&self, _target: EntityRef) -> Result<WatchId, WatchError> {
            unimplemented!("watching is outside this fixture")
        }

        async fn watch_singleton_current(
            &self,
            _target: SingletonRef,
        ) -> Result<WatchId, WatchError> {
            unimplemented!("watching is outside this fixture")
        }

        async fn unwatch(&self, _watch_id: WatchId) -> Result<(), WatchError> {
            unimplemented!("watching is outside this fixture")
        }
    }

    #[tokio::test]
    async fn owned_turn_messaging_preserves_sender_and_parent_deadline() {
        let backend = Arc::new(RecordingBackend::default());
        let protocol = Arc::new(TurnProtocol::build().expect("turn protocol"));
        let actor_system =
            ActorSystem::new(backend.clone(), [RegisteredActorProtocol::new(protocol)])
                .expect("actor system");
        let self_ref = actor_ref("self", 1);
        let original_sender = actor_ref("sender", 2);
        let target = actor_ref("target", 3)
            .try_typed::<TurnProtocol>()
            .expect("typed target");
        let parent_deadline = Instant::now() + Duration::from_secs(1);
        let messaging = ActorTurnMessaging {
            actor_system,
            self_ref: Some(self_ref.clone()),
            sender: Some(original_sender.clone()),
            deadline: Some(parent_deadline),
        };

        messaging
            .tell(target.clone(), Probe { value: 1 })
            .await
            .expect("typed tell");
        messaging
            .forward(target.clone(), Probe { value: 2 })
            .await
            .expect("typed forward");
        let reply = messaging
            .ask(target, Query { value: 3 }, Duration::from_secs(30))
            .await
            .expect("typed ask");

        assert_eq!(reply.value, 41);
        let tell_senders = backend.tell_senders.lock().expect("tell sender mutex");
        assert!(
            tell_senders[0]
                .as_ref()
                .is_some_and(|sender| { sender.same_activation(&self_ref) })
        );
        assert!(
            tell_senders[1]
                .as_ref()
                .is_some_and(|sender| { sender.same_activation(&original_sender) })
        );
        let ask_deadlines = backend.ask_deadlines.lock().expect("ask deadline mutex");
        assert_eq!(ask_deadlines.as_slice(), &[parent_deadline]);
    }

    fn actor_ref(segment: &str, sequence: u64) -> ActorRef {
        let node_incarnation = NodeIncarnation::new(7).expect("node incarnation");
        ActorRef::new(
            ClusterId::new("turn-test").expect("cluster ID"),
            NodeAddress::new("127.0.0.1", 19097).expect("node address"),
            node_incarnation,
            ActorPath::user([segment]).expect("actor path"),
            ActivationId::new(node_incarnation, sequence).expect("activation ID"),
            ProtocolId::new(97).expect("protocol ID"),
        )
        .expect("actor ref")
    }
}
