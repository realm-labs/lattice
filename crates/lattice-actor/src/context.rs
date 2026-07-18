use std::{
    any::type_name,
    collections::HashMap,
    fmt,
    future::Future,
    sync::{Arc, Mutex, OnceLock},
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

pub struct ActorContext<A: Actor> {
    handle: ActorHandle<A>,
    self_ref: Option<ActorRef>,
    actor_system: Option<Arc<OnceLock<ActorSystem>>>,
    service: ServiceContext,
    spawner: ActorSpawner,
    lifecycle_request: Option<StopReason>,
    tasks: JoinSet<()>,
    pipe_tasks: JoinSet<()>,
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
            .field("lifecycle_request", &self.lifecycle_request)
            .field("task_count", &self.tasks.len())
            .field("pipe_task_count", &self.pipe_tasks.len())
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
            spawner,
            lifecycle_request: None,
            tasks: JoinSet::new(),
            pipe_tasks: JoinSet::new(),
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

    /// Sends to a process-local handle with this actor as the envelope sender.
    pub fn tell_local<B, M>(
        &self,
        target: &ActorHandle<B>,
        message: M,
    ) -> Result<(), ActorTellError>
    where
        B: Actor + Handler<M>,
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
    ) -> Result<(), ActorTellError>
    where
        B: Actor + Handler<M>,
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
    /// capacity and is aborted when the actor stops.
    ///
    /// Use [`Self::defer_reply`] when the continuation owns an ask reply token
    /// and must inherit that request's deadline and failure semantics.
    pub fn pipe_to_self<Fut, Map, M>(
        &mut self,
        future: Fut,
        map: Map,
    ) -> Result<(), PipeToSelfError>
    where
        A: Handler<M>,
        M: Message,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
        Map: FnOnce(Fut::Output) -> M + Send + 'static,
    {
        self.reserve_pipe_task()?;
        let handle = self.handle.clone();
        self.pipe_tasks.spawn(async move {
            let message = map(future.await);
            if let Err(error) = handle.send_tell_internal(message).await {
                tracing::debug!(
                    actor.type = type_name::<A>(),
                    %error,
                    "actor pipe-to-self continuation was not delivered"
                );
            }
        });
        Ok(())
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
        M: Message,
        T: Send + 'static,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
        Map: FnOnce(Fut::Output, ReplyTo<T>) -> M + Send + 'static,
    {
        let control = reply_to.control();
        if let Err(error) = self.reserve_pipe_task() {
            control.cancel(ActorCallError::MailboxFull);
            return Err(error);
        }

        let handle = self.handle.clone();
        let deadline = control.deadline();
        self.pipe_tasks.spawn(async move {
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

    fn reserve_pipe_task(&mut self) -> Result<(), PipeToSelfError> {
        self.reap_runtime_work();
        if self.pipe_tasks.len() >= self.deferred_capacity {
            Err(PipeToSelfError::Capacity {
                capacity: self.deferred_capacity,
            })
        } else {
            Ok(())
        }
    }

    pub fn watch<B>(&mut self, target: &ActorHandle<B>) -> Result<WatchId, ActorError>
    where
        A: Handler<ActorTerminated>,
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
                    let _ = self_handle.try_tell_internal(notification);
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
        self.pipe_tasks.abort_all();
    }

    pub(crate) fn reap_runtime_work(&mut self) {
        Self::reap_tasks(&mut self.tasks, "scoped");
        Self::reap_tasks(&mut self.pipe_tasks, "pipe_to_self");
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
