use std::any::type_name;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::Instrument;

use lattice_core::actor_ref::ActorRef;
use lattice_core::direct_link::runtime::DirectLinkManager;
use lattice_core::service_context::ServiceContext;

use crate::error::ActorError;
use crate::handle::ActorHandle;
use crate::traits::{
    Actor, ChildActorKey, ChildActorOptions, ChildSupervision, Handler, Message, PassivationReason,
    StopReason,
};
use crate::watch::{ActorTerminated, WatchId};

pub struct ActorContext<A: Actor> {
    handle: ActorHandle<A>,
    self_ref: Option<ActorRef>,
    service: ServiceContext,
    lifecycle_request: Option<StopReason>,
    tasks: Vec<JoinHandle<()>>,
    watches: HashMap<WatchId, JoinHandle<()>>,
    children: HashMap<ChildActorKey, Box<dyn ChildStop>>,
    next_watch_id: u64,
}

impl<A: Actor> fmt::Debug for ActorContext<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorContext")
            .field("handle", &self.handle)
            .field("self_ref", &self.self_ref)
            .field("service", &self.service)
            .field("lifecycle_request", &self.lifecycle_request)
            .field("task_count", &self.tasks.len())
            .field("watch_count", &self.watches.len())
            .field("child_count", &self.children.len())
            .field("next_watch_id", &self.next_watch_id)
            .finish()
    }
}

impl<A: Actor> ActorContext<A> {
    pub(crate) fn new(
        handle: ActorHandle<A>,
        self_ref: Option<ActorRef>,
        service: ServiceContext,
    ) -> Self {
        Self {
            handle,
            self_ref,
            service,
            lifecycle_request: None,
            tasks: Vec::new(),
            watches: HashMap::new(),
            children: HashMap::new(),
            next_watch_id: 0,
        }
    }

    pub fn self_ref(&self) -> Option<&ActorRef> {
        self.self_ref.as_ref()
    }

    pub fn service(&self) -> &ServiceContext {
        &self.service
    }

    pub fn links(&self) -> DirectLinkManager {
        DirectLinkManager::new(self.service.clone(), self.self_ref.clone())
    }

    pub fn require_self_ref(&self) -> Result<&ActorRef, ActorError> {
        self.self_ref
            .as_ref()
            .ok_or_else(|| ActorError::new("actor self ref is not available"))
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
        M: Message<Reply = ()>,
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
        M: Message<Reply = ()>,
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
        self.tasks.push(tokio::spawn(future));
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
        let handle =
            crate::runtime::spawn_actor_with_context(actor, options.mailbox, self.service.clone());
        let slot = Arc::new(ChildSlot::new(handle.clone()));
        self.children
            .insert(key, Box::new(ChildSlotStopper(slot.clone())));
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
        let handle = crate::runtime::spawn_actor_with_context(
            factory(),
            options.mailbox,
            self.service.clone(),
        );
        let slot = Arc::new(ChildSlot::new(handle.clone()));
        self.children
            .insert(key, Box::new(ChildSlotStopper(slot.clone())));
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
        for task in self.tasks.drain(..) {
            task.abort();
        }
        for (_watch_id, task) in self.watches.drain() {
            task.abort();
        }
    }

    pub(crate) fn stop_all_children(&mut self, reason: StopReason) {
        for (_key, child) in self.children.drain() {
            child.stop(reason);
        }
    }

    pub(crate) fn take_lifecycle_request(&mut self) -> Option<StopReason> {
        self.lifecycle_request.take()
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
                self.spawn_scoped(async move {
                    loop {
                        if terminations.recv().await.is_err() {
                            break;
                        }
                        let replacement = crate::runtime::spawn_actor_with_context(
                            factory(),
                            options.mailbox,
                            service.clone(),
                        );
                        terminations = replacement.subscribe_terminated();
                        slot.replace(replacement);
                    }
                });
            }
        }
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

struct ChildSlotStopper<C: Actor>(Arc<ChildSlot<C>>);

impl<C: Actor> ChildStop for ChildSlotStopper<C> {
    fn stop(self: Box<Self>, reason: StopReason) {
        if let Some(handle) = self.0.current.lock().expect("child slot poisoned").take() {
            let _ = handle.try_stop_internal(reason);
        }
    }
}
