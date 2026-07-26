//! Child activation ownership and supervision.
//!
//! A parent holds every child through a [`ChildSlot`] so that supervision restarts, directory
//! bookkeeping, and stop propagation all observe the same activation, even after a replacement
//! takes over the slot.

use std::{
    any::type_name,
    sync::{Arc, Mutex, OnceLock},
};

use lattice_core::{
    actor_ref::{ActorRef, ProtocolId},
    service_context::ServiceContext,
};
use tokio::task::AbortHandle;

use super::ActorContext;
use crate::{
    directory::ActivationDirectory,
    error::{ActorError, ActorTellError},
    handle::{ActorHandle, TerminalHook},
    observation::ActorObserverHandle,
    recipient::ActorSystem,
    runtime::{ActorSpawnContext, ActorSpawnOptions, PassivationPolicy, spawner::ActorSpawner},
    traits::{Actor, ChildActorKey, ChildActorOptions, ChildSupervision, StopReason},
};

impl<A: Actor> ActorContext<A> {
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
        let handle = self
            .child_spawn_env()
            .spawn(actor, &options, child_ref.clone())?;
        self.adopt_child(key, &options, handle.clone(), child_ref, None::<fn() -> C>);
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
        let handle = self
            .child_spawn_env()
            .spawn(factory(), &options, child_ref.clone())?;
        self.adopt_child(key, &options, handle.clone(), child_ref, Some(factory));
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

    pub(crate) fn stop_all_children(&mut self, reason: StopReason) {
        for (_key, child) in self.children.drain() {
            child.stop(reason);
        }
    }

    fn child_spawn_env(&self) -> ChildSpawnEnv {
        ChildSpawnEnv {
            service: self.service.clone(),
            actor_system: self.actor_system.clone(),
            observer: self.handle.observer().clone(),
            spawner: self.spawner.clone(),
        }
    }

    fn adopt_child<C, F>(
        &mut self,
        key: ChildActorKey,
        options: &ChildActorOptions,
        handle: ActorHandle<C>,
        reference: Option<ActorRef>,
        factory: Option<F>,
    ) where
        C: Actor,
        F: FnMut() -> C + Send + 'static,
    {
        let slot = Arc::new(ChildSlot::new(handle, reference));
        self.children.insert(
            key,
            Box::new(ChildSlotStopper {
                slot: slot.clone(),
                directory: self.service.extension::<ActivationDirectory>(),
            }),
        );
        self.spawn_supervision_task(slot, options, factory);
    }

    fn spawn_supervision_task<C, F>(
        &mut self,
        slot: Arc<ChildSlot<C>>,
        options: &ChildActorOptions,
        mut factory: Option<F>,
    ) where
        C: Actor,
        F: FnMut() -> C + Send + 'static,
    {
        let supervision = match options.supervision {
            ChildSupervision::StopChild => return,
            ChildSupervision::StopParent => {
                let Some(activation) = slot.snapshot() else {
                    return;
                };
                let parent = self.handle.clone();
                let mut terminations = activation.handle.subscribe_terminated();
                self.spawn_scoped_task(async move {
                    if terminations.recv().await.is_ok() {
                        let _ = parent.try_stop_internal(StopReason::Requested);
                    }
                })
            }
            ChildSupervision::RestartChild => {
                let (Some(mut factory), Some(activation)) = (factory.take(), slot.snapshot())
                else {
                    return;
                };
                let mut terminations = activation.handle.subscribe_terminated();
                let mut reference = activation.reference;
                let env = self.child_spawn_env();
                let options = options.clone();
                let supervised = slot.clone();
                self.spawn_scoped_task(async move {
                    loop {
                        if terminations.recv().await.is_err() {
                            break;
                        }
                        // The replacement is a distinct activation, so it takes a fresh activation
                        // ID. References to the dead child must never resolve to it.
                        reference = match reference.as_ref().map(next_child_activation).transpose()
                        {
                            Ok(reference) => reference,
                            Err(error) => {
                                tracing::warn!(
                                    %error,
                                    "supervised child replacement reference could not be derived"
                                );
                                break;
                            }
                        };
                        let replacement = match env.spawn(factory(), &options, reference.clone()) {
                            Ok(replacement) => replacement,
                            Err(error) => {
                                tracing::warn!(
                                    %error,
                                    "supervised child could not be restarted"
                                );
                                break;
                            }
                        };
                        terminations = replacement.subscribe_terminated();
                        supervised.replace(replacement, reference.clone());
                    }
                })
            }
        };
        slot.set_supervision(supervision);
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

pub(super) trait ChildStop: Send {
    fn stop(self: Box<Self>, reason: StopReason);
}

/// Shared spawn inputs for a parent's children, usable from a supervision task
/// that no longer holds the [`ActorContext`].
struct ChildSpawnEnv {
    service: ServiceContext,
    actor_system: Option<Arc<OnceLock<ActorSystem>>>,
    observer: ActorObserverHandle,
    spawner: ActorSpawner,
}

impl ChildSpawnEnv {
    fn spawn<C>(
        &self,
        actor: C,
        options: &ChildActorOptions,
        reference: Option<ActorRef>,
    ) -> Result<ActorHandle<C>, ActorError>
    where
        C: Actor,
    {
        let directory = self.service.extension::<ActivationDirectory>();
        let terminal_hook: Option<TerminalHook> = match (directory.clone(), reference.clone()) {
            (Some(directory), Some(reference)) => Some(Box::new(move |_local_ref| {
                directory.remove(&reference);
            })),
            _ => None,
        };
        let handle = crate::runtime::spawn_actor_with_self_ref(
            actor,
            ActorSpawnContext {
                options: ActorSpawnOptions {
                    mailbox: options.mailbox,
                    execution: Some(options.execution),
                    scheduler_key: options.scheduler_key.clone(),
                    passivation: PassivationPolicy::Disabled,
                    self_ref: reference.clone(),
                    service: self.service.clone(),
                },
                actor_system: self.actor_system.clone(),
                observer: self.observer.clone(),
                terminal_hook,
                spawner: self.spawner.clone(),
            },
        )
        .map_err(|error| ActorError::new(error.to_string()))?;
        if let Some(directory) = &directory {
            if let Err(error) = directory.register(&handle) {
                let _ = handle.try_stop_internal(StopReason::StartFailed);
                return Err(ActorError::new(error.to_string()));
            }
            if handle.terminal_cleanup_started()
                && let Some(reference) = &reference
            {
                directory.remove(reference);
            }
        }
        Ok(handle)
    }
}

fn next_child_activation(previous: &ActorRef) -> Result<ActorRef, ActorError> {
    ActorRef::new(
        previous.cluster_id().clone(),
        previous.node_address().clone(),
        previous.node_incarnation(),
        previous.actor_path().clone(),
        crate::runtime::next_activation_id(previous.node_incarnation()),
        previous.protocol_id(),
    )
    .map_err(|error| ActorError::new(error.to_string()))
}

fn request_child_stop<C>(handle: ActorHandle<C>, reason: StopReason)
where
    C: Actor,
{
    let Err(ActorTellError::MailboxFull(reason)) = handle.try_stop_internal(reason) else {
        return;
    };
    // The parent has already released this child, so a full system lane would otherwise leave it
    // running with no owner able to retry.
    tokio::spawn(async move {
        let _ = handle.stop_when_capacity_internal(reason).await;
    });
}

struct ChildActivation<C: Actor> {
    handle: ActorHandle<C>,
    reference: Option<ActorRef>,
}

struct ChildSlot<C: Actor> {
    current: Mutex<Option<ChildActivation<C>>>,
    supervision: Mutex<Option<AbortHandle>>,
}

impl<C: Actor> ChildSlot<C> {
    fn new(handle: ActorHandle<C>, reference: Option<ActorRef>) -> Self {
        Self {
            current: Mutex::new(Some(ChildActivation { handle, reference })),
            supervision: Mutex::new(None),
        }
    }

    fn snapshot(&self) -> Option<ChildActivation<C>> {
        self.current
            .lock()
            .expect("child slot poisoned")
            .as_ref()
            .map(|activation| ChildActivation {
                handle: activation.handle.clone(),
                reference: activation.reference.clone(),
            })
    }

    fn take(&self) -> Option<ChildActivation<C>> {
        self.current.lock().expect("child slot poisoned").take()
    }

    fn replace(&self, handle: ActorHandle<C>, reference: Option<ActorRef>) {
        *self.current.lock().expect("child slot poisoned") =
            Some(ChildActivation { handle, reference });
    }

    fn set_supervision(&self, task: AbortHandle) {
        *self.supervision.lock().expect("child slot poisoned") = Some(task);
    }

    fn abort_supervision(&self) {
        if let Some(task) = self.supervision.lock().expect("child slot poisoned").take() {
            task.abort();
        }
    }
}

struct ChildSlotStopper<C: Actor> {
    slot: Arc<ChildSlot<C>>,
    directory: Option<Arc<ActivationDirectory>>,
}

impl<C: Actor> ChildStop for ChildSlotStopper<C> {
    fn stop(self: Box<Self>, reason: StopReason) {
        // Releasing a child also ends its supervision: a replacement would belong to no parent,
        // would not be stopped with one, and would keep its own mailbox alive forever.
        self.slot.abort_supervision();
        let Some(activation) = self.slot.take() else {
            return;
        };
        if let (Some(directory), Some(reference)) = (&self.directory, &activation.reference) {
            directory.remove(reference);
        }
        request_child_stop(activation.handle, reason);
    }
}
