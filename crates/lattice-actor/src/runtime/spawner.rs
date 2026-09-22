use crate::error::ActorSpawnError;
use crate::handle::ActorHandle;
use crate::traits::Actor;
use crate::{
    attachments::ActorRuntimeAttachments, environment::ActorEnvironment,
    observation::ActorObserverHandle, watch::LocalActorRef,
};

use super::{ActorExecutionPolicy, ActorScheduler, ActorSpawnContext, ActorSpawnOptions};

/// Cloneable execution entry point. Does not keep its owning runtime alive.
///
/// The owner must outlive all spawned Actors. Once it is dropped, spawning fails
/// with [`ActorSpawnError::ExecutorStartFailed`] instead of creating another executor.
#[derive(Clone)]
pub struct ActorSpawner {
    scheduler: ActorScheduler,
    default_execution: ActorExecutionPolicy,
    environment: ActorEnvironment,
    observer: ActorObserverHandle,
}

impl ActorSpawner {
    pub(super) fn new(
        scheduler: ActorScheduler,
        default_execution: ActorExecutionPolicy,
        environment: ActorEnvironment,
        observer: ActorObserverHandle,
    ) -> Self {
        Self {
            scheduler,
            default_execution,
            environment,
            observer,
        }
    }

    pub fn environment(&self) -> &ActorEnvironment {
        &self.environment
    }

    /// Overrides observation for this entry point without changing its executors.
    pub fn with_observer(mut self, observer: ActorObserverHandle) -> Self {
        self.observer = observer;
        self
    }

    pub fn spawn_actor<A: Actor>(
        &self,
        actor: A,
        options: ActorSpawnOptions,
    ) -> Result<ActorHandle<A>, ActorSpawnError> {
        self.spawn_managed_actor(actor, options, ActorRuntimeAttachments::empty(), None)
    }

    /// Spawns an activation with integration attachments and terminal cleanup.
    #[doc(hidden)]
    pub fn spawn_managed_actor<A: Actor>(
        &self,
        actor: A,
        options: ActorSpawnOptions,
        runtime_attachments: ActorRuntimeAttachments,
        terminal_hook: Option<Box<dyn FnOnce(LocalActorRef) + Send + 'static>>,
    ) -> Result<ActorHandle<A>, ActorSpawnError> {
        self.spawn(
            actor,
            ActorSpawnContext {
                options,
                environment: self.environment.clone(),
                observer: self.observer.clone(),
                terminal_hook,
                runtime_attachments,
                spawner: self.clone(),
            },
        )
    }

    pub(super) fn spawn<A>(
        &self,
        actor: A,
        context: ActorSpawnContext,
    ) -> Result<ActorHandle<A>, ActorSpawnError>
    where
        A: Actor,
    {
        let execution = context
            .options
            .execution
            .clone()
            .unwrap_or_else(|| self.default_execution.clone());
        self.scheduler.spawn(actor, context, execution)
    }
}
