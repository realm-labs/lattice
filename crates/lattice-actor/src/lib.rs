mod context;
mod error;
mod handle;
mod mailbox;
mod registry;
mod remote_watch;
mod runtime;
mod traits;
mod watch;

pub use context::ActorContext;
pub use error::{
    ActorActivationError, ActorCallError, ActorError, ActorSpawnError, ActorStopError,
    ActorTellError,
};
pub use handle::ActorHandle;
pub use mailbox::MailboxConfig;
pub use registry::{
    ActorCreateContext, ActorFactory, ActorLoader, ActorRefConfig, ActorRegistry,
    ActorRegistryConfig,
};
pub use remote_watch::{CrossNodeWatchRegistry, RemoteActorRef, RemoteWatchEvent};
pub use runtime::{
    ActorExecutionPolicy, ActorRuntime, ActorRuntimeConfig, ActorScheduler, ActorSpawnOptions,
    PassivationPolicy, spawn_actor,
};
pub use traits::{
    Actor, ActorLifecycleState, ChildActorKey, ChildActorOptions, ChildSupervision, Handler,
    Message, PassivationReason, StopReason,
};
pub use watch::{ActorIncarnation, ActorTerminated, LocalActorRef, TerminatedReason, WatchId};

#[cfg(test)]
mod tests;
