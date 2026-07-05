pub mod context;
pub mod error;
pub mod handle;
pub mod mailbox;
pub mod registry;
pub mod remote_watch;
pub mod runtime;
pub mod traits;
pub mod watch;

pub use context::ActorContext;
pub use error::{ActorCallError, ActorError, ActorSpawnError, ActorStopError, ActorTellError};
pub use handle::ActorHandle;
pub use mailbox::MailboxConfig;
pub use registry::{ActorFactory, ActorLoader, ActorRegistry};
pub use runtime::{
    ActorRuntime, ActorRuntimeConfig, ActorSpawnOptions, PassivationPolicy, spawn_actor,
};
pub use traits::{Actor, Handler, HandlerErrorAction, Message, PassivationReason, StopReason};

#[cfg(test)]
mod tests;
