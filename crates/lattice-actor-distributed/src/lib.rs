//! Distributed addressing, protocols, hosting, and routing for `lattice-actor`.
//!
//! The local Actor runtime lives in `lattice-actor`; this crate adapts typed
//! protocol addresses to local handles or remoting without changing the local
//! runtime's data model.

#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

pub use lattice_actor::{
    Message, Request, actor_behavior, attachments, context, error, handle, mailbox, observation,
    reply, runtime, service, state_machine, traits, watch,
};

pub mod activation;
pub mod directory;
pub mod entity;
pub mod host;
pub mod protocol;
pub mod recipient;
pub mod reference;
pub mod registry;

pub use entity::{EntityActivationState, EntityKey, EntityKeyDecodeError, ShardedActor};
pub use reference::{ActorRef, EntityRef, Recipient, SingletonRef};
pub use registry::{ActorKey, ActorKind};
