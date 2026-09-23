//! Actor identities, addresses, protocols, and wire-level watch values.

pub use crate::actor_watch::{TerminatedReason, WatchId, WatchStatus};
pub use crate::addresses::{ActorAddress, EntityAddress, RecipientAddress, SingletonAddress};
pub use crate::primitives::{
    ActivationId, ActorId, ActorPath, ErasedProtocol, ProtocolId, ProtocolTag,
};
