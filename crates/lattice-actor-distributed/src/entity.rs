//! Distributed entity identity and activation concepts.

use lattice_actor::traits::Actor;
use lattice_model::actor::ActorId;
use thiserror::Error;

/// Stable conversion between a domain key and its distributed entity ID.
pub trait EntityKey: Clone + Send + Sync + 'static {
    fn to_entity_id(&self) -> Result<ActorId, EntityKeyDecodeError>;
    fn try_from_entity_id(entity_id: &ActorId) -> Result<Self, EntityKeyDecodeError>;
}

/// An Actor addressed through a distributed entity key.
pub trait ShardedActor: Actor {
    type Key: EntityKey;
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("entity key encoding is invalid: {reason}")]
pub struct EntityKeyDecodeError {
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityActivationState {
    Absent,
    Activating,
    Loading,
    Active,
}
