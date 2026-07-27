//! Node-local addressing keys.
//!
//! Lattice carries two identity vocabularies and they are not interchangeable:
//!
//! - This module plus [`crate::kind`] hold the *node-local* vocabulary. [`ActorId`],
//!   [`RouteKey`] and [`crate::kind::ActorKind`] are application-shaped keys used for
//!   registry lookups and gateway routing. They are deliberately unvalidated and
//!   unbounded so applications can key actors by whatever their domain uses.
//! - [`crate::actor_ref`] holds the *boundary* vocabulary. [`crate::actor_ref::EntityId`],
//!   [`crate::actor_ref::EntityType`] and friends are canonical and length-bounded
//!   because they are serialized into references that cross nodes.
//!
//! The two meet at exactly two conversion points, and both bound the local value:
//!
//! - `lattice_actor::registry` encodes an [`ActorId`] and an
//!   [`crate::kind::ActorKind`] into [`crate::actor_ref::ActorPath`] segments. The
//!   hex encoding doubles the byte length, so identities above
//!   `MAX_ACTOR_PATH_SEGMENT_BYTES / 2` produce no addressable reference and the
//!   activation stays node-local.
//! - `lattice_service::cluster` maps `ActorId::Bytes` to
//!   [`crate::actor_ref::EntityId`] through its checked constructor, so oversized
//!   or empty payloads never reach a placement route.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum RouteKey {
    Str(String),
    U64(u64),
    I64(i64),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ActorId {
    Str(String),
    U64(u64),
    I64(i64),
    Bytes(Vec<u8>),
}

impl ActorId {
    pub fn to_route_key(&self) -> RouteKey {
        match self {
            Self::Str(value) => RouteKey::Str(value.clone()),
            Self::U64(value) => RouteKey::U64(*value),
            Self::I64(value) => RouteKey::I64(*value),
            Self::Bytes(value) => RouteKey::Bytes(value.clone()),
        }
    }
}

pub trait ActorKey: Clone + Send + Sync + 'static {
    fn to_route_key(&self) -> RouteKey;
    fn to_actor_id(&self) -> ActorId;
    fn try_from_actor_id(actor_id: &ActorId) -> Result<Self, ActorKeyDecodeError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("failed to decode actor key: {reason}")]
pub struct ActorKeyDecodeError {
    pub reason: String,
}
