//! Node-local addressing keys.
//!
//! Lattice carries two identity vocabularies and they are not interchangeable:
//!
//! - This module plus [`crate::kind`] hold the *node-local* vocabulary. [`ActorId`] and
//!   [`crate::kind::ActorKind`] are application-shaped keys used for registry lookups. They are
//!   deliberately unvalidated and unbounded so applications can key actors by whatever their
//!   domain uses.
//! - [`crate::actor_address`] holds the *boundary* vocabulary. [`crate::actor_address::EntityId`],
//!   [`crate::actor_address::EntityType`] and friends are canonical and length-bounded
//!   because they are serialized into references that cross nodes.
//!
//! The two meet at exactly two conversion points, and both bound the local value:
//!
//! - `lattice_actor_distributed::registry` encodes an [`ActorId`] and an
//!   [`crate::kind::ActorKind`] into [`crate::actor_address::ActorPath`] segments. The
//!   hex encoding doubles the byte length, so identities above
//!   `MAX_ACTOR_PATH_SEGMENT_BYTES / 2` produce no addressable reference and the
//!   activation stays node-local.
//! - `lattice_service::cluster` maps `ActorId::Bytes` to
//!   [`crate::actor_address::EntityId`] through its checked constructor, so oversized
//!   or empty payloads never reach a placement route.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ActorId {
    Str(String),
    U64(u64),
    I64(i64),
    Bytes(Vec<u8>),
}
