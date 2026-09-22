//! Application-defined keys used by the distributed activation registry.

use lattice_model::actor::ProtocolTag;
use serde::{Deserialize, Serialize};

/// Application-shaped key for one registry activation.
///
/// A key is local registry vocabulary. It is encoded into a validated
/// [`ActorPath`](lattice_model::actor::ActorPath) only when an activation is published.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ActorKey {
    Str(String),
    U64(u64),
    I64(i64),
    Bytes(Vec<u8>),
}

/// Public contract for an Actor category, independent of its server implementation.
///
/// Define a marker in the shared protocol crate and implement this trait there. Multiple
/// definitions may share a protocol, but each hosted category must have a distinct stable name.
/// The name becomes part of published Actor paths; changing it changes those paths.
/// `ActorRegistry<D, A>` binds this definition to a concrete Actor implementation. Its protocol
/// binding must use `D::Protocol`, so callers cannot substitute an unrelated protocol.
///
/// Use `ErasedProtocol` for a definition used only by a node-local registry. Such a definition
/// cannot be passed to `new_bound`, which requires an actual wire protocol.
pub trait ActorDefinition: Send + Sync + 'static {
    const NAME: &'static str;
    type Protocol: ProtocolTag;
}
