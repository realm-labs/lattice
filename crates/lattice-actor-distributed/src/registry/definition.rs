//! Actor category contracts independent of their server implementations.

use lattice_model::actor::ProtocolTag;

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
