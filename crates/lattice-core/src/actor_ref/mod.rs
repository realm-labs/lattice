//! Addressing vocabulary for the cluster boundary.
//!
//! [`identity`] holds the validated, length-bounded identity primitives and
//! [`refs`] composes them into the actor, entity and singleton references that
//! cross the wire. Both are published through this module so the addressing
//! path stays a single import.

mod identity;
mod refs;

pub use identity::*;
pub use refs::*;
