//! Addressing vocabulary for the cluster boundary.
//!
//! [`identity`] holds the validated, length-bounded identity primitives and
//! [`addresses`] composes them into the actor, entity and singleton addresses that
//! cross the wire. Both are published through this module so the addressing
//! path stays a single import.

mod addresses;
mod identity;

pub use addresses::*;
pub use identity::*;
