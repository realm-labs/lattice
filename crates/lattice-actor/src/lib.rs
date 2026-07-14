#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

extern crate self as lattice_actor;

pub use lattice_actor_macros::{Message, Request};

pub mod context;
pub mod directory;
pub mod error;
pub mod handle;
pub mod host;
pub mod mailbox;
pub mod observation;
pub mod protocol;
pub mod recipient;
pub mod registry;
pub mod reply;
pub mod runtime;
pub mod traits;
pub mod watch;

#[cfg(test)]
mod tests;
