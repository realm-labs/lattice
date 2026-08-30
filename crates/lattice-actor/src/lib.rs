//! Process-local typed Actor runtime.
//!
//! This crate contains local handles, mailboxes, scheduling, supervision,
//! timers, and lifecycle management. Serializable addresses, protocols,
//! registries, and remoting live in `lattice-actor-distributed`.

#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

extern crate self as lattice_actor;

pub use lattice_actor_macros::{Message, Request, actor_behavior};

pub mod context;
pub mod error;
pub mod handle;
pub mod mailbox;
pub mod observation;
pub mod reply;
pub mod resources;
pub mod runtime;
pub mod state_machine;
pub mod traits;
pub mod watch;

#[cfg(test)]
mod tests;
