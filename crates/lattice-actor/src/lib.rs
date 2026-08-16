//! Process-local typed Actor runtime.
//!
//! The default build contains local handles, mailboxes, scheduling, supervision, timers, and
//! lifecycle management. Enable the `distributed` feature for serializable Actor references,
//! protocol dispatch, registries, remote recipients, and remoting integration.

#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

extern crate self as lattice_actor;

pub use lattice_actor_macros::{Message, Request, actor_behavior};

pub mod context;
#[cfg(feature = "distributed")]
pub mod directory;
pub mod error;
pub mod handle;
#[cfg(feature = "distributed")]
pub mod host;
pub mod mailbox;
pub mod observation;
#[cfg(feature = "distributed")]
pub mod protocol;
#[cfg(feature = "distributed")]
pub mod recipient;
#[cfg(feature = "distributed")]
pub mod registry;
pub mod reply;
pub mod runtime;
pub mod state_machine;
pub mod traits;
pub mod watch;

#[cfg(test)]
mod tests;
