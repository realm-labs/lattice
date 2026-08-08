#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

pub mod actor_ref;
pub mod coordinator;
pub mod failpoint;
pub mod id;
pub mod instance;
pub mod kind;
pub mod release;
pub mod service_context;
pub mod trace;
pub mod watch;

#[cfg(test)]
mod tests;
