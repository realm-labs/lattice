#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

mod actor_watch;
mod addresses;
mod coordination;
mod primitives;
mod release_model;

pub mod actor;
pub mod cluster;
pub mod service;
pub mod trace;

pub use primitives::ModelError;

#[cfg(test)]
mod tests;
