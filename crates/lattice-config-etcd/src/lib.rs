#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

pub mod client;
pub mod codec;
pub mod config;
pub mod store;
pub mod watch;

#[cfg(test)]
mod tests;
