#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

pub mod aggregate;
pub mod config_store;
pub mod dns;
pub mod provider;
pub mod static_provider;

#[cfg(test)]
mod tests;
