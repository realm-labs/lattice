//! Cluster services and lifecycle assembly for Lattice.
//!
//! TLS support follows `lattice-remoting`: the default `rustls-ring` feature enables TLS with
//! ring, while `rustls-aws-lc` selects AWS-LC when default features are disabled. Building with
//! default features disabled omits the endpoint TLS configuration API and uses plaintext remoting.

#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

pub mod backend;
pub mod builder;
pub mod cluster;
pub mod config;
mod control;
pub mod deployment;
pub mod error;
mod exact_tell_routes;
pub mod lifecycle;
pub mod registration;
pub mod supervisor;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
