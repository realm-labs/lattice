//! Bounded TCP and TLS remoting for Lattice.
//!
//! # TLS crypto providers
//!
//! The default `rustls-ring` feature enables TLS with the ring provider. Applications that prefer
//! AWS-LC can disable default features and enable `rustls-aws-lc` instead. The features are
//! additive, so applications may enable both and select a provider explicitly. Disabling default
//! features without enabling `tls` builds plaintext TCP remoting without the TLS dependencies.
//!
//! This crate accepts application-built rustls client and server configurations and does not
//! install a process-wide default crypto provider. Applications remain responsible for selecting
//! and installing a process default when another TLS dependency relies on rustls' implicit
//! builders.

#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

pub mod association;
pub mod bootstrap;
pub mod config;
pub mod control;
pub mod endpoint;
pub mod handshake;
pub mod lane;
pub mod messaging;
pub mod protocol;
pub mod transport;
pub mod watch;
pub mod wire;
