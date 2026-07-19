//! MongoDB persistence, snapshot-diff boundaries, and actor-local coordination.

pub mod document;
pub mod error;
pub mod loading;
pub mod persistence;
pub mod scan;
pub mod store;

pub use lattice_store_mongodb_macros::{MongoDocument, MongoDocumentSet, MongoScan};

extern crate self as lattice_store_mongodb;
