//! MongoDB persistence, snapshot-diff boundaries, and actor-local coordination.

pub mod actor;
pub mod bson_serde;
pub mod coordinator;
pub mod direct;
pub mod document;
pub mod document_set;
pub mod error;
pub mod mongo;
pub mod mongo_store;
pub mod prepared;
pub mod scan;
pub mod tracked;

pub use lattice_store_mongodb_macros::{MongoDocument, MongoDocumentSet, MongoScan};

pub use document_set::{MongoDocumentCollection, MongoDocumentSet};
pub use error::{MongoStoreError, MongoStoreErrorKind};

extern crate self as lattice_store_mongodb;
