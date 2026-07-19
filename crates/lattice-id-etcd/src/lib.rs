//! Etcd-backed worker-ID leases for `lattice-id`.
//!
//! Slot keys use native Etcd leases while persistent history keys distinguish
//! first allocation from reuse. All mutations are guarded by transactions and
//! the opaque fencing token issued with each lease.
//!
//! Create the backend explicitly and pass it to
//! [`lattice_id::service::DistributedIdService`]:
//!
//! ```no_run
//! use lattice_id_etcd::{
//!     config::EtcdWorkerIdStoreConfig,
//!     store::EtcdWorkerIdLeaseStore,
//! };
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let store = EtcdWorkerIdLeaseStore::connect(EtcdWorkerIdStoreConfig {
//!     endpoints: vec!["http://127.0.0.1:2379".to_string()],
//!     key_prefix: "/lattice/worker-ids".to_string(),
//! })
//! .await?;
//! # let _ = store;
//! # Ok(())
//! # }
//! ```
//!
//! Worker IDs are isolated by the owner's cluster ID. Slot keys expire with
//! native Etcd leases, while history keys deliberately remain so a later owner
//! applies the reuse cooldown. Fencing tokens are redacted from `Debug`; do not
//! expose them through application logging.

#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

pub mod config;
pub mod store;
