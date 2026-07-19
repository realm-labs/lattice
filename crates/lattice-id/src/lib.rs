//! Distributed and owner-local Snowflake ID generation.
//!
//! [`snowflake`] contains the allocation-free ID algorithms. [`worker`]
//! defines the fenced worker-ID lease contract and an in-memory implementation.
//! [`service`] keeps a process-wide lease alive and gates generation whenever
//! ownership cannot be proven. Backend crates, such as `lattice-id-etcd`,
//! implement the lease contract without adding network work to the ID hot path.
//!
//! The distributed service is deliberately independent from actors and
//! `LatticeService`: applications start it explicitly and retain its lifecycle
//! handle.
//!
//! # Choosing a generator
//!
//! - Use [`snowflake::SnowflakeIdGenerator`] when the worker ID is assigned by
//!   some other trusted mechanism.
//! - Use [`snowflake::LocalIdGenerator`] for IDs whose uniqueness only needs to
//!   hold inside one actor or owner. It is intentionally not `Sync`.
//! - Use [`service::DistributedIdService`] with a [`worker::WorkerIdLeaseStore`]
//!   when nodes must coordinate worker IDs. Clone the returned generator into
//!   request paths and retain the service as the lifecycle handle.
//!
//! # Distributed usage
//!
//! ```
//! use std::sync::Arc;
//!
//! use lattice_core::actor_ref::{ClusterId, NodeIncarnation};
//! use lattice_id::{
//!     service::{DistributedIdConfig, DistributedIdService},
//!     worker::{InMemoryWorkerIdLeaseStore, WorkerIdOwner},
//! };
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let store = Arc::new(InMemoryWorkerIdLeaseStore::default());
//! let owner = WorkerIdOwner::for_node(
//!     ClusterId::new("game-production")?,
//!     "game-01",
//!     NodeIncarnation::new(1)?,
//! )?;
//! let service = DistributedIdService::start(
//!     store,
//!     owner,
//!     DistributedIdConfig::default(),
//! )
//! .await?;
//!
//! // Generation does not perform network I/O. The async form waits only when
//! // all sequence values for the current millisecond have been consumed.
//! let ids = service.generator();
//! let immediate = ids.try_next_id()?;
//! let waited = ids.next_id().await?;
//! assert_ne!(immediate, waited);
//!
//! service.shutdown().await?;
//! # Ok(())
//! # }
//! ```
//!
//! A production application substitutes a backend such as
//! `lattice-id-etcd`. If lease ownership can no longer be proven, both
//! generation methods fail immediately with
//! [`service::DistributedIdError::LeaseUnavailable`]. The service keeps trying
//! to reacquire a worker ID in the background, and existing generator clones
//! resume after it becomes active again. Reused worker IDs observe a clock-skew
//! cooldown before generation is enabled.

#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

pub mod service;
pub mod snowflake;
pub mod worker;
