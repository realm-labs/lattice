//! MongoDB persistence for actor-owned Lattice state.
//!
//! This crate connects four concerns that otherwise tend to be reimplemented
//! in every actor:
//!
//! - typed MongoDB documents whose identity remains inside the business value;
//! - acknowledgement-based BSON baselines and field-level change detection;
//! - eager, lazy, idle-unloadable, and row-lazy loading models; and
//! - two-phase asynchronous flushing through a Lattice actor.
//!
//! It is intentionally MongoDB-specific. It does not define a common database
//! abstraction, choose business ownership keys, or prescribe whether an
//! actor-local collection is represented by a map, vector, or custom indexes.
//!
//! # Mental model
//!
//! A persisted value moves through the following lifecycle:
//!
//! 1. [`store::MongoStore`] loads a typed document and its optimistic-lock
//!    metadata.
//! 2. [`persistence::coordinator::MongoPersistenceCoordinator`] registers the
//!    document and owns its last storage-acknowledged [`scan::ScanSnapshot`].
//! 3. Business code mutates a [`document::tracked::Tracked`] value through
//!    exclusive access. Requesting mutable access advances its mutation epoch.
//! 4. A preparation pass scans a bounded number of documents and fields and
//!    produces an immutable [`persistence::request::PreparedFlush`].
//! 5. The write runs without borrowing actor state across `await`.
//! 6. Only an applied completion advances the BSON baseline, version, scan
//!    cursor, and mutation epoch. A failed or undispatched write keeps the old
//!    baseline so the current business state can be prepared again.
//!
//! This is an optimistic, per-document protocol. A flush may contain several
//! documents, but it does not promise transaction atomicity across them.
//! Version conflicts never overwrite remote state. Documents use the safe
//! aggregate default, which blocks the coordinator until the application
//! reloads or explicitly removes the conflicted registration. Independent
//! rows can opt into document-local quarantine so unrelated state continues to
//! flush.
//!
//! # Defining a document
//!
//! Derive [`MongoDocument`] to define identity and collection mapping, and
//! derive [`MongoScan`] to generate field-ordered baseline and diff code:
//!
//! ```
//! use std::collections::HashMap;
//!
//! use lattice_store_mongodb::scan::{MongoScan as _, ScanBudget, ScanCursor};
//! use lattice_store_mongodb::{MongoDocument, MongoScan};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Serialize, Deserialize, MongoDocument, MongoScan)]
//! #[mongo(collection = "players")]
//! struct PlayerDocument {
//!     #[mongo(id)]
//!     id: u64,
//!     level: i32,
//!     #[mongo(scan = "map")]
//!     items: HashMap<String, i32>,
//! }
//!
//! let mut player = PlayerDocument {
//!     id: 42,
//!     level: 1,
//!     items: HashMap::from([("potion".to_owned(), 3)]),
//! };
//! let baseline = player.capture()?;
//!
//! player.level += 1;
//! player.items.insert("potion".to_owned(), 4);
//! let delta = player.diff(
//!     &baseline,
//!     ScanCursor::default(),
//!     &mut ScanBudget::generous(),
//! )?;
//!
//! assert!(delta.complete);
//! assert_eq!(delta.changes.len(), 2);
//! # Ok::<(), lattice_store_mongodb::scan::ScanError>(())
//! ```
//!
//! The `#[mongo(id)]` field is the ordinary business ID in Rust. Persistence
//! maps it to MongoDB `_id`; it is not duplicated in the stored business body.
//! The flat storage envelope also owns `version` and `updated_at_ms`, which
//! cannot be shadowed by business fields.
//!
//! By default, a version conflict or unexpectedly missing remote document
//! blocks every document registered in the same coordinator because it may
//! indicate a stale actor incarnation. Independent child rows can opt into
//! local quarantine:
//!
//! ```ignore
//! #[derive(Serialize, Deserialize, MongoDocument, MongoScan)]
//! #[mongo(collection = "world_alliance_members", conflict = "quarantine")]
//! struct AllianceMemberDocument {
//!     #[mongo(id)]
//!     id: WorldMemberKey,
//!     // ...
//! }
//! ```
//!
//! A quarantined document retains its acknowledged baseline and is skipped by
//! later preparations. Inspect it with
//! [`persistence::coordinator::MongoPersistenceCoordinator::document_conflict`],
//! then use
//! [`persistence::coordinator::MongoPersistenceCoordinator::resolve_conflict_with_loaded`]
//! after reloading remote state, or explicitly remove an optional registration
//! with
//! [`persistence::coordinator::MongoPersistenceCoordinator::detach_conflicted`].
//!
//! Persisted fields must not change through atomics, locks, or other interior
//! mutability reachable from `&self`. Such a change would bypass
//! [`document::tracked::Tracked`]'s mutation epoch. Mutable access may cause a
//! harmless false-positive scan even when no serialized value changed, but a
//! real mutation must never escape epoch tracking.
//!
//! # Actor-owned document sets
//!
//! Most actors should derive [`MongoDocumentSet`] for their complete persistent
//! state. The generated implementation provides:
//!
//! - `load`, which queries and registers all eager fields before returning;
//! - a typed `Loaded...` input for already loaded values;
//! - registration of singleton and runtime-sized document collections; and
//! - `prepare_set`, which enumerates every resident persistent document.
//!
//! A plain `Tracked<D>` field is an eager singleton. Mark a business-owned
//! collection with `#[mongo(many)]` and implement
//! [`document::set::MongoDocumentCollection`] to control its owner query,
//! actor-local representation, and derived indexes. The framework validates
//! ownership and registration, but the collection decides how loaded rows are
//! assembled.
//!
//! Loading behavior is expressed by the field type, keeping eager APIs
//! synchronous:
//!
//! | State model | Primary type | Access model |
//! | --- | --- | --- |
//! | Eager singleton | [`document::tracked::Tracked`] | Loaded at actor start; synchronous access |
//! | Eager complete collection | `#[mongo(many)]` collection | Loaded at actor start; ordinary collection APIs |
//! | Resident lazy singleton | [`loading::document::MongoLazyDocument`] | First access awaits; then remains resident |
//! | Idle-unloadable singleton | [`loading::document::MongoUnloadableDocument`] | First access awaits; clean idle state may detach |
//! | Resident lazy collection | [`loading::collection::MongoLazyCollection`] | Complete owner collection loads as one unit |
//! | Idle-unloadable collection | [`loading::collection::MongoUnloadableCollection`] | Clean complete collection may detach |
//! | Row-lazy table | [`loading::table::MongoLazyTable`] | Point/page loads await; resident rows are synchronous |
//! | Idle-unloadable table | [`loading::table::MongoUnloadableTable`] | Clean idle rows may detach independently |
//!
//! Whole-collection lazy models are appropriate when the collection is used as
//! one business unit. Use a row-lazy table when a large owner collection needs
//! bounded resident memory or page-oriented iteration.
//!
//! # Scanning and Map fields
//!
//! [`scan::ScanBudget`] limits documents and complete business fields. A field
//! is the smallest resumable unit: when a preparation exhausts its field
//! budget, the next preparation resumes at the following field. A Map field
//! consumes one field budget and is fully visited during that call.
//!
//! Maps are whole fields by default. `#[mongo(scan = "map")]` opts into exact
//! per-entry `$set` and `$unset` updates. The generated scan walks the Rust Map,
//! encodes one value at a time, retains BSON only for changed values, and never
//! materializes the complete BSON Map merely to compute a diff. Detecting
//! changes still requires an O(N) visit unless business code maintains a
//! mutation journal.
//!
//! MongoDB path segments cannot safely contain `.`, a leading `$`, NUL, or the
//! empty string. Use [`document::bson_serde::path_key_map`] together with Map
//! scanning for reversible, collision-free stored keys and update paths. If a
//! field has another custom Map serializer, declare
//! `#[mongo(scan = "map", adapter = YourAdapter)]` and implement
//! [`scan::MongoMapScanAdapter`]. Its entry-level key and value BSON must match
//! the field's full Serde representation exactly.
//!
//! # Preparing and flushing
//!
//! [`persistence::coordinator::MongoPersistenceCoordinator::prepare`] is
//! synchronous and receives a closure over
//! [`persistence::coordinator::MongoPreparation`]. Use `scan` for an ordinary
//! document or `scan_tracked` for mutation-epoch-aware scanning. Generated
//! document sets normally use
//! [`persistence::coordinator::MongoPersistenceCoordinator::prepare_set`].
//!
//! Preparation has an explicit two-phase lifecycle:
//!
//! - a clean preparation is acknowledged with `complete_clean`;
//! - a write preparation enters the in-flight state with `begin_flush`;
//! - `complete` applies exact per-document outcomes;
//! - `dispatch_failed` restores the prepared work for retry; and
//! - a version conflict remains blocked until application intervention.
//!
//! Actor code normally uses
//! [`persistence::coordinator::MongoPersistenceCoordinator::dispatch_prepared`]
//! instead of calling those methods individually. It submits the store future
//! through `ActorContext::pipe_to_self`; the actor's
//! [`lattice_actor::traits::Handler`] for
//! [`persistence::actor::MongoFlushCompleted`] later calls `apply_completion`.
//! The default retry policy starts at 50 ms, doubles, and caps at 2 seconds.
//!
//! For data that does not need snapshot scanning, use
//! [`persistence::direct::DirectDocumentStore`] for explicit whole-document
//! insert, replace, and delete operations. Coordinated writes execute through
//! [`persistence::request::PreparedWriteStore`].
//!
//! # Error and recovery model
//!
//! [`error::MongoStoreError`] retains the underlying source and distinguishes
//! configuration, BSON encoding/decoding, driver operations, timeouts, and
//! clock failures. Its recovery classification tells the coordinator whether
//! an outcome may be retried as-is or must wait for a new mutation and fresh
//! preparation. Definitive server rejections such as oversized documents do
//! not loop forever with the same rejected payload.
//!
//! Every retained failure state has an explicit intervention path:
//!
//! - `retry_rejected` forces a fresh scan after an external problem is fixed;
//! - `replace_rejected_with_loaded` and `detach_rejected` replace or explicitly
//!   discard definitively rejected state;
//! - `resolve_conflict_with_loaded` and `detach_conflicted` resolve optimistic
//!   lock conflicts and unexpectedly missing documents; and
//! - `abort_retry_as_unknown` and `abort_in_flight_as_unknown` stop an
//!   unresolvable ambiguous operation without pretending it failed.
//!
//! Aborted ambiguous operations become `OutcomeUnknown` conflicts. Their
//! baseline is never advanced, and the application must reload or explicitly
//! detach them. Actor-dispatched in-flight work is cancelled when possible;
//! any completion already queued for the abandoned generation is ignored.
//!
//! # Module guide
//!
//! - [`document`] — document identity, flat envelopes, tracking, and document sets;
//! - [`store`] — MongoDB connection plus typed reads and write execution;
//! - [`scan`] — baselines, field budgets, stable hashing, and partial updates;
//! - [`persistence`] — requests, coordination, retries, and actor integration;
//! - [`loading`] — lazy and idle-unloadable state models; and
//! - [`error`] — structured storage errors and recovery classification.

pub mod document;
pub mod error;
pub mod loading;
pub mod persistence;
pub mod scan;
pub mod store;

pub use lattice_store_mongodb_macros::{MongoDocument, MongoDocumentSet, MongoScan};

extern crate self as lattice_store_mongodb;
