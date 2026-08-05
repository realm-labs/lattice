//! Reusable actor-local coordination for scanned MongoDB documents.

mod completion;
pub mod drain;
mod flush;
mod preparation;
mod recovery;
mod registration;
mod retry;
mod state;

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use lattice_actor::{context::PipeTaskHandle, registry::ActorCreateContext};

use crate::document::tracked::{Tracked, TrackedMutationSignal};
use crate::error::MongoStoreError;
use crate::scan::{ScanBudget, ScanError, ScanWorkMetrics};

use self::state::{DocumentPresence, DocumentRejection, DocumentState};

use super::request::{
    DocumentCommit, FlushGeneration, InFlightCommit, PreparedDocumentWrite, PreparedFlush,
    WriteToken,
};
use super::types::MongoDocumentKey;

/// Retry timing for failed persistence dispatches and document writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub max_exponent: u32,
    /// Percentage of each backoff step that may be removed at random so that
    /// actors recovering from one storage outage do not retry in lockstep.
    /// Zero keeps the delay of every attempt exactly deterministic.
    pub jitter_percent: u32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(2),
            max_exponent: 6,
            jitter_percent: 50,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistenceConflict {
    pub key: MongoDocumentKey,
    pub expected_version: i64,
    pub kind: PersistenceConflictKind,
    pub policy: ConflictPolicy,
}

/// How one optimistic-lock conflict affects the other documents registered by
/// the same actor activation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConflictPolicy {
    /// Stop all later preparation until the application reloads or explicitly
    /// removes the conflicted document. This is the safe aggregate default.
    #[default]
    BlockCoordinator,
    /// Quarantine only the conflicted document and keep preparing unrelated
    /// documents owned by the same actor activation.
    QuarantineDocument,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceConflictKind {
    VersionConflict,
    NotFound,
    /// A previously dispatched operation may or may not have reached MongoDB.
    OutcomeUnknown,
    /// Storage refused the write because a strictly newer activation owns the
    /// document. This activation has lost the entity and must stop writing;
    /// it always blocks the coordinator regardless of [`ConflictPolicy`].
    Fenced {
        observed_epoch: i64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PersistenceCounters {
    pub scans: u64,
    pub changed_paths: u64,
    /// Documents handed to the backing store for an actual write attempt.
    pub attempted_documents: u64,
    pub applied_documents: u64,
    /// Documents rejected during preparation or failed by the backing store.
    pub failed_documents: u64,
    pub conflicts: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PersistenceScanMetrics {
    /// Number of individual business values encoded for diff scans.
    pub encoded_values: u64,
    /// Estimated BSON bytes produced while encoding individual business values.
    ///
    /// This is calculated from the encoded BSON values and deliberately avoids
    /// a second BSON serialization solely for metrics.
    pub estimated_encoded_bytes: u64,
    /// Nanoseconds spent encoding Rust business values into BSON.
    pub encoding_nanos: u64,
    /// Number of map entries hashed while preparing field-level diffs.
    pub map_entries_hashed: u64,
    /// Completed tracked scans triggered by a new mutation epoch that found no
    /// serialized business change.
    pub false_positive_scans: u64,
}

impl PersistenceScanMetrics {
    fn record_work(&mut self, work: ScanWorkMetrics) {
        self.encoded_values = self.encoded_values.saturating_add(work.encoded_values);
        self.estimated_encoded_bytes = self
            .estimated_encoded_bytes
            .saturating_add(work.estimated_encoded_bytes);
        self.encoding_nanos = self.encoding_nanos.saturating_add(work.encoding_nanos);
        self.map_entries_hashed = self
            .map_entries_hashed
            .saturating_add(work.map_entries_hashed);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PersistenceReport {
    pub clean: usize,
    pub applied: usize,
    pub failed: usize,
    pub conflicts: usize,
}

/// Owns acknowledgement baselines and the exact in-flight generation for a
/// heterogeneous set of MongoDB documents belonging to one actor activation.
#[derive(Debug)]
pub struct MongoPersistenceCoordinator {
    documents: BTreeMap<MongoDocumentKey, DocumentState>,
    pub(in crate::persistence) mutation_signal: TrackedMutationSignal,
    pub(in crate::persistence) dirty_message_claimed: bool,
    activation_epoch: u64,
    next_sequence: u64,
    in_flight: Option<InFlightCommit>,
    in_flight_task: Option<(FlushGeneration, PipeTaskHandle)>,
    retry_pending: Option<PreparedFlush>,
    abandoned_generations: BTreeSet<FlushGeneration>,
    last_error: Option<String>,
    retry_attempt: u32,
    retry_not_before: Option<Instant>,
    retry_wakeup_armed: Option<Instant>,
    retry_policy: RetryPolicy,
    retry_entropy: u64,
    counters: PersistenceCounters,
    scan_metrics: PersistenceScanMetrics,
}

impl MongoPersistenceCoordinator {
    /// Creates a coordinator using the placement authority attached to this Actor activation.
    ///
    /// Placement-managed application Actors should use this constructor so business code never
    /// allocates or persists its own fencing epoch.
    pub fn for_actor(context: &ActorCreateContext) -> Result<Self, PersistenceError> {
        let fencing_token = context
            .fencing_token()
            .ok_or(PersistenceError::MissingActorFencingToken)?;
        Ok(Self::new(placement_storage_epoch(fencing_token.get())?))
    }

    /// Creates an unfenced coordinator for tests and Actors outside placement management.
    pub fn standalone() -> Self {
        Self::new(1)
    }

    pub fn new(activation_epoch: u64) -> Self {
        Self::with_retry_policy(activation_epoch, RetryPolicy::default())
    }

    pub fn with_retry_policy(activation_epoch: u64, retry_policy: RetryPolicy) -> Self {
        Self {
            documents: BTreeMap::new(),
            mutation_signal: TrackedMutationSignal::new(),
            dirty_message_claimed: false,
            activation_epoch,
            next_sequence: 1,
            in_flight: None,
            in_flight_task: None,
            retry_pending: None,
            abandoned_generations: BTreeSet::new(),
            last_error: None,
            retry_attempt: 0,
            retry_not_before: None,
            retry_wakeup_armed: None,
            retry_policy,
            retry_entropy: uuid::Uuid::new_v4().as_u64_pair().0 | 1,
            counters: PersistenceCounters::default(),
            scan_metrics: PersistenceScanMetrics::default(),
        }
    }

    pub fn has_in_flight(&self) -> bool {
        self.in_flight.is_some()
    }

    pub fn pending_document_count(&self) -> usize {
        self.in_flight
            .as_ref()
            .map_or(0, |commit| commit.document_commits.len())
            + self
                .documents
                .values()
                .filter(|document| {
                    document.presence.is_pending_create()
                        || document.rejection.is_some()
                        || document.conflict.is_some()
                })
                .count()
    }

    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    pub const fn counters(&self) -> &PersistenceCounters {
        &self.counters
    }

    pub const fn scan_metrics(&self) -> &PersistenceScanMetrics {
        &self.scan_metrics
    }

    pub fn document_meta(&self, key: &MongoDocumentKey) -> Option<(i64, i64)> {
        self.documents
            .get(key)
            .map(|state| (state.version, state.updated_at_ms))
    }

    pub(super) fn tracked<T>(&self, value: T) -> Tracked<T> {
        Tracked::signaled(value, self.mutation_signal.clone())
    }
}

/// A single synchronous preparation pass over business-owned document values.
pub struct MongoPreparation<'a> {
    documents: &'a BTreeMap<MongoDocumentKey, DocumentState>,
    generation: FlushGeneration,
    budget: ScanBudget,
    next_token: u64,
    writes: Vec<PreparedDocumentWrite>,
    document_commits: BTreeMap<WriteToken, DocumentCommit>,
    clean_commits: Vec<DocumentCommit>,
    rejections: BTreeMap<MongoDocumentKey, DocumentRejection>,
    scans: u64,
    changed_paths: u64,
    scan_complete: bool,
    continue_after_document_failures: bool,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum PersistenceError {
    #[error("placement-managed persistence requires an Actor fencing token")]
    MissingActorFencingToken,
    #[error("Actor fencing token {token} exceeds the MongoDB fencing range")]
    ActorFencingTokenOutOfRange { token: u64 },
    #[error("required document in collection {collection} with ID {id} was not found")]
    RequiredDocumentMissing {
        collection: &'static str,
        id: String,
    },
    #[error(
        "loaded document in collection {collection} has ID {actual}, expected aggregate ID {expected}"
    )]
    DocumentIdMismatch {
        collection: &'static str,
        expected: String,
        actual: String,
    },
    #[error("document is already registered: {0:?}")]
    DuplicateDocument(MongoDocumentKey),
    #[error("document is not registered: {0:?}")]
    UnknownDocument(MongoDocumentKey),
    #[error("a persistence flush is already in flight")]
    FlushInFlight,
    #[error("no persistence flush is in flight")]
    NoFlushInFlight,
    #[error("no exact persistence retry is pending")]
    NoRetryPending,
    #[error("a version conflict blocks persistence")]
    ConflictBlocked,
    #[error("a clean completion contained document writes")]
    ExpectedCleanCommit,
    #[error("persistence generation overflow")]
    GenerationOverflow,
    #[error("persistence write token overflow")]
    WriteTokenOverflow,
    #[error("flush outcome tokens do not match the in-flight request")]
    OutcomeTokenMismatch,
    #[error("applied version did not advance exactly once: {0:?}")]
    InvalidAppliedVersion(MongoDocumentKey),
    #[error("new document has not been durably created: {0:?}")]
    CreatePending(MongoDocumentKey),
    #[error("document is not conflicted: {0:?}")]
    DocumentNotConflicted(MongoDocumentKey),
    #[error("document conflict must be resolved explicitly before detaching: {0:?}")]
    DocumentConflictPending(MongoDocumentKey),
    #[error("document is not rejected: {0:?}")]
    DocumentNotRejected(MongoDocumentKey),
    #[error("document rejection must be resolved explicitly before detaching: {0:?}")]
    DocumentRejectionPending(MongoDocumentKey),
    #[error("document write retry must be applied or aborted before detaching: {0:?}")]
    DocumentRetryPending(MongoDocumentKey),
    #[error("persistence generation was explicitly abandoned: {0:?}")]
    AbandonedGeneration(FlushGeneration),
    #[error("stale activation epoch: expected {expected}, got {actual}")]
    StaleActivation { expected: u64, actual: u64 },
    #[error(
        "activation epoch {activation_epoch} was fenced by activation epoch {observed_epoch}; this activation must stop instead of reloading"
    )]
    ActivationFenced {
        activation_epoch: u64,
        observed_epoch: i64,
    },
    #[error("foreign flush generation: expected {expected:?}, got {actual:?}")]
    ForeignGeneration {
        expected: FlushGeneration,
        actual: FlushGeneration,
    },
    #[error(transparent)]
    Scan(#[from] ScanError),
    #[error(transparent)]
    Store(#[from] MongoStoreError),
}

// Keep placement-owned epochs in a range that dominates epochs written by the former
// process-local counter. This makes upgrading existing documents safe without a separate
// migration document; subtracting the base recovers the durable placement generation.
const PLACEMENT_STORAGE_EPOCH_BASE: u64 = 1_u64 << 62;

fn placement_storage_epoch(token: u64) -> Result<u64, PersistenceError> {
    let epoch = PLACEMENT_STORAGE_EPOCH_BASE
        .checked_add(token)
        .ok_or(PersistenceError::ActorFencingTokenOutOfRange { token })?;
    (epoch <= i64::MAX as u64)
        .then_some(epoch)
        .ok_or(PersistenceError::ActorFencingTokenOutOfRange { token })
}

#[cfg(test)]
mod tests;
