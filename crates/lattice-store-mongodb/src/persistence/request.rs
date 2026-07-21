//! Immutable persistence requests and exact per-document outcomes.

use std::collections::{BTreeMap, BTreeSet};

use crate::error::MongoStoreError;
use mongodb::bson::{Bson, Document};

use crate::scan::{ScanCommit, ScanSnapshot};

use super::types::{MongoDocumentKey, MongoFieldPath};

#[async_trait::async_trait]
pub trait PreparedWriteStore: Send + Sync + 'static {
    /// Executes one prepared batch and returns exactly one outcome per token.
    ///
    /// The same logical write may be delivered more than once after an
    /// ambiguous transport failure or an actor shutdown race. Implementations
    /// must use `expected_version` and `operation_id` to reconcile exact
    /// retries rather than apply the mutation twice. `MongoStore` implements
    /// this contract using optimistic filters and `_lattice_write_id`.
    async fn flush(
        &self,
        writes: Vec<PreparedDocumentWrite>,
    ) -> Result<FlushOutcome, MongoStoreError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WriteToken(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FlushGeneration {
    pub activation_epoch: u64,
    pub sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateMode {
    /// Fails when any document already owns the requested `_id`.
    InsertOnly,
    /// Inserts when absent and may replace only a version-zero placeholder.
    /// Existing durable versions are reported as conflicts and never reset.
    UpsertAllowed,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DocumentOperation {
    Update {
        sets: BTreeMap<MongoFieldPath, Bson>,
        unsets: BTreeSet<MongoFieldPath>,
    },
    Create {
        document: Document,
        mode: CreateMode,
    },
    Delete,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PreparedDocumentWrite {
    pub token: WriteToken,
    pub key: MongoDocumentKey,
    /// Canonical BSON representation used for the MongoDB `_id` filter.
    ///
    /// `key.id` is only a stable actor-local/diagnostic identity and must not
    /// be reused as a storage value because typed IDs are not always strings.
    pub document_id: Bson,
    pub expected_version: i64,
    /// Stable across retries of this exact logical write and persisted in the
    /// document so an acknowledged timeout can be reconciled safely.
    pub operation_id: String,
    pub operation: DocumentOperation,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FlushRequest {
    pub generation: FlushGeneration,
    pub writes: Vec<PreparedDocumentWrite>,
}

#[derive(Debug, Clone)]
pub struct DocumentCommit {
    pub key: MongoDocumentKey,
    pub scan: ScanCommit,
    /// Mutation epoch observed while preparing this document. Scan-only
    /// callers leave this unset.
    pub mutation_epoch: Option<u64>,
    /// Whether this batch reached the end of the field sequence, even if an
    /// epoch change requires another full sweep before the document is clean.
    pub(crate) sweep_complete: bool,
    pub scan_complete: bool,
    pub(crate) changed: bool,
    /// A full baseline captured from a prepared Create. Unlike an incremental
    /// scan commit, this represents every business field written by the
    /// operation and is installed only after the Create is acknowledged.
    pub(crate) replacement_baseline: Option<ScanSnapshot>,
}

#[derive(Debug, Clone)]
pub struct InFlightCommit {
    pub generation: FlushGeneration,
    pub document_commits: BTreeMap<WriteToken, DocumentCommit>,
    pub clean_commits: Vec<DocumentCommit>,
    pub writes: BTreeMap<WriteToken, PreparedDocumentWrite>,
}

#[derive(Debug, Clone)]
pub struct PreparedFlush {
    pub request: Option<FlushRequest>,
    pub commit: InFlightCommit,
    /// True only when every visited document reached the end of its scan.
    /// A write-free partial scan must not be treated as a clean drain.
    pub scan_complete: bool,
}

#[derive(Debug)]
pub enum DocumentWriteOutcome {
    Applied {
        previous_version: i64,
        new_version: i64,
        updated_at_ms: i64,
    },
    VersionConflict {
        expected_version: i64,
    },
    NotFound {
        expected_version: i64,
    },
    Failed {
        error: MongoStoreError,
    },
    NotAttempted,
}

#[derive(Debug, Default)]
pub struct FlushOutcome {
    pub documents: BTreeMap<WriteToken, DocumentWriteOutcome>,
}

impl FlushOutcome {
    /// Validate the one-to-one request/outcome contract before actor-local
    /// commits are consumed.
    pub fn validate_for(&self, request: &FlushRequest) -> Result<(), OutcomeContractError> {
        let expected = request
            .writes
            .iter()
            .map(|write| write.token)
            .collect::<BTreeSet<_>>();
        if expected.len() != request.writes.len() {
            return Err(OutcomeContractError::DuplicateRequestToken);
        }
        let actual = self.documents.keys().copied().collect::<BTreeSet<_>>();
        let missing = expected.difference(&actual).copied().collect::<Vec<_>>();
        let foreign = actual.difference(&expected).copied().collect::<Vec<_>>();
        if missing.is_empty() && foreign.is_empty() {
            Ok(())
        } else {
            Err(OutcomeContractError::TokenMismatch { missing, foreign })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutcomeContractError {
    DuplicateRequestToken,
    TokenMismatch {
        missing: Vec<WriteToken>,
        foreign: Vec<WriteToken>,
    },
}

impl std::fmt::Display for OutcomeContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateRequestToken => f.write_str("flush request contains duplicate tokens"),
            Self::TokenMismatch { missing, foreign } => write!(
                f,
                "flush outcome token mismatch; missing={missing:?}, foreign={foreign:?}"
            ),
        }
    }
}

impl std::error::Error for OutcomeContractError {}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use mongodb::bson::Bson;

    use super::{
        DocumentOperation, DocumentWriteOutcome, FlushGeneration, FlushOutcome, FlushRequest,
        OutcomeContractError, PreparedDocumentWrite, WriteToken,
    };
    use crate::persistence::types::{MongoDocumentKey, MongoFieldPath};

    fn request(tokens: &[u64]) -> FlushRequest {
        FlushRequest {
            generation: FlushGeneration {
                activation_epoch: 7,
                sequence: 2,
            },
            writes: tokens
                .iter()
                .map(|token| PreparedDocumentWrite {
                    token: WriteToken(*token),
                    key: MongoDocumentKey::new("test", token.to_string()),
                    document_id: Bson::Int64(
                        i64::try_from(*token).expect("test token should fit BSON i64"),
                    ),
                    expected_version: 3,
                    operation_id: format!("operation-{token}"),
                    operation: DocumentOperation::Update {
                        sets: BTreeMap::from([(
                            MongoFieldPath::new("value"),
                            mongodb::bson::Bson::Int32(1),
                        )]),
                        unsets: BTreeSet::new(),
                    },
                })
                .collect(),
        }
    }

    #[test]
    fn exact_per_document_outcome_contract_accepts_all_tokens_once() {
        let request = request(&[1, 2]);
        let outcome = FlushOutcome {
            documents: BTreeMap::from([
                (
                    WriteToken(1),
                    DocumentWriteOutcome::Applied {
                        previous_version: 3,
                        new_version: 4,
                        updated_at_ms: 123,
                    },
                ),
                (WriteToken(2), DocumentWriteOutcome::NotAttempted),
            ]),
        };
        outcome
            .validate_for(&request)
            .expect("matching tokens should validate");
    }

    #[test]
    fn missing_and_foreign_tokens_are_rejected_before_commits_apply() {
        let request = request(&[1, 2]);
        let outcome = FlushOutcome {
            documents: BTreeMap::from([
                (WriteToken(1), DocumentWriteOutcome::NotAttempted),
                (WriteToken(9), DocumentWriteOutcome::NotAttempted),
            ]),
        };
        assert_eq!(
            outcome.validate_for(&request),
            Err(OutcomeContractError::TokenMismatch {
                missing: vec![WriteToken(2)],
                foreign: vec![WriteToken(9)],
            })
        );
    }

    #[test]
    fn duplicate_request_tokens_are_rejected() {
        assert_eq!(
            FlushOutcome::default().validate_for(&request(&[1, 1])),
            Err(OutcomeContractError::DuplicateRequestToken)
        );
    }
}
