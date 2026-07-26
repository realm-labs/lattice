use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// Broad failure category for MongoDB persistence operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MongoStoreErrorKind {
    InvalidConfig,
    Encode,
    Decode,
    Driver,
    Timeout,
    Clock,
    Other,
}

/// How the persistence coordinator may safely recover from a failed write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MongoStoreErrorRecovery {
    /// The write result may be ambiguous, so the exact prepared operation must
    /// be retried with the same operation ID.
    RetryExact,
    /// Storage definitively rejected the operation. The rejected payload must
    /// not be retried until actor-owned state changes.
    ReprepareAfterMutation,
}

/// An owned, cloneable persistence error with a stable category and context.
#[derive(Debug, Clone)]
pub struct MongoStoreError {
    kind: MongoStoreErrorKind,
    recovery: MongoStoreErrorRecovery,
    message: String,
    source: Option<Arc<dyn Error + Send + Sync>>,
}

impl MongoStoreError {
    /// Creates an uncategorized error. Prefer the category-specific constructors
    /// for errors exposed by public operations.
    pub fn new(message: impl Into<String>) -> Self {
        Self::without_source(
            MongoStoreErrorKind::Other,
            MongoStoreErrorRecovery::RetryExact,
            message,
        )
    }

    /// Creates an error for a write that storage definitively did not apply.
    /// The coordinator will wait for a new mutation epoch and then prepare a
    /// fresh operation from current actor state.
    pub fn rejected(message: impl Into<String>) -> Self {
        Self::without_source(
            MongoStoreErrorKind::Other,
            MongoStoreErrorRecovery::ReprepareAfterMutation,
            message,
        )
    }

    pub fn invalid_config(field: &'static str, message: impl fmt::Display) -> Self {
        Self::without_source(
            MongoStoreErrorKind::InvalidConfig,
            MongoStoreErrorRecovery::ReprepareAfterMutation,
            format!("invalid MongoDB configuration `{field}`: {message}"),
        )
    }

    pub fn encode(context: &'static str, source: impl Error + Send + Sync + 'static) -> Self {
        Self::with_source(
            MongoStoreErrorKind::Encode,
            MongoStoreErrorRecovery::ReprepareAfterMutation,
            context,
            source,
        )
    }

    pub fn decode(context: &'static str, source: impl Error + Send + Sync + 'static) -> Self {
        Self::with_source(
            MongoStoreErrorKind::Decode,
            MongoStoreErrorRecovery::ReprepareAfterMutation,
            context,
            source,
        )
    }

    pub fn driver(context: &'static str, source: mongodb::error::Error) -> Self {
        let recovery = mongodb_error_recovery(&source);
        Self::with_source(MongoStoreErrorKind::Driver, recovery, context, source)
    }

    pub(crate) fn operation(
        context: &'static str,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        let recovery = (&source as &(dyn Error + 'static))
            .downcast_ref::<mongodb::error::Error>()
            .map_or(MongoStoreErrorRecovery::RetryExact, mongodb_error_recovery);
        Self::with_source(MongoStoreErrorKind::Driver, recovery, context, source)
    }

    pub fn timeout(context: &'static str, duration: Duration) -> Self {
        Self::without_source(
            MongoStoreErrorKind::Timeout,
            MongoStoreErrorRecovery::RetryExact,
            format!("{context}: timed out after {duration:?}"),
        )
    }

    pub fn clock(message: impl Into<String>) -> Self {
        Self::without_source(
            MongoStoreErrorKind::Clock,
            MongoStoreErrorRecovery::ReprepareAfterMutation,
            message,
        )
    }

    pub const fn kind(&self) -> MongoStoreErrorKind {
        self.kind
    }

    pub const fn recovery(&self) -> MongoStoreErrorRecovery {
        self.recovery
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn is_write_rejection(&self) -> bool {
        self.source
            .as_deref()
            .and_then(|source| source.downcast_ref::<mongodb::error::Error>())
            .is_some_and(|error| mongodb_error_kind_is_write_rejection(error.kind.as_ref()))
    }

    fn without_source(
        kind: MongoStoreErrorKind,
        recovery: MongoStoreErrorRecovery,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            recovery,
            message: message.into(),
            source: None,
        }
    }

    fn with_source(
        kind: MongoStoreErrorKind,
        recovery: MongoStoreErrorRecovery,
        context: &'static str,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        let message = format!("{context}: {source}");
        Self {
            kind,
            recovery,
            message,
            source: Some(Arc::new(source)),
        }
    }
}

/// Server error codes reporting a step-down, election, or shutdown. MongoDB
/// reports them as ordinary command or write errors, and the driver returns
/// them unchanged once its own single retryable-write attempt is exhausted.
const RETRYABLE_SERVER_CODES: [i32; 13] = [
    6, 7, 89, 91, 189, 262, 9001, 10058, 10107, 11600, 11602, 13435, 13436,
];

fn mongodb_error_recovery(error: &mongodb::error::Error) -> MongoStoreErrorRecovery {
    mongodb_error_kind_recovery(
        error.kind.as_ref(),
        error.contains_label(mongodb::error::RETRYABLE_WRITE_ERROR),
    )
}

fn mongodb_error_kind_recovery(
    kind: &mongodb::error::ErrorKind,
    retryable_write: bool,
) -> MongoStoreErrorRecovery {
    use mongodb::error::{ErrorKind, WriteFailure};

    if retryable_write
        || mongodb_server_error_code(kind)
            .is_some_and(|code| RETRYABLE_SERVER_CODES.contains(&code))
    {
        return MongoStoreErrorRecovery::RetryExact;
    }
    match kind {
        ErrorKind::InvalidArgument { .. } | ErrorKind::BsonSerialization(_) => {
            MongoStoreErrorRecovery::ReprepareAfterMutation
        }
        ErrorKind::Write(WriteFailure::WriteError(_)) => {
            MongoStoreErrorRecovery::ReprepareAfterMutation
        }
        // A top-level command error that survived the failover codes above is a
        // structured server rejection. Ambiguous outcomes arrive as
        // transport/write-concern failures instead.
        ErrorKind::Command(_) => MongoStoreErrorRecovery::ReprepareAfterMutation,
        _ => MongoStoreErrorRecovery::RetryExact,
    }
}

fn mongodb_server_error_code(kind: &mongodb::error::ErrorKind) -> Option<i32> {
    use mongodb::error::{ErrorKind, WriteFailure};

    match kind {
        ErrorKind::Command(error) => Some(error.code),
        ErrorKind::Write(WriteFailure::WriteError(error)) => Some(error.code),
        ErrorKind::Write(WriteFailure::WriteConcernError(error)) => Some(error.code),
        _ => None,
    }
}

fn mongodb_error_kind_is_write_rejection(kind: &mongodb::error::ErrorKind) -> bool {
    use mongodb::error::{ErrorKind, WriteFailure};

    matches!(kind, ErrorKind::Write(WriteFailure::WriteError(_)))
}

impl fmt::Display for MongoStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for MongoStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mongodb::bson::{Bson, doc};
    use mongodb::error::{CommandError, ErrorKind, WriteError, WriteFailure};

    use super::{
        MongoStoreError, MongoStoreErrorRecovery, mongodb_error_kind_is_write_rejection,
        mongodb_error_kind_recovery,
    };

    fn command_error(code: i32, code_name: &str) -> ErrorKind {
        ErrorKind::Command(
            mongodb::bson::from_document::<CommandError>(doc! {
                "code": code,
                "codeName": code_name,
                "errmsg": "server reported a command failure",
                "topologyVersion": Bson::Null,
            })
            .expect("command error should decode"),
        )
    }

    #[test]
    fn recovery_mode_distinguishes_ambiguous_and_definitive_failures() {
        assert_eq!(
            MongoStoreError::timeout("write", Duration::from_secs(1)).recovery(),
            MongoStoreErrorRecovery::RetryExact,
        );
        assert_eq!(
            MongoStoreError::rejected("document too large").recovery(),
            MongoStoreErrorRecovery::ReprepareAfterMutation,
        );
    }

    #[test]
    fn write_rejection_detection_uses_only_the_structured_error_variant() {
        let validation = mongodb::bson::from_document::<WriteError>(doc! {
            "code": 121,
            "errmsg": "message text and server code are intentionally irrelevant",
        })
        .expect("write error should decode");

        assert!(mongodb_error_kind_is_write_rejection(&ErrorKind::Write(
            WriteFailure::WriteError(validation),
        )));
    }

    #[test]
    fn replica_set_failover_command_errors_keep_the_exact_write_retryable() {
        for (code, code_name) in [
            (10107, "NotWritablePrimary"),
            (11602, "InterruptedDueToReplStateChange"),
            (11600, "InterruptedAtShutdown"),
            (91, "ShutdownInProgress"),
            (189, "PrimarySteppedDown"),
            (13435, "NotPrimaryNoSecondaryOk"),
            (13436, "NotPrimaryOrSecondary"),
        ] {
            assert_eq!(
                MongoStoreError::driver(
                    "update prepared document",
                    command_error(code, code_name).into(),
                )
                .recovery(),
                MongoStoreErrorRecovery::RetryExact,
                "server code {code} ({code_name}) must not become a definitive rejection",
            );
        }
    }

    #[test]
    fn definitive_command_errors_still_wait_for_a_new_mutation() {
        assert_eq!(
            MongoStoreError::driver(
                "update prepared document",
                command_error(2, "BadValue").into(),
            )
            .recovery(),
            MongoStoreErrorRecovery::ReprepareAfterMutation,
        );
    }

    #[test]
    fn the_retryable_write_label_overrides_the_command_rejection_default() {
        let kind = command_error(2, "BadValue");
        assert_eq!(
            mongodb_error_kind_recovery(&kind, true),
            MongoStoreErrorRecovery::RetryExact,
        );
        assert_eq!(
            mongodb_error_kind_recovery(&kind, false),
            MongoStoreErrorRecovery::ReprepareAfterMutation,
        );
    }
}
