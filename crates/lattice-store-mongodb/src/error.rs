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

fn mongodb_error_recovery(error: &mongodb::error::Error) -> MongoStoreErrorRecovery {
    use mongodb::error::{ErrorKind, WriteFailure};

    match error.kind.as_ref() {
        ErrorKind::InvalidArgument { .. } | ErrorKind::BsonSerialization(_) => {
            MongoStoreErrorRecovery::ReprepareAfterMutation
        }
        ErrorKind::Write(WriteFailure::WriteError(_)) => {
            MongoStoreErrorRecovery::ReprepareAfterMutation
        }
        // A top-level command error is a structured server rejection. Ambiguous
        // outcomes arrive as transport/write-concern failures instead.
        ErrorKind::Command(_) => MongoStoreErrorRecovery::ReprepareAfterMutation,
        _ => MongoStoreErrorRecovery::RetryExact,
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

    use mongodb::bson::doc;
    use mongodb::error::{ErrorKind, WriteError, WriteFailure};

    use super::{MongoStoreError, MongoStoreErrorRecovery, mongodb_error_kind_is_write_rejection};

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
}
