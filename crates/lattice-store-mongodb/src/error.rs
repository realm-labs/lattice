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

/// An owned, cloneable persistence error with a stable category and context.
#[derive(Debug, Clone)]
pub struct MongoStoreError {
    kind: MongoStoreErrorKind,
    message: String,
    source: Option<Arc<dyn Error + Send + Sync>>,
}

impl MongoStoreError {
    /// Creates an uncategorized error. Prefer the category-specific constructors
    /// for errors exposed by public operations.
    pub fn new(message: impl Into<String>) -> Self {
        Self::without_source(MongoStoreErrorKind::Other, message)
    }

    pub fn invalid_config(field: &'static str, message: impl fmt::Display) -> Self {
        Self::without_source(
            MongoStoreErrorKind::InvalidConfig,
            format!("invalid MongoDB configuration `{field}`: {message}"),
        )
    }

    pub fn encode(context: &'static str, source: impl Error + Send + Sync + 'static) -> Self {
        Self::with_source(MongoStoreErrorKind::Encode, context, source)
    }

    pub fn decode(context: &'static str, source: impl Error + Send + Sync + 'static) -> Self {
        Self::with_source(MongoStoreErrorKind::Decode, context, source)
    }

    pub fn driver(context: &'static str, source: mongodb::error::Error) -> Self {
        Self::with_source(MongoStoreErrorKind::Driver, context, source)
    }

    pub(crate) fn operation(
        context: &'static str,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self::with_source(MongoStoreErrorKind::Driver, context, source)
    }

    pub fn timeout(context: &'static str, duration: Duration) -> Self {
        Self::without_source(
            MongoStoreErrorKind::Timeout,
            format!("{context}: timed out after {duration:?}"),
        )
    }

    pub fn clock(message: impl Into<String>) -> Self {
        Self::without_source(MongoStoreErrorKind::Clock, message)
    }

    pub const fn kind(&self) -> MongoStoreErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    fn without_source(kind: MongoStoreErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }

    fn with_source(
        kind: MongoStoreErrorKind,
        context: &'static str,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        let message = format!("{context}: {source}");
        Self {
            kind,
            message,
            source: Some(Arc::new(source)),
        }
    }
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
