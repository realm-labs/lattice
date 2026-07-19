//! Shared policy contracts for lazy and idle-unloadable fields.

use std::time::Duration;

use crate::persistence::coordinator::{MongoPreparation, PersistenceError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdleUnloadStatus {
    Unloaded,
    NotLoaded,
    NotIdle,
    NeedsFlush,
}

/// Internal contract used by `MongoDocumentSet` derive for resident lazy
/// fields. Business code normally uses the concrete field type directly.
#[doc(hidden)]
pub trait MongoLazyField<OwnerId>: Sized + Send {
    fn new_lazy(owner_id: OwnerId) -> Self;

    fn scan_loaded(&self, preparation: &mut MongoPreparation<'_>) -> Result<(), PersistenceError>;
}

/// Internal contract used by `MongoDocumentSet` derive for idle-unloadable
/// lazy fields.
#[doc(hidden)]
pub trait MongoUnloadableField<OwnerId>: Sized + Send {
    fn new_unloadable(owner_id: OwnerId, idle_after: Duration) -> Self;

    fn scan_loaded(&self, preparation: &mut MongoPreparation<'_>) -> Result<(), PersistenceError>;
}
