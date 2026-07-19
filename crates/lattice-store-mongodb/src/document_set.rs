//! Strongly typed collections of actor-owned MongoDB documents.

use crate::coordinator::{MongoPersistenceCoordinator, MongoPreparation, PersistenceError};
use crate::error::MongoStoreError;
use crate::mongo_store::MongoStore;
use crate::prepared::PreparedFlush;
use crate::scan::{MongoScan, ScanBudget};
use crate::tracked::Tracked;

/// A runtime-sized group of documents owned by one aggregate.
///
/// Implementations retain full control over their actor-local representation:
/// a group may use a map, a vector, or additional derived indexes. The
/// document-set derive uses this adapter only to load, validate, register, and
/// enumerate the persistent documents.
pub trait MongoDocumentCollection<OwnerId>: Sized + Send {
    type Document: MongoScan;

    /// Builds the MongoDB filter used to load every document owned by `owner_id`.
    fn load_filter(owner_id: &OwnerId) -> Result<mongodb::bson::Document, MongoStoreError>;

    /// Returns the aggregate owner encoded by one loaded document.
    fn owner_id(document: &Self::Document) -> &OwnerId;

    /// Builds the business collection after every document has been registered
    /// with the persistence coordinator.
    fn from_documents(documents: Vec<Tracked<Self::Document>>) -> Result<Self, PersistenceError>;

    /// Enumerates the authoritative persistent documents. Derived business
    /// indexes should not be returned here.
    fn documents(&self) -> impl Iterator<Item = &Tracked<Self::Document>>;
}

/// A typed set of singleton and runtime-sized MongoDB documents sharing one
/// aggregate ID.
///
/// Implementations are normally generated with [`crate::MongoDocumentSet`].
pub trait MongoDocumentSet: Sized + Send {
    type Id: Clone + PartialEq + std::fmt::Debug + Send + 'static;
    type Loaded;

    const DOCUMENT_COUNT: usize;

    fn from_loaded(
        id: &Self::Id,
        loaded: Self::Loaded,
        coordinator: &mut MongoPersistenceCoordinator,
    ) -> Result<Self, PersistenceError>;

    /// Loads every eager singleton and `#[mongo(many)]` collection, then
    /// registers them with `coordinator`. Fields declared with
    /// `#[mongo(lazy)]` or `#[mongo(lazy_unload = "...")]` are initialized
    /// without performing I/O.
    fn load<'a>(
        store: &'a MongoStore,
        id: &Self::Id,
        coordinator: &'a mut MongoPersistenceCoordinator,
    ) -> impl std::future::Future<Output = Result<Self, PersistenceError>> + Send + 'a;

    fn scan_all(&self, preparation: &mut MongoPreparation<'_>) -> Result<(), PersistenceError>;
}

impl MongoPersistenceCoordinator {
    /// Builds and registers a statically declared set of loaded documents.
    pub fn attach_loaded_set<S>(
        &mut self,
        id: &S::Id,
        loaded: S::Loaded,
    ) -> Result<S, PersistenceError>
    where
        S: MongoDocumentSet,
    {
        S::from_loaded(id, loaded, self)
    }

    /// Prepares all documents in a statically declared document set.
    pub fn prepare_set<S>(
        &mut self,
        budget: ScanBudget,
        documents: &S,
    ) -> Result<PreparedFlush, PersistenceError>
    where
        S: MongoDocumentSet,
    {
        self.prepare(budget, |preparation| documents.scan_all(preparation))
    }
}
