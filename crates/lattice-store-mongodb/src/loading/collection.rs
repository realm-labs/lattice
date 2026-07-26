//! Complete owner collections loaded as one lazy unit.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use crate::document::MongoDocument;
use crate::document::set::MongoDocumentCollection;
use crate::persistence::coordinator::{
    MongoPersistenceCoordinator, MongoPreparation, PersistenceError,
};
use crate::persistence::types::MongoDocumentKey;
use crate::store::MongoStore;

use super::policy::{IdleUnloadStatus, MongoLazyField, MongoUnloadableField};

/// A complete business collection loaded on first access and then retained.
#[derive(Debug)]
pub struct MongoLazyCollection<OwnerId, C> {
    owner_id: OwnerId,
    loaded: Option<C>,
}

impl<OwnerId, C> MongoLazyCollection<OwnerId, C>
where
    OwnerId: Clone + PartialEq + std::fmt::Debug + Send + 'static,
    C: MongoDocumentCollection<OwnerId>,
{
    pub fn new(owner_id: OwnerId) -> Self {
        Self {
            owner_id,
            loaded: None,
        }
    }

    pub fn is_loaded(&self) -> bool {
        self.loaded.is_some()
    }

    pub fn get_loaded(&self) -> Option<&C> {
        self.loaded.as_ref()
    }

    /// Returns mutable access only when the full collection is resident.
    /// This never performs I/O.
    pub fn get_loaded_mut(&mut self) -> Option<&mut C> {
        self.loaded.as_mut()
    }

    pub async fn get<'a>(
        &'a mut self,
        store: &MongoStore,
        persistence: &mut MongoPersistenceCoordinator,
    ) -> Result<&'a C, PersistenceError> {
        self.ensure_loaded(store, persistence).await?;
        Ok(self.loaded.as_ref().expect("lazy collection just loaded"))
    }

    pub async fn get_mut<'a>(
        &'a mut self,
        store: &MongoStore,
        persistence: &mut MongoPersistenceCoordinator,
    ) -> Result<&'a mut C, PersistenceError> {
        self.ensure_loaded(store, persistence).await?;
        Ok(self.loaded.as_mut().expect("lazy collection just loaded"))
    }

    async fn ensure_loaded(
        &mut self,
        store: &MongoStore,
        persistence: &mut MongoPersistenceCoordinator,
    ) -> Result<(), PersistenceError> {
        if self.loaded.is_some() {
            return Ok(());
        }
        let loaded = store
            .find_many_scanned::<C::Document>(C::load_filter(&self.owner_id)?)
            .await?;
        for document in &loaded {
            let actual = C::owner_id(document.value());
            if actual != &self.owner_id {
                return Err(PersistenceError::DocumentIdMismatch {
                    collection: C::Document::COLLECTION,
                    expected: format!("{:?}", self.owner_id),
                    actual: format!("{actual:?}"),
                });
            }
        }
        let documents = persistence.track_loaded_scanned_many(loaded)?;
        self.loaded = Some(C::from_documents(documents)?);
        Ok(())
    }

    pub(crate) fn scan_loaded(
        &self,
        preparation: &mut MongoPreparation<'_>,
    ) -> Result<(), PersistenceError> {
        if let Some(collection) = &self.loaded {
            for document in collection.documents() {
                preparation.scan_tracked(document)?;
            }
        }
        Ok(())
    }
}

impl<OwnerId, C> MongoLazyField<OwnerId> for MongoLazyCollection<OwnerId, C>
where
    OwnerId: Clone + PartialEq + std::fmt::Debug + Send + 'static,
    C: MongoDocumentCollection<OwnerId>,
{
    fn new_lazy(owner_id: OwnerId) -> Self {
        Self::new(owner_id)
    }

    fn scan_loaded(&self, preparation: &mut MongoPreparation<'_>) -> Result<(), PersistenceError> {
        self.scan_loaded(preparation)
    }
}

/// A complete lazy collection that may detach all of its documents after an
/// idle period. Row-level eviction belongs to `MongoUnloadableTable` instead.
#[derive(Debug)]
pub struct MongoUnloadableCollection<OwnerId, C> {
    inner: MongoLazyCollection<OwnerId, C>,
    idle_after: Duration,
    last_access: Option<Instant>,
}

impl<OwnerId, C> MongoUnloadableCollection<OwnerId, C>
where
    OwnerId: Clone + PartialEq + std::fmt::Debug + Send + 'static,
    C: MongoDocumentCollection<OwnerId>,
{
    pub fn new(owner_id: OwnerId, idle_after: Duration) -> Self {
        assert!(
            !idle_after.is_zero(),
            "idle unload duration must be positive"
        );
        Self {
            inner: MongoLazyCollection::new(owner_id),
            idle_after,
            last_access: None,
        }
    }

    pub fn is_loaded(&self) -> bool {
        self.inner.is_loaded()
    }

    pub fn get_loaded(&mut self) -> Option<&C> {
        let value = self.inner.get_loaded()?;
        self.last_access = Some(Instant::now());
        Some(value)
    }

    pub fn get_loaded_mut(&mut self) -> Option<&mut C> {
        let value = self.inner.get_loaded_mut()?;
        self.last_access = Some(Instant::now());
        Some(value)
    }

    pub async fn get<'a>(
        &'a mut self,
        store: &MongoStore,
        persistence: &mut MongoPersistenceCoordinator,
    ) -> Result<&'a C, PersistenceError> {
        self.last_access = Some(Instant::now());
        self.inner.get(store, persistence).await
    }

    pub async fn get_mut<'a>(
        &'a mut self,
        store: &MongoStore,
        persistence: &mut MongoPersistenceCoordinator,
    ) -> Result<&'a mut C, PersistenceError> {
        self.last_access = Some(Instant::now());
        self.inner.get_mut(store, persistence).await
    }

    pub fn unload_idle(
        &mut self,
        now: Instant,
        persistence: &mut MongoPersistenceCoordinator,
    ) -> Result<IdleUnloadStatus, PersistenceError> {
        let Some(collection) = self.inner.loaded.as_ref() else {
            return Ok(IdleUnloadStatus::NotLoaded);
        };
        let Some(last_access) = self.last_access else {
            return Ok(IdleUnloadStatus::NotIdle);
        };
        if now.saturating_duration_since(last_access) < self.idle_after {
            return Ok(IdleUnloadStatus::NotIdle);
        }
        // `documents()` is business-controlled, so the same registration may be
        // enumerated twice. Detaching is not idempotent: the second visit would
        // fail an already-detached document and leave the collection resident
        // with a partially unregistered set. Prove uniqueness and cleanliness
        // before detaching anything.
        let mut keys = BTreeSet::new();
        for document in collection.documents() {
            let key = MongoDocumentKey::for_document::<C::Document>(document.read().id())?;
            if !keys.insert(key.clone()) {
                return Err(PersistenceError::DuplicateDocument(key));
            }
            if !persistence.tracked_is_clean(document)? {
                return Ok(IdleUnloadStatus::NeedsFlush);
            }
        }
        for document in collection.documents() {
            if !persistence.detach_tracked_if_clean(document)? {
                return Ok(IdleUnloadStatus::NeedsFlush);
            }
        }
        self.inner.loaded = None;
        self.last_access = None;
        Ok(IdleUnloadStatus::Unloaded)
    }
}

impl<OwnerId, C> MongoUnloadableField<OwnerId> for MongoUnloadableCollection<OwnerId, C>
where
    OwnerId: Clone + PartialEq + std::fmt::Debug + Send + 'static,
    C: MongoDocumentCollection<OwnerId>,
{
    fn new_unloadable(owner_id: OwnerId, idle_after: Duration) -> Self {
        Self::new(owner_id, idle_after)
    }

    fn scan_loaded(&self, preparation: &mut MongoPreparation<'_>) -> Result<(), PersistenceError> {
        self.inner.scan_loaded(preparation)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use serde::{Deserialize, Serialize};

    use super::{MongoLazyCollection, MongoUnloadableCollection};
    use crate::document::LoadedDocument;
    use crate::document::set::MongoDocumentCollection;
    use crate::document::tracked::Tracked;
    use crate::persistence::coordinator::{MongoPersistenceCoordinator, PersistenceError};
    use crate::{MongoDocument, MongoScan};

    #[derive(Debug, Serialize, Deserialize, MongoDocument, MongoScan)]
    #[mongo(collection = "lazy_collection_tests")]
    struct TestDocument {
        #[mongo(id)]
        id: u64,
        value: i32,
    }

    /// A business collection that enumerates its only row twice, which the
    /// trait permits because `documents()` is entirely business-controlled.
    struct DuplicateEnumeratingCollection {
        rows: Vec<Tracked<TestDocument>>,
    }

    impl MongoDocumentCollection<u64> for DuplicateEnumeratingCollection {
        type Document = TestDocument;

        fn load_filter(
            _owner_id: &u64,
        ) -> Result<mongodb::bson::Document, crate::error::MongoStoreError> {
            Ok(mongodb::bson::doc! {})
        }

        fn owner_id(document: &Self::Document) -> &u64 {
            &document.id
        }

        fn from_documents(
            documents: Vec<Tracked<Self::Document>>,
        ) -> Result<Self, PersistenceError> {
            Ok(Self { rows: documents })
        }

        fn documents(&self) -> impl Iterator<Item = &Tracked<Self::Document>> {
            self.rows.iter().chain(self.rows.iter())
        }
    }

    #[test]
    fn a_collection_that_repeats_a_document_is_rejected_before_anything_detaches() {
        let mut coordinator = MongoPersistenceCoordinator::new(3);
        let tracked = coordinator
            .track_loaded(LoadedDocument {
                version: 1,
                updated_at_ms: 0,
                value: TestDocument { id: 42, value: 7 },
            })
            .expect("fixture row should attach");
        let mut collection = MongoUnloadableCollection {
            inner: MongoLazyCollection {
                owner_id: 42,
                loaded: Some(DuplicateEnumeratingCollection {
                    rows: vec![tracked],
                }),
            },
            idle_after: Duration::from_secs(1),
            last_access: Some(Instant::now() - Duration::from_secs(10)),
        };

        let error = collection
            .unload_idle(Instant::now(), &mut coordinator)
            .expect_err("a repeated document must not be detached twice");
        assert!(matches!(error, PersistenceError::DuplicateDocument(_)));
        assert!(
            collection.is_loaded(),
            "a rejected unload must leave the collection resident"
        );
        let resident = collection
            .get_loaded()
            .expect("the collection should stay loaded");
        assert!(
            coordinator
                .tracked_is_clean(&resident.rows[0])
                .expect("the row must still be registered"),
            "no document may be detached while the enumeration is rejected"
        );
    }
}
