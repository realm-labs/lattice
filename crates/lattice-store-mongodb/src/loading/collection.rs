//! Complete owner collections loaded as one lazy unit.

use std::time::{Duration, Instant};

use crate::document::MongoDocument;
use crate::document::set::MongoDocumentCollection;
use crate::persistence::coordinator::{
    MongoPersistenceCoordinator, MongoPreparation, PersistenceError,
};
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
            .find_many::<C::Document>(C::load_filter(&self.owner_id)?)
            .await?;
        for document in &loaded {
            let actual = C::owner_id(&document.value);
            if actual != &self.owner_id {
                return Err(PersistenceError::DocumentIdMismatch {
                    collection: C::Document::COLLECTION,
                    expected: format!("{:?}", self.owner_id),
                    actual: format!("{actual:?}"),
                });
            }
        }
        let documents = persistence.track_loaded_many(loaded)?;
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
        for document in collection.documents() {
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
