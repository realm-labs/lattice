//! Lazy singleton documents.

use std::time::{Duration, Instant};

use crate::document::MongoDocument;
use crate::document::tracked::Tracked;
use crate::persistence::coordinator::{
    MongoPersistenceCoordinator, MongoPreparation, PersistenceError,
};
use crate::persistence::request::CreateMode;
use crate::scan::MongoScan;
use crate::store::MongoStore;

use super::policy::{IdleUnloadStatus, MongoLazyField, MongoUnloadableField};

/// A required singleton document loaded on first asynchronous access and then
/// retained for the actor lifetime.
#[derive(Debug)]
pub struct MongoLazyDocument<D>
where
    D: MongoScan,
{
    id: D::Id,
    loaded: Option<Tracked<D>>,
}

impl<D> MongoLazyDocument<D>
where
    D: MongoScan,
{
    pub fn new(id: D::Id) -> Self {
        Self { id, loaded: None }
    }

    pub fn id(&self) -> &D::Id {
        &self.id
    }

    pub fn is_loaded(&self) -> bool {
        self.loaded.is_some()
    }

    /// Returns the currently loaded value without triggering I/O.
    pub fn get_loaded(&self) -> Option<&D> {
        self.loaded.as_ref().map(Tracked::read)
    }

    /// Returns mutable access only when the document is already resident.
    /// This never performs I/O.
    pub fn get_loaded_mut(&mut self) -> Option<&mut D> {
        self.loaded.as_mut().map(Tracked::write)
    }

    /// Returns the currently loaded tracked value without triggering I/O.
    pub fn get_loaded_tracked(&self) -> Option<&Tracked<D>> {
        self.loaded.as_ref()
    }

    pub async fn get<'a>(
        &'a mut self,
        store: &MongoStore,
        persistence: &mut MongoPersistenceCoordinator,
    ) -> Result<&'a D, PersistenceError> {
        self.ensure_loaded(store, persistence).await?;
        Ok(self
            .loaded
            .as_ref()
            .expect("lazy document just loaded")
            .read())
    }

    pub async fn get_mut<'a>(
        &'a mut self,
        store: &MongoStore,
        persistence: &mut MongoPersistenceCoordinator,
    ) -> Result<&'a mut D, PersistenceError> {
        self.ensure_loaded(store, persistence).await?;
        Ok(self
            .loaded
            .as_mut()
            .expect("lazy document just loaded")
            .write())
    }

    pub async fn get_optional<'a>(
        &'a mut self,
        store: &MongoStore,
        persistence: &mut MongoPersistenceCoordinator,
    ) -> Result<Option<&'a D>, PersistenceError> {
        self.ensure_loaded_optional(store, persistence).await?;
        Ok(self.loaded.as_ref().map(Tracked::read))
    }

    /// Registers a newly created singleton without querying MongoDB.
    pub fn attach_new<'a>(
        &'a mut self,
        persistence: &mut MongoPersistenceCoordinator,
        value: D,
        mode: CreateMode,
    ) -> Result<&'a mut D, PersistenceError> {
        if value.id() != &self.id {
            return Err(PersistenceError::DocumentIdMismatch {
                collection: D::COLLECTION,
                expected: format!("{:?}", self.id),
                actual: format!("{:?}", value.id()),
            });
        }
        let tracked = persistence.track_new(value, mode)?;
        self.loaded = Some(tracked);
        Ok(self
            .loaded
            .as_mut()
            .expect("new document just attached")
            .write())
    }

    async fn ensure_loaded(
        &mut self,
        store: &MongoStore,
        persistence: &mut MongoPersistenceCoordinator,
    ) -> Result<(), PersistenceError> {
        if self.ensure_loaded_optional(store, persistence).await? {
            return Ok(());
        }
        Err(PersistenceError::RequiredDocumentMissing {
            collection: D::COLLECTION,
            id: format!("{:?}", self.id),
        })
    }

    async fn ensure_loaded_optional(
        &mut self,
        store: &MongoStore,
        persistence: &mut MongoPersistenceCoordinator,
    ) -> Result<bool, PersistenceError> {
        if self.loaded.is_some() {
            return Ok(true);
        }
        let Some(loaded) = store.find_one_scanned::<D>(self.id.clone()).await? else {
            return Ok(false);
        };
        self.loaded = Some(persistence.track_loaded_scanned(loaded)?);
        Ok(true)
    }

    pub(crate) fn scan_loaded(
        &self,
        preparation: &mut MongoPreparation<'_>,
    ) -> Result<(), PersistenceError> {
        if let Some(document) = &self.loaded {
            preparation.scan_tracked(document)?;
        }
        Ok(())
    }
}

impl<OwnerId, D> MongoLazyField<OwnerId> for MongoLazyDocument<D>
where
    OwnerId: Send,
    D: MongoScan + MongoDocument<Id = OwnerId>,
{
    fn new_lazy(owner_id: OwnerId) -> Self {
        Self::new(owner_id)
    }

    fn scan_loaded(&self, preparation: &mut MongoPreparation<'_>) -> Result<(), PersistenceError> {
        self.scan_loaded(preparation)
    }
}

/// A required singleton lazy document that may detach after an idle period.
#[derive(Debug)]
pub struct MongoUnloadableDocument<D>
where
    D: MongoScan,
{
    inner: MongoLazyDocument<D>,
    idle_after: Duration,
    last_access: Option<Instant>,
}

impl<D> MongoUnloadableDocument<D>
where
    D: MongoScan,
{
    pub fn new(id: D::Id, idle_after: Duration) -> Self {
        assert!(
            !idle_after.is_zero(),
            "idle unload duration must be positive"
        );
        Self {
            inner: MongoLazyDocument::new(id),
            idle_after,
            last_access: None,
        }
    }

    pub fn id(&self) -> &D::Id {
        self.inner.id()
    }

    pub fn is_loaded(&self) -> bool {
        self.inner.is_loaded()
    }

    pub fn get_loaded(&mut self) -> Option<&D> {
        let value = self.inner.get_loaded()?;
        self.last_access = Some(Instant::now());
        Some(value)
    }

    pub fn get_loaded_mut(&mut self) -> Option<&mut D> {
        let value = self.inner.get_loaded_mut()?;
        self.last_access = Some(Instant::now());
        Some(value)
    }

    pub async fn get<'a>(
        &'a mut self,
        store: &MongoStore,
        persistence: &mut MongoPersistenceCoordinator,
    ) -> Result<&'a D, PersistenceError> {
        self.last_access = Some(Instant::now());
        self.inner.get(store, persistence).await
    }

    pub async fn get_mut<'a>(
        &'a mut self,
        store: &MongoStore,
        persistence: &mut MongoPersistenceCoordinator,
    ) -> Result<&'a mut D, PersistenceError> {
        self.last_access = Some(Instant::now());
        self.inner.get_mut(store, persistence).await
    }

    pub fn attach_new<'a>(
        &'a mut self,
        persistence: &mut MongoPersistenceCoordinator,
        value: D,
        mode: CreateMode,
    ) -> Result<&'a mut D, PersistenceError> {
        self.last_access = Some(Instant::now());
        self.inner.attach_new(persistence, value, mode)
    }

    pub fn unload_idle(
        &mut self,
        now: Instant,
        persistence: &mut MongoPersistenceCoordinator,
    ) -> Result<IdleUnloadStatus, PersistenceError> {
        let Some(document) = self.inner.loaded.as_ref() else {
            return Ok(IdleUnloadStatus::NotLoaded);
        };
        let Some(last_access) = self.last_access else {
            return Ok(IdleUnloadStatus::NotIdle);
        };
        if now.saturating_duration_since(last_access) < self.idle_after {
            return Ok(IdleUnloadStatus::NotIdle);
        }
        if !persistence.detach_tracked_if_clean(document)? {
            return Ok(IdleUnloadStatus::NeedsFlush);
        }
        self.inner.loaded = None;
        self.last_access = None;
        Ok(IdleUnloadStatus::Unloaded)
    }
}

impl<OwnerId, D> MongoUnloadableField<OwnerId> for MongoUnloadableDocument<D>
where
    OwnerId: Send,
    D: MongoScan + MongoDocument<Id = OwnerId>,
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
    use crate::document::LoadedDocument;
    use crate::persistence::coordinator::MongoPersistenceCoordinator;
    use crate::{MongoDocument, MongoScan};
    use serde::{Deserialize, Serialize};
    use std::time::{Duration, Instant};

    use super::{IdleUnloadStatus, MongoUnloadableDocument};

    #[derive(Debug, Serialize, Deserialize, MongoDocument, MongoScan)]
    #[mongo(collection = "lazy_unload_tests")]
    struct TestDocument {
        #[mongo(id)]
        id: u64,
        value: i32,
    }

    fn loaded_slot(
        coordinator: &mut MongoPersistenceCoordinator,
        now: Instant,
    ) -> MongoUnloadableDocument<TestDocument> {
        let tracked = coordinator
            .track_loaded(LoadedDocument {
                version: 1,
                updated_at_ms: 0,
                value: TestDocument { id: 7, value: 1 },
            })
            .expect("test document should attach");
        let mut slot = MongoUnloadableDocument::new(7, Duration::from_secs(10));
        slot.inner.loaded = Some(tracked);
        slot.last_access = Some(now - Duration::from_secs(11));
        slot
    }

    #[test]
    fn idle_unload_detaches_only_acknowledged_documents() {
        let now = Instant::now();
        let mut coordinator = MongoPersistenceCoordinator::new(1);
        let mut slot = loaded_slot(&mut coordinator, now);

        assert_eq!(
            slot.unload_idle(now, &mut coordinator)
                .expect("clean unload should succeed"),
            IdleUnloadStatus::Unloaded
        );
        assert!(!slot.is_loaded());
    }

    #[test]
    fn idle_unload_retains_dirty_documents_until_they_are_flushed() {
        let now = Instant::now();
        let mut coordinator = MongoPersistenceCoordinator::new(1);
        let mut slot = loaded_slot(&mut coordinator, now);
        slot.get_loaded_mut().expect("document is resident").value += 1;
        slot.last_access = Some(now - Duration::from_secs(11));

        assert_eq!(
            slot.unload_idle(now, &mut coordinator)
                .expect("dirty check should succeed"),
            IdleUnloadStatus::NeedsFlush
        );
        assert!(slot.is_loaded());
    }
}
