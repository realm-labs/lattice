//! Row-level lazy MongoDB tables with bounded resident caches.

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::ops::Bound::{Excluded, Unbounded};
use std::time::{Duration, Instant};

use mongodb::bson::{Bson, Document, doc};
use serde::Serialize;

use crate::document::MongoDocument;
use crate::document::tracked::Tracked;
use crate::error::MongoStoreError;
use crate::persistence::coordinator::{
    MongoPersistenceCoordinator, MongoPreparation, PersistenceError,
};
use crate::persistence::request::CreateMode;
use crate::scan::MongoScan;
use crate::store::MongoStore;

use super::policy::{MongoLazyField, MongoUnloadableField};

/// Business mapping between an aggregate owner, a row cache key, and the
/// document identity stored in MongoDB.
pub trait MongoTableSpec<OwnerId>: Send + Sync + 'static {
    type Key: Clone + Ord + std::fmt::Debug + Serialize + Send + 'static;
    type Document: MongoScan;

    /// Mongo field used for stable keyset pagination.
    const PAGE_KEY_FIELD: &'static str;

    fn document_id(owner_id: &OwnerId, key: &Self::Key) -> <Self::Document as MongoDocument>::Id;

    fn owner_id(document: &Self::Document) -> &OwnerId;

    fn key(document: &Self::Document) -> &Self::Key;

    fn owner_filter(owner_id: &OwnerId) -> Result<Document, MongoStoreError>;

    fn encode_page_key(key: &Self::Key) -> Result<Bson, MongoStoreError> {
        mongodb::bson::to_bson(key)
            .map_err(|error| MongoStoreError::encode("encode Mongo table page key", error))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableEvictionBudget {
    pub max_rows: usize,
}

impl Default for TableEvictionBudget {
    fn default() -> Self {
        Self { max_rows: 128 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TableEvictionReport {
    pub examined: usize,
    pub unloaded: usize,
    pub needs_flush: usize,
}

#[derive(Debug)]
struct LoadedTableRow<D>
where
    D: MongoScan,
{
    document: Tracked<D>,
    last_access: Instant,
}

/// A row-level table that loads documents by key and retains every loaded row
/// for the actor lifetime.
#[derive(Debug)]
pub struct MongoLazyTable<OwnerId, S>
where
    S: MongoTableSpec<OwnerId>,
{
    owner_id: OwnerId,
    rows: BTreeMap<S::Key, LoadedTableRow<S::Document>>,
    marker: PhantomData<S>,
}

impl<OwnerId, S> MongoLazyTable<OwnerId, S>
where
    OwnerId: Clone + PartialEq + std::fmt::Debug + Send + 'static,
    S: MongoTableSpec<OwnerId>,
{
    pub fn new(owner_id: OwnerId) -> Self {
        Self {
            owner_id,
            rows: BTreeMap::new(),
            marker: PhantomData,
        }
    }

    pub fn owner_id(&self) -> &OwnerId {
        &self.owner_id
    }

    pub fn loaded_len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_loaded(&self, key: &S::Key) -> bool {
        self.rows.contains_key(key)
    }

    pub fn get_loaded(&self, key: &S::Key) -> Option<&S::Document> {
        self.rows.get(key).map(|row| row.document.read())
    }

    pub fn get_loaded_mut(&mut self, key: &S::Key) -> Option<&mut S::Document> {
        let row = self.rows.get_mut(key)?;
        row.last_access = Instant::now();
        Some(row.document.write())
    }

    pub fn iter_loaded(&self) -> impl Iterator<Item = &S::Document> {
        self.rows.values().map(|row| row.document.read())
    }

    pub async fn get<'a>(
        &'a mut self,
        store: &MongoStore,
        persistence: &mut MongoPersistenceCoordinator,
        key: &S::Key,
    ) -> Result<Option<&'a S::Document>, PersistenceError> {
        if !self.ensure_loaded(store, persistence, key).await? {
            return Ok(None);
        }
        let row = self.rows.get_mut(key).expect("table row just loaded");
        row.last_access = Instant::now();
        Ok(Some(row.document.read()))
    }

    pub async fn get_mut<'a>(
        &'a mut self,
        store: &MongoStore,
        persistence: &mut MongoPersistenceCoordinator,
        key: &S::Key,
    ) -> Result<Option<&'a mut S::Document>, PersistenceError> {
        if !self.ensure_loaded(store, persistence, key).await? {
            return Ok(None);
        }
        let row = self.rows.get_mut(key).expect("table row just loaded");
        row.last_access = Instant::now();
        Ok(Some(row.document.write()))
    }

    /// Registers a newly created row and makes it resident without querying
    /// MongoDB.
    pub fn attach_new<'a>(
        &'a mut self,
        persistence: &mut MongoPersistenceCoordinator,
        document: S::Document,
        mode: CreateMode,
    ) -> Result<&'a mut S::Document, PersistenceError> {
        self.validate_document(&document, None)?;
        let key = S::key(&document).clone();
        let tracked = persistence.track_new(document, mode)?;
        self.rows.insert(
            key.clone(),
            LoadedTableRow {
                document: tracked,
                last_access: Instant::now(),
            },
        );
        Ok(self
            .rows
            .get_mut(&key)
            .expect("new table row just attached")
            .document
            .write())
    }

    async fn ensure_loaded(
        &mut self,
        store: &MongoStore,
        persistence: &mut MongoPersistenceCoordinator,
        key: &S::Key,
    ) -> Result<bool, PersistenceError> {
        if self.rows.contains_key(key) {
            return Ok(true);
        }
        let id = S::document_id(&self.owner_id, key);
        let Some(loaded) = store.find_one_scanned::<S::Document>(id).await? else {
            return Ok(false);
        };
        self.validate_document(loaded.value(), Some(key))?;
        let tracked = persistence.track_loaded_scanned(loaded)?;
        self.rows.insert(
            key.clone(),
            LoadedTableRow {
                document: tracked,
                last_access: Instant::now(),
            },
        );
        Ok(true)
    }

    /// Loads one stable keyset page. Fetching the page is asynchronous; page
    /// iteration itself is an ordinary synchronous iterator.
    pub async fn load_page<'a>(
        &'a mut self,
        store: &MongoStore,
        persistence: &mut MongoPersistenceCoordinator,
        filter: Document,
        after: Option<&S::Key>,
        limit: u32,
    ) -> Result<MongoTablePage<'a, OwnerId, S>, PersistenceError> {
        if limit == 0 {
            return Err(MongoStoreError::invalid_config("page limit", "must be positive").into());
        }
        let mut filters = vec![S::owner_filter(&self.owner_id)?];
        if !filter.is_empty() {
            filters.push(filter);
        }
        if let Some(after) = after {
            let mut cursor = Document::new();
            cursor.insert(
                S::PAGE_KEY_FIELD,
                doc! { "$gt": S::encode_page_key(after)? },
            );
            filters.push(cursor);
        }
        let filter = if filters.len() == 1 {
            filters.pop().expect("one table page filter")
        } else {
            doc! { "$and": filters }
        };
        let mut sort = Document::new();
        sort.insert(S::PAGE_KEY_FIELD, 1);
        let fetch_limit = limit.checked_add(1).ok_or_else(|| {
            MongoStoreError::invalid_config("page limit", "must be less than u32::MAX")
        })?;
        let mut loaded = store
            .find_page_scanned::<S::Document>(filter, sort, fetch_limit)
            .await?;
        let has_more = loaded.len() > limit as usize;
        loaded.truncate(limit as usize);
        let mut keys = Vec::with_capacity(loaded.len());
        let mut new_keys = Vec::new();
        let mut new_documents = Vec::new();
        for document in loaded {
            self.validate_document(document.value(), None)?;
            let key = S::key(document.value()).clone();
            keys.push(key.clone());
            if !self.rows.contains_key(&key) {
                new_keys.push(key);
                new_documents.push(document);
            }
        }
        let tracked = persistence.track_loaded_scanned_many(new_documents)?;
        for (key, document) in new_keys.into_iter().zip(tracked) {
            self.rows.insert(
                key,
                LoadedTableRow {
                    document,
                    last_access: Instant::now(),
                },
            );
        }
        let now = Instant::now();
        for key in &keys {
            if let Some(row) = self.rows.get_mut(key) {
                row.last_access = now;
            }
        }
        Ok(MongoTablePage {
            table: self,
            keys,
            has_more,
        })
    }

    fn validate_document(
        &self,
        document: &S::Document,
        expected_key: Option<&S::Key>,
    ) -> Result<(), PersistenceError> {
        let actual_owner = S::owner_id(document);
        if actual_owner != &self.owner_id {
            return Err(PersistenceError::DocumentIdMismatch {
                collection: S::Document::COLLECTION,
                expected: format!("{:?}", self.owner_id),
                actual: format!("{actual_owner:?}"),
            });
        }
        if let Some(expected_key) = expected_key {
            let actual_key = S::key(document);
            if actual_key != expected_key {
                return Err(PersistenceError::DocumentIdMismatch {
                    collection: S::Document::COLLECTION,
                    expected: format!("{expected_key:?}"),
                    actual: format!("{actual_key:?}"),
                });
            }
        }
        let expected_id = S::document_id(&self.owner_id, S::key(document));
        if document.id() != &expected_id {
            return Err(PersistenceError::DocumentIdMismatch {
                collection: S::Document::COLLECTION,
                expected: format!("{expected_id:?}"),
                actual: format!("{:?}", document.id()),
            });
        }
        Ok(())
    }

    pub(crate) fn scan_loaded(
        &self,
        preparation: &mut MongoPreparation<'_>,
    ) -> Result<(), PersistenceError> {
        for row in self.rows.values() {
            preparation.scan_tracked(&row.document)?;
        }
        Ok(())
    }
}

impl<OwnerId, S> MongoLazyField<OwnerId> for MongoLazyTable<OwnerId, S>
where
    OwnerId: Clone + PartialEq + std::fmt::Debug + Send + 'static,
    S: MongoTableSpec<OwnerId>,
{
    fn new_lazy(owner_id: OwnerId) -> Self {
        Self::new(owner_id)
    }

    fn scan_loaded(&self, preparation: &mut MongoPreparation<'_>) -> Result<(), PersistenceError> {
        self.scan_loaded(preparation)
    }
}

pub struct MongoTablePage<'a, OwnerId, S>
where
    S: MongoTableSpec<OwnerId>,
{
    table: &'a MongoLazyTable<OwnerId, S>,
    keys: Vec<S::Key>,
    has_more: bool,
}

impl<'a, OwnerId, S> MongoTablePage<'a, OwnerId, S>
where
    OwnerId: Clone + PartialEq + std::fmt::Debug + Send + 'static,
    S: MongoTableSpec<OwnerId>,
{
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn has_more(&self) -> bool {
        self.has_more
    }

    pub fn keys(&self) -> &[S::Key] {
        &self.keys
    }

    pub fn next_cursor(&self) -> Option<&S::Key> {
        self.keys.last()
    }

    pub fn iter(&self) -> impl Iterator<Item = &S::Document> {
        self.keys
            .iter()
            .filter_map(|key| self.table.get_loaded(key))
    }
}

/// A row-level lazy table whose clean idle rows may be detached.
pub struct MongoUnloadableTable<OwnerId, S>
where
    S: MongoTableSpec<OwnerId>,
{
    inner: MongoLazyTable<OwnerId, S>,
    idle_after: Duration,
    eviction_cursor: Option<S::Key>,
}

impl<OwnerId, S> MongoUnloadableTable<OwnerId, S>
where
    OwnerId: Clone + PartialEq + std::fmt::Debug + Send + 'static,
    S: MongoTableSpec<OwnerId>,
{
    pub fn new(owner_id: OwnerId, idle_after: Duration) -> Self {
        assert!(
            !idle_after.is_zero(),
            "idle unload duration must be positive"
        );
        Self {
            inner: MongoLazyTable::new(owner_id),
            idle_after,
            eviction_cursor: None,
        }
    }

    pub fn loaded_len(&self) -> usize {
        self.inner.loaded_len()
    }

    pub fn get_loaded(&mut self, key: &S::Key) -> Option<&S::Document> {
        let row = self.inner.rows.get_mut(key)?;
        row.last_access = Instant::now();
        Some(row.document.read())
    }

    pub fn get_loaded_mut(&mut self, key: &S::Key) -> Option<&mut S::Document> {
        self.inner.get_loaded_mut(key)
    }

    pub fn iter_loaded(&mut self) -> impl Iterator<Item = &S::Document> {
        let now = Instant::now();
        for row in self.inner.rows.values_mut() {
            row.last_access = now;
        }
        self.inner.iter_loaded()
    }

    pub async fn get<'a>(
        &'a mut self,
        store: &MongoStore,
        persistence: &mut MongoPersistenceCoordinator,
        key: &S::Key,
    ) -> Result<Option<&'a S::Document>, PersistenceError> {
        self.inner.get(store, persistence, key).await
    }

    pub async fn get_mut<'a>(
        &'a mut self,
        store: &MongoStore,
        persistence: &mut MongoPersistenceCoordinator,
        key: &S::Key,
    ) -> Result<Option<&'a mut S::Document>, PersistenceError> {
        self.inner.get_mut(store, persistence, key).await
    }

    pub fn attach_new<'a>(
        &'a mut self,
        persistence: &mut MongoPersistenceCoordinator,
        document: S::Document,
        mode: CreateMode,
    ) -> Result<&'a mut S::Document, PersistenceError> {
        self.inner.attach_new(persistence, document, mode)
    }

    pub async fn load_page<'a>(
        &'a mut self,
        store: &MongoStore,
        persistence: &mut MongoPersistenceCoordinator,
        filter: Document,
        after: Option<&S::Key>,
        limit: u32,
    ) -> Result<MongoTablePage<'a, OwnerId, S>, PersistenceError> {
        self.inner
            .load_page(store, persistence, filter, after, limit)
            .await
    }

    pub fn unload_idle(
        &mut self,
        now: Instant,
        persistence: &mut MongoPersistenceCoordinator,
        budget: TableEvictionBudget,
    ) -> Result<TableEvictionReport, PersistenceError> {
        if budget.max_rows == 0 || self.inner.rows.is_empty() {
            return Ok(TableEvictionReport::default());
        }
        let keys = match &self.eviction_cursor {
            Some(cursor) => self
                .inner
                .rows
                .range((Excluded(cursor), Unbounded))
                .chain(self.inner.rows.range(..=cursor))
                .map(|(key, _)| key.clone())
                .take(budget.max_rows)
                .collect::<Vec<_>>(),
            None => self
                .inner
                .rows
                .keys()
                .take(budget.max_rows)
                .cloned()
                .collect::<Vec<_>>(),
        };
        let mut report = TableEvictionReport::default();
        for key in keys {
            report.examined += 1;
            self.eviction_cursor = Some(key.clone());
            let expired = self.inner.rows.get(&key).is_some_and(|row| {
                now.saturating_duration_since(row.last_access) >= self.idle_after
            });
            if !expired {
                continue;
            }
            let clean = {
                let row = self
                    .inner
                    .rows
                    .get(&key)
                    .expect("expired row remains loaded");
                persistence.tracked_is_clean(&row.document)?
            };
            if !clean {
                report.needs_flush += 1;
                continue;
            }
            let detached = {
                let row = self.inner.rows.get(&key).expect("clean row remains loaded");
                persistence.detach_tracked_if_clean(&row.document)?
            };
            if detached {
                self.inner.rows.remove(&key);
                report.unloaded += 1;
            } else {
                report.needs_flush += 1;
            }
        }
        if self.inner.rows.is_empty() {
            self.eviction_cursor = None;
        }
        Ok(report)
    }
}

impl<OwnerId, S> MongoUnloadableField<OwnerId> for MongoUnloadableTable<OwnerId, S>
where
    OwnerId: Clone + PartialEq + std::fmt::Debug + Send + 'static,
    S: MongoTableSpec<OwnerId>,
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
    use super::{
        LoadedTableRow, MongoLazyTable, MongoTableSpec, MongoUnloadableTable, TableEvictionBudget,
    };
    use crate::document::LoadedDocument;
    use crate::error::MongoStoreError;
    use crate::persistence::coordinator::MongoPersistenceCoordinator;
    use crate::{MongoDocument, MongoScan};
    use mongodb::bson::{Document, doc};
    use serde::{Deserialize, Serialize};
    use std::marker::PhantomData;
    use std::time::{Duration, Instant};

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
    struct TestId {
        owner: u64,
        row: u64,
    }

    #[derive(Debug, Serialize, Deserialize, MongoDocument, MongoScan)]
    #[mongo(collection = "lazy_table_tests")]
    struct TestDocument {
        #[mongo(id)]
        id: TestId,
        value: i32,
    }

    struct TestSpec;

    impl MongoTableSpec<u64> for TestSpec {
        type Key = u64;
        type Document = TestDocument;

        const PAGE_KEY_FIELD: &'static str = "_id.row";

        fn document_id(owner_id: &u64, key: &Self::Key) -> TestId {
            TestId {
                owner: *owner_id,
                row: *key,
            }
        }

        fn owner_id(document: &Self::Document) -> &u64 {
            &document.id.owner
        }

        fn key(document: &Self::Document) -> &Self::Key {
            &document.id.row
        }

        fn owner_filter(owner_id: &u64) -> Result<Document, MongoStoreError> {
            Ok(doc! { "_id.owner": i64::try_from(*owner_id).expect("test owner fits i64") })
        }
    }

    #[test]
    fn table_idle_unload_is_bounded_and_keeps_dirty_rows() {
        let now = Instant::now();
        let expired = now - Duration::from_secs(11);
        let mut coordinator = MongoPersistenceCoordinator::new(1);
        let mut rows = std::collections::BTreeMap::new();
        for key in 1..=3 {
            let tracked = coordinator
                .track_loaded(LoadedDocument {
                    version: 1,
                    updated_at_ms: 0,
                    value: TestDocument {
                        id: TestId { owner: 7, row: key },
                        value: 0,
                    },
                })
                .expect("test row should attach");
            rows.insert(
                key,
                LoadedTableRow {
                    document: tracked,
                    last_access: expired,
                },
            );
        }
        rows.get_mut(&2)
            .expect("second row exists")
            .document
            .write()
            .value = 1;
        let mut table = MongoUnloadableTable {
            inner: MongoLazyTable {
                owner_id: 7,
                rows,
                marker: PhantomData::<TestSpec>,
            },
            idle_after: Duration::from_secs(10),
            eviction_cursor: None,
        };

        let first = table
            .unload_idle(now, &mut coordinator, TableEvictionBudget { max_rows: 2 })
            .expect("bounded eviction should succeed");
        assert_eq!(first.examined, 2);
        assert_eq!(first.unloaded, 1);
        assert_eq!(first.needs_flush, 1);
        assert!(!table.inner.is_loaded(&1));
        assert!(table.inner.is_loaded(&2));
        assert!(table.inner.is_loaded(&3));

        let second = table
            .unload_idle(now, &mut coordinator, TableEvictionBudget { max_rows: 1 })
            .expect("eviction cursor should continue after the previous bucket");
        assert_eq!(second.examined, 1);
        assert_eq!(second.unloaded, 1);
        assert!(!table.inner.is_loaded(&3));
    }
}
