//! Registration lifecycle: attaching, tracking, and detaching documents.

use std::collections::BTreeSet;

use crate::document::tracked::Tracked;
use crate::document::{LoadedDocument, LoadedDocumentMeta, LoadedScannedDocument};
use crate::persistence::request::CreateMode;
use crate::persistence::types::MongoDocumentKey;
use crate::scan::{MongoScan, ScanCursor};

use super::{DocumentPresence, DocumentState, MongoPersistenceCoordinator, PersistenceError};

impl MongoPersistenceCoordinator {
    pub fn attach_loaded<D>(
        &mut self,
        value: &D,
        meta: LoadedDocumentMeta,
    ) -> Result<(), PersistenceError>
    where
        D: MongoScan,
    {
        self.attach(value, meta, None, DocumentPresence::Persisted)
    }

    pub fn attach_loaded_tracked<D>(
        &mut self,
        value: &D,
        mutation_epoch: u64,
        meta: LoadedDocumentMeta,
    ) -> Result<(), PersistenceError>
    where
        D: MongoScan,
    {
        self.attach(
            value,
            meta,
            Some(mutation_epoch),
            DocumentPresence::Persisted,
        )
    }

    pub fn track_loaded<D>(
        &mut self,
        loaded: LoadedDocument<D>,
    ) -> Result<Tracked<D>, PersistenceError>
    where
        D: MongoScan,
    {
        let (value, meta) = loaded.split();
        self.attach_loaded_tracked(&value, 0, meta)?;
        Ok(self.tracked(value))
    }

    #[doc(hidden)]
    pub fn track_loaded_scanned<D>(
        &mut self,
        loaded: LoadedScannedDocument<D>,
    ) -> Result<Tracked<D>, PersistenceError>
    where
        D: MongoScan,
    {
        let (loaded, baseline) = loaded.into_parts();
        let LoadedDocument {
            version,
            updated_at_ms,
            value,
        } = loaded;
        let key = MongoDocumentKey::for_document::<D>(value.id())?;
        if self.documents.contains_key(&key) {
            return Err(PersistenceError::DuplicateDocument(key));
        }
        self.documents.insert(
            key,
            DocumentState {
                baseline,
                cursor: ScanCursor::default(),
                acknowledged_mutation_epoch: Some(0),
                scanning_mutation_epoch: None,
                scanning_changed: false,
                version,
                updated_at_ms,
                presence: DocumentPresence::Persisted,
                rejection: None,
                conflict_policy: D::CONFLICT_POLICY,
                conflict: None,
            },
        );
        Ok(self.tracked(value))
    }

    #[doc(hidden)]
    pub fn track_loaded_scanned_many<D>(
        &mut self,
        loaded: Vec<LoadedScannedDocument<D>>,
    ) -> Result<Vec<Tracked<D>>, PersistenceError>
    where
        D: MongoScan,
    {
        let mut keys = BTreeSet::new();
        let mut pending = Vec::with_capacity(loaded.len());
        for loaded in loaded {
            let (loaded, baseline) = loaded.into_parts();
            let LoadedDocument {
                version,
                updated_at_ms,
                value,
            } = loaded;
            let key = MongoDocumentKey::for_document::<D>(value.id())?;
            if self.documents.contains_key(&key) || !keys.insert(key.clone()) {
                return Err(PersistenceError::DuplicateDocument(key));
            }
            pending.push((
                key,
                DocumentState {
                    baseline,
                    cursor: ScanCursor::default(),
                    acknowledged_mutation_epoch: Some(0),
                    scanning_mutation_epoch: None,
                    scanning_changed: false,
                    version,
                    updated_at_ms,
                    presence: DocumentPresence::Persisted,
                    rejection: None,
                    conflict_policy: D::CONFLICT_POLICY,
                    conflict: None,
                },
                value,
            ));
        }
        let mut tracked = Vec::with_capacity(pending.len());
        for (key, state, value) in pending {
            self.documents.insert(key, state);
            tracked.push(self.tracked(value));
        }
        Ok(tracked)
    }

    /// Atomically registers a runtime-sized batch of loaded documents of one
    /// type and returns actor-local tracked values in input order.
    pub fn track_loaded_many<D>(
        &mut self,
        loaded: Vec<LoadedDocument<D>>,
    ) -> Result<Vec<Tracked<D>>, PersistenceError>
    where
        D: MongoScan,
    {
        let mut pending = Vec::with_capacity(loaded.len());
        let mut keys = BTreeSet::new();

        for loaded in loaded {
            let (value, meta) = loaded.split();
            let key = MongoDocumentKey::for_document::<D>(value.id())?;
            if self.documents.contains_key(&key) || !keys.insert(key.clone()) {
                return Err(PersistenceError::DuplicateDocument(key));
            }
            pending.push((
                key,
                DocumentState {
                    baseline: value.capture()?,
                    cursor: ScanCursor::default(),
                    acknowledged_mutation_epoch: Some(0),
                    scanning_mutation_epoch: None,
                    scanning_changed: false,
                    version: meta.version,
                    updated_at_ms: meta.updated_at_ms,
                    presence: DocumentPresence::Persisted,
                    rejection: None,
                    conflict_policy: D::CONFLICT_POLICY,
                    conflict: None,
                },
                value,
            ));
        }

        let mut tracked = Vec::with_capacity(pending.len());
        for (key, state, value) in pending {
            self.documents.insert(key, state);
            tracked.push(self.tracked(value));
        }
        Ok(tracked)
    }

    pub fn attach_new<D>(&mut self, value: &D, mode: CreateMode) -> Result<(), PersistenceError>
    where
        D: MongoScan,
    {
        self.attach(
            value,
            LoadedDocumentMeta {
                version: 0,
                updated_at_ms: 0,
            },
            None,
            DocumentPresence::PendingCreate { mode },
        )?;
        self.mutation_signal.mark_dirty();
        Ok(())
    }

    pub fn track_new<D>(
        &mut self,
        value: D,
        mode: CreateMode,
    ) -> Result<Tracked<D>, PersistenceError>
    where
        D: MongoScan,
    {
        self.attach_new(&value, mode)?;
        Ok(self.tracked(value))
    }

    /// Tracks a document known to be absent from storage using its current
    /// value as the in-memory baseline. No Create is prepared until a real BSON
    /// change from that baseline is found.
    pub fn track_absent<D>(&mut self, value: D) -> Result<Tracked<D>, PersistenceError>
    where
        D: MongoScan,
    {
        self.attach(
            &value,
            LoadedDocumentMeta {
                version: 0,
                updated_at_ms: 0,
            },
            Some(0),
            DocumentPresence::Absent {
                mode: CreateMode::InsertOnly,
            },
        )?;
        Ok(self.tracked(value))
    }

    fn attach<D>(
        &mut self,
        value: &D,
        meta: LoadedDocumentMeta,
        mutation_epoch: Option<u64>,
        presence: DocumentPresence,
    ) -> Result<(), PersistenceError>
    where
        D: MongoScan,
    {
        let key = MongoDocumentKey::for_document::<D>(value.id())?;
        if self.documents.contains_key(&key) {
            return Err(PersistenceError::DuplicateDocument(key));
        }
        self.documents.insert(
            key,
            DocumentState {
                baseline: value.capture()?,
                cursor: ScanCursor::default(),
                acknowledged_mutation_epoch: mutation_epoch,
                scanning_mutation_epoch: None,
                scanning_changed: false,
                version: meta.version,
                updated_at_ms: meta.updated_at_ms,
                presence,
                rejection: None,
                conflict_policy: D::CONFLICT_POLICY,
                conflict: None,
            },
        );
        Ok(())
    }

    /// Unregisters a document without deleting it from MongoDB.
    pub fn detach<D>(&mut self, id: &D::Id) -> Result<(), PersistenceError>
    where
        D: MongoScan,
    {
        if self.in_flight.is_some() {
            return Err(PersistenceError::FlushInFlight);
        }
        if self.has_blocking_conflict() {
            return Err(PersistenceError::ConflictBlocked);
        }
        let key = MongoDocumentKey::for_document::<D>(id)?;
        let document = self
            .documents
            .get(&key)
            .ok_or_else(|| PersistenceError::UnknownDocument(key.clone()))?;
        if document.presence.is_pending_create() {
            return Err(PersistenceError::CreatePending(key));
        }
        if document.conflict.is_some() {
            return Err(PersistenceError::DocumentConflictPending(key));
        }
        if document.rejection.is_some() {
            return Err(PersistenceError::DocumentRejectionPending(key));
        }
        if self.retry_pending_contains(&key) {
            return Err(PersistenceError::DocumentRetryPending(key));
        }
        self.documents.remove(&key);
        self.clear_last_error_if_recovered();
        Ok(())
    }

    /// Returns whether a pending exact retry still replays a commit for this
    /// document. Its completion resolves against the registration, so the
    /// document must stay registered until the retry is applied or aborted.
    fn retry_pending_contains(&self, key: &MongoDocumentKey) -> bool {
        self.retry_pending.as_ref().is_some_and(|pending| {
            pending
                .commit
                .document_commits
                .values()
                .chain(pending.commit.clean_commits.iter())
                .any(|commit| commit.key == *key)
        })
    }

    /// Returns whether a tracked document is durably clean and can be
    /// detached without losing actor-local state.
    pub fn tracked_is_clean<D>(&self, tracked: &Tracked<D>) -> Result<bool, PersistenceError>
    where
        D: MongoScan,
    {
        let value = tracked.read();
        let key = MongoDocumentKey::for_document::<D>(value.id())?;
        let state = self
            .documents
            .get(&key)
            .ok_or_else(|| PersistenceError::UnknownDocument(key.clone()))?;
        let in_flight = self.in_flight.as_ref().is_some_and(|commit| {
            commit
                .document_commits
                .values()
                .chain(commit.clean_commits.iter())
                .any(|document| document.key == key)
        });
        let conflicted = state.conflict.is_some();
        Ok(!in_flight
            && !conflicted
            && !self.retry_pending_contains(&key)
            && !state.presence.is_pending_create()
            && state.rejection.is_none()
            && state.scanning_mutation_epoch.is_none()
            && state.cursor == ScanCursor::default()
            && state.acknowledged_mutation_epoch == Some(tracked.mutation_epoch()))
    }

    /// Detaches a tracked document only when its current mutation epoch has
    /// already been acknowledged by storage.
    pub fn detach_tracked_if_clean<D>(
        &mut self,
        tracked: &Tracked<D>,
    ) -> Result<bool, PersistenceError>
    where
        D: MongoScan,
    {
        if !self.tracked_is_clean(tracked)? {
            return Ok(false);
        }
        let key = MongoDocumentKey::for_document::<D>(tracked.read().id())?;
        self.documents.remove(&key);
        Ok(true)
    }
}
