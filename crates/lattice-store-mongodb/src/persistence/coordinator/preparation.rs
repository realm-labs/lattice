use std::collections::{BTreeMap, BTreeSet};

use crate::document::{encode_business_document, encode_document_id, tracked::Tracked};
use crate::persistence::request::{
    DocumentCommit, DocumentOperation, FlushGeneration, FlushRequest, InFlightCommit,
    PreparedDocumentWrite, PreparedFlush, WriteToken,
};
use crate::persistence::types::MongoDocumentKey;
use crate::scan::{FieldChange, MongoScan, ScanBudget};

use super::{DocumentState, MongoPreparation, PersistenceError};

impl<'a> MongoPreparation<'a> {
    pub(super) fn new(
        documents: &'a BTreeMap<MongoDocumentKey, DocumentState>,
        generation: FlushGeneration,
        budget: ScanBudget,
    ) -> Self {
        Self {
            documents,
            generation,
            budget,
            next_token: 1,
            writes: Vec::new(),
            document_commits: BTreeMap::new(),
            clean_commits: Vec::new(),
            scans: 0,
            changed_paths: 0,
            scan_complete: true,
        }
    }

    pub fn scan<D>(&mut self, value: &D) -> Result<(), PersistenceError>
    where
        D: MongoScan,
    {
        self.scan_inner(value, None)
    }

    pub fn scan_tracked<D>(&mut self, tracked: &Tracked<D>) -> Result<(), PersistenceError>
    where
        D: MongoScan,
    {
        let value = tracked.read();
        let mutation_epoch = tracked.mutation_epoch();
        let key = MongoDocumentKey::for_document::<D>(value.id())?;
        let state = self
            .documents
            .get(&key)
            .ok_or_else(|| PersistenceError::UnknownDocument(key))?;
        if !state.needs_tracked_scan(mutation_epoch) {
            return Ok(());
        }
        self.scan_inner(value, Some(mutation_epoch))
    }

    fn scan_inner<D>(
        &mut self,
        value: &D,
        mutation_epoch: Option<u64>,
    ) -> Result<(), PersistenceError>
    where
        D: MongoScan,
    {
        let key = MongoDocumentKey::for_document::<D>(value.id())?;
        let state = self
            .documents
            .get(&key)
            .ok_or_else(|| PersistenceError::UnknownDocument(key.clone()))?;
        if state
            .rejection
            .as_ref()
            .is_some_and(|rejection| rejection.mutation_epoch == mutation_epoch)
        {
            self.scan_complete = false;
            return Ok(());
        }
        let cursor = state.scan_cursor();
        let delta = value.diff(&state.baseline, cursor.clone(), &mut self.budget)?;
        let scan_complete = delta.complete && state.sweep_is_current(mutation_epoch);
        self.scan_complete &= scan_complete;
        self.scans = self.scans.saturating_add(1);
        self.changed_paths = self
            .changed_paths
            .saturating_add(delta.changes.len() as u64);
        if delta.changes.is_empty() && !delta.complete && delta.next_cursor == cursor {
            return Ok(());
        }
        let changed = !delta.changes.is_empty() || state.create_mode.is_some();
        let commit = DocumentCommit {
            key: key.clone(),
            scan: delta.commit,
            mutation_epoch,
            sweep_complete: delta.complete,
            scan_complete,
            changed,
        };
        if delta.changes.is_empty() && state.create_mode.is_none() {
            self.clean_commits.push(commit);
            return Ok(());
        }

        let token = WriteToken(self.next_token);
        self.next_token = self
            .next_token
            .checked_add(1)
            .ok_or(PersistenceError::WriteTokenOverflow)?;
        let operation = if let Some(mode) = state.create_mode {
            DocumentOperation::Create {
                document: encode_business_document(value)?,
                mode,
            }
        } else {
            let mut sets = BTreeMap::new();
            let mut unsets = BTreeSet::new();
            for change in delta.changes {
                match change {
                    FieldChange::Set { path, value } => {
                        sets.insert(path, value);
                    }
                    FieldChange::Unset { path } => {
                        unsets.insert(path);
                    }
                }
            }
            DocumentOperation::Update { sets, unsets }
        };
        self.writes.push(PreparedDocumentWrite {
            token,
            key,
            document_id: encode_document_id::<D>(value.id())?,
            expected_version: state.version,
            operation_id: uuid::Uuid::new_v4().simple().to_string(),
            operation,
        });
        self.document_commits.insert(token, commit);
        Ok(())
    }

    pub(super) fn finish(self) -> PreparedFlush {
        let writes = self
            .writes
            .iter()
            .cloned()
            .map(|write| (write.token, write))
            .collect();
        let commit = InFlightCommit {
            generation: self.generation,
            document_commits: self.document_commits,
            clean_commits: self.clean_commits,
            writes,
        };
        let request = (!self.writes.is_empty()).then_some(FlushRequest {
            generation: self.generation,
            writes: self.writes,
        });
        PreparedFlush {
            request,
            commit,
            scan_complete: self.scan_complete,
        }
    }
}
