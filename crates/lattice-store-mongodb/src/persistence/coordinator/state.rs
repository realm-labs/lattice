//! Per-document registration state owned by one coordinator activation.

use crate::persistence::request::CreateMode;
use crate::scan::{ScanCursor, ScanSnapshot};

use super::{ConflictPolicy, PersistenceConflict};

#[derive(Debug)]
pub(super) struct DocumentState {
    pub(super) baseline: ScanSnapshot,
    pub(super) cursor: ScanCursor,
    pub(super) acknowledged_mutation_epoch: Option<u64>,
    pub(super) scanning_mutation_epoch: Option<u64>,
    pub(super) scanning_changed: bool,
    pub(super) version: i64,
    pub(super) updated_at_ms: i64,
    pub(super) presence: DocumentPresence,
    pub(super) rejection: Option<DocumentRejection>,
    pub(super) conflict_policy: ConflictPolicy,
    pub(super) conflict: Option<PersistenceConflict>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DocumentPresence {
    Persisted,
    /// Created explicitly with `track_new`; the next scan must emit Create.
    PendingCreate {
        mode: CreateMode,
    },
    /// Missing in storage and represented by an in-memory default. It remains
    /// write-free until a scan finds a real change from that default baseline.
    Absent {
        mode: CreateMode,
    },
}

impl DocumentPresence {
    pub(super) fn pending_create_mode(self) -> Option<CreateMode> {
        match self {
            Self::PendingCreate { mode } => Some(mode),
            Self::Persisted | Self::Absent { .. } => None,
        }
    }

    pub(super) fn absent_create_mode(self) -> Option<CreateMode> {
        match self {
            Self::Absent { mode } => Some(mode),
            Self::Persisted | Self::PendingCreate { .. } => None,
        }
    }

    pub(super) fn is_pending_create(self) -> bool {
        matches!(self, Self::PendingCreate { .. })
    }
}

#[derive(Debug)]
pub(super) struct DocumentRejection {
    pub(super) mutation_epoch: Option<u64>,
    pub(super) error: String,
}

impl DocumentState {
    pub(super) fn needs_tracked_scan(&self, mutation_epoch: u64) -> bool {
        self.acknowledged_mutation_epoch != Some(mutation_epoch)
            || self.scanning_mutation_epoch.is_some()
            || self.presence.is_pending_create()
    }

    pub(super) fn scan_cursor(&self) -> ScanCursor {
        self.cursor.clone()
    }

    pub(super) fn sweep_is_current(&self, mutation_epoch: Option<u64>) -> bool {
        mutation_epoch.is_none()
            || self.scanning_mutation_epoch.is_none()
            || self.scanning_mutation_epoch == mutation_epoch
    }

    pub(super) fn apply_commit_metadata(
        &mut self,
        mutation_epoch: Option<u64>,
        scan_complete: bool,
        sweep_complete: bool,
        changed: bool,
    ) -> bool {
        let Some(mutation_epoch) = mutation_epoch else {
            return false;
        };
        let changed = self.scanning_changed || changed;
        if scan_complete {
            let false_positive =
                self.acknowledged_mutation_epoch != Some(mutation_epoch) && !changed;
            self.acknowledged_mutation_epoch = Some(mutation_epoch);
            self.scanning_mutation_epoch = None;
            self.scanning_changed = false;
            false_positive
        } else {
            if sweep_complete || self.scanning_mutation_epoch.is_none() {
                self.scanning_mutation_epoch = Some(mutation_epoch);
            }
            self.scanning_changed = changed;
            false
        }
    }
}
