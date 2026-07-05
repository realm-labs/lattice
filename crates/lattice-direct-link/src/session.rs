use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use lattice_core::{DirectLinkMessageId, DirectLinkSession, LinkCloseReason, LinkError, LinkId};

#[derive(Debug, Default, Clone)]
pub struct DirectLinkMetrics {
    inner: Arc<Mutex<DirectLinkMetricsInner>>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DirectLinkMetricsSnapshot {
    pub opened: u64,
    pub closed: u64,
    pub dropped: u64,
    pub coalesced: u64,
    pub decode_errors: u64,
    pub backpressure_events: u64,
}

#[derive(Debug, Default)]
struct DirectLinkMetricsInner {
    snapshot: DirectLinkMetricsSnapshot,
}

impl DirectLinkMetrics {
    pub fn snapshot(&self) -> DirectLinkMetricsSnapshot {
        self.inner
            .lock()
            .expect("direct link metrics poisoned")
            .snapshot
            .clone()
    }

    pub fn record_open(&self) {
        self.inner
            .lock()
            .expect("direct link metrics poisoned")
            .snapshot
            .opened += 1;
    }

    pub fn record_close(&self) {
        self.inner
            .lock()
            .expect("direct link metrics poisoned")
            .snapshot
            .closed += 1;
    }
}

#[derive(Debug, Default)]
pub struct DirectLinkSessionManager {
    sessions: Mutex<BTreeMap<LinkId, DirectLinkSession>>,
    metrics: DirectLinkMetrics,
}

impl DirectLinkSessionManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn metrics(&self) -> DirectLinkMetrics {
        self.metrics.clone()
    }

    pub fn insert(&self, session: DirectLinkSession) -> Result<(), LinkError> {
        let duplicate =
            session
                .stream
                .duplicate_message_id()
                .map(|message_id| LinkError::DuplicateMessageId {
                    stream_name: session.stream.stream_name.clone(),
                    message_id,
                });
        if let Some(error) = duplicate {
            return Err(error);
        }
        self.sessions
            .lock()
            .expect("direct link sessions poisoned")
            .insert(session.link_id.clone(), session);
        self.metrics.record_open();
        Ok(())
    }

    pub fn accepted_message_ids(&self, link_id: &LinkId) -> Option<BTreeSet<DirectLinkMessageId>> {
        self.sessions
            .lock()
            .expect("direct link sessions poisoned")
            .get(link_id)
            .map(|session| session.accepted_message_ids.clone())
    }

    pub fn close(&self, link_id: &LinkId, _reason: LinkCloseReason) -> bool {
        let removed = self
            .sessions
            .lock()
            .expect("direct link sessions poisoned")
            .remove(link_id)
            .is_some();
        if removed {
            self.metrics.record_close();
        }
        removed
    }
}
