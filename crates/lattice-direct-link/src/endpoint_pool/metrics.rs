use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::endpoint_pool::DirectLinkConnectionId;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DirectLinkEndpointPoolMetricsSnapshot {
    pub physical_connections_opened: u64,
    pub physical_connections_closed: u64,
    pub active_physical_connections: u64,
    pub logical_links_opened: u64,
    pub logical_links_closed: u64,
    pub active_logical_links: u64,
    pub frames_written: u64,
    pub reconnects: u64,
    pub pool_rejections: u64,
    pub pool_queue_backpressure_events: u64,
    pub links_per_connection: BTreeMap<DirectLinkConnectionId, usize>,
    pub frames_per_connection: BTreeMap<DirectLinkConnectionId, u64>,
}

#[derive(Debug, Default)]
pub(crate) struct DirectLinkEndpointPoolMetrics {
    physical_connections_opened: AtomicU64,
    physical_connections_closed: AtomicU64,
    active_physical_connections: AtomicU64,
    logical_links_opened: AtomicU64,
    logical_links_closed: AtomicU64,
    active_logical_links: AtomicU64,
    frames_written: AtomicU64,
    reconnects: AtomicU64,
    pool_rejections: AtomicU64,
    pool_queue_backpressure_events: AtomicU64,
    links_per_connection: std::sync::Mutex<BTreeMap<DirectLinkConnectionId, usize>>,
    frames_per_connection: std::sync::Mutex<BTreeMap<DirectLinkConnectionId, u64>>,
}

impl DirectLinkEndpointPoolMetrics {
    pub(crate) fn snapshot(&self) -> DirectLinkEndpointPoolMetricsSnapshot {
        DirectLinkEndpointPoolMetricsSnapshot {
            physical_connections_opened: self.physical_connections_opened.load(Ordering::Relaxed),
            physical_connections_closed: self.physical_connections_closed.load(Ordering::Relaxed),
            active_physical_connections: self.active_physical_connections.load(Ordering::Relaxed),
            logical_links_opened: self.logical_links_opened.load(Ordering::Relaxed),
            logical_links_closed: self.logical_links_closed.load(Ordering::Relaxed),
            active_logical_links: self.active_logical_links.load(Ordering::Relaxed),
            frames_written: self.frames_written.load(Ordering::Relaxed),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            pool_rejections: self.pool_rejections.load(Ordering::Relaxed),
            pool_queue_backpressure_events: self
                .pool_queue_backpressure_events
                .load(Ordering::Relaxed),
            links_per_connection: self
                .links_per_connection
                .lock()
                .expect("direct link endpoint pool link metrics poisoned")
                .clone(),
            frames_per_connection: self
                .frames_per_connection
                .lock()
                .expect("direct link endpoint pool frame metrics poisoned")
                .clone(),
        }
    }

    pub(crate) fn record_connection_opened(&self, connection_id: DirectLinkConnectionId) {
        self.physical_connections_opened
            .fetch_add(1, Ordering::Relaxed);
        self.active_physical_connections
            .fetch_add(1, Ordering::Relaxed);
        self.links_per_connection
            .lock()
            .expect("direct link endpoint pool link metrics poisoned")
            .entry(connection_id)
            .or_insert(0);
        self.frames_per_connection
            .lock()
            .expect("direct link endpoint pool frame metrics poisoned")
            .entry(connection_id)
            .or_insert(0);
    }

    pub(crate) fn record_connection_closed(&self, connection_id: DirectLinkConnectionId) {
        self.physical_connections_closed
            .fetch_add(1, Ordering::Relaxed);
        self.active_physical_connections
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(1))
            })
            .ok();
        self.links_per_connection
            .lock()
            .expect("direct link endpoint pool link metrics poisoned")
            .remove(&connection_id);
    }

    pub(crate) fn record_link_opened(&self, connection_id: DirectLinkConnectionId) {
        self.logical_links_opened.fetch_add(1, Ordering::Relaxed);
        self.active_logical_links.fetch_add(1, Ordering::Relaxed);
        *self
            .links_per_connection
            .lock()
            .expect("direct link endpoint pool link metrics poisoned")
            .entry(connection_id)
            .or_insert(0) += 1;
    }

    pub(crate) fn record_link_closed(&self, connection_id: DirectLinkConnectionId) {
        self.logical_links_closed.fetch_add(1, Ordering::Relaxed);
        self.active_logical_links
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(1))
            })
            .ok();
        let mut links = self
            .links_per_connection
            .lock()
            .expect("direct link endpoint pool link metrics poisoned");
        if let Some(count) = links.get_mut(&connection_id) {
            *count = count.saturating_sub(1);
        }
    }

    pub(crate) fn record_frame_written(&self, connection_id: DirectLinkConnectionId) {
        self.frames_written.fetch_add(1, Ordering::Relaxed);
        *self
            .frames_per_connection
            .lock()
            .expect("direct link endpoint pool frame metrics poisoned")
            .entry(connection_id)
            .or_insert(0) += 1;
    }

    pub(crate) fn record_pool_rejection(&self) {
        self.pool_rejections.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_pool_queue_backpressure(&self) {
        self.pool_queue_backpressure_events
            .fetch_add(1, Ordering::Relaxed);
    }
}
