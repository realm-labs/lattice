use std::sync::atomic::{AtomicU64, Ordering};

/// A lock-free point-in-time view of one Association's transport hot-path counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AssociationMetricsSnapshot {
    pub outbound_queue_rejections: u64,
    pub association_byte_budget_rejections: u64,
    pub node_byte_budget_rejections: u64,
    pub outbound_write_batches: u64,
    pub outbound_written_frames: u64,
    pub outbound_socket_writes: u64,
    pub exact_target_cache_hits: u64,
    pub exact_target_cache_misses: u64,
}

impl AssociationMetricsSnapshot {
    /// Returns the mean number of frames included in each completed outbound write batch.
    pub fn average_outbound_batch_frames(self) -> f64 {
        if self.outbound_write_batches == 0 {
            return 0.0;
        }
        self.outbound_written_frames as f64 / self.outbound_write_batches as f64
    }

    /// Returns the mean number of socket write calls needed to complete one outbound batch.
    pub fn average_socket_writes_per_batch(self) -> f64 {
        if self.outbound_write_batches == 0 {
            return 0.0;
        }
        self.outbound_socket_writes as f64 / self.outbound_write_batches as f64
    }
}

#[derive(Debug, Default)]
pub(crate) struct AssociationMetrics {
    outbound_queue_rejections: AtomicU64,
    association_byte_budget_rejections: AtomicU64,
    node_byte_budget_rejections: AtomicU64,
    outbound_write_batches: AtomicU64,
    outbound_written_frames: AtomicU64,
    outbound_socket_writes: AtomicU64,
    exact_target_cache_hits: AtomicU64,
    exact_target_cache_misses: AtomicU64,
}

impl AssociationMetrics {
    pub(crate) fn record_queue_rejection(&self) {
        self.outbound_queue_rejections
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_association_byte_budget_rejection(&self) {
        self.association_byte_budget_rejections
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_node_byte_budget_rejection(&self) {
        self.node_byte_budget_rejections
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_write_batch(&self, frames: usize, socket_writes: usize) {
        self.outbound_write_batches.fetch_add(1, Ordering::Relaxed);
        self.outbound_written_frames
            .fetch_add(frames as u64, Ordering::Relaxed);
        self.outbound_socket_writes
            .fetch_add(socket_writes as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_exact_target_cache(&self, hits: u64, misses: u64) {
        self.exact_target_cache_hits
            .fetch_add(hits, Ordering::Relaxed);
        self.exact_target_cache_misses
            .fetch_add(misses, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> AssociationMetricsSnapshot {
        AssociationMetricsSnapshot {
            outbound_queue_rejections: self.outbound_queue_rejections.load(Ordering::Relaxed),
            association_byte_budget_rejections: self
                .association_byte_budget_rejections
                .load(Ordering::Relaxed),
            node_byte_budget_rejections: self.node_byte_budget_rejections.load(Ordering::Relaxed),
            outbound_write_batches: self.outbound_write_batches.load(Ordering::Relaxed),
            outbound_written_frames: self.outbound_written_frames.load(Ordering::Relaxed),
            outbound_socket_writes: self.outbound_socket_writes.load(Ordering::Relaxed),
            exact_target_cache_hits: self.exact_target_cache_hits.load(Ordering::Relaxed),
            exact_target_cache_misses: self.exact_target_cache_misses.load(Ordering::Relaxed),
        }
    }
}
