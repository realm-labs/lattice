use std::collections::VecDeque;

use lattice_core::direct_link::options::BackpressurePolicy;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackpressureOutcome<T> {
    Enqueued,
    WouldBlock(T),
    Rejected(T),
    DroppedNewest(T),
    DroppedOldest(T),
    Coalesced(T),
    Disconnect(T),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackpressureSnapshot {
    pub policy: BackpressurePolicy,
    pub pending: usize,
    pub dropped: u64,
    pub coalesced: u64,
}

#[derive(Debug, Clone)]
pub struct BackpressureQueue<T> {
    policy: BackpressurePolicy,
    pending: VecDeque<T>,
    dropped: u64,
    coalesced: u64,
}

impl<T> BackpressureQueue<T> {
    pub fn new(policy: BackpressurePolicy) -> Self {
        Self {
            policy,
            pending: VecDeque::new(),
            dropped: 0,
            coalesced: 0,
        }
    }

    pub fn policy(&self) -> &BackpressurePolicy {
        &self.policy
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub fn pop_front(&mut self) -> Option<T> {
        self.pending.pop_front()
    }

    pub fn snapshot(&self) -> BackpressureSnapshot {
        BackpressureSnapshot {
            policy: self.policy.clone(),
            pending: self.pending.len(),
            dropped: self.dropped,
            coalesced: self.coalesced,
        }
    }

    pub fn try_enqueue(&mut self, item: T) -> BackpressureOutcome<T> {
        let max_pending = self.policy.max_pending();
        if self.pending.len() < max_pending {
            self.pending.push_back(item);
            return BackpressureOutcome::Enqueued;
        }

        match self.policy {
            BackpressurePolicy::Block { .. } => BackpressureOutcome::WouldBlock(item),
            BackpressurePolicy::FailFast { .. } => BackpressureOutcome::Rejected(item),
            BackpressurePolicy::DropNewest { .. } => {
                self.dropped += 1;
                BackpressureOutcome::DroppedNewest(item)
            }
            BackpressurePolicy::DropOldest { .. } => {
                if let Some(dropped) = self.pending.pop_front() {
                    self.pending.push_back(item);
                    self.dropped += 1;
                    BackpressureOutcome::DroppedOldest(dropped)
                } else {
                    self.dropped += 1;
                    BackpressureOutcome::DroppedNewest(item)
                }
            }
            BackpressurePolicy::Coalesce { .. } => {
                if let Some(coalesced) = self.pending.pop_back() {
                    self.pending.push_back(item);
                    self.coalesced += 1;
                    BackpressureOutcome::Coalesced(coalesced)
                } else {
                    self.coalesced += 1;
                    BackpressureOutcome::Coalesced(item)
                }
            }
            BackpressurePolicy::Disconnect { .. } => BackpressureOutcome::Disconnect(item),
        }
    }
}

#[cfg(test)]
mod tests {
    use lattice_core::direct_link::options::CoalesceKey;

    use crate::backpressure::*;

    #[test]
    fn block_policy_reports_would_block_without_mutating_queue() {
        let mut queue = BackpressureQueue::new(BackpressurePolicy::Block { max_pending: 1 });

        assert_eq!(queue.try_enqueue(1), BackpressureOutcome::Enqueued);
        assert_eq!(queue.try_enqueue(2), BackpressureOutcome::WouldBlock(2));

        assert_eq!(queue.len(), 1);
        assert_eq!(queue.pop_front(), Some(1));
    }

    #[test]
    fn fail_fast_rejects_new_message_without_mutating_queue() {
        let mut queue = BackpressureQueue::new(BackpressurePolicy::FailFast { max_pending: 1 });

        assert_eq!(queue.try_enqueue(1), BackpressureOutcome::Enqueued);
        assert_eq!(queue.try_enqueue(2), BackpressureOutcome::Rejected(2));

        assert_eq!(queue.pop_front(), Some(1));
    }

    #[test]
    fn drop_newest_discards_new_message_and_records_drop() {
        let mut queue = BackpressureQueue::new(BackpressurePolicy::DropNewest { max_pending: 1 });

        assert_eq!(queue.try_enqueue(1), BackpressureOutcome::Enqueued);
        assert_eq!(queue.try_enqueue(2), BackpressureOutcome::DroppedNewest(2));

        assert_eq!(queue.pop_front(), Some(1));
        assert_eq!(queue.snapshot().dropped, 1);
    }

    #[test]
    fn drop_oldest_replaces_oldest_pending_message_and_records_drop() {
        let mut queue = BackpressureQueue::new(BackpressurePolicy::DropOldest { max_pending: 1 });

        assert_eq!(queue.try_enqueue(1), BackpressureOutcome::Enqueued);
        assert_eq!(queue.try_enqueue(2), BackpressureOutcome::DroppedOldest(1));

        assert_eq!(queue.pop_front(), Some(2));
        assert_eq!(queue.snapshot().dropped, 1);
    }

    #[test]
    fn coalesce_replaces_latest_pending_message_and_records_coalesce() {
        let mut queue = BackpressureQueue::new(BackpressurePolicy::Coalesce {
            max_pending: 1,
            key: CoalesceKey::new("position"),
        });

        assert_eq!(queue.try_enqueue(1), BackpressureOutcome::Enqueued);
        assert_eq!(queue.try_enqueue(2), BackpressureOutcome::Coalesced(1));

        assert_eq!(queue.pop_front(), Some(2));
        assert_eq!(queue.snapshot().coalesced, 1);
    }

    #[test]
    fn disconnect_policy_requests_disconnect_without_mutating_queue() {
        let mut queue = BackpressureQueue::new(BackpressurePolicy::Disconnect { max_pending: 1 });

        assert_eq!(queue.try_enqueue(1), BackpressureOutcome::Enqueued);
        assert_eq!(queue.try_enqueue(2), BackpressureOutcome::Disconnect(2));

        assert_eq!(queue.pop_front(), Some(1));
    }
}
