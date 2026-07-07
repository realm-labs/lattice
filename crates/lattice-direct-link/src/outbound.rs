use lattice_core::direct_link::errors::LinkSendError;
use lattice_core::direct_link::options::LinkCloseReason;
use lattice_core::direct_link::runtime::OutboundDirectLinkMessage;

use crate::backpressure::{BackpressureOutcome, BackpressureQueue, BackpressureSnapshot};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundQueueEvent {
    Enqueued,
    DroppedNewest(OutboundDirectLinkMessage),
    DroppedOldest(OutboundDirectLinkMessage),
    Coalesced(OutboundDirectLinkMessage),
}

#[derive(Debug, Clone)]
pub struct OutboundDirectLinkQueue {
    queue: BackpressureQueue<OutboundDirectLinkMessage>,
    closed: Option<LinkCloseReason>,
}

impl OutboundDirectLinkQueue {
    pub fn new(queue: BackpressureQueue<OutboundDirectLinkMessage>) -> Self {
        Self {
            queue,
            closed: None,
        }
    }

    pub fn try_enqueue(
        &mut self,
        message: OutboundDirectLinkMessage,
    ) -> Result<OutboundQueueEvent, LinkSendError> {
        if let Some(reason) = self.closed.clone() {
            return Err(LinkSendError::Closed { reason });
        }

        match self.queue.try_enqueue(message) {
            BackpressureOutcome::Enqueued => Ok(OutboundQueueEvent::Enqueued),
            BackpressureOutcome::WouldBlock(_) | BackpressureOutcome::Rejected(_) => {
                Err(LinkSendError::BackpressureFull)
            }
            BackpressureOutcome::DroppedNewest(message) => {
                Ok(OutboundQueueEvent::DroppedNewest(message))
            }
            BackpressureOutcome::DroppedOldest(message) => {
                Ok(OutboundQueueEvent::DroppedOldest(message))
            }
            BackpressureOutcome::Coalesced(message) => Ok(OutboundQueueEvent::Coalesced(message)),
            BackpressureOutcome::Disconnect(_) => {
                let reason = LinkCloseReason::BackpressureExceeded;
                self.closed = Some(reason.clone());
                Err(LinkSendError::Closed { reason })
            }
        }
    }

    pub fn pop_front(&mut self) -> Option<OutboundDirectLinkMessage> {
        self.queue.pop_front()
    }

    pub fn snapshot(&self) -> BackpressureSnapshot {
        self.queue.snapshot()
    }

    pub fn is_closed(&self) -> bool {
        self.closed.is_some()
    }
}

#[cfg(test)]
mod tests {
    use lattice_core::direct_link::ids::{DirectLinkMessageId, LinkId};
    use lattice_core::direct_link::messages::LinkMessageFlags;
    use lattice_core::direct_link::options::{BackpressurePolicy, CoalesceKey, LinkDirection};

    use crate::outbound::*;

    #[test]
    fn block_and_fail_fast_map_full_queue_to_backpressure_error() {
        for policy in [
            BackpressurePolicy::Block { max_pending: 1 },
            BackpressurePolicy::FailFast { max_pending: 1 },
        ] {
            let mut queue = OutboundDirectLinkQueue::new(BackpressureQueue::new(policy));
            assert_eq!(
                queue.try_enqueue(message(1)),
                Ok(OutboundQueueEvent::Enqueued)
            );
            assert!(matches!(
                queue.try_enqueue(message(2)),
                Err(LinkSendError::BackpressureFull)
            ));
            assert_eq!(queue.pop_front(), Some(message(1)));
        }
    }

    #[test]
    fn drop_newest_reports_dropped_new_message() {
        let mut queue =
            OutboundDirectLinkQueue::new(BackpressureQueue::new(BackpressurePolicy::DropNewest {
                max_pending: 1,
            }));

        assert_eq!(
            queue.try_enqueue(message(1)),
            Ok(OutboundQueueEvent::Enqueued)
        );
        assert_eq!(
            queue.try_enqueue(message(2)),
            Ok(OutboundQueueEvent::DroppedNewest(message(2)))
        );

        assert_eq!(queue.pop_front(), Some(message(1)));
        assert_eq!(queue.snapshot().dropped, 1);
    }

    #[test]
    fn drop_oldest_reports_dropped_old_message_and_keeps_new_message() {
        let mut queue =
            OutboundDirectLinkQueue::new(BackpressureQueue::new(BackpressurePolicy::DropOldest {
                max_pending: 1,
            }));

        assert_eq!(
            queue.try_enqueue(message(1)),
            Ok(OutboundQueueEvent::Enqueued)
        );
        assert_eq!(
            queue.try_enqueue(message(2)),
            Ok(OutboundQueueEvent::DroppedOldest(message(1)))
        );

        assert_eq!(queue.pop_front(), Some(message(2)));
        assert_eq!(queue.snapshot().dropped, 1);
    }

    #[test]
    fn coalesce_reports_replaced_message_and_keeps_new_message() {
        let mut queue =
            OutboundDirectLinkQueue::new(BackpressureQueue::new(BackpressurePolicy::Coalesce {
                max_pending: 1,
                key: CoalesceKey::new("position"),
            }));

        assert_eq!(
            queue.try_enqueue(message(1)),
            Ok(OutboundQueueEvent::Enqueued)
        );
        assert_eq!(
            queue.try_enqueue(message(2)),
            Ok(OutboundQueueEvent::Coalesced(message(1)))
        );

        assert_eq!(queue.pop_front(), Some(message(2)));
        assert_eq!(queue.snapshot().coalesced, 1);
    }

    #[test]
    fn disconnect_closes_queue_with_backpressure_reason() {
        let mut queue =
            OutboundDirectLinkQueue::new(BackpressureQueue::new(BackpressurePolicy::Disconnect {
                max_pending: 1,
            }));

        assert_eq!(
            queue.try_enqueue(message(1)),
            Ok(OutboundQueueEvent::Enqueued)
        );
        assert!(matches!(
            queue.try_enqueue(message(2)),
            Err(LinkSendError::Closed {
                reason: LinkCloseReason::BackpressureExceeded
            })
        ));
        assert!(queue.is_closed());
        assert!(matches!(
            queue.try_enqueue(message(3)),
            Err(LinkSendError::Closed {
                reason: LinkCloseReason::BackpressureExceeded
            })
        ));
    }

    fn message(sequence: u64) -> OutboundDirectLinkMessage {
        OutboundDirectLinkMessage {
            link_id: LinkId::new("link-outbound"),
            direction: LinkDirection::SourceToTarget,
            message_id: DirectLinkMessageId(sequence),
            proto_full_name: "test.Payload",
            metadata: Vec::new(),
            payload: vec![sequence as u8],
            flags: LinkMessageFlags::EMPTY,
        }
    }
}
