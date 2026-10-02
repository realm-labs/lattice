//! One owner and one outstanding operation for each lane.

use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, mpsc};

use crate::{association::LaneKind, wire::Frame};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LanePhase {
    WaitingInbound,
    Sleeping,
    Dialing(u64),
    Running { attempt: u64, nonce: u128 },
    Backoff(u64),
    Closed,
}

pub(super) struct LaneState {
    pub(super) kind: LaneKind,
    pub(super) phase: LanePhase,
    pub(super) receiver: Option<mpsc::Receiver<Frame>>,
    pub(super) permit: Option<OwnedSemaphorePermit>,
    pub(super) keep_permit_on_failure: bool,
    // A wake can arrive after the socket detached but before its completion event.
    pub(super) wake_requested: bool,
    pub(super) backoff: Duration,
    next_attempt: u64,
}

impl LaneState {
    pub(super) fn new(kind: LaneKind, receiver: mpsc::Receiver<Frame>, backoff: Duration) -> Self {
        Self {
            kind,
            phase: LanePhase::WaitingInbound,
            receiver: Some(receiver),
            permit: None,
            keep_permit_on_failure: false,
            wake_requested: false,
            backoff,
            next_attempt: 0,
        }
    }

    pub(super) fn attempt(&mut self) -> u64 {
        self.next_attempt += 1;
        self.next_attempt
    }

    pub(super) fn can_start(&self) -> bool {
        matches!(self.phase, LanePhase::WaitingInbound | LanePhase::Sleeping)
            && self.receiver.is_some()
    }

    pub(super) fn is_current_dial(&self, attempt: u64) -> bool {
        self.phase == LanePhase::Dialing(attempt)
    }

    pub(super) fn is_current_connection(&self, attempt: u64, nonce: u128) -> bool {
        self.phase == LanePhase::Running { attempt, nonce }
    }
}

/// A receiver has exactly one owner: the supervisor while parked, or this lease while running.
pub(super) struct LaneLease {
    pub(super) receiver: mpsc::Receiver<Frame>,
    pub(super) permit: OwnedSemaphorePermit,
}

pub(super) fn lane_index(lane: LaneKind) -> usize {
    match lane {
        LaneKind::Control => 0,
        LaneKind::Interactive => 1,
        LaneKind::Bulk(index) => usize::from(index) + 2,
    }
}
