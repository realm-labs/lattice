//! One owner and one outstanding operation for each lane.
//!
//! ```text
//! dialer:   WaitingInbound / Sleeping -> Dialing -> Running
//!           Dialing failure / Running failure -> Backoff -> Dialing
//!           Running idle -> Sleeping (unless a wake or queued frame requires retry)
//! accepter: WaitingInbound -> Running -> WaitingInbound
//! all:      any phase -> Closed on association retirement
//! ```
//!
//! Tokens identify operations, not node incarnations. Every scheduled retry and dial advances
//! the token so a completion from an older operation cannot overwrite the current phase.

use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, mpsc};

use crate::{association::LaneKind, wire::Frame};

/// Actor-owned socket phase, independent of the published association admission state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LanePhase {
    /// Parked receiver awaiting an incoming candidate or initial outgoing request.
    WaitingInbound,
    /// Outgoing data lane has released its socket and permit until another wake.
    Sleeping,
    /// One outbound setup task owns the reservation for this attempt token.
    Dialing(u64),
    /// One socket task owns the receiver lease for these connection identifiers.
    Running {
        /// Local operation token assigned during dialing or incoming adoption.
        attempt: u64,
        /// Wire nonce used for nonce-qualified registration and detach.
        nonce: u128,
    },
    /// A reliable timer will deliver a retry event with this token.
    Backoff(u64),
    /// Retirement has invalidated further connection work.
    Closed,
}

/// Resources and retry bookkeeping owned exclusively by the management actor.
pub(super) struct LaneState {
    /// Lane role; its vector position follows `lane_index`.
    pub(super) kind: LaneKind,
    /// Current operation and the tokens used to validate completion events.
    pub(super) phase: LanePhase,
    /// Present while parked or dialing; absent while a socket lease owns the consumer.
    pub(super) receiver: Option<mpsc::Receiver<Frame>>,
    /// Parked reconnect reservation; dialing and running tasks own their permits elsewhere.
    pub(super) permit: Option<OwnedSemaphorePermit>,
    /// Records whether a failed dial should preserve a reservation inherited from a live socket.
    pub(super) keep_permit_on_failure: bool,
    /// Retains a wake observed after socket detach but before processing its completion event.
    pub(super) wake_requested: bool,
    /// Delay for the next retry, doubled on failure and reset after socket completion/adoption.
    pub(super) backoff: Duration,
    /// Monotonic local allocator for dial and retry tokens.
    next_attempt: u64,
}

impl LaneState {
    /// Parks a receiver with no socket or permit, ready for initial connection establishment.
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

    /// Allocates a fresh token before changing the outstanding operation.
    pub(super) fn attempt(&mut self) -> u64 {
        self.next_attempt += 1;
        self.next_attempt
    }

    /// Checks whether a parked receiver is available without an overlapping lane operation.
    pub(super) fn can_start(&self) -> bool {
        matches!(self.phase, LanePhase::WaitingInbound | LanePhase::Sleeping)
            && self.receiver.is_some()
    }

    /// Validates a dial result before adopting or publishing anything it contains.
    pub(super) fn is_current_dial(&self, attempt: u64) -> bool {
        self.phase == LanePhase::Dialing(attempt)
    }

    /// Validates a socket completion against both local operation and registered connection.
    pub(super) fn is_current_connection(&self, attempt: u64, nonce: u128) -> bool {
        self.phase == LanePhase::Running { attempt, nonce }
    }
}

/// A receiver has exactly one owner: the supervisor while parked, or this lease while running.
pub(super) struct LaneLease {
    /// Exclusive outbound queue consumer, returned to the actor on normal task completion.
    pub(super) receiver: mpsc::Receiver<Frame>,
    /// Connection capacity retained until the actor chooses sleep, retry or retirement.
    pub(super) permit: OwnedSemaphorePermit,
}

/// Maps lane roles to slots in the same order as endpoint lane and receiver enumeration.
pub(super) fn lane_index(lane: LaneKind) -> usize {
    match lane {
        LaneKind::Control => 0,
        LaneKind::Interactive => 1,
        LaneKind::Bulk(index) => usize::from(index) + 2,
    }
}
