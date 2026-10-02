//! Bidirectional I/O for one registered association lane.
//!
//! A lane owns its socket and borrows its queue receiver for the duration of [`BidirectionalLane::run`].
//! Frames are decoded and dispatched directly, without entering the connection supervisor's
//! mailbox. Replies complete pending asks through the shared messaging layer. The caller recovers
//! the receiver after normal completion and decides whether to sleep, retry or retire the group.
//!
//! # Runtime responsibilities
//!
//! The runtime selects between reads, queued writes, completed inbound asks, control application
//! results, shutdown and timers. Control has heartbeats and a separate ordered application worker.
//! Data lanes have idle timers; Interactive cannot sleep while relevant asks remain unfinished.
//! Only normal completion of the run future executes its detach epilogue. Cancellation is handled
//! by endpoint/supervisor lifetime cleanup, which invalidates registration and releases resources.

#![deny(missing_docs)]

use std::{sync::Arc, time::Duration};

use thiserror::Error;
use tokio::{
    sync::{mpsc, watch},
    task::JoinError,
};

use crate::{
    association::{Association, AssociationError, LaneKind},
    config::{ABSOLUTE_MAX_READY_READ_BATCH_FRAMES, ABSOLUTE_MAX_READY_WRITE_BATCH_FRAMES},
    control::{ControlDispatch, ControlDispatchError, ReliableControlError},
    messaging::{
        error::RemoteMessageError, inbound::InboundDispatch, outbound::OutboundMessaging,
        target_cache::ExactTargetCache, target_dictionary::ExactTargetDictionary,
    },
    transport::RemotingIo,
    wire::{Frame, FrameKind, WireError},
};

mod ask;
mod control;
mod inbound;
mod outbound;
mod runtime;

use runtime::run_bidirectional_lane_inner;

/// Limits and timers for one socket runtime, normally derived from endpoint configuration.
///
/// Validated when [`BidirectionalLane::run`] starts, rather than during construction.
#[derive(Debug, Clone, Copy)]
pub struct BidirectionalLaneConfig {
    /// Maximum encoded frame body size, including the header, in bytes.
    pub maximum_frame_size: usize,
    /// Maximum unfinished inbound asks admitted on this lane.
    pub maximum_concurrent_inbound_asks: usize,
    /// Interval between Control heartbeats; unused for data-lane liveness.
    pub heartbeat_interval: Duration,
    /// Number of heartbeat intervals without a received frame before Control fails.
    pub heartbeat_miss_limit: u32,
    /// Maximum retry age after the first transient control application failure.
    pub control_apply_retry_timeout: Duration,
    /// Time without data activity before a data socket sleeps, subject to pending asks.
    pub idle_data_connection_timeout: Duration,
    /// Maximum cached inbound exact-target resolutions on this socket.
    pub maximum_cached_exact_targets: usize,
    /// Capacity of the socket reader's read-ahead buffer, in bytes.
    pub socket_read_ahead_bytes: usize,
    /// Maximum ready queued frames gathered in one write batch.
    pub maximum_ready_write_batch_frames: usize,
    /// Maximum already buffered frames processed in one read batch.
    pub maximum_ready_read_batch_frames: usize,
    /// Byte limit used to coalesce a write batch.
    pub maximum_coalesced_write_batch_bytes: usize,
    /// Capacity of the ordered Control application worker's queue.
    pub maximum_pending_control_applies: usize,
}

impl BidirectionalLaneConfig {
    fn validate(self) -> Result<Self, LaneError> {
        if self.maximum_frame_size < 8
            || self.maximum_concurrent_inbound_asks == 0
            || self.maximum_cached_exact_targets == 0
            || self.socket_read_ahead_bytes == 0
            || self.maximum_ready_write_batch_frames == 0
            || self.maximum_ready_write_batch_frames > ABSOLUTE_MAX_READY_WRITE_BATCH_FRAMES
            || self.maximum_ready_read_batch_frames == 0
            || self.maximum_ready_read_batch_frames > ABSOLUTE_MAX_READY_READ_BATCH_FRAMES
            || self.maximum_coalesced_write_batch_bytes == 0
            || self.maximum_pending_control_applies == 0
        {
            return Err(LaneError::InvalidLimit);
        }
        if self.heartbeat_interval.is_zero()
            || self.heartbeat_miss_limit == 0
            || self.control_apply_retry_timeout.is_zero()
            || self.idle_data_connection_timeout.is_zero()
        {
            return Err(LaneError::InvalidHeartbeat);
        }
        Ok(self)
    }
}

/// Shared message and control dispatch services used directly by socket runtimes.
#[derive(Clone)]
pub struct LaneServices {
    messaging: Arc<OutboundMessaging>,
    dispatch: Arc<dyn InboundDispatch>,
    control_dispatch: Arc<dyn ControlDispatch>,
}

impl LaneServices {
    /// Combines outbound ask tracking, inbound business dispatch and inbound control dispatch.
    pub fn new(
        messaging: Arc<OutboundMessaging>,
        dispatch: Arc<dyn InboundDispatch>,
        control_dispatch: Arc<dyn ControlDispatch>,
    ) -> Self {
        Self {
            messaging,
            dispatch,
            control_dispatch,
        }
    }
}

/// Runs one negotiated socket for a registered lane and exact connection nonce.
///
/// Construction does not register the connection. The caller must register it and supply the
/// corresponding receiver before running it; endpoint supervisors perform that ownership transfer.
pub struct BidirectionalLane {
    association: Arc<Association>,
    lane: LaneKind,
    connection_nonce: u128,
    services: LaneServices,
    config: BidirectionalLaneConfig,
}

impl BidirectionalLane {
    /// Describes a socket runtime without registering a lane or starting I/O.
    pub fn new(
        association: Arc<Association>,
        lane: LaneKind,
        connection_nonce: u128,
        services: LaneServices,
        config: BidirectionalLaneConfig,
    ) -> Self {
        Self {
            association,
            lane,
            connection_nonce,
            services,
            config,
        }
    }

    /// Drives socket reads, writes and dispatch until shutdown, idle exit or failure.
    ///
    /// Consumes the stream and returns the receiver to its caller by ending the borrow. On
    /// completion, detaches only the matching connection nonce. Errors and remote closure on
    /// Control or Interactive also fail affected outbound asks; Bulk exit does not do so.
    ///
    /// Dropping this future releases its socket and runtime state, but skips the normal detach
    /// epilogue. The cancelling owner must invalidate registration and recover or retire receiver
    /// ownership. Endpoint supervision retires the generation if its I/O task cannot return a lease.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid runtime settings, malformed or misplaced frames, socket I/O,
    /// Control heartbeat/write timeout, or failed message/control dispatch.
    pub async fn run<S>(
        self,
        receiver: &mut mpsc::Receiver<Frame>,
        stream: S,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<LaneExit, LaneError>
    where
        S: RemotingIo,
    {
        let mut target_cache = ExactTargetCache::new(self.config.maximum_cached_exact_targets);
        let mut target_dictionary = ExactTargetDictionary::new();
        // Keep the large lane state behind one allocation boundary so task cancellation cannot
        // recursively drop the complete endpoint -> connection -> lane future tree on one worker
        // stack.
        let result = Box::pin(run_bidirectional_lane_inner(
            &self,
            receiver,
            stream,
            shutdown,
            &mut target_cache,
            &mut target_dictionary,
        ))
        .await;
        let (hits, misses) = target_cache.take_metrics();
        self.association.record_exact_target_cache(hits, misses);
        self.association.detach(self.lane, self.connection_nonce);
        if self.lane.fails_pending_asks() && matches!(result, Err(_) | Ok(LaneExit::RemoteClose)) {
            self.services
                .messaging
                .fail_association(self.association.id());
        }
        result
    }
}

/// Normal reason a socket runtime stopped; supervision decides the subsequent lane phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneExit {
    /// The endpoint shutdown watch was closed or set to stop.
    Shutdown,
    /// The outbound queue has no remaining senders.
    QueueClosed,
    /// A peer Close frame or endpoint-level disconnect ended this socket.
    RemoteClose,
    /// A data socket reached its idle limit; the association may remain active.
    Idle,
}

/// Failure of a lane's I/O, dispatch, control worker or runtime configuration.
#[derive(Debug, Error)]
pub enum LaneError {
    /// A required heartbeat, retry or idle timer is zero.
    #[error("lane heartbeat interval must be nonzero")]
    InvalidHeartbeat,
    /// A resource limit is zero, unsupported, or too small for a frame header.
    #[error("lane runtime limit must be nonzero and frame size must include the header")]
    InvalidLimit,
    /// Control received no frame within its heartbeat liveness window.
    #[error("control lane missed its bounded heartbeat window")]
    HeartbeatTimeout,
    /// A Control socket write exceeded its bounded liveness window.
    #[error("control lane socket write exceeded its bounded window")]
    WriteTimeout,
    /// The ordered Control worker ended without a requested shutdown.
    #[error("control apply worker stopped unexpectedly")]
    ControlWorkerClosed,
    /// The ordered Control worker has no available queue slot.
    #[error("control apply queue is full")]
    ControlApplyBackpressure,
    /// A frame submitted to the Control worker is not an application operation.
    #[error("control apply worker received an unexpected frame")]
    UnexpectedControlWork,
    /// The frame kind is not permitted on the receiving lane.
    #[error("lane received frame kind {kind:?} on {lane:?}")]
    UnexpectedFrame {
        /// Lane on which the frame arrived.
        lane: LaneKind,
        /// Unexpected frame kind.
        kind: FrameKind,
    },
    /// A wake frame names an unsupported data lane or has malformed bytes.
    #[error("lane wake frame has an invalid payload")]
    InvalidLaneWake,
    /// An inbound ask task panicked or was cancelled.
    #[error("inbound ask task failed")]
    Join(#[source] JoinError),
    /// Business dispatch could not process an inbound message.
    #[error("inbound actor dispatch failed")]
    Dispatch(#[from] RemoteMessageError),
    /// Control application failed beyond the worker's accepted recovery policy.
    #[error("reliable control dispatch failed")]
    ControlDispatch(#[from] ControlDispatchError),
    /// Reliable control sequencing or bounds rejected an inbound operation.
    #[error("reliable control state rejected a frame")]
    ReliableControl(#[from] ReliableControlError),
    /// Association control bookkeeping rejected an operation.
    #[error("association rejected a reliable control acknowledgement")]
    Association(#[from] AssociationError),
    /// Socket I/O or frame encoding/decoding failed.
    #[error("lane socket failed")]
    Wire(#[source] WireError),
}

impl From<WireError> for LaneError {
    fn from(error: WireError) -> Self {
        Self::Wire(error)
    }
}

#[cfg(test)]
mod tests;
