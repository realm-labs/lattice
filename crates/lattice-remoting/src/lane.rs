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

#[derive(Debug, Clone, Copy)]
pub struct BidirectionalLaneConfig {
    pub maximum_frame_size: usize,
    pub maximum_concurrent_inbound_asks: usize,
    pub heartbeat_interval: Duration,
    pub heartbeat_miss_limit: u32,
    pub control_apply_retry_timeout: Duration,
    pub idle_data_connection_timeout: Duration,
    pub maximum_cached_exact_targets: usize,
    pub socket_read_ahead_bytes: usize,
    pub maximum_ready_write_batch_frames: usize,
    pub maximum_ready_read_batch_frames: usize,
    pub maximum_coalesced_write_batch_bytes: usize,
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

#[derive(Clone)]
pub struct LaneServices {
    messaging: Arc<OutboundMessaging>,
    dispatch: Arc<dyn InboundDispatch>,
    control_dispatch: Arc<dyn ControlDispatch>,
}

impl LaneServices {
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

pub struct BidirectionalLane {
    association: Arc<Association>,
    lane: LaneKind,
    connection_nonce: u128,
    services: LaneServices,
    config: BidirectionalLaneConfig,
}

impl BidirectionalLane {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneExit {
    Shutdown,
    QueueClosed,
    RemoteClose,
    Idle,
}

#[derive(Debug, Error)]
pub enum LaneError {
    #[error("lane heartbeat interval must be nonzero")]
    InvalidHeartbeat,
    #[error("lane runtime limit must be nonzero and frame size must include the header")]
    InvalidLimit,
    #[error("control lane missed its bounded heartbeat window")]
    HeartbeatTimeout,
    #[error("control lane socket write exceeded its bounded window")]
    WriteTimeout,
    #[error("control apply worker stopped unexpectedly")]
    ControlWorkerClosed,
    #[error("control apply queue is full")]
    ControlApplyBackpressure,
    #[error("control apply worker received an unexpected frame")]
    UnexpectedControlWork,
    #[error("lane received frame kind {kind:?} on {lane:?}")]
    UnexpectedFrame { lane: LaneKind, kind: FrameKind },
    #[error("lane wake frame has an invalid payload")]
    InvalidLaneWake,
    #[error("inbound ask task failed")]
    Join(#[source] JoinError),
    #[error("inbound actor dispatch failed")]
    Dispatch(#[from] RemoteMessageError),
    #[error("reliable control dispatch failed")]
    ControlDispatch(#[from] ControlDispatchError),
    #[error("reliable control state rejected a frame")]
    ReliableControl(#[from] ReliableControlError),
    #[error("association rejected a reliable control acknowledgement")]
    Association(#[from] AssociationError),
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
