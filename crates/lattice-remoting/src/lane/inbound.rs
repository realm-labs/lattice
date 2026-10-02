//! Frame classification, decoding and direct inbound dispatch.
//!
//! Bulk carries one-way Tell traffic; Interactive carries asks, replies and failures. Control
//! carries heartbeats, acknowledgements, wake requests and application commands. Wrong-lane
//! frames fail the socket. Malformed Bulk tells are dropped, while request/reply decoding errors
//! propagate because their completion obligations cannot be silently ignored.
//!
//! Dispatch returns an action to the event loop instead of borrowing its writer or pending-ask
//! collection. Outbound replies complete pending asks immediately through shared messaging state.
//! Inbound asks become separately polled work; control application goes to its ordered worker.

use bytes::Bytes;

use super::{
    BidirectionalLaneConfig, LaneError, LaneServices, ask::InboundAskWork,
    control::decode_lane_wake,
};
use crate::{
    association::{Association, LaneKind},
    control::decode_control_ack,
    messaging::{
        codec::{
            decode_ask_cached, decode_entity_ask, decode_entity_tell, decode_failure, decode_reply,
            decode_singleton_ask, decode_singleton_tell, decode_tell_cached, failure_frame,
        },
        error::{AskError, RemoteFailureCode},
        inbound::dispatch_tell,
        target::RemoteFailure,
        target_cache::ExactTargetCache,
        target_dictionary::ExactTargetDictionary,
    },
    wire::{Frame, FrameKind},
};

/// Follow-up work performed by the socket loop after classification and direct dispatch.
pub(super) enum InboundAction {
    /// The frame has been consumed and requires no socket-loop work.
    Continue,
    /// Poll a newly admitted inbound ask alongside socket I/O.
    EnqueueAsk(InboundAskWork),
    /// Write a protocol response, such as a heartbeat acknowledgement or ask rejection.
    Write(Frame),
    /// Submit application work without blocking the read loop on control retries.
    ApplyControl(Frame),
    /// End this socket in response to a peer Close frame.
    Close,
}

/// Decodes and dispatches frames without owning socket writes or the event loop.
pub(super) struct InboundLane<'a> {
    association: &'a Association,
    lane: LaneKind,
    services: &'a LaneServices,
    maximum_concurrent_asks: usize,
    /// Cache of resolved local actor targets; entries are scoped to this socket runtime.
    target_cache: &'a mut ExactTargetCache,
    /// Peer-defined compact target IDs; socket replacement creates a fresh dictionary.
    target_dictionary: &'a mut ExactTargetDictionary,
}

impl<'a> InboundLane<'a> {
    pub(super) fn new(
        association: &'a Association,
        lane: LaneKind,
        services: &'a LaneServices,
        config: BidirectionalLaneConfig,
        target_cache: &'a mut ExactTargetCache,
        target_dictionary: &'a mut ExactTargetDictionary,
    ) -> Self {
        Self {
            association,
            lane,
            services,
            maximum_concurrent_asks: config.maximum_concurrent_inbound_asks,
            target_cache,
            target_dictionary,
        }
    }

    /// Consumes a frame or returns follow-up work, checking its kind against this lane's role.
    ///
    /// `pending_asks` is supplied by the loop so decoding can reject capacity overflow with a
    /// correlated failure frame before adding another request future.
    pub(super) async fn dispatch(
        &mut self,
        frame: Frame,
        pending_asks: usize,
    ) -> Result<InboundAction, LaneError> {
        let dispatch = self.services.dispatch.as_ref();
        match frame.kind {
            FrameKind::Tell if matches!(self.lane, LaneKind::Bulk(_)) => {
                match decode_tell_cached(&frame, self.target_cache, self.target_dictionary) {
                    Ok(tell) => {
                        let _ = dispatch_tell(dispatch, tell).await;
                    }
                    Err(_) => self.association.record_dropped_inbound_frame(),
                }
            }
            FrameKind::EntityTell if matches!(self.lane, LaneKind::Bulk(_)) => {
                match decode_entity_tell(&frame) {
                    Ok(tell) => {
                        let _ = dispatch
                            .tell_entity(tell.target, tell.message_id, tell.payload)
                            .await;
                    }
                    Err(_) => self.association.record_dropped_inbound_frame(),
                }
            }
            FrameKind::SingletonTell if matches!(self.lane, LaneKind::Bulk(_)) => {
                match decode_singleton_tell(&frame) {
                    Ok(tell) => {
                        let _ = dispatch
                            .tell_singleton(tell.target, tell.message_id, tell.payload)
                            .await;
                    }
                    Err(_) => self.association.record_dropped_inbound_frame(),
                }
            }
            FrameKind::Ask if self.lane == LaneKind::Interactive => {
                let ask = decode_ask_cached(&frame, self.target_cache)?;
                return Ok(self.admit_ask(InboundAskWork::Exact(ask), pending_asks));
            }
            FrameKind::EntityAsk if self.lane == LaneKind::Interactive => {
                let ask = decode_entity_ask(&frame)?;
                return Ok(self.admit_ask(InboundAskWork::Entity(ask), pending_asks));
            }
            FrameKind::SingletonAsk if self.lane == LaneKind::Interactive => {
                let ask = decode_singleton_ask(&frame)?;
                return Ok(self.admit_ask(InboundAskWork::Singleton(ask), pending_asks));
            }
            FrameKind::Reply if self.lane == LaneKind::Interactive => {
                let (correlation, payload) = decode_reply(&frame)?;
                if !self.services.messaging.complete_reply(correlation, payload) {
                    self.association.record_discarded_reply();
                }
            }
            FrameKind::Failure if self.lane == LaneKind::Interactive => {
                let failure = decode_failure(&frame)?;
                if !self
                    .services
                    .messaging
                    .complete_failure(failure.correlation_id, AskError::Remote(failure.code))
                {
                    self.association.record_discarded_reply();
                }
            }
            FrameKind::Heartbeat if self.lane == LaneKind::Control => {
                return Ok(InboundAction::Write(Frame::new(
                    FrameKind::HeartbeatAck,
                    Bytes::new(),
                )));
            }
            FrameKind::HeartbeatAck if self.lane == LaneKind::Control => {}
            FrameKind::ControlEnvelope | FrameKind::CoordinatorEvent
                if self.lane == LaneKind::Control =>
            {
                return Ok(InboundAction::ApplyControl(frame));
            }
            FrameKind::ControlAck if self.lane == LaneKind::Control => {
                self.association
                    .acknowledge_control(decode_control_ack(&frame)?)?;
            }
            FrameKind::Backpressure => {}
            FrameKind::LaneWake if self.lane == LaneKind::Control => {
                let lane = decode_lane_wake(&frame)?;
                self.association.notify_lane_wake(lane)?;
            }
            FrameKind::Close => return Ok(InboundAction::Close),
            kind => {
                return Err(LaneError::UnexpectedFrame {
                    lane: self.lane,
                    kind,
                });
            }
        }
        Ok(InboundAction::Continue)
    }

    /// Rejects a request at the inbound concurrency limit without invoking actor dispatch.
    fn admit_ask(&self, ask: InboundAskWork, pending_asks: usize) -> InboundAction {
        if pending_asks == self.maximum_concurrent_asks {
            InboundAction::Write(failure_frame(&RemoteFailure {
                correlation_id: ask.correlation_id(),
                code: RemoteFailureCode::MailboxFull,
                safe_detail: None,
            }))
        } else {
            InboundAction::EnqueueAsk(ask)
        }
    }

    pub(super) fn report_cache_metrics(&mut self) {
        if let Some((hits, misses)) = self.target_cache.take_metrics_if_ready() {
            self.association.record_exact_target_cache(hits, misses);
        }
    }
}
