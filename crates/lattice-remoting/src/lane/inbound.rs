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

pub(super) enum InboundAction {
    Continue,
    EnqueueAsk(InboundAskWork),
    Write(Frame),
    ApplyControl(Frame),
    Close,
}

/// Decodes and dispatches frames without owning socket writes or the event loop.
pub(super) struct InboundLane<'a> {
    association: &'a Association,
    lane: LaneKind,
    services: &'a LaneServices,
    maximum_concurrent_asks: usize,
    target_cache: &'a mut ExactTargetCache,
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
