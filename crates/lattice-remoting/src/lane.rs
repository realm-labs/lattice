use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use lattice_core::failpoint::Failpoint;
use thiserror::Error;
#[cfg(test)]
use tokio::task::JoinSet;
use tokio::{
    sync::{mpsc, watch},
    task::JoinError,
    time::{Instant as TokioInstant, MissedTickBehavior},
};

use crate::{
    association::{Association, AssociationError, LaneKind},
    config::{ABSOLUTE_MAX_READY_READ_BATCH_FRAMES, ABSOLUTE_MAX_READY_WRITE_BATCH_FRAMES},
    control::{ControlDispatch, ControlDispatchError, ReliableControlError, decode_control_ack},
    messaging::{
        codec::{
            decode_ask_cached, decode_entity_ask, decode_entity_tell_cached, decode_failure,
            decode_reply, decode_singleton_ask, decode_singleton_tell_cached, decode_tell_cached,
            failure_frame,
        },
        error::{AskError, RemoteFailureCode, RemoteMessageError},
        inbound::{InboundDispatch, dispatch_tell},
        outbound::{OutboundMessaging, PreparedOutboundFrame},
        target::RemoteFailure,
        target_cache::ExactTargetCache,
        target_dictionary::ExactTargetDictionary,
    },
    transport::{FramedReader, FramedWriter, RemotingIo},
    wire::{Frame, FrameCodec, FrameKind, WireError},
};

mod ask;
mod control;

use ask::{InboundAskWork, dispatch_inbound_ask};
use control::{ControlWorkerGuard, apply_control_frame, decode_lane_wake};

#[derive(Debug, Clone, Copy)]
pub struct BidirectionalLaneConfig {
    pub maximum_frame_size: usize,
    pub maximum_concurrent_inbound_asks: usize,
    pub heartbeat_interval: Duration,
    pub heartbeat_miss_limit: u32,
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
        let result = run_bidirectional_lane_inner(
            &self,
            receiver,
            stream,
            shutdown,
            &mut target_cache,
            &mut target_dictionary,
        )
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

async fn run_bidirectional_lane_inner<S>(
    runtime: &BidirectionalLane,
    receiver: &mut mpsc::Receiver<Frame>,
    stream: S,
    shutdown: &mut watch::Receiver<bool>,
    target_cache: &mut ExactTargetCache,
    target_dictionary: &mut ExactTargetDictionary,
) -> Result<LaneExit, LaneError>
where
    S: RemotingIo,
{
    let association = runtime.association.as_ref();
    let lane = runtime.lane;
    let messaging = runtime.services.messaging.as_ref();
    let dispatch = runtime.services.dispatch.clone();
    let control_dispatch = runtime.services.control_dispatch.clone();
    let config = runtime.config.validate()?;
    if *shutdown.borrow() {
        return Ok(LaneExit::Shutdown);
    }
    let codec = FrameCodec::new(config.maximum_frame_size)?;
    let (read, write) = tokio::io::split(stream);
    let mut reader =
        FramedReader::new_with_read_ahead(read, codec.clone(), config.socket_read_ahead_bytes);
    let mut writer = FramedWriter::new_with_tuning(
        write,
        codec,
        config.maximum_ready_write_batch_frames,
        config.maximum_coalesced_write_batch_bytes,
    );
    let write_timeout = (lane == LaneKind::Control)
        .then(|| config.heartbeat_interval * config.heartbeat_miss_limit);
    let bulk_stripe = match lane {
        LaneKind::Bulk(index) => Some(usize::from(index)),
        _ => None,
    };
    let mut asks = FuturesUnordered::new();
    let mut heartbeat = tokio::time::interval(config.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_received = Instant::now();
    let mut outbound_candidates = Vec::with_capacity(config.maximum_ready_write_batch_frames);
    let mut outbound_batch = Vec::with_capacity(config.maximum_ready_write_batch_frames);
    let mut outbound_correlations = Vec::with_capacity(config.maximum_ready_write_batch_frames);
    let (control_apply_tx, mut control_apply_rx, _control_worker) = if lane == LaneKind::Control {
        let (commands, mut command_rx) =
            mpsc::channel::<Frame>(config.maximum_pending_control_applies);
        let (results, result_rx) = mpsc::channel(config.maximum_pending_control_applies);
        let association = runtime.association.clone();
        let control_dispatch = control_dispatch.clone();
        let worker = tokio::spawn(async move {
            while let Some(frame) = command_rx.recv().await {
                let mut retry_backoff = Duration::from_millis(25);
                let result = loop {
                    let result = apply_control_frame(
                        association.clone(),
                        control_dispatch.clone(),
                        frame.clone(),
                    )
                    .await;
                    if matches!(
                        result,
                        Err(LaneError::ControlDispatch(
                            ControlDispatchError::Unavailable
                        ))
                    ) {
                        // CoordinatorEvent is deliberately best-effort (for example, a claim
                        // grant is renewed by the next coordinator tick). A stale event can
                        // target a placement consumer that is paused until membership recovers.
                        // Retrying it here would head-of-line block the reliable membership
                        // snapshot queued behind it and make that recovery impossible.
                        if frame.kind == FrameKind::CoordinatorEvent {
                            tracing::debug!(
                                target: "lattice_remoting::control",
                                association_id = association.id().get(),
                                "dropping ephemeral coordinator event while its consumer is unavailable"
                            );
                            break Ok(None);
                        }
                        tokio::time::sleep(retry_backoff).await;
                        retry_backoff = retry_backoff.saturating_mul(2).min(Duration::from_secs(1));
                        continue;
                    }
                    break result;
                };
                let failed = result.is_err();
                if results.send(result).await.is_err() || failed {
                    break;
                }
            }
        });
        (
            Some(commands),
            Some(result_rx),
            Some(ControlWorkerGuard(worker)),
        )
    } else {
        (None, None, None)
    };
    let idle = tokio::time::sleep(config.idle_data_connection_timeout);
    tokio::pin!(idle);

    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    if lane == LaneKind::Control {
                        let _ = write_within(
                            write_timeout,
                            writer.write_frame(&Frame::new(FrameKind::Close, Bytes::new())),
                        )
                        .await;
                    }
                    write_within(write_timeout, writer.flush()).await?;
                    return Ok(LaneExit::Shutdown);
                }
            }
            completed = async {
                control_apply_rx
                    .as_mut()
                    .expect("control result branch requires a worker")
                    .recv()
                    .await
            }, if control_apply_rx.is_some() => {
                let Some(completed) = completed else {
                    return Err(LaneError::ControlWorkerClosed);
                };
                if let Some(frame) = completed? {
                    write_within(write_timeout, writer.write_frame(&frame)).await?;
                }
            }
            _ = heartbeat.tick(), if lane == LaneKind::Control => {
                if Instant::now().duration_since(last_received)
                    >= config.heartbeat_interval * config.heartbeat_miss_limit
                {
                    return Err(LaneError::HeartbeatTimeout);
                }
                write_within(
                    write_timeout,
                    writer.write_frame(&Frame::new(FrameKind::Heartbeat, Bytes::new())),
                )
                .await?;
            }
            completed = asks.next(), if !asks.is_empty() => {
                let Some(completed) = completed else {
                    continue;
                };
                outbound_batch.clear();
                outbound_batch.push(completed?);
                while outbound_batch.len() < config.maximum_ready_write_batch_frames {
                    let Some(completed) = asks.next().now_or_never().flatten() else {
                        break;
                    };
                    outbound_batch.push(completed?);
                }
                if outbound_batch.len() == 1 {
                    write_within(write_timeout, writer.write_frame(&outbound_batch[0])).await?;
                } else {
                    write_within(
                        write_timeout,
                        writer.write_frames_with_commit(&outbound_batch, |_| {}),
                    )
                    .await?;
                }
                idle.as_mut().reset(
                    TokioInstant::now() + config.idle_data_connection_timeout
                );
            }
            outbound = receiver.recv() => {
                let Some(frame) = outbound else {
                    return Ok(LaneExit::QueueClosed);
                };
                outbound_candidates.clear();
                outbound_candidates.push(frame);
                let batch_limit = if lane == LaneKind::Control {
                    1
                } else {
                    config.maximum_ready_write_batch_frames
                };
                while outbound_candidates.len() < batch_limit {
                    let Ok(frame) = receiver.try_recv() else {
                        break;
                    };
                    outbound_candidates.push(frame);
                }
                outbound_batch.clear();
                outbound_correlations.clear();
                let mut reserved_bytes = 0;
                for mut frame in outbound_candidates.drain(..) {
                    let frame_bytes = frame.payload_len();
                    if let Some(stripe) = bulk_stripe {
                        frame.expand_stale_compact_target(association.bulk_lane_epoch(stripe));
                    }
                    let Some(prepared) =
                        messaging.prepare_outbound_for_socket_write(&mut frame)
                    else {
                        association.release_queued_bytes(frame_bytes);
                        continue;
                    };
                    reserved_bytes += frame_bytes;
                    outbound_correlations.push(match prepared {
                        PreparedOutboundFrame::Other => None,
                        PreparedOutboundFrame::Ask(correlation) => Some(correlation),
                    });
                    outbound_batch.push(frame);
                }
                if outbound_batch.is_empty() {
                    continue;
                }
                if outbound_batch
                    .iter()
                    .any(|frame| frame.kind == FrameKind::ControlEnvelope)
                {
                    lattice_core::failpoint::hit(
                        Failpoint::ControlAfterOutboxBeforeSocketWrite,
                    );
                }
                let frame_count = outbound_batch.len();
                let result = if frame_count == 1 && !matches!(lane, LaneKind::Bulk(_)) {
                    let correlation = outbound_correlations[0];
                    write_within(
                        write_timeout,
                        writer.write_frame_with_commit_outcome(&outbound_batch[0], || {
                            if let Some(correlation) = correlation {
                                messaging.mark_socket_write_started(correlation);
                            }
                        }),
                    )
                    .await
                } else {
                    write_within(
                        write_timeout,
                        writer.write_frames_with_commit(&outbound_batch, |index| {
                            if let Some(correlation) = outbound_correlations[index] {
                                messaging.mark_socket_write_started(correlation);
                            }
                        }),
                    )
                    .await
                };
                association.release_queued_bytes(reserved_bytes);
                let outcome = result?;
                association.record_outbound_write(frame_count, outcome.socket_writes);
                idle.as_mut().reset(
                    TokioInstant::now() + config.idle_data_connection_timeout
                );
            }
            inbound = reader.read_frame() => {
                let mut next_frame = Some(inbound?);
                association.record_peer_activity();
                let mut processed_frames = 0;
                while let Some(frame) = next_frame {
                last_received = Instant::now();
                idle.as_mut().reset(
                    TokioInstant::now() + config.idle_data_connection_timeout
                );
                match frame.kind {
                    FrameKind::Tell if matches!(lane, LaneKind::Bulk(_)) => {
                        match decode_tell_cached(&frame, target_cache, target_dictionary) {
                            Ok(tell) => {
                                let _ = dispatch_tell(dispatch.as_ref(), tell).await;
                            }
                            Err(_) => association.record_dropped_inbound_frame(),
                        }
                    }
                    FrameKind::EntityTell if matches!(lane, LaneKind::Bulk(_)) => {
                        match decode_entity_tell_cached(&frame, target_cache) {
                            Ok(tell) => {
                                let _ = dispatch
                                    .tell_entity(
                                        tell.sender,
                                        tell.target,
                                        tell.message_id,
                                        tell.payload,
                                    )
                                    .await;
                            }
                            Err(_) => association.record_dropped_inbound_frame(),
                        }
                    }
                    FrameKind::SingletonTell if matches!(lane, LaneKind::Bulk(_)) => {
                        match decode_singleton_tell_cached(&frame, target_cache) {
                            Ok(tell) => {
                                let _ = dispatch
                                    .tell_singleton(
                                        tell.sender,
                                        tell.target,
                                        tell.message_id,
                                        tell.payload,
                                    )
                                    .await;
                            }
                            Err(_) => association.record_dropped_inbound_frame(),
                        }
                    }
                    FrameKind::Ask if lane == LaneKind::Interactive => {
                        let ask = decode_ask_cached(&frame, target_cache)?;
                        if asks.len() == config.maximum_concurrent_inbound_asks {
                            write_within(write_timeout, writer.write_frame(&failure_frame(&RemoteFailure {
                                correlation_id: ask.correlation_id,
                                code: RemoteFailureCode::MailboxFull,
                                safe_detail: None,
                            }))).await?;
                        } else {
                            asks.push(dispatch_inbound_ask(
                                dispatch.clone(),
                                InboundAskWork::Exact(ask),
                            ));
                        }
                    }
                    FrameKind::EntityAsk if lane == LaneKind::Interactive => {
                        let ask = decode_entity_ask(&frame)?;
                        if asks.len() == config.maximum_concurrent_inbound_asks {
                            write_within(write_timeout, writer.write_frame(&failure_frame(&RemoteFailure {
                                correlation_id: ask.correlation_id,
                                code: RemoteFailureCode::MailboxFull,
                                safe_detail: None,
                            }))).await?;
                        } else {
                            asks.push(dispatch_inbound_ask(
                                dispatch.clone(),
                                InboundAskWork::Entity(ask),
                            ));
                        }
                    }
                    FrameKind::SingletonAsk if lane == LaneKind::Interactive => {
                        let ask = decode_singleton_ask(&frame)?;
                        if asks.len() == config.maximum_concurrent_inbound_asks {
                            write_within(write_timeout, writer.write_frame(&failure_frame(&RemoteFailure {
                                correlation_id: ask.correlation_id,
                                code: RemoteFailureCode::MailboxFull,
                                safe_detail: None,
                            }))).await?;
                        } else {
                            asks.push(dispatch_inbound_ask(
                                dispatch.clone(),
                                InboundAskWork::Singleton(ask),
                            ));
                        }
                    }
                    FrameKind::Reply if lane == LaneKind::Interactive => {
                        let (correlation, payload) = decode_reply(&frame)?;
                        if !messaging.complete_reply(correlation, payload) {
                            association.record_discarded_reply();
                        }
                    }
                    FrameKind::Failure if lane == LaneKind::Interactive => {
                        let failure = decode_failure(&frame)?;
                        if !messaging.complete_failure(
                            failure.correlation_id,
                            AskError::Remote(failure.code),
                        ) {
                            association.record_discarded_reply();
                        }
                    }
                    FrameKind::Heartbeat if lane == LaneKind::Control => {
                        write_within(
                            write_timeout,
                            writer.write_frame(&Frame::new(FrameKind::HeartbeatAck, Bytes::new())),
                        )
                        .await?;
                    }
                    FrameKind::HeartbeatAck if lane == LaneKind::Control => {}
                    FrameKind::ControlEnvelope if lane == LaneKind::Control => control_apply_tx
                        .as_ref()
                        .expect("control lane requires an apply worker")
                        .try_send(frame)
                        .map_err(|_| LaneError::ControlApplyBackpressure)?,
                    FrameKind::ControlAck if lane == LaneKind::Control => {
                        association.acknowledge_control(decode_control_ack(&frame)?)?;
                    }
                    FrameKind::CoordinatorEvent if lane == LaneKind::Control => control_apply_tx
                        .as_ref()
                        .expect("control lane requires an apply worker")
                        .try_send(frame)
                        .map_err(|_| LaneError::ControlApplyBackpressure)?,
                    FrameKind::Backpressure => {}
                    FrameKind::LaneWake if lane == LaneKind::Control => {
                        let lane = decode_lane_wake(&frame)?;
                        association
                            .notify_lane_wake(lane)
                            .map_err(LaneError::Association)?;
                    }
                    FrameKind::Close => return Ok(LaneExit::RemoteClose),
                    kind => return Err(LaneError::UnexpectedFrame { lane, kind }),
                }
                if let Some((hits, misses)) = target_cache.take_metrics_if_ready() {
                    association.record_exact_target_cache(hits, misses);
                }
                processed_frames += 1;
                next_frame = if processed_frames < config.maximum_ready_read_batch_frames {
                    reader.try_read_frame()?
                } else {
                    None
                };
                }
            }
            () = &mut idle, if lane != LaneKind::Control => {
                if lane == LaneKind::Interactive
                    && (!asks.is_empty()
                        || messaging.has_pending_for_association(association.id()))
                {
                    idle.as_mut().reset(
                        TokioInstant::now() + config.idle_data_connection_timeout
                    );
                    continue;
                }
                write_within(write_timeout, writer.flush()).await?;
                return Ok(LaneExit::Idle);
            }
        }
    }
}

async fn write_within<T, F>(limit: Option<Duration>, write: F) -> Result<T, LaneError>
where
    F: Future<Output = Result<T, WireError>>,
{
    match limit {
        Some(limit) => tokio::time::timeout(limit, write)
            .await
            .map_err(|_| LaneError::WriteTimeout)?
            .map_err(LaneError::from),
        None => write.await.map_err(LaneError::from),
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
