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
    control::{
        CommandId, ControlApply, ControlDispatch, ControlDispatchError, ReliableControlError,
        control_ack_frame, decode_control_ack, decode_control_envelope,
    },
    messaging::{
        codec::{
            decode_ask_cached, decode_entity_ask, decode_entity_tell_cached, decode_failure,
            decode_reply, decode_singleton_ask, decode_singleton_tell_cached, decode_tell_cached,
            failure_frame, reply_frame,
        },
        error::{AskError, RemoteFailureCode, RemoteMessageError},
        inbound::{InboundDispatch, dispatch_tell, failure_code},
        outbound::{OutboundMessaging, PreparedOutboundFrame},
        target::{CorrelationId, InboundAsk, InboundEntityAsk, InboundSingletonAsk, RemoteFailure},
        target_cache::ExactTargetCache,
        target_dictionary::ExactTargetDictionary,
    },
    transport::{FramedReader, FramedWriter, RemotingIo},
    wire::{Frame, FrameCodec, FrameKind, WireError},
};

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

enum InboundAskWork {
    Exact(InboundAsk),
    Entity(InboundEntityAsk),
    Singleton(InboundSingletonAsk),
}

async fn dispatch_inbound_ask(
    dispatch: Arc<dyn InboundDispatch>,
    work: InboundAskWork,
) -> Result<Frame, RemoteMessageError> {
    match work {
        InboundAskWork::Exact(ask) => {
            let deadline = Instant::now()
                .checked_add(ask.timeout_budget)
                .ok_or(RemoteMessageError::DeadlineExceeded)?;
            let result = dispatch
                .ask(ask.target, ask.message_id, ask.payload, deadline)
                .await;
            Ok(inbound_ask_response(ask.correlation_id, result))
        }
        InboundAskWork::Entity(ask) => {
            let deadline = Instant::now()
                .checked_add(ask.timeout_budget)
                .ok_or(RemoteMessageError::DeadlineExceeded)?;
            let result = dispatch
                .ask_entity(ask.target, ask.message_id, ask.payload, deadline)
                .await;
            Ok(inbound_ask_response(ask.correlation_id, result))
        }
        InboundAskWork::Singleton(ask) => {
            let deadline = Instant::now()
                .checked_add(ask.timeout_budget)
                .ok_or(RemoteMessageError::DeadlineExceeded)?;
            let result = dispatch
                .ask_singleton(ask.target, ask.message_id, ask.payload, deadline)
                .await;
            Ok(inbound_ask_response(ask.correlation_id, result))
        }
    }
}

fn inbound_ask_response(
    correlation_id: CorrelationId,
    result: Result<Bytes, RemoteMessageError>,
) -> Frame {
    match result {
        Ok(payload) => reply_frame(correlation_id, payload),
        Err(error) => failure_frame(&RemoteFailure {
            correlation_id,
            code: failure_code(&error),
            safe_detail: None,
        }),
    }
}

struct ControlWorkerGuard(tokio::task::JoinHandle<()>);

impl Drop for ControlWorkerGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn apply_control_frame(
    association: Arc<Association>,
    control_dispatch: Arc<dyn ControlDispatch>,
    frame: Frame,
) -> Result<Option<Frame>, LaneError> {
    match frame.kind {
        FrameKind::ControlEnvelope => {
            let envelope = decode_control_envelope(&frame)?;
            match association.preview_control(&envelope) {
                ControlApply::Apply(_) => {
                    let result = control_dispatch
                        .apply(
                            association.key().clone(),
                            envelope.command_id,
                            envelope.payload.clone(),
                        )
                        .await;
                    match result {
                        Ok(()) | Err(ControlDispatchError::InvalidCommand) => {}
                        Err(error) => return Err(error.into()),
                    }
                    lattice_core::failpoint::hit(Failpoint::ControlAfterRemoteApplyBeforeAck);
                    let ack = association.commit_control(envelope);
                    Ok(Some(control_ack_frame(ack)))
                }
                ControlApply::Duplicate(anticipated) => {
                    let ack = if association.current_control_ack().cumulative_sequence
                        < anticipated.cumulative_sequence
                    {
                        association.commit_control(envelope)
                    } else {
                        anticipated
                    };
                    Ok(Some(control_ack_frame(ack)))
                }
                ControlApply::Gap(gap) => {
                    control_dispatch
                        .reconcile(association.key().clone(), Some(gap))
                        .await?;
                    Ok(None)
                }
                ControlApply::ReconcileEpoch => {
                    control_dispatch
                        .reconcile(association.key().clone(), None)
                        .await?;
                    Ok(None)
                }
            }
        }
        FrameKind::CoordinatorEvent => {
            control_dispatch
                .apply(
                    association.key().clone(),
                    CommandId::generate(),
                    frame.into_payload(),
                )
                .await?;
            Ok(None)
        }
        _ => Err(LaneError::UnexpectedControlWork),
    }
}

fn decode_lane_wake(frame: &Frame) -> Result<LaneKind, LaneError> {
    let [encoded] = frame.payload() else {
        return Err(LaneError::InvalidLaneWake);
    };
    if *encoded == 0 {
        return Ok(LaneKind::Interactive);
    }
    Ok(LaneKind::Bulk(encoded.saturating_sub(1)))
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
mod tests {
    use async_trait::async_trait;
    use lattice_core::actor_ref::{
        ActivationId, ActorPath, ActorRef, ClusterId, NodeAddress, NodeIncarnation, ProtocolId,
    };
    use tokio::net::{TcpListener, TcpStream};

    use super::*;
    use crate::{
        association::{AssociationKey, LaneAttachment},
        config::RemotingConfig,
        control::RejectControlDispatch,
        messaging::{
            error::RemoteMessageError,
            outbound::OutboundMessage,
            target::{ExactActorTarget, SenderIdentity},
        },
        protocol::{ProtocolDescriptor, ProtocolFingerprint},
        transport::FramedConnection,
    };

    struct EchoDispatch {
        delay: Duration,
    }

    #[derive(Default)]
    struct RecordingDispatch {
        tells: std::sync::Mutex<Vec<ExactActorTarget>>,
    }

    #[async_trait]
    impl InboundDispatch for RecordingDispatch {
        async fn tell(
            &self,
            _sender: Option<ActorRef>,
            target: ExactActorTarget,
            _message_id: u64,
            _payload: Bytes,
        ) -> Result<(), RemoteMessageError> {
            self.tells
                .lock()
                .expect("recording dispatch poisoned")
                .push(target);
            Ok(())
        }

        async fn ask(
            &self,
            _target: ExactActorTarget,
            _message_id: u64,
            payload: Bytes,
            _deadline: Instant,
        ) -> Result<Bytes, RemoteMessageError> {
            Ok(payload)
        }
    }

    #[async_trait]
    impl InboundDispatch for EchoDispatch {
        async fn tell(
            &self,
            _sender: Option<ActorRef>,
            _target: ExactActorTarget,
            _message_id: u64,
            _payload: Bytes,
        ) -> Result<(), RemoteMessageError> {
            Ok(())
        }

        async fn ask(
            &self,
            _target: ExactActorTarget,
            _message_id: u64,
            payload: Bytes,
            deadline: Instant,
        ) -> Result<Bytes, RemoteMessageError> {
            tokio::time::sleep(self.delay).await;
            if Instant::now() >= deadline {
                return Err(RemoteMessageError::DeadlineExceeded);
            }
            Ok(payload)
        }
    }

    fn active_association(
        local: NodeIncarnation,
        remote: NodeIncarnation,
        remote_address: NodeAddress,
        protocol_id: ProtocolId,
        fingerprint: ProtocolFingerprint,
    ) -> Arc<Association> {
        let key = AssociationKey {
            cluster_id: ClusterId::new("lane-test").unwrap(),
            local_incarnation: local,
            remote_address,
            remote_incarnation: remote,
        };
        let association =
            Arc::new(Association::new(key.clone(), RemotingConfig::default()).unwrap());
        for (lane, nonce) in [
            (LaneKind::Control, 1),
            (LaneKind::Interactive, 2),
            (LaneKind::Bulk(0), 3),
        ] {
            association
                .attach(LaneAttachment {
                    association_id: association.id(),
                    key: key.clone(),
                    lane,
                    connection_nonce: nonce,
                })
                .unwrap();
        }
        association
            .install_peer_catalogue([ProtocolDescriptor {
                protocol_id,
                fingerprint,
            }])
            .unwrap();
        association
    }

    #[tokio::test]
    async fn interactive_lane_stays_awake_while_ask_is_in_flight() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socket = listener.local_addr().unwrap();
        let client_incarnation = NodeIncarnation::new(1).unwrap();
        let server_incarnation = NodeIncarnation::new(2).unwrap();
        let protocol_id = ProtocolId::new(7).unwrap();
        let fingerprint = ProtocolFingerprint::digest(b"lane-test/v1");
        let server_address = NodeAddress::new("127.0.0.1", socket.port()).unwrap();
        let client_address = NodeAddress::new("127.0.0.1", 25549).unwrap();
        let client_association = active_association(
            client_incarnation,
            server_incarnation,
            server_address.clone(),
            protocol_id,
            fingerprint,
        );
        let server_association = active_association(
            server_incarnation,
            client_incarnation,
            client_address,
            protocol_id,
            fingerprint,
        );
        let mut client_receiver = client_association.take_receivers().unwrap().interactive;
        let mut server_receiver = server_association.take_receivers().unwrap().interactive;
        let client_messaging = Arc::new(OutboundMessaging::new(8).unwrap());
        let server_messaging = Arc::new(OutboundMessaging::new(8).unwrap());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut server_shutdown = shutdown_rx.clone();
        let server_lane = {
            let association = server_association.clone();
            let messaging = server_messaging.clone();
            tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                BidirectionalLane::new(
                    association,
                    LaneKind::Interactive,
                    2,
                    LaneServices::new(
                        messaging,
                        Arc::new(EchoDispatch {
                            delay: Duration::from_millis(125),
                        }),
                        Arc::new(RejectControlDispatch),
                    ),
                    BidirectionalLaneConfig {
                        maximum_frame_size: 4096,
                        maximum_concurrent_inbound_asks: 8,
                        heartbeat_interval: Duration::from_millis(100),
                        heartbeat_miss_limit: 10,
                        idle_data_connection_timeout: Duration::from_millis(25),
                        maximum_cached_exact_targets: 8,
                        socket_read_ahead_bytes: 1024,
                        maximum_ready_write_batch_frames: 8,
                        maximum_ready_read_batch_frames: 8,
                        maximum_coalesced_write_batch_bytes: 4096,
                        maximum_pending_control_applies: 8,
                    },
                )
                .run(&mut server_receiver, stream, &mut server_shutdown)
                .await
            })
        };
        let stream = TcpStream::connect(socket).await.unwrap();
        let mut client_shutdown = shutdown_rx;
        let client_lane = {
            let association = client_association.clone();
            let messaging = client_messaging.clone();
            tokio::spawn(async move {
                BidirectionalLane::new(
                    association,
                    LaneKind::Interactive,
                    2,
                    LaneServices::new(
                        messaging,
                        Arc::new(EchoDispatch {
                            delay: Duration::from_millis(125),
                        }),
                        Arc::new(RejectControlDispatch),
                    ),
                    BidirectionalLaneConfig {
                        maximum_frame_size: 4096,
                        maximum_concurrent_inbound_asks: 8,
                        heartbeat_interval: Duration::from_millis(100),
                        heartbeat_miss_limit: 10,
                        idle_data_connection_timeout: Duration::from_millis(25),
                        maximum_cached_exact_targets: 8,
                        socket_read_ahead_bytes: 1024,
                        maximum_ready_write_batch_frames: 8,
                        maximum_ready_read_batch_frames: 8,
                        maximum_coalesced_write_batch_bytes: 4096,
                        maximum_pending_control_applies: 8,
                    },
                )
                .run(&mut client_receiver, stream, &mut client_shutdown)
                .await
            })
        };
        let target = ActorRef::new(
            ClusterId::new("lane-test").unwrap(),
            server_address,
            server_incarnation,
            ActorPath::user(["user", "echo"]).unwrap(),
            ActivationId::new(server_incarnation, 1).unwrap(),
            protocol_id,
        )
        .unwrap();
        let mut pending = JoinSet::new();
        for index in 0_u8..8 {
            let messaging = client_messaging.clone();
            let association = client_association.clone();
            let target = target.clone();
            pending.spawn(async move {
                let expected = Bytes::from(vec![index]);
                let reply = messaging
                    .ask(
                        &association,
                        &SenderIdentity::Process(9),
                        &target,
                        OutboundMessage::new(fingerprint, u64::from(index) + 1, expected.clone()),
                        Instant::now() + Duration::from_secs(1),
                    )
                    .await
                    .unwrap();
                (reply, expected)
            });
        }
        while let Some(completed) = pending.join_next().await {
            let (reply, expected) = completed.unwrap();
            assert_eq!(reply, expected);
        }
        shutdown_tx.send(true).unwrap();
        assert_eq!(client_lane.await.unwrap().unwrap(), LaneExit::Shutdown);
        assert_eq!(server_lane.await.unwrap().unwrap(), LaneExit::Shutdown);
    }

    fn duplex_lane_config() -> BidirectionalLaneConfig {
        BidirectionalLaneConfig {
            maximum_frame_size: 4096,
            maximum_concurrent_inbound_asks: 8,
            heartbeat_interval: Duration::from_millis(50),
            heartbeat_miss_limit: 2,
            idle_data_connection_timeout: Duration::from_secs(5),
            maximum_cached_exact_targets: 8,
            socket_read_ahead_bytes: 1024,
            maximum_ready_write_batch_frames: 8,
            maximum_ready_read_batch_frames: 8,
            maximum_coalesced_write_batch_bytes: 4096,
            maximum_pending_control_applies: 8,
        }
    }

    fn lane_services(
        messaging: Arc<OutboundMessaging>,
        dispatch: Arc<dyn InboundDispatch>,
    ) -> LaneServices {
        LaneServices::new(messaging, dispatch, Arc::new(RejectControlDispatch))
    }

    fn lane_target(
        address: &NodeAddress,
        incarnation: NodeIncarnation,
        protocol_id: ProtocolId,
    ) -> ActorRef {
        ActorRef::new(
            ClusterId::new("lane-test").unwrap(),
            address.clone(),
            incarnation,
            ActorPath::user(["user", "echo"]).unwrap(),
            ActivationId::new(incarnation, 1).unwrap(),
            protocol_id,
        )
        .unwrap()
    }

    async fn wait_for(mut condition: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("lane did not reach the expected state");
    }

    #[tokio::test]
    async fn a_bulk_stripe_failure_keeps_interactive_asks_pending() {
        let client_incarnation = NodeIncarnation::new(1).unwrap();
        let server_incarnation = NodeIncarnation::new(2).unwrap();
        let protocol_id = ProtocolId::new(7).unwrap();
        let fingerprint = ProtocolFingerprint::digest(b"lane-test/v1");
        let server_address = NodeAddress::new("127.0.0.1", 25551).unwrap();
        let association = active_association(
            client_incarnation,
            server_incarnation,
            server_address.clone(),
            protocol_id,
            fingerprint,
        );
        let messaging = Arc::new(OutboundMessaging::new(8).unwrap());
        let target = lane_target(&server_address, server_incarnation, protocol_id);
        let ask = {
            let messaging = messaging.clone();
            let association = association.clone();
            tokio::spawn(async move {
                messaging
                    .ask(
                        &association,
                        &SenderIdentity::Process(9),
                        &target,
                        OutboundMessage::new(fingerprint, 1, Bytes::from_static(b"ask")),
                        Instant::now() + Duration::from_secs(5),
                    )
                    .await
            })
        };
        wait_for(|| messaging.pending_count() == 1).await;
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let mut bulk = association.take_lane_receiver(LaneKind::Bulk(0)).unwrap();
        let (peer, lane_io) = tokio::io::duplex(1024);
        drop(peer);
        let bulk_result = BidirectionalLane::new(
            association.clone(),
            LaneKind::Bulk(0),
            3,
            lane_services(messaging.clone(), Arc::new(RecordingDispatch::default())),
            duplex_lane_config(),
        )
        .run(&mut bulk, lane_io, &mut shutdown_rx)
        .await;

        assert!(bulk_result.is_err());
        assert_eq!(messaging.pending_count(), 1);
        assert!(!ask.is_finished());

        let mut interactive = association
            .take_lane_receiver(LaneKind::Interactive)
            .unwrap();
        let (peer, lane_io) = tokio::io::duplex(1024);
        drop(peer);
        let interactive_result = BidirectionalLane::new(
            association.clone(),
            LaneKind::Interactive,
            2,
            lane_services(messaging.clone(), Arc::new(RecordingDispatch::default())),
            duplex_lane_config(),
        )
        .run(&mut interactive, lane_io, &mut shutdown_rx)
        .await;

        assert!(interactive_result.is_err());
        assert_eq!(messaging.pending_count(), 0);
        assert_eq!(
            ask.await.unwrap().unwrap_err(),
            AskError::AssociationLostBeforeWrite
        );
    }

    #[tokio::test]
    async fn a_blocked_control_write_fails_within_the_heartbeat_window() {
        let association = active_association(
            NodeIncarnation::new(1).unwrap(),
            NodeIncarnation::new(2).unwrap(),
            NodeAddress::new("127.0.0.1", 25554).unwrap(),
            ProtocolId::new(7).unwrap(),
            ProtocolFingerprint::digest(b"lane-test/v1"),
        );
        let mut control = association.take_lane_receiver(LaneKind::Control).unwrap();
        let (_peer, lane_io) = tokio::io::duplex(16);
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            BidirectionalLane::new(
                association.clone(),
                LaneKind::Control,
                1,
                lane_services(
                    Arc::new(OutboundMessaging::new(8).unwrap()),
                    Arc::new(RecordingDispatch::default()),
                ),
                duplex_lane_config(),
            )
            .run(&mut control, lane_io, &mut shutdown_rx),
        )
        .await
        .expect("a control lane write must not outlive its heartbeat window");

        assert!(matches!(result, Err(LaneError::WriteTimeout)));
    }

    #[tokio::test]
    async fn a_queued_compact_tell_is_expanded_after_the_stripe_reconnects() {
        let client_incarnation = NodeIncarnation::new(1).unwrap();
        let server_incarnation = NodeIncarnation::new(2).unwrap();
        let protocol_id = ProtocolId::new(7).unwrap();
        let fingerprint = ProtocolFingerprint::digest(b"lane-test/v1");
        let server_address = NodeAddress::new("127.0.0.1", 25555).unwrap();
        let client_address = NodeAddress::new("127.0.0.1", 25556).unwrap();
        let client_association = active_association(
            client_incarnation,
            server_incarnation,
            server_address.clone(),
            protocol_id,
            fingerprint,
        );
        let server_association = active_association(
            server_incarnation,
            client_incarnation,
            client_address,
            protocol_id,
            fingerprint,
        );
        let messaging = Arc::new(OutboundMessaging::new(8).unwrap());
        let target = lane_target(&server_address, server_incarnation, protocol_id);
        let route = messaging
            .prepare_exact_tell_route(
                client_association.clone(),
                &SenderIdentity::Process(9),
                &target,
                fingerprint,
            )
            .unwrap();
        let mut client_bulk = client_association
            .take_lane_receiver(LaneKind::Bulk(0))
            .unwrap();
        route.tell(1, Bytes::from_static(b"registration")).unwrap();
        let registration = client_bulk.recv().await.unwrap();
        route.tell(1, Bytes::from_static(b"compact")).unwrap();
        client_association.release_queued_bytes(registration.payload_len());

        client_association.detach(LaneKind::Bulk(0), 3);
        client_association
            .attach(LaneAttachment {
                association_id: client_association.id(),
                key: client_association.key().clone(),
                lane: LaneKind::Bulk(0),
                connection_nonce: 4,
            })
            .unwrap();

        let mut server_bulk = server_association
            .take_lane_receiver(LaneKind::Bulk(0))
            .unwrap();
        let dispatch = Arc::new(RecordingDispatch::default());
        let (client_io, server_io) = tokio::io::duplex(4096);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut client_shutdown = shutdown_rx.clone();
        let mut server_shutdown = shutdown_rx;
        let server_lane = {
            let association = server_association.clone();
            let dispatch = dispatch.clone();
            let messaging = messaging.clone();
            tokio::spawn(async move {
                BidirectionalLane::new(
                    association,
                    LaneKind::Bulk(0),
                    3,
                    lane_services(messaging, dispatch),
                    duplex_lane_config(),
                )
                .run(&mut server_bulk, server_io, &mut server_shutdown)
                .await
            })
        };
        let client_lane = {
            let association = client_association.clone();
            let messaging = messaging.clone();
            tokio::spawn(async move {
                BidirectionalLane::new(
                    association,
                    LaneKind::Bulk(0),
                    4,
                    lane_services(messaging, Arc::new(RecordingDispatch::default())),
                    duplex_lane_config(),
                )
                .run(&mut client_bulk, client_io, &mut client_shutdown)
                .await
            })
        };

        wait_for(|| {
            dispatch
                .tells
                .lock()
                .expect("recording dispatch poisoned")
                .len()
                == 1
        })
        .await;
        shutdown_tx.send(true).unwrap();
        assert_eq!(client_lane.await.unwrap().unwrap(), LaneExit::Shutdown);
        assert_eq!(server_lane.await.unwrap().unwrap(), LaneExit::Shutdown);
        let dispatched = dispatch.tells.lock().expect("recording dispatch poisoned");
        let dispatched: ActorRef = dispatched[0].actor_ref().unwrap();
        assert!(dispatched.same_activation(&target));
        assert_eq!(server_association.metrics().dropped_inbound_frames, 0);
    }

    #[tokio::test]
    async fn an_unregistered_dictionary_id_drops_one_frame_and_keeps_the_lane() {
        let client_incarnation = NodeIncarnation::new(1).unwrap();
        let server_incarnation = NodeIncarnation::new(2).unwrap();
        let protocol_id = ProtocolId::new(7).unwrap();
        let fingerprint = ProtocolFingerprint::digest(b"lane-test/v1");
        let server_address = NodeAddress::new("127.0.0.1", 25557).unwrap();
        let client_address = NodeAddress::new("127.0.0.1", 25558).unwrap();
        let client_association = active_association(
            client_incarnation,
            server_incarnation,
            server_address.clone(),
            protocol_id,
            fingerprint,
        );
        let server_association = active_association(
            server_incarnation,
            client_incarnation,
            client_address,
            protocol_id,
            fingerprint,
        );
        let messaging = Arc::new(OutboundMessaging::new(8).unwrap());
        let target = lane_target(&server_address, server_incarnation, protocol_id);
        let route = messaging
            .prepare_exact_tell_route(
                client_association.clone(),
                &SenderIdentity::Process(9),
                &target,
                fingerprint,
            )
            .unwrap();
        route.tell(1, Bytes::from_static(b"registration")).unwrap();
        route.tell(1, Bytes::from_static(b"compact")).unwrap();
        let mut client_bulk = client_association
            .take_lane_receiver(LaneKind::Bulk(0))
            .unwrap();
        let registration = client_bulk.recv().await.unwrap();
        let compact = client_bulk.recv().await.unwrap();
        assert!(compact.payload_len() < registration.payload_len());

        let mut server_bulk = server_association
            .take_lane_receiver(LaneKind::Bulk(0))
            .unwrap();
        let dispatch = Arc::new(RecordingDispatch::default());
        let (peer, lane_io) = tokio::io::duplex(4096);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut lane_shutdown = shutdown_rx;
        let lane = {
            let association = server_association.clone();
            let dispatch = dispatch.clone();
            let messaging = messaging.clone();
            tokio::spawn(async move {
                BidirectionalLane::new(
                    association,
                    LaneKind::Bulk(0),
                    3,
                    lane_services(messaging, dispatch),
                    duplex_lane_config(),
                )
                .run(&mut server_bulk, lane_io, &mut lane_shutdown)
                .await
            })
        };
        let mut peer = FramedConnection::new(peer, FrameCodec::new(4096).unwrap());
        peer.write_frame(&compact).await.unwrap();
        peer.write_frame(&registration).await.unwrap();

        wait_for(|| {
            dispatch
                .tells
                .lock()
                .expect("recording dispatch poisoned")
                .len()
                == 1
        })
        .await;
        assert_eq!(server_association.metrics().dropped_inbound_frames, 1);
        shutdown_tx.send(true).unwrap();
        assert_eq!(lane.await.unwrap().unwrap(), LaneExit::Shutdown);
    }

    #[tokio::test]
    async fn a_reply_without_a_pending_ask_is_discarded_and_counted() {
        let association = active_association(
            NodeIncarnation::new(1).unwrap(),
            NodeIncarnation::new(2).unwrap(),
            NodeAddress::new("127.0.0.1", 25559).unwrap(),
            ProtocolId::new(7).unwrap(),
            ProtocolFingerprint::digest(b"lane-test/v1"),
        );
        let mut interactive = association
            .take_lane_receiver(LaneKind::Interactive)
            .unwrap();
        let (peer, lane_io) = tokio::io::duplex(4096);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut lane_shutdown = shutdown_rx;
        let lane = {
            let association = association.clone();
            tokio::spawn(async move {
                BidirectionalLane::new(
                    association,
                    LaneKind::Interactive,
                    2,
                    lane_services(
                        Arc::new(OutboundMessaging::new(8).unwrap()),
                        Arc::new(RecordingDispatch::default()),
                    ),
                    duplex_lane_config(),
                )
                .run(&mut interactive, lane_io, &mut lane_shutdown)
                .await
            })
        };
        let mut peer = FramedConnection::new(peer, FrameCodec::new(4096).unwrap());
        peer.write_frame(&reply_frame(
            CorrelationId::new(9, 1).unwrap(),
            Bytes::from_static(b"late"),
        ))
        .await
        .unwrap();

        wait_for(|| association.metrics().discarded_replies == 1).await;
        shutdown_tx.send(true).unwrap();
        assert_eq!(lane.await.unwrap().unwrap(), LaneExit::Shutdown);
    }

    #[tokio::test]
    async fn the_control_lane_announces_close_before_shutdown() {
        let association = active_association(
            NodeIncarnation::new(1).unwrap(),
            NodeIncarnation::new(2).unwrap(),
            NodeAddress::new("127.0.0.1", 25560).unwrap(),
            ProtocolId::new(7).unwrap(),
            ProtocolFingerprint::digest(b"lane-test/v1"),
        );
        let mut control = association.take_lane_receiver(LaneKind::Control).unwrap();
        let (peer, lane_io) = tokio::io::duplex(4096);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut lane_shutdown = shutdown_rx;
        let lane = {
            let association = association.clone();
            tokio::spawn(async move {
                BidirectionalLane::new(
                    association,
                    LaneKind::Control,
                    1,
                    lane_services(
                        Arc::new(OutboundMessaging::new(8).unwrap()),
                        Arc::new(RecordingDispatch::default()),
                    ),
                    duplex_lane_config(),
                )
                .run(&mut control, lane_io, &mut lane_shutdown)
                .await
            })
        };
        let mut peer = FramedConnection::new(peer, FrameCodec::new(4096).unwrap());
        assert_eq!(peer.read_frame().await.unwrap().kind, FrameKind::Heartbeat);
        shutdown_tx.send(true).unwrap();

        let announced = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if peer.read_frame().await.unwrap().kind == FrameKind::Close {
                    return;
                }
            }
        })
        .await;

        assert!(announced.is_ok());
        assert_eq!(lane.await.unwrap().unwrap(), LaneExit::Shutdown);
    }

    #[tokio::test]
    async fn a_remote_close_completes_pending_asks_by_dispatch_knowledge() {
        let client_incarnation = NodeIncarnation::new(1).unwrap();
        let server_incarnation = NodeIncarnation::new(2).unwrap();
        let protocol_id = ProtocolId::new(7).unwrap();
        let fingerprint = ProtocolFingerprint::digest(b"lane-test/v1");
        let server_address = NodeAddress::new("127.0.0.1", 25561).unwrap();
        let association = active_association(
            client_incarnation,
            server_incarnation,
            server_address.clone(),
            protocol_id,
            fingerprint,
        );
        let messaging = Arc::new(OutboundMessaging::new(8).unwrap());
        let target = lane_target(&server_address, server_incarnation, protocol_id);
        let ask = {
            let messaging = messaging.clone();
            let association = association.clone();
            tokio::spawn(async move {
                messaging
                    .ask(
                        &association,
                        &SenderIdentity::Process(9),
                        &target,
                        OutboundMessage::new(fingerprint, 1, Bytes::from_static(b"ask")),
                        Instant::now() + Duration::from_secs(5),
                    )
                    .await
            })
        };
        wait_for(|| messaging.pending_count() == 1).await;

        let mut control = association.take_lane_receiver(LaneKind::Control).unwrap();
        let (peer, lane_io) = tokio::io::duplex(4096);
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let mut peer = FramedConnection::new(peer, FrameCodec::new(4096).unwrap());
        peer.write_frame(&Frame::new(FrameKind::Close, Bytes::new()))
            .await
            .unwrap();

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            BidirectionalLane::new(
                association.clone(),
                LaneKind::Control,
                1,
                lane_services(messaging.clone(), Arc::new(RecordingDispatch::default())),
                duplex_lane_config(),
            )
            .run(&mut control, lane_io, &mut shutdown_rx),
        )
        .await
        .expect("a remote close must exit the control lane");

        assert_eq!(result.unwrap(), LaneExit::RemoteClose);
        assert_eq!(
            ask.await.unwrap().unwrap_err(),
            AskError::AssociationLostBeforeWrite
        );
    }
}
