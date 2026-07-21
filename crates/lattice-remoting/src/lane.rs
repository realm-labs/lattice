use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use lattice_core::failpoint::Failpoint;
use thiserror::Error;
use tokio::{
    sync::{mpsc, watch},
    task::{JoinError, JoinSet},
    time::{Instant as TokioInstant, MissedTickBehavior},
};

use crate::{
    association::{Association, AssociationError, LaneKind},
    control::{
        CommandId, ControlApply, ControlDispatch, ControlDispatchError, ReliableControlError,
        control_ack_frame, decode_control_ack, decode_control_envelope,
    },
    messaging::{
        codec::{
            ask_correlation, decode_ask, decode_entity_ask, decode_entity_tell, decode_failure,
            decode_reply, decode_singleton_ask, decode_singleton_tell, decode_tell, failure_frame,
            reply_frame,
        },
        error::{AskError, RemoteFailureCode, RemoteMessageError},
        inbound::{InboundDispatch, failure_code},
        outbound::OutboundMessaging,
        target::RemoteFailure,
    },
    transport::{FramedReader, FramedWriter, RemotingIo},
    wire::{Frame, FrameCodec, FrameKind, WireError},
};

// Coalesce only bulk frames that are already queued; never delay the first frame to fill a batch.
const MAX_BULK_WRITE_BATCH_FRAMES: usize = 32;

#[derive(Debug, Clone, Copy)]
pub struct BidirectionalLaneConfig {
    pub maximum_frame_size: usize,
    pub maximum_concurrent_inbound_asks: usize,
    pub heartbeat_interval: Duration,
    pub heartbeat_miss_limit: u32,
    pub idle_data_connection_timeout: Duration,
}

impl BidirectionalLaneConfig {
    fn validate(self) -> Result<Self, LaneError> {
        if self.maximum_frame_size < 8 || self.maximum_concurrent_inbound_asks == 0 {
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
        let result = run_bidirectional_lane_inner(&self, receiver, stream, shutdown).await;
        self.association.detach(self.lane, self.connection_nonce);
        if result.is_err() {
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
    let mut reader = FramedReader::new(read, codec.clone());
    let mut writer = FramedWriter::new(write, codec);
    let mut asks = JoinSet::new();
    let mut heartbeat = tokio::time::interval(config.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_received = Instant::now();
    let mut outbound_candidates = Vec::with_capacity(MAX_BULK_WRITE_BATCH_FRAMES);
    let mut outbound_batch = Vec::with_capacity(MAX_BULK_WRITE_BATCH_FRAMES);
    let idle = tokio::time::sleep(config.idle_data_connection_timeout);
    tokio::pin!(idle);

    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    writer.flush().await?;
                    return Ok(LaneExit::Shutdown);
                }
            }
            completed = asks.join_next(), if !asks.is_empty() => {
                let Some(completed) = completed else {
                    continue;
                };
                let frame = completed.map_err(LaneError::Join)??;
                writer.write_frame(&frame).await?;
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
                let batch_limit = if matches!(lane, LaneKind::Bulk(_)) {
                    MAX_BULK_WRITE_BATCH_FRAMES
                } else {
                    1
                };
                while outbound_candidates.len() < batch_limit {
                    let Ok(frame) = receiver.try_recv() else {
                        break;
                    };
                    outbound_candidates.push(frame);
                }
                outbound_batch.clear();
                let mut reserved_bytes = 0;
                for mut frame in outbound_candidates.drain(..) {
                    let frame_bytes = frame.payload_len();
                    if !messaging.prepare_ask_for_socket_write(&mut frame) {
                        association.release_queued_bytes(frame_bytes);
                        continue;
                    }
                    reserved_bytes += frame_bytes;
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
                let result = if matches!(lane, LaneKind::Bulk(_)) {
                    writer
                        .write_frames_with_commit(&outbound_batch, |_| {})
                        .await
                } else {
                    let correlation = ask_correlation(&outbound_batch[0]);
                    writer
                        .write_frame_with_commit(&outbound_batch[0], || {
                            if let Some(correlation) = correlation {
                                messaging.mark_socket_write_started(correlation);
                            }
                        })
                        .await
                };
                association.release_queued_bytes(reserved_bytes);
                result?;
                idle.as_mut().reset(
                    TokioInstant::now() + config.idle_data_connection_timeout
                );
            }
            inbound = reader.read_frame() => {
                let frame = inbound?;
                last_received = Instant::now();
                idle.as_mut().reset(
                    TokioInstant::now() + config.idle_data_connection_timeout
                );
                match frame.kind {
                    FrameKind::Tell if matches!(lane, LaneKind::Bulk(_)) => {
                        let tell = decode_tell(&frame)?;
                        let _ = dispatch
                            .tell(tell.sender, tell.target, tell.message_id, tell.payload)
                            .await;
                    }
                    FrameKind::EntityTell if matches!(lane, LaneKind::Bulk(_)) => {
                        let tell = decode_entity_tell(&frame)?;
                        let _ = dispatch
                            .tell_entity(tell.sender, tell.target, tell.message_id, tell.payload)
                            .await;
                    }
                    FrameKind::SingletonTell if matches!(lane, LaneKind::Bulk(_)) => {
                        let tell = decode_singleton_tell(&frame)?;
                        let _ = dispatch
                            .tell_singleton(tell.sender, tell.target, tell.message_id, tell.payload)
                            .await;
                    }
                    FrameKind::Ask if lane == LaneKind::Interactive => {
                        let ask = decode_ask(&frame)?;
                        if asks.len() == config.maximum_concurrent_inbound_asks {
                            writer.write_frame(&failure_frame(&RemoteFailure {
                                correlation_id: ask.correlation_id,
                                code: RemoteFailureCode::MailboxFull,
                                safe_detail: None,
                            })).await?;
                        } else {
                            let dispatch = dispatch.clone();
                            asks.spawn(async move {
                                let deadline = Instant::now()
                                    .checked_add(ask.timeout_budget)
                                    .ok_or(RemoteMessageError::DeadlineExceeded)?;
                                Ok::<_, RemoteMessageError>(match dispatch
                                    .ask(ask.target, ask.message_id, ask.payload, deadline)
                                    .await
                                {
                                    Ok(payload) => reply_frame(ask.correlation_id, payload),
                                    Err(error) => failure_frame(&RemoteFailure {
                                        correlation_id: ask.correlation_id,
                                        code: failure_code(&error),
                                        safe_detail: None,
                                    }),
                                })
                            });
                        }
                    }
                    FrameKind::EntityAsk if lane == LaneKind::Interactive => {
                        let ask = decode_entity_ask(&frame)?;
                        if asks.len() == config.maximum_concurrent_inbound_asks {
                            writer.write_frame(&failure_frame(&RemoteFailure {
                                correlation_id: ask.correlation_id,
                                code: RemoteFailureCode::MailboxFull,
                                safe_detail: None,
                            })).await?;
                        } else {
                            let dispatch = dispatch.clone();
                            asks.spawn(async move {
                                let deadline = Instant::now()
                                    .checked_add(ask.timeout_budget)
                                    .ok_or(RemoteMessageError::DeadlineExceeded)?;
                                Ok::<_, RemoteMessageError>(match dispatch
                                    .ask_entity(ask.target, ask.message_id, ask.payload, deadline)
                                    .await
                                {
                                    Ok(payload) => reply_frame(ask.correlation_id, payload),
                                    Err(error) => failure_frame(&RemoteFailure {
                                        correlation_id: ask.correlation_id,
                                        code: failure_code(&error),
                                        safe_detail: None,
                                    }),
                                })
                            });
                        }
                    }
                    FrameKind::SingletonAsk if lane == LaneKind::Interactive => {
                        let ask = decode_singleton_ask(&frame)?;
                        if asks.len() == config.maximum_concurrent_inbound_asks {
                            writer.write_frame(&failure_frame(&RemoteFailure {
                                correlation_id: ask.correlation_id,
                                code: RemoteFailureCode::MailboxFull,
                                safe_detail: None,
                            })).await?;
                        } else {
                            let dispatch = dispatch.clone();
                            asks.spawn(async move {
                                let deadline = Instant::now()
                                    .checked_add(ask.timeout_budget)
                                    .ok_or(RemoteMessageError::DeadlineExceeded)?;
                                Ok::<_, RemoteMessageError>(match dispatch
                                    .ask_singleton(ask.target, ask.message_id, ask.payload, deadline)
                                    .await
                                {
                                    Ok(payload) => reply_frame(ask.correlation_id, payload),
                                    Err(error) => failure_frame(&RemoteFailure {
                                        correlation_id: ask.correlation_id,
                                        code: failure_code(&error),
                                        safe_detail: None,
                                    }),
                                })
                            });
                        }
                    }
                    FrameKind::Reply if lane == LaneKind::Interactive => {
                        let (correlation, payload) = decode_reply(&frame)?;
                        messaging.complete_reply(correlation, payload);
                    }
                    FrameKind::Failure if lane == LaneKind::Interactive => {
                        let failure = decode_failure(&frame)?;
                        messaging.complete_failure(
                            failure.correlation_id,
                            AskError::Remote(failure.code),
                        );
                    }
                    FrameKind::Heartbeat if lane == LaneKind::Control => {
                        writer
                            .write_frame(&Frame::new(FrameKind::HeartbeatAck, Bytes::new()))
                            .await?;
                    }
                    FrameKind::HeartbeatAck if lane == LaneKind::Control => {}
                    FrameKind::ControlEnvelope if lane == LaneKind::Control => {
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
                                lattice_core::failpoint::hit(
                                    Failpoint::ControlAfterRemoteApplyBeforeAck,
                                );
                                let ack = association.commit_control(envelope);
                                writer.write_frame(&control_ack_frame(ack)).await?;
                            }
                            ControlApply::Duplicate(anticipated) => {
                                let ack = if association.current_control_ack().cumulative_sequence
                                    < anticipated.cumulative_sequence
                                {
                                    association.commit_control(envelope)
                                } else {
                                    anticipated
                                };
                                writer.write_frame(&control_ack_frame(ack)).await?;
                            }
                            ControlApply::Gap(gap) => {
                                control_dispatch
                                    .reconcile(association.key().clone(), Some(gap))
                                    .await?;
                            }
                            ControlApply::ReconcileEpoch => {
                                control_dispatch
                                    .reconcile(association.key().clone(), None)
                                    .await?;
                            }
                        }
                    }
                    FrameKind::ControlAck if lane == LaneKind::Control => {
                        association.acknowledge_control(decode_control_ack(&frame)?)?;
                    }
                    FrameKind::CoordinatorEvent if lane == LaneKind::Control => {
                        control_dispatch
                            .apply(
                                association.key().clone(),
                                CommandId::generate(),
                                frame.into_payload(),
                            )
                            .await?;
                    }
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
            }
            _ = heartbeat.tick(), if lane == LaneKind::Control => {
                if Instant::now().duration_since(last_received)
                    >= config.heartbeat_interval * config.heartbeat_miss_limit
                {
                    return Err(LaneError::HeartbeatTimeout);
                }
                writer
                    .write_frame(&Frame::new(FrameKind::Heartbeat, Bytes::new()))
                    .await?;
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
                writer.flush().await?;
                return Ok(LaneExit::Idle);
            }
        }
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
    };

    struct EchoDispatch {
        delay: Duration,
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
        let reply = client_messaging
            .ask(
                &client_association,
                &SenderIdentity::Process(9),
                &target,
                OutboundMessage::new(fingerprint, 1, Bytes::from_static(b"echo")),
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(reply, Bytes::from_static(b"echo"));
        shutdown_tx.send(true).unwrap();
        assert_eq!(client_lane.await.unwrap().unwrap(), LaneExit::Shutdown);
        assert_eq!(server_lane.await.unwrap().unwrap(), LaneExit::Shutdown);
    }
}
