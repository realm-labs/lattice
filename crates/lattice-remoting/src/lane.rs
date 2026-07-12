use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use crate::association::{Association, LaneKind};
use crate::control::{
    ControlApply, ControlDispatch, control_ack_frame, decode_control_ack, decode_control_envelope,
};
use crate::messaging::codec::{
    ask_correlation, decode_ask, decode_entity_ask, decode_entity_tell, decode_failure,
    decode_reply, decode_singleton_ask, decode_singleton_tell, decode_tell, failure_frame,
    reply_frame,
};
use crate::messaging::error::{AskError, RemoteFailureCode, RemoteMessageError};
use crate::messaging::inbound::{InboundDispatch, failure_code};
use crate::messaging::outbound::OutboundMessaging;
use crate::messaging::target::RemoteFailure;
use crate::transport::{FramedReader, FramedWriter, RemotingIo};
use crate::wire::{Frame, FrameKind, WireError};

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

#[allow(clippy::too_many_arguments)]
pub async fn run_bidirectional_lane<S>(
    association: Arc<Association>,
    lane: LaneKind,
    connection_nonce: u128,
    receiver: &mut mpsc::Receiver<Frame>,
    stream: S,
    messaging: Arc<OutboundMessaging>,
    dispatch: Arc<dyn InboundDispatch>,
    control_dispatch: Arc<dyn ControlDispatch>,
    config: BidirectionalLaneConfig,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<LaneExit, LaneError>
where
    S: RemotingIo,
{
    let config = config.validate()?;
    let result = run_bidirectional_lane_inner(
        &association,
        lane,
        receiver,
        stream,
        &messaging,
        dispatch,
        control_dispatch,
        config,
        shutdown,
    )
    .await;
    association.detach(lane, connection_nonce);
    if result.is_err() {
        messaging.fail_association(association.id());
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn run_bidirectional_lane_inner<S>(
    association: &Association,
    lane: LaneKind,
    receiver: &mut mpsc::Receiver<Frame>,
    stream: S,
    messaging: &OutboundMessaging,
    dispatch: Arc<dyn InboundDispatch>,
    control_dispatch: Arc<dyn ControlDispatch>,
    config: BidirectionalLaneConfig,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<LaneExit, LaneError>
where
    S: RemotingIo,
{
    let codec = crate::wire::FrameCodec::new(config.maximum_frame_size)?;
    let (read, write) = tokio::io::split(stream);
    let mut reader = FramedReader::new(read, codec.clone());
    let mut writer = FramedWriter::new(write, codec);
    let mut asks = JoinSet::new();
    let mut heartbeat = tokio::time::interval(config.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_received = Instant::now();
    let mut last_activity = Instant::now();
    let mut idle = tokio::time::interval(config.idle_data_connection_timeout);
    idle.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    idle.reset();

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
            }
            outbound = receiver.recv() => {
                let Some(mut frame) = outbound else {
                    return Ok(LaneExit::QueueClosed);
                };
                let reserved_bytes = frame.payload.len();
                if !messaging.prepare_ask_for_socket_write(&mut frame) {
                    association.release_queued_bytes(reserved_bytes);
                    continue;
                }
                let correlation = ask_correlation(&frame);
                if frame.kind == FrameKind::ControlEnvelope {
                    lattice_core::failpoint::hit(
                        lattice_core::failpoint::Failpoint::ControlAfterOutboxBeforeSocketWrite,
                    );
                }
                let result = writer.write_frame_with_commit(&frame, || {
                    if let Some(correlation) = correlation {
                        messaging.mark_socket_write_started(correlation);
                    }
                }).await;
                association.release_queued_bytes(reserved_bytes);
                result?;
                last_activity = Instant::now();
            }
            inbound = reader.read_frame() => {
                let frame = inbound?;
                last_received = Instant::now();
                last_activity = last_received;
                match frame.kind {
                    FrameKind::Tell if matches!(lane, LaneKind::Bulk(_)) => {
                        let tell = decode_tell(&frame)?;
                        let _ = dispatch.tell(tell.target, tell.message_id, tell.payload).await;
                    }
                    FrameKind::EntityTell if matches!(lane, LaneKind::Bulk(_)) => {
                        let tell = decode_entity_tell(&frame)?;
                        let _ = dispatch
                            .tell_entity(tell.target, tell.message_id, tell.payload)
                            .await;
                    }
                    FrameKind::SingletonTell if matches!(lane, LaneKind::Bulk(_)) => {
                        let tell = decode_singleton_tell(&frame)?;
                        let _ = dispatch
                            .tell_singleton(tell.target, tell.message_id, tell.payload)
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
                        writer.write_frame(&Frame {
                            kind: FrameKind::HeartbeatAck,
                            payload: Bytes::new(),
                        }).await?;
                    }
                    FrameKind::HeartbeatAck if lane == LaneKind::Control => {}
                    FrameKind::ControlEnvelope if lane == LaneKind::Control => {
                        let envelope = decode_control_envelope(&frame)?;
                        match association.preview_control(&envelope) {
                            ControlApply::Apply(_) => {
                                control_dispatch
                                    .apply(
                                        association.key().clone(),
                                        envelope.command_id,
                                        envelope.payload.clone(),
                                    )
                                    .await?;
                                lattice_core::failpoint::hit(
                                    lattice_core::failpoint::Failpoint::ControlAfterRemoteApplyBeforeAck,
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
                                crate::control::CommandId::generate(),
                                frame.payload,
                            )
                            .await?;
                    }
                    FrameKind::Backpressure => {}
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
                writer.write_frame(&Frame {
                    kind: FrameKind::Heartbeat,
                    payload: Bytes::new(),
                }).await?;
            }
            _ = idle.tick(), if lane != LaneKind::Control => {
                if Instant::now().duration_since(last_activity)
                    >= config.idle_data_connection_timeout
                {
                    writer.flush().await?;
                    return Ok(LaneExit::Idle);
                }
            }
        }
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
    #[error("lane received frame kind {kind:?} on {lane:?}")]
    UnexpectedFrame { lane: LaneKind, kind: FrameKind },
    #[error("inbound ask task failed")]
    Join(#[source] tokio::task::JoinError),
    #[error("inbound actor dispatch failed")]
    Dispatch(#[from] RemoteMessageError),
    #[error("reliable control dispatch failed")]
    ControlDispatch(#[from] crate::control::ControlDispatchError),
    #[error("reliable control state rejected a frame")]
    ReliableControl(#[from] crate::control::ReliableControlError),
    #[error("association rejected a reliable control acknowledgement")]
    Association(#[from] crate::association::AssociationError),
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
    use super::*;
    use crate::association::{AssociationKey, LaneAttachment};
    use crate::config::RemotingConfig;
    use crate::messaging::error::RemoteMessageError;
    use crate::messaging::target::{ExactActorTarget, SenderIdentity};
    use crate::protocol::{ProtocolDescriptor, ProtocolFingerprint};
    use async_trait::async_trait;
    use lattice_core::actor_ref::{
        ActivationId, ActorPath, ActorRef, ClusterId, NodeAddress, NodeIncarnation, ProtocolId,
    };

    struct EchoDispatch;

    #[async_trait]
    impl InboundDispatch for EchoDispatch {
        async fn tell(
            &self,
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
    async fn real_tcp_bidirectional_interactive_lane_completes_ask() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
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
                run_bidirectional_lane(
                    association,
                    LaneKind::Interactive,
                    2,
                    &mut server_receiver,
                    stream,
                    messaging,
                    Arc::new(EchoDispatch),
                    Arc::new(crate::control::RejectControlDispatch),
                    BidirectionalLaneConfig {
                        maximum_frame_size: 4096,
                        maximum_concurrent_inbound_asks: 8,
                        heartbeat_interval: Duration::from_millis(100),
                        heartbeat_miss_limit: 10,
                        idle_data_connection_timeout: Duration::from_secs(60),
                    },
                    &mut server_shutdown,
                )
                .await
            })
        };
        let stream = tokio::net::TcpStream::connect(socket).await.unwrap();
        let mut client_shutdown = shutdown_rx;
        let client_lane = {
            let association = client_association.clone();
            let messaging = client_messaging.clone();
            tokio::spawn(async move {
                run_bidirectional_lane(
                    association,
                    LaneKind::Interactive,
                    2,
                    &mut client_receiver,
                    stream,
                    messaging,
                    Arc::new(EchoDispatch),
                    Arc::new(crate::control::RejectControlDispatch),
                    BidirectionalLaneConfig {
                        maximum_frame_size: 4096,
                        maximum_concurrent_inbound_asks: 8,
                        heartbeat_interval: Duration::from_millis(100),
                        heartbeat_miss_limit: 10,
                        idle_data_connection_timeout: Duration::from_secs(60),
                    },
                    &mut client_shutdown,
                )
                .await
            })
        };
        let target = ActorRef::<()>::new(
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
                fingerprint,
                1,
                Bytes::from_static(b"echo"),
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
