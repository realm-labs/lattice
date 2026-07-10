use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use lattice_core::direct_link::errors::LinkError;
use lattice_core::direct_link::ids::LinkId;
use lattice_core::direct_link::options::LinkCloseReason;
use tokio::sync::{mpsc, oneshot};

use crate::endpoint_pool::{DirectLinkConnectionId, PooledDirectLinkEndpointPoolInner};
use crate::protocol::{DirectLinkFrame, DirectLinkFrameKind};
use crate::transport::{DirectLinkConnection, DirectLinkTransport};

pub(crate) enum ConnectionCommand {
    Write {
        frame: DirectLinkFrame,
        completion: Option<oneshot::Sender<Result<(), LinkError>>>,
    },
    WriteAndRead {
        frame: DirectLinkFrame,
        response: oneshot::Sender<Result<DirectLinkFrame, LinkError>>,
    },
}

impl fmt::Debug for ConnectionCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Write { frame, completion } => formatter
                .debug_struct("Write")
                .field("frame", frame)
                .field("expects_completion", &completion.is_some())
                .finish(),
            Self::WriteAndRead { frame, .. } => formatter
                .debug_struct("WriteAndRead")
                .field("frame", frame)
                .finish(),
        }
    }
}

pub(crate) fn spawn_connection_task<T>(
    mut connection: T::Connection,
    pool: Arc<PooledDirectLinkEndpointPoolInner<T>>,
    connection_id: DirectLinkConnectionId,
) -> mpsc::Sender<ConnectionCommand>
where
    T: DirectLinkTransport,
{
    let (tx, mut rx) = mpsc::channel(1024);
    tokio::spawn(async move {
        let mut pending_responses: HashMap<
            LinkId,
            oneshot::Sender<Result<DirectLinkFrame, LinkError>>,
        > = HashMap::new();
        let mut read_enabled = false;
        loop {
            tokio::select! {
                biased;
                command = rx.recv() => {
                    let Some(command) = command else {
                        break;
                    };
                    match command {
                        ConnectionCommand::Write { frame, completion } => {
                    let write_result = connection.write_frame(frame).await;
                    if let Err(error) = write_result {
                        if let Some(completion) = completion {
                            let _ = completion.send(Err(error));
                        }
                        break;
                    }
                    pool.metrics.record_frame_written(connection_id);
                    if let Some(completion) = completion {
                        let _ = completion.send(Ok(()));
                    }
                    read_enabled = true;
                        }
                        ConnectionCommand::WriteAndRead { frame, response } => {
                    let link_id = frame.link_id.clone();
                    let write_result = connection.write_frame(frame).await;
                    if let Err(error) = write_result {
                        let _ = response.send(Err(error));
                        break;
                    }
                    pool.metrics.record_frame_written(connection_id);
                    pending_responses.insert(link_id, response);
                    read_enabled = true;
                        }
                    }
                },
                frame = connection.read_frame(), if read_enabled => {
                    match frame {
                        Ok(frame) => {
                            let should_break = handle_connection_frame(
                                &mut connection,
                                &pool,
                                connection_id,
                                frame,
                                &mut pending_responses,
                            )
                            .await;
                            if should_break {
                                break;
                            }
                        }
                        Err(error) => {
                            for (_, response) in pending_responses.drain() {
                                let _ = response.send(Err(LinkError::Protocol(error.to_string())));
                            }
                            break;
                        }
                    }
                },
            }
        }
        let _ = connection.close().await;
        pool.remove_connection(connection_id).await;
    });
    tx
}

async fn handle_connection_frame<T>(
    connection: &mut T::Connection,
    pool: &Arc<PooledDirectLinkEndpointPoolInner<T>>,
    _connection_id: DirectLinkConnectionId,
    frame: DirectLinkFrame,
    pending_responses: &mut HashMap<LinkId, oneshot::Sender<Result<DirectLinkFrame, LinkError>>>,
) -> bool
where
    T: DirectLinkTransport,
{
    if matches!(
        frame.kind,
        DirectLinkFrameKind::OpenLinkAck | DirectLinkFrameKind::OpenLinkReject
    ) {
        if let Some(response) = pending_responses.remove(&frame.link_id) {
            let _ = response.send(Ok(frame));
            return false;
        }
        return true;
    }

    match frame.kind {
        DirectLinkFrameKind::ProtocolError => {
            let reason = String::from_utf8(frame.payload.clone())
                .unwrap_or_else(|_| "remote protocol error".to_string());
            if let Some(response) = pending_responses.remove(&frame.link_id) {
                let _ = response.send(Err(LinkError::Protocol(reason.clone())));
                pool.close_connection(_connection_id, LinkCloseReason::ProtocolError(reason))
                    .await;
                return true;
            }
            pool.process_protocol_error_frame(frame).await.is_err()
        }
        DirectLinkFrameKind::Close | DirectLinkFrameKind::CloseDirection => {
            let reason = frame.decode_close_reason();
            match frame.kind {
                DirectLinkFrameKind::CloseDirection => {
                    pool.close_logical_direction(&frame.link_id, frame.direction(), reason)
                        .await;
                }
                DirectLinkFrameKind::Close => {
                    pool.close_logical_link(&frame.link_id, reason).await;
                }
                _ => {}
            }
            false
        }
        DirectLinkFrameKind::Heartbeat => connection
            .write_frame(DirectLinkFrame::heartbeat_ack(frame.link_id))
            .await
            .is_err(),
        DirectLinkFrameKind::Message => pool.process_message_frame(frame).await.is_err(),
        _ => false,
    }
}

pub(crate) async fn send_frame(
    writer: &mpsc::Sender<ConnectionCommand>,
    frame: DirectLinkFrame,
) -> Result<(), LinkError> {
    let (tx, rx) = oneshot::channel();
    writer
        .send(ConnectionCommand::Write {
            frame,
            completion: Some(tx),
        })
        .await
        .map_err(|_| LinkError::Protocol("direct link pooled writer is closed".to_string()))?;
    rx.await
        .map_err(|_| LinkError::Protocol("direct link pooled writer stopped".to_string()))?
}

pub(crate) async fn send_frame_for_response(
    writer: &mpsc::Sender<ConnectionCommand>,
    frame: DirectLinkFrame,
) -> Result<DirectLinkFrame, LinkError> {
    let (tx, rx) = oneshot::channel();
    writer
        .send(ConnectionCommand::WriteAndRead {
            frame,
            response: tx,
        })
        .await
        .map_err(|_| LinkError::Protocol("direct link pooled writer is closed".to_string()))?;
    rx.await
        .map_err(|_| LinkError::Protocol("direct link pooled writer stopped".to_string()))?
}
