//! Serial codec fixture; production connections use BidirectionalLane.
use super::codec::{
    decode_ask, decode_failure, decode_reply, decode_tell, failure_frame, reply_frame,
};
use super::error::{AskError, RemoteMessageError};
use super::inbound::{InboundDispatch, dispatch_tell, failure_code};
use super::outbound::OutboundMessaging;
use super::target::RemoteFailure;
use super::{Arc, Bytes, Error, Frame, FrameKind, Instant};
use crate::transport::{FramedConnection, RemotingIo};
use crate::wire::WireError;

#[derive(Debug, Error)]
pub(super) enum InboundConnectionError {
    #[error("inbound remoting socket failed")]
    Wire(#[from] WireError),
    #[error("inbound remoting message is invalid")]
    Message(#[from] RemoteMessageError),
    #[error("frame kind {0:?} is invalid on the actor data loop")]
    UnexpectedFrame(FrameKind),
}

pub(super) async fn serve_inbound_connection<S, D>(
    mut connection: FramedConnection<S>,
    dispatch: Arc<D>,
    outbound: Option<Arc<OutboundMessaging>>,
) -> Result<(), InboundConnectionError>
where
    S: RemotingIo,
    D: InboundDispatch + ?Sized,
{
    loop {
        let frame = connection.read_frame().await?;
        match frame.kind {
            FrameKind::Tell => {
                let tell = decode_tell(&frame)?;
                let _ = dispatch_tell(dispatch.as_ref(), tell).await;
            }
            FrameKind::Ask => {
                let ask = decode_ask(&frame)?;
                let deadline = Instant::now()
                    .checked_add(ask.timeout_budget)
                    .ok_or(RemoteMessageError::DeadlineExceeded)?;
                let response = match dispatch
                    .ask(ask.target, ask.message_id, ask.payload, deadline)
                    .await
                {
                    Ok(payload) => reply_frame(ask.correlation_id, payload),
                    Err(error) => failure_frame(&RemoteFailure {
                        correlation_id: ask.correlation_id,
                        code: failure_code(&error),
                        safe_detail: None,
                    }),
                };
                connection.write_frame(&response).await?;
            }
            FrameKind::Reply => {
                let (correlation, payload) = decode_reply(&frame)?;
                if let Some(outbound) = &outbound {
                    outbound.complete_reply(correlation, payload);
                }
            }
            FrameKind::Failure => {
                let failure = decode_failure(&frame)?;
                if let Some(outbound) = &outbound {
                    outbound
                        .complete_failure(failure.correlation_id, AskError::Remote(failure.code));
                }
            }
            FrameKind::Heartbeat => {
                connection
                    .write_frame(&Frame::new(FrameKind::HeartbeatAck, Bytes::new()))
                    .await?;
            }
            FrameKind::HeartbeatAck | FrameKind::Backpressure => {}
            FrameKind::Close => return Ok(()),
            _ => return Err(InboundConnectionError::UnexpectedFrame(frame.kind)),
        }
    }
}
