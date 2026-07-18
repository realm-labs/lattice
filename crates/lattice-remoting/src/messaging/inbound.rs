use super::codec::{
    decode_ask, decode_failure, decode_reply, decode_tell, failure_frame, reply_frame,
};
use super::error::{AskError, InboundConnectionError, RemoteFailureCode, RemoteMessageError};
use super::outbound::OutboundMessaging;
use super::target::{ExactActorTarget, LogicalEntityTarget, LogicalSingletonTarget, RemoteFailure};
use super::{
    ActorRef, Arc, Bytes, Frame, FrameKind, FramedConnection, Instant, RemotingIo, async_trait,
};

#[async_trait]
pub trait InboundDispatch: Send + Sync + 'static {
    async fn tell(
        &self,
        sender: Option<ActorRef>,
        target: ExactActorTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError>;

    async fn ask(
        &self,
        target: ExactActorTarget,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError>;

    async fn tell_entity(
        &self,
        _sender: Option<ActorRef>,
        _target: LogicalEntityTarget,
        _message_id: u64,
        _payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }

    async fn ask_entity(
        &self,
        _target: LogicalEntityTarget,
        _message_id: u64,
        _payload: Bytes,
        _deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }

    async fn tell_singleton(
        &self,
        _sender: Option<ActorRef>,
        _target: LogicalSingletonTarget,
        _message_id: u64,
        _payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }

    async fn ask_singleton(
        &self,
        _target: LogicalSingletonTarget,
        _message_id: u64,
        _payload: Bytes,
        _deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }
}

pub async fn serve_inbound_connection<S, D>(
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
                let _ = dispatch
                    .tell(tell.sender, tell.target, tell.message_id, tell.payload)
                    .await;
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
                    .write_frame(&Frame {
                        kind: FrameKind::HeartbeatAck,
                        payload: Bytes::new(),
                    })
                    .await?;
            }
            FrameKind::HeartbeatAck | FrameKind::Backpressure => {}
            FrameKind::Close => return Ok(()),
            _ => return Err(InboundConnectionError::UnexpectedFrame(frame.kind)),
        }
    }
}

pub(crate) fn failure_code(error: &RemoteMessageError) -> RemoteFailureCode {
    match error {
        RemoteMessageError::StaleActivation => RemoteFailureCode::StaleActivation,
        RemoteMessageError::StaleAuthority => RemoteFailureCode::StaleActivation,
        RemoteMessageError::UnsupportedProtocol | RemoteMessageError::UnknownMessage => {
            RemoteFailureCode::UnknownMessage
        }
        RemoteMessageError::ProtocolFingerprintMismatch => RemoteFailureCode::ProtocolMismatch,
        RemoteMessageError::MailboxRejected => RemoteFailureCode::MailboxFull,
        RemoteMessageError::BufferFull => RemoteFailureCode::MailboxFull,
        RemoteMessageError::InvalidPayload => RemoteFailureCode::DecodeFailed,
        RemoteMessageError::DeadlineExceeded => RemoteFailureCode::DeadlineExceeded,
        RemoteMessageError::Unauthorized => RemoteFailureCode::Unauthorized,
        RemoteMessageError::ActorPanicked => RemoteFailureCode::ActorPanicked,
        RemoteMessageError::ShardUnavailable
        | RemoteMessageError::ZeroPendingLimit
        | RemoteMessageError::HandlerFailed => RemoteFailureCode::HandlerFailed,
    }
}
