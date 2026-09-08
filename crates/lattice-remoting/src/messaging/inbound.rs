use super::error::{RemoteFailureCode, RemoteMessageError};
use super::target::{ExactActorTarget, InboundTell, LogicalEntityTarget, LogicalSingletonTarget};
use super::{Bytes, Instant, async_trait};

#[async_trait]
pub trait InboundDispatch: Send + Sync + 'static {
    fn try_tell_immediate(&self, tell: InboundTell) -> ImmediateTellDispatch {
        ImmediateTellDispatch::Deferred(tell)
    }

    async fn tell(
        &self,
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

pub enum ImmediateTellDispatch {
    Complete(Result<(), RemoteMessageError>),
    Deferred(InboundTell),
}

pub(crate) async fn dispatch_tell<D: InboundDispatch + ?Sized>(
    dispatch: &D,
    tell: InboundTell,
) -> Result<(), RemoteMessageError> {
    match dispatch.try_tell_immediate(tell) {
        ImmediateTellDispatch::Complete(result) => result,
        ImmediateTellDispatch::Deferred(tell) => {
            dispatch
                .tell(tell.target, tell.message_id, tell.payload)
                .await
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
