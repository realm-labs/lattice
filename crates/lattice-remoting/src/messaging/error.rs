use super::{AssociationError, Error, FrameKind};
use crate::wire::WireError;

#[derive(Debug, Error)]
pub enum InboundConnectionError {
    #[error("inbound remoting socket failed")]
    Wire(#[from] WireError),
    #[error("inbound remoting message is invalid")]
    Message(#[from] RemoteMessageError),
    #[error("frame kind {0:?} is invalid on the actor data loop")]
    UnexpectedFrame(FrameKind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum RemoteFailureCode {
    StaleActivation = 1,
    UnknownMessage = 2,
    DecodeFailed = 3,
    MailboxFull = 4,
    MailboxClosed = 5,
    Unauthorized = 6,
    DeadlineExceeded = 7,
    HandlerFailed = 8,
    ProtocolMismatch = 9,
    Internal = 10,
    ActorPanicked = 11,
}

impl TryFrom<u32> for RemoteFailureCode {
    type Error = ();

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::StaleActivation),
            2 => Ok(Self::UnknownMessage),
            3 => Ok(Self::DecodeFailed),
            4 => Ok(Self::MailboxFull),
            5 => Ok(Self::MailboxClosed),
            6 => Ok(Self::Unauthorized),
            7 => Ok(Self::DeadlineExceeded),
            8 => Ok(Self::HandlerFailed),
            9 => Ok(Self::ProtocolMismatch),
            10 => Ok(Self::Internal),
            11 => Ok(Self::ActorPanicked),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Error)]
pub enum TellError {
    #[error("remote actor protocol is unavailable")]
    Protocol(#[source] RemoteMessageError),
    #[error("association rejected tell admission")]
    Association(#[source] AssociationError),
    #[error("remote or logical target rejected tell")]
    Remote(#[source] RemoteMessageError),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AskError {
    #[error("ask deadline exceeded")]
    DeadlineExceeded,
    #[error("pending ask limit reached")]
    PendingLimit,
    #[error("ask correlation sequence exhausted")]
    CorrelationExhausted,
    #[error("association failed before socket write commitment")]
    AssociationLostBeforeWrite,
    #[error("ask result is unknown after socket write commitment")]
    UnknownResult,
    #[error("remote actor protocol is unavailable")]
    Protocol(RemoteMessageError),
    #[error("association rejected ask admission: {0}")]
    Association(String),
    #[error("remote execution failed with code {0:?}")]
    Remote(RemoteFailureCode),
}

impl From<AssociationError> for AskError {
    fn from(value: AssociationError) -> Self {
        Self::Association(value.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RemoteMessageError {
    #[error("pending ask limit must be nonzero")]
    ZeroPendingLimit,
    #[error("peer does not support the actor protocol")]
    UnsupportedProtocol,
    #[error("peer actor protocol fingerprint differs")]
    ProtocolFingerprintMismatch,
    #[error("actor protocol does not register the message ID")]
    UnknownMessage,
    #[error("target actor activation is stale or absent")]
    StaleActivation,
    #[error("logical target owner or assignment generation is stale")]
    StaleAuthority,
    #[error("logical target has no currently authorized owner")]
    ShardUnavailable,
    #[error("logical routing buffer reached its configured bound")]
    BufferFull,
    #[error("target actor mailbox rejected the message")]
    MailboxRejected,
    #[error("message payload is invalid")]
    InvalidPayload,
    #[error("message deadline elapsed")]
    DeadlineExceeded,
    #[error("message is unauthorized")]
    Unauthorized,
    #[error("remote handler failed")]
    HandlerFailed,
    #[error("remote actor panicked")]
    ActorPanicked,
}

#[cfg(test)]
mod tests {
    use super::RemoteFailureCode;

    #[test]
    fn actor_panicked_failure_code_is_stable() {
        assert_eq!(RemoteFailureCode::ActorPanicked as u16, 11);
        assert_eq!(
            RemoteFailureCode::try_from(11),
            Ok(RemoteFailureCode::ActorPanicked)
        );
    }
}
