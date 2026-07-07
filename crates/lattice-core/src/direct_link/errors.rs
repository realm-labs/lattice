use thiserror::Error;

use crate::ActorRef;

use super::{DirectLinkMessageId, LinkCloseReason};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LinkMetadataError {
    #[error("direct-link metadata bytes were provided for a unit metadata stream")]
    UnexpectedMetadata,
    #[error("failed to decode direct-link metadata: {0}")]
    Decode(String),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LinkSendError {
    #[error("direct link is closed: {reason:?}")]
    Closed { reason: LinkCloseReason },
    #[error("direct link backpressure queue is full")]
    BackpressureFull,
    #[error("message type is not supported by this direct link stream")]
    UnsupportedMessageType,
    #[error("encoded message is larger than the negotiated direct link frame size")]
    MessageTooLarge,
    #[error("failed to encode direct link message: {0}")]
    Encode(String),
    #[error("failed to encode direct link metadata: {0}")]
    EncodeMetadata(String),
    #[error("direct link protocol error: {0}")]
    Protocol(String),
}

#[derive(Debug, Error)]
pub enum LinkError {
    #[error("direct link runtime is not configured")]
    Unavailable,
    #[error("actor context has no source ActorRef")]
    MissingSourceActor,
    #[error("direct link stream {stream_name} has duplicate message id {message_id:?}")]
    DuplicateMessageId {
        stream_name: String,
        message_id: DirectLinkMessageId,
    },
    #[error("target is not the current direct link owner")]
    NotOwner { redirect: Option<Box<ActorRef>> },
    #[error("target owner epoch is fenced")]
    Fenced,
    #[error("target actor is unavailable")]
    ActorUnavailable,
    #[error("direct link stream is unsupported")]
    UnsupportedStream,
    #[error("direct link message type is unsupported")]
    UnsupportedMessageType,
    #[error("direct link is unauthorized")]
    Unauthorized,
    #[error("target is overloaded")]
    Overloaded,
    #[error("direct link protocol version is unsupported")]
    ProtocolVersionMismatch,
    #[error("direct link protocol error: {0}")]
    Protocol(String),
}
