pub mod association;
pub mod config;
pub mod control;
pub mod endpoint;
pub mod handshake;
pub mod lane;
pub mod messaging;
pub mod protocol;
pub mod transport;
pub mod watch;
pub mod wire;

pub use association::{
    Association, AssociationId, AssociationKey, AssociationManager, AssociationState,
    LaneAttachment, LaneKind,
};
pub use config::{RemotingConfig, RemotingConfigError};
pub use control::{
    CommandId, ControlAck, ControlApply, ControlDispatch, ControlDispatchError, ControlEnvelope,
    ControlGap, RejectControlDispatch, ReliableControl, control_ack_frame, control_envelope_frame,
    decode_control_ack, decode_control_envelope,
};
pub use endpoint::{EndpointError, EndpointSecurity, RemotingEndpoint};
pub use handshake::{
    FeatureBits, Handshake, HandshakeAck, HandshakeError, HandshakeValidator, NodeIdentity,
};
pub use lane::{BidirectionalLaneConfig, LaneExit, run_bidirectional_lane};
pub use messaging::{
    AskError, CorrelationId, ExactActorTarget, InboundAsk, InboundConnectionError, InboundDispatch,
    InboundEntityAsk, InboundEntityTell, InboundSingletonAsk, InboundSingletonTell, InboundTell,
    LogicalEntityTarget, LogicalSingletonTarget, OutboundMessaging, RemoteFailure,
    RemoteFailureCode, RemoteMessageError, SenderIdentity, TellError, ask_correlation, decode_ask,
    decode_entity_ask, decode_entity_tell, decode_failure, decode_reply, decode_singleton_ask,
    decode_singleton_tell, decode_tell, failure_frame, reply_frame, serve_inbound_connection,
};
pub use protocol::{
    CatalogueDecision, ProtocolCatalogue, ProtocolDescriptor, ProtocolFingerprint, catalogue_frame,
    decode_catalogue_frame,
};
pub use transport::{
    FramedReader, FramedWriter, NegotiationError, negotiate_inbound, negotiate_outbound,
};
pub use watch::{
    CurrentActivationResolver, TerminatedReason, WatchCommand, WatchError, WatchId, WatchRegistry,
    WatchStatus, decode_watch_command, encode_watch_command, is_watch_control,
};
pub use wire::{Frame, FrameCodec, FrameKind, WireError};
