use lattice_core::{DirectLinkMessageId, LinkDirection, LinkId, LinkMessageFlags, LinkSequence};
use thiserror::Error;

use crate::session::{
    DirectLinkPeerIdentity, OpenLinkAck, OpenLinkEnvelope, OpenLinkReject, OpenLinkRequest,
};

const MAGIC: u32 = 0x4c44_4c4b;
const VERSION: u16 = 1;
const FIXED_HEADER_LEN: usize = 4 + 2 + 1 + 4 + 2 + 8 + 8 + 4 + 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DirectLinkFrameKind {
    OpenLink = 1,
    OpenLinkAck = 2,
    OpenLinkReject = 3,
    Message = 4,
    Heartbeat = 5,
    HeartbeatAck = 6,
    Backpressure = 7,
    CloseDirection = 8,
    Close = 9,
    ProtocolError = 10,
}

impl DirectLinkFrameKind {
    fn from_wire(value: u8) -> Result<Self, FrameCodecError> {
        match value {
            1 => Ok(Self::OpenLink),
            2 => Ok(Self::OpenLinkAck),
            3 => Ok(Self::OpenLinkReject),
            4 => Ok(Self::Message),
            5 => Ok(Self::Heartbeat),
            6 => Ok(Self::HeartbeatAck),
            7 => Ok(Self::Backpressure),
            8 => Ok(Self::CloseDirection),
            9 => Ok(Self::Close),
            10 => Ok(Self::ProtocolError),
            other => Err(FrameCodecError::UnknownFrameKind(other)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectLinkFrame {
    pub kind: DirectLinkFrameKind,
    pub link_id: LinkId,
    pub sequence: LinkSequence,
    pub message_id: Option<DirectLinkMessageId>,
    pub flags: LinkMessageFlags,
    pub header: Vec<u8>,
    pub payload: Vec<u8>,
}

impl DirectLinkFrame {
    pub fn message(
        link_id: LinkId,
        sequence: LinkSequence,
        message_id: DirectLinkMessageId,
        payload: Vec<u8>,
    ) -> Self {
        Self::directed_message(
            link_id,
            LinkDirection::SourceToTarget,
            sequence,
            message_id,
            payload,
        )
    }

    pub fn directed_message(
        link_id: LinkId,
        direction: LinkDirection,
        sequence: LinkSequence,
        message_id: DirectLinkMessageId,
        payload: Vec<u8>,
    ) -> Self {
        Self::directed_message_with_header(
            link_id,
            direction,
            sequence,
            message_id,
            Vec::new(),
            payload,
        )
    }

    pub fn directed_message_with_header(
        link_id: LinkId,
        direction: LinkDirection,
        sequence: LinkSequence,
        message_id: DirectLinkMessageId,
        header: Vec<u8>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            kind: DirectLinkFrameKind::Message,
            link_id,
            sequence,
            message_id: Some(message_id),
            flags: flags_for_direction(direction),
            header,
            payload,
        }
    }

    pub fn heartbeat(link_id: LinkId) -> Self {
        Self::control(DirectLinkFrameKind::Heartbeat, link_id)
    }

    pub fn heartbeat_ack(link_id: LinkId) -> Self {
        Self::control(DirectLinkFrameKind::HeartbeatAck, link_id)
    }

    pub fn open_link(request: &OpenLinkRequest) -> Result<Self, FrameCodecError> {
        Self::open_link_envelope(&OpenLinkEnvelope::new(request.clone()))
    }

    pub fn open_link_with_peer_identity(
        request: &OpenLinkRequest,
        peer_identity: DirectLinkPeerIdentity,
    ) -> Result<Self, FrameCodecError> {
        Self::open_link_envelope(&OpenLinkEnvelope::with_peer_identity(
            request.clone(),
            peer_identity,
        ))
    }

    pub fn open_link_envelope(envelope: &OpenLinkEnvelope) -> Result<Self, FrameCodecError> {
        Ok(Self {
            kind: DirectLinkFrameKind::OpenLink,
            link_id: envelope.request.link_id.clone(),
            sequence: LinkSequence(0),
            message_id: None,
            flags: LinkMessageFlags::EMPTY,
            header: Vec::new(),
            payload: serde_json::to_vec(envelope)
                .map_err(|error| FrameCodecError::HandshakePayload(error.to_string()))?,
        })
    }

    pub fn decode_open_link(&self) -> Result<OpenLinkRequest, FrameCodecError> {
        Ok(self.decode_open_link_envelope()?.request)
    }

    pub fn decode_open_link_envelope(&self) -> Result<OpenLinkEnvelope, FrameCodecError> {
        if self.kind != DirectLinkFrameKind::OpenLink {
            return Err(FrameCodecError::UnexpectedFrameKind {
                expected: DirectLinkFrameKind::OpenLink,
                actual: self.kind,
            });
        }
        serde_json::from_slice::<OpenLinkPayload>(&self.payload)
            .map(OpenLinkPayload::into_envelope)
            .map_err(|error| FrameCodecError::HandshakePayload(error.to_string()))
    }

    pub fn open_link_ack(ack: &OpenLinkAck) -> Result<Self, FrameCodecError> {
        Ok(Self {
            kind: DirectLinkFrameKind::OpenLinkAck,
            link_id: ack.link_id.clone(),
            sequence: LinkSequence(0),
            message_id: None,
            flags: LinkMessageFlags::EMPTY,
            header: Vec::new(),
            payload: serde_json::to_vec(ack)
                .map_err(|error| FrameCodecError::HandshakePayload(error.to_string()))?,
        })
    }

    pub fn decode_open_link_ack(&self) -> Result<OpenLinkAck, FrameCodecError> {
        self.decode_handshake_payload(DirectLinkFrameKind::OpenLinkAck)
    }

    pub fn open_link_reject(reject: &OpenLinkReject) -> Result<Self, FrameCodecError> {
        Ok(Self {
            kind: DirectLinkFrameKind::OpenLinkReject,
            link_id: reject.link_id.clone(),
            sequence: LinkSequence(0),
            message_id: None,
            flags: LinkMessageFlags::EMPTY,
            header: Vec::new(),
            payload: serde_json::to_vec(reject)
                .map_err(|error| FrameCodecError::HandshakePayload(error.to_string()))?,
        })
    }

    pub fn decode_open_link_reject(&self) -> Result<OpenLinkReject, FrameCodecError> {
        self.decode_handshake_payload(DirectLinkFrameKind::OpenLinkReject)
    }

    fn control(kind: DirectLinkFrameKind, link_id: LinkId) -> Self {
        Self {
            kind,
            link_id,
            sequence: LinkSequence(0),
            message_id: None,
            flags: LinkMessageFlags::EMPTY,
            header: Vec::new(),
            payload: Vec::new(),
        }
    }

    pub fn direction(&self) -> LinkDirection {
        direction_from_flags(&self.flags)
    }

    fn decode_handshake_payload<T>(
        &self,
        expected_kind: DirectLinkFrameKind,
    ) -> Result<T, FrameCodecError>
    where
        T: serde::de::DeserializeOwned,
    {
        if self.kind != expected_kind {
            return Err(FrameCodecError::UnexpectedFrameKind {
                expected: expected_kind,
                actual: self.kind,
            });
        }
        serde_json::from_slice(&self.payload)
            .map_err(|error| FrameCodecError::HandshakePayload(error.to_string()))
    }
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum OpenLinkPayload {
    Envelope(OpenLinkEnvelope),
    Request(OpenLinkRequest),
}

impl OpenLinkPayload {
    fn into_envelope(self) -> OpenLinkEnvelope {
        match self {
            Self::Envelope(envelope) => envelope,
            Self::Request(request) => OpenLinkEnvelope::new(request),
        }
    }
}

fn flags_for_direction(direction: LinkDirection) -> LinkMessageFlags {
    match direction {
        LinkDirection::SourceToTarget => LinkMessageFlags::EMPTY,
        LinkDirection::TargetToSource => LinkMessageFlags::from_bits(0b1),
    }
}

fn direction_from_flags(flags: &LinkMessageFlags) -> LinkDirection {
    if flags.bits() & 0b1 == 0 {
        LinkDirection::SourceToTarget
    } else {
        LinkDirection::TargetToSource
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DirectLinkFrameCodec {
    max_frame_size: usize,
}

impl DirectLinkFrameCodec {
    pub fn new(max_frame_size: usize) -> Self {
        Self { max_frame_size }
    }

    pub fn encode(&self, frame: &DirectLinkFrame) -> Result<Vec<u8>, FrameCodecError> {
        let link_id = frame.link_id.as_str().as_bytes();
        let link_len = u16::try_from(link_id.len()).map_err(|_| FrameCodecError::LinkIdTooLarge)?;
        let header_len =
            u32::try_from(frame.header.len()).map_err(|_| FrameCodecError::FrameTooLarge)?;
        let payload_len =
            u32::try_from(frame.payload.len()).map_err(|_| FrameCodecError::FrameTooLarge)?;
        let total_len = FIXED_HEADER_LEN
            .checked_add(usize::from(link_len))
            .and_then(|value| value.checked_add(frame.header.len()))
            .and_then(|value| value.checked_add(frame.payload.len()))
            .ok_or(FrameCodecError::FrameTooLarge)?;
        self.check_frame_size(total_len)?;

        let mut output = Vec::with_capacity(total_len);
        output.extend_from_slice(&MAGIC.to_be_bytes());
        output.extend_from_slice(&VERSION.to_be_bytes());
        output.push(frame.kind as u8);
        output.extend_from_slice(&frame.flags.bits().to_be_bytes());
        output.extend_from_slice(&link_len.to_be_bytes());
        output.extend_from_slice(&frame.sequence.0.to_be_bytes());
        output.extend_from_slice(&frame.message_id.map(|id| id.0).unwrap_or(0).to_be_bytes());
        output.extend_from_slice(&header_len.to_be_bytes());
        output.extend_from_slice(&payload_len.to_be_bytes());
        output.extend_from_slice(link_id);
        output.extend_from_slice(&frame.header);
        output.extend_from_slice(&frame.payload);
        Ok(output)
    }

    pub fn decode(&self, bytes: &[u8]) -> Result<DirectLinkFrame, FrameCodecError> {
        self.check_frame_size(bytes.len())?;
        if bytes.len() < FIXED_HEADER_LEN {
            return Err(FrameCodecError::Truncated);
        }
        let mut cursor = Cursor::new(bytes);
        let magic = cursor.u32()?;
        if magic != MAGIC {
            return Err(FrameCodecError::BadMagic);
        }
        let version = cursor.u16()?;
        if version != VERSION {
            return Err(FrameCodecError::UnsupportedVersion(version));
        }
        let kind = DirectLinkFrameKind::from_wire(cursor.u8()?)?;
        let flags = LinkMessageFlags::from_bits(cursor.u32()?);
        let link_len = usize::from(cursor.u16()?);
        let sequence = LinkSequence(cursor.u64()?);
        let raw_message_id = cursor.u64()?;
        let header_len = cursor.u32()? as usize;
        let payload_len = cursor.u32()? as usize;
        let expected_len = FIXED_HEADER_LEN
            .checked_add(link_len)
            .and_then(|value| value.checked_add(header_len))
            .and_then(|value| value.checked_add(payload_len))
            .ok_or(FrameCodecError::FrameTooLarge)?;
        if bytes.len() != expected_len {
            return Err(FrameCodecError::LengthMismatch {
                expected: expected_len,
                actual: bytes.len(),
            });
        }
        let link_id = cursor.bytes(link_len)?;
        let header = cursor.bytes(header_len)?.to_vec();
        let payload = cursor.bytes(payload_len)?.to_vec();
        let link_id = std::str::from_utf8(link_id)
            .map_err(|_| FrameCodecError::InvalidLinkId)?
            .to_string();

        Ok(DirectLinkFrame {
            kind,
            link_id: LinkId::new(link_id),
            sequence,
            message_id: (raw_message_id != 0).then_some(DirectLinkMessageId(raw_message_id)),
            flags,
            header,
            payload,
        })
    }

    pub(crate) fn check_frame_size(&self, size: usize) -> Result<(), FrameCodecError> {
        if self.max_frame_size != 0 && size > self.max_frame_size {
            return Err(FrameCodecError::FrameTooLarge);
        }
        Ok(())
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FrameCodecError {
    #[error("direct link frame is truncated")]
    Truncated,
    #[error("direct link frame has invalid magic")]
    BadMagic,
    #[error("unsupported direct link frame version {0}")]
    UnsupportedVersion(u16),
    #[error("unknown direct link frame kind {0}")]
    UnknownFrameKind(u8),
    #[error("direct link frame exceeds maximum size")]
    FrameTooLarge,
    #[error("direct link id is too large to encode")]
    LinkIdTooLarge,
    #[error("direct link frame length mismatch: expected {expected}, actual {actual}")]
    LengthMismatch { expected: usize, actual: usize },
    #[error("direct link frame contains an invalid link id")]
    InvalidLinkId,
    #[error("unexpected direct link frame kind: expected {expected:?}, actual {actual:?}")]
    UnexpectedFrameKind {
        expected: DirectLinkFrameKind,
        actual: DirectLinkFrameKind,
    },
    #[error("direct link handshake payload error: {0}")]
    HandshakePayload(String),
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn u8(&mut self) -> Result<u8, FrameCodecError> {
        Ok(*self.bytes(1)?.first().ok_or(FrameCodecError::Truncated)?)
    }

    fn u16(&mut self) -> Result<u16, FrameCodecError> {
        let bytes: [u8; 2] = self
            .bytes(2)?
            .try_into()
            .map_err(|_| FrameCodecError::Truncated)?;
        Ok(u16::from_be_bytes(bytes))
    }

    fn u32(&mut self) -> Result<u32, FrameCodecError> {
        let bytes: [u8; 4] = self
            .bytes(4)?
            .try_into()
            .map_err(|_| FrameCodecError::Truncated)?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64, FrameCodecError> {
        let bytes: [u8; 8] = self
            .bytes(8)?
            .try_into()
            .map_err(|_| FrameCodecError::Truncated)?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn bytes(&mut self, len: usize) -> Result<&'a [u8], FrameCodecError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(FrameCodecError::FrameTooLarge)?;
        let slice = self
            .bytes
            .get(self.offset..end)
            .ok_or(FrameCodecError::Truncated)?;
        self.offset = end;
        Ok(slice)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    use lattice_core::{
        ActorId, ActorKind, ActorRef, BackpressurePolicy, DirectLinkMode, DirectLinkOptions,
        InstanceId, ServiceKind,
    };

    use crate::session::{
        DIRECT_LINK_PROTOCOL_VERSION, NegotiatedDirection, OpenLinkDirection, OpenLinkRejectReason,
    };

    #[test]
    fn frame_codec_round_trips_message_frame() {
        let codec = DirectLinkFrameCodec::new(1024);
        let frame = DirectLinkFrame::message(
            LinkId::new("link-1"),
            LinkSequence(7),
            DirectLinkMessageId(42),
            b"payload".to_vec(),
        );

        let encoded = codec.encode(&frame).unwrap();
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(decoded, frame);
    }

    #[test]
    fn frame_codec_round_trips_target_to_source_message_frame() {
        let codec = DirectLinkFrameCodec::new(1024);
        let frame = DirectLinkFrame::directed_message(
            LinkId::new("link-1"),
            LinkDirection::TargetToSource,
            LinkSequence(7),
            DirectLinkMessageId(42),
            b"payload".to_vec(),
        );

        let encoded = codec.encode(&frame).unwrap();
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(decoded, frame);
        assert_eq!(decoded.direction(), LinkDirection::TargetToSource);
    }

    #[test]
    fn frame_codec_rejects_oversized_frames() {
        let codec = DirectLinkFrameCodec::new(8);
        let frame = DirectLinkFrame::message(
            LinkId::new("link-1"),
            LinkSequence(7),
            DirectLinkMessageId(42),
            b"payload".to_vec(),
        );

        assert_eq!(codec.encode(&frame), Err(FrameCodecError::FrameTooLarge));
    }

    #[test]
    fn frame_codec_round_trips_open_link_handshake_frames() {
        let codec = DirectLinkFrameCodec::new(4096);
        let link_id = LinkId::new("link-open");
        let message_id = DirectLinkMessageId(11);
        let request = OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: test_actor_ref("Gateway", "GatewaySession", 99),
            target: test_actor_ref("World", "World", 7),
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection {
                link_id: link_id.clone(),
                stream_name: "movement".to_string(),
                supported_message_type_ids: BTreeSet::from([message_id]),
            },
            target_to_source: None,
            options: DirectLinkOptions::default(),
        };

        let open_frame = DirectLinkFrame::open_link(&request).unwrap();
        let decoded_open_frame = codec.decode(&codec.encode(&open_frame).unwrap()).unwrap();
        let decoded_request = decoded_open_frame.decode_open_link().unwrap();
        assert_eq!(decoded_request.link_id, request.link_id);
        assert_eq!(decoded_request.source, request.source);
        assert_eq!(decoded_request.target, request.target);
        assert_eq!(
            decoded_request.source_to_target.supported_message_type_ids,
            BTreeSet::from([message_id])
        );

        let peer_identity = DirectLinkPeerIdentity::new(
            ServiceKind::new("Gateway"),
            InstanceId::new("instance-99"),
            "spiffe://lattice.test/svc/Gateway/instance/instance-99",
        );
        let authenticated_open_frame =
            DirectLinkFrame::open_link_with_peer_identity(&request, peer_identity.clone()).unwrap();
        let decoded_authenticated_frame = codec
            .decode(&codec.encode(&authenticated_open_frame).unwrap())
            .unwrap();
        let decoded_envelope = decoded_authenticated_frame
            .decode_open_link_envelope()
            .unwrap();
        assert_eq!(decoded_envelope.request.source, request.source);
        assert_eq!(decoded_envelope.peer_identity, Some(peer_identity));

        let ack = OpenLinkAck {
            link_id: link_id.clone(),
            source_to_target: NegotiatedDirection {
                direction: LinkDirection::SourceToTarget,
                stream_name: "movement".to_string(),
                accepted_message_type_ids: BTreeSet::from([message_id]),
                next_receive_sequence: LinkSequence(1),
                backpressure: BackpressurePolicy::FailFast { max_pending: 8 },
                closed: false,
            },
            target_to_source: None,
        };
        let ack_frame = DirectLinkFrame::open_link_ack(&ack).unwrap();
        let decoded_ack_frame = codec.decode(&codec.encode(&ack_frame).unwrap()).unwrap();
        assert_eq!(decoded_ack_frame.decode_open_link_ack().unwrap(), ack);

        let reject = OpenLinkReject::new(link_id, OpenLinkRejectReason::Unauthorized);
        let reject_frame = DirectLinkFrame::open_link_reject(&reject).unwrap();
        let decoded_reject_frame = codec.decode(&codec.encode(&reject_frame).unwrap()).unwrap();
        assert_eq!(
            decoded_reject_frame.decode_open_link_reject().unwrap(),
            reject
        );
    }

    fn test_actor_ref(service: &str, actor: &str, id: u64) -> ActorRef {
        ActorRef::direct(
            ServiceKind::new(service),
            ActorKind::new(actor),
            ActorId::U64(id),
            InstanceId::new("codec-test"),
            "tcp://127.0.0.1:1".parse().unwrap(),
            None,
        )
    }
}
