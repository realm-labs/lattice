use std::io::Error as IoError;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;
use thiserror::Error;

pub const TRANSPORT_MAJOR: u16 = 1;
pub const TRANSPORT_MINOR: u16 = 2;
const HEADER_LEN: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum FrameKind {
    Handshake = 1,
    HandshakeAck = 2,
    Heartbeat = 3,
    HeartbeatAck = 4,
    Tell = 5,
    Ask = 6,
    Reply = 7,
    Failure = 8,
    Watch = 9,
    WatchAck = 10,
    Unwatch = 11,
    Terminated = 12,
    CoordinatorRequest = 13,
    CoordinatorReply = 14,
    CoordinatorEvent = 15,
    ControlEnvelope = 16,
    ControlAck = 17,
    ProtocolCatalogue = 18,
    Backpressure = 19,
    Close = 20,
    ProtocolError = 21,
    EntityTell = 22,
    EntityAsk = 23,
    SingletonTell = 24,
    SingletonAsk = 25,
    BootstrapRequest = 26,
    BootstrapResponse = 27,
}

impl TryFrom<u16> for FrameKind {
    type Error = WireError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Handshake),
            2 => Ok(Self::HandshakeAck),
            3 => Ok(Self::Heartbeat),
            4 => Ok(Self::HeartbeatAck),
            5 => Ok(Self::Tell),
            6 => Ok(Self::Ask),
            7 => Ok(Self::Reply),
            8 => Ok(Self::Failure),
            9 => Ok(Self::Watch),
            10 => Ok(Self::WatchAck),
            11 => Ok(Self::Unwatch),
            12 => Ok(Self::Terminated),
            13 => Ok(Self::CoordinatorRequest),
            14 => Ok(Self::CoordinatorReply),
            15 => Ok(Self::CoordinatorEvent),
            16 => Ok(Self::ControlEnvelope),
            17 => Ok(Self::ControlAck),
            18 => Ok(Self::ProtocolCatalogue),
            19 => Ok(Self::Backpressure),
            20 => Ok(Self::Close),
            21 => Ok(Self::ProtocolError),
            22 => Ok(Self::EntityTell),
            23 => Ok(Self::EntityAsk),
            24 => Ok(Self::SingletonTell),
            25 => Ok(Self::SingletonAsk),
            26 => Ok(Self::BootstrapRequest),
            27 => Ok(Self::BootstrapResponse),
            _ => Err(WireError::UnknownFrameKind(value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub kind: FrameKind,
    pub payload: Bytes,
}

impl Frame {
    pub fn encode_message<M: Message>(kind: FrameKind, message: &M) -> Self {
        Self {
            kind,
            payload: Bytes::from(message.encode_to_vec()),
        }
    }

    pub fn decode_message<M: Message + Default>(&self) -> Result<M, WireError> {
        M::decode(self.payload.clone()).map_err(WireError::Decode)
    }
}

#[derive(Debug, Clone)]
pub struct FrameCodec {
    max_frame_size: usize,
}

impl FrameCodec {
    pub fn new(max_frame_size: usize) -> Result<Self, WireError> {
        if max_frame_size < HEADER_LEN {
            return Err(WireError::InvalidFrameLimit(max_frame_size));
        }
        Ok(Self { max_frame_size })
    }

    pub fn max_frame_size(&self) -> usize {
        self.max_frame_size
    }

    pub fn encode(&self, frame: &Frame) -> Result<Bytes, WireError> {
        let frame_len =
            HEADER_LEN
                .checked_add(frame.payload.len())
                .ok_or(WireError::FrameTooLarge {
                    actual: usize::MAX,
                    maximum: self.max_frame_size,
                })?;
        if frame_len > self.max_frame_size || frame_len > u32::MAX as usize {
            return Err(WireError::FrameTooLarge {
                actual: frame_len,
                maximum: self.max_frame_size,
            });
        }
        let mut output = BytesMut::with_capacity(4 + frame_len);
        output.put_u32(frame_len as u32);
        output.put_u16(TRANSPORT_MAJOR);
        output.put_u16(TRANSPORT_MINOR);
        output.put_u16(frame.kind as u16);
        output.put_u16(0);
        output.extend_from_slice(&frame.payload);
        Ok(output.freeze())
    }

    pub fn decode(&self, mut input: Bytes) -> Result<Frame, WireError> {
        if input.len() < 4 {
            return Err(WireError::Truncated);
        }
        let frame_len = input.get_u32() as usize;
        if frame_len > self.max_frame_size {
            return Err(WireError::FrameTooLarge {
                actual: frame_len,
                maximum: self.max_frame_size,
            });
        }
        if frame_len != input.len() || frame_len < HEADER_LEN {
            return Err(WireError::InvalidLength {
                declared: frame_len,
                actual: input.len(),
            });
        }
        let major = input.get_u16();
        let minor = input.get_u16();
        if major != TRANSPORT_MAJOR || minor > TRANSPORT_MINOR {
            return Err(WireError::UnsupportedVersion { major, minor });
        }
        let kind = FrameKind::try_from(input.get_u16())?;
        let reserved = input.get_u16();
        if reserved != 0 {
            return Err(WireError::ReservedBits(reserved));
        }
        Ok(Frame {
            kind,
            payload: input,
        })
    }
}

#[derive(Debug, Error)]
pub enum WireError {
    #[error("frame limit {0} is too small")]
    InvalidFrameLimit(usize),
    #[error("frame size {actual} exceeds maximum {maximum}")]
    FrameTooLarge { actual: usize, maximum: usize },
    #[error("frame is truncated")]
    Truncated,
    #[error("declared frame length {declared} does not match {actual} available bytes")]
    InvalidLength { declared: usize, actual: usize },
    #[error("unsupported transport version {major}.{minor}")]
    UnsupportedVersion { major: u16, minor: u16 },
    #[error("unknown frame kind {0}")]
    UnknownFrameKind(u16),
    #[error("reserved frame bits are nonzero: {0:#x}")]
    ReservedBits(u16),
    #[error("protobuf payload decode failed")]
    Decode(#[source] prost::DecodeError),
    #[error("I/O failed")]
    Io(#[from] IoError),
    #[error("TLS transport validation failed: {0}")]
    Tls(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_oversized_length_before_payload_decode() {
        let codec = FrameCodec::new(256).unwrap();
        let mut bytes = BytesMut::new();
        bytes.put_u32(257);
        assert!(matches!(
            codec.decode(bytes.freeze()),
            Err(WireError::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn frame_round_trips_without_json() {
        let codec = FrameCodec::new(1024).unwrap();
        let frame = Frame {
            kind: FrameKind::Tell,
            payload: Bytes::from_static(b"opaque"),
        };
        assert_eq!(codec.decode(codec.encode(&frame).unwrap()).unwrap(), frame);
    }
}
