use lattice_core::{DirectLinkMessageId, LinkId, LinkMessageFlags, LinkSequence};
use thiserror::Error;

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
        Self {
            kind: DirectLinkFrameKind::Message,
            link_id,
            sequence,
            message_id: Some(message_id),
            flags: LinkMessageFlags::EMPTY,
            header: Vec::new(),
            payload,
        }
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

    fn check_frame_size(&self, size: usize) -> Result<(), FrameCodecError> {
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
}
