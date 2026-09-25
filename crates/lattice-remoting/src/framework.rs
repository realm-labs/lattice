//! A small, stable identity envelope for connection setup, before any peer payload is interpreted.
//! Business frames do not carry this envelope: their association has already passed admission.

use lattice_model::framework::LatticeVersion;
use prost::Message;

use crate::wire::{Frame, FrameKind, WireError};

#[derive(Clone, PartialEq, Message)]
struct FrameworkEnvelope {
    #[prost(string, tag = "1")]
    identity: String,
    #[prost(bytes = "vec", tag = "2")]
    payload: Vec<u8>,
}

pub(crate) fn encode_framework_frame<M: Message>(kind: FrameKind, message: &M) -> Frame {
    Frame::encode_message(
        kind,
        &FrameworkEnvelope {
            identity: LatticeVersion::CURRENT.to_owned(),
            payload: message.encode_to_vec(),
        },
    )
}

pub(crate) fn decode_framework_frame<M: Message + Default>(frame: &Frame) -> Result<M, WireError> {
    let envelope = frame.decode_message::<FrameworkEnvelope>()?;
    if envelope.identity != LatticeVersion::CURRENT {
        return Err(WireError::FrameworkMismatch {
            expected: LatticeVersion::CURRENT,
            actual: envelope.identity,
        });
    }
    M::decode(envelope.payload.as_slice()).map_err(WireError::Decode)
}

#[cfg(test)]
mod tests {
    use super::{FrameworkEnvelope, decode_framework_frame, encode_framework_frame};
    use crate::{
        bootstrap::{BootstrapRequest, BootstrapResponse},
        handshake::{Handshake, HandshakeAck},
        wire::{Frame, FrameKind, WireError},
    };

    #[test]
    fn foreign_identity_is_rejected_before_decoding_the_setup_payload() {
        for kind in [
            FrameKind::Handshake,
            FrameKind::HandshakeAck,
            FrameKind::BootstrapRequest,
            FrameKind::BootstrapResponse,
        ] {
            let frame = Frame::encode_message(
                kind,
                &FrameworkEnvelope {
                    identity: "foreign-build".to_owned(),
                    payload: vec![0xff],
                },
            );
            assert!(matches!(
                decode_framework_frame::<FrameworkEnvelope>(&frame),
                Err(WireError::FrameworkMismatch { .. })
            ));
            match kind {
                FrameKind::Handshake => assert!(Handshake::from_frame(&frame).is_err()),
                FrameKind::HandshakeAck => assert!(HandshakeAck::from_frame(&frame).is_err()),
                FrameKind::BootstrapRequest => {
                    assert!(BootstrapRequest::from_frame(&frame).is_err())
                }
                FrameKind::BootstrapResponse => {
                    assert!(BootstrapResponse::from_frame(&frame).is_err())
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn matching_identity_allows_the_current_payload() {
        let message = FrameworkEnvelope {
            identity: "payload".to_owned(),
            payload: vec![1, 2],
        };
        let frame = encode_framework_frame(FrameKind::Handshake, &message);
        assert_eq!(
            decode_framework_frame::<FrameworkEnvelope>(&frame).unwrap(),
            message
        );
    }

    #[test]
    fn a_missing_identity_is_not_a_compatibility_fallback() {
        let frame = Frame::encode_message(
            FrameKind::Handshake,
            &FrameworkEnvelope {
                identity: String::new(),
                payload: Vec::new(),
            },
        );
        assert!(matches!(
            decode_framework_frame::<FrameworkEnvelope>(&frame),
            Err(WireError::FrameworkMismatch { .. })
        ));
    }
}
