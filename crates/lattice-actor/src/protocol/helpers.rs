use std::collections::BTreeMap;

use crate::observation::ProtocolFailure;

use super::{ClientBinding, CodecDescriptor, DispatchError, DispatchMode, ProtocolId};

pub(super) fn bounded_error(mut message: String) -> String {
    message.truncate(256);
    message
}

pub(super) fn canonical_descriptor(
    protocol_id: ProtocolId,
    name: &str,
    bindings: &BTreeMap<u64, ClientBinding>,
) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(&protocol_id.get().to_be_bytes());
    output.extend_from_slice(&(name.len() as u32).to_be_bytes());
    output.extend_from_slice(name.as_bytes());
    output.extend_from_slice(&(bindings.len() as u32).to_be_bytes());
    for descriptor in bindings.values().map(|binding| &binding.descriptor) {
        output.extend_from_slice(&descriptor.message_id.to_be_bytes());
        output.push(match descriptor.mode {
            DispatchMode::Tell => 0,
            DispatchMode::Ask => 1,
        });
        let response_codec = descriptor
            .response_codec
            .unwrap_or(CodecDescriptor::new(0, 0));
        for value in [
            descriptor.request_codec.id,
            u64::from(descriptor.request_codec.version),
            u64::from(descriptor.request_schema_version),
            response_codec.id,
            u64::from(response_codec.version),
            u64::from(descriptor.response_schema_version.unwrap_or(0)),
            descriptor.max_payload as u64,
        ] {
            output.extend_from_slice(&value.to_be_bytes());
        }
    }
    output
}

pub(super) fn protocol_failure(error: &DispatchError) -> ProtocolFailure {
    match error {
        DispatchError::UnregisteredType | DispatchError::UnknownMessage(_) => {
            ProtocolFailure::UnknownMessage
        }
        DispatchError::ModeMismatch => ProtocolFailure::ModeMismatch,
        DispatchError::PayloadTooLarge { .. } => ProtocolFailure::PayloadTooLarge,
        DispatchError::Decode(_) => ProtocolFailure::DecodeFailed,
        DispatchError::Encode(_) => ProtocolFailure::EncodeFailed,
        DispatchError::MissingDeadline => ProtocolFailure::MissingDeadline,
        DispatchError::MailboxRejected => ProtocolFailure::MailboxRejected,
        DispatchError::Actor(_) => ProtocolFailure::ActorFailed,
        DispatchError::ReplyTypeMismatch => ProtocolFailure::ReplyTypeMismatch,
    }
}
