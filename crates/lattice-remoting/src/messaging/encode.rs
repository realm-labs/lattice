use bytes::{BufMut, Bytes, BytesMut};
use lattice_core::actor_ref::{ActorPath, ActorRef, ProtocolTag};

use super::target::{CorrelationId, LogicalEntityTarget, LogicalSingletonTarget};
use super::{Frame, FrameKind};

#[derive(Debug, Clone)]
pub(super) struct PreparedExactTarget {
    encoded: Bytes,
}

impl PreparedExactTarget {
    pub(super) fn new<A: ProtocolTag>(target: &ActorRef<A>) -> Self {
        let mut encoded = BytesMut::with_capacity(exact_target_len(target));
        encode_exact_target(target, &mut encoded);
        Self {
            encoded: encoded.freeze(),
        }
    }

    fn len(&self) -> usize {
        self.encoded.len()
    }

    fn encode(&self, output: &mut impl BufMut) {
        output.put_slice(&self.encoded);
    }
}

pub(super) fn tell_frame<A: ProtocolTag>(
    target: &ActorRef<A>,
    sender_actor: Option<&ActorRef>,
    message_id: u64,
    payload: Bytes,
) -> Frame {
    let target_len = exact_target_len(target);
    let sender_len = sender_actor.map(exact_target_len);
    let encoded_len = nested_len(2, target_len)
        + varint_field_len(3, message_id)
        + bytes_field_len(4, &payload)
        + sender_len.map_or(0, |len| nested_len(5, len));
    encode_frame(FrameKind::Tell, encoded_len, |output| {
        encode_nested_prefix(2, target_len, output);
        encode_exact_target(target, output);
        encode_varint_field(3, message_id, output);
        encode_bytes_field(4, &payload, output);
        if let Some(sender) = sender_actor {
            encode_nested_prefix(5, sender_len.expect("sender length must exist"), output);
            encode_exact_target(sender, output);
        }
    })
}

pub(super) fn prepared_tell_frame(
    target: &PreparedExactTarget,
    sender_actor: Option<&PreparedExactTarget>,
    message_id: u64,
    payload: Bytes,
) -> Frame {
    let target_len = target.len();
    let sender_len = sender_actor.map(PreparedExactTarget::len);
    let encoded_len = nested_len(2, target_len)
        + varint_field_len(3, message_id)
        + bytes_field_len(4, &payload)
        + sender_len.map_or(0, |len| nested_len(5, len));
    encode_frame(FrameKind::Tell, encoded_len, |output| {
        encode_nested_prefix(2, target_len, output);
        target.encode(output);
        encode_varint_field(3, message_id, output);
        encode_bytes_field(4, &payload, output);
        if let Some(sender) = sender_actor {
            encode_nested_prefix(5, sender_len.expect("sender length must exist"), output);
            sender.encode(output);
        }
    })
}

pub(super) fn ask_frame<A: ProtocolTag>(
    target: &ActorRef<A>,
    correlation: CorrelationId,
    timeout_nanos: u64,
    message_id: u64,
    payload: Bytes,
) -> Frame {
    let target_len = exact_target_len(target);
    let correlation = correlation.to_bytes();
    let encoded_len = nested_len(2, target_len)
        + bytes_field_len(3, &correlation)
        + varint_field_len(4, timeout_nanos)
        + varint_field_len(5, message_id)
        + bytes_field_len(6, &payload);
    encode_frame(FrameKind::Ask, encoded_len, |output| {
        encode_nested_prefix(2, target_len, output);
        encode_exact_target(target, output);
        encode_bytes_field(3, &correlation, output);
        encode_varint_field(4, timeout_nanos, output);
        encode_varint_field(5, message_id, output);
        encode_bytes_field(6, &payload, output);
    })
}

pub(super) fn entity_tell_frame(
    target: &LogicalEntityTarget,
    sender_actor: Option<&ActorRef>,
    message_id: u64,
    payload: Bytes,
) -> Frame {
    let target_len = entity_target_len(target);
    let sender_len = sender_actor.map(exact_target_len);
    let encoded_len = nested_len(2, target_len)
        + varint_field_len(3, message_id)
        + bytes_field_len(4, &payload)
        + sender_len.map_or(0, |len| nested_len(5, len));
    encode_frame(FrameKind::EntityTell, encoded_len, |output| {
        encode_nested_prefix(2, target_len, output);
        encode_entity_target(target, output);
        encode_varint_field(3, message_id, output);
        encode_bytes_field(4, &payload, output);
        if let Some(sender) = sender_actor {
            encode_nested_prefix(5, sender_len.expect("sender length must exist"), output);
            encode_exact_target(sender, output);
        }
    })
}

pub(super) fn entity_ask_frame(
    target: &LogicalEntityTarget,
    correlation: CorrelationId,
    timeout_nanos: u64,
    message_id: u64,
    payload: Bytes,
) -> Frame {
    let target_len = entity_target_len(target);
    let correlation = correlation.to_bytes();
    let encoded_len = nested_len(2, target_len)
        + bytes_field_len(3, &correlation)
        + varint_field_len(4, timeout_nanos)
        + varint_field_len(5, message_id)
        + bytes_field_len(6, &payload);
    encode_frame(FrameKind::EntityAsk, encoded_len, |output| {
        encode_nested_prefix(2, target_len, output);
        encode_entity_target(target, output);
        encode_bytes_field(3, &correlation, output);
        encode_varint_field(4, timeout_nanos, output);
        encode_varint_field(5, message_id, output);
        encode_bytes_field(6, &payload, output);
    })
}

pub(super) fn singleton_tell_frame(
    target: &LogicalSingletonTarget,
    sender_actor: Option<&ActorRef>,
    message_id: u64,
    payload: Bytes,
) -> Frame {
    let target_len = singleton_target_len(target);
    let sender_len = sender_actor.map(exact_target_len);
    let encoded_len = nested_len(2, target_len)
        + varint_field_len(3, message_id)
        + bytes_field_len(4, &payload)
        + sender_len.map_or(0, |len| nested_len(5, len));
    encode_frame(FrameKind::SingletonTell, encoded_len, |output| {
        encode_nested_prefix(2, target_len, output);
        encode_singleton_target(target, output);
        encode_varint_field(3, message_id, output);
        encode_bytes_field(4, &payload, output);
        if let Some(sender) = sender_actor {
            encode_nested_prefix(5, sender_len.expect("sender length must exist"), output);
            encode_exact_target(sender, output);
        }
    })
}

pub(super) fn singleton_ask_frame(
    target: &LogicalSingletonTarget,
    correlation: CorrelationId,
    timeout_nanos: u64,
    message_id: u64,
    payload: Bytes,
) -> Frame {
    let target_len = singleton_target_len(target);
    let correlation = correlation.to_bytes();
    let encoded_len = nested_len(2, target_len)
        + bytes_field_len(3, &correlation)
        + varint_field_len(4, timeout_nanos)
        + varint_field_len(5, message_id)
        + bytes_field_len(6, &payload);
    encode_frame(FrameKind::SingletonAsk, encoded_len, |output| {
        encode_nested_prefix(2, target_len, output);
        encode_singleton_target(target, output);
        encode_bytes_field(3, &correlation, output);
        encode_varint_field(4, timeout_nanos, output);
        encode_varint_field(5, message_id, output);
        encode_bytes_field(6, &payload, output);
    })
}

pub(super) fn reply_frame(correlation: CorrelationId, payload: Bytes) -> Frame {
    let correlation = correlation.to_bytes();
    let encoded_len = bytes_field_len(1, &correlation) + bytes_field_len(2, &payload);
    encode_frame(FrameKind::Reply, encoded_len, |output| {
        encode_bytes_field(1, &correlation, output);
        encode_bytes_field(2, &payload, output);
    })
}

pub(super) fn failure_frame(correlation: CorrelationId, code: u32, safe_detail: &str) -> Frame {
    let correlation = correlation.to_bytes();
    let encoded_len = bytes_field_len(1, &correlation)
        + varint_field_len(2, u64::from(code))
        + string_field_len(3, safe_detail);
    encode_frame(FrameKind::Failure, encoded_len, |output| {
        encode_bytes_field(1, &correlation, output);
        encode_varint_field(2, u64::from(code), output);
        encode_string_field(3, safe_detail, output);
    })
}

fn encode_frame(kind: FrameKind, encoded_len: usize, encode: impl FnOnce(&mut BytesMut)) -> Frame {
    Frame::encode_payload(kind, encoded_len, encode)
}

fn exact_target_len<A: ProtocolTag>(target: &ActorRef<A>) -> usize {
    string_field_len(1, target.cluster_id().as_str())
        + string_field_len(2, target.node_address().host())
        + varint_field_len(3, u64::from(target.node_address().port()))
        + bytes_field_len(4, &target.node_incarnation().get().to_be_bytes())
        + actor_path_field_len(5, target.actor_path())
        + varint_field_len(6, target.activation_id().local_sequence())
        + varint_field_len(7, target.protocol_id().get())
}

fn encode_exact_target<A: ProtocolTag>(target: &ActorRef<A>, output: &mut impl BufMut) {
    encode_string_field(1, target.cluster_id().as_str(), output);
    encode_string_field(2, target.node_address().host(), output);
    encode_varint_field(3, u64::from(target.node_address().port()), output);
    encode_bytes_field(4, &target.node_incarnation().get().to_be_bytes(), output);
    encode_actor_path_field(5, target.actor_path(), output);
    encode_varint_field(6, target.activation_id().local_sequence(), output);
    encode_varint_field(7, target.protocol_id().get(), output);
}

fn entity_target_len(target: &LogicalEntityTarget) -> usize {
    string_field_len(1, target.reference.cluster_id().as_str())
        + string_field_len(2, target.owner_address.host())
        + varint_field_len(3, u64::from(target.owner_address.port()))
        + bytes_field_len(4, &target.owner_incarnation.get().to_be_bytes())
        + string_field_len(5, target.reference.entity_type().as_str())
        + bytes_field_len(6, target.reference.entity_id().as_bytes())
        + varint_field_len(7, target.reference.protocol_id().get())
        + bytes_field_len(8, target.reference.config_fingerprint().as_bytes())
        + varint_field_len(9, target.assignment_generation)
        + string_field_len(10, target.reference.domain().as_str())
}

fn encode_entity_target(target: &LogicalEntityTarget, output: &mut impl BufMut) {
    encode_string_field(1, target.reference.cluster_id().as_str(), output);
    encode_string_field(2, target.owner_address.host(), output);
    encode_varint_field(3, u64::from(target.owner_address.port()), output);
    encode_bytes_field(4, &target.owner_incarnation.get().to_be_bytes(), output);
    encode_string_field(5, target.reference.entity_type().as_str(), output);
    encode_bytes_field(6, target.reference.entity_id().as_bytes(), output);
    encode_varint_field(7, target.reference.protocol_id().get(), output);
    encode_bytes_field(8, target.reference.config_fingerprint().as_bytes(), output);
    encode_varint_field(9, target.assignment_generation, output);
    encode_string_field(10, target.reference.domain().as_str(), output);
}

fn singleton_target_len(target: &LogicalSingletonTarget) -> usize {
    string_field_len(1, target.reference.cluster_id().as_str())
        + string_field_len(2, target.owner_address.host())
        + varint_field_len(3, u64::from(target.owner_address.port()))
        + bytes_field_len(4, &target.owner_incarnation.get().to_be_bytes())
        + string_field_len(5, target.reference.singleton_kind().as_str())
        + varint_field_len(6, target.reference.protocol_id().get())
        + bytes_field_len(7, target.reference.config_fingerprint().as_bytes())
        + varint_field_len(8, target.assignment_generation)
        + string_field_len(9, target.reference.domain().as_str())
}

fn encode_singleton_target(target: &LogicalSingletonTarget, output: &mut impl BufMut) {
    encode_string_field(1, target.reference.cluster_id().as_str(), output);
    encode_string_field(2, target.owner_address.host(), output);
    encode_varint_field(3, u64::from(target.owner_address.port()), output);
    encode_bytes_field(4, &target.owner_incarnation.get().to_be_bytes(), output);
    encode_string_field(5, target.reference.singleton_kind().as_str(), output);
    encode_varint_field(6, target.reference.protocol_id().get(), output);
    encode_bytes_field(7, target.reference.config_fingerprint().as_bytes(), output);
    encode_varint_field(8, target.assignment_generation, output);
    encode_string_field(9, target.reference.domain().as_str(), output);
}

fn actor_path_len(path: &ActorPath) -> usize {
    path.segments().map(|segment| 1 + segment.len()).sum()
}

fn actor_path_field_len(tag: u32, path: &ActorPath) -> usize {
    length_delimited_field_len(tag, actor_path_len(path))
}

fn encode_actor_path_field(tag: u32, path: &ActorPath, output: &mut impl BufMut) {
    let len = actor_path_len(path);
    encode_length_delimited_prefix(tag, len, output);
    for segment in path.segments() {
        output.put_u8(b'/');
        output.put_slice(segment.as_bytes());
    }
}

fn nested_len(tag: u32, encoded_len: usize) -> usize {
    length_delimited_field_len(tag, encoded_len)
}

fn encode_nested_prefix(tag: u32, encoded_len: usize, output: &mut impl BufMut) {
    encode_length_delimited_prefix(tag, encoded_len, output);
}

fn string_field_len(tag: u32, value: &str) -> usize {
    if value.is_empty() {
        0
    } else {
        length_delimited_field_len(tag, value.len())
    }
}

fn encode_string_field(tag: u32, value: &str, output: &mut impl BufMut) {
    if value.is_empty() {
        return;
    }
    encode_length_delimited_prefix(tag, value.len(), output);
    output.put_slice(value.as_bytes());
}

fn bytes_field_len(tag: u32, value: &[u8]) -> usize {
    if value.is_empty() {
        0
    } else {
        length_delimited_field_len(tag, value.len())
    }
}

fn encode_bytes_field(tag: u32, value: &[u8], output: &mut impl BufMut) {
    if value.is_empty() {
        return;
    }
    encode_length_delimited_prefix(tag, value.len(), output);
    output.put_slice(value);
}

fn length_delimited_field_len(tag: u32, value_len: usize) -> usize {
    encoded_key_len(tag) + encoded_varint_len(value_len as u64) + value_len
}

fn encode_length_delimited_prefix(tag: u32, value_len: usize, output: &mut impl BufMut) {
    encode_key(tag, 2, output);
    encode_varint(value_len as u64, output);
}

fn varint_field_len(tag: u32, value: u64) -> usize {
    if value == 0 {
        0
    } else {
        encoded_key_len(tag) + encoded_varint_len(value)
    }
}

fn encode_varint_field(tag: u32, value: u64, output: &mut impl BufMut) {
    if value == 0 {
        return;
    }
    encode_key(tag, 0, output);
    encode_varint(value, output);
}

fn encode_key(tag: u32, wire_type: u32, output: &mut impl BufMut) {
    debug_assert!((1..(1 << 29)).contains(&tag));
    encode_varint(u64::from((tag << 3) | wire_type), output);
}

fn encoded_key_len(tag: u32) -> usize {
    encoded_varint_len(u64::from(tag << 3))
}

fn encode_varint(mut value: u64, output: &mut impl BufMut) {
    for _ in 0..10 {
        if value < 0x80 {
            output.put_u8(value as u8);
            return;
        }
        output.put_u8(((value & 0x7f) | 0x80) as u8);
        value >>= 7;
    }
}

const fn encoded_varint_len(value: u64) -> usize {
    let significant_bit = 63 - (value | 1).leading_zeros();
    ((significant_bit * 9 + 73) / 64) as usize
}

#[cfg(test)]
mod tests {
    use lattice_core::actor_ref::{
        ActivationId, ClusterId, ConfigFingerprint, EntityId, EntityRef, EntityType, NodeAddress,
        NodeIncarnation, PlacementDomainId, ProtocolId, SingletonKind, SingletonRef,
    };

    use super::*;
    use crate::messaging::codec::{
        AskWire, EntityAskWire, EntityTellWire, FailureWire, ReplyWire, SingletonAskWire,
        SingletonTellWire, TellWire, entity_target_to_wire, singleton_target_to_wire,
        target_to_wire,
    };

    fn actor(host: &str, incarnation: u128, sequence: u64) -> ActorRef {
        let incarnation = NodeIncarnation::new(incarnation).unwrap();
        ActorRef::new(
            ClusterId::new("test-cluster").unwrap(),
            NodeAddress::new(host, 25520).unwrap(),
            incarnation,
            ActorPath::user(["user", "玩家", "one"]).unwrap(),
            ActivationId::new(incarnation, sequence).unwrap(),
            ProtocolId::new(7).unwrap(),
        )
        .unwrap()
    }

    fn entity_target() -> LogicalEntityTarget {
        LogicalEntityTarget {
            reference: EntityRef::new(
                ClusterId::new("test-cluster").unwrap(),
                PlacementDomainId::new("world").unwrap(),
                EntityType::new("player").unwrap(),
                EntityId::new(b"player-1".to_vec()).unwrap(),
                ProtocolId::new(7).unwrap(),
                ConfigFingerprint::new([3; 32]),
            )
            .unwrap(),
            owner_address: NodeAddress::new("entity-owner", 25521).unwrap(),
            owner_incarnation: NodeIncarnation::new(12).unwrap(),
            assignment_generation: 9,
        }
    }

    fn singleton_target() -> LogicalSingletonTarget {
        LogicalSingletonTarget {
            reference: SingletonRef::new(
                ClusterId::new("test-cluster").unwrap(),
                PlacementDomainId::new("world").unwrap(),
                SingletonKind::new("ranking").unwrap(),
                ProtocolId::new(7).unwrap(),
                ConfigFingerprint::new([4; 32]),
            )
            .unwrap(),
            owner_address: NodeAddress::new("singleton-owner", 25522).unwrap(),
            owner_incarnation: NodeIncarnation::new(13).unwrap(),
            assignment_generation: 10,
        }
    }

    #[test]
    fn borrowed_exact_frames_match_prost_encoding() {
        let target = actor("target", 11, 1);
        let sender = actor("sender", 12, 2);
        let correlation = CorrelationId::new(17, 23).unwrap();
        let payload = Bytes::from_static(b"payload");

        assert_eq!(
            tell_frame(&target, Some(&sender), 31, payload.clone()),
            Frame::encode_message(
                FrameKind::Tell,
                &TellWire {
                    target: Some(target_to_wire(&target)),
                    message_id: 31,
                    payload: payload.clone(),
                    sender_actor: Some(target_to_wire(&sender)),
                },
            )
        );
        assert_eq!(
            prepared_tell_frame(
                &PreparedExactTarget::new(&target),
                Some(&PreparedExactTarget::new(&sender)),
                31,
                payload.clone(),
            ),
            tell_frame(&target, Some(&sender), 31, payload.clone())
        );
        assert_eq!(
            ask_frame(&target, correlation, 41, 31, payload.clone()),
            Frame::encode_message(
                FrameKind::Ask,
                &AskWire {
                    target: Some(target_to_wire(&target)),
                    correlation_id: Bytes::copy_from_slice(&correlation.to_bytes()),
                    timeout_nanos: 41,
                    message_id: 31,
                    payload,
                },
            )
        );
        assert_eq!(
            tell_frame(&target, None, 31, Bytes::new()),
            Frame::encode_message(
                FrameKind::Tell,
                &TellWire {
                    target: Some(target_to_wire(&target)),
                    message_id: 31,
                    payload: Bytes::new(),
                    sender_actor: None,
                },
            )
        );
    }

    #[test]
    fn borrowed_logical_and_completion_frames_match_prost_encoding() {
        let entity = entity_target();
        let singleton = singleton_target();
        let sender = actor("sender", 12, 2);
        let correlation = CorrelationId::new(17, 23).unwrap();
        let payload = Bytes::from_static(b"payload");

        assert_eq!(
            entity_tell_frame(&entity, Some(&sender), 31, payload.clone()),
            Frame::encode_message(
                FrameKind::EntityTell,
                &EntityTellWire {
                    target: Some(entity_target_to_wire(&entity)),
                    message_id: 31,
                    payload: payload.clone(),
                    sender_actor: Some(target_to_wire(&sender)),
                },
            )
        );
        assert_eq!(
            entity_ask_frame(&entity, correlation, 41, 31, payload.clone()),
            Frame::encode_message(
                FrameKind::EntityAsk,
                &EntityAskWire {
                    target: Some(entity_target_to_wire(&entity)),
                    correlation_id: Bytes::copy_from_slice(&correlation.to_bytes()),
                    timeout_nanos: 41,
                    message_id: 31,
                    payload: payload.clone(),
                },
            )
        );
        assert_eq!(
            singleton_tell_frame(&singleton, Some(&sender), 31, payload.clone()),
            Frame::encode_message(
                FrameKind::SingletonTell,
                &SingletonTellWire {
                    target: Some(singleton_target_to_wire(&singleton)),
                    message_id: 31,
                    payload: payload.clone(),
                    sender_actor: Some(target_to_wire(&sender)),
                },
            )
        );
        assert_eq!(
            singleton_ask_frame(&singleton, correlation, 41, 31, payload.clone()),
            Frame::encode_message(
                FrameKind::SingletonAsk,
                &SingletonAskWire {
                    target: Some(singleton_target_to_wire(&singleton)),
                    correlation_id: Bytes::copy_from_slice(&correlation.to_bytes()),
                    timeout_nanos: 41,
                    message_id: 31,
                    payload: payload.clone(),
                },
            )
        );
        assert_eq!(
            reply_frame(correlation, payload.clone()),
            Frame::encode_message(
                FrameKind::Reply,
                &ReplyWire {
                    correlation_id: Bytes::copy_from_slice(&correlation.to_bytes()),
                    payload,
                },
            )
        );
        assert_eq!(
            failure_frame(correlation, 5, "safe detail"),
            Frame::encode_message(
                FrameKind::Failure,
                &FailureWire {
                    correlation_id: Bytes::copy_from_slice(&correlation.to_bytes()),
                    code: 5,
                    safe_detail: "safe detail".to_owned(),
                },
            )
        );
    }
}
