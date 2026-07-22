use super::error::{RemoteFailureCode, RemoteMessageError};
use super::target::{
    CorrelationId, ExactActorTarget, InboundAsk, InboundEntityAsk, InboundEntityTell,
    InboundSingletonAsk, InboundSingletonTell, InboundTell, LogicalEntityTarget,
    LogicalSingletonTarget, RemoteFailure,
};
use super::target_cache::ExactTargetCache;
use super::target_dictionary::ExactTargetDictionary;
use super::{
    ActivationId, ActorPath, ActorRef, Bytes, ClusterId, ConfigFingerprint, Duration, EntityId,
    EntityRef, EntityType, Frame, FrameKind, Message, NodeAddress, NodeIncarnation,
    PlacementDomainId, ProtocolId, SingletonKind, SingletonRef,
};

pub fn ask_correlation(frame: &Frame) -> Option<CorrelationId> {
    let bytes = match frame.kind {
        FrameKind::Ask => frame.decode_message::<AskWire>().ok()?.correlation_id,
        FrameKind::EntityAsk => frame.decode_message::<EntityAskWire>().ok()?.correlation_id,
        FrameKind::SingletonAsk => {
            frame
                .decode_message::<SingletonAskWire>()
                .ok()?
                .correlation_id
        }
        _ => return None,
    };
    CorrelationId::from_bytes(&bytes)
}

pub fn decode_tell(frame: &Frame) -> Result<InboundTell, RemoteMessageError> {
    decode_tell_inner(frame, None, None)
}

pub(crate) fn decode_tell_cached(
    frame: &Frame,
    cache: &mut ExactTargetCache,
    dictionary: &mut ExactTargetDictionary,
) -> Result<InboundTell, RemoteMessageError> {
    decode_tell_inner(frame, Some(cache), Some(dictionary))
}

fn decode_tell_inner(
    frame: &Frame,
    mut cache: Option<&mut ExactTargetCache>,
    dictionary: Option<&mut ExactTargetDictionary>,
) -> Result<InboundTell, RemoteMessageError> {
    if frame.kind != FrameKind::Tell {
        return Err(RemoteMessageError::InvalidPayload);
    }
    let wire = frame
        .decode_message::<TellWire>()
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    if wire.message_id == 0 {
        return Err(RemoteMessageError::InvalidPayload);
    }
    let sender = decode_sender_with_cache(wire.sender_actor, cache.as_deref_mut())?;
    let target = match (wire.target_id, wire.target, cache, dictionary) {
        (0, Some(target), Some(cache), _) => cache.resolve(target)?,
        (0, Some(target), None, _) => target_from_wire(target)?,
        (0, None, _, _) => return Err(RemoteMessageError::InvalidPayload),
        (id, target, Some(cache), Some(dictionary)) => dictionary.resolve(id, target, cache)?,
        (_, Some(target), Some(cache), None) => cache.resolve(target)?,
        (_, Some(target), None, _) => target_from_wire(target)?,
        (_, None, _, _) => return Err(RemoteMessageError::InvalidPayload),
    };
    Ok(InboundTell {
        sender,
        target,
        message_id: wire.message_id,
        payload: wire.payload,
    })
}

pub fn decode_ask(frame: &Frame) -> Result<InboundAsk, RemoteMessageError> {
    decode_ask_inner(frame, None)
}

pub(crate) fn decode_ask_cached(
    frame: &Frame,
    cache: &mut ExactTargetCache,
) -> Result<InboundAsk, RemoteMessageError> {
    decode_ask_inner(frame, Some(cache))
}

fn decode_ask_inner(
    frame: &Frame,
    cache: Option<&mut ExactTargetCache>,
) -> Result<InboundAsk, RemoteMessageError> {
    if frame.kind != FrameKind::Ask {
        return Err(RemoteMessageError::InvalidPayload);
    }
    let wire = frame
        .decode_message::<AskWire>()
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    let correlation_id = CorrelationId::from_bytes(&wire.correlation_id)
        .ok_or(RemoteMessageError::InvalidPayload)?;
    if wire.message_id == 0 || wire.timeout_nanos == 0 {
        return Err(RemoteMessageError::InvalidPayload);
    }
    let target_wire = wire.target.ok_or(RemoteMessageError::InvalidPayload)?;
    Ok(InboundAsk {
        target: if let Some(cache) = cache {
            cache.resolve(target_wire)?
        } else {
            target_from_wire(target_wire)?
        },
        correlation_id,
        timeout_budget: Duration::from_nanos(wire.timeout_nanos),
        message_id: wire.message_id,
        payload: wire.payload,
    })
}

pub fn decode_entity_tell(frame: &Frame) -> Result<InboundEntityTell, RemoteMessageError> {
    decode_entity_tell_inner(frame, None)
}

pub(crate) fn decode_entity_tell_cached(
    frame: &Frame,
    cache: &mut ExactTargetCache,
) -> Result<InboundEntityTell, RemoteMessageError> {
    decode_entity_tell_inner(frame, Some(cache))
}

fn decode_entity_tell_inner(
    frame: &Frame,
    cache: Option<&mut ExactTargetCache>,
) -> Result<InboundEntityTell, RemoteMessageError> {
    if frame.kind != FrameKind::EntityTell {
        return Err(RemoteMessageError::InvalidPayload);
    }
    let wire = frame
        .decode_message::<EntityTellWire>()
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    if wire.message_id == 0 {
        return Err(RemoteMessageError::InvalidPayload);
    }
    Ok(InboundEntityTell {
        sender: decode_sender_with_cache(wire.sender_actor, cache)?,
        target: entity_target_from_wire(wire.target.ok_or(RemoteMessageError::InvalidPayload)?)?,
        message_id: wire.message_id,
        payload: wire.payload,
    })
}

pub fn decode_entity_ask(frame: &Frame) -> Result<InboundEntityAsk, RemoteMessageError> {
    if frame.kind != FrameKind::EntityAsk {
        return Err(RemoteMessageError::InvalidPayload);
    }
    let wire = frame
        .decode_message::<EntityAskWire>()
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    if wire.message_id == 0 || wire.timeout_nanos == 0 {
        return Err(RemoteMessageError::InvalidPayload);
    }
    Ok(InboundEntityAsk {
        target: entity_target_from_wire(wire.target.ok_or(RemoteMessageError::InvalidPayload)?)?,
        correlation_id: CorrelationId::from_bytes(&wire.correlation_id)
            .ok_or(RemoteMessageError::InvalidPayload)?,
        timeout_budget: Duration::from_nanos(wire.timeout_nanos),
        message_id: wire.message_id,
        payload: wire.payload,
    })
}

pub fn decode_singleton_tell(frame: &Frame) -> Result<InboundSingletonTell, RemoteMessageError> {
    decode_singleton_tell_inner(frame, None)
}

pub(crate) fn decode_singleton_tell_cached(
    frame: &Frame,
    cache: &mut ExactTargetCache,
) -> Result<InboundSingletonTell, RemoteMessageError> {
    decode_singleton_tell_inner(frame, Some(cache))
}

fn decode_singleton_tell_inner(
    frame: &Frame,
    cache: Option<&mut ExactTargetCache>,
) -> Result<InboundSingletonTell, RemoteMessageError> {
    if frame.kind != FrameKind::SingletonTell {
        return Err(RemoteMessageError::InvalidPayload);
    }
    let wire = frame
        .decode_message::<SingletonTellWire>()
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    if wire.message_id == 0 {
        return Err(RemoteMessageError::InvalidPayload);
    }
    Ok(InboundSingletonTell {
        sender: decode_sender_with_cache(wire.sender_actor, cache)?,
        target: singleton_target_from_wire(wire.target.ok_or(RemoteMessageError::InvalidPayload)?)?,
        message_id: wire.message_id,
        payload: wire.payload,
    })
}

pub fn decode_singleton_ask(frame: &Frame) -> Result<InboundSingletonAsk, RemoteMessageError> {
    if frame.kind != FrameKind::SingletonAsk {
        return Err(RemoteMessageError::InvalidPayload);
    }
    let wire = frame
        .decode_message::<SingletonAskWire>()
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    if wire.message_id == 0 || wire.timeout_nanos == 0 {
        return Err(RemoteMessageError::InvalidPayload);
    }
    Ok(InboundSingletonAsk {
        target: singleton_target_from_wire(wire.target.ok_or(RemoteMessageError::InvalidPayload)?)?,
        correlation_id: CorrelationId::from_bytes(&wire.correlation_id)
            .ok_or(RemoteMessageError::InvalidPayload)?,
        timeout_budget: Duration::from_nanos(wire.timeout_nanos),
        message_id: wire.message_id,
        payload: wire.payload,
    })
}

pub fn reply_frame(correlation_id: CorrelationId, payload: Bytes) -> Frame {
    super::encode::reply_frame(correlation_id, payload)
}

pub fn decode_reply(frame: &Frame) -> Result<(CorrelationId, Bytes), RemoteMessageError> {
    if frame.kind != FrameKind::Reply {
        return Err(RemoteMessageError::InvalidPayload);
    }
    let reply = frame
        .decode_message::<ReplyWire>()
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    Ok((
        CorrelationId::from_bytes(&reply.correlation_id)
            .ok_or(RemoteMessageError::InvalidPayload)?,
        reply.payload,
    ))
}

pub fn failure_frame(failure: &RemoteFailure) -> Frame {
    let detail = failure.safe_detail.as_deref().unwrap_or("");
    let detail = detail
        .char_indices()
        .nth(256)
        .map_or(detail, |(end, _)| &detail[..end]);
    super::encode::failure_frame(failure.correlation_id, failure.code as u32, detail)
}

pub fn decode_failure(frame: &Frame) -> Result<RemoteFailure, RemoteMessageError> {
    if frame.kind != FrameKind::Failure {
        return Err(RemoteMessageError::InvalidPayload);
    }
    let failure = frame
        .decode_message::<FailureWire>()
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    let code = RemoteFailureCode::try_from(failure.code)
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    if failure.safe_detail.len() > 256 {
        return Err(RemoteMessageError::InvalidPayload);
    }
    Ok(RemoteFailure {
        correlation_id: CorrelationId::from_bytes(&failure.correlation_id)
            .ok_or(RemoteMessageError::InvalidPayload)?,
        code,
        safe_detail: (!failure.safe_detail.is_empty()).then_some(failure.safe_detail),
    })
}

#[derive(Clone, PartialEq, Eq, Hash, Message)]
pub(super) struct ExactActorTargetWire {
    #[prost(bytes = "bytes", tag = "1")]
    pub(super) cluster_id: Bytes,
    #[prost(bytes = "bytes", tag = "2")]
    pub(super) host: Bytes,
    #[prost(uint32, tag = "3")]
    pub(super) port: u32,
    #[prost(bytes = "bytes", tag = "4")]
    pub(super) node_incarnation: Bytes,
    #[prost(bytes = "bytes", tag = "5")]
    pub(super) actor_path: Bytes,
    #[prost(uint64, tag = "6")]
    pub(super) activation_sequence: u64,
    #[prost(uint64, tag = "7")]
    pub(super) protocol_id: u64,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct TellWire {
    #[prost(message, optional, tag = "2")]
    pub(super) target: Option<ExactActorTargetWire>,
    #[prost(uint64, tag = "3")]
    pub(super) message_id: u64,
    #[prost(bytes = "bytes", tag = "4")]
    pub(super) payload: Bytes,
    #[prost(message, optional, tag = "5")]
    pub(super) sender_actor: Option<ExactActorTargetWire>,
    #[prost(uint64, tag = "6")]
    pub(super) target_id: u64,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct AskWire {
    #[prost(message, optional, tag = "2")]
    pub(super) target: Option<ExactActorTargetWire>,
    #[prost(bytes = "bytes", tag = "3")]
    pub(super) correlation_id: Bytes,
    #[prost(uint64, tag = "4")]
    pub(super) timeout_nanos: u64,
    #[prost(uint64, tag = "5")]
    pub(super) message_id: u64,
    #[prost(bytes = "bytes", tag = "6")]
    pub(super) payload: Bytes,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct EntityTargetWire {
    #[prost(string, tag = "1")]
    pub(super) cluster_id: String,
    #[prost(string, tag = "2")]
    pub(super) owner_host: String,
    #[prost(uint32, tag = "3")]
    pub(super) owner_port: u32,
    #[prost(bytes = "bytes", tag = "4")]
    pub(super) owner_incarnation: Bytes,
    #[prost(string, tag = "5")]
    pub(super) entity_type: String,
    #[prost(bytes = "bytes", tag = "6")]
    pub(super) entity_id: Bytes,
    #[prost(uint64, tag = "7")]
    pub(super) protocol_id: u64,
    #[prost(bytes = "bytes", tag = "8")]
    pub(super) config_fingerprint: Bytes,
    #[prost(uint64, tag = "9")]
    pub(super) assignment_generation: u64,
    #[prost(string, tag = "10")]
    pub(super) domain: String,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct EntityTellWire {
    #[prost(message, optional, tag = "2")]
    pub(super) target: Option<EntityTargetWire>,
    #[prost(uint64, tag = "3")]
    pub(super) message_id: u64,
    #[prost(bytes = "bytes", tag = "4")]
    pub(super) payload: Bytes,
    #[prost(message, optional, tag = "5")]
    pub(super) sender_actor: Option<ExactActorTargetWire>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct EntityAskWire {
    #[prost(message, optional, tag = "2")]
    pub(super) target: Option<EntityTargetWire>,
    #[prost(bytes = "bytes", tag = "3")]
    pub(super) correlation_id: Bytes,
    #[prost(uint64, tag = "4")]
    pub(super) timeout_nanos: u64,
    #[prost(uint64, tag = "5")]
    pub(super) message_id: u64,
    #[prost(bytes = "bytes", tag = "6")]
    pub(super) payload: Bytes,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct SingletonTargetWire {
    #[prost(string, tag = "1")]
    pub(super) cluster_id: String,
    #[prost(string, tag = "2")]
    pub(super) owner_host: String,
    #[prost(uint32, tag = "3")]
    pub(super) owner_port: u32,
    #[prost(bytes = "bytes", tag = "4")]
    pub(super) owner_incarnation: Bytes,
    #[prost(string, tag = "5")]
    pub(super) singleton_kind: String,
    #[prost(uint64, tag = "6")]
    pub(super) protocol_id: u64,
    #[prost(bytes = "bytes", tag = "7")]
    pub(super) config_fingerprint: Bytes,
    #[prost(uint64, tag = "8")]
    pub(super) assignment_generation: u64,
    #[prost(string, tag = "9")]
    pub(super) domain: String,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct SingletonTellWire {
    #[prost(message, optional, tag = "2")]
    pub(super) target: Option<SingletonTargetWire>,
    #[prost(uint64, tag = "3")]
    pub(super) message_id: u64,
    #[prost(bytes = "bytes", tag = "4")]
    pub(super) payload: Bytes,
    #[prost(message, optional, tag = "5")]
    pub(super) sender_actor: Option<ExactActorTargetWire>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct SingletonAskWire {
    #[prost(message, optional, tag = "2")]
    pub(super) target: Option<SingletonTargetWire>,
    #[prost(bytes = "bytes", tag = "3")]
    pub(super) correlation_id: Bytes,
    #[prost(uint64, tag = "4")]
    pub(super) timeout_nanos: u64,
    #[prost(uint64, tag = "5")]
    pub(super) message_id: u64,
    #[prost(bytes = "bytes", tag = "6")]
    pub(super) payload: Bytes,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct ReplyWire {
    #[prost(bytes = "bytes", tag = "1")]
    pub(super) correlation_id: Bytes,
    #[prost(bytes = "bytes", tag = "2")]
    pub(super) payload: Bytes,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct FailureWire {
    #[prost(bytes = "bytes", tag = "1")]
    pub(super) correlation_id: Bytes,
    #[prost(uint32, tag = "2")]
    pub(super) code: u32,
    #[prost(string, tag = "3")]
    pub(super) safe_detail: String,
}

#[cfg(test)]
pub(super) fn target_to_wire<A: lattice_core::actor_ref::ProtocolTag>(
    target: &ActorRef<A>,
) -> ExactActorTargetWire {
    ExactActorTargetWire {
        cluster_id: Bytes::copy_from_slice(target.cluster_id().as_str().as_bytes()),
        host: Bytes::copy_from_slice(target.node_address().host().as_bytes()),
        port: u32::from(target.node_address().port()),
        node_incarnation: Bytes::copy_from_slice(&target.node_incarnation().get().to_be_bytes()),
        actor_path: Bytes::copy_from_slice(target.actor_path().to_string().as_bytes()),
        activation_sequence: target.activation_id().local_sequence(),
        protocol_id: target.protocol_id().get(),
    }
}

pub(super) fn target_from_wire(
    wire: ExactActorTargetWire,
) -> Result<ExactActorTarget, RemoteMessageError> {
    let node_bytes: [u8; 16] = wire
        .node_incarnation
        .as_ref()
        .try_into()
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    let node_incarnation = NodeIncarnation::new(u128::from_be_bytes(node_bytes))
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    let port = u16::try_from(wire.port).map_err(|_| RemoteMessageError::InvalidPayload)?;
    Ok(ExactActorTarget {
        cluster_id: ClusterId::new(decode_wire_string(wire.cluster_id)?)
            .map_err(|_| RemoteMessageError::InvalidPayload)?,
        node_address: NodeAddress::new(decode_wire_string(wire.host)?, port)
            .map_err(|_| RemoteMessageError::InvalidPayload)?,
        node_incarnation,
        actor_path: ActorPath::try_from(decode_wire_string(wire.actor_path)?)
            .map_err(|_| RemoteMessageError::InvalidPayload)?,
        activation_id: ActivationId::new(node_incarnation, wire.activation_sequence)
            .map_err(|_| RemoteMessageError::InvalidPayload)?,
        protocol_id: ProtocolId::new(wire.protocol_id)
            .map_err(|_| RemoteMessageError::InvalidPayload)?,
    })
}

fn decode_wire_string(bytes: Bytes) -> Result<String, RemoteMessageError> {
    String::from_utf8(bytes.to_vec()).map_err(|_| RemoteMessageError::InvalidPayload)
}

fn decode_sender(
    sender: Option<ExactActorTargetWire>,
) -> Result<Option<ActorRef>, RemoteMessageError> {
    sender
        .map(target_from_wire)
        .transpose()?
        .map(|target| {
            target
                .actor_ref()
                .map_err(|_| RemoteMessageError::InvalidPayload)
        })
        .transpose()
}

fn decode_sender_with_cache(
    sender: Option<ExactActorTargetWire>,
    cache: Option<&mut ExactTargetCache>,
) -> Result<Option<ActorRef>, RemoteMessageError> {
    match (sender, cache) {
        (Some(wire), Some(cache)) => cache
            .resolve(wire)?
            .actor_ref()
            .map(Some)
            .map_err(|_| RemoteMessageError::InvalidPayload),
        (sender, _) => decode_sender(sender),
    }
}

#[cfg(test)]
pub(super) fn entity_target_to_wire(target: &LogicalEntityTarget) -> EntityTargetWire {
    EntityTargetWire {
        cluster_id: target.reference.cluster_id().as_str().to_owned(),
        owner_host: target.owner_address.host().to_owned(),
        owner_port: u32::from(target.owner_address.port()),
        owner_incarnation: Bytes::copy_from_slice(&target.owner_incarnation.get().to_be_bytes()),
        domain: target.reference.domain().as_str().to_owned(),
        entity_type: target.reference.entity_type().as_str().to_owned(),
        entity_id: Bytes::copy_from_slice(target.reference.entity_id().as_bytes()),
        protocol_id: target.reference.protocol_id().get(),
        config_fingerprint: Bytes::copy_from_slice(
            target.reference.config_fingerprint().as_bytes(),
        ),
        assignment_generation: target.assignment_generation,
    }
}

pub(super) fn entity_target_from_wire(
    wire: EntityTargetWire,
) -> Result<LogicalEntityTarget, RemoteMessageError> {
    let owner_incarnation = parse_incarnation(&wire.owner_incarnation)?;
    let owner_port =
        u16::try_from(wire.owner_port).map_err(|_| RemoteMessageError::InvalidPayload)?;
    let fingerprint: [u8; 32] = wire
        .config_fingerprint
        .as_ref()
        .try_into()
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    if wire.assignment_generation == 0 {
        return Err(RemoteMessageError::InvalidPayload);
    }
    Ok(LogicalEntityTarget {
        reference: EntityRef::new(
            ClusterId::new(wire.cluster_id).map_err(|_| RemoteMessageError::InvalidPayload)?,
            PlacementDomainId::new(wire.domain).map_err(|_| RemoteMessageError::InvalidPayload)?,
            EntityType::new(wire.entity_type).map_err(|_| RemoteMessageError::InvalidPayload)?,
            EntityId::new(wire.entity_id.to_vec())
                .map_err(|_| RemoteMessageError::InvalidPayload)?,
            ProtocolId::new(wire.protocol_id).map_err(|_| RemoteMessageError::InvalidPayload)?,
            ConfigFingerprint::new(fingerprint),
        )
        .map_err(|_| RemoteMessageError::InvalidPayload)?,
        owner_address: NodeAddress::new(wire.owner_host, owner_port)
            .map_err(|_| RemoteMessageError::InvalidPayload)?,
        owner_incarnation,
        assignment_generation: wire.assignment_generation,
    })
}

#[cfg(test)]
pub(super) fn singleton_target_to_wire(target: &LogicalSingletonTarget) -> SingletonTargetWire {
    SingletonTargetWire {
        cluster_id: target.reference.cluster_id().as_str().to_owned(),
        owner_host: target.owner_address.host().to_owned(),
        owner_port: u32::from(target.owner_address.port()),
        owner_incarnation: Bytes::copy_from_slice(&target.owner_incarnation.get().to_be_bytes()),
        domain: target.reference.domain().as_str().to_owned(),
        singleton_kind: target.reference.singleton_kind().as_str().to_owned(),
        protocol_id: target.reference.protocol_id().get(),
        config_fingerprint: Bytes::copy_from_slice(
            target.reference.config_fingerprint().as_bytes(),
        ),
        assignment_generation: target.assignment_generation,
    }
}

pub(super) fn singleton_target_from_wire(
    wire: SingletonTargetWire,
) -> Result<LogicalSingletonTarget, RemoteMessageError> {
    let owner_incarnation = parse_incarnation(&wire.owner_incarnation)?;
    let owner_port =
        u16::try_from(wire.owner_port).map_err(|_| RemoteMessageError::InvalidPayload)?;
    let fingerprint: [u8; 32] = wire
        .config_fingerprint
        .as_ref()
        .try_into()
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    if wire.assignment_generation == 0 {
        return Err(RemoteMessageError::InvalidPayload);
    }
    Ok(LogicalSingletonTarget {
        reference: SingletonRef::new(
            ClusterId::new(wire.cluster_id).map_err(|_| RemoteMessageError::InvalidPayload)?,
            PlacementDomainId::new(wire.domain).map_err(|_| RemoteMessageError::InvalidPayload)?,
            SingletonKind::new(wire.singleton_kind)
                .map_err(|_| RemoteMessageError::InvalidPayload)?,
            ProtocolId::new(wire.protocol_id).map_err(|_| RemoteMessageError::InvalidPayload)?,
            ConfigFingerprint::new(fingerprint),
        )
        .map_err(|_| RemoteMessageError::InvalidPayload)?,
        owner_address: NodeAddress::new(wire.owner_host, owner_port)
            .map_err(|_| RemoteMessageError::InvalidPayload)?,
        owner_incarnation,
        assignment_generation: wire.assignment_generation,
    })
}

pub(super) fn parse_incarnation(bytes: &[u8]) -> Result<NodeIncarnation, RemoteMessageError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    NodeIncarnation::new(u128::from_be_bytes(bytes)).map_err(|_| RemoteMessageError::InvalidPayload)
}
