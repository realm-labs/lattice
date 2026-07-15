use super::error::{AskError, RemoteFailureCode, RemoteMessageError};
use super::target::{
    CorrelationId, ExactActorTarget, InboundAsk, InboundEntityAsk, InboundEntityTell,
    InboundSingletonAsk, InboundSingletonTell, InboundTell, LogicalEntityTarget,
    LogicalSingletonTarget, RemoteFailure,
};
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

pub(super) fn set_logical_ask_correlation(
    frame: &mut Frame,
    correlation: CorrelationId,
) -> Result<(), AskError> {
    match frame.kind {
        FrameKind::EntityAsk => {
            let mut wire = frame
                .decode_message::<EntityAskWire>()
                .map_err(|_| AskError::Protocol(RemoteMessageError::InvalidPayload))?;
            wire.correlation_id = correlation.to_bytes().to_vec();
            *frame = Frame::encode_message(FrameKind::EntityAsk, &wire);
            Ok(())
        }
        FrameKind::SingletonAsk => {
            let mut wire = frame
                .decode_message::<SingletonAskWire>()
                .map_err(|_| AskError::Protocol(RemoteMessageError::InvalidPayload))?;
            wire.correlation_id = correlation.to_bytes().to_vec();
            *frame = Frame::encode_message(FrameKind::SingletonAsk, &wire);
            Ok(())
        }
        _ => Err(AskError::Protocol(RemoteMessageError::InvalidPayload)),
    }
}

pub fn decode_tell(frame: &Frame) -> Result<InboundTell, RemoteMessageError> {
    if frame.kind != FrameKind::Tell {
        return Err(RemoteMessageError::InvalidPayload);
    }
    let wire = frame
        .decode_message::<TellWire>()
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    if wire.message_id == 0 {
        return Err(RemoteMessageError::InvalidPayload);
    }
    Ok(InboundTell {
        sender: decode_sender(wire.sender_actor)?,
        target: target_from_wire(wire.target.ok_or(RemoteMessageError::InvalidPayload)?)?,
        message_id: wire.message_id,
        payload: Bytes::from(wire.payload),
    })
}

pub fn decode_ask(frame: &Frame) -> Result<InboundAsk, RemoteMessageError> {
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
    Ok(InboundAsk {
        target: target_from_wire(wire.target.ok_or(RemoteMessageError::InvalidPayload)?)?,
        correlation_id,
        timeout_budget: Duration::from_nanos(wire.timeout_nanos),
        message_id: wire.message_id,
        payload: Bytes::from(wire.payload),
    })
}

pub fn decode_entity_tell(frame: &Frame) -> Result<InboundEntityTell, RemoteMessageError> {
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
        sender: decode_sender(wire.sender_actor)?,
        target: entity_target_from_wire(wire.target.ok_or(RemoteMessageError::InvalidPayload)?)?,
        message_id: wire.message_id,
        payload: Bytes::from(wire.payload),
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
        payload: Bytes::from(wire.payload),
    })
}

pub fn decode_singleton_tell(frame: &Frame) -> Result<InboundSingletonTell, RemoteMessageError> {
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
        sender: decode_sender(wire.sender_actor)?,
        target: singleton_target_from_wire(wire.target.ok_or(RemoteMessageError::InvalidPayload)?)?,
        message_id: wire.message_id,
        payload: Bytes::from(wire.payload),
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
        payload: Bytes::from(wire.payload),
    })
}

pub fn reply_frame(correlation_id: CorrelationId, payload: Bytes) -> Frame {
    Frame::encode_message(
        FrameKind::Reply,
        &ReplyWire {
            correlation_id: correlation_id.to_bytes().to_vec(),
            payload: payload.to_vec(),
        },
    )
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
        Bytes::from(reply.payload),
    ))
}

pub fn failure_frame(failure: &RemoteFailure) -> Frame {
    let detail = failure.safe_detail.as_deref().unwrap_or("");
    Frame::encode_message(
        FrameKind::Failure,
        &FailureWire {
            correlation_id: failure.correlation_id.to_bytes().to_vec(),
            code: failure.code as u32,
            safe_detail: detail.chars().take(256).collect(),
        },
    )
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

#[derive(Clone, PartialEq, Message)]
pub(super) struct ExactActorTargetWire {
    #[prost(string, tag = "1")]
    pub(super) cluster_id: String,
    #[prost(string, tag = "2")]
    pub(super) host: String,
    #[prost(uint32, tag = "3")]
    pub(super) port: u32,
    #[prost(bytes = "vec", tag = "4")]
    pub(super) node_incarnation: Vec<u8>,
    #[prost(string, tag = "5")]
    pub(super) actor_path: String,
    #[prost(uint64, tag = "6")]
    pub(super) activation_sequence: u64,
    #[prost(uint64, tag = "7")]
    pub(super) protocol_id: u64,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct TellWire {
    #[prost(bytes = "vec", tag = "1")]
    pub(super) sender: Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub(super) target: Option<ExactActorTargetWire>,
    #[prost(uint64, tag = "3")]
    pub(super) message_id: u64,
    #[prost(bytes = "vec", tag = "4")]
    pub(super) payload: Vec<u8>,
    #[prost(message, optional, tag = "5")]
    pub(super) sender_actor: Option<ExactActorTargetWire>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct AskWire {
    #[prost(bytes = "vec", tag = "1")]
    pub(super) sender: Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub(super) target: Option<ExactActorTargetWire>,
    #[prost(bytes = "vec", tag = "3")]
    pub(super) correlation_id: Vec<u8>,
    #[prost(uint64, tag = "4")]
    pub(super) timeout_nanos: u64,
    #[prost(uint64, tag = "5")]
    pub(super) message_id: u64,
    #[prost(bytes = "vec", tag = "6")]
    pub(super) payload: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct EntityTargetWire {
    #[prost(string, tag = "1")]
    pub(super) cluster_id: String,
    #[prost(string, tag = "2")]
    pub(super) owner_host: String,
    #[prost(uint32, tag = "3")]
    pub(super) owner_port: u32,
    #[prost(bytes = "vec", tag = "4")]
    pub(super) owner_incarnation: Vec<u8>,
    #[prost(string, tag = "5")]
    pub(super) entity_type: String,
    #[prost(bytes = "vec", tag = "6")]
    pub(super) entity_id: Vec<u8>,
    #[prost(uint64, tag = "7")]
    pub(super) protocol_id: u64,
    #[prost(bytes = "vec", tag = "8")]
    pub(super) config_fingerprint: Vec<u8>,
    #[prost(uint64, tag = "9")]
    pub(super) assignment_generation: u64,
    #[prost(string, tag = "10")]
    pub(super) domain: String,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct EntityTellWire {
    #[prost(bytes = "vec", tag = "1")]
    pub(super) sender: Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub(super) target: Option<EntityTargetWire>,
    #[prost(uint64, tag = "3")]
    pub(super) message_id: u64,
    #[prost(bytes = "vec", tag = "4")]
    pub(super) payload: Vec<u8>,
    #[prost(message, optional, tag = "5")]
    pub(super) sender_actor: Option<ExactActorTargetWire>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct EntityAskWire {
    #[prost(bytes = "vec", tag = "1")]
    pub(super) sender: Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub(super) target: Option<EntityTargetWire>,
    #[prost(bytes = "vec", tag = "3")]
    pub(super) correlation_id: Vec<u8>,
    #[prost(uint64, tag = "4")]
    pub(super) timeout_nanos: u64,
    #[prost(uint64, tag = "5")]
    pub(super) message_id: u64,
    #[prost(bytes = "vec", tag = "6")]
    pub(super) payload: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct SingletonTargetWire {
    #[prost(string, tag = "1")]
    pub(super) cluster_id: String,
    #[prost(string, tag = "2")]
    pub(super) owner_host: String,
    #[prost(uint32, tag = "3")]
    pub(super) owner_port: u32,
    #[prost(bytes = "vec", tag = "4")]
    pub(super) owner_incarnation: Vec<u8>,
    #[prost(string, tag = "5")]
    pub(super) singleton_kind: String,
    #[prost(uint64, tag = "6")]
    pub(super) protocol_id: u64,
    #[prost(bytes = "vec", tag = "7")]
    pub(super) config_fingerprint: Vec<u8>,
    #[prost(uint64, tag = "8")]
    pub(super) assignment_generation: u64,
    #[prost(string, tag = "9")]
    pub(super) domain: String,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct SingletonTellWire {
    #[prost(bytes = "vec", tag = "1")]
    pub(super) sender: Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub(super) target: Option<SingletonTargetWire>,
    #[prost(uint64, tag = "3")]
    pub(super) message_id: u64,
    #[prost(bytes = "vec", tag = "4")]
    pub(super) payload: Vec<u8>,
    #[prost(message, optional, tag = "5")]
    pub(super) sender_actor: Option<ExactActorTargetWire>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct SingletonAskWire {
    #[prost(bytes = "vec", tag = "1")]
    pub(super) sender: Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub(super) target: Option<SingletonTargetWire>,
    #[prost(bytes = "vec", tag = "3")]
    pub(super) correlation_id: Vec<u8>,
    #[prost(uint64, tag = "4")]
    pub(super) timeout_nanos: u64,
    #[prost(uint64, tag = "5")]
    pub(super) message_id: u64,
    #[prost(bytes = "vec", tag = "6")]
    pub(super) payload: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct ReplyWire {
    #[prost(bytes = "vec", tag = "1")]
    pub(super) correlation_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub(super) payload: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct FailureWire {
    #[prost(bytes = "vec", tag = "1")]
    pub(super) correlation_id: Vec<u8>,
    #[prost(uint32, tag = "2")]
    pub(super) code: u32,
    #[prost(string, tag = "3")]
    pub(super) safe_detail: String,
}

pub(super) fn target_to_wire(target: &ExactActorTarget) -> ExactActorTargetWire {
    ExactActorTargetWire {
        cluster_id: target.cluster_id.as_str().to_owned(),
        host: target.node_address.host().to_owned(),
        port: u32::from(target.node_address.port()),
        node_incarnation: target.node_incarnation.get().to_be_bytes().to_vec(),
        actor_path: target.actor_path.to_string(),
        activation_sequence: target.activation_id.local_sequence(),
        protocol_id: target.protocol_id.get(),
    }
}

pub(super) fn target_from_wire(
    wire: ExactActorTargetWire,
) -> Result<ExactActorTarget, RemoteMessageError> {
    let node_bytes: [u8; 16] = wire
        .node_incarnation
        .as_slice()
        .try_into()
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    let node_incarnation = NodeIncarnation::new(u128::from_be_bytes(node_bytes))
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    let port = u16::try_from(wire.port).map_err(|_| RemoteMessageError::InvalidPayload)?;
    Ok(ExactActorTarget {
        cluster_id: ClusterId::new(wire.cluster_id)
            .map_err(|_| RemoteMessageError::InvalidPayload)?,
        node_address: NodeAddress::new(wire.host, port)
            .map_err(|_| RemoteMessageError::InvalidPayload)?,
        node_incarnation,
        actor_path: ActorPath::try_from(wire.actor_path)
            .map_err(|_| RemoteMessageError::InvalidPayload)?,
        activation_id: ActivationId::new(node_incarnation, wire.activation_sequence)
            .map_err(|_| RemoteMessageError::InvalidPayload)?,
        protocol_id: ProtocolId::new(wire.protocol_id)
            .map_err(|_| RemoteMessageError::InvalidPayload)?,
    })
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

pub(super) fn entity_target_to_wire(target: &LogicalEntityTarget) -> EntityTargetWire {
    EntityTargetWire {
        cluster_id: target.reference.cluster_id().as_str().to_owned(),
        owner_host: target.owner_address.host().to_owned(),
        owner_port: u32::from(target.owner_address.port()),
        owner_incarnation: target.owner_incarnation.get().to_be_bytes().to_vec(),
        domain: target.reference.domain().as_str().to_owned(),
        entity_type: target.reference.entity_type().as_str().to_owned(),
        entity_id: target.reference.entity_id().as_bytes().to_vec(),
        protocol_id: target.reference.protocol_id().get(),
        config_fingerprint: target.reference.config_fingerprint().as_bytes().to_vec(),
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
            EntityId::new(wire.entity_id).map_err(|_| RemoteMessageError::InvalidPayload)?,
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

pub(super) fn singleton_target_to_wire(target: &LogicalSingletonTarget) -> SingletonTargetWire {
    SingletonTargetWire {
        cluster_id: target.reference.cluster_id().as_str().to_owned(),
        owner_host: target.owner_address.host().to_owned(),
        owner_port: u32::from(target.owner_address.port()),
        owner_incarnation: target.owner_incarnation.get().to_be_bytes().to_vec(),
        domain: target.reference.domain().as_str().to_owned(),
        singleton_kind: target.reference.singleton_kind().as_str().to_owned(),
        protocol_id: target.reference.protocol_id().get(),
        config_fingerprint: target.reference.config_fingerprint().as_bytes().to_vec(),
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
