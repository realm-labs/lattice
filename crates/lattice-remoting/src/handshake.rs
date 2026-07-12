use lattice_core::actor_ref::{ClusterId, NodeAddress, NodeIncarnation};
use prost::Message;
use thiserror::Error;

use crate::association::{AssociationId, LaneKind};
use crate::wire::{Frame, FrameKind, TRANSPORT_MAJOR, TRANSPORT_MINOR, WireError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeatureBits(u64);

impl FeatureBits {
    pub const NONE: Self = Self(0);
    pub const RELIABLE_CONTROL: Self = Self(1 << 0);
    pub const PROTOCOL_CATALOGUE: Self = Self(1 << 1);
    pub const MULTI_LANE: Self = Self(1 << 2);
    pub const REQUIRED_V1: Self =
        Self(Self::RELIABLE_CONTROL.0 | Self::PROTOCOL_CATALOGUE.0 | Self::MULTI_LANE.0);

    pub const fn from_bits(bits: u64) -> Self {
        Self(bits)
    }

    pub const fn bits(self) -> u64 {
        self.0
    }

    pub const fn contains(self, required: Self) -> bool {
        self.0 & required.0 == required.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeIdentity {
    pub cluster_id: ClusterId,
    pub node_id: String,
    pub address: NodeAddress,
    pub incarnation: NodeIncarnation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    pub source: NodeIdentity,
    pub expected_remote: NodeIdentity,
    pub association_id: AssociationId,
    pub lane: LaneKind,
    pub connection_nonce: u128,
    pub maximum_frame_size: usize,
    pub features: FeatureBits,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeAck {
    pub association_id: AssociationId,
    pub lane: LaneKind,
    pub connection_nonce: u128,
    pub maximum_frame_size: usize,
}

impl HandshakeAck {
    pub fn for_handshake(handshake: &Handshake, maximum_frame_size: usize) -> Self {
        Self {
            association_id: handshake.association_id,
            lane: handshake.lane,
            connection_nonce: handshake.connection_nonce,
            maximum_frame_size,
        }
    }

    pub fn to_frame(&self) -> Frame {
        let (lane_kind, lane_index) = lane_to_wire(self.lane);
        Frame::encode_message(
            FrameKind::HandshakeAck,
            &HandshakeAckWire {
                association_id: self.association_id.get().to_be_bytes().to_vec(),
                lane_kind,
                lane_index,
                connection_nonce: self.connection_nonce.to_be_bytes().to_vec(),
                maximum_frame_size: self.maximum_frame_size.min(u32::MAX as usize) as u32,
            },
        )
    }

    pub fn from_frame(frame: &Frame) -> Result<Self, HandshakeError> {
        if frame.kind != FrameKind::HandshakeAck {
            return Err(HandshakeError::WrongFrameKind);
        }
        let wire = frame
            .decode_message::<HandshakeAckWire>()
            .map_err(HandshakeError::Wire)?;
        Ok(Self {
            association_id: AssociationId::new(parse_u128(&wire.association_id)?)
                .ok_or(HandshakeError::InvalidIdentity)?,
            lane: lane_from_wire(wire.lane_kind, wire.lane_index)?,
            connection_nonce: parse_u128(&wire.connection_nonce)?,
            maximum_frame_size: wire.maximum_frame_size as usize,
        })
    }

    pub fn validate_for(&self, handshake: &Handshake) -> Result<(), HandshakeError> {
        if self.association_id != handshake.association_id
            || self.lane != handshake.lane
            || self.connection_nonce != handshake.connection_nonce
            || self.maximum_frame_size == 0
            || self.maximum_frame_size > handshake.maximum_frame_size
        {
            return Err(HandshakeError::AckMismatch);
        }
        Ok(())
    }
}

impl Handshake {
    pub fn to_frame(&self) -> Frame {
        Frame::encode_message(FrameKind::Handshake, &HandshakeWire::from(self))
    }

    pub fn from_frame(frame: &Frame) -> Result<Self, HandshakeError> {
        if frame.kind != FrameKind::Handshake {
            return Err(HandshakeError::WrongFrameKind);
        }
        let wire = frame
            .decode_message::<HandshakeWire>()
            .map_err(HandshakeError::Wire)?;
        Self::try_from(wire)
    }
}

pub struct HandshakeValidator {
    local: NodeIdentity,
    required_features: FeatureBits,
    maximum_frame_size: usize,
    bulk_stripes: usize,
}

impl HandshakeValidator {
    pub fn new(
        local: NodeIdentity,
        maximum_frame_size: usize,
        bulk_stripes: usize,
    ) -> Result<Self, HandshakeError> {
        validate_node(&local)?;
        if maximum_frame_size == 0 || !(1..=4).contains(&bulk_stripes) {
            return Err(HandshakeError::InvalidLimits);
        }
        Ok(Self {
            local,
            required_features: FeatureBits::REQUIRED_V1,
            maximum_frame_size,
            bulk_stripes,
        })
    }

    pub fn validate(&self, handshake: &Handshake) -> Result<usize, HandshakeError> {
        validate_node(&handshake.source)?;
        validate_node(&handshake.expected_remote)?;
        if handshake.expected_remote != self.local {
            return Err(HandshakeError::WrongDestination);
        }
        if handshake.source.cluster_id != self.local.cluster_id {
            return Err(HandshakeError::ClusterMismatch);
        }
        if !handshake.features.contains(self.required_features) {
            return Err(HandshakeError::MissingFeatures);
        }
        if handshake.connection_nonce == 0 || handshake.maximum_frame_size == 0 {
            return Err(HandshakeError::InvalidLimits);
        }
        if let LaneKind::Bulk(index) = handshake.lane
            && usize::from(index) >= self.bulk_stripes
        {
            return Err(HandshakeError::InvalidLane);
        }
        Ok(handshake.maximum_frame_size.min(self.maximum_frame_size))
    }
}

#[derive(Clone, PartialEq, Message)]
struct HandshakeWire {
    #[prost(uint32, tag = "1")]
    protocol_major: u32,
    #[prost(uint32, tag = "2")]
    protocol_minor: u32,
    #[prost(string, tag = "3")]
    cluster_id: String,
    #[prost(string, tag = "4")]
    source_node_id: String,
    #[prost(string, tag = "5")]
    source_host: String,
    #[prost(uint32, tag = "6")]
    source_port: u32,
    #[prost(bytes = "vec", tag = "7")]
    source_incarnation: Vec<u8>,
    #[prost(string, tag = "8")]
    remote_node_id: String,
    #[prost(string, tag = "9")]
    remote_host: String,
    #[prost(uint32, tag = "10")]
    remote_port: u32,
    #[prost(bytes = "vec", tag = "11")]
    remote_incarnation: Vec<u8>,
    #[prost(bytes = "vec", tag = "12")]
    association_id: Vec<u8>,
    #[prost(uint32, tag = "13")]
    lane_kind: u32,
    #[prost(uint32, tag = "14")]
    lane_index: u32,
    #[prost(bytes = "vec", tag = "15")]
    connection_nonce: Vec<u8>,
    #[prost(uint32, tag = "16")]
    maximum_frame_size: u32,
    #[prost(uint64, tag = "17")]
    features: u64,
}

#[derive(Clone, PartialEq, Message)]
struct HandshakeAckWire {
    #[prost(bytes = "vec", tag = "1")]
    association_id: Vec<u8>,
    #[prost(uint32, tag = "2")]
    lane_kind: u32,
    #[prost(uint32, tag = "3")]
    lane_index: u32,
    #[prost(bytes = "vec", tag = "4")]
    connection_nonce: Vec<u8>,
    #[prost(uint32, tag = "5")]
    maximum_frame_size: u32,
}

impl From<&Handshake> for HandshakeWire {
    fn from(value: &Handshake) -> Self {
        let (lane_kind, lane_index) = lane_to_wire(value.lane);
        Self {
            protocol_major: u32::from(TRANSPORT_MAJOR),
            protocol_minor: u32::from(TRANSPORT_MINOR),
            cluster_id: value.source.cluster_id.as_str().to_owned(),
            source_node_id: value.source.node_id.clone(),
            source_host: value.source.address.host().to_owned(),
            source_port: u32::from(value.source.address.port()),
            source_incarnation: value.source.incarnation.get().to_be_bytes().to_vec(),
            remote_node_id: value.expected_remote.node_id.clone(),
            remote_host: value.expected_remote.address.host().to_owned(),
            remote_port: u32::from(value.expected_remote.address.port()),
            remote_incarnation: value
                .expected_remote
                .incarnation
                .get()
                .to_be_bytes()
                .to_vec(),
            association_id: value.association_id.get().to_be_bytes().to_vec(),
            lane_kind,
            lane_index,
            connection_nonce: value.connection_nonce.to_be_bytes().to_vec(),
            maximum_frame_size: value.maximum_frame_size.min(u32::MAX as usize) as u32,
            features: value.features.bits(),
        }
    }
}

impl TryFrom<HandshakeWire> for Handshake {
    type Error = HandshakeError;

    fn try_from(value: HandshakeWire) -> Result<Self, Self::Error> {
        if value.protocol_major != u32::from(TRANSPORT_MAJOR)
            || value.protocol_minor > u32::from(TRANSPORT_MINOR)
        {
            return Err(HandshakeError::UnsupportedVersion {
                major: value.protocol_major,
                minor: value.protocol_minor,
            });
        }
        let cluster_id =
            ClusterId::new(value.cluster_id).map_err(|_| HandshakeError::InvalidIdentity)?;
        let source = NodeIdentity {
            cluster_id: cluster_id.clone(),
            node_id: value.source_node_id,
            address: node_address(value.source_host, value.source_port)?,
            incarnation: NodeIncarnation::new(parse_u128(&value.source_incarnation)?)
                .map_err(|_| HandshakeError::InvalidIdentity)?,
        };
        let expected_remote = NodeIdentity {
            cluster_id,
            node_id: value.remote_node_id,
            address: node_address(value.remote_host, value.remote_port)?,
            incarnation: NodeIncarnation::new(parse_u128(&value.remote_incarnation)?)
                .map_err(|_| HandshakeError::InvalidIdentity)?,
        };
        let lane = lane_from_wire(value.lane_kind, value.lane_index)?;
        let association_id = AssociationId::new(parse_u128(&value.association_id)?)
            .ok_or(HandshakeError::InvalidIdentity)?;
        let handshake = Self {
            source,
            expected_remote,
            association_id,
            lane,
            connection_nonce: parse_u128(&value.connection_nonce)?,
            maximum_frame_size: value.maximum_frame_size as usize,
            features: FeatureBits::from_bits(value.features),
        };
        validate_node(&handshake.source)?;
        validate_node(&handshake.expected_remote)?;
        Ok(handshake)
    }
}

fn validate_node(identity: &NodeIdentity) -> Result<(), HandshakeError> {
    if identity.node_id.is_empty()
        || identity.node_id.len() > 128
        || identity.node_id.contains(['/', '\\'])
        || identity.node_id.chars().any(char::is_control)
    {
        return Err(HandshakeError::InvalidIdentity);
    }
    Ok(())
}

fn node_address(host: String, port: u32) -> Result<NodeAddress, HandshakeError> {
    let port = u16::try_from(port).map_err(|_| HandshakeError::InvalidIdentity)?;
    NodeAddress::new(host, port).map_err(|_| HandshakeError::InvalidIdentity)
}

fn parse_u128(bytes: &[u8]) -> Result<u128, HandshakeError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| HandshakeError::InvalidIdentity)?;
    Ok(u128::from_be_bytes(bytes))
}

fn lane_to_wire(lane: LaneKind) -> (u32, u32) {
    match lane {
        LaneKind::Control => (0, 0),
        LaneKind::Interactive => (1, 0),
        LaneKind::Bulk(index) => (2, u32::from(index)),
    }
}

fn lane_from_wire(kind: u32, index: u32) -> Result<LaneKind, HandshakeError> {
    match (kind, index) {
        (0, 0) => Ok(LaneKind::Control),
        (1, 0) => Ok(LaneKind::Interactive),
        (2, index) if index <= u32::from(u8::MAX) => Ok(LaneKind::Bulk(index as u8)),
        _ => Err(HandshakeError::InvalidLane),
    }
}

#[derive(Debug, Error)]
pub enum HandshakeError {
    #[error("handshake used the wrong frame kind")]
    WrongFrameKind,
    #[error("handshake frame is invalid")]
    Wire(#[source] WireError),
    #[error("unsupported handshake transport version {major}.{minor}")]
    UnsupportedVersion { major: u32, minor: u32 },
    #[error("handshake node identity is invalid")]
    InvalidIdentity,
    #[error("handshake names a different destination identity or incarnation")]
    WrongDestination,
    #[error("handshake cluster ID differs")]
    ClusterMismatch,
    #[error("handshake is missing mandatory transport features")]
    MissingFeatures,
    #[error("handshake lane is invalid")]
    InvalidLane,
    #[error("handshake limits are invalid")]
    InvalidLimits,
    #[error("handshake acknowledgement does not bind the requested association lane")]
    AckMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(name: &str, incarnation: u128, port: u16) -> NodeIdentity {
        NodeIdentity {
            cluster_id: ClusterId::new("test").unwrap(),
            node_id: name.to_owned(),
            address: NodeAddress::new("127.0.0.1", port).unwrap(),
            incarnation: NodeIncarnation::new(incarnation).unwrap(),
        }
    }

    #[test]
    fn binary_handshake_round_trips_and_rejects_old_incarnation() {
        let local = identity("local", 1, 25520);
        let remote = identity("remote", 2, 25521);
        let handshake = Handshake {
            source: remote.clone(),
            expected_remote: local.clone(),
            association_id: AssociationId::new(9).unwrap(),
            lane: LaneKind::Control,
            connection_nonce: 10,
            maximum_frame_size: 256 * 1024,
            features: FeatureBits::REQUIRED_V1,
        };
        let decoded = Handshake::from_frame(&handshake.to_frame()).unwrap();
        HandshakeValidator::new(local.clone(), 256 * 1024, 1)
            .unwrap()
            .validate(&decoded)
            .unwrap();

        let validator =
            HandshakeValidator::new(identity("local", 3, 25520), 256 * 1024, 1).unwrap();
        assert!(matches!(
            validator.validate(&decoded),
            Err(HandshakeError::WrongDestination)
        ));
    }
}
