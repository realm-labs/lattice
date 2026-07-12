use std::time::Duration;

use lattice_core::actor_ref::{ClusterId, NodeAddress, NodeIncarnation};
use prost::{Enumeration, Message};

use crate::handshake::{FeatureBits, NodeIdentity};
use crate::wire::{Frame, FrameKind, TRANSPORT_MAJOR, TRANSPORT_MINOR, WireError};

pub const MAX_BOOTSTRAP_REASON_BYTES: usize = 256;
pub const MAX_BOOTSTRAP_RETRY_AFTER: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapProbeTarget {
    pub address: NodeAddress,
    pub expected_node_id: Option<String>,
    pub tls_server_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapRequest {
    pub local: NodeIdentity,
    pub requested_cluster_id: ClusterId,
    pub expected_node_id: Option<String>,
    pub transport_major: u16,
    pub transport_minor: u16,
    pub features: FeatureBits,
    pub nonce: u128,
}

impl BootstrapRequest {
    pub fn new(
        local: NodeIdentity,
        requested_cluster_id: ClusterId,
        expected_node_id: Option<String>,
    ) -> Self {
        let nonce = uuid::Uuid::new_v4().as_u128().max(1);
        Self {
            local,
            requested_cluster_id,
            expected_node_id,
            transport_major: TRANSPORT_MAJOR,
            transport_minor: TRANSPORT_MINOR,
            features: FeatureBits::REQUIRED_V1,
            nonce,
        }
    }

    pub fn to_frame(&self) -> Frame {
        Frame::encode_message(
            FrameKind::BootstrapRequest,
            &BootstrapRequestWire::from(self),
        )
    }

    pub fn from_frame(frame: &Frame) -> Result<Self, BootstrapError> {
        if frame.kind != FrameKind::BootstrapRequest {
            return Err(BootstrapError::WrongFrameKind);
        }
        let wire = frame
            .decode_message::<BootstrapRequestWire>()
            .map_err(BootstrapError::Wire)?;
        Self::try_from(wire)
    }

    pub fn rejection(&self, local: &NodeIdentity) -> Option<BootstrapRejectionCode> {
        if self.nonce == 0 || validate_identity(&self.local).is_err() {
            return Some(BootstrapRejectionCode::InvalidIdentity);
        }
        if self.transport_major != TRANSPORT_MAJOR || self.transport_minor > TRANSPORT_MINOR {
            return Some(BootstrapRejectionCode::IncompatibleTransport);
        }
        if !self.features.contains(FeatureBits::REQUIRED_V1) {
            return Some(BootstrapRejectionCode::MissingRequiredFeature);
        }
        if self.requested_cluster_id != local.cluster_id
            || self.local.cluster_id != self.requested_cluster_id
        {
            return Some(BootstrapRejectionCode::ClusterMismatch);
        }
        if self
            .expected_node_id
            .as_ref()
            .is_some_and(|expected| expected != &local.node_id)
        {
            return Some(BootstrapRejectionCode::ExpectedNodeMismatch);
        }
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapLeader {
    pub identity: NodeIdentity,
    pub term: u64,
    pub protocol_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapResult {
    Identity {
        remote: NodeIdentity,
        leader: Option<BootstrapLeader>,
    },
    Redirect {
        remote: NodeIdentity,
        leader: BootstrapLeader,
    },
    ReverseDial {
        remote: NodeIdentity,
        leader: Option<BootstrapLeader>,
    },
    Rejected {
        code: BootstrapRejectionCode,
    },
    RetryAfter {
        delay: Duration,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapResponse {
    pub nonce: u128,
    pub transport_major: u16,
    pub transport_minor: u16,
    pub features: FeatureBits,
    pub result: BootstrapResult,
}

impl BootstrapResponse {
    pub fn new(nonce: u128, result: BootstrapResult) -> Self {
        Self {
            nonce,
            transport_major: TRANSPORT_MAJOR,
            transport_minor: TRANSPORT_MINOR,
            features: FeatureBits::REQUIRED_V1,
            result,
        }
    }

    pub fn rejected(nonce: u128, code: BootstrapRejectionCode) -> Self {
        Self::new(nonce, BootstrapResult::Rejected { code })
    }

    pub fn to_frame(&self) -> Frame {
        Frame::encode_message(
            FrameKind::BootstrapResponse,
            &BootstrapResponseWire::from(self),
        )
    }

    pub fn from_frame(frame: &Frame) -> Result<Self, BootstrapError> {
        if frame.kind != FrameKind::BootstrapResponse {
            return Err(BootstrapError::WrongFrameKind);
        }
        let wire = frame
            .decode_message::<BootstrapResponseWire>()
            .map_err(BootstrapError::Wire)?;
        Self::try_from(wire)
    }

    pub fn validate_for(&self, request: &BootstrapRequest) -> Result<(), BootstrapError> {
        if self.nonce != request.nonce {
            return Err(BootstrapError::NonceMismatch);
        }
        if self.transport_major != TRANSPORT_MAJOR
            || self.transport_minor > TRANSPORT_MINOR
            || !self.features.contains(FeatureBits::REQUIRED_V1)
        {
            return Err(BootstrapError::IncompatibleTransport);
        }
        match &self.result {
            BootstrapResult::Identity { remote, leader }
            | BootstrapResult::ReverseDial { remote, leader } => {
                validate_remote(request, remote)?;
                if let Some(leader) = leader {
                    validate_leader(request, leader)?;
                }
            }
            BootstrapResult::Redirect { remote, leader } => {
                validate_remote(request, remote)?;
                validate_leader(request, leader)?;
            }
            BootstrapResult::Rejected { .. } => {}
            BootstrapResult::RetryAfter { delay, reason } => {
                if delay.is_zero()
                    || *delay > MAX_BOOTSTRAP_RETRY_AFTER
                    || reason.is_empty()
                    || reason.len() > MAX_BOOTSTRAP_REASON_BYTES
                {
                    return Err(BootstrapError::InvalidResponse);
                }
            }
        }
        Ok(())
    }

    pub fn remote_identity(&self) -> Option<&NodeIdentity> {
        match &self.result {
            BootstrapResult::Identity { remote, .. }
            | BootstrapResult::Redirect { remote, .. }
            | BootstrapResult::ReverseDial { remote, .. } => Some(remote),
            BootstrapResult::Rejected { .. } | BootstrapResult::RetryAfter { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enumeration)]
#[repr(i32)]
pub enum BootstrapRejectionCode {
    ClusterMismatch = 1,
    ExpectedNodeMismatch = 2,
    IncompatibleTransport = 3,
    MissingRequiredFeature = 4,
    InvalidIdentity = 5,
    AuthenticationFailure = 6,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapRoute {
    Accept { leader: Option<BootstrapLeader> },
    Redirect { leader: BootstrapLeader },
    RetryAfter { delay: Duration, reason: String },
    Reject { code: BootstrapRejectionCode },
}

pub trait BootstrapHandler: Send + Sync {
    fn route(&self, request: &BootstrapRequest) -> BootstrapRoute;
}

#[derive(Debug)]
pub struct AcceptBootstrap;

impl BootstrapHandler for AcceptBootstrap {
    fn route(&self, _request: &BootstrapRequest) -> BootstrapRoute {
        BootstrapRoute::Accept { leader: None }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    #[error("bootstrap used the wrong frame kind")]
    WrongFrameKind,
    #[error("bootstrap frame is invalid")]
    Wire(#[source] WireError),
    #[error("bootstrap identity is invalid")]
    InvalidIdentity,
    #[error("bootstrap result has an invalid kind")]
    InvalidResult,
    #[error("bootstrap response nonce does not match the request")]
    NonceMismatch,
    #[error("bootstrap transport version or required features are incompatible")]
    IncompatibleTransport,
    #[error("bootstrap response is invalid")]
    InvalidResponse,
    #[error("bootstrap returned identity for a different cluster or expected node")]
    IdentityMismatch,
}

#[derive(Clone, PartialEq, Message)]
struct BootstrapRequestWire {
    #[prost(uint32, tag = "1")]
    transport_major: u32,
    #[prost(uint32, tag = "2")]
    transport_minor: u32,
    #[prost(uint64, tag = "3")]
    features: u64,
    #[prost(string, tag = "4")]
    requested_cluster_id: String,
    #[prost(message, optional, tag = "5")]
    local: Option<NodeIdentityWire>,
    #[prost(string, optional, tag = "6")]
    expected_node_id: Option<String>,
    #[prost(bytes = "vec", tag = "7")]
    nonce: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct BootstrapResponseWire {
    #[prost(uint32, tag = "1")]
    transport_major: u32,
    #[prost(uint32, tag = "2")]
    transport_minor: u32,
    #[prost(uint64, tag = "3")]
    features: u64,
    #[prost(bytes = "vec", tag = "4")]
    nonce: Vec<u8>,
    #[prost(enumeration = "BootstrapResultKind", tag = "5")]
    result_kind: i32,
    #[prost(message, optional, tag = "6")]
    remote: Option<NodeIdentityWire>,
    #[prost(message, optional, tag = "7")]
    leader: Option<BootstrapLeaderWire>,
    #[prost(enumeration = "BootstrapRejectionCode", optional, tag = "8")]
    rejection_code: Option<i32>,
    #[prost(uint64, tag = "9")]
    retry_after_millis: u64,
    #[prost(string, tag = "10")]
    reason: String,
}

#[derive(Clone, PartialEq, Message)]
struct NodeIdentityWire {
    #[prost(string, tag = "1")]
    cluster_id: String,
    #[prost(string, tag = "2")]
    node_id: String,
    #[prost(string, tag = "3")]
    host: String,
    #[prost(uint32, tag = "4")]
    port: u32,
    #[prost(bytes = "vec", tag = "5")]
    incarnation: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct BootstrapLeaderWire {
    #[prost(message, optional, tag = "1")]
    identity: Option<NodeIdentityWire>,
    #[prost(uint64, tag = "2")]
    term: u64,
    #[prost(uint64, tag = "3")]
    protocol_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enumeration)]
#[repr(i32)]
enum BootstrapResultKind {
    Identity = 1,
    Redirect = 2,
    ReverseDial = 3,
    Rejected = 4,
    RetryAfter = 5,
}

impl From<&BootstrapRequest> for BootstrapRequestWire {
    fn from(value: &BootstrapRequest) -> Self {
        Self {
            transport_major: u32::from(value.transport_major),
            transport_minor: u32::from(value.transport_minor),
            features: value.features.bits(),
            requested_cluster_id: value.requested_cluster_id.as_str().to_string(),
            local: Some(NodeIdentityWire::from(&value.local)),
            expected_node_id: value.expected_node_id.clone(),
            nonce: value.nonce.to_be_bytes().to_vec(),
        }
    }
}

impl TryFrom<BootstrapRequestWire> for BootstrapRequest {
    type Error = BootstrapError;

    fn try_from(value: BootstrapRequestWire) -> Result<Self, Self::Error> {
        Ok(Self {
            local: value
                .local
                .ok_or(BootstrapError::InvalidIdentity)?
                .try_into()?,
            requested_cluster_id: ClusterId::new(value.requested_cluster_id)
                .map_err(|_| BootstrapError::InvalidIdentity)?,
            expected_node_id: value.expected_node_id,
            transport_major: u16::try_from(value.transport_major)
                .map_err(|_| BootstrapError::IncompatibleTransport)?,
            transport_minor: u16::try_from(value.transport_minor)
                .map_err(|_| BootstrapError::IncompatibleTransport)?,
            features: FeatureBits::from_bits(value.features),
            nonce: parse_u128(&value.nonce)?,
        })
    }
}

impl From<&BootstrapResponse> for BootstrapResponseWire {
    fn from(value: &BootstrapResponse) -> Self {
        let (result_kind, remote, leader, rejection_code, retry_after_millis, reason) =
            match &value.result {
                BootstrapResult::Identity { remote, leader } => (
                    BootstrapResultKind::Identity,
                    Some(NodeIdentityWire::from(remote)),
                    leader.as_ref().map(BootstrapLeaderWire::from),
                    None,
                    0,
                    String::new(),
                ),
                BootstrapResult::Redirect { remote, leader } => (
                    BootstrapResultKind::Redirect,
                    Some(NodeIdentityWire::from(remote)),
                    Some(BootstrapLeaderWire::from(leader)),
                    None,
                    0,
                    String::new(),
                ),
                BootstrapResult::ReverseDial { remote, leader } => (
                    BootstrapResultKind::ReverseDial,
                    Some(NodeIdentityWire::from(remote)),
                    leader.as_ref().map(BootstrapLeaderWire::from),
                    None,
                    0,
                    String::new(),
                ),
                BootstrapResult::Rejected { code } => (
                    BootstrapResultKind::Rejected,
                    None,
                    None,
                    Some(*code as i32),
                    0,
                    String::new(),
                ),
                BootstrapResult::RetryAfter { delay, reason } => (
                    BootstrapResultKind::RetryAfter,
                    None,
                    None,
                    None,
                    delay.as_millis().min(u128::from(u64::MAX)) as u64,
                    reason.clone(),
                ),
            };
        Self {
            transport_major: u32::from(value.transport_major),
            transport_minor: u32::from(value.transport_minor),
            features: value.features.bits(),
            nonce: value.nonce.to_be_bytes().to_vec(),
            result_kind: result_kind as i32,
            remote,
            leader,
            rejection_code,
            retry_after_millis,
            reason,
        }
    }
}

impl TryFrom<BootstrapResponseWire> for BootstrapResponse {
    type Error = BootstrapError;

    fn try_from(value: BootstrapResponseWire) -> Result<Self, Self::Error> {
        let remote = value.remote.map(NodeIdentity::try_from).transpose()?;
        let leader = value.leader.map(BootstrapLeader::try_from).transpose()?;
        let result = match BootstrapResultKind::try_from(value.result_kind)
            .map_err(|_| BootstrapError::InvalidResult)?
        {
            BootstrapResultKind::Identity => BootstrapResult::Identity {
                remote: remote.ok_or(BootstrapError::InvalidResult)?,
                leader,
            },
            BootstrapResultKind::Redirect => BootstrapResult::Redirect {
                remote: remote.ok_or(BootstrapError::InvalidResult)?,
                leader: leader.ok_or(BootstrapError::InvalidResult)?,
            },
            BootstrapResultKind::ReverseDial => BootstrapResult::ReverseDial {
                remote: remote.ok_or(BootstrapError::InvalidResult)?,
                leader,
            },
            BootstrapResultKind::Rejected => BootstrapResult::Rejected {
                code: BootstrapRejectionCode::try_from(
                    value.rejection_code.ok_or(BootstrapError::InvalidResult)?,
                )
                .map_err(|_| BootstrapError::InvalidResult)?,
            },
            BootstrapResultKind::RetryAfter => BootstrapResult::RetryAfter {
                delay: Duration::from_millis(value.retry_after_millis),
                reason: value.reason,
            },
        };
        Ok(Self {
            nonce: parse_u128(&value.nonce)?,
            transport_major: u16::try_from(value.transport_major)
                .map_err(|_| BootstrapError::IncompatibleTransport)?,
            transport_minor: u16::try_from(value.transport_minor)
                .map_err(|_| BootstrapError::IncompatibleTransport)?,
            features: FeatureBits::from_bits(value.features),
            result,
        })
    }
}

impl From<&NodeIdentity> for NodeIdentityWire {
    fn from(value: &NodeIdentity) -> Self {
        Self {
            cluster_id: value.cluster_id.as_str().to_string(),
            node_id: value.node_id.clone(),
            host: value.address.host().to_string(),
            port: u32::from(value.address.port()),
            incarnation: value.incarnation.get().to_be_bytes().to_vec(),
        }
    }
}

impl TryFrom<NodeIdentityWire> for NodeIdentity {
    type Error = BootstrapError;

    fn try_from(value: NodeIdentityWire) -> Result<Self, Self::Error> {
        let identity = Self {
            cluster_id: ClusterId::new(value.cluster_id)
                .map_err(|_| BootstrapError::InvalidIdentity)?,
            node_id: value.node_id,
            address: NodeAddress::new(
                value.host,
                u16::try_from(value.port).map_err(|_| BootstrapError::InvalidIdentity)?,
            )
            .map_err(|_| BootstrapError::InvalidIdentity)?,
            incarnation: NodeIncarnation::new(parse_u128(&value.incarnation)?)
                .map_err(|_| BootstrapError::InvalidIdentity)?,
        };
        validate_identity(&identity)?;
        Ok(identity)
    }
}

impl From<&BootstrapLeader> for BootstrapLeaderWire {
    fn from(value: &BootstrapLeader) -> Self {
        Self {
            identity: Some(NodeIdentityWire::from(&value.identity)),
            term: value.term,
            protocol_generation: value.protocol_generation,
        }
    }
}

impl TryFrom<BootstrapLeaderWire> for BootstrapLeader {
    type Error = BootstrapError;

    fn try_from(value: BootstrapLeaderWire) -> Result<Self, Self::Error> {
        let leader = Self {
            identity: value
                .identity
                .ok_or(BootstrapError::InvalidIdentity)?
                .try_into()?,
            term: value.term,
            protocol_generation: value.protocol_generation,
        };
        if leader.term == 0 || leader.protocol_generation == 0 {
            return Err(BootstrapError::InvalidResponse);
        }
        Ok(leader)
    }
}

fn validate_remote(
    request: &BootstrapRequest,
    remote: &NodeIdentity,
) -> Result<(), BootstrapError> {
    validate_identity(remote)?;
    if remote.cluster_id != request.requested_cluster_id
        || request
            .expected_node_id
            .as_ref()
            .is_some_and(|expected| expected != &remote.node_id)
    {
        return Err(BootstrapError::IdentityMismatch);
    }
    Ok(())
}

fn validate_leader(
    request: &BootstrapRequest,
    leader: &BootstrapLeader,
) -> Result<(), BootstrapError> {
    validate_identity(&leader.identity)?;
    if leader.identity.cluster_id != request.requested_cluster_id
        || leader.term == 0
        || leader.protocol_generation == 0
    {
        return Err(BootstrapError::IdentityMismatch);
    }
    Ok(())
}

fn validate_identity(identity: &NodeIdentity) -> Result<(), BootstrapError> {
    if identity.node_id.is_empty()
        || identity.node_id.len() > 128
        || identity.node_id.contains(['/', '\\'])
        || identity.node_id.chars().any(char::is_control)
    {
        return Err(BootstrapError::InvalidIdentity);
    }
    Ok(())
}

fn parse_u128(bytes: &[u8]) -> Result<u128, BootstrapError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| BootstrapError::InvalidIdentity)?;
    Ok(u128::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(name: &str, incarnation: u128, port: u16) -> NodeIdentity {
        NodeIdentity {
            cluster_id: ClusterId::new("test").unwrap(),
            node_id: name.to_string(),
            address: NodeAddress::new("127.0.0.1", port).unwrap(),
            incarnation: NodeIncarnation::new(incarnation).unwrap(),
        }
    }

    #[test]
    fn request_and_response_round_trip_with_exact_identity() {
        let request = BootstrapRequest::new(
            identity("client", 1, 7447),
            ClusterId::new("test").unwrap(),
            Some("server".to_string()),
        );
        let request = BootstrapRequest::from_frame(&request.to_frame()).unwrap();
        let response = BootstrapResponse::new(
            request.nonce,
            BootstrapResult::Identity {
                remote: identity("server", 2, 7448),
                leader: None,
            },
        );
        let response = BootstrapResponse::from_frame(&response.to_frame()).unwrap();

        response.validate_for(&request).unwrap();
        assert_eq!(response.remote_identity().unwrap().node_id, "server");
    }

    #[test]
    fn nonce_expected_identity_and_required_feature_are_fenced() {
        let mut request = BootstrapRequest::new(
            identity("client", 1, 7447),
            ClusterId::new("test").unwrap(),
            Some("expected".to_string()),
        );
        let response = BootstrapResponse::new(
            request.nonce + 1,
            BootstrapResult::Identity {
                remote: identity("replacement", 2, 7448),
                leader: None,
            },
        );
        assert!(matches!(
            response.validate_for(&request),
            Err(BootstrapError::NonceMismatch)
        ));
        request.features = FeatureBits::NONE;
        assert_eq!(
            request.rejection(&identity("server", 2, 7448)),
            Some(BootstrapRejectionCode::MissingRequiredFeature)
        );
    }
}
