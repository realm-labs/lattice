use std::{
    fmt,
    hash::Hash,
    net::{IpAddr, Ipv6Addr},
    sync::Arc,
};

use serde::{Deserialize, Serialize, de::Error as SerdeDeError};
use thiserror::Error;

pub const MAX_CLUSTER_ID_BYTES: usize = 128;
pub const MAX_NODE_HOST_BYTES: usize = 253;
pub const MAX_ACTOR_PATH_DEPTH: usize = 64;
pub const MAX_ACTOR_PATH_BYTES: usize = 1024;
pub const MAX_ACTOR_PATH_SEGMENT_BYTES: usize = 128;
pub const MAX_ENTITY_ID_BYTES: usize = 256;
pub const MAX_LOGICAL_KIND_BYTES: usize = 128;
pub const MAX_PLACEMENT_DOMAIN_ID_BYTES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ReferenceError {
    #[error("{field} must not be empty")]
    Empty { field: &'static str },
    #[error("{field} exceeds its {limit}-byte limit")]
    TooLong { field: &'static str, limit: usize },
    #[error("{field} is not canonical")]
    NonCanonical { field: &'static str },
    #[error("actor path exceeds its {limit}-segment depth limit")]
    PathTooDeep { limit: usize },
    #[error("untrusted actor paths cannot enter the reserved /system namespace")]
    ReservedSystemPath,
    #[error("protocol ID zero is reserved")]
    ReservedProtocolId,
    #[error("reference protocol ID {actual} does not match expected protocol ID {expected}")]
    ProtocolMismatch { expected: u64, actual: u64 },
    #[error("activation local sequence zero is reserved")]
    ReservedActivationSequence,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClusterId(Arc<str>);

impl ClusterId {
    pub fn new(value: impl Into<String>) -> Result<Self, ReferenceError> {
        Ok(Self(
            validate_token(value.into(), "cluster ID", MAX_CLUSTER_ID_BYTES)?.into(),
        ))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ClusterId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeAddress {
    host: Arc<str>,
    port: u16,
}

impl NodeAddress {
    pub fn new(host: impl Into<String>, port: u16) -> Result<Self, ReferenceError> {
        let host = validate_token(host.into(), "node host", MAX_NODE_HOST_BYTES)?;
        let is_ip = host.parse::<IpAddr>().is_ok();
        if port == 0
            || host.contains('/')
            || (!is_ip && host.contains(':'))
            || host.starts_with('[')
            || host.ends_with(']')
            || host.chars().any(char::is_whitespace)
        {
            return Err(ReferenceError::NonCanonical {
                field: "node address",
            });
        }
        Ok(Self {
            host: host.into(),
            port,
        })
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

impl fmt::Display for NodeAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.parse::<Ipv6Addr>().is_ok() {
            write!(formatter, "[{}]:{}", self.host, self.port)
        } else {
            write!(formatter, "{}:{}", self.host, self.port)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeIncarnation(u128);

impl NodeIncarnation {
    pub fn new(value: u128) -> Result<Self, ReferenceError> {
        if value == 0 {
            return Err(ReferenceError::NonCanonical {
                field: "node incarnation",
            });
        }
        Ok(Self(value))
    }

    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().as_u128())
    }

    pub fn get(self) -> u128 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ActivationId {
    node_incarnation: NodeIncarnation,
    local_sequence: u64,
}

impl ActivationId {
    pub fn new(
        node_incarnation: NodeIncarnation,
        local_sequence: u64,
    ) -> Result<Self, ReferenceError> {
        if local_sequence == 0 {
            return Err(ReferenceError::ReservedActivationSequence);
        }
        Ok(Self {
            node_incarnation,
            local_sequence,
        })
    }

    pub fn node_incarnation(self) -> NodeIncarnation {
        self.node_incarnation
    }

    pub fn local_sequence(self) -> u64 {
        self.local_sequence
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ActorPath {
    segments: Arc<[String]>,
}

impl ActorPath {
    pub fn user<I, S>(segments: I) -> Result<Self, ReferenceError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::from_segments(segments, false)
    }

    pub fn child(&self, segment: impl Into<String>) -> Result<Self, ReferenceError> {
        Self::from_segments(
            self.segments
                .iter()
                .cloned()
                .chain(std::iter::once(segment.into())),
            self.is_system(),
        )
    }

    pub fn segments(&self) -> impl ExactSizeIterator<Item = &str> {
        self.segments.iter().map(String::as_str)
    }

    pub fn is_system(&self) -> bool {
        self.segments
            .first()
            .is_some_and(|segment| segment == "system")
    }

    fn from_segments<I, S>(segments: I, allow_system: bool) -> Result<Self, ReferenceError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let segments = segments.into_iter().map(Into::into).collect::<Vec<_>>();
        if segments.is_empty() {
            return Err(ReferenceError::Empty {
                field: "actor path",
            });
        }
        if segments.len() > MAX_ACTOR_PATH_DEPTH {
            return Err(ReferenceError::PathTooDeep {
                limit: MAX_ACTOR_PATH_DEPTH,
            });
        }
        for segment in &segments {
            validate_path_segment(segment)?;
        }
        if !allow_system && segments[0] == "system" {
            return Err(ReferenceError::ReservedSystemPath);
        }
        let encoded_len = 1 + segments
            .iter()
            .map(|segment| segment.len() + 1)
            .sum::<usize>()
            - 1;
        if encoded_len > MAX_ACTOR_PATH_BYTES {
            return Err(ReferenceError::TooLong {
                field: "actor path",
                limit: MAX_ACTOR_PATH_BYTES,
            });
        }
        Ok(Self {
            segments: segments.into(),
        })
    }
}

impl fmt::Display for ActorPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for segment in self.segments.iter() {
            write!(formatter, "/{segment}")?;
        }
        Ok(())
    }
}

impl TryFrom<String> for ActorPath {
    type Error = ReferenceError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if !value.starts_with('/') || value.ends_with('/') || value.contains("//") {
            return Err(ReferenceError::NonCanonical {
                field: "actor path",
            });
        }
        Self::user(value[1..].split('/'))
    }
}

impl From<ActorPath> for String {
    fn from(value: ActorPath) -> Self {
        value.to_string()
    }
}

impl Serialize for ActorPath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ActorPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .try_into()
            .map_err(SerdeDeError::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProtocolId(u64);

impl ProtocolId {
    pub fn new(value: u64) -> Result<Self, ReferenceError> {
        if value == 0 {
            return Err(ReferenceError::ReservedProtocolId);
        }
        Ok(Self(value))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

/// A zero-sized type tag carried by typed actor references.
///
/// Concrete protocol tags declare their stable wire protocol ID. The erased
/// tag deliberately accepts every valid protocol ID so infrastructure can
/// route and observe references without knowing their application protocol.
pub trait ProtocolTag: fmt::Debug + Clone + PartialEq + Eq + Hash + Send + Sync + 'static {
    const PROTOCOL_ID: Option<u64>;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct ErasedProtocol;

impl ProtocolTag for ErasedProtocol {
    const PROTOCOL_ID: Option<u64> = None;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "Vec<u8>", into = "Vec<u8>")]
pub struct EntityId(Vec<u8>);

impl EntityId {
    pub fn new(value: impl Into<Vec<u8>>) -> Result<Self, ReferenceError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ReferenceError::Empty { field: "entity ID" });
        }
        if value.len() > MAX_ENTITY_ID_BYTES {
            return Err(ReferenceError::TooLong {
                field: "entity ID",
                limit: MAX_ENTITY_ID_BYTES,
            });
        }
        Ok(Self(value))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl TryFrom<Vec<u8>> for EntityId {
    type Error = ReferenceError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<EntityId> for Vec<u8> {
    fn from(value: EntityId) -> Self {
        value.0
    }
}

macro_rules! bounded_kind {
    ($name:ident, $field:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, ReferenceError> {
                Ok(Self(validate_token(
                    value.into(),
                    $field,
                    MAX_LOGICAL_KIND_BYTES,
                )?))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

bounded_kind!(EntityType, "entity type");
bounded_kind!(SingletonKind, "singleton kind");

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct PlacementDomainId(String);

impl PlacementDomainId {
    pub fn new(value: impl Into<String>) -> Result<Self, ReferenceError> {
        Ok(Self(validate_token(
            value.into(),
            "placement domain ID",
            MAX_PLACEMENT_DOMAIN_ID_BYTES,
        )?))
    }

    pub fn from_entity_type(entity_type: &EntityType) -> Self {
        Self(entity_type.as_str().to_owned())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PlacementDomainId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for PlacementDomainId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(SerdeDeError::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ConfigFingerprint([u8; 32]);

impl ConfigFingerprint {
    pub const fn new(value: [u8; 32]) -> Self {
        Self(value)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

fn validate_token(
    value: String,
    field: &'static str,
    limit: usize,
) -> Result<String, ReferenceError> {
    if value.is_empty() {
        return Err(ReferenceError::Empty { field });
    }
    if value.len() > limit {
        return Err(ReferenceError::TooLong { field, limit });
    }
    if value == "."
        || value == ".."
        || value.contains(['/', '\\', '\0'])
        || value.chars().any(char::is_control)
    {
        return Err(ReferenceError::NonCanonical { field });
    }
    Ok(value)
}

fn validate_path_segment(segment: &str) -> Result<(), ReferenceError> {
    validate_token(
        segment.to_owned(),
        "actor path segment",
        MAX_ACTOR_PATH_SEGMENT_BYTES,
    )?;
    if segment.contains('%') {
        return Err(ReferenceError::NonCanonical {
            field: "actor path segment",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_rejects_reserved_and_noncanonical_segments() {
        assert_eq!(
            ActorPath::user(["system", "coordinator"]),
            Err(ReferenceError::ReservedSystemPath)
        );
        assert!(ActorPath::user(["user", ".."]).is_err());
        assert!(ActorPath::user(["user", "child/name"]).is_err());
    }

    #[test]
    fn immutable_exact_identity_parts_clone_by_sharing_storage() {
        let cluster = ClusterId::new("test").unwrap();
        let cluster_clone = cluster.clone();
        assert!(Arc::ptr_eq(&cluster.0, &cluster_clone.0));

        let address = NodeAddress::new("127.0.0.1", 25520).unwrap();
        let address_clone = address.clone();
        assert!(Arc::ptr_eq(&address.host, &address_clone.host));

        let path = ActorPath::user(["user", "actor"]).unwrap();
        let path_clone = path.clone();
        assert!(Arc::ptr_eq(&path.segments, &path_clone.segments));

        assert_eq!(serde_json::to_string(&cluster).unwrap(), "\"test\"");
        assert_eq!(
            serde_json::from_str::<ActorPath>("\"/user/actor\"").unwrap(),
            path
        );
    }

    #[test]
    fn serde_cannot_construct_a_reserved_system_path() {
        let result = serde_json::from_str::<ActorPath>("\"/system/coordinator\"");
        assert!(result.is_err());
    }

    #[test]
    fn placement_domain_id_is_bounded_canonical_and_serialized_explicitly() {
        let domain = PlacementDomainId::new("player").unwrap();
        assert_eq!(domain.as_str(), "player");
        assert_eq!(serde_json::to_string(&domain).unwrap(), "\"player\"");
        assert_eq!(
            serde_json::from_str::<PlacementDomainId>("\"player\"").unwrap(),
            domain
        );
        assert!(PlacementDomainId::new("").is_err());
        assert!(PlacementDomainId::new("player/world").is_err());
        assert!(PlacementDomainId::new("player\\world").is_err());
        assert!(PlacementDomainId::new("player\nworld").is_err());
        assert!(PlacementDomainId::new("x".repeat(MAX_PLACEMENT_DOMAIN_ID_BYTES + 1)).is_err());
        for invalid in ["", ".", "..", "a/b", "a\\b", "a\0b", "a\nb"] {
            let encoded = serde_json::to_string(invalid).unwrap();
            assert!(serde_json::from_str::<PlacementDomainId>(&encoded).is_err());
        }
    }

    #[test]
    fn placement_domain_id_round_trips_generated_canonical_tokens() {
        let alphabet = b"abcdefghijklmnopqrstuvwxyz0123456789-_";
        let mut state = 0x9e37_79b9_u64;
        for length in 1..=MAX_PLACEMENT_DOMAIN_ID_BYTES {
            let mut value = String::with_capacity(length);
            for _ in 0..length {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                value.push(alphabet[(state as usize) % alphabet.len()] as char);
            }
            let domain = PlacementDomainId::new(value).unwrap();
            let encoded = serde_json::to_vec(&domain).unwrap();
            assert_eq!(
                serde_json::from_slice::<PlacementDomainId>(&encoded).unwrap(),
                domain
            );
        }
    }
}
