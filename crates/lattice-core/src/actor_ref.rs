use std::fmt;
use std::marker::PhantomData;
use std::net::IpAddr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const MAX_CLUSTER_ID_BYTES: usize = 128;
pub const MAX_NODE_HOST_BYTES: usize = 253;
pub const MAX_ACTOR_PATH_DEPTH: usize = 64;
pub const MAX_ACTOR_PATH_BYTES: usize = 1024;
pub const MAX_ACTOR_PATH_SEGMENT_BYTES: usize = 128;
pub const MAX_ENTITY_ID_BYTES: usize = 256;
pub const MAX_LOGICAL_KIND_BYTES: usize = 128;

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
    #[error("activation local sequence zero is reserved")]
    ReservedActivationSequence,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClusterId(String);

impl ClusterId {
    pub fn new(value: impl Into<String>) -> Result<Self, ReferenceError> {
        Ok(Self(validate_token(
            value.into(),
            "cluster ID",
            MAX_CLUSTER_ID_BYTES,
        )?))
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
    host: String,
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
        Ok(Self { host, port })
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
        if self.host.parse::<std::net::Ipv6Addr>().is_ok() {
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
    segments: Vec<String>,
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
        let mut segments = self.segments.clone();
        segments.push(segment.into());
        Self::from_segments(segments, self.is_system())
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
        Ok(Self { segments })
    }
}

impl fmt::Display for ActorPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for segment in &self.segments {
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
            .map_err(serde::de::Error::custom)
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct ActorRef<A = ()> {
    cluster_id: ClusterId,
    node_address: NodeAddress,
    node_incarnation: NodeIncarnation,
    actor_path: ActorPath,
    activation_id: ActivationId,
    protocol_id: ProtocolId,
    #[serde(skip)]
    actor: PhantomData<fn() -> A>,
}

impl<A> ActorRef<A> {
    pub fn new(
        cluster_id: ClusterId,
        node_address: NodeAddress,
        node_incarnation: NodeIncarnation,
        actor_path: ActorPath,
        activation_id: ActivationId,
        protocol_id: ProtocolId,
    ) -> Result<Self, ReferenceError> {
        if activation_id.node_incarnation() != node_incarnation {
            return Err(ReferenceError::NonCanonical {
                field: "activation node incarnation",
            });
        }
        Ok(Self {
            cluster_id,
            node_address,
            node_incarnation,
            actor_path,
            activation_id,
            protocol_id,
            actor: PhantomData,
        })
    }

    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    pub fn node_address(&self) -> &NodeAddress {
        &self.node_address
    }

    pub fn node_incarnation(&self) -> NodeIncarnation {
        self.node_incarnation
    }

    pub fn actor_path(&self) -> &ActorPath {
        &self.actor_path
    }

    pub fn activation_id(&self) -> ActivationId {
        self.activation_id
    }

    pub fn protocol_id(&self) -> ProtocolId {
        self.protocol_id
    }

    pub fn cast<B>(&self) -> ActorRef<B> {
        ActorRef {
            cluster_id: self.cluster_id.clone(),
            node_address: self.node_address.clone(),
            node_incarnation: self.node_incarnation,
            actor_path: self.actor_path.clone(),
            activation_id: self.activation_id,
            protocol_id: self.protocol_id,
            actor: PhantomData,
        }
    }

    pub fn erase(&self) -> ActorRef<()> {
        self.cast()
    }

    pub fn same_activation<B>(&self, other: &ActorRef<B>) -> bool {
        self.cluster_id == other.cluster_id
            && self.node_address == other.node_address
            && self.node_incarnation == other.node_incarnation
            && self.actor_path == other.actor_path
            && self.activation_id == other.activation_id
            && self.protocol_id == other.protocol_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct EntityRef<A = ()> {
    cluster_id: ClusterId,
    entity_type: EntityType,
    entity_id: EntityId,
    protocol_id: ProtocolId,
    entity_config_fingerprint: ConfigFingerprint,
    #[serde(skip)]
    actor: PhantomData<fn() -> A>,
}

impl<A> EntityRef<A> {
    pub fn new(
        cluster_id: ClusterId,
        entity_type: EntityType,
        entity_id: EntityId,
        protocol_id: ProtocolId,
        entity_config_fingerprint: ConfigFingerprint,
    ) -> Self {
        Self {
            cluster_id,
            entity_type,
            entity_id,
            protocol_id,
            entity_config_fingerprint,
            actor: PhantomData,
        }
    }

    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    pub fn entity_type(&self) -> &EntityType {
        &self.entity_type
    }

    pub fn entity_id(&self) -> &EntityId {
        &self.entity_id
    }

    pub fn protocol_id(&self) -> ProtocolId {
        self.protocol_id
    }

    pub fn config_fingerprint(&self) -> ConfigFingerprint {
        self.entity_config_fingerprint
    }

    pub fn cast<B>(&self) -> EntityRef<B> {
        EntityRef {
            cluster_id: self.cluster_id.clone(),
            entity_type: self.entity_type.clone(),
            entity_id: self.entity_id.clone(),
            protocol_id: self.protocol_id,
            entity_config_fingerprint: self.entity_config_fingerprint,
            actor: PhantomData,
        }
    }

    pub fn erase(&self) -> EntityRef<()> {
        self.cast()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct SingletonRef<A = ()> {
    cluster_id: ClusterId,
    singleton_kind: SingletonKind,
    protocol_id: ProtocolId,
    singleton_config_fingerprint: ConfigFingerprint,
    #[serde(skip)]
    actor: PhantomData<fn() -> A>,
}

impl<A> SingletonRef<A> {
    pub fn new(
        cluster_id: ClusterId,
        singleton_kind: SingletonKind,
        protocol_id: ProtocolId,
        singleton_config_fingerprint: ConfigFingerprint,
    ) -> Self {
        Self {
            cluster_id,
            singleton_kind,
            protocol_id,
            singleton_config_fingerprint,
            actor: PhantomData,
        }
    }

    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    pub fn singleton_kind(&self) -> &SingletonKind {
        &self.singleton_kind
    }

    pub fn protocol_id(&self) -> ProtocolId {
        self.protocol_id
    }

    pub fn config_fingerprint(&self) -> ConfigFingerprint {
        self.singleton_config_fingerprint
    }

    pub fn cast<B>(&self) -> SingletonRef<B> {
        SingletonRef {
            cluster_id: self.cluster_id.clone(),
            singleton_kind: self.singleton_kind.clone(),
            protocol_id: self.protocol_id,
            singleton_config_fingerprint: self.singleton_config_fingerprint,
            actor: PhantomData,
        }
    }

    pub fn erase(&self) -> SingletonRef<()> {
        self.cast()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(bound = "")]
#[doc(hidden)]
pub enum RecipientRef<A = ()> {
    Actor(ActorRef<A>),
    Entity(EntityRef<A>),
    Singleton(SingletonRef<A>),
}

impl<A> RecipientRef<A> {
    pub fn erase(&self) -> RecipientRef<()> {
        match self {
            Self::Actor(reference) => RecipientRef::Actor(reference.erase()),
            Self::Entity(reference) => RecipientRef::Entity(reference.erase()),
            Self::Singleton(reference) => RecipientRef::Singleton(reference.erase()),
        }
    }
}

impl<A> From<ActorRef<A>> for RecipientRef<A> {
    fn from(reference: ActorRef<A>) -> Self {
        Self::Actor(reference)
    }
}

impl<A> From<&ActorRef<A>> for RecipientRef<A> {
    fn from(reference: &ActorRef<A>) -> Self {
        Self::Actor(reference.cast())
    }
}

impl<A> From<EntityRef<A>> for RecipientRef<A> {
    fn from(reference: EntityRef<A>) -> Self {
        Self::Entity(reference)
    }
}

impl<A> From<&EntityRef<A>> for RecipientRef<A> {
    fn from(reference: &EntityRef<A>) -> Self {
        Self::Entity(reference.cast())
    }
}

impl<A> From<SingletonRef<A>> for RecipientRef<A> {
    fn from(reference: SingletonRef<A>) -> Self {
        Self::Singleton(reference)
    }
}

impl<A> From<&SingletonRef<A>> for RecipientRef<A> {
    fn from(reference: &SingletonRef<A>) -> Self {
        Self::Singleton(reference.cast())
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
    fn actor_reference_requires_activation_from_the_named_node() {
        let node = NodeIncarnation::new(1).unwrap();
        let other = NodeIncarnation::new(2).unwrap();
        let result = ActorRef::<()>::new(
            ClusterId::new("test").unwrap(),
            NodeAddress::new("127.0.0.1", 25520).unwrap(),
            node,
            ActorPath::user(["user", "actor"]).unwrap(),
            ActivationId::new(other, 1).unwrap(),
            ProtocolId::new(7).unwrap(),
        );
        assert!(matches!(result, Err(ReferenceError::NonCanonical { .. })));
    }

    #[test]
    fn serde_cannot_construct_a_reserved_system_path() {
        let result = serde_json::from_str::<ActorPath>("\"/system/coordinator\"");
        assert!(result.is_err());
    }
}
