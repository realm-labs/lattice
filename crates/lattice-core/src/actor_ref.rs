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

/// A zero-sized type tag carried by typed actor references.
///
/// Concrete protocol tags declare their stable wire protocol ID. The erased
/// tag deliberately accepts every valid protocol ID so infrastructure can
/// route and observe references without knowing their application protocol.
pub trait ProtocolTag:
    fmt::Debug + Clone + PartialEq + Eq + std::hash::Hash + Send + Sync + 'static
{
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
        Self::new(value).map_err(serde::de::Error::custom)
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(bound = "")]
pub struct ActorRef<P: ProtocolTag = ErasedProtocol> {
    cluster_id: ClusterId,
    node_address: NodeAddress,
    node_incarnation: NodeIncarnation,
    actor_path: ActorPath,
    activation_id: ActivationId,
    protocol_id: ProtocolId,
    #[serde(skip)]
    protocol: PhantomData<fn() -> P>,
}

impl<P: ProtocolTag> ActorRef<P> {
    fn from_parts(
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
        validate_protocol::<P>(protocol_id)?;
        Ok(Self {
            cluster_id,
            node_address,
            node_incarnation,
            actor_path,
            activation_id,
            protocol_id,
            protocol: PhantomData,
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

    pub fn try_typed<Q: ProtocolTag>(&self) -> Result<ActorRef<Q>, ReferenceError> {
        ActorRef::from_parts(
            self.cluster_id.clone(),
            self.node_address.clone(),
            self.node_incarnation,
            self.actor_path.clone(),
            self.activation_id,
            self.protocol_id,
        )
    }

    pub fn erase(&self) -> ActorRef<ErasedProtocol> {
        ActorRef {
            cluster_id: self.cluster_id.clone(),
            node_address: self.node_address.clone(),
            node_incarnation: self.node_incarnation,
            actor_path: self.actor_path.clone(),
            activation_id: self.activation_id,
            protocol_id: self.protocol_id,
            protocol: PhantomData,
        }
    }

    pub fn same_activation<Q: ProtocolTag>(&self, other: &ActorRef<Q>) -> bool {
        self.cluster_id == other.cluster_id
            && self.node_address == other.node_address
            && self.node_incarnation == other.node_incarnation
            && self.actor_path == other.actor_path
            && self.activation_id == other.activation_id
            && self.protocol_id == other.protocol_id
    }
}

impl ActorRef<ErasedProtocol> {
    pub fn new(
        cluster_id: ClusterId,
        node_address: NodeAddress,
        node_incarnation: NodeIncarnation,
        actor_path: ActorPath,
        activation_id: ActivationId,
        protocol_id: ProtocolId,
    ) -> Result<Self, ReferenceError> {
        Self::from_parts(
            cluster_id,
            node_address,
            node_incarnation,
            actor_path,
            activation_id,
            protocol_id,
        )
    }
}

#[derive(Deserialize)]
struct ActorRefData {
    cluster_id: ClusterId,
    node_address: NodeAddress,
    node_incarnation: NodeIncarnation,
    actor_path: ActorPath,
    activation_id: ActivationId,
    protocol_id: ProtocolId,
}

impl<'de, P: ProtocolTag> Deserialize<'de> for ActorRef<P> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = ActorRefData::deserialize(deserializer)?;
        Self::from_parts(
            data.cluster_id,
            data.node_address,
            data.node_incarnation,
            data.actor_path,
            data.activation_id,
            data.protocol_id,
        )
        .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(bound = "")]
pub struct EntityRef<P: ProtocolTag = ErasedProtocol> {
    cluster_id: ClusterId,
    domain: PlacementDomainId,
    entity_type: EntityType,
    entity_id: EntityId,
    protocol_id: ProtocolId,
    entity_config_fingerprint: ConfigFingerprint,
    #[serde(skip)]
    protocol: PhantomData<fn() -> P>,
}

impl<P: ProtocolTag> EntityRef<P> {
    fn from_parts(
        cluster_id: ClusterId,
        domain: PlacementDomainId,
        entity_type: EntityType,
        entity_id: EntityId,
        protocol_id: ProtocolId,
        entity_config_fingerprint: ConfigFingerprint,
    ) -> Result<Self, ReferenceError> {
        validate_protocol::<P>(protocol_id)?;
        Ok(Self {
            cluster_id,
            domain,
            entity_type,
            entity_id,
            protocol_id,
            entity_config_fingerprint,
            protocol: PhantomData,
        })
    }

    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    pub fn domain(&self) -> &PlacementDomainId {
        &self.domain
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

    pub fn try_typed<Q: ProtocolTag>(&self) -> Result<EntityRef<Q>, ReferenceError> {
        EntityRef::from_parts(
            self.cluster_id.clone(),
            self.domain.clone(),
            self.entity_type.clone(),
            self.entity_id.clone(),
            self.protocol_id,
            self.entity_config_fingerprint,
        )
    }

    pub fn erase(&self) -> EntityRef<ErasedProtocol> {
        EntityRef {
            cluster_id: self.cluster_id.clone(),
            domain: self.domain.clone(),
            entity_type: self.entity_type.clone(),
            entity_id: self.entity_id.clone(),
            protocol_id: self.protocol_id,
            entity_config_fingerprint: self.entity_config_fingerprint,
            protocol: PhantomData,
        }
    }
}

impl EntityRef<ErasedProtocol> {
    pub fn new(
        cluster_id: ClusterId,
        domain: PlacementDomainId,
        entity_type: EntityType,
        entity_id: EntityId,
        protocol_id: ProtocolId,
        entity_config_fingerprint: ConfigFingerprint,
    ) -> Result<Self, ReferenceError> {
        Self::from_parts(
            cluster_id,
            domain,
            entity_type,
            entity_id,
            protocol_id,
            entity_config_fingerprint,
        )
    }
}

#[derive(Deserialize)]
struct EntityRefData {
    cluster_id: ClusterId,
    domain: PlacementDomainId,
    entity_type: EntityType,
    entity_id: EntityId,
    protocol_id: ProtocolId,
    entity_config_fingerprint: ConfigFingerprint,
}

impl<'de, P: ProtocolTag> Deserialize<'de> for EntityRef<P> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = EntityRefData::deserialize(deserializer)?;
        Self::from_parts(
            data.cluster_id,
            data.domain,
            data.entity_type,
            data.entity_id,
            data.protocol_id,
            data.entity_config_fingerprint,
        )
        .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(bound = "")]
pub struct SingletonRef<P: ProtocolTag = ErasedProtocol> {
    cluster_id: ClusterId,
    domain: PlacementDomainId,
    singleton_kind: SingletonKind,
    protocol_id: ProtocolId,
    singleton_config_fingerprint: ConfigFingerprint,
    #[serde(skip)]
    protocol: PhantomData<fn() -> P>,
}

impl<P: ProtocolTag> SingletonRef<P> {
    fn from_parts(
        cluster_id: ClusterId,
        domain: PlacementDomainId,
        singleton_kind: SingletonKind,
        protocol_id: ProtocolId,
        singleton_config_fingerprint: ConfigFingerprint,
    ) -> Result<Self, ReferenceError> {
        validate_protocol::<P>(protocol_id)?;
        Ok(Self {
            cluster_id,
            domain,
            singleton_kind,
            protocol_id,
            singleton_config_fingerprint,
            protocol: PhantomData,
        })
    }

    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    pub fn domain(&self) -> &PlacementDomainId {
        &self.domain
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

    pub fn try_typed<Q: ProtocolTag>(&self) -> Result<SingletonRef<Q>, ReferenceError> {
        SingletonRef::from_parts(
            self.cluster_id.clone(),
            self.domain.clone(),
            self.singleton_kind.clone(),
            self.protocol_id,
            self.singleton_config_fingerprint,
        )
    }

    pub fn erase(&self) -> SingletonRef<ErasedProtocol> {
        SingletonRef {
            cluster_id: self.cluster_id.clone(),
            domain: self.domain.clone(),
            singleton_kind: self.singleton_kind.clone(),
            protocol_id: self.protocol_id,
            singleton_config_fingerprint: self.singleton_config_fingerprint,
            protocol: PhantomData,
        }
    }
}

impl SingletonRef<ErasedProtocol> {
    pub fn new(
        cluster_id: ClusterId,
        domain: PlacementDomainId,
        singleton_kind: SingletonKind,
        protocol_id: ProtocolId,
        singleton_config_fingerprint: ConfigFingerprint,
    ) -> Result<Self, ReferenceError> {
        Self::from_parts(
            cluster_id,
            domain,
            singleton_kind,
            protocol_id,
            singleton_config_fingerprint,
        )
    }
}

#[derive(Deserialize)]
struct SingletonRefData {
    cluster_id: ClusterId,
    domain: PlacementDomainId,
    singleton_kind: SingletonKind,
    protocol_id: ProtocolId,
    singleton_config_fingerprint: ConfigFingerprint,
}

impl<'de, P: ProtocolTag> Deserialize<'de> for SingletonRef<P> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = SingletonRefData::deserialize(deserializer)?;
        Self::from_parts(
            data.cluster_id,
            data.domain,
            data.singleton_kind,
            data.protocol_id,
            data.singleton_config_fingerprint,
        )
        .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(bound = "P: ProtocolTag")]
#[doc(hidden)]
pub enum RecipientRef<P: ProtocolTag = ErasedProtocol> {
    Actor(ActorRef<P>),
    Entity(EntityRef<P>),
    Singleton(SingletonRef<P>),
}

impl<P: ProtocolTag> RecipientRef<P> {
    pub fn erase(&self) -> RecipientRef<ErasedProtocol> {
        match self {
            Self::Actor(reference) => RecipientRef::Actor(reference.erase()),
            Self::Entity(reference) => RecipientRef::Entity(reference.erase()),
            Self::Singleton(reference) => RecipientRef::Singleton(reference.erase()),
        }
    }
}

impl<P: ProtocolTag> From<ActorRef<P>> for RecipientRef<P> {
    fn from(reference: ActorRef<P>) -> Self {
        Self::Actor(reference)
    }
}

impl<P: ProtocolTag> From<&ActorRef<P>> for RecipientRef<P> {
    fn from(reference: &ActorRef<P>) -> Self {
        Self::Actor(reference.clone())
    }
}

impl<P: ProtocolTag> From<EntityRef<P>> for RecipientRef<P> {
    fn from(reference: EntityRef<P>) -> Self {
        Self::Entity(reference)
    }
}

impl<P: ProtocolTag> From<&EntityRef<P>> for RecipientRef<P> {
    fn from(reference: &EntityRef<P>) -> Self {
        Self::Entity(reference.clone())
    }
}

impl<P: ProtocolTag> From<SingletonRef<P>> for RecipientRef<P> {
    fn from(reference: SingletonRef<P>) -> Self {
        Self::Singleton(reference)
    }
}

impl<P: ProtocolTag> From<&SingletonRef<P>> for RecipientRef<P> {
    fn from(reference: &SingletonRef<P>) -> Self {
        Self::Singleton(reference.clone())
    }
}

fn validate_protocol<P: ProtocolTag>(protocol_id: ProtocolId) -> Result<(), ReferenceError> {
    if let Some(expected) = P::PROTOCOL_ID
        && expected != protocol_id.get()
    {
        return Err(ReferenceError::ProtocolMismatch {
            expected,
            actual: protocol_id.get(),
        });
    }
    Ok(())
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

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    struct TestProtocol;

    impl ProtocolTag for TestProtocol {
        const PROTOCOL_ID: Option<u64> = Some(7);
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    struct OtherProtocol;

    impl ProtocolTag for OtherProtocol {
        const PROTOCOL_ID: Option<u64> = Some(8);
    }

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
        let result = ActorRef::new(
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

    #[test]
    fn typed_reference_conversion_and_deserialization_validate_protocol_id() {
        let incarnation = NodeIncarnation::new(3).unwrap();
        let erased = ActorRef::new(
            ClusterId::new("test").unwrap(),
            NodeAddress::new("127.0.0.1", 25520).unwrap(),
            incarnation,
            ActorPath::user(["user", "actor"]).unwrap(),
            ActivationId::new(incarnation, 1).unwrap(),
            ProtocolId::new(7).unwrap(),
        )
        .unwrap();

        let typed = erased.try_typed::<TestProtocol>().unwrap();
        assert!(typed.same_activation(&erased));
        assert!(matches!(
            erased.try_typed::<OtherProtocol>(),
            Err(ReferenceError::ProtocolMismatch {
                expected: 8,
                actual: 7
            })
        ));

        let encoded = serde_json::to_vec(&typed).unwrap();
        let decoded: ActorRef<TestProtocol> = serde_json::from_slice(&encoded).unwrap();
        assert!(decoded.same_activation(&typed));
        assert!(serde_json::from_slice::<ActorRef<OtherProtocol>>(&encoded).is_err());
        assert_eq!(
            serde_json::to_value(&typed).unwrap(),
            serde_json::to_value(&erased).unwrap()
        );

        let entity = EntityRef::new(
            ClusterId::new("test").unwrap(),
            PlacementDomainId::new("world").unwrap(),
            EntityType::new("world").unwrap(),
            EntityId::new(b"entity-1".to_vec()).unwrap(),
            ProtocolId::new(7).unwrap(),
            ConfigFingerprint::new([1; 32]),
        )
        .unwrap()
        .try_typed::<TestProtocol>()
        .unwrap();
        let encoded = serde_json::to_vec(&entity).unwrap();
        assert!(serde_json::from_slice::<EntityRef<TestProtocol>>(&encoded).is_ok());
        assert!(serde_json::from_slice::<EntityRef<OtherProtocol>>(&encoded).is_err());

        let singleton = SingletonRef::new(
            ClusterId::new("test").unwrap(),
            PlacementDomainId::new("control").unwrap(),
            SingletonKind::new("leader").unwrap(),
            ProtocolId::new(7).unwrap(),
            ConfigFingerprint::new([2; 32]),
        )
        .unwrap()
        .try_typed::<TestProtocol>()
        .unwrap();
        let encoded = serde_json::to_vec(&singleton).unwrap();
        assert!(serde_json::from_slice::<SingletonRef<TestProtocol>>(&encoded).is_ok());
        assert!(serde_json::from_slice::<SingletonRef<OtherProtocol>>(&encoded).is_err());
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
