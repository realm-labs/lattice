use std::marker::PhantomData;

use serde::{Deserialize, Serialize, de::Error as SerdeDeError};

use crate::actor_address::identity::{
    ActivationId, ActorPath, AddressError, ClusterId, ConfigFingerprint, EntityId, EntityType,
    ErasedProtocol, NodeAddress, NodeIncarnation, PlacementDomainId, ProtocolId, ProtocolTag,
    SingletonKind,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(bound = "")]
pub struct ActorAddress<P: ProtocolTag = ErasedProtocol> {
    cluster_id: ClusterId,
    node_address: NodeAddress,
    node_incarnation: NodeIncarnation,
    actor_path: ActorPath,
    activation_id: ActivationId,
    protocol_id: ProtocolId,
    #[serde(skip)]
    protocol: PhantomData<fn() -> P>,
}

impl<P: ProtocolTag> ActorAddress<P> {
    fn from_parts(
        cluster_id: ClusterId,
        node_address: NodeAddress,
        node_incarnation: NodeIncarnation,
        actor_path: ActorPath,
        activation_id: ActivationId,
        protocol_id: ProtocolId,
    ) -> Result<Self, AddressError> {
        if activation_id.node_incarnation() != node_incarnation {
            return Err(AddressError::NonCanonical {
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

    pub fn try_typed<Q: ProtocolTag>(&self) -> Result<ActorAddress<Q>, AddressError> {
        ActorAddress::from_parts(
            self.cluster_id.clone(),
            self.node_address.clone(),
            self.node_incarnation,
            self.actor_path.clone(),
            self.activation_id,
            self.protocol_id,
        )
    }

    pub fn erase(&self) -> ActorAddress<ErasedProtocol> {
        ActorAddress {
            cluster_id: self.cluster_id.clone(),
            node_address: self.node_address.clone(),
            node_incarnation: self.node_incarnation,
            actor_path: self.actor_path.clone(),
            activation_id: self.activation_id,
            protocol_id: self.protocol_id,
            protocol: PhantomData,
        }
    }

    pub fn same_activation<Q: ProtocolTag>(&self, other: &ActorAddress<Q>) -> bool {
        self.cluster_id == other.cluster_id
            && self.node_address == other.node_address
            && self.node_incarnation == other.node_incarnation
            && self.actor_path == other.actor_path
            && self.activation_id == other.activation_id
            && self.protocol_id == other.protocol_id
    }
}

impl ActorAddress<ErasedProtocol> {
    pub fn new(
        cluster_id: ClusterId,
        node_address: NodeAddress,
        node_incarnation: NodeIncarnation,
        actor_path: ActorPath,
        activation_id: ActivationId,
        protocol_id: ProtocolId,
    ) -> Result<Self, AddressError> {
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
struct ActorAddressData {
    cluster_id: ClusterId,
    node_address: NodeAddress,
    node_incarnation: NodeIncarnation,
    actor_path: ActorPath,
    activation_id: ActivationId,
    protocol_id: ProtocolId,
}

impl<'de, P: ProtocolTag> Deserialize<'de> for ActorAddress<P> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = ActorAddressData::deserialize(deserializer)?;
        Self::from_parts(
            data.cluster_id,
            data.node_address,
            data.node_incarnation,
            data.actor_path,
            data.activation_id,
            data.protocol_id,
        )
        .map_err(SerdeDeError::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(bound = "")]
pub struct EntityAddress<P: ProtocolTag = ErasedProtocol> {
    cluster_id: ClusterId,
    domain: PlacementDomainId,
    entity_type: EntityType,
    entity_id: EntityId,
    protocol_id: ProtocolId,
    entity_config_fingerprint: ConfigFingerprint,
    #[serde(skip)]
    protocol: PhantomData<fn() -> P>,
}

impl<P: ProtocolTag> EntityAddress<P> {
    fn from_parts(
        cluster_id: ClusterId,
        domain: PlacementDomainId,
        entity_type: EntityType,
        entity_id: EntityId,
        protocol_id: ProtocolId,
        entity_config_fingerprint: ConfigFingerprint,
    ) -> Result<Self, AddressError> {
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

    pub fn try_typed<Q: ProtocolTag>(&self) -> Result<EntityAddress<Q>, AddressError> {
        EntityAddress::from_parts(
            self.cluster_id.clone(),
            self.domain.clone(),
            self.entity_type.clone(),
            self.entity_id.clone(),
            self.protocol_id,
            self.entity_config_fingerprint,
        )
    }

    pub fn erase(&self) -> EntityAddress<ErasedProtocol> {
        EntityAddress {
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

impl EntityAddress<ErasedProtocol> {
    pub fn new(
        cluster_id: ClusterId,
        domain: PlacementDomainId,
        entity_type: EntityType,
        entity_id: EntityId,
        protocol_id: ProtocolId,
        entity_config_fingerprint: ConfigFingerprint,
    ) -> Result<Self, AddressError> {
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
struct EntityAddressData {
    cluster_id: ClusterId,
    domain: PlacementDomainId,
    entity_type: EntityType,
    entity_id: EntityId,
    protocol_id: ProtocolId,
    entity_config_fingerprint: ConfigFingerprint,
}

impl<'de, P: ProtocolTag> Deserialize<'de> for EntityAddress<P> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = EntityAddressData::deserialize(deserializer)?;
        Self::from_parts(
            data.cluster_id,
            data.domain,
            data.entity_type,
            data.entity_id,
            data.protocol_id,
            data.entity_config_fingerprint,
        )
        .map_err(SerdeDeError::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(bound = "")]
pub struct SingletonAddress<P: ProtocolTag = ErasedProtocol> {
    cluster_id: ClusterId,
    domain: PlacementDomainId,
    singleton_kind: SingletonKind,
    protocol_id: ProtocolId,
    singleton_config_fingerprint: ConfigFingerprint,
    #[serde(skip)]
    protocol: PhantomData<fn() -> P>,
}

impl<P: ProtocolTag> SingletonAddress<P> {
    fn from_parts(
        cluster_id: ClusterId,
        domain: PlacementDomainId,
        singleton_kind: SingletonKind,
        protocol_id: ProtocolId,
        singleton_config_fingerprint: ConfigFingerprint,
    ) -> Result<Self, AddressError> {
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

    pub fn try_typed<Q: ProtocolTag>(&self) -> Result<SingletonAddress<Q>, AddressError> {
        SingletonAddress::from_parts(
            self.cluster_id.clone(),
            self.domain.clone(),
            self.singleton_kind.clone(),
            self.protocol_id,
            self.singleton_config_fingerprint,
        )
    }

    pub fn erase(&self) -> SingletonAddress<ErasedProtocol> {
        SingletonAddress {
            cluster_id: self.cluster_id.clone(),
            domain: self.domain.clone(),
            singleton_kind: self.singleton_kind.clone(),
            protocol_id: self.protocol_id,
            singleton_config_fingerprint: self.singleton_config_fingerprint,
            protocol: PhantomData,
        }
    }
}

impl SingletonAddress<ErasedProtocol> {
    pub fn new(
        cluster_id: ClusterId,
        domain: PlacementDomainId,
        singleton_kind: SingletonKind,
        protocol_id: ProtocolId,
        singleton_config_fingerprint: ConfigFingerprint,
    ) -> Result<Self, AddressError> {
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
struct SingletonAddressData {
    cluster_id: ClusterId,
    domain: PlacementDomainId,
    singleton_kind: SingletonKind,
    protocol_id: ProtocolId,
    singleton_config_fingerprint: ConfigFingerprint,
}

impl<'de, P: ProtocolTag> Deserialize<'de> for SingletonAddress<P> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = SingletonAddressData::deserialize(deserializer)?;
        Self::from_parts(
            data.cluster_id,
            data.domain,
            data.singleton_kind,
            data.protocol_id,
            data.singleton_config_fingerprint,
        )
        .map_err(SerdeDeError::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(bound = "P: ProtocolTag")]
#[doc(hidden)]
pub enum RecipientAddress<P: ProtocolTag = ErasedProtocol> {
    Actor(ActorAddress<P>),
    Entity(EntityAddress<P>),
    Singleton(SingletonAddress<P>),
}

impl<P: ProtocolTag> RecipientAddress<P> {
    pub fn erase(&self) -> RecipientAddress<ErasedProtocol> {
        match self {
            Self::Actor(reference) => RecipientAddress::Actor(reference.erase()),
            Self::Entity(reference) => RecipientAddress::Entity(reference.erase()),
            Self::Singleton(reference) => RecipientAddress::Singleton(reference.erase()),
        }
    }
}

impl<P: ProtocolTag> From<ActorAddress<P>> for RecipientAddress<P> {
    fn from(reference: ActorAddress<P>) -> Self {
        Self::Actor(reference)
    }
}

impl<P: ProtocolTag> From<&ActorAddress<P>> for RecipientAddress<P> {
    fn from(reference: &ActorAddress<P>) -> Self {
        Self::Actor(reference.clone())
    }
}

impl<P: ProtocolTag> From<EntityAddress<P>> for RecipientAddress<P> {
    fn from(reference: EntityAddress<P>) -> Self {
        Self::Entity(reference)
    }
}

impl<P: ProtocolTag> From<&EntityAddress<P>> for RecipientAddress<P> {
    fn from(reference: &EntityAddress<P>) -> Self {
        Self::Entity(reference.clone())
    }
}

impl<P: ProtocolTag> From<SingletonAddress<P>> for RecipientAddress<P> {
    fn from(reference: SingletonAddress<P>) -> Self {
        Self::Singleton(reference)
    }
}

impl<P: ProtocolTag> From<&SingletonAddress<P>> for RecipientAddress<P> {
    fn from(reference: &SingletonAddress<P>) -> Self {
        Self::Singleton(reference.clone())
    }
}

fn validate_protocol<P: ProtocolTag>(protocol_id: ProtocolId) -> Result<(), AddressError> {
    if let Some(expected) = P::PROTOCOL_ID
        && expected != protocol_id.get()
    {
        return Err(AddressError::ProtocolMismatch {
            expected,
            actual: protocol_id.get(),
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
    fn actor_address_requires_activation_from_the_named_node() {
        let node = NodeIncarnation::new(1).unwrap();
        let other = NodeIncarnation::new(2).unwrap();
        let result = ActorAddress::new(
            ClusterId::new("test").unwrap(),
            NodeAddress::new("127.0.0.1", 25520).unwrap(),
            node,
            ActorPath::user(["user", "actor"]).unwrap(),
            ActivationId::new(other, 1).unwrap(),
            ProtocolId::new(7).unwrap(),
        );
        assert!(matches!(result, Err(AddressError::NonCanonical { .. })));
    }

    #[test]
    fn typed_address_conversion_and_deserialization_validate_protocol_id() {
        let incarnation = NodeIncarnation::new(3).unwrap();
        let erased = ActorAddress::new(
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
            Err(AddressError::ProtocolMismatch {
                expected: 8,
                actual: 7
            })
        ));

        let encoded = serde_json::to_vec(&typed).unwrap();
        let decoded: ActorAddress<TestProtocol> = serde_json::from_slice(&encoded).unwrap();
        assert!(decoded.same_activation(&typed));
        assert!(serde_json::from_slice::<ActorAddress<OtherProtocol>>(&encoded).is_err());
        assert_eq!(
            serde_json::to_value(&typed).unwrap(),
            serde_json::to_value(&erased).unwrap()
        );

        let entity = EntityAddress::new(
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
        assert!(serde_json::from_slice::<EntityAddress<TestProtocol>>(&encoded).is_ok());
        assert!(serde_json::from_slice::<EntityAddress<OtherProtocol>>(&encoded).is_err());

        let singleton = SingletonAddress::new(
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
        assert!(serde_json::from_slice::<SingletonAddress<TestProtocol>>(&encoded).is_ok());
        assert!(serde_json::from_slice::<SingletonAddress<OtherProtocol>>(&encoded).is_err());
    }
}
