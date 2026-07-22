use std::{fmt, sync::Arc};

use lattice_actor::mailbox::MailboxConfig;
use lattice_actor::registry::ActorRegistryConfig;
use lattice_core::actor_ref::{EntityType, PlacementDomainId, ProtocolId, SingletonKind};
use lattice_core::kind::ActorKind;
use lattice_placement::coordinator::SingletonConfig;
use lattice_placement::{
    mapping::{ShardMapper, Xxh3V1ShardMapper},
    region::{EntityConfig, RegionError},
};

/// Application-facing declaration for a hosted or proxy-only sharded entity.
#[derive(Clone)]
pub struct EntityOptions {
    pub domain: PlacementDomainId,
    pub entity_type: EntityType,
    pub shard_count: u32,
    pub allocation_policy_id: String,
    pub allocation_policy_version: u32,
    pub hard_constraints: Vec<String>,
    pub shard_mapper: Arc<dyn ShardMapper>,
    pub actor_kind: ActorKind,
    pub registry: ActorRegistryConfig,
}

impl EntityOptions {
    pub fn new(domain: PlacementDomainId, entity_type: EntityType, shard_count: u32) -> Self {
        let actor_kind = ActorKind::new(entity_type.as_str());
        Self {
            domain,
            entity_type,
            shard_count,
            allocation_policy_id: "weighted-least-load".to_owned(),
            allocation_policy_version: 1,
            hard_constraints: Vec::new(),
            shard_mapper: Arc::new(Xxh3V1ShardMapper),
            actor_kind,
            registry: ActorRegistryConfig::default(),
        }
    }

    pub fn actor_kind(mut self, actor_kind: ActorKind) -> Self {
        self.actor_kind = actor_kind;
        self
    }

    pub fn mailbox(mut self, mailbox: MailboxConfig) -> Self {
        self.registry.mailbox = mailbox;
        self
    }

    pub fn registry(mut self, registry: ActorRegistryConfig) -> Self {
        self.registry = registry;
        self
    }

    pub fn allocation_policy(mut self, id: impl Into<String>, version: u32) -> Self {
        self.allocation_policy_id = id.into();
        self.allocation_policy_version = version;
        self
    }

    pub fn hard_constraints(mut self, constraints: Vec<String>) -> Self {
        self.hard_constraints = constraints;
        self
    }

    pub fn shard_mapper<M: ShardMapper>(mut self, mapper: M) -> Self {
        self.shard_mapper = Arc::new(mapper);
        self
    }

    pub fn build(&self, protocol_id: ProtocolId) -> Result<EntityConfig, RegionError> {
        EntityConfig::new(
            self.domain.clone(),
            self.entity_type.clone(),
            protocol_id,
            self.shard_count,
            self.allocation_policy_id.clone(),
            self.allocation_policy_version,
            self.hard_constraints.clone(),
        )?
        .with_shard_mapper(self.shard_mapper.as_ref())
    }
}

impl fmt::Debug for EntityOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EntityOptions")
            .field("domain", &self.domain)
            .field("entity_type", &self.entity_type)
            .field("shard_count", &self.shard_count)
            .field("allocation_policy_id", &self.allocation_policy_id)
            .field("allocation_policy_version", &self.allocation_policy_version)
            .field("hard_constraints", &self.hard_constraints)
            .field("shard_mapper_id", &self.shard_mapper.mapper_id())
            .field("shard_mapper_version", &self.shard_mapper.mapper_version())
            .field("actor_kind", &self.actor_kind)
            .field("registry", &self.registry)
            .finish()
    }
}

/// Application-facing declaration for a hosted or proxy-only cluster singleton.
#[derive(Debug, Clone)]
pub struct SingletonOptions {
    pub domain: PlacementDomainId,
    pub kind: SingletonKind,
    pub actor_kind: ActorKind,
    pub registry: ActorRegistryConfig,
}

impl SingletonOptions {
    pub fn new(domain: PlacementDomainId, kind: SingletonKind) -> Self {
        let actor_kind = ActorKind::new(kind.as_str());
        Self {
            domain,
            kind,
            actor_kind,
            registry: ActorRegistryConfig::default(),
        }
    }

    pub fn actor_kind(mut self, actor_kind: ActorKind) -> Self {
        self.actor_kind = actor_kind;
        self
    }

    pub fn mailbox(mut self, mailbox: MailboxConfig) -> Self {
        self.registry.mailbox = mailbox;
        self
    }

    pub fn registry(mut self, registry: ActorRegistryConfig) -> Self {
        self.registry = registry;
        self
    }

    pub fn build(&self, protocol_id: ProtocolId) -> SingletonConfig {
        SingletonConfig::new(self.domain.clone(), self.kind.clone(), protocol_id)
    }
}

#[cfg(test)]
mod tests {
    use lattice_core::actor_ref::{EntityId, ProtocolId};
    use lattice_placement::{mapping::ShardMappingError, types::ShardId};

    use super::*;

    struct WorldMapper;

    impl ShardMapper for WorldMapper {
        fn mapper_id(&self) -> &'static str {
            "world-affinity"
        }

        fn mapper_version(&self) -> u32 {
            3
        }

        fn shard_for(
            &self,
            entity_id: &EntityId,
            shard_count: u32,
        ) -> Result<ShardId, ShardMappingError> {
            let world = entity_id
                .as_bytes()
                .first()
                .copied()
                .ok_or(ShardMappingError::InvalidEntityId)?;
            Ok(ShardId::new(u32::from(world) % shard_count))
        }
    }

    #[test]
    fn entity_options_persist_the_selected_mapper_identity() {
        let config = EntityOptions::new(
            PlacementDomainId::new("minecraft").unwrap(),
            EntityType::new("region").unwrap(),
            256,
        )
        .shard_mapper(WorldMapper)
        .build(ProtocolId::new(7).unwrap())
        .unwrap();

        assert_eq!(config.shard_mapper_id, "world-affinity");
        assert_eq!(config.shard_mapper_version, 3);
        assert_eq!(
            config
                .shard_for_with(&WorldMapper, &EntityId::new(vec![42, 1, 2]).unwrap())
                .unwrap(),
            ShardId::new(42)
        );
    }
}
