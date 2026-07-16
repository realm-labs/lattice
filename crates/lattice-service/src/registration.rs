use lattice_actor::mailbox::MailboxConfig;
use lattice_actor::registry::ActorRegistryConfig;
use lattice_core::actor_ref::{EntityType, PlacementDomainId, ProtocolId, SingletonKind};
use lattice_core::kind::ActorKind;
use lattice_placement::coordinator::SingletonConfig;
use lattice_placement::region::{EntityConfig, RegionError};

/// Application-facing declaration for a hosted or proxy-only sharded entity.
#[derive(Debug, Clone)]
pub struct EntityOptions {
    pub domain: PlacementDomainId,
    pub entity_type: EntityType,
    pub shard_count: u32,
    pub allocation_policy_id: String,
    pub allocation_policy_version: u32,
    pub hard_constraints: Vec<String>,
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

    pub fn build(&self, protocol_id: ProtocolId) -> Result<EntityConfig, RegionError> {
        EntityConfig::new(
            self.domain.clone(),
            self.entity_type.clone(),
            protocol_id,
            self.shard_count,
            self.allocation_policy_id.clone(),
            self.allocation_policy_version,
            self.hard_constraints.clone(),
        )
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
