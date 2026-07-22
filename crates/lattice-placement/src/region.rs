use std::{
    collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use lattice_core::actor_ref::{
    ClusterId, ConfigFingerprint, EntityId, EntityRef, EntityType, NodeIncarnation,
    PlacementDomainId, ProtocolId, ProtocolTag, ReferenceError,
};
use thiserror::Error;

use crate::{
    mapping::{
        ShardMapper, ShardMapperBinding, ShardMappingError, XXH3_V1_MAPPER_ID,
        XXH3_V1_MAPPER_VERSION, XXH3_V1_SEED, Xxh3V1ShardMapper,
    },
    types::{AssignmentGeneration, MonotonicTime, NodeKey, PlacementSlotState, Revision, ShardId},
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EntityConfig {
    pub domain: PlacementDomainId,
    pub entity_type: EntityType,
    pub protocol_id: ProtocolId,
    pub shard_count: u32,
    #[serde(
        default = "default_shard_mapper_id",
        skip_serializing_if = "is_default_shard_mapper_id"
    )]
    pub shard_mapper_id: String,
    // Keep the generation-5 wire field name so default-mapper rolling upgrades
    // remain readable by older processes. Custom mapper fingerprints are still
    // rejected by processes that do not understand `shard_mapper_id`.
    #[serde(
        rename = "shard_hash_version",
        default = "default_shard_mapper_version"
    )]
    pub shard_mapper_version: u32,
    pub allocation_policy_id: String,
    pub allocation_policy_version: u32,
    pub hard_constraints: Vec<String>,
    fingerprint: ConfigFingerprint,
}

impl EntityConfig {
    pub fn new(
        domain: PlacementDomainId,
        entity_type: EntityType,
        protocol_id: ProtocolId,
        shard_count: u32,
        allocation_policy_id: impl Into<String>,
        allocation_policy_version: u32,
        hard_constraints: Vec<String>,
    ) -> Result<Self, RegionError> {
        Self::new_with_mapper_identity(
            domain,
            entity_type,
            protocol_id,
            shard_count,
            (XXH3_V1_MAPPER_ID, XXH3_V1_MAPPER_VERSION),
            (allocation_policy_id, allocation_policy_version),
            hard_constraints,
        )
    }

    fn new_with_mapper_identity<M, P>(
        domain: PlacementDomainId,
        entity_type: EntityType,
        protocol_id: ProtocolId,
        shard_count: u32,
        mapper: (M, u32),
        allocation_policy: (P, u32),
        mut hard_constraints: Vec<String>,
    ) -> Result<Self, RegionError>
    where
        M: Into<String>,
        P: Into<String>,
    {
        if shard_count == 0 || shard_count > 1_048_576 {
            return Err(RegionError::InvalidShardCount);
        }
        let shard_mapper_id = mapper.0.into();
        let shard_mapper_version = mapper.1;
        let allocation_policy_id = allocation_policy.0.into();
        let allocation_policy_version = allocation_policy.1;
        if allocation_policy_id.is_empty()
            || allocation_policy_id.len() > 128
            || shard_mapper_id.is_empty()
            || shard_mapper_id.len() > 128
            || shard_mapper_version == 0
            || allocation_policy_version == 0
            || hard_constraints.len() > 64
            || hard_constraints.iter().any(|value| value.len() > 256)
        {
            return Err(RegionError::InvalidConfig);
        }
        hard_constraints.sort();
        hard_constraints.dedup();
        let mut canonical = Vec::new();
        canonical.extend_from_slice(&(domain.as_str().len() as u32).to_be_bytes());
        canonical.extend_from_slice(domain.as_str().as_bytes());
        canonical.extend_from_slice(&(entity_type.as_str().len() as u32).to_be_bytes());
        canonical.extend_from_slice(entity_type.as_str().as_bytes());
        canonical.extend_from_slice(&protocol_id.get().to_be_bytes());
        canonical.extend_from_slice(&shard_count.to_be_bytes());
        if shard_mapper_id == XXH3_V1_MAPPER_ID && shard_mapper_version == XXH3_V1_MAPPER_VERSION {
            // Preserve the original Xxh3V1 configuration fingerprint.
            canonical.extend_from_slice(&XXH3_V1_MAPPER_VERSION.to_be_bytes());
            canonical.extend_from_slice(&XXH3_V1_SEED.to_be_bytes());
        } else {
            canonical.extend_from_slice(&0_u32.to_be_bytes());
            canonical.extend_from_slice(b"custom-shard-mapper");
            canonical.extend_from_slice(&(shard_mapper_id.len() as u32).to_be_bytes());
            canonical.extend_from_slice(shard_mapper_id.as_bytes());
            canonical.extend_from_slice(&shard_mapper_version.to_be_bytes());
        }
        canonical.extend_from_slice(&(allocation_policy_id.len() as u32).to_be_bytes());
        canonical.extend_from_slice(allocation_policy_id.as_bytes());
        canonical.extend_from_slice(&allocation_policy_version.to_be_bytes());
        canonical.extend_from_slice(&(hard_constraints.len() as u32).to_be_bytes());
        for constraint in &hard_constraints {
            canonical.extend_from_slice(&(constraint.len() as u32).to_be_bytes());
            canonical.extend_from_slice(constraint.as_bytes());
        }
        let fingerprint = ConfigFingerprint::new(*blake3::hash(&canonical).as_bytes());
        Ok(Self {
            domain,
            entity_type,
            protocol_id,
            shard_count,
            shard_mapper_id,
            shard_mapper_version,
            allocation_policy_id,
            allocation_policy_version,
            hard_constraints,
            fingerprint,
        })
    }

    pub fn fingerprint(&self) -> ConfigFingerprint {
        self.fingerprint
    }

    pub fn with_shard_mapper(mut self, mapper: &dyn ShardMapper) -> Result<Self, RegionError> {
        self.shard_mapper_id = mapper.mapper_id().to_owned();
        self.shard_mapper_version = mapper.mapper_version();
        Self::new_with_mapper_identity(
            self.domain,
            self.entity_type,
            self.protocol_id,
            self.shard_count,
            (self.shard_mapper_id, self.shard_mapper_version),
            (self.allocation_policy_id, self.allocation_policy_version),
            self.hard_constraints,
        )
    }

    pub fn validate(&self) -> Result<(), RegionError> {
        let rebuilt = Self::new_with_mapper_identity(
            self.domain.clone(),
            self.entity_type.clone(),
            self.protocol_id,
            self.shard_count,
            (self.shard_mapper_id.clone(), self.shard_mapper_version),
            (
                self.allocation_policy_id.clone(),
                self.allocation_policy_version,
            ),
            self.hard_constraints.clone(),
        )?;
        if rebuilt.fingerprint != self.fingerprint {
            return Err(RegionError::InvalidConfig);
        }
        Ok(())
    }

    pub fn shard_for(&self, entity_id: &EntityId) -> Result<ShardId, ShardMappingError> {
        self.shard_for_with(&Xxh3V1ShardMapper, entity_id)
    }

    pub fn shard_for_with(
        &self,
        mapper: &dyn ShardMapper,
        entity_id: &EntityId,
    ) -> Result<ShardId, ShardMappingError> {
        self.validate_mapper(mapper)?;
        let shard_id = mapper.shard_for(entity_id, self.shard_count)?;
        if shard_id.get() >= self.shard_count {
            return Err(ShardMappingError::ShardOutOfRange);
        }
        Ok(shard_id)
    }

    pub fn validate_mapper(&self, mapper: &dyn ShardMapper) -> Result<(), ShardMappingError> {
        if mapper.mapper_id() != self.shard_mapper_id
            || mapper.mapper_version() != self.shard_mapper_version
        {
            return Err(ShardMappingError::MapperMismatch);
        }
        Ok(())
    }

    pub fn bind_mapper(
        &self,
        mapper: Arc<dyn ShardMapper>,
    ) -> Result<ShardMapperBinding, ShardMappingError> {
        ShardMapperBinding::new(
            mapper,
            &self.shard_mapper_id,
            self.shard_mapper_version,
            self.shard_count,
        )
    }

    pub fn entity_ref<P: ProtocolTag>(
        &self,
        cluster_id: ClusterId,
        entity_id: EntityId,
    ) -> Result<EntityRef<P>, ReferenceError> {
        EntityRef::new(
            cluster_id,
            self.domain.clone(),
            self.entity_type.clone(),
            entity_id,
            self.protocol_id,
            self.fingerprint,
        )?
        .try_typed()
    }
}

fn default_shard_mapper_id() -> String {
    XXH3_V1_MAPPER_ID.to_owned()
}

fn is_default_shard_mapper_id(mapper_id: &str) -> bool {
    mapper_id == XXH3_V1_MAPPER_ID
}

const fn default_shard_mapper_version() -> u32 {
    XXH3_V1_MAPPER_VERSION
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardHome {
    pub owner: NodeKey,
    pub generation: AssignmentGeneration,
    pub revision: Revision,
    pub state: PlacementSlotState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BufferedMessage {
    pub entity_id: EntityId,
    pub message_id: u64,
    pub mode: BufferedMessageMode,
    pub payload: Bytes,
    pub admitted_at: MonotonicTime,
    pub expires_at: MonotonicTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferedMessageMode {
    Tell,
    Ask { deadline: MonotonicTime },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionConfig {
    pub maximum_cached_homes: usize,
    pub maximum_inflight_lookups: usize,
    pub maximum_buffered_messages: usize,
    pub maximum_buffered_bytes: usize,
    pub maximum_buffer_age_millis: u64,
}

impl Default for RegionConfig {
    fn default() -> Self {
        Self {
            maximum_cached_homes: 4096,
            maximum_inflight_lookups: 256,
            maximum_buffered_messages: 4096,
            maximum_buffered_bytes: 16 * 1024 * 1024,
            maximum_buffer_age_millis: 5_000,
        }
    }
}

impl RegionConfig {
    fn validate(&self) -> Result<(), RegionError> {
        if [
            self.maximum_cached_homes,
            self.maximum_inflight_lookups,
            self.maximum_buffered_messages,
            self.maximum_buffered_bytes,
            self.maximum_buffer_age_millis as usize,
        ]
        .contains(&0)
        {
            return Err(RegionError::ZeroLimit);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    Local {
        shard_id: ShardId,
        generation: AssignmentGeneration,
    },
    Remote {
        shard_id: ShardId,
        home: ShardHome,
    },
    Buffered {
        shard_id: ShardId,
        start_lookup: bool,
    },
}

pub struct ShardRegion {
    local_incarnation: NodeIncarnation,
    mapper: ShardMapperBinding,
    config: RegionConfig,
    homes: BTreeMap<ShardId, ShardHome>,
    inflight: HashSet<ShardId>,
    buffers: BTreeMap<ShardId, VecDeque<BufferedMessage>>,
    buffered_messages: usize,
    buffered_bytes: usize,
    applied_revision: Option<Revision>,
}

impl ShardRegion {
    pub fn new(
        local_incarnation: NodeIncarnation,
        entity: EntityConfig,
        config: RegionConfig,
    ) -> Result<Self, RegionError> {
        Self::with_mapper(
            local_incarnation,
            entity,
            Arc::new(Xxh3V1ShardMapper),
            config,
        )
    }

    pub fn with_mapper(
        local_incarnation: NodeIncarnation,
        entity: EntityConfig,
        mapper: Arc<dyn ShardMapper>,
        config: RegionConfig,
    ) -> Result<Self, RegionError> {
        config.validate()?;
        let mapper = entity.bind_mapper(mapper)?;
        Ok(Self {
            local_incarnation,
            mapper,
            config,
            homes: BTreeMap::new(),
            inflight: HashSet::new(),
            buffers: BTreeMap::new(),
            buffered_messages: 0,
            buffered_bytes: 0,
            applied_revision: None,
        })
    }

    pub fn apply_home(
        &mut self,
        shard_id: ShardId,
        home: ShardHome,
    ) -> Result<Vec<BufferedMessage>, RegionError> {
        if self
            .applied_revision
            .is_some_and(|revision| home.revision <= revision)
        {
            return Err(RegionError::StaleRevision);
        }
        if self.homes.len() == self.config.maximum_cached_homes
            && !self.homes.contains_key(&shard_id)
        {
            return Err(RegionError::HomeCacheFull);
        }
        self.applied_revision = Some(home.revision);
        self.homes.insert(shard_id, home);
        self.inflight.remove(&shard_id);
        Ok(self.take_buffer(shard_id))
    }

    pub fn invalidate_for_handoff(
        &mut self,
        shard_id: ShardId,
        revision: Revision,
    ) -> Result<(), RegionError> {
        if self
            .applied_revision
            .is_some_and(|current| revision <= current)
        {
            return Err(RegionError::StaleRevision);
        }
        self.applied_revision = Some(revision);
        self.homes.remove(&shard_id);
        Ok(())
    }

    pub fn route(
        &mut self,
        entity_id: EntityId,
        message_id: u64,
        mode: BufferedMessageMode,
        payload: Bytes,
        now: MonotonicTime,
    ) -> Result<RouteDecision, RegionError> {
        if message_id == 0 {
            return Err(RegionError::InvalidMessage);
        }
        let shard_id = self.mapper.shard_for(&entity_id)?;
        if let Some(home) = self.homes.get(&shard_id)
            && home.state == PlacementSlotState::Running
        {
            return if home.owner.incarnation == self.local_incarnation {
                Ok(RouteDecision::Local {
                    shard_id,
                    generation: home.generation,
                })
            } else {
                Ok(RouteDecision::Remote {
                    shard_id,
                    home: home.clone(),
                })
            };
        }
        self.expire_buffers(now);
        if self.buffered_messages == self.config.maximum_buffered_messages
            || self.buffered_bytes.saturating_add(payload.len())
                > self.config.maximum_buffered_bytes
        {
            return Err(RegionError::BufferFull);
        }
        let residence_deadline = now
            .checked_add(Duration::from_millis(self.config.maximum_buffer_age_millis))
            .ok_or(RegionError::InvalidTime)?;
        let expires_at = match mode {
            BufferedMessageMode::Tell => residence_deadline,
            BufferedMessageMode::Ask { deadline } => deadline.min(residence_deadline),
        };
        if expires_at <= now {
            return Err(RegionError::MessageExpired);
        }
        self.buffered_messages += 1;
        self.buffered_bytes += payload.len();
        self.buffers
            .entry(shard_id)
            .or_default()
            .push_back(BufferedMessage {
                entity_id,
                message_id,
                mode,
                payload,
                admitted_at: now,
                expires_at,
            });
        let start_lookup = if self.inflight.contains(&shard_id) {
            false
        } else if self.inflight.len() == self.config.maximum_inflight_lookups {
            return Err(RegionError::LookupLimit);
        } else {
            self.inflight.insert(shard_id);
            true
        };
        Ok(RouteDecision::Buffered {
            shard_id,
            start_lookup,
        })
    }

    fn expire_buffers(&mut self, now: MonotonicTime) {
        self.buffers.retain(|_, queue| {
            while queue
                .front()
                .is_some_and(|message| message.expires_at <= now)
            {
                if let Some(expired) = queue.pop_front() {
                    self.buffered_messages = self.buffered_messages.saturating_sub(1);
                    self.buffered_bytes = self.buffered_bytes.saturating_sub(expired.payload.len());
                }
            }
            !queue.is_empty()
        });
    }

    fn take_buffer(&mut self, shard_id: ShardId) -> Vec<BufferedMessage> {
        let messages = self.buffers.remove(&shard_id).unwrap_or_default();
        self.buffered_messages = self.buffered_messages.saturating_sub(messages.len());
        let bytes = messages.iter().map(|message| message.payload.len()).sum();
        self.buffered_bytes = self.buffered_bytes.saturating_sub(bytes);
        messages.into_iter().collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoffBarrier {
    pub entity_type: EntityType,
    pub shard_id: ShardId,
    pub revision: Revision,
    required_sessions: BTreeSet<NodeIncarnation>,
    applied_sessions: BTreeSet<NodeIncarnation>,
}

impl HandoffBarrier {
    pub fn freeze(
        entity_type: EntityType,
        shard_id: ShardId,
        revision: Revision,
        subscribed_sessions: impl IntoIterator<Item = NodeIncarnation>,
    ) -> Self {
        Self {
            entity_type,
            shard_id,
            revision,
            required_sessions: subscribed_sessions.into_iter().collect(),
            applied_sessions: BTreeSet::new(),
        }
    }

    pub fn apply_revision(
        &mut self,
        session: NodeIncarnation,
        revision: Revision,
    ) -> Result<(), RegionError> {
        if revision < self.revision || !self.required_sessions.contains(&session) {
            return Err(RegionError::UnexpectedBarrierMember);
        }
        self.applied_sessions.insert(session);
        Ok(())
    }

    pub fn fence_departed_session(&mut self, session: NodeIncarnation) -> bool {
        self.required_sessions.remove(&session)
    }

    pub fn is_complete(&self) -> bool {
        self.required_sessions == self.applied_sessions
    }

    pub fn required_sessions(&self) -> &BTreeSet<NodeIncarnation> {
        &self.required_sessions
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RegionError {
    #[error("entity shard count is zero or exceeds the fixed bound")]
    InvalidShardCount,
    #[error("entity configuration is invalid")]
    InvalidConfig,
    #[error("entity shard mapping failed")]
    ShardMapping(#[from] ShardMappingError),
    #[error("Region limit must be nonzero")]
    ZeroLimit,
    #[error("Region home cache is full")]
    HomeCacheFull,
    #[error("Region lookup concurrency limit reached")]
    LookupLimit,
    #[error("Region message buffer is full")]
    BufferFull,
    #[error("Region time arithmetic overflowed")]
    InvalidTime,
    #[error("Region message ID is zero")]
    InvalidMessage,
    #[error("Region message deadline elapsed before buffering")]
    MessageExpired,
    #[error("Coordinator revision is stale or duplicated")]
    StaleRevision,
    #[error("handoff acknowledgement is from an unrelated session or revision")]
    UnexpectedBarrierMember,
}

#[cfg(test)]
mod tests {
    use lattice_core::actor_ref::{NodeAddress, ProtocolId};

    use super::*;

    #[derive(Debug, Clone, Copy)]
    struct WorldRegionMapper;

    impl ShardMapper for WorldRegionMapper {
        fn mapper_id(&self) -> &'static str {
            "minecraft-world"
        }

        fn mapper_version(&self) -> u32 {
            1
        }

        fn shard_for(
            &self,
            entity_id: &EntityId,
            shard_count: u32,
        ) -> Result<ShardId, ShardMappingError> {
            let world = entity_id
                .as_bytes()
                .get(..8)
                .and_then(|bytes| bytes.try_into().ok())
                .map(u64::from_be_bytes)
                .ok_or(ShardMappingError::InvalidEntityId)?;
            Ok(ShardId::new((world % u64::from(shard_count)) as u32))
        }
    }

    fn region_id(world: u64, x: i32, z: i32) -> EntityId {
        let mut bytes = Vec::with_capacity(16);
        bytes.extend_from_slice(&world.to_be_bytes());
        bytes.extend_from_slice(&x.to_be_bytes());
        bytes.extend_from_slice(&z.to_be_bytes());
        EntityId::new(bytes).unwrap()
    }

    fn entity() -> EntityConfig {
        EntityConfig::new(
            PlacementDomainId::new("buffer-test").unwrap(),
            EntityType::new("buffer-test").unwrap(),
            ProtocolId::new(7).unwrap(),
            1,
            "test",
            1,
            Vec::new(),
        )
        .unwrap()
    }

    #[test]
    fn entity_config_fingerprint_has_a_domain_scoped_golden_vector() {
        let config = EntityConfig::new(
            PlacementDomainId::new("player").unwrap(),
            EntityType::new("account").unwrap(),
            ProtocolId::new(0x0102_0304_0506_0708).unwrap(),
            128,
            "weighted-least-load",
            3,
            vec!["region=ap-east".to_owned(), "tier=premium".to_owned()],
        )
        .unwrap();
        assert_eq!(
            *config.fingerprint().as_bytes(),
            [
                23, 114, 47, 89, 240, 189, 169, 100, 98, 193, 20, 241, 131, 150, 55, 150, 122, 170,
                148, 77, 168, 48, 140, 17, 155, 110, 142, 6, 249, 118, 222, 155,
            ]
        );

        let another_domain = EntityConfig::new(
            PlacementDomainId::new("world").unwrap(),
            config.entity_type.clone(),
            config.protocol_id,
            config.shard_count,
            config.allocation_policy_id.clone(),
            config.allocation_policy_version,
            config.hard_constraints.clone(),
        )
        .unwrap();
        assert_ne!(config.fingerprint(), another_domain.fingerprint());
    }

    #[test]
    fn default_mapper_keeps_the_generation_five_wire_field() {
        let config = entity();
        let encoded = serde_json::to_value(&config).unwrap();
        assert_eq!(encoded["shard_hash_version"], 1);
        assert!(encoded.get("shard_mapper_id").is_none());
        assert!(encoded.get("shard_mapper_version").is_none());

        let decoded: EntityConfig = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.shard_mapper_id, XXH3_V1_MAPPER_ID);
        decoded.validate().unwrap();
    }

    #[test]
    fn custom_mapper_groups_regions_by_world_and_is_part_of_the_fingerprint() {
        let default = EntityConfig::new(
            PlacementDomainId::new("minecraft").unwrap(),
            EntityType::new("region").unwrap(),
            ProtocolId::new(7).unwrap(),
            128,
            "weighted-least-load",
            1,
            Vec::new(),
        )
        .unwrap();
        let custom = default
            .clone()
            .with_shard_mapper(&WorldRegionMapper)
            .unwrap();
        assert_ne!(default.fingerprint(), custom.fingerprint());
        assert_eq!(
            custom
                .shard_for_with(&WorldRegionMapper, &region_id(7, -10, 4))
                .unwrap(),
            custom
                .shard_for_with(&WorldRegionMapper, &region_id(7, 900, -300))
                .unwrap()
        );
        assert_ne!(
            custom
                .shard_for_with(&WorldRegionMapper, &region_id(7, 0, 0))
                .unwrap(),
            custom
                .shard_for_with(&WorldRegionMapper, &region_id(8, 0, 0))
                .unwrap()
        );
        assert_eq!(
            custom.shard_for(&region_id(7, 0, 0)),
            Err(ShardMappingError::MapperMismatch)
        );
    }

    #[test]
    fn custom_mapper_drives_region_single_flight_by_affinity_group() {
        let config = EntityConfig::new(
            PlacementDomainId::new("minecraft").unwrap(),
            EntityType::new("region").unwrap(),
            ProtocolId::new(7).unwrap(),
            128,
            "weighted-least-load",
            1,
            Vec::new(),
        )
        .unwrap()
        .with_shard_mapper(&WorldRegionMapper)
        .unwrap();
        let mut region = ShardRegion::with_mapper(
            NodeIncarnation::new(1).unwrap(),
            config,
            Arc::new(WorldRegionMapper),
            RegionConfig::default(),
        )
        .unwrap();
        let first = region
            .route(
                region_id(42, 0, 0),
                1,
                BufferedMessageMode::Tell,
                Bytes::new(),
                MonotonicTime::from_millis(1),
            )
            .unwrap();
        let second = region
            .route(
                region_id(42, 100, -100),
                2,
                BufferedMessageMode::Tell,
                Bytes::new(),
                MonotonicTime::from_millis(2),
            )
            .unwrap();
        assert_eq!(
            first,
            RouteDecision::Buffered {
                shard_id: ShardId::new(42),
                start_lookup: true,
            }
        );
        assert_eq!(
            second,
            RouteDecision::Buffered {
                shard_id: ShardId::new(42),
                start_lookup: false,
            }
        );
    }

    #[test]
    fn unknown_home_buffers_complete_delivery_intent_single_flight() {
        let local = NodeIncarnation::new(1).unwrap();
        let remote = NodeIncarnation::new(2).unwrap();
        let mut region = ShardRegion::new(local, entity(), RegionConfig::default()).unwrap();
        let entity_id = EntityId::new(b"entity".to_vec()).unwrap();
        let deadline = MonotonicTime::from_millis(900);
        assert_eq!(
            region
                .route(
                    entity_id.clone(),
                    41,
                    BufferedMessageMode::Ask { deadline },
                    Bytes::from_static(b"request"),
                    MonotonicTime::from_millis(100),
                )
                .unwrap(),
            RouteDecision::Buffered {
                shard_id: ShardId::new(0),
                start_lookup: true,
            }
        );
        assert_eq!(
            region
                .route(
                    entity_id,
                    42,
                    BufferedMessageMode::Tell,
                    Bytes::from_static(b"tell"),
                    MonotonicTime::from_millis(101),
                )
                .unwrap(),
            RouteDecision::Buffered {
                shard_id: ShardId::new(0),
                start_lookup: false,
            }
        );
        let flushed = region
            .apply_home(
                ShardId::new(0),
                ShardHome {
                    owner: NodeKey {
                        node_id: "remote".to_owned(),
                        address: NodeAddress::new("127.0.0.1", 2552).unwrap(),
                        incarnation: remote,
                    },
                    generation: AssignmentGeneration::new(1).unwrap(),
                    revision: Revision::new(1).unwrap(),
                    state: PlacementSlotState::Running,
                },
            )
            .unwrap();
        assert_eq!(flushed.len(), 2);
        assert_eq!(flushed[0].message_id, 41);
        assert_eq!(flushed[0].mode, BufferedMessageMode::Ask { deadline });
        assert_eq!(flushed[1].message_id, 42);
        assert_eq!(flushed[1].mode, BufferedMessageMode::Tell);
    }

    #[test]
    fn expired_ask_is_rejected_before_buffer_admission() {
        let mut region = ShardRegion::new(
            NodeIncarnation::new(1).unwrap(),
            entity(),
            RegionConfig::default(),
        )
        .unwrap();
        assert_eq!(
            region
                .route(
                    EntityId::new(b"expired".to_vec()).unwrap(),
                    1,
                    BufferedMessageMode::Ask {
                        deadline: MonotonicTime::from_millis(10),
                    },
                    Bytes::from_static(b"request"),
                    MonotonicTime::from_millis(10),
                )
                .unwrap_err(),
            RegionError::MessageExpired
        );
    }
}
