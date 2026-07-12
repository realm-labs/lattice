use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};

use bytes::Bytes;
use lattice_core::actor_ref::{
    ConfigFingerprint, EntityId, EntityRef, EntityType, NodeIncarnation, ProtocolId,
};
use thiserror::Error;
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::types::{
    AssignmentGeneration, MonotonicTime, NodeKey, PlacementSlotState, Revision, ShardId,
};

pub const XXH3_V1_SEED: u64 = 0x4c41_5454_4943_4531;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityConfig {
    pub entity_type: EntityType,
    pub protocol_id: ProtocolId,
    pub shard_count: u32,
    pub allocation_policy_id: String,
    pub allocation_policy_version: u32,
    pub hard_constraints: Vec<String>,
    fingerprint: ConfigFingerprint,
}

impl EntityConfig {
    pub fn new(
        entity_type: EntityType,
        protocol_id: ProtocolId,
        shard_count: u32,
        allocation_policy_id: impl Into<String>,
        allocation_policy_version: u32,
        mut hard_constraints: Vec<String>,
    ) -> Result<Self, RegionError> {
        if shard_count == 0 || shard_count > 1_048_576 {
            return Err(RegionError::InvalidShardCount);
        }
        let allocation_policy_id = allocation_policy_id.into();
        if allocation_policy_id.is_empty()
            || allocation_policy_id.len() > 128
            || allocation_policy_version == 0
            || hard_constraints.len() > 64
            || hard_constraints.iter().any(|value| value.len() > 256)
        {
            return Err(RegionError::InvalidConfig);
        }
        hard_constraints.sort();
        hard_constraints.dedup();
        let mut canonical = Vec::new();
        canonical.extend_from_slice(entity_type.as_str().as_bytes());
        canonical.extend_from_slice(&protocol_id.get().to_be_bytes());
        canonical.extend_from_slice(&shard_count.to_be_bytes());
        canonical.extend_from_slice(allocation_policy_id.as_bytes());
        canonical.extend_from_slice(&allocation_policy_version.to_be_bytes());
        for constraint in &hard_constraints {
            canonical.extend_from_slice(&(constraint.len() as u32).to_be_bytes());
            canonical.extend_from_slice(constraint.as_bytes());
        }
        let fingerprint = ConfigFingerprint::new(*blake3::hash(&canonical).as_bytes());
        Ok(Self {
            entity_type,
            protocol_id,
            shard_count,
            allocation_policy_id,
            allocation_policy_version,
            hard_constraints,
            fingerprint,
        })
    }

    pub fn fingerprint(&self) -> ConfigFingerprint {
        self.fingerprint
    }

    pub fn shard_for(&self, entity_id: &EntityId) -> ShardId {
        ShardId::new(
            (xxh3_64_with_seed(entity_id.as_bytes(), XXH3_V1_SEED) % u64::from(self.shard_count))
                as u32,
        )
    }

    pub fn entity_ref<A>(
        &self,
        cluster_id: lattice_core::actor_ref::ClusterId,
        entity_id: EntityId,
    ) -> EntityRef<A> {
        EntityRef::new(
            cluster_id,
            self.entity_type.clone(),
            entity_id,
            self.protocol_id,
            self.fingerprint,
        )
    }
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
    pub payload: Bytes,
    pub admitted_at: MonotonicTime,
    pub expires_at: MonotonicTime,
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
    entity: EntityConfig,
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
        config.validate()?;
        Ok(Self {
            local_incarnation,
            entity,
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
        payload: Bytes,
        now: MonotonicTime,
    ) -> Result<RouteDecision, RegionError> {
        let shard_id = self.entity.shard_for(&entity_id);
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
        let expires_at = now
            .checked_add(std::time::Duration::from_millis(
                self.config.maximum_buffer_age_millis,
            ))
            .ok_or(RegionError::InvalidTime)?;
        self.buffered_messages += 1;
        self.buffered_bytes += payload.len();
        self.buffers
            .entry(shard_id)
            .or_default()
            .push_back(BufferedMessage {
                entity_id,
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
        if revision != self.revision || !self.required_sessions.contains(&session) {
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
    #[error("Coordinator revision is stale or duplicated")]
    StaleRevision,
    #[error("handoff acknowledgement is from an unrelated session or revision")]
    UnexpectedBarrierMember,
}
