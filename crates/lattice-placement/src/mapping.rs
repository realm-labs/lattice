use std::{fmt, sync::Arc};

use lattice_core::actor_ref::EntityId;
use thiserror::Error;
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::types::ShardId;

pub const XXH3_V1_MAPPER_ID: &str = "xxh3-v1";
pub const XXH3_V1_MAPPER_VERSION: u32 = 1;
pub const XXH3_V1_SEED: u64 = 0x4c41_5454_4943_4531;

/// Deterministically maps a canonical entity ID into the configured shard range.
///
/// Implementations run on every host and proxy that routes the entity type. The
/// ID and version are persisted in [`crate::region::EntityConfig`] and must be
/// changed whenever mapping behavior changes.
pub trait ShardMapper: Send + Sync + 'static {
    fn mapper_id(&self) -> &'static str;

    fn mapper_version(&self) -> u32;

    fn shard_for(
        &self,
        entity_id: &EntityId,
        shard_count: u32,
    ) -> Result<ShardId, ShardMappingError>;
}

/// A mapper whose identity has already been checked against an entity config.
#[derive(Clone)]
pub struct ShardMapperBinding {
    mapper: Arc<dyn ShardMapper>,
    shard_count: u32,
}

impl ShardMapperBinding {
    pub(crate) fn new(
        mapper: Arc<dyn ShardMapper>,
        expected_id: &str,
        expected_version: u32,
        shard_count: u32,
    ) -> Result<Self, ShardMappingError> {
        if mapper.mapper_id() != expected_id || mapper.mapper_version() != expected_version {
            return Err(ShardMappingError::MapperMismatch);
        }
        if shard_count == 0 {
            return Err(ShardMappingError::InvalidShardCount);
        }
        Ok(Self {
            mapper,
            shard_count,
        })
    }

    pub fn shard_for(&self, entity_id: &EntityId) -> Result<ShardId, ShardMappingError> {
        let shard_id = self.mapper.shard_for(entity_id, self.shard_count)?;
        if shard_id.get() >= self.shard_count {
            return Err(ShardMappingError::ShardOutOfRange);
        }
        Ok(shard_id)
    }
}

impl fmt::Debug for ShardMapperBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ShardMapperBinding")
            .field("mapper_id", &self.mapper.mapper_id())
            .field("mapper_version", &self.mapper.mapper_version())
            .field("shard_count", &self.shard_count)
            .finish()
    }
}

/// The stable built-in `xxh3(entity_id) % shard_count` mapping.
#[derive(Debug, Clone, Copy, Default)]
pub struct Xxh3V1ShardMapper;

impl ShardMapper for Xxh3V1ShardMapper {
    fn mapper_id(&self) -> &'static str {
        XXH3_V1_MAPPER_ID
    }

    fn mapper_version(&self) -> u32 {
        XXH3_V1_MAPPER_VERSION
    }

    fn shard_for(
        &self,
        entity_id: &EntityId,
        shard_count: u32,
    ) -> Result<ShardId, ShardMappingError> {
        if shard_count == 0 {
            return Err(ShardMappingError::InvalidShardCount);
        }
        Ok(ShardId::new(
            (xxh3_64_with_seed(entity_id.as_bytes(), XXH3_V1_SEED) % u64::from(shard_count)) as u32,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ShardMappingError {
    #[error("shard mapper identity does not match the entity configuration")]
    MapperMismatch,
    #[error("entity shard count is zero")]
    InvalidShardCount,
    #[error("entity ID is invalid for this shard mapper")]
    InvalidEntityId,
    #[error("shard mapper returned a shard outside the configured range")]
    ShardOutOfRange,
}
