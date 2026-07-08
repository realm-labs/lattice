use async_trait::async_trait;
use lattice_core::actor_ref::Epoch;

use crate::coordination::reports::{
    PrepareVirtualShardMigrationRequest, VirtualShardMigrationOutcome,
};
use crate::coordination::singleton::SingletonControl;
use crate::error::PlacementError;
use crate::registry::InstanceRecord;
use crate::storage::{ActorPlacementKey, SingletonKey};

#[async_trait]
pub trait LogicControl: Clone + Send + Sync + 'static {
    async fn activate_actor(
        &self,
        instance: &InstanceRecord,
        key: &ActorPlacementKey,
        epoch: Epoch,
    ) -> Result<(), PlacementError>;
}

#[async_trait]
pub trait VirtualShardMigrationControl: LogicControl {
    async fn prepare_virtual_shard_migration(
        &self,
        instance: &InstanceRecord,
        request: PrepareVirtualShardMigrationRequest,
    ) -> Result<VirtualShardMigrationOutcome, PlacementError>;
}

#[derive(Debug, Clone, Default)]
pub struct NoopLogicControl;

#[async_trait]
impl LogicControl for NoopLogicControl {
    async fn activate_actor(
        &self,
        _instance: &InstanceRecord,
        _key: &ActorPlacementKey,
        _epoch: Epoch,
    ) -> Result<(), PlacementError> {
        Ok(())
    }
}

#[async_trait]
impl SingletonControl for NoopLogicControl {
    async fn activate_singleton(
        &self,
        _instance: &InstanceRecord,
        _key: &SingletonKey,
        _epoch: Epoch,
    ) -> Result<(), PlacementError> {
        Ok(())
    }
}

#[async_trait]
impl VirtualShardMigrationControl for NoopLogicControl {
    async fn prepare_virtual_shard_migration(
        &self,
        _instance: &InstanceRecord,
        request: PrepareVirtualShardMigrationRequest,
    ) -> Result<VirtualShardMigrationOutcome, PlacementError> {
        Ok(VirtualShardMigrationOutcome {
            shard_id: request.shard_id,
            eligible: true,
            running_actors: 0,
            passivated_actors: 0,
        })
    }
}
