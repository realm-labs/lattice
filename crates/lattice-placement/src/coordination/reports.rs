use std::collections::BTreeSet;

use lattice_core::actor_ref::Epoch;
use lattice_core::instance::InstanceId;
use lattice_core::kind::{ActorKind, ServiceKind};

use crate::sharding::VirtualShardId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainReport {
    pub drained_instance: InstanceId,
    pub migrated_actors: usize,
    pub migrated_virtual_shards: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailoverReport {
    pub failed_instance: InstanceId,
    pub reassigned_actors: usize,
    pub reassigned_singletons: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseExpiryReconcileReport {
    pub service_kind: ServiceKind,
    pub expired_instances: Vec<InstanceId>,
    pub failovers: Vec<FailoverReport>,
    pub skipped_instances: Vec<InstanceId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebalanceVirtualShardsRequest {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub shard_count: u32,
    pub eligible_shards: BTreeSet<VirtualShardId>,
    pub max_migrations: usize,
    pub movement_policy: VirtualShardMovementPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebalanceVirtualShardsReport {
    pub ready_instances: usize,
    pub assignments_written: usize,
    pub moved_shards: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrepareVirtualShardMigrationRequest {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub shard_id: VirtualShardId,
    pub shard_count: u32,
    pub owner_epoch: Epoch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualShardMigrationOutcome {
    pub shard_id: VirtualShardId,
    pub eligible: bool,
    pub running_actors: usize,
    pub passivated_actors: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VirtualShardMovementPolicy {
    EligibleOnly,
    AllowRunningMigration,
}
