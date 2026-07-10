use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use lattice_core::actor_ref::Epoch;
use lattice_core::id::RouteKey;
use lattice_core::instance::InstanceId;
use lattice_core::kind::{ActorKind, ServiceKind};
use serde::{Deserialize, Serialize};

use crate::error::PlacementError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct VirtualShardId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VirtualShardMapper {
    shard_count: u32,
}

impl VirtualShardMapper {
    pub fn new(shard_count: u32) -> Result<Self, PlacementError> {
        if shard_count == 0 {
            return Err(PlacementError::InvalidShardCount);
        }
        Ok(Self { shard_count })
    }

    pub fn shard_for_route_key(&self, route_key: &RouteKey) -> VirtualShardId {
        VirtualShardId((stable_route_hash(route_key) % u64::from(self.shard_count)) as u32)
    }

    pub fn shard_count(self) -> u32 {
        self.shard_count
    }
}

fn stable_route_hash(route_key: &RouteKey) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    fn write(hash: &mut u64, bytes: &[u8]) {
        for byte in bytes {
            *hash ^= u64::from(*byte);
            *hash = hash.wrapping_mul(0x100000001b3);
        }
    }

    match route_key {
        RouteKey::U64(value) => {
            write(&mut hash, b"u64");
            write(&mut hash, &value.to_be_bytes());
        }
        RouteKey::I64(value) => {
            write(&mut hash, b"i64");
            write(&mut hash, &value.to_be_bytes());
        }
        RouteKey::Str(value) => {
            write(&mut hash, b"str");
            write(&mut hash, value.as_bytes());
        }
        RouteKey::Bytes(value) => {
            write(&mut hash, b"bytes");
            write(&mut hash, value);
        }
    }
    hash
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualShardAssignment {
    pub shard_id: VirtualShardId,
    pub owner: InstanceId,
    pub epoch: Epoch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualShardAssignInput {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub shard_count: u32,
    pub instances: Vec<InstanceId>,
    pub previous: Vec<VirtualShardAssignment>,
    pub eligible_shards: BTreeSet<VirtualShardId>,
    pub max_migrations: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualShardAssignPlan {
    pub assignments: Vec<VirtualShardAssignment>,
}

impl VirtualShardAssignPlan {
    pub fn owner_of(&self, shard_id: VirtualShardId) -> Option<&VirtualShardAssignment> {
        self.assignments
            .iter()
            .find(|assignment| assignment.shard_id == shard_id)
    }
}

#[async_trait]
pub trait VirtualShardAssigner: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    async fn plan(
        &self,
        input: VirtualShardAssignInput,
    ) -> Result<VirtualShardAssignPlan, PlacementError>;
}

#[derive(Default, Clone)]
pub struct VirtualShardAssignerRegistry {
    assigners: Arc<std::sync::Mutex<HashMap<&'static str, Arc<dyn VirtualShardAssigner>>>>,
}

impl fmt::Debug for VirtualShardAssignerRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let assigner_count = self
            .assigners
            .lock()
            .map(|assigners| assigners.len())
            .unwrap_or_default();
        formatter
            .debug_struct("VirtualShardAssignerRegistry")
            .field("assigner_count", &assigner_count)
            .finish()
    }
}

impl VirtualShardAssignerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<A>(&self, assigner: A) -> Result<(), PlacementError>
    where
        A: VirtualShardAssigner,
    {
        let mut assigners = self
            .assigners
            .lock()
            .expect("assigner registry mutex poisoned");
        let name = assigner.name();
        if assigners.contains_key(name) {
            return Err(PlacementError::DuplicateAssigner { name });
        }
        assigners.insert(name, Arc::new(assigner));
        Ok(())
    }

    pub fn get(&self, name: &'static str) -> Option<Arc<dyn VirtualShardAssigner>> {
        self.assigners
            .lock()
            .expect("assigner registry mutex poisoned")
            .get(name)
            .cloned()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RoundRobinShardAssigner;

#[async_trait]
impl VirtualShardAssigner for RoundRobinShardAssigner {
    fn name(&self) -> &'static str {
        "round_robin"
    }

    async fn plan(
        &self,
        input: VirtualShardAssignInput,
    ) -> Result<VirtualShardAssignPlan, PlacementError> {
        if input.shard_count == 0 {
            return Err(PlacementError::InvalidShardCount);
        }
        if input.instances.is_empty() {
            return Err(PlacementError::NoReadyInstances);
        }

        let previous = previous_assignments_by_shard(&input.previous);
        let mut assignments = Vec::with_capacity(input.shard_count as usize);
        for shard in 0..input.shard_count {
            let shard_id = VirtualShardId(shard);
            let owner = input.instances[shard as usize % input.instances.len()].clone();
            let epoch = next_epoch(previous.get(&shard_id), &owner);
            assignments.push(VirtualShardAssignment {
                shard_id,
                owner,
                epoch,
            });
        }
        Ok(VirtualShardAssignPlan { assignments })
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct GradualRebalanceShardAssigner;

#[async_trait]
impl VirtualShardAssigner for GradualRebalanceShardAssigner {
    fn name(&self) -> &'static str {
        "gradual_rebalance"
    }

    async fn plan(
        &self,
        input: VirtualShardAssignInput,
    ) -> Result<VirtualShardAssignPlan, PlacementError> {
        let desired = RoundRobinShardAssigner.plan(input.clone()).await?;
        if input.previous.is_empty() {
            return Ok(desired);
        }

        let previous = previous_assignments_by_shard(&input.previous);
        let mut remaining_migrations = input.max_migrations;
        let mut assignments = Vec::with_capacity(desired.assignments.len());
        for desired_assignment in desired.assignments {
            let Some(previous_assignment) = previous.get(&desired_assignment.shard_id) else {
                assignments.push(desired_assignment);
                continue;
            };
            if previous_assignment.owner == desired_assignment.owner {
                assignments.push(previous_assignment.clone());
                continue;
            }

            let eligible = input.eligible_shards.is_empty()
                || input.eligible_shards.contains(&desired_assignment.shard_id);
            if eligible && remaining_migrations > 0 {
                remaining_migrations -= 1;
                assignments.push(desired_assignment);
            } else {
                assignments.push(previous_assignment.clone());
            }
        }

        Ok(VirtualShardAssignPlan { assignments })
    }
}

fn previous_assignments_by_shard(
    previous: &[VirtualShardAssignment],
) -> BTreeMap<VirtualShardId, VirtualShardAssignment> {
    previous
        .iter()
        .map(|assignment| (assignment.shard_id, assignment.clone()))
        .collect()
}

fn next_epoch(previous: Option<&VirtualShardAssignment>, owner: &InstanceId) -> Epoch {
    match previous {
        Some(previous) if &previous.owner == owner => previous.epoch,
        Some(previous) => Epoch(previous.epoch.0 + 1),
        None => Epoch(1),
    }
}
