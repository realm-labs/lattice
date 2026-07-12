use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use lattice_core::actor_ref::{EntityType, NodeIncarnation, SingletonKind};
use lattice_remoting::protocol::ProtocolDescriptor;
use thiserror::Error;

use crate::types::{CoordinatorTerm, MonotonicTime, NodeKey, Revision, ShardId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderRecord {
    pub node: NodeKey,
    pub protocol_generation: u64,
    pub term: CoordinatorTerm,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeHello {
    pub node: NodeKey,
    pub roles: BTreeSet<String>,
    pub capacity_units: u64,
    pub hosted_entity_types: BTreeSet<EntityType>,
    pub proxied_entity_types: BTreeSet<EntityType>,
    pub singleton_eligibility: BTreeSet<SingletonKind>,
    pub used_singletons: BTreeSet<SingletonKind>,
    pub protocols: Vec<ProtocolDescriptor>,
}

impl NodeHello {
    pub fn validate(&self, limits: &SessionLimits) -> Result<(), CoordinatorError> {
        self.node
            .validate()
            .map_err(|_| CoordinatorError::InvalidHello)?;
        if self.capacity_units == 0
            || self.roles.len() > limits.maximum_roles
            || self.hosted_entity_types.len() > limits.maximum_entity_types
            || self.proxied_entity_types.len() > limits.maximum_entity_types
            || self.singleton_eligibility.len() > limits.maximum_singletons
            || self.used_singletons.len() > limits.maximum_singletons
            || self.protocols.len() > limits.maximum_protocols
            || self
                .roles
                .iter()
                .any(|role| role.is_empty() || role.len() > 128)
        {
            return Err(CoordinatorError::InvalidHello);
        }
        let mut ids = BTreeSet::new();
        if self
            .protocols
            .iter()
            .any(|protocol| !ids.insert(protocol.protocol_id.get()))
        {
            return Err(CoordinatorError::InvalidHello);
        }
        Ok(())
    }

    pub fn subscribes_to(&self, entity_type: &EntityType) -> bool {
        self.hosted_entity_types.contains(entity_type)
            || self.proxied_entity_types.contains(entity_type)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionLimits {
    pub maximum_roles: usize,
    pub maximum_entity_types: usize,
    pub maximum_singletons: usize,
    pub maximum_protocols: usize,
}

impl Default for SessionLimits {
    fn default() -> Self {
        Self {
            maximum_roles: 32,
            maximum_entity_types: 256,
            maximum_singletons: 256,
            maximum_protocols: 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotLimits {
    pub maximum_records: usize,
    pub maximum_bytes: usize,
    pub maximum_chunks: usize,
    pub maximum_chunk_bytes: usize,
    pub staging_timeout_millis: u64,
}

impl Default for SnapshotLimits {
    fn default() -> Self {
        Self {
            maximum_records: 100_000,
            maximum_bytes: 32 * 1024 * 1024,
            maximum_chunks: 1024,
            maximum_chunk_bytes: 256 * 1024,
            staging_timeout_millis: 10_000,
        }
    }
}

impl SnapshotLimits {
    fn validate(&self) -> Result<(), CoordinatorError> {
        if [
            self.maximum_records,
            self.maximum_bytes,
            self.maximum_chunks,
            self.maximum_chunk_bytes,
            self.staging_timeout_millis as usize,
        ]
        .contains(&0)
            || self.maximum_chunk_bytes > self.maximum_bytes
        {
            return Err(CoordinatorError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SnapshotRecord {
    pub key: String,
    pub value: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotBegin {
    pub snapshot_id: u128,
    pub revision: Revision,
    pub record_count: usize,
    pub total_bytes: usize,
    pub chunk_count: usize,
    pub digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotChunk {
    pub snapshot_id: u128,
    pub index: usize,
    pub records: Vec<SnapshotRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotEnd {
    pub snapshot_id: u128,
    pub revision: Revision,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInstall {
    pub revision: Revision,
    pub records: Vec<SnapshotRecord>,
}

pub fn build_snapshot(
    revision: Revision,
    mut records: Vec<SnapshotRecord>,
    limits: &SnapshotLimits,
) -> Result<(SnapshotBegin, Vec<SnapshotChunk>, SnapshotEnd), CoordinatorError> {
    limits.validate()?;
    records.sort();
    if records.len() > limits.maximum_records {
        return Err(CoordinatorError::SnapshotLimit);
    }
    let total_bytes = records
        .iter()
        .try_fold(0_usize, |total, record| {
            total
                .checked_add(record.key.len())?
                .checked_add(record.value.len())
        })
        .ok_or(CoordinatorError::SnapshotLimit)?;
    if total_bytes > limits.maximum_bytes {
        return Err(CoordinatorError::SnapshotLimit);
    }
    let digest = snapshot_digest(&records);
    let snapshot_id = uuid::Uuid::new_v4().as_u128();
    let mut chunks = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 0_usize;
    for record in records {
        let bytes = record.key.len().saturating_add(record.value.len());
        if bytes > limits.maximum_chunk_bytes {
            return Err(CoordinatorError::SnapshotLimit);
        }
        if !current.is_empty() && current_bytes.saturating_add(bytes) > limits.maximum_chunk_bytes {
            chunks.push(SnapshotChunk {
                snapshot_id,
                index: chunks.len(),
                records: std::mem::take(&mut current),
            });
            current_bytes = 0;
        }
        current_bytes += bytes;
        current.push(record);
    }
    if !current.is_empty() {
        chunks.push(SnapshotChunk {
            snapshot_id,
            index: chunks.len(),
            records: current,
        });
    }
    if chunks.len() > limits.maximum_chunks {
        return Err(CoordinatorError::SnapshotLimit);
    }
    let begin = SnapshotBegin {
        snapshot_id,
        revision,
        record_count: chunks.iter().map(|chunk| chunk.records.len()).sum(),
        total_bytes,
        chunk_count: chunks.len(),
        digest,
    };
    let end = SnapshotEnd {
        snapshot_id,
        revision,
    };
    Ok((begin, chunks, end))
}

pub struct SnapshotStager {
    limits: SnapshotLimits,
    begin: SnapshotBegin,
    deadline: MonotonicTime,
    next_chunk: usize,
    records: Vec<SnapshotRecord>,
    bytes: usize,
}

impl SnapshotStager {
    pub fn begin(
        begin: SnapshotBegin,
        limits: SnapshotLimits,
        now: MonotonicTime,
    ) -> Result<Self, CoordinatorError> {
        limits.validate()?;
        if begin.record_count > limits.maximum_records
            || begin.total_bytes > limits.maximum_bytes
            || begin.chunk_count > limits.maximum_chunks
        {
            return Err(CoordinatorError::SnapshotLimit);
        }
        let deadline = now
            .checked_add(std::time::Duration::from_millis(
                limits.staging_timeout_millis,
            ))
            .ok_or(CoordinatorError::SnapshotLimit)?;
        Ok(Self {
            limits,
            begin,
            deadline,
            next_chunk: 0,
            records: Vec::new(),
            bytes: 0,
        })
    }

    pub fn push(
        &mut self,
        chunk: SnapshotChunk,
        now: MonotonicTime,
    ) -> Result<(), CoordinatorError> {
        if now >= self.deadline
            || chunk.snapshot_id != self.begin.snapshot_id
            || chunk.index != self.next_chunk
            || chunk.records.is_empty()
        {
            return Err(CoordinatorError::SnapshotSequence);
        }
        let chunk_bytes = chunk
            .records
            .iter()
            .map(|record| record.key.len().saturating_add(record.value.len()))
            .sum::<usize>();
        if chunk_bytes > self.limits.maximum_chunk_bytes
            || self.records.len().saturating_add(chunk.records.len()) > self.begin.record_count
            || self.bytes.saturating_add(chunk_bytes) > self.begin.total_bytes
        {
            return Err(CoordinatorError::SnapshotLimit);
        }
        self.next_chunk += 1;
        self.bytes += chunk_bytes;
        self.records.extend(chunk.records);
        Ok(())
    }

    pub fn finish(
        self,
        end: SnapshotEnd,
        now: MonotonicTime,
    ) -> Result<SnapshotInstall, CoordinatorError> {
        if now >= self.deadline
            || end.snapshot_id != self.begin.snapshot_id
            || end.revision != self.begin.revision
            || self.next_chunk != self.begin.chunk_count
            || self.records.len() != self.begin.record_count
            || self.bytes != self.begin.total_bytes
            || snapshot_digest(&self.records) != self.begin.digest
        {
            return Err(CoordinatorError::SnapshotIntegrity);
        }
        Ok(SnapshotInstall {
            revision: self.begin.revision,
            records: self.records,
        })
    }
}

fn snapshot_digest(records: &[SnapshotRecord]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for record in records {
        hasher.update(&(record.key.len() as u64).to_be_bytes());
        hasher.update(record.key.as_bytes());
        hasher.update(&(record.value.len() as u64).to_be_bytes());
        hasher.update(&record.value);
    }
    *hasher.finalize().as_bytes()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorDelta {
    pub revision: Revision,
    pub records: Vec<SnapshotRecord>,
}

#[derive(Debug, Default)]
pub struct CoordinatorSession {
    revision: Option<Revision>,
    records: BTreeMap<String, Bytes>,
    ready: bool,
}

impl CoordinatorSession {
    pub fn install(&mut self, snapshot: SnapshotInstall) -> Result<(), CoordinatorError> {
        let mut records = BTreeMap::new();
        for record in snapshot.records {
            if records.insert(record.key, record.value).is_some() {
                return Err(CoordinatorError::DuplicateRecord);
            }
        }
        self.records = records;
        self.revision = Some(snapshot.revision);
        self.ready = true;
        Ok(())
    }

    pub fn apply_delta(&mut self, delta: CoordinatorDelta) -> Result<(), CoordinatorError> {
        let current = self.revision.ok_or(CoordinatorError::SnapshotRequired)?;
        if delta.revision.get() != current.get().saturating_add(1) {
            self.ready = false;
            return Err(CoordinatorError::RevisionGap);
        }
        let mut next = self.records.clone();
        for record in delta.records {
            next.insert(record.key, record.value);
        }
        self.records = next;
        self.revision = Some(delta.revision);
        Ok(())
    }

    pub fn ready(&self) -> bool {
        self.ready
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeLoadReport {
    pub node: NodeKey,
    pub sequence: u64,
    pub observed_at: MonotonicTime,
    pub total_weight: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardLoadReport {
    pub node: NodeKey,
    pub entity_type: EntityType,
    pub shard_id: ShardId,
    pub sequence: u64,
    pub observed_at: MonotonicTime,
    pub weight: u64,
}

pub struct LoadTable {
    maximum_nodes: usize,
    maximum_shards: usize,
    nodes: BTreeMap<NodeIncarnation, NodeLoadReport>,
    shards: BTreeMap<(NodeIncarnation, EntityType, ShardId), ShardLoadReport>,
}

impl LoadTable {
    pub fn new(maximum_nodes: usize, maximum_shards: usize) -> Result<Self, CoordinatorError> {
        if maximum_nodes == 0 || maximum_shards == 0 {
            return Err(CoordinatorError::InvalidLimits);
        }
        Ok(Self {
            maximum_nodes,
            maximum_shards,
            nodes: BTreeMap::new(),
            shards: BTreeMap::new(),
        })
    }

    pub fn report_node(&mut self, report: NodeLoadReport) -> Result<bool, CoordinatorError> {
        report
            .node
            .validate()
            .map_err(|_| CoordinatorError::InvalidLoad)?;
        let key = report.node.incarnation;
        if self.nodes.len() == self.maximum_nodes && !self.nodes.contains_key(&key) {
            return Err(CoordinatorError::LoadCapacity);
        }
        if self
            .nodes
            .get(&key)
            .is_some_and(|current| report.sequence <= current.sequence)
        {
            return Ok(false);
        }
        if report.sequence == 0 {
            return Err(CoordinatorError::InvalidLoad);
        }
        self.nodes.insert(key, report);
        Ok(true)
    }

    pub fn report_shard(&mut self, report: ShardLoadReport) -> Result<bool, CoordinatorError> {
        report
            .node
            .validate()
            .map_err(|_| CoordinatorError::InvalidLoad)?;
        let key = (
            report.node.incarnation,
            report.entity_type.clone(),
            report.shard_id,
        );
        if self.shards.len() == self.maximum_shards && !self.shards.contains_key(&key) {
            return Err(CoordinatorError::LoadCapacity);
        }
        if self
            .shards
            .get(&key)
            .is_some_and(|current| report.sequence <= current.sequence)
        {
            return Ok(false);
        }
        if report.sequence == 0 {
            return Err(CoordinatorError::InvalidLoad);
        }
        self.shards.insert(key, report);
        Ok(true)
    }

    pub fn clear_for_leader_change(&mut self) {
        self.nodes.clear();
        self.shards.clear();
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CoordinatorError {
    #[error("Coordinator session limits are invalid")]
    InvalidLimits,
    #[error("node registration is invalid or over its bounds")]
    InvalidHello,
    #[error("snapshot exceeds its configured bounds")]
    SnapshotLimit,
    #[error("snapshot chunks are missing, duplicated, out of order, or expired")]
    SnapshotSequence,
    #[error("snapshot count, bytes, revision, or digest does not match")]
    SnapshotIntegrity,
    #[error("snapshot contains a duplicate key")]
    DuplicateRecord,
    #[error("Coordinator snapshot is required")]
    SnapshotRequired,
    #[error("Coordinator revision gap requires resnapshot")]
    RevisionGap,
    #[error("load report is invalid")]
    InvalidLoad,
    #[error("load report table reached its cardinality bound")]
    LoadCapacity,
}
