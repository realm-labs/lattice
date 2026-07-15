use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use lattice_core::actor_ref::{
    ConfigFingerprint, EntityType, NodeIncarnation, PlacementDomainId, ProtocolId, SingletonKind,
};
use lattice_remoting::protocol::ProtocolDescriptor;
use serde::{Deserialize, Serialize};

use lattice_core::coordinator::CoordinatorScope;
use thiserror::Error;

use crate::region::EntityConfig;
use crate::types::{
    CoordinatorTerm, MembershipVersion, MonotonicTime, NodeKey, PlacementVersion, ShardId,
};

pub const COORDINATOR_PROTOCOL_GENERATION: u64 = 5;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SingletonConfig {
    pub domain: PlacementDomainId,
    pub kind: SingletonKind,
    pub protocol_id: ProtocolId,
    config_fingerprint: ConfigFingerprint,
}

impl SingletonConfig {
    pub fn new(domain: PlacementDomainId, kind: SingletonKind, protocol_id: ProtocolId) -> Self {
        let mut canonical = Vec::new();
        canonical.extend_from_slice(&(domain.as_str().len() as u32).to_be_bytes());
        canonical.extend_from_slice(domain.as_str().as_bytes());
        canonical.extend_from_slice(&(kind.as_str().len() as u32).to_be_bytes());
        canonical.extend_from_slice(kind.as_str().as_bytes());
        canonical.extend_from_slice(&protocol_id.get().to_be_bytes());
        let config_fingerprint = ConfigFingerprint::new(*blake3::hash(&canonical).as_bytes());
        Self {
            domain,
            kind,
            protocol_id,
            config_fingerprint,
        }
    }

    pub fn fingerprint(&self) -> ConfigFingerprint {
        self.config_fingerprint
    }

    pub fn validate(&self) -> bool {
        Self::new(self.domain.clone(), self.kind.clone(), self.protocol_id).config_fingerprint
            == self.config_fingerprint
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaderRecord {
    pub scope: CoordinatorScope,
    pub node: NodeKey,
    pub protocol_generation: u64,
    pub term: CoordinatorTerm,
}

/// Exact lease-backed membership leadership identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipLeaderGuard {
    record: LeaderRecord,
}

impl MembershipLeaderGuard {
    pub fn new(record: LeaderRecord) -> Result<Self, CoordinatorError> {
        if record.scope != CoordinatorScope::Membership {
            return Err(CoordinatorError::InvalidLeader);
        }
        record.validate()?;
        Ok(Self { record })
    }

    pub fn record(&self) -> &LeaderRecord {
        &self.record
    }

    pub fn term(&self) -> CoordinatorTerm {
        self.record.term
    }
}

/// Exact lease-backed placement-domain leadership identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementLeaderGuard {
    record: LeaderRecord,
}

pub(crate) trait ExactLeaderGuard {
    fn record(&self) -> &LeaderRecord;

    fn term(&self) -> CoordinatorTerm {
        self.record().term
    }

    fn scope(&self) -> &CoordinatorScope {
        &self.record().scope
    }
}

impl ExactLeaderGuard for MembershipLeaderGuard {
    fn record(&self) -> &LeaderRecord {
        &self.record
    }
}

impl ExactLeaderGuard for PlacementLeaderGuard {
    fn record(&self) -> &LeaderRecord {
        &self.record
    }
}

impl PlacementLeaderGuard {
    pub fn new(record: LeaderRecord) -> Result<Self, CoordinatorError> {
        if !matches!(record.scope, CoordinatorScope::Placement(_)) {
            return Err(CoordinatorError::InvalidLeader);
        }
        record.validate()?;
        Ok(Self { record })
    }

    pub fn record(&self) -> &LeaderRecord {
        &self.record
    }

    pub fn term(&self) -> CoordinatorTerm {
        self.record.term
    }

    pub fn domain(&self) -> &PlacementDomainId {
        let CoordinatorScope::Placement(domain) = &self.record.scope else {
            unreachable!("placement guard constructor validates its scope")
        };
        domain
    }
}

impl LeaderRecord {
    pub fn validate(&self) -> Result<(), CoordinatorError> {
        self.node
            .validate()
            .map_err(|_| CoordinatorError::InvalidLeader)?;
        if self.protocol_generation != COORDINATOR_PROTOCOL_GENERATION {
            return Err(CoordinatorError::InvalidLeader);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberHello {
    pub node: NodeKey,
    pub roles: BTreeSet<String>,
    pub failure_domains: BTreeMap<String, String>,
    pub protocols: Vec<ProtocolDescriptor>,
    pub remoting_capabilities: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementDomainHello {
    pub node: NodeKey,
    pub domain: PlacementDomainId,
    pub domain_config_fingerprint: ConfigFingerprint,
    pub capacity_units: u64,
    pub hosted_entity_types: BTreeSet<EntityType>,
    pub proxied_entity_types: BTreeSet<EntityType>,
    pub singleton_eligibility: BTreeSet<SingletonKind>,
    pub used_singletons: BTreeSet<SingletonKind>,
    pub entity_configs: Vec<EntityConfig>,
    pub singleton_configs: Vec<SingletonConfig>,
    pub constraints: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberStatus {
    Joining,
    Up,
    Leaving,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberRecord {
    pub node: NodeKey,
    pub hello: MemberHello,
    pub status: MemberStatus,
    pub version: MembershipVersion,
    pub lease_id: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DomainMemberStatus {
    Joining,
    Up,
    Leaving,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainMemberRecord {
    pub node: NodeKey,
    pub hello: PlacementDomainHello,
    pub status: DomainMemberStatus,
    pub version: PlacementVersion,
}

impl DomainMemberRecord {
    pub fn validate(&self, limits: &SessionLimits) -> Result<(), CoordinatorError> {
        self.hello.validate(limits)?;
        if self.node != self.hello.node || self.version.domain != self.hello.domain {
            return Err(CoordinatorError::InvalidDomainMember);
        }
        Ok(())
    }
}

impl MemberRecord {
    pub fn validate(&self, limits: &SessionLimits) -> Result<(), CoordinatorError> {
        self.hello.validate(limits)?;
        if self.node != self.hello.node || self.lease_id == 0 {
            return Err(CoordinatorError::InvalidMember);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberRemovalReason {
    GracefulLeave,
    FailureDetected,
    ForceRemoved,
    IncarnationReplaced,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberChange {
    Upsert(Box<MemberRecord>),
    Removed {
        node: NodeKey,
        reason: MemberRemovalReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberEvent {
    pub version: MembershipVersion,
    pub change: MemberChange,
}

impl MemberHello {
    pub fn validate(&self, limits: &SessionLimits) -> Result<(), CoordinatorError> {
        self.node
            .validate()
            .map_err(|_| CoordinatorError::InvalidHello)?;
        if self.roles.len() > limits.maximum_roles
            || self.failure_domains.len() > limits.maximum_attributes
            || self.protocols.len() > limits.maximum_protocols
            || self.remoting_capabilities.len() > limits.maximum_capabilities
            || self
                .roles
                .iter()
                .any(|role| role.is_empty() || role.len() > 128)
            || self.failure_domains.iter().any(|(key, value)| {
                key.is_empty() || key.len() > 128 || value.is_empty() || value.len() > 256
            })
            || self
                .remoting_capabilities
                .iter()
                .any(|capability| capability.is_empty() || capability.len() > 128)
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
}

impl PlacementDomainHello {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node: NodeKey,
        domain: PlacementDomainId,
        capacity_units: u64,
        hosted_entity_types: BTreeSet<EntityType>,
        proxied_entity_types: BTreeSet<EntityType>,
        singleton_eligibility: BTreeSet<SingletonKind>,
        used_singletons: BTreeSet<SingletonKind>,
        entity_configs: Vec<EntityConfig>,
        singleton_configs: Vec<SingletonConfig>,
        constraints: BTreeMap<String, String>,
    ) -> Self {
        let domain_config_fingerprint = placement_domain_fingerprint(&domain);
        Self {
            node,
            domain,
            domain_config_fingerprint,
            capacity_units,
            hosted_entity_types,
            proxied_entity_types,
            singleton_eligibility,
            used_singletons,
            entity_configs,
            singleton_configs,
            constraints,
        }
    }

    pub fn validate(&self, limits: &SessionLimits) -> Result<(), CoordinatorError> {
        self.node
            .validate()
            .map_err(|_| CoordinatorError::InvalidHello)?;
        if self.capacity_units == 0
            || self.domain_config_fingerprint != placement_domain_fingerprint(&self.domain)
            || self.hosted_entity_types.len() > limits.maximum_entity_types
            || self.proxied_entity_types.len() > limits.maximum_entity_types
            || self.singleton_eligibility.len() > limits.maximum_singletons
            || self.used_singletons.len() > limits.maximum_singletons
            || self.entity_configs.len() > limits.maximum_entity_types
            || self.singleton_configs.len() > limits.maximum_singletons
            || self.constraints.len() > limits.maximum_attributes
            || self.constraints.iter().any(|(key, value)| {
                key.is_empty() || key.len() > 128 || value.is_empty() || value.len() > 256
            })
        {
            return Err(CoordinatorError::InvalidHello);
        }
        let mut entity_types = BTreeSet::new();
        if self.entity_configs.iter().any(|config| {
            config.validate().is_err()
                || config.domain != self.domain
                || !entity_types.insert(config.entity_type.clone())
                || !self.hosted_entity_types.contains(&config.entity_type)
        }) {
            return Err(CoordinatorError::InvalidHello);
        }
        let mut singleton_kinds = BTreeSet::new();
        if self.singleton_configs.iter().any(|config| {
            !config.validate()
                || config.domain != self.domain
                || !singleton_kinds.insert((config.domain.clone(), config.kind.clone()))
                || !self.singleton_eligibility.contains(&config.kind)
        }) {
            return Err(CoordinatorError::InvalidHello);
        }
        Ok(())
    }

    pub fn subscribes_to(&self, entity_type: &EntityType) -> bool {
        self.hosted_entity_types.contains(entity_type)
            || self.proxied_entity_types.contains(entity_type)
    }
}

fn placement_domain_fingerprint(domain: &PlacementDomainId) -> ConfigFingerprint {
    let mut canonical = b"lattice-placement-domain-v1".to_vec();
    canonical.extend_from_slice(&(domain.as_str().len() as u32).to_be_bytes());
    canonical.extend_from_slice(domain.as_str().as_bytes());
    ConfigFingerprint::new(*blake3::hash(&canonical).as_bytes())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionLimits {
    pub maximum_roles: usize,
    pub maximum_attributes: usize,
    pub maximum_capabilities: usize,
    pub maximum_entity_types: usize,
    pub maximum_singletons: usize,
    pub maximum_protocols: usize,
}

impl Default for SessionLimits {
    fn default() -> Self {
        Self {
            maximum_roles: 32,
            maximum_attributes: 64,
            maximum_capabilities: 64,
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SnapshotRecord {
    pub key: String,
    pub value: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotVersion {
    Membership(MembershipVersion),
    Placement(PlacementVersion),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotBegin {
    pub snapshot_id: u128,
    pub version: SnapshotVersion,
    pub record_count: usize,
    pub total_bytes: usize,
    pub chunk_count: usize,
    pub digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotChunk {
    pub snapshot_id: u128,
    pub index: usize,
    pub records: Vec<SnapshotRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotEnd {
    pub snapshot_id: u128,
    pub version: SnapshotVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotInstall {
    pub version: SnapshotVersion,
    pub records: Vec<SnapshotRecord>,
}

pub fn build_snapshot(
    version: SnapshotVersion,
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
        version: version.clone(),
        record_count: chunks.iter().map(|chunk| chunk.records.len()).sum(),
        total_bytes,
        chunk_count: chunks.len(),
        digest,
    };
    let end = SnapshotEnd {
        snapshot_id,
        version,
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
            || end.version != self.begin.version
            || self.next_chunk != self.begin.chunk_count
            || self.records.len() != self.begin.record_count
            || self.bytes != self.begin.total_bytes
            || snapshot_digest(&self.records) != self.begin.digest
        {
            return Err(CoordinatorError::SnapshotIntegrity);
        }
        Ok(SnapshotInstall {
            version: self.begin.version,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoordinatorDelta {
    pub version: PlacementVersion,
    pub records: Vec<SnapshotRecord>,
}

#[derive(Debug, Default)]
pub struct MembershipState {
    version: Option<MembershipVersion>,
    records: BTreeMap<String, Bytes>,
    ready: bool,
}

impl MembershipState {
    pub fn install(&mut self, snapshot: SnapshotInstall) -> Result<(), MembershipStateError> {
        let SnapshotVersion::Membership(version) = snapshot.version else {
            return Err(MembershipStateError::UnexpectedSnapshot);
        };
        if self.version.is_some_and(|current| {
            version.term < current.term
                || (version.term == current.term && version.revision < current.revision)
        }) {
            self.ready = false;
            return Err(MembershipStateError::StaleTerm);
        }
        let mut records = BTreeMap::new();
        for record in snapshot.records {
            if records.insert(record.key, record.value).is_some() {
                return Err(MembershipStateError::DuplicateRecord);
            }
        }
        self.version = Some(version);
        self.records = records;
        self.ready = true;
        Ok(())
    }

    pub fn apply(&mut self, event: MemberEvent) -> Result<(), MembershipStateError> {
        let current = self.version.ok_or(MembershipStateError::SnapshotRequired)?;
        if !current.accepts_delta_after(event.version) {
            self.ready = false;
            return if event.version.term > current.term {
                Err(MembershipStateError::SnapshotRequired)
            } else if event.version.term < current.term {
                Err(MembershipStateError::StaleTerm)
            } else {
                Err(MembershipStateError::RevisionGap)
            };
        }
        match event.change {
            MemberChange::Upsert(member) => {
                let key = format!("member/{}", member.node.node_id);
                let value =
                    serde_json::to_vec(&member).map_err(|_| MembershipStateError::MemberCodec)?;
                self.records.insert(key, Bytes::from(value));
            }
            MemberChange::Removed { node, .. } => {
                let key = format!("member/{}", node.node_id);
                let remove = self
                    .records
                    .get(&key)
                    .and_then(|value| serde_json::from_slice::<MemberRecord>(value).ok())
                    .is_some_and(|member| member.node == node);
                if remove {
                    self.records.remove(&key);
                }
            }
        }
        self.version = Some(event.version);
        Ok(())
    }

    pub fn version(&self) -> Option<MembershipVersion> {
        self.version
    }

    pub fn records(&self) -> impl ExactSizeIterator<Item = (&str, &Bytes)> {
        self.records
            .iter()
            .map(|(key, value)| (key.as_str(), value))
    }

    pub fn ready(&self) -> bool {
        self.ready
    }

    pub fn member(&self, node: &NodeKey) -> Option<MemberRecord> {
        self.records
            .get(&format!("member/{}", node.node_id))
            .and_then(|value| serde_json::from_slice(value).ok())
            .filter(|member: &MemberRecord| member.node == *node)
    }
}

#[derive(Debug)]
pub struct PlacementDomainState {
    domain: PlacementDomainId,
    version: Option<PlacementVersion>,
    records: BTreeMap<String, Bytes>,
    ready: bool,
}

impl PlacementDomainState {
    pub fn new(domain: PlacementDomainId) -> Self {
        Self {
            domain,
            version: None,
            records: BTreeMap::new(),
            ready: false,
        }
    }

    pub fn install(&mut self, snapshot: SnapshotInstall) -> Result<(), PlacementDomainStateError> {
        let SnapshotVersion::Placement(version) = snapshot.version else {
            return Err(PlacementDomainStateError::UnexpectedSnapshot);
        };
        if version.domain != self.domain {
            return Err(PlacementDomainStateError::DomainMismatch);
        }
        if self.version.as_ref().is_some_and(|current| {
            version.term < current.term
                || (version.term == current.term && version.revision < current.revision)
        }) {
            self.ready = false;
            return Err(PlacementDomainStateError::StaleTerm);
        }
        let mut records = BTreeMap::new();
        for record in snapshot.records {
            if records.insert(record.key, record.value).is_some() {
                return Err(PlacementDomainStateError::DuplicateRecord);
            }
        }
        self.version = Some(version);
        self.records = records;
        self.ready = true;
        Ok(())
    }

    pub fn apply(&mut self, delta: CoordinatorDelta) -> Result<(), PlacementDomainStateError> {
        if delta.version.domain != self.domain {
            return Err(PlacementDomainStateError::DomainMismatch);
        }
        let current = self
            .version
            .as_ref()
            .ok_or(PlacementDomainStateError::SnapshotRequired)?;
        if !current.accepts_delta_after(&delta.version) {
            self.ready = false;
            return if delta.version.term > current.term {
                Err(PlacementDomainStateError::SnapshotRequired)
            } else if delta.version.term < current.term {
                Err(PlacementDomainStateError::StaleTerm)
            } else {
                Err(PlacementDomainStateError::RevisionGap)
            };
        }
        for record in delta.records {
            self.records.insert(record.key, record.value);
        }
        self.version = Some(delta.version);
        Ok(())
    }

    pub fn version(&self) -> Option<&PlacementVersion> {
        self.version.as_ref()
    }

    pub fn records(&self) -> impl Iterator<Item = (&str, &Bytes)> {
        self.records
            .iter()
            .map(|(key, value)| (key.as_str(), value))
    }

    pub fn ready(&self) -> bool {
        self.ready
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MembershipStateError {
    #[error("membership snapshot is required")]
    SnapshotRequired,
    #[error("membership revision gap requires a snapshot")]
    RevisionGap,
    #[error("membership state belongs to a stale term")]
    StaleTerm,
    #[error("membership snapshot contains a duplicate key")]
    DuplicateRecord,
    #[error("membership record codec failed")]
    MemberCodec,
    #[error("placement snapshot cannot be installed in membership state")]
    UnexpectedSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PlacementDomainStateError {
    #[error("placement-domain snapshot is required")]
    SnapshotRequired,
    #[error("placement-domain revision gap requires a snapshot")]
    RevisionGap,
    #[error("placement-domain state belongs to a stale term")]
    StaleTerm,
    #[error("placement-domain snapshot contains a duplicate key")]
    DuplicateRecord,
    #[error("placement snapshot belongs to another domain")]
    DomainMismatch,
    #[error("membership snapshot cannot be installed in placement-domain state")]
    UnexpectedSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeLoadReport {
    pub node: NodeKey,
    pub sequence: u64,
    pub observed_at: MonotonicTime,
    pub total_weight: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

    pub fn node(&self, incarnation: NodeIncarnation) -> Option<&NodeLoadReport> {
        self.nodes.get(&incarnation)
    }

    pub fn shard(
        &self,
        incarnation: NodeIncarnation,
        entity_type: &EntityType,
        shard_id: ShardId,
    ) -> Option<&ShardLoadReport> {
        self.shards
            .get(&(incarnation, entity_type.clone(), shard_id))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CoordinatorError {
    #[error("Coordinator leader record is invalid or uses an unsupported generation")]
    InvalidLeader,
    #[error("Coordinator session limits are invalid")]
    InvalidLimits,
    #[error("node registration is invalid or over its bounds")]
    InvalidHello,
    #[error("member record is invalid")]
    InvalidMember,
    #[error("placement-domain member record is invalid")]
    InvalidDomainMember,
    #[error("member record codec failed")]
    MemberCodec,
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
    #[error("Coordinator state belongs to a stale term")]
    StaleTerm,
    #[error("load report is invalid")]
    InvalidLoad,
    #[error("load report table reached its cardinality bound")]
    LoadCapacity,
}

#[cfg(test)]
mod state_version_tests {
    use super::*;
    use crate::types::{CoordinatorTerm, Revision};

    fn version(term: u64, revision: u64) -> PlacementVersion {
        PlacementVersion::new(
            PlacementDomainId::new("test").unwrap(),
            CoordinatorTerm::new(term).unwrap(),
            Revision::new(revision).unwrap(),
        )
    }

    #[test]
    fn singleton_config_fingerprint_has_a_domain_scoped_golden_vector() {
        let config = SingletonConfig::new(
            PlacementDomainId::new("battle").unwrap(),
            SingletonKind::new("scheduler").unwrap(),
            ProtocolId::new(0x1112_1314_1516_1718).unwrap(),
        );
        assert_eq!(
            *config.fingerprint().as_bytes(),
            [
                217, 135, 248, 93, 126, 177, 148, 200, 159, 106, 130, 175, 125, 104, 218, 226, 30,
                96, 2, 62, 254, 44, 79, 26, 117, 71, 189, 8, 177, 11, 237, 216,
            ]
        );
        assert_ne!(
            config.fingerprint(),
            SingletonConfig::new(
                PlacementDomainId::new("world").unwrap(),
                config.kind.clone(),
                config.protocol_id,
            )
            .fingerprint()
        );
    }

    #[test]
    fn higher_term_delta_requires_snapshot_and_lower_term_snapshot_is_stale() {
        let mut session = PlacementDomainState::new(PlacementDomainId::new("test").unwrap());
        session
            .install(SnapshotInstall {
                version: SnapshotVersion::Placement(version(1, 7)),
                records: Vec::new(),
            })
            .unwrap();
        assert_eq!(
            session
                .apply(CoordinatorDelta {
                    version: version(2, 8),
                    records: Vec::new(),
                })
                .unwrap_err(),
            PlacementDomainStateError::SnapshotRequired
        );
        assert!(!session.ready());
        session
            .install(SnapshotInstall {
                version: SnapshotVersion::Placement(version(2, 8)),
                records: Vec::new(),
            })
            .unwrap();
        assert_eq!(
            session
                .install(SnapshotInstall {
                    version: SnapshotVersion::Placement(version(1, 9)),
                    records: Vec::new(),
                })
                .unwrap_err(),
            PlacementDomainStateError::StaleTerm
        );
        assert!(!session.ready());
    }
}
