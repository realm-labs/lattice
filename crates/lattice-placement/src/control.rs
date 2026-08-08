use std::{collections::BTreeMap, sync::RwLock, time::Duration};

use bytes::Bytes;
use lattice_core::{
    actor_ref::{EntityType, NodeIncarnation, PlacementDomainId, SingletonKind},
    coordinator::CoordinatorScope,
};
use lattice_remoting::{
    association::AssociationKey,
    control::{
        CommandId, ControlDispatch, ControlDispatchError, ControlGap, ControlRejectReason,
        ControlRetryReason, ControlStreamId,
    },
};
use prost::Message;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::{
    coordinator::{
        CoordinatorDelta, MemberEvent, MemberHello, MemberRecord, MemberRemovalReason,
        NodeLoadReport, PlacementDomainHello, ShardLoadReport, SnapshotBegin, SnapshotChunk,
        SnapshotEnd, SnapshotRecord, SnapshotVersion,
    },
    types::{
        AssignmentGeneration, ClaimGrant, CoordinatorTerm, MembershipVersion, NodeKey,
        PlacementSlotKey, PlacementVersion, Revision, ShardId,
    },
};

// Generation 9 gives snapshots a dedicated protobuf representation. Other commands retain their
// established JSON representation inside the protobuf envelope, so Serde compatibility fields on
// domain models never pass through a positional binary codec.
pub const PLACEMENT_CONTROL_GENERATION: u64 = 9;
pub const DEFAULT_MAX_CONTROL_PAYLOAD: usize = 256 * 1024;

pub fn control_stream_id(scope: &CoordinatorScope) -> ControlStreamId {
    match scope {
        CoordinatorScope::Membership => {
            ControlStreamId::new(3).expect("membership control stream ID is nonzero")
        }
        CoordinatorScope::Placement(domain) => {
            let mut canonical = b"lattice-placement-control-stream-v1\0".to_vec();
            canonical.extend_from_slice(domain.as_str().as_bytes());
            let digest = blake3::hash(&canonical);
            let mut encoded = [0_u8; 16];
            encoded.copy_from_slice(&digest.as_bytes()[..16]);
            let value = u128::from_be_bytes(encoded) | (1_u128 << 127);
            ControlStreamId::new(value).expect("placement control stream hash is nonzero")
        }
    }
}

fn map_try_send_error<T>(error: mpsc::error::TrySendError<T>) -> ControlDispatchError {
    match error {
        mpsc::error::TrySendError::Full(_) => {
            ControlDispatchError::RetryLater(ControlRetryReason::ConsumerBusy)
        }
        mpsc::error::TrySendError::Closed(_) => {
            ControlDispatchError::RetryLater(ControlRetryReason::AssociationStarting)
        }
    }
}

fn application_timeout() -> ControlDispatchError {
    ControlDispatchError::RetryLater(ControlRetryReason::ApplicationTimeout)
}

fn completion_closed() -> ControlDispatchError {
    ControlDispatchError::RetryLater(ControlRetryReason::AssociationStarting)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementControlCommand {
    MemberHello(MemberHello),
    PlacementDomainHello(PlacementDomainHello),
    NodeHeartbeat {
        incarnation: NodeIncarnation,
        sequence: u64,
    },
    JoinReady {
        snapshot_version: MembershipVersion,
    },
    MemberUp(MemberRecord),
    MemberDelta(MemberEvent),
    SubscribeEntity(EntityType),
    SubscribeSingleton(SingletonKind),
    SnapshotBegin(SnapshotBegin),
    SnapshotChunk(SnapshotChunk),
    SnapshotEnd(SnapshotEnd),
    StateDelta(CoordinatorDelta),
    AppliedRevision(PlacementVersion),
    ClaimGranted(ClaimGrant),
    NodeLoad(NodeLoadReport),
    ShardLoad(ShardLoadReport),
    ResolveShard {
        request_id: u128,
        domain: PlacementDomainId,
        entity_type: EntityType,
        shard_id: ShardId,
    },
    ResolveSingleton {
        request_id: u128,
        domain: PlacementDomainId,
        kind: SingletonKind,
    },
    ResolutionFailed {
        request_id: u128,
        slot: PlacementSlotKey,
        reason: PlacementResolutionFailure,
    },
    DrainSlot {
        slot: PlacementSlotKey,
        generation: AssignmentGeneration,
        version: PlacementVersion,
    },
    SlotDrained {
        slot: PlacementSlotKey,
        generation: AssignmentGeneration,
    },
    SlotStopFailed {
        slot: PlacementSlotKey,
        generation: AssignmentGeneration,
    },
    SlotReady {
        slot: PlacementSlotKey,
        generation: AssignmentGeneration,
    },
    BeginDrain {
        operation_id: String,
        expected_incarnation: NodeIncarnation,
    },
    DrainReady {
        operation_id: String,
        expected_incarnation: NodeIncarnation,
    },
    DrainComplete {
        operation_id: String,
        expected_incarnation: NodeIncarnation,
    },
    MembershipDrainComplete {
        operation_id: String,
        expected_incarnation: NodeIncarnation,
    },
    ForceRemove {
        operation_id: String,
        node_id: String,
        expected_incarnation: NodeIncarnation,
    },
}

impl PlacementControlCommand {
    pub const fn name(&self) -> &'static str {
        match self {
            Self::MemberHello(_) => "MemberHello",
            Self::PlacementDomainHello(_) => "PlacementDomainHello",
            Self::NodeHeartbeat { .. } => "NodeHeartbeat",
            Self::JoinReady { .. } => "JoinReady",
            Self::MemberUp(_) => "MemberUp",
            Self::MemberDelta(_) => "MemberDelta",
            Self::SubscribeEntity(_) => "SubscribeEntity",
            Self::SubscribeSingleton(_) => "SubscribeSingleton",
            Self::SnapshotBegin(_) => "SnapshotBegin",
            Self::SnapshotChunk(_) => "SnapshotChunk",
            Self::SnapshotEnd(_) => "SnapshotEnd",
            Self::StateDelta(_) => "StateDelta",
            Self::AppliedRevision(_) => "AppliedRevision",
            Self::ClaimGranted(_) => "ClaimGranted",
            Self::NodeLoad(_) => "NodeLoad",
            Self::ShardLoad(_) => "ShardLoad",
            Self::ResolveShard { .. } => "ResolveShard",
            Self::ResolveSingleton { .. } => "ResolveSingleton",
            Self::ResolutionFailed { .. } => "ResolutionFailed",
            Self::DrainSlot { .. } => "DrainSlot",
            Self::SlotDrained { .. } => "SlotDrained",
            Self::SlotStopFailed { .. } => "SlotStopFailed",
            Self::SlotReady { .. } => "SlotReady",
            Self::BeginDrain { .. } => "BeginDrain",
            Self::DrainReady { .. } => "DrainReady",
            Self::DrainComplete { .. } => "DrainComplete",
            Self::MembershipDrainComplete { .. } => "MembershipDrainComplete",
            Self::ForceRemove { .. } => "ForceRemove",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementResolutionFailure {
    NoEligibleHost,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedPlacementControlCommand {
    pub scope: CoordinatorScope,
    #[serde(default)]
    pub coordinator_term: Option<u64>,
    pub command: PlacementControlCommand,
}

pub fn encode_control_command(
    scope: &CoordinatorScope,
    command: &PlacementControlCommand,
    maximum_payload: usize,
) -> Result<Bytes, PlacementControlError> {
    encode_control_command_inner(scope, None, command, maximum_payload)
}

pub fn encode_control_command_for_term(
    scope: &CoordinatorScope,
    coordinator_term: u64,
    command: &PlacementControlCommand,
    maximum_payload: usize,
) -> Result<Bytes, PlacementControlError> {
    if coordinator_term == 0 {
        return Err(PlacementControlError::InvalidCoordinatorTerm);
    }
    encode_control_command_inner(scope, Some(coordinator_term), command, maximum_payload)
}

fn encode_control_command_inner(
    scope: &CoordinatorScope,
    coordinator_term: Option<u64>,
    command: &PlacementControlCommand,
    maximum_payload: usize,
) -> Result<Bytes, PlacementControlError> {
    if maximum_payload == 0 {
        return Err(PlacementControlError::InvalidLimit);
    }
    let encoded = encode_control_command_unbounded(scope, coordinator_term, command)?;
    if encoded.len() > maximum_payload {
        return Err(PlacementControlError::PayloadTooLarge);
    }
    Ok(Bytes::from(encoded))
}

/// Returns the final number of bytes sent through the control lane, including the protobuf
/// envelope. Snapshot construction uses this instead of estimating from the raw Etcd values.
pub fn encoded_control_command_len(
    scope: &CoordinatorScope,
    coordinator_term: Option<u64>,
    command: &PlacementControlCommand,
) -> Result<usize, PlacementControlError> {
    Ok(encode_control_command_unbounded(scope, coordinator_term, command)?.len())
}

fn encode_control_command_unbounded(
    scope: &CoordinatorScope,
    coordinator_term: Option<u64>,
    command: &PlacementControlCommand,
) -> Result<Vec<u8>, PlacementControlError> {
    if coordinator_term == Some(0) {
        return Err(PlacementControlError::InvalidCoordinatorTerm);
    }
    let wire = PlacementControlWire {
        generation: PLACEMENT_CONTROL_GENERATION,
        scope: Some(PlacementControlScopeWire::from_scope(scope)),
        coordinator_term,
        command: Some(placement_control_wire::Command::from_command(command)?),
    };
    Ok(wire.encode_to_vec())
}

pub fn decode_control_command(
    payload: &[u8],
    maximum_payload: usize,
) -> Result<ScopedPlacementControlCommand, PlacementControlError> {
    if maximum_payload == 0 {
        return Err(PlacementControlError::InvalidLimit);
    }
    if payload.len() > maximum_payload {
        return Err(PlacementControlError::PayloadTooLarge);
    }
    let wire = PlacementControlWire::decode(payload).map_err(|_| PlacementControlError::Codec)?;
    if wire.generation != PLACEMENT_CONTROL_GENERATION {
        return Err(PlacementControlError::GenerationMismatch);
    }
    let scope = wire
        .scope
        .ok_or(PlacementControlError::Codec)?
        .into_scope()?;
    if wire.coordinator_term == Some(0) {
        return Err(PlacementControlError::InvalidCoordinatorTerm);
    }
    let command = wire
        .command
        .ok_or(PlacementControlError::Codec)?
        .into_command()?;
    Ok(ScopedPlacementControlCommand {
        scope,
        coordinator_term: wire.coordinator_term,
        command,
    })
}

#[derive(Clone, PartialEq, Message)]
struct PlacementControlWire {
    #[prost(uint64, tag = "1")]
    generation: u64,
    #[prost(message, optional, tag = "2")]
    scope: Option<PlacementControlScopeWire>,
    #[prost(uint64, optional, tag = "3")]
    coordinator_term: Option<u64>,
    #[prost(oneof = "placement_control_wire::Command", tags = "4, 5, 6, 7")]
    command: Option<placement_control_wire::Command>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum PlacementControlScopeKindWire {
    Unspecified = 0,
    Membership = 1,
    Placement = 2,
}

#[derive(Clone, PartialEq, Message)]
struct PlacementControlScopeWire {
    #[prost(enumeration = "PlacementControlScopeKindWire", tag = "1")]
    kind: i32,
    #[prost(string, tag = "2")]
    placement_domain: String,
}

impl PlacementControlScopeWire {
    fn from_scope(scope: &CoordinatorScope) -> Self {
        match scope {
            CoordinatorScope::Membership => Self {
                kind: PlacementControlScopeKindWire::Membership as i32,
                placement_domain: String::new(),
            },
            CoordinatorScope::Placement(domain) => Self {
                kind: PlacementControlScopeKindWire::Placement as i32,
                placement_domain: domain.as_str().to_owned(),
            },
        }
    }

    fn into_scope(self) -> Result<CoordinatorScope, PlacementControlError> {
        match PlacementControlScopeKindWire::try_from(self.kind)
            .map_err(|_| PlacementControlError::Codec)?
        {
            PlacementControlScopeKindWire::Membership if self.placement_domain.is_empty() => {
                Ok(CoordinatorScope::Membership)
            }
            PlacementControlScopeKindWire::Placement => {
                PlacementDomainId::new(self.placement_domain)
                    .map(CoordinatorScope::Placement)
                    .map_err(|_| PlacementControlError::Codec)
            }
            _ => Err(PlacementControlError::Codec),
        }
    }
}

mod placement_control_wire {
    use super::{
        PlacementControlCommand, PlacementControlError, SnapshotBeginWire, SnapshotChunkWire,
        SnapshotEndWire,
    };

    #[derive(Clone, PartialEq, prost::Oneof)]
    pub(super) enum Command {
        #[prost(bytes, tag = "4")]
        Json(Vec<u8>),
        #[prost(message, tag = "5")]
        SnapshotBegin(SnapshotBeginWire),
        #[prost(message, tag = "6")]
        SnapshotChunk(SnapshotChunkWire),
        #[prost(message, tag = "7")]
        SnapshotEnd(SnapshotEndWire),
    }

    impl Command {
        pub(super) fn from_command(
            command: &PlacementControlCommand,
        ) -> Result<Self, PlacementControlError> {
            Ok(match command {
                PlacementControlCommand::SnapshotBegin(begin) => {
                    Self::SnapshotBegin(SnapshotBeginWire::from_begin(begin)?)
                }
                PlacementControlCommand::SnapshotChunk(chunk) => {
                    Self::SnapshotChunk(SnapshotChunkWire::from_chunk(chunk)?)
                }
                PlacementControlCommand::SnapshotEnd(end) => {
                    Self::SnapshotEnd(SnapshotEndWire::from_end(end))
                }
                command => Self::Json(
                    serde_json::to_vec(command).map_err(|_| PlacementControlError::Codec)?,
                ),
            })
        }

        pub(super) fn into_command(self) -> Result<PlacementControlCommand, PlacementControlError> {
            match self {
                Self::Json(payload) => {
                    let command: PlacementControlCommand = serde_json::from_slice(&payload)
                        .map_err(|_| PlacementControlError::Codec)?;
                    if matches!(
                        command,
                        PlacementControlCommand::SnapshotBegin(_)
                            | PlacementControlCommand::SnapshotChunk(_)
                            | PlacementControlCommand::SnapshotEnd(_)
                    ) {
                        return Err(PlacementControlError::Codec);
                    }
                    Ok(command)
                }
                Self::SnapshotBegin(begin) => begin
                    .into_begin()
                    .map(PlacementControlCommand::SnapshotBegin),
                Self::SnapshotChunk(chunk) => chunk
                    .into_chunk()
                    .map(PlacementControlCommand::SnapshotChunk),
                Self::SnapshotEnd(end) => end.into_end().map(PlacementControlCommand::SnapshotEnd),
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum SnapshotVersionKindWire {
    Unspecified = 0,
    Membership = 1,
    Placement = 2,
}

#[derive(Clone, PartialEq, Message)]
struct SnapshotVersionWire {
    #[prost(enumeration = "SnapshotVersionKindWire", tag = "1")]
    kind: i32,
    #[prost(string, tag = "2")]
    placement_domain: String,
    #[prost(uint64, tag = "3")]
    coordinator_term: u64,
    #[prost(uint64, tag = "4")]
    revision: u64,
}

impl SnapshotVersionWire {
    fn from_version(version: &SnapshotVersion) -> Self {
        match version {
            SnapshotVersion::Membership(version) => Self {
                kind: SnapshotVersionKindWire::Membership as i32,
                placement_domain: String::new(),
                coordinator_term: version.term.get(),
                revision: version.revision.get(),
            },
            SnapshotVersion::Placement(version) => Self {
                kind: SnapshotVersionKindWire::Placement as i32,
                placement_domain: version.domain.as_str().to_owned(),
                coordinator_term: version.term.get(),
                revision: version.revision.get(),
            },
        }
    }

    fn into_version(self) -> Result<SnapshotVersion, PlacementControlError> {
        let term = CoordinatorTerm::new(self.coordinator_term)
            .map_err(|_| PlacementControlError::Codec)?;
        let revision = Revision::new(self.revision).map_err(|_| PlacementControlError::Codec)?;
        match SnapshotVersionKindWire::try_from(self.kind)
            .map_err(|_| PlacementControlError::Codec)?
        {
            SnapshotVersionKindWire::Membership if self.placement_domain.is_empty() => Ok(
                SnapshotVersion::Membership(MembershipVersion::new(term, revision)),
            ),
            SnapshotVersionKindWire::Placement => {
                let domain = PlacementDomainId::new(self.placement_domain)
                    .map_err(|_| PlacementControlError::Codec)?;
                Ok(SnapshotVersion::Placement(PlacementVersion::new(
                    domain, term, revision,
                )))
            }
            _ => Err(PlacementControlError::Codec),
        }
    }
}

#[derive(Clone, PartialEq, Message)]
struct SnapshotRecordWire {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
}

impl From<&SnapshotRecord> for SnapshotRecordWire {
    fn from(record: &SnapshotRecord) -> Self {
        Self {
            key: record.key.clone(),
            value: record.value.to_vec(),
        }
    }
}

impl From<SnapshotRecordWire> for SnapshotRecord {
    fn from(record: SnapshotRecordWire) -> Self {
        Self {
            key: record.key,
            value: Bytes::from(record.value),
        }
    }
}

#[derive(Clone, PartialEq, Message)]
struct SnapshotBeginWire {
    #[prost(bytes = "vec", tag = "1")]
    snapshot_id: Vec<u8>,
    #[prost(message, optional, tag = "2")]
    version: Option<SnapshotVersionWire>,
    #[prost(uint64, tag = "3")]
    record_count: u64,
    #[prost(uint64, tag = "4")]
    total_bytes: u64,
    #[prost(uint64, tag = "5")]
    chunk_count: u64,
    #[prost(bytes = "vec", tag = "6")]
    digest: Vec<u8>,
}

impl SnapshotBeginWire {
    fn from_begin(begin: &SnapshotBegin) -> Result<Self, PlacementControlError> {
        Ok(Self {
            snapshot_id: begin.snapshot_id.to_be_bytes().to_vec(),
            version: Some(SnapshotVersionWire::from_version(&begin.version)),
            record_count: u64::try_from(begin.record_count)
                .map_err(|_| PlacementControlError::Codec)?,
            total_bytes: u64::try_from(begin.total_bytes)
                .map_err(|_| PlacementControlError::Codec)?,
            chunk_count: u64::try_from(begin.chunk_count)
                .map_err(|_| PlacementControlError::Codec)?,
            digest: begin.digest.to_vec(),
        })
    }

    fn into_begin(self) -> Result<SnapshotBegin, PlacementControlError> {
        Ok(SnapshotBegin {
            snapshot_id: decode_u128(&self.snapshot_id)?,
            version: self
                .version
                .ok_or(PlacementControlError::Codec)?
                .into_version()?,
            record_count: usize::try_from(self.record_count)
                .map_err(|_| PlacementControlError::Codec)?,
            total_bytes: usize::try_from(self.total_bytes)
                .map_err(|_| PlacementControlError::Codec)?,
            chunk_count: usize::try_from(self.chunk_count)
                .map_err(|_| PlacementControlError::Codec)?,
            digest: decode_digest(&self.digest)?,
        })
    }
}

#[derive(Clone, PartialEq, Message)]
struct SnapshotChunkWire {
    #[prost(bytes = "vec", tag = "1")]
    snapshot_id: Vec<u8>,
    #[prost(uint64, tag = "2")]
    index: u64,
    #[prost(message, repeated, tag = "3")]
    records: Vec<SnapshotRecordWire>,
}

impl SnapshotChunkWire {
    fn from_chunk(chunk: &SnapshotChunk) -> Result<Self, PlacementControlError> {
        Ok(Self {
            snapshot_id: chunk.snapshot_id.to_be_bytes().to_vec(),
            index: u64::try_from(chunk.index).map_err(|_| PlacementControlError::Codec)?,
            records: chunk.records.iter().map(SnapshotRecordWire::from).collect(),
        })
    }

    fn into_chunk(self) -> Result<SnapshotChunk, PlacementControlError> {
        Ok(SnapshotChunk {
            snapshot_id: decode_u128(&self.snapshot_id)?,
            index: usize::try_from(self.index).map_err(|_| PlacementControlError::Codec)?,
            records: self.records.into_iter().map(SnapshotRecord::from).collect(),
        })
    }
}

#[derive(Clone, PartialEq, Message)]
struct SnapshotEndWire {
    #[prost(bytes = "vec", tag = "1")]
    snapshot_id: Vec<u8>,
    #[prost(message, optional, tag = "2")]
    version: Option<SnapshotVersionWire>,
}

impl SnapshotEndWire {
    fn from_end(end: &SnapshotEnd) -> Self {
        Self {
            snapshot_id: end.snapshot_id.to_be_bytes().to_vec(),
            version: Some(SnapshotVersionWire::from_version(&end.version)),
        }
    }

    fn into_end(self) -> Result<SnapshotEnd, PlacementControlError> {
        Ok(SnapshotEnd {
            snapshot_id: decode_u128(&self.snapshot_id)?,
            version: self
                .version
                .ok_or(PlacementControlError::Codec)?
                .into_version()?,
        })
    }
}

fn decode_u128(bytes: &[u8]) -> Result<u128, PlacementControlError> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| PlacementControlError::Codec)?;
    Ok(u128::from_be_bytes(bytes))
}

fn decode_digest(bytes: &[u8]) -> Result<[u8; 32], PlacementControlError> {
    bytes.try_into().map_err(|_| PlacementControlError::Codec)
}

#[derive(Debug, Clone)]
pub struct InboundPlacementControl {
    pub association: AssociationKey,
    pub command_id: CommandId,
    pub scope: CoordinatorScope,
    pub coordinator_term: Option<u64>,
    pub command: PlacementControlCommand,
}

#[derive(Debug, Clone)]
pub enum PlacementControlEventKind {
    Command(Box<InboundPlacementControl>),
    Reconcile {
        association: AssociationKey,
        gap: Option<ControlGap>,
    },
    GlobalMemberRemoved {
        node: NodeKey,
        reason: MemberRemovalReason,
    },
}

pub struct PlacementControlEvent {
    pub kind: PlacementControlEventKind,
    pub(crate) completion: oneshot::Sender<Result<(), ControlDispatchError>>,
}

impl PlacementControlEvent {
    pub fn complete(self, result: Result<(), ControlDispatchError>) {
        let _ = self.completion.send(result);
    }
}

pub struct PlacementControlRouter {
    sender: mpsc::Sender<PlacementControlEvent>,
    maximum_payload: usize,
    application_timeout: Duration,
}

impl PlacementControlRouter {
    pub fn bounded(
        capacity: usize,
        maximum_payload: usize,
    ) -> Result<(Self, mpsc::Receiver<PlacementControlEvent>), PlacementControlError> {
        Self::bounded_with_timeout(capacity, maximum_payload, Duration::from_secs(5))
    }

    pub fn bounded_with_timeout(
        capacity: usize,
        maximum_payload: usize,
        application_timeout: Duration,
    ) -> Result<(Self, mpsc::Receiver<PlacementControlEvent>), PlacementControlError> {
        if capacity == 0 || maximum_payload == 0 || application_timeout.is_zero() {
            return Err(PlacementControlError::InvalidLimit);
        }
        let (sender, receiver) = mpsc::channel(capacity);
        Ok((
            Self {
                sender,
                maximum_payload,
                application_timeout,
            },
            receiver,
        ))
    }
}

#[async_trait::async_trait]
impl ControlDispatch for PlacementControlRouter {
    async fn apply(
        &self,
        association: AssociationKey,
        stream_id: ControlStreamId,
        command_id: CommandId,
        payload: Bytes,
    ) -> Result<(), ControlDispatchError> {
        let scoped = decode_control_command(&payload, self.maximum_payload)
            .map_err(|_| ControlDispatchError::InvalidCommand)?;
        if stream_id != control_stream_id(&scoped.scope) {
            return Err(ControlDispatchError::InvalidCommand);
        }
        let (completion, applied) = oneshot::channel();
        self.sender
            .try_send(PlacementControlEvent {
                kind: PlacementControlEventKind::Command(Box::new(InboundPlacementControl {
                    association,
                    command_id,
                    scope: scoped.scope,
                    coordinator_term: scoped.coordinator_term,
                    command: scoped.command,
                })),
                completion,
            })
            .map_err(map_try_send_error)?;
        tokio::time::timeout(self.application_timeout, applied)
            .await
            .map_err(|_| application_timeout())?
            .map_err(|_| completion_closed())?
    }

    async fn apply_ephemeral(
        &self,
        association: AssociationKey,
        command_id: CommandId,
        payload: Bytes,
    ) -> Result<(), ControlDispatchError> {
        let scoped = decode_control_command(&payload, self.maximum_payload)
            .map_err(|_| ControlDispatchError::InvalidCommand)?;
        let (completion, _applied) = oneshot::channel();
        self.sender
            .try_send(PlacementControlEvent {
                kind: PlacementControlEventKind::Command(Box::new(InboundPlacementControl {
                    association,
                    command_id,
                    scope: scoped.scope,
                    coordinator_term: scoped.coordinator_term,
                    command: scoped.command,
                })),
                completion,
            })
            .map_err(map_try_send_error)
    }

    async fn reconcile(
        &self,
        association: AssociationKey,
        gap: Option<ControlGap>,
    ) -> Result<(), ControlDispatchError> {
        let (completion, reconciled) = oneshot::channel();
        self.sender
            .try_send(PlacementControlEvent {
                kind: PlacementControlEventKind::Reconcile { association, gap },
                completion,
            })
            .map_err(map_try_send_error)?;
        tokio::time::timeout(self.application_timeout, reconciled)
            .await
            .map_err(|_| application_timeout())?
            .map_err(|_| completion_closed())?
    }
}

/// Bounded receiver directory used by logic nodes with independent domain sessions.
pub struct PlacementControlDirectory {
    senders: RwLock<BTreeMap<CoordinatorScope, mpsc::Sender<PlacementControlEvent>>>,
    capacity_per_scope: usize,
    maximum_scopes: usize,
    maximum_payload: usize,
    application_timeout: Duration,
}

impl PlacementControlDirectory {
    pub fn new(
        capacity_per_scope: usize,
        maximum_scopes: usize,
        maximum_payload: usize,
    ) -> Result<Self, PlacementControlError> {
        if capacity_per_scope == 0 || maximum_scopes == 0 || maximum_payload == 0 {
            return Err(PlacementControlError::InvalidLimit);
        }
        Ok(Self {
            senders: RwLock::new(BTreeMap::new()),
            capacity_per_scope,
            maximum_scopes,
            maximum_payload,
            application_timeout: Duration::from_secs(5),
        })
    }

    pub fn register(
        &self,
        scope: CoordinatorScope,
    ) -> Result<mpsc::Receiver<PlacementControlEvent>, PlacementControlError> {
        let mut senders = self.senders.write().expect("control directory poisoned");
        if senders.contains_key(&scope) || senders.len() == self.maximum_scopes {
            return Err(PlacementControlError::InvalidLimit);
        }
        let (sender, receiver) = mpsc::channel(self.capacity_per_scope);
        senders.insert(scope, sender);
        Ok(receiver)
    }
}

#[async_trait::async_trait]
impl ControlDispatch for PlacementControlDirectory {
    async fn apply(
        &self,
        association: AssociationKey,
        stream_id: ControlStreamId,
        command_id: CommandId,
        payload: Bytes,
    ) -> Result<(), ControlDispatchError> {
        let scoped = decode_control_command(&payload, self.maximum_payload)
            .map_err(|_| ControlDispatchError::InvalidCommand)?;
        if stream_id != control_stream_id(&scoped.scope) {
            return Err(ControlDispatchError::InvalidCommand);
        }
        let sender = self
            .senders
            .read()
            .expect("control directory poisoned")
            .get(&scoped.scope)
            .cloned()
            .ok_or(ControlDispatchError::Rejected(
                ControlRejectReason::UnknownScope,
            ))?;
        let (completion, applied) = oneshot::channel();
        sender
            .try_send(PlacementControlEvent {
                kind: PlacementControlEventKind::Command(Box::new(InboundPlacementControl {
                    association,
                    command_id,
                    scope: scoped.scope,
                    coordinator_term: scoped.coordinator_term,
                    command: scoped.command,
                })),
                completion,
            })
            .map_err(map_try_send_error)?;
        tokio::time::timeout(self.application_timeout, applied)
            .await
            .map_err(|_| application_timeout())?
            .map_err(|_| completion_closed())?
    }

    async fn apply_ephemeral(
        &self,
        association: AssociationKey,
        command_id: CommandId,
        payload: Bytes,
    ) -> Result<(), ControlDispatchError> {
        let scoped = decode_control_command(&payload, self.maximum_payload)
            .map_err(|_| ControlDispatchError::InvalidCommand)?;
        let sender = self
            .senders
            .read()
            .expect("control directory poisoned")
            .get(&scoped.scope)
            .cloned()
            .ok_or(ControlDispatchError::Rejected(
                ControlRejectReason::UnknownScope,
            ))?;
        let (completion, _applied) = oneshot::channel();
        sender
            .try_send(PlacementControlEvent {
                kind: PlacementControlEventKind::Command(Box::new(InboundPlacementControl {
                    association,
                    command_id,
                    scope: scoped.scope,
                    coordinator_term: scoped.coordinator_term,
                    command: scoped.command,
                })),
                completion,
            })
            .map_err(map_try_send_error)
    }

    async fn reconcile(
        &self,
        association: AssociationKey,
        gap: Option<ControlGap>,
    ) -> Result<(), ControlDispatchError> {
        let senders = self
            .senders
            .read()
            .expect("control directory poisoned")
            .iter()
            .filter(|(scope, _)| gap.is_none_or(|gap| control_stream_id(scope) == gap.stream_id))
            .map(|(_, sender)| sender.clone())
            .collect::<Vec<_>>();
        if senders.is_empty() {
            return if gap.is_some() {
                Err(ControlDispatchError::InvalidCommand)
            } else {
                Err(ControlDispatchError::Unsupported)
            };
        }
        let mut completions = Vec::with_capacity(senders.len());
        for sender in senders {
            let (completion, reconciled) = oneshot::channel();
            sender
                .try_send(PlacementControlEvent {
                    kind: PlacementControlEventKind::Reconcile {
                        association: association.clone(),
                        gap,
                    },
                    completion,
                })
                .map_err(map_try_send_error)?;
            completions.push(reconciled);
        }
        for completion in completions {
            tokio::time::timeout(self.application_timeout, completion)
                .await
                .map_err(|_| application_timeout())?
                .map_err(|_| completion_closed())??;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PlacementControlError {
    #[error("placement control limits must be nonzero")]
    InvalidLimit,
    #[error("placement control payload exceeds its bound")]
    PayloadTooLarge,
    #[error("placement control payload is malformed")]
    Codec,
    #[error("placement control schema generation differs")]
    GenerationMismatch,
    #[error("placement control Coordinator term must be nonzero")]
    InvalidCoordinatorTerm,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use lattice_core::actor_ref::{NodeAddress, ProtocolId};

    use super::*;
    use crate::region::EntityConfig;

    #[test]
    fn control_generation_round_trips_and_rejects_oversize() {
        let command = PlacementControlCommand::BeginDrain {
            operation_id: "drain-1".to_string(),
            expected_incarnation: NodeIncarnation::new(1).unwrap(),
        };
        let scope = CoordinatorScope::Placement(PlacementDomainId::new("control-test").unwrap());
        let payload = encode_control_command(&scope, &command, 1024).unwrap();
        assert_eq!(
            decode_control_command(&payload, 1024).unwrap(),
            ScopedPlacementControlCommand {
                scope,
                coordinator_term: None,
                command
            }
        );
        assert_eq!(
            decode_control_command(&payload, 1).unwrap_err(),
            PlacementControlError::PayloadTooLarge
        );
    }

    #[test]
    fn coordinator_term_round_trips_and_rejects_zero() {
        let command = PlacementControlCommand::NodeHeartbeat {
            incarnation: NodeIncarnation::new(1).unwrap(),
            sequence: 7,
        };
        let scope = CoordinatorScope::Membership;
        let payload = encode_control_command_for_term(&scope, 29, &command, 1024).unwrap();
        assert_eq!(
            decode_control_command(&payload, 1024).unwrap(),
            ScopedPlacementControlCommand {
                scope: scope.clone(),
                coordinator_term: Some(29),
                command,
            }
        );
        assert_eq!(
            encode_control_command_for_term(
                &scope,
                0,
                &PlacementControlCommand::NodeHeartbeat {
                    incarnation: NodeIncarnation::new(1).unwrap(),
                    sequence: 8,
                },
                1024,
            )
            .unwrap_err(),
            PlacementControlError::InvalidCoordinatorTerm
        );
    }

    #[test]
    fn configured_placement_domain_hello_round_trips() {
        let domain = PlacementDomainId::new("player").unwrap();
        let entity_type = EntityType::new("player").unwrap();
        let entity_config = EntityConfig::new(
            domain.clone(),
            entity_type.clone(),
            ProtocolId::new(0x1001).unwrap(),
            1024,
            "weighted-v1",
            1,
            Vec::new(),
        )
        .unwrap();
        let hello = PlacementDomainHello::builder(
            NodeKey {
                node_id: "player-1".to_owned(),
                address: NodeAddress::new("127.0.0.1", 25520).unwrap(),
                incarnation: NodeIncarnation::new(7).unwrap(),
            },
            domain.clone(),
            8,
        )
        .hosted_entity_types(BTreeSet::from([entity_type]))
        .entity_configs(vec![entity_config])
        .build();
        let command = PlacementControlCommand::PlacementDomainHello(hello);
        let scope = CoordinatorScope::Placement(domain);

        let payload = encode_control_command_for_term(&scope, 29, &command, 16 * 1024).unwrap();

        assert_eq!(
            decode_control_command(&payload, 16 * 1024).unwrap(),
            ScopedPlacementControlCommand {
                scope,
                coordinator_term: Some(29),
                command,
            }
        );
    }

    #[test]
    fn snapshot_chunks_use_the_dedicated_protobuf_variant() {
        let scope = CoordinatorScope::Placement(PlacementDomainId::new("player").unwrap());
        let command = PlacementControlCommand::SnapshotChunk(SnapshotChunk {
            snapshot_id: 0x0102_0304,
            index: 3,
            records: vec![SnapshotRecord {
                key: "slot/player/3".to_owned(),
                value: Bytes::from(vec![0xff; 1024]),
            }],
        });

        let payload = encode_control_command_for_term(&scope, 29, &command, 2048).unwrap();
        let wire = PlacementControlWire::decode(payload.as_ref()).unwrap();

        assert!(matches!(
            wire.command,
            Some(placement_control_wire::Command::SnapshotChunk(_))
        ));
        assert!(payload.len() < 1200);
        assert_eq!(
            decode_control_command(&payload, 2048).unwrap().command,
            command
        );
    }

    #[test]
    fn control_command_names_do_not_include_payloads() {
        assert_eq!(
            PlacementControlCommand::NodeHeartbeat {
                incarnation: NodeIncarnation::new(1).unwrap(),
                sequence: 7,
            }
            .name(),
            "NodeHeartbeat"
        );
        assert_eq!(
            PlacementControlCommand::MembershipDrainComplete {
                operation_id: "terminal-leave-1".to_string(),
                expected_incarnation: NodeIncarnation::new(1).unwrap(),
            }
            .name(),
            "MembershipDrainComplete"
        );
    }

    #[test]
    fn control_stream_ids_are_stable_and_domain_scoped() {
        let membership = control_stream_id(&CoordinatorScope::Membership);
        let world = control_stream_id(&CoordinatorScope::Placement(
            PlacementDomainId::new("world").unwrap(),
        ));
        let player = control_stream_id(&CoordinatorScope::Placement(
            PlacementDomainId::new("player").unwrap(),
        ));

        assert_eq!(membership, ControlStreamId::new(3).unwrap());
        assert_eq!(
            world,
            control_stream_id(&CoordinatorScope::Placement(
                PlacementDomainId::new("world").unwrap()
            ))
        );
        assert_ne!(world, player);
        assert_ne!(world, membership);
    }
}
