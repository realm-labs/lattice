use std::{collections::BTreeMap, sync::RwLock, time::Duration};

use bytes::Bytes;
use lattice_core::{
    actor_ref::{EntityType, NodeIncarnation, PlacementDomainId, SingletonKind},
    coordinator::CoordinatorScope,
};
use lattice_remoting::{
    association::AssociationKey,
    control::{CommandId, ControlDispatch, ControlDispatchError, ControlGap},
};
use prost::Message;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::{
    coordinator::{
        CoordinatorDelta, MemberEvent, MemberHello, MemberRecord, MemberRemovalReason,
        NodeLoadReport, PlacementDomainHello, ShardLoadReport, SnapshotBegin, SnapshotChunk,
        SnapshotEnd,
    },
    types::{
        AssignmentGeneration, ClaimGrant, MembershipVersion, NodeKey, PlacementSlotKey,
        PlacementVersion, ShardId,
    },
};

pub const PLACEMENT_CONTROL_GENERATION: u64 = 7;
pub const DEFAULT_MAX_CONTROL_PAYLOAD: usize = 256 * 1024;

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
    let payload = serde_json::to_vec(&ScopedPlacementControlCommand {
        scope: scope.clone(),
        coordinator_term,
        command: command.clone(),
    })
    .map_err(|_| PlacementControlError::Codec)?;
    let wire = PlacementControlWire {
        generation: PLACEMENT_CONTROL_GENERATION,
        payload,
    };
    let encoded = wire.encode_to_vec();
    if encoded.len() > maximum_payload {
        return Err(PlacementControlError::PayloadTooLarge);
    }
    Ok(Bytes::from(encoded))
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
    serde_json::from_slice(&wire.payload).map_err(|_| PlacementControlError::Codec)
}

#[derive(Clone, PartialEq, Message)]
struct PlacementControlWire {
    #[prost(uint64, tag = "1")]
    generation: u64,
    #[prost(bytes = "vec", tag = "2")]
    payload: Vec<u8>,
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
        command_id: CommandId,
        payload: Bytes,
    ) -> Result<(), ControlDispatchError> {
        let scoped = decode_control_command(&payload, self.maximum_payload)
            .map_err(|_| ControlDispatchError::InvalidCommand)?;
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
            .map_err(|_| ControlDispatchError::Unavailable)?;
        tokio::time::timeout(self.application_timeout, applied)
            .await
            .map_err(|_| ControlDispatchError::Unavailable)?
            .map_err(|_| ControlDispatchError::Unavailable)?
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
            .map_err(|_| ControlDispatchError::Unavailable)?;
        tokio::time::timeout(self.application_timeout, reconciled)
            .await
            .map_err(|_| ControlDispatchError::Unavailable)?
            .map_err(|_| ControlDispatchError::Unavailable)?
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
            .ok_or(ControlDispatchError::Unavailable)?;
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
            .map_err(|_| ControlDispatchError::Unavailable)?;
        tokio::time::timeout(self.application_timeout, applied)
            .await
            .map_err(|_| ControlDispatchError::Unavailable)?
            .map_err(|_| ControlDispatchError::Unavailable)?
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
            .values()
            .cloned()
            .collect::<Vec<_>>();
        if senders.is_empty() {
            return Err(ControlDispatchError::Unavailable);
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
                .map_err(|_| ControlDispatchError::Unavailable)?;
            completions.push(reconciled);
        }
        for completion in completions {
            tokio::time::timeout(self.application_timeout, completion)
                .await
                .map_err(|_| ControlDispatchError::Unavailable)?
                .map_err(|_| ControlDispatchError::Unavailable)??;
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
    use super::*;

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
}
