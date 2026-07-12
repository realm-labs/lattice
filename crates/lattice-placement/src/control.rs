use bytes::Bytes;
use lattice_core::actor_ref::{EntityType, NodeIncarnation, SingletonKind};
use lattice_remoting::{
    AssociationKey, CommandId, ControlDispatch, ControlDispatchError, ControlGap,
};
use prost::Message;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::coordinator::{
    CoordinatorDelta, NodeHello, NodeLoadReport, ShardLoadReport, SnapshotBegin, SnapshotChunk,
    SnapshotEnd,
};
use crate::types::{AssignmentGeneration, ClaimGrant, Revision, ShardId};

pub const PLACEMENT_CONTROL_GENERATION: u64 = 2;
pub const DEFAULT_MAX_CONTROL_PAYLOAD: usize = 256 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementControlCommand {
    NodeHello(NodeHello),
    NodeHeartbeat {
        incarnation: NodeIncarnation,
        sequence: u64,
    },
    NodeRemoved(NodeIncarnation),
    SubscribeEntity(EntityType),
    SubscribeSingleton(SingletonKind),
    SnapshotBegin(SnapshotBegin),
    SnapshotChunk(SnapshotChunk),
    SnapshotEnd(SnapshotEnd),
    StateDelta(CoordinatorDelta),
    AppliedRevision(Revision),
    ClaimGranted(ClaimGrant),
    NodeLoad(NodeLoadReport),
    ShardLoad(ShardLoadReport),
    ResolveShard {
        request_id: u128,
        entity_type: EntityType,
        shard_id: ShardId,
    },
    ResolveSingleton {
        request_id: u128,
        kind: SingletonKind,
    },
    DrainSlot {
        slot: crate::types::PlacementSlotKey,
        generation: AssignmentGeneration,
        revision: Revision,
    },
    SlotDrained {
        slot: crate::types::PlacementSlotKey,
        generation: AssignmentGeneration,
    },
    SlotStopFailed {
        slot: crate::types::PlacementSlotKey,
        generation: AssignmentGeneration,
    },
    SlotReady {
        slot: crate::types::PlacementSlotKey,
        generation: AssignmentGeneration,
    },
    BeginDrain,
    DrainComplete,
}

pub fn encode_control_command(
    command: &PlacementControlCommand,
    maximum_payload: usize,
) -> Result<Bytes, PlacementControlError> {
    if maximum_payload == 0 {
        return Err(PlacementControlError::InvalidLimit);
    }
    let payload = serde_json::to_vec(command).map_err(|_| PlacementControlError::Codec)?;
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
) -> Result<PlacementControlCommand, PlacementControlError> {
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
    pub command: PlacementControlCommand,
}

#[derive(Debug, Clone)]
pub enum PlacementControlEventKind {
    Command(Box<InboundPlacementControl>),
    Reconcile {
        association: AssociationKey,
        gap: Option<ControlGap>,
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
    application_timeout: std::time::Duration,
}

impl PlacementControlRouter {
    pub fn bounded(
        capacity: usize,
        maximum_payload: usize,
    ) -> Result<(Self, mpsc::Receiver<PlacementControlEvent>), PlacementControlError> {
        Self::bounded_with_timeout(capacity, maximum_payload, std::time::Duration::from_secs(5))
    }

    pub fn bounded_with_timeout(
        capacity: usize,
        maximum_payload: usize,
        application_timeout: std::time::Duration,
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
        let command = decode_control_command(&payload, self.maximum_payload)
            .map_err(|_| ControlDispatchError::InvalidCommand)?;
        let (completion, applied) = oneshot::channel();
        self.sender
            .try_send(PlacementControlEvent {
                kind: PlacementControlEventKind::Command(Box::new(InboundPlacementControl {
                    association,
                    command_id,
                    command,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_generation_round_trips_and_rejects_oversize() {
        let command = PlacementControlCommand::BeginDrain;
        let payload = encode_control_command(&command, 1024).unwrap();
        assert_eq!(decode_control_command(&payload, 1024).unwrap(), command);
        assert_eq!(
            decode_control_command(&payload, 1).unwrap_err(),
            PlacementControlError::PayloadTooLarge
        );
    }
}
