use std::collections::{HashSet, VecDeque};

use async_trait::async_trait;
use bytes::Bytes;
use prost::Message;
use thiserror::Error;

use crate::association::{AssociationId, AssociationKey};
use crate::wire::{Frame, FrameKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CommandId(u128);

impl CommandId {
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().as_u128())
    }

    pub const fn new(value: u128) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    pub const fn get(self) -> u128 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlEnvelope {
    pub association_epoch: AssociationId,
    pub sequence: u64,
    pub command_id: CommandId,
    pub payload: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlAck {
    pub association_epoch: AssociationId,
    pub cumulative_sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlGap {
    pub expected: u64,
    pub received: u64,
}

pub fn control_envelope_frame(envelope: &ControlEnvelope) -> Frame {
    Frame::encode_message(
        FrameKind::ControlEnvelope,
        &ControlEnvelopeWire {
            association_epoch: envelope.association_epoch.get().to_be_bytes().to_vec(),
            sequence: envelope.sequence,
            command_id: envelope.command_id.get().to_be_bytes().to_vec(),
            payload: envelope.payload.to_vec(),
        },
    )
}

pub fn decode_control_envelope(frame: &Frame) -> Result<ControlEnvelope, ReliableControlError> {
    if frame.kind != FrameKind::ControlEnvelope {
        return Err(ReliableControlError::WrongFrameKind);
    }
    let wire = frame
        .decode_message::<ControlEnvelopeWire>()
        .map_err(|_| ReliableControlError::InvalidWire)?;
    Ok(ControlEnvelope {
        association_epoch: AssociationId::new(parse_u128(&wire.association_epoch)?)
            .ok_or(ReliableControlError::InvalidWire)?,
        sequence: (wire.sequence != 0)
            .then_some(wire.sequence)
            .ok_or(ReliableControlError::InvalidWire)?,
        command_id: CommandId::new(parse_u128(&wire.command_id)?)
            .ok_or(ReliableControlError::InvalidWire)?,
        payload: Bytes::from(wire.payload),
    })
}

pub fn control_ack_frame(ack: ControlAck) -> Frame {
    Frame::encode_message(
        FrameKind::ControlAck,
        &ControlAckWire {
            association_epoch: ack.association_epoch.get().to_be_bytes().to_vec(),
            cumulative_sequence: ack.cumulative_sequence,
        },
    )
}

pub fn decode_control_ack(frame: &Frame) -> Result<ControlAck, ReliableControlError> {
    if frame.kind != FrameKind::ControlAck {
        return Err(ReliableControlError::WrongFrameKind);
    }
    let wire = frame
        .decode_message::<ControlAckWire>()
        .map_err(|_| ReliableControlError::InvalidWire)?;
    Ok(ControlAck {
        association_epoch: AssociationId::new(parse_u128(&wire.association_epoch)?)
            .ok_or(ReliableControlError::InvalidWire)?,
        cumulative_sequence: wire.cumulative_sequence,
    })
}

#[derive(Clone, PartialEq, Message)]
struct ControlEnvelopeWire {
    #[prost(bytes = "vec", tag = "1")]
    association_epoch: Vec<u8>,
    #[prost(uint64, tag = "2")]
    sequence: u64,
    #[prost(bytes = "vec", tag = "3")]
    command_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    payload: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct ControlAckWire {
    #[prost(bytes = "vec", tag = "1")]
    association_epoch: Vec<u8>,
    #[prost(uint64, tag = "2")]
    cumulative_sequence: u64,
}

fn parse_u128(bytes: &[u8]) -> Result<u128, ReliableControlError> {
    bytes
        .try_into()
        .map(u128::from_be_bytes)
        .map_err(|_| ReliableControlError::InvalidWire)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlApply {
    Apply(ControlEnvelope),
    Duplicate(ControlAck),
    Gap(ControlGap),
    ReconcileEpoch,
}

#[async_trait]
pub trait ControlDispatch: Send + Sync + 'static {
    async fn apply(
        &self,
        association: AssociationKey,
        command_id: CommandId,
        payload: Bytes,
    ) -> Result<(), ControlDispatchError>;

    async fn reconcile(
        &self,
        association: AssociationKey,
        gap: Option<ControlGap>,
    ) -> Result<(), ControlDispatchError>;
}

#[derive(Debug, Default)]
pub struct RejectControlDispatch;

#[async_trait]
impl ControlDispatch for RejectControlDispatch {
    async fn apply(
        &self,
        _association: AssociationKey,
        _command_id: CommandId,
        _payload: Bytes,
    ) -> Result<(), ControlDispatchError> {
        Err(ControlDispatchError::Unsupported)
    }

    async fn reconcile(
        &self,
        _association: AssociationKey,
        _gap: Option<ControlGap>,
    ) -> Result<(), ControlDispatchError> {
        Err(ControlDispatchError::Unsupported)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ControlDispatchError {
    #[error("this endpoint has no consumer for reliable control commands")]
    Unsupported,
    #[error("reliable control command is invalid")]
    InvalidCommand,
    #[error("reliable control consumer is unavailable")]
    Unavailable,
}

#[derive(Debug)]
pub struct ReliableControl {
    epoch: AssociationId,
    next_outbound_sequence: u64,
    next_inbound_sequence: u64,
    outbox: VecDeque<ControlEnvelope>,
    outbox_bytes: usize,
    applied_order: VecDeque<CommandId>,
    applied: HashSet<CommandId>,
    max_frames: usize,
    max_bytes: usize,
}

impl ReliableControl {
    pub fn new(
        epoch: AssociationId,
        max_frames: usize,
        max_bytes: usize,
    ) -> Result<Self, ReliableControlError> {
        if max_frames == 0 || max_bytes == 0 {
            return Err(ReliableControlError::ZeroLimit);
        }
        Ok(Self {
            epoch,
            next_outbound_sequence: 1,
            next_inbound_sequence: 1,
            outbox: VecDeque::new(),
            outbox_bytes: 0,
            applied_order: VecDeque::new(),
            applied: HashSet::new(),
            max_frames,
            max_bytes,
        })
    }

    pub fn enqueue(
        &mut self,
        command_id: CommandId,
        payload: Bytes,
    ) -> Result<ControlEnvelope, ReliableControlError> {
        if self.outbox.len() == self.max_frames
            || self.outbox_bytes.saturating_add(payload.len()) > self.max_bytes
        {
            return Err(ReliableControlError::OutboxFull);
        }
        let sequence = self.next_outbound_sequence;
        self.next_outbound_sequence = self
            .next_outbound_sequence
            .checked_add(1)
            .ok_or(ReliableControlError::SequenceExhausted)?;
        let envelope = ControlEnvelope {
            association_epoch: self.epoch,
            sequence,
            command_id,
            payload,
        };
        self.outbox_bytes = self.outbox_bytes.saturating_add(envelope.payload.len());
        self.outbox.push_back(envelope.clone());
        Ok(envelope)
    }

    pub fn acknowledge(&mut self, ack: ControlAck) -> Result<(), ReliableControlError> {
        if ack.association_epoch != self.epoch {
            return Err(ReliableControlError::WrongEpoch);
        }
        while self
            .outbox
            .front()
            .is_some_and(|item| item.sequence <= ack.cumulative_sequence)
        {
            if let Some(item) = self.outbox.pop_front() {
                self.outbox_bytes = self.outbox_bytes.saturating_sub(item.payload.len());
            }
        }
        Ok(())
    }

    pub fn rollback_last(&mut self, command_id: CommandId) -> bool {
        let Some(last) = self.outbox.back() else {
            return false;
        };
        if last.command_id != command_id
            || last.sequence.saturating_add(1) != self.next_outbound_sequence
        {
            return false;
        }
        if let Some(last) = self.outbox.pop_back() {
            self.outbox_bytes = self.outbox_bytes.saturating_sub(last.payload.len());
            self.next_outbound_sequence = last.sequence;
            true
        } else {
            false
        }
    }

    pub fn receive(&mut self, envelope: ControlEnvelope) -> ControlApply {
        let is_next_sequence = envelope.sequence == self.next_inbound_sequence;
        let decision = self.preview(&envelope);
        if matches!(decision, ControlApply::Apply(_))
            || matches!(
                decision,
                ControlApply::Duplicate(_) if is_next_sequence
            )
        {
            self.commit(envelope);
        }
        decision
    }

    pub fn preview(&self, envelope: &ControlEnvelope) -> ControlApply {
        if envelope.association_epoch != self.epoch {
            return ControlApply::ReconcileEpoch;
        }
        if envelope.sequence < self.next_inbound_sequence {
            return ControlApply::Duplicate(self.current_ack());
        }
        if envelope.sequence > self.next_inbound_sequence {
            return ControlApply::Gap(ControlGap {
                expected: self.next_inbound_sequence,
                received: envelope.sequence,
            });
        }
        if self.applied.contains(&envelope.command_id) {
            return ControlApply::Duplicate(ControlAck {
                association_epoch: self.epoch,
                cumulative_sequence: envelope.sequence,
            });
        }
        ControlApply::Apply(envelope.clone())
    }

    pub fn commit(&mut self, envelope: ControlEnvelope) -> ControlAck {
        debug_assert_eq!(envelope.association_epoch, self.epoch);
        debug_assert_eq!(envelope.sequence, self.next_inbound_sequence);
        self.next_inbound_sequence = self.next_inbound_sequence.saturating_add(1);
        self.applied.insert(envelope.command_id);
        self.applied_order.push_back(envelope.command_id);
        while self.applied_order.len() > self.max_frames {
            if let Some(expired) = self.applied_order.pop_front() {
                self.applied.remove(&expired);
            }
        }
        self.current_ack()
    }

    pub fn current_ack(&self) -> ControlAck {
        ControlAck {
            association_epoch: self.epoch,
            cumulative_sequence: self.next_inbound_sequence.saturating_sub(1),
        }
    }

    pub fn replay(&self) -> impl ExactSizeIterator<Item = &ControlEnvelope> {
        self.outbox.iter()
    }

    pub fn reset_epoch(&mut self, epoch: AssociationId) {
        self.epoch = epoch;
        self.next_outbound_sequence = 1;
        self.next_inbound_sequence = 1;
        self.outbox.clear();
        self.outbox_bytes = 0;
        self.applied.clear();
        self.applied_order.clear();
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ReliableControlError {
    #[error("reliable control limits must be nonzero")]
    ZeroLimit,
    #[error("reliable control outbox is full")]
    OutboxFull,
    #[error("reliable control sequence is exhausted")]
    SequenceExhausted,
    #[error("control acknowledgement belongs to another association epoch")]
    WrongEpoch,
    #[error("reliable control used the wrong frame kind")]
    WrongFrameKind,
    #[error("reliable control frame is invalid")]
    InvalidWire,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_is_same_epoch_only_and_commands_are_deduplicated() {
        let epoch = AssociationId::new(1).unwrap();
        let mut sender = ReliableControl::new(epoch, 4, 1024).unwrap();
        let command = CommandId::new(7).unwrap();
        let envelope = sender
            .enqueue(command, Bytes::from_static(b"state"))
            .unwrap();
        assert_eq!(sender.replay().len(), 1);

        let mut receiver = ReliableControl::new(epoch, 4, 1024).unwrap();
        assert!(matches!(
            receiver.receive(envelope.clone()),
            ControlApply::Apply(_)
        ));
        assert!(matches!(
            receiver.receive(envelope),
            ControlApply::Duplicate(_)
        ));

        sender.reset_epoch(AssociationId::new(2).unwrap());
        assert_eq!(sender.replay().len(), 0);
    }

    #[test]
    fn a_gap_requests_reconciliation_without_advancing() {
        let epoch = AssociationId::new(1).unwrap();
        let mut receiver = ReliableControl::new(epoch, 4, 1024).unwrap();
        let result = receiver.receive(ControlEnvelope {
            association_epoch: epoch,
            sequence: 2,
            command_id: CommandId::new(9).unwrap(),
            payload: Bytes::new(),
        });
        assert_eq!(
            result,
            ControlApply::Gap(ControlGap {
                expected: 1,
                received: 2
            })
        );
    }
}
