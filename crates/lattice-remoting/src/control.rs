use std::collections::{HashSet, VecDeque};

use bytes::Bytes;
use thiserror::Error;

use crate::association::AssociationId;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlApply {
    Apply(ControlEnvelope),
    Duplicate(ControlAck),
    Gap(ControlGap),
    ReconcileEpoch,
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

    pub fn receive(&mut self, envelope: ControlEnvelope) -> ControlApply {
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
        self.next_inbound_sequence = self.next_inbound_sequence.saturating_add(1);
        if self.applied.contains(&envelope.command_id) {
            return ControlApply::Duplicate(self.current_ack());
        }
        self.applied.insert(envelope.command_id);
        self.applied_order.push_back(envelope.command_id);
        while self.applied_order.len() > self.max_frames {
            if let Some(expired) = self.applied_order.pop_front() {
                self.applied.remove(&expired);
            }
        }
        ControlApply::Apply(envelope)
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
