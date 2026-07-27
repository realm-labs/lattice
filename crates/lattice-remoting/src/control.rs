use std::collections::{BTreeMap, HashSet, VecDeque};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ControlStreamId(u128);

impl ControlStreamId {
    pub const DEFAULT: Self = Self(1);
    pub const WATCH: Self = Self(2);

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
    pub stream_id: ControlStreamId,
    pub sequence: u64,
    pub command_id: CommandId,
    pub payload: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlAck {
    pub association_epoch: AssociationId,
    pub stream_id: ControlStreamId,
    pub cumulative_sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlGap {
    pub stream_id: ControlStreamId,
    pub expected: u64,
    pub received: u64,
}

pub fn control_envelope_frame(envelope: &ControlEnvelope) -> Frame {
    Frame::encode_message(
        FrameKind::ControlEnvelope,
        &ControlEnvelopeWire {
            association_epoch: envelope.association_epoch.get().to_be_bytes().to_vec(),
            stream_id: envelope.stream_id.get().to_be_bytes().to_vec(),
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
        stream_id: parse_stream_id(&wire.stream_id)?,
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
            stream_id: ack.stream_id.get().to_be_bytes().to_vec(),
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
        stream_id: parse_stream_id(&wire.stream_id)?,
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
    #[prost(bytes = "vec", tag = "5")]
    stream_id: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct ControlAckWire {
    #[prost(bytes = "vec", tag = "1")]
    association_epoch: Vec<u8>,
    #[prost(uint64, tag = "2")]
    cumulative_sequence: u64,
    #[prost(bytes = "vec", tag = "3")]
    stream_id: Vec<u8>,
}

fn parse_u128(bytes: &[u8]) -> Result<u128, ReliableControlError> {
    bytes
        .try_into()
        .map(u128::from_be_bytes)
        .map_err(|_| ReliableControlError::InvalidWire)
}

fn parse_stream_id(bytes: &[u8]) -> Result<ControlStreamId, ReliableControlError> {
    ControlStreamId::new(parse_u128(bytes)?).ok_or(ReliableControlError::InvalidWire)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlApply {
    Apply(ControlEnvelope),
    Duplicate(ControlAck),
    Gap(ControlGap),
    ReconcileEpoch,
    StreamLimit,
}

#[async_trait]
pub trait ControlDispatch: Send + Sync + 'static {
    async fn apply(
        &self,
        association: AssociationKey,
        stream_id: ControlStreamId,
        command_id: CommandId,
        payload: Bytes,
    ) -> Result<(), ControlDispatchError>;

    /// Offers a best-effort command without waiting for the application to finish it.
    ///
    /// Implementations that can enqueue independently should override this method so ephemeral
    /// traffic cannot head-of-line block reliable control recovery. The default preserves the
    /// original behavior for dispatchers that do not distinguish delivery classes.
    async fn apply_ephemeral(
        &self,
        association: AssociationKey,
        command_id: CommandId,
        payload: Bytes,
    ) -> Result<(), ControlDispatchError> {
        self.apply(association, ControlStreamId::DEFAULT, command_id, payload)
            .await
    }

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
        _stream_id: ControlStreamId,
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
    #[error("reliable control command was rejected: {0}")]
    Rejected(ControlRejectReason),
    #[error("reliable control command should be retried later: {0}")]
    RetryLater(ControlRetryReason),
    #[error("reliable control scope must be reconciled: {0}")]
    ResetRequired(ControlResetReason),
    #[error("reliable control consumer failed: {0}")]
    Fatal(ControlFatalReason),
}

impl ControlDispatchError {
    pub const fn retry_later(reason: ControlRetryReason) -> Self {
        Self::RetryLater(reason)
    }

    pub const fn consumer_closed() -> Self {
        Self::Fatal(ControlFatalReason::ConsumerClosed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ControlRejectReason {
    #[error("consumer capacity is exhausted")]
    Capacity,
    #[error("the target scope is not registered")]
    UnknownScope,
    #[error("the command belongs to a stale generation")]
    StaleGeneration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ControlRetryReason {
    #[error("consumer mailbox is full")]
    ConsumerBusy,
    #[error("consumer did not complete before its application deadline")]
    ApplicationTimeout,
    #[error("association is not ready")]
    AssociationStarting,
    #[error("reliable control outbox is full")]
    OutboxFull,
    #[error("downstream effect queue is applying backpressure")]
    EffectBackpressure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ControlResetReason {
    #[error("association epoch changed")]
    AssociationChanged,
    #[error("scope generation changed")]
    ScopeChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ControlFatalReason {
    #[error("consumer mailbox is closed")]
    ConsumerClosed,
    #[error("control task supervisor is unavailable")]
    SupervisorUnavailable,
    #[error("control retry deadline was exhausted")]
    RetryDeadlineExceeded,
}

#[derive(Debug)]
pub struct ReliableControl {
    epoch: AssociationId,
    next_outbound_sequence: BTreeMap<ControlStreamId, u64>,
    next_inbound_sequence: BTreeMap<ControlStreamId, u64>,
    outbox: VecDeque<ControlEnvelope>,
    outbox_bytes: usize,
    applied_order: VecDeque<CommandId>,
    applied: HashSet<CommandId>,
    max_frames: usize,
    max_bytes: usize,
    max_frames_per_stream: usize,
    max_bytes_per_stream: usize,
    max_streams: usize,
}

impl ReliableControl {
    pub fn new(
        epoch: AssociationId,
        max_frames: usize,
        max_bytes: usize,
    ) -> Result<Self, ReliableControlError> {
        Self::new_with_stream_limits(epoch, max_frames, max_bytes, max_frames, max_bytes)
    }

    pub fn new_with_stream_limits(
        epoch: AssociationId,
        max_frames: usize,
        max_bytes: usize,
        max_frames_per_stream: usize,
        max_bytes_per_stream: usize,
    ) -> Result<Self, ReliableControlError> {
        Self::new_with_limits(
            epoch,
            max_frames,
            max_bytes,
            max_frames_per_stream,
            max_bytes_per_stream,
            max_frames,
        )
    }

    pub fn new_with_limits(
        epoch: AssociationId,
        max_frames: usize,
        max_bytes: usize,
        max_frames_per_stream: usize,
        max_bytes_per_stream: usize,
        max_streams: usize,
    ) -> Result<Self, ReliableControlError> {
        if max_frames == 0
            || max_bytes == 0
            || max_frames_per_stream == 0
            || max_bytes_per_stream == 0
            || max_streams == 0
            || max_frames_per_stream > max_frames
            || max_bytes_per_stream > max_bytes
        {
            return Err(ReliableControlError::ZeroLimit);
        }
        Ok(Self {
            epoch,
            next_outbound_sequence: BTreeMap::new(),
            next_inbound_sequence: BTreeMap::new(),
            outbox: VecDeque::new(),
            outbox_bytes: 0,
            applied_order: VecDeque::new(),
            applied: HashSet::new(),
            max_frames,
            max_bytes,
            max_frames_per_stream,
            max_bytes_per_stream,
            max_streams,
        })
    }

    pub fn enqueue(
        &mut self,
        command_id: CommandId,
        payload: Bytes,
    ) -> Result<ControlEnvelope, ReliableControlError> {
        self.enqueue_in(ControlStreamId::DEFAULT, command_id, payload)
    }

    pub fn enqueue_in(
        &mut self,
        stream_id: ControlStreamId,
        command_id: CommandId,
        payload: Bytes,
    ) -> Result<ControlEnvelope, ReliableControlError> {
        if self.outbox.len() == self.max_frames
            || self.outbox_bytes.saturating_add(payload.len()) > self.max_bytes
        {
            return Err(ReliableControlError::OutboxFull);
        }
        if !self.next_outbound_sequence.contains_key(&stream_id)
            && self.next_outbound_sequence.len() == self.max_streams
        {
            return Err(ReliableControlError::StreamLimit);
        }
        let mut stream_frames = 0_usize;
        let mut stream_bytes = 0_usize;
        for item in &self.outbox {
            if item.stream_id == stream_id {
                stream_frames = stream_frames.saturating_add(1);
                stream_bytes = stream_bytes.saturating_add(item.payload.len());
            }
        }
        if stream_frames == self.max_frames_per_stream
            || stream_bytes.saturating_add(payload.len()) > self.max_bytes_per_stream
        {
            return Err(ReliableControlError::OutboxFull);
        }
        let next_sequence = self.next_outbound_sequence.entry(stream_id).or_insert(1);
        let sequence = *next_sequence;
        *next_sequence = next_sequence
            .checked_add(1)
            .ok_or(ReliableControlError::SequenceExhausted)?;
        let envelope = ControlEnvelope {
            association_epoch: self.epoch,
            stream_id,
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
        self.outbox.retain(|item| {
            if item.stream_id == ack.stream_id && item.sequence <= ack.cumulative_sequence {
                self.outbox_bytes = self.outbox_bytes.saturating_sub(item.payload.len());
                false
            } else {
                true
            }
        });
        Ok(())
    }

    pub fn rollback_last(&mut self, command_id: CommandId) -> bool {
        let Some(last) = self.outbox.back() else {
            return false;
        };
        if last.command_id != command_id
            || last.sequence.saturating_add(1)
                != self
                    .next_outbound_sequence
                    .get(&last.stream_id)
                    .copied()
                    .unwrap_or(1)
        {
            return false;
        }
        if let Some(last) = self.outbox.pop_back() {
            self.outbox_bytes = self.outbox_bytes.saturating_sub(last.payload.len());
            self.next_outbound_sequence
                .insert(last.stream_id, last.sequence);
            true
        } else {
            false
        }
    }

    pub fn receive(&mut self, envelope: ControlEnvelope) -> ControlApply {
        let is_next_sequence = envelope.sequence
            == self
                .next_inbound_sequence
                .get(&envelope.stream_id)
                .copied()
                .unwrap_or(1);
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
        if !self.next_inbound_sequence.contains_key(&envelope.stream_id)
            && self.next_inbound_sequence.len() == self.max_streams
        {
            return ControlApply::StreamLimit;
        }
        let expected = self
            .next_inbound_sequence
            .get(&envelope.stream_id)
            .copied()
            .unwrap_or(1);
        if envelope.sequence < expected {
            return ControlApply::Duplicate(self.current_ack(envelope.stream_id));
        }
        if envelope.sequence > expected {
            return ControlApply::Gap(ControlGap {
                stream_id: envelope.stream_id,
                expected,
                received: envelope.sequence,
            });
        }
        if self.applied.contains(&envelope.command_id) {
            return ControlApply::Duplicate(ControlAck {
                association_epoch: self.epoch,
                stream_id: envelope.stream_id,
                cumulative_sequence: envelope.sequence,
            });
        }
        ControlApply::Apply(envelope.clone())
    }

    pub fn commit(&mut self, envelope: ControlEnvelope) -> ControlAck {
        debug_assert_eq!(envelope.association_epoch, self.epoch);
        let next_sequence = self
            .next_inbound_sequence
            .entry(envelope.stream_id)
            .or_insert(1);
        debug_assert_eq!(envelope.sequence, *next_sequence);
        *next_sequence = next_sequence.saturating_add(1);
        let stream_id = envelope.stream_id;
        self.applied.insert(envelope.command_id);
        self.applied_order.push_back(envelope.command_id);
        while self.applied_order.len() > self.max_frames {
            if let Some(expired) = self.applied_order.pop_front() {
                self.applied.remove(&expired);
            }
        }
        self.current_ack(stream_id)
    }

    pub fn current_ack(&self, stream_id: ControlStreamId) -> ControlAck {
        ControlAck {
            association_epoch: self.epoch,
            stream_id,
            cumulative_sequence: self
                .next_inbound_sequence
                .get(&stream_id)
                .copied()
                .unwrap_or(1)
                .saturating_sub(1),
        }
    }

    pub fn replay(&self) -> impl ExactSizeIterator<Item = &ControlEnvelope> {
        self.outbox.iter()
    }

    pub fn contains_outbound(&self, command_id: CommandId) -> bool {
        self.outbox
            .iter()
            .any(|envelope| envelope.command_id == command_id)
    }

    pub fn reset_epoch(&mut self, epoch: AssociationId) {
        self.epoch = epoch;
        self.next_outbound_sequence.clear();
        self.next_inbound_sequence.clear();
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
    #[error("reliable control stream registry is full")]
    StreamLimit,
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
            stream_id: ControlStreamId::DEFAULT,
            sequence: 2,
            command_id: CommandId::new(9).unwrap(),
            payload: Bytes::new(),
        });
        assert_eq!(
            result,
            ControlApply::Gap(ControlGap {
                stream_id: ControlStreamId::DEFAULT,
                expected: 1,
                received: 2
            })
        );
    }

    #[test]
    fn streams_advance_and_acknowledge_independently_inside_one_global_budget() {
        let epoch = AssociationId::new(1).unwrap();
        let first = ControlStreamId::new(10).unwrap();
        let second = ControlStreamId::new(11).unwrap();
        let mut sender = ReliableControl::new(epoch, 4, 1024).unwrap();
        let first_envelope = sender
            .enqueue_in(
                first,
                CommandId::new(1).unwrap(),
                Bytes::from_static(b"first"),
            )
            .unwrap();
        let second_envelope = sender
            .enqueue_in(
                second,
                CommandId::new(2).unwrap(),
                Bytes::from_static(b"second"),
            )
            .unwrap();
        assert_eq!(first_envelope.sequence, 1);
        assert_eq!(second_envelope.sequence, 1);

        sender
            .acknowledge(ControlAck {
                association_epoch: epoch,
                stream_id: second,
                cumulative_sequence: 1,
            })
            .unwrap();
        assert_eq!(sender.replay().len(), 1);
        assert_eq!(sender.replay().next().unwrap().stream_id, first);

        let mut receiver = ReliableControl::new(epoch, 4, 1024).unwrap();
        assert!(matches!(
            receiver.preview(&second_envelope),
            ControlApply::Apply(_)
        ));
        receiver.commit(second_envelope);
        assert!(matches!(
            receiver.preview(&first_envelope),
            ControlApply::Apply(_)
        ));
    }

    #[test]
    fn one_stream_cannot_consume_the_entire_association_outbox() {
        let epoch = AssociationId::new(1).unwrap();
        let noisy = ControlStreamId::new(10).unwrap();
        let healthy = ControlStreamId::new(11).unwrap();
        let mut sender = ReliableControl::new_with_stream_limits(epoch, 4, 1024, 2, 512).unwrap();
        sender
            .enqueue_in(noisy, CommandId::new(1).unwrap(), Bytes::from_static(b"a"))
            .unwrap();
        sender
            .enqueue_in(noisy, CommandId::new(2).unwrap(), Bytes::from_static(b"b"))
            .unwrap();
        assert_eq!(
            sender
                .enqueue_in(noisy, CommandId::new(3).unwrap(), Bytes::from_static(b"c"))
                .unwrap_err(),
            ReliableControlError::OutboxFull
        );
        sender
            .enqueue_in(
                healthy,
                CommandId::new(4).unwrap(),
                Bytes::from_static(b"d"),
            )
            .unwrap();
        assert_eq!(sender.replay().len(), 3);
    }

    #[test]
    fn stream_registries_are_bounded_on_both_sides() {
        let epoch = AssociationId::new(1).unwrap();
        let first = ControlStreamId::new(10).unwrap();
        let second = ControlStreamId::new(11).unwrap();
        let mut sender = ReliableControl::new_with_limits(epoch, 4, 1024, 4, 1024, 1).unwrap();
        sender
            .enqueue_in(first, CommandId::new(1).unwrap(), Bytes::new())
            .unwrap();
        assert_eq!(
            sender
                .enqueue_in(second, CommandId::new(2).unwrap(), Bytes::new())
                .unwrap_err(),
            ReliableControlError::StreamLimit
        );

        let first_envelope = ControlEnvelope {
            association_epoch: epoch,
            stream_id: first,
            sequence: 1,
            command_id: CommandId::new(3).unwrap(),
            payload: Bytes::new(),
        };
        let second_envelope = ControlEnvelope {
            association_epoch: epoch,
            stream_id: second,
            sequence: 1,
            command_id: CommandId::new(4).unwrap(),
            payload: Bytes::new(),
        };
        let mut receiver = ReliableControl::new_with_limits(epoch, 4, 1024, 4, 1024, 1).unwrap();
        assert!(matches!(
            receiver.receive(first_envelope),
            ControlApply::Apply(_)
        ));
        assert_eq!(
            receiver.preview(&second_envelope),
            ControlApply::StreamLimit
        );
    }
}
