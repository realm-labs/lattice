use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
};

use lattice_core::actor_ref::{ClusterId, NodeAddress, NodeIncarnation, ProtocolId};
use thiserror::Error;
use tokio::sync::{Notify, mpsc};

use crate::{
    config::{RemotingConfig, RemotingConfigError},
    control::{
        CommandId, ControlAck, ControlApply, ControlEnvelope, ReliableControl,
        ReliableControlError, control_envelope_frame,
    },
    protocol::{
        CatalogueDecision, CatalogueError, ProtocolCatalogue, ProtocolDescriptor,
        ProtocolFingerprint,
    },
    wire::{Frame, FrameKind},
};

mod manager;
pub mod metrics;
mod wake;

use metrics::{AssociationMetrics, AssociationMetricsSnapshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AssociationId(u128);

impl AssociationId {
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AssociationKey {
    pub cluster_id: ClusterId,
    pub local_incarnation: NodeIncarnation,
    pub remote_address: NodeAddress,
    pub remote_incarnation: NodeIncarnation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LaneKind {
    Control,
    Interactive,
    Bulk(u8),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaneAttachment {
    pub association_id: AssociationId,
    pub key: AssociationKey,
    pub lane: LaneKind,
    pub connection_nonce: u128,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachmentDecision {
    Attached,
    ReplacedDuplicate,
    RejectedDuplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AssociationState {
    Establishing = 0,
    Active = 1,
    Reconnecting = 2,
    Closing = 3,
    Closed = 4,
}

impl AssociationState {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Establishing,
            1 => Self::Active,
            2 => Self::Reconnecting,
            3 => Self::Closing,
            4 => Self::Closed,
            _ => unreachable!("association state is only written from AssociationState"),
        }
    }
}

#[derive(Debug)]
struct AssociationInner {
    lanes: HashMap<LaneKind, u128>,
}

#[derive(Debug)]
pub struct AssociationReceivers {
    pub control: mpsc::Receiver<Frame>,
    pub interactive: mpsc::Receiver<Frame>,
    pub bulk: Vec<mpsc::Receiver<Frame>>,
}

#[derive(Debug)]
pub struct Association {
    id: AssociationId,
    key: AssociationKey,
    config: RemotingConfig,
    state: AtomicU8,
    attached_lanes: AtomicU64,
    wake_pending_lanes: AtomicU64,
    inner: Mutex<AssociationInner>,
    control: mpsc::Sender<Frame>,
    interactive: mpsc::Sender<Frame>,
    bulk: Vec<mpsc::Sender<Frame>>,
    bulk_lane_epochs: Vec<AtomicU64>,
    next_outbound_exact_target_ids: Vec<AtomicU64>,
    receivers: Mutex<AssociationReceiverSlots>,
    queued_bytes: AtomicUsize,
    node_queued_bytes: Arc<AtomicUsize>,
    peer_catalogue: OnceLock<ProtocolCatalogue>,
    reliable_control: Mutex<ReliableControl>,
    interactive_wake: Notify,
    bulk_wakes: Vec<Notify>,
    metrics: AssociationMetrics,
}

pub(crate) struct BulkAdmission<'a> {
    association: &'a Association,
    permit: Option<mpsc::Permit<'a, Frame>>,
    reserved_bytes: usize,
}

impl BulkAdmission<'_> {
    pub(crate) fn send(mut self, frame: Frame) {
        debug_assert_eq!(frame.payload_len(), self.reserved_bytes);
        self.permit
            .take()
            .expect("bulk admission permit is consumed once")
            .send(frame);
        self.reserved_bytes = 0;
    }
}

impl Drop for BulkAdmission<'_> {
    fn drop(&mut self) {
        if self.reserved_bytes != 0 {
            self.association.release_queued_bytes(self.reserved_bytes);
        }
    }
}

impl Association {
    pub fn new(key: AssociationKey, config: RemotingConfig) -> Result<Self, AssociationError> {
        Self::new_with_id(key, AssociationId::generate(), config)
    }

    pub fn new_with_id(
        key: AssociationKey,
        id: AssociationId,
        config: RemotingConfig,
    ) -> Result<Self, AssociationError> {
        Self::new_with_id_and_budget(key, id, config, Arc::new(AtomicUsize::new(0)))
    }

    fn new_with_id_and_budget(
        key: AssociationKey,
        id: AssociationId,
        config: RemotingConfig,
        node_queued_bytes: Arc<AtomicUsize>,
    ) -> Result<Self, AssociationError> {
        config.validate().map_err(AssociationError::InvalidConfig)?;
        let max_control_outbox_frames = config.max_control_outbox_frames;
        let max_control_outbox_bytes = config.max_control_outbox_bytes;
        let bulk_stripes = config.bulk_stripes;
        let (control, control_rx) = mpsc::channel(config.control_queue_frames);
        let (interactive, interactive_rx) = mpsc::channel(config.interactive_queue_frames);
        let mut bulk = Vec::with_capacity(config.bulk_stripes);
        let mut bulk_rx = Vec::with_capacity(config.bulk_stripes);
        for _ in 0..config.bulk_stripes {
            let (sender, receiver) = mpsc::channel(config.bulk_queue_frames_per_stripe);
            bulk.push(sender);
            bulk_rx.push(receiver);
        }
        Ok(Self {
            id,
            key,
            config,
            state: AtomicU8::new(AssociationState::Establishing as u8),
            attached_lanes: AtomicU64::new(0),
            wake_pending_lanes: AtomicU64::new(0),
            inner: Mutex::new(AssociationInner {
                lanes: HashMap::new(),
            }),
            control,
            interactive,
            bulk,
            bulk_lane_epochs: (0..bulk_stripes).map(|_| AtomicU64::new(0)).collect(),
            next_outbound_exact_target_ids: (0..bulk_stripes).map(|_| AtomicU64::new(0)).collect(),
            receivers: Mutex::new(AssociationReceiverSlots {
                control: Some(control_rx),
                interactive: Some(interactive_rx),
                bulk: bulk_rx.into_iter().map(Some).collect(),
            }),
            queued_bytes: AtomicUsize::new(0),
            node_queued_bytes,
            peer_catalogue: OnceLock::new(),
            reliable_control: Mutex::new(
                ReliableControl::new(id, max_control_outbox_frames, max_control_outbox_bytes)
                    .expect("validated reliable control limits"),
            ),
            interactive_wake: Notify::new(),
            bulk_wakes: (0..bulk_stripes).map(|_| Notify::new()).collect(),
            metrics: AssociationMetrics::default(),
        })
    }

    pub fn id(&self) -> AssociationId {
        self.id
    }

    /// Returns cumulative transport counters for this Association generation.
    pub fn metrics(&self) -> AssociationMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn key(&self) -> &AssociationKey {
        &self.key
    }

    pub fn state(&self) -> AssociationState {
        AssociationState::from_u8(self.state.load(Ordering::Acquire))
    }

    pub fn take_receivers(&self) -> Option<AssociationReceivers> {
        let mut slots = self
            .receivers
            .lock()
            .expect("association receivers poisoned");
        if slots.control.is_none()
            || slots.interactive.is_none()
            || slots.bulk.iter().any(Option::is_none)
        {
            return None;
        }
        Some(AssociationReceivers {
            control: slots.control.take().expect("checked control receiver"),
            interactive: slots
                .interactive
                .take()
                .expect("checked interactive receiver"),
            bulk: slots
                .bulk
                .iter_mut()
                .map(|receiver| receiver.take().expect("checked bulk receiver"))
                .collect(),
        })
    }

    pub fn take_lane_receiver(&self, lane: LaneKind) -> Option<mpsc::Receiver<Frame>> {
        let mut slots = self
            .receivers
            .lock()
            .expect("association receivers poisoned");
        match lane {
            LaneKind::Control => slots.control.take(),
            LaneKind::Interactive => slots.interactive.take(),
            LaneKind::Bulk(index) => slots.bulk.get_mut(usize::from(index))?.take(),
        }
    }

    pub(crate) fn lane_receiver_available(&self, lane: LaneKind) -> bool {
        let slots = self
            .receivers
            .lock()
            .expect("association receivers poisoned");
        match lane {
            LaneKind::Control => slots.control.is_some(),
            LaneKind::Interactive => slots.interactive.is_some(),
            LaneKind::Bulk(index) => slots
                .bulk
                .get(usize::from(index))
                .is_some_and(Option::is_some),
        }
    }

    pub fn return_lane_receiver(
        &self,
        lane: LaneKind,
        receiver: mpsc::Receiver<Frame>,
    ) -> Result<(), AssociationError> {
        let mut slots = self
            .receivers
            .lock()
            .expect("association receivers poisoned");
        let slot = match lane {
            LaneKind::Control => &mut slots.control,
            LaneKind::Interactive => &mut slots.interactive,
            LaneKind::Bulk(index) => slots
                .bulk
                .get_mut(usize::from(index))
                .ok_or(AssociationError::InvalidBulkStripe(index))?,
        };
        if slot.is_some() {
            return Err(AssociationError::LaneReceiverConflict);
        }
        *slot = Some(receiver);
        Ok(())
    }

    pub fn attach(
        &self,
        attachment: LaneAttachment,
    ) -> Result<AttachmentDecision, AssociationError> {
        self.attach_with_activation(attachment)
            .map(|(decision, _)| decision)
    }

    pub(crate) fn attach_with_activation(
        &self,
        attachment: LaneAttachment,
    ) -> Result<(AttachmentDecision, bool), AssociationError> {
        if attachment.association_id != self.id || attachment.key != self.key {
            return Err(AssociationError::IdentityMismatch);
        }
        if let LaneKind::Bulk(index) = attachment.lane
            && usize::from(index) >= self.config.bulk_stripes
        {
            return Err(AssociationError::InvalidBulkStripe(index));
        }
        let mut inner = self.inner.lock().expect("association state poisoned");
        if matches!(
            self.state(),
            AssociationState::Closing | AssociationState::Closed
        ) {
            return Err(AssociationError::Closed);
        }
        let decision = match inner.lanes.get_mut(&attachment.lane) {
            None => {
                inner
                    .lanes
                    .insert(attachment.lane, attachment.connection_nonce);
                AttachmentDecision::Attached
            }
            Some(current) if attachment.connection_nonce < *current => {
                *current = attachment.connection_nonce;
                AttachmentDecision::ReplacedDuplicate
            }
            Some(_) => AttachmentDecision::RejectedDuplicate,
        };
        if decision != AttachmentDecision::RejectedDuplicate
            && let LaneKind::Bulk(index) = attachment.lane
        {
            self.bulk_lane_epochs[usize::from(index)].fetch_add(1, Ordering::AcqRel);
        }
        let lane_mask = lane_mask(attachment.lane);
        self.attached_lanes.fetch_or(lane_mask, Ordering::Release);
        self.wake_pending_lanes
            .fetch_and(!lane_mask, Ordering::AcqRel);
        let activated =
            self.state() != AssociationState::Active && self.has_complete_lane_group(&inner.lanes);
        if activated {
            self.state
                .store(AssociationState::Active as u8, Ordering::Release);
        }
        Ok((decision, activated))
    }

    pub(crate) fn attach_and_replay(
        &self,
        attachment: LaneAttachment,
    ) -> Result<AttachmentDecision, AssociationError> {
        let reliable_control = self
            .reliable_control
            .lock()
            .expect("reliable control state poisoned");
        let (decision, activated) = self.attach_with_activation(attachment)?;
        if activated {
            for envelope in reliable_control.replay() {
                self.try_admit_control(control_envelope_frame(envelope))?;
            }
        }
        Ok(decision)
    }

    pub fn detach(&self, lane: LaneKind, connection_nonce: u128) {
        let mut inner = self.inner.lock().expect("association state poisoned");
        if inner.lanes.get(&lane) != Some(&connection_nonce) {
            return;
        }
        inner.lanes.remove(&lane);
        self.attached_lanes
            .fetch_and(!lane_mask(lane), Ordering::AcqRel);
        if lane == LaneKind::Control || self.state() != AssociationState::Active {
            self.state
                .store(AssociationState::Reconnecting as u8, Ordering::Release);
        }
        if lane == LaneKind::Control {
            self.wake_pending_lanes.store(0, Ordering::Release);
            drop(inner);
            self.interactive_wake.notify_one();
            for wake in &self.bulk_wakes {
                wake.notify_one();
            }
        }
    }

    pub fn try_admit_control(&self, frame: Frame) -> Result<(), AssociationError> {
        self.try_admit(&self.control, frame)
    }

    pub fn admit_control_command(
        &self,
        payload: bytes::Bytes,
    ) -> Result<CommandId, AssociationError> {
        let command_id = CommandId::generate();
        let mut reliable_control = self
            .reliable_control
            .lock()
            .expect("reliable control state poisoned");
        let envelope = reliable_control
            .enqueue(command_id, payload)
            .map_err(AssociationError::ReliableControl)?;
        if self.state() == AssociationState::Active
            && let Err(error) = self.try_admit_control(control_envelope_frame(&envelope))
        {
            reliable_control.rollback_last(command_id);
            return Err(error);
        }
        Ok(command_id)
    }

    pub fn admit_ephemeral_control(&self, payload: bytes::Bytes) -> Result<(), AssociationError> {
        self.try_admit_control(Frame::new(FrameKind::CoordinatorEvent, payload))
    }

    pub fn replay_control_frames(&self) -> Vec<Frame> {
        self.reliable_control
            .lock()
            .expect("reliable control state poisoned")
            .replay()
            .map(control_envelope_frame)
            .collect()
    }

    pub fn control_outbox_len(&self) -> usize {
        self.reliable_control
            .lock()
            .expect("reliable control state poisoned")
            .replay()
            .len()
    }

    pub fn control_command_pending(&self, command_id: CommandId) -> bool {
        self.reliable_control
            .lock()
            .expect("reliable control state poisoned")
            .contains_outbound(command_id)
    }

    pub fn preview_control(&self, envelope: &ControlEnvelope) -> ControlApply {
        self.reliable_control
            .lock()
            .expect("reliable control state poisoned")
            .preview(envelope)
    }

    pub fn commit_control(&self, envelope: ControlEnvelope) -> ControlAck {
        self.reliable_control
            .lock()
            .expect("reliable control state poisoned")
            .commit(envelope)
    }

    pub fn acknowledge_control(&self, ack: ControlAck) -> Result<(), AssociationError> {
        self.reliable_control
            .lock()
            .expect("reliable control state poisoned")
            .acknowledge(ack)
            .map_err(AssociationError::ReliableControl)
    }

    pub fn current_control_ack(&self) -> ControlAck {
        self.reliable_control
            .lock()
            .expect("reliable control state poisoned")
            .current_ack()
    }

    pub fn install_peer_catalogue<I>(&self, descriptors: I) -> Result<(), AssociationError>
    where
        I: IntoIterator<Item = ProtocolDescriptor>,
    {
        let mut catalogue = ProtocolCatalogue::new(self.config.max_protocols_per_peer)
            .expect("validated protocol catalogue limit");
        catalogue
            .install(descriptors)
            .map_err(AssociationError::Catalogue)?;
        if let Some(installed) = self.peer_catalogue.get() {
            return if installed == &catalogue {
                Ok(())
            } else {
                Err(AssociationError::Catalogue(
                    CatalogueError::ChangedAfterInstall,
                ))
            };
        }
        match self.peer_catalogue.set(catalogue) {
            Ok(()) => Ok(()),
            Err(catalogue) if self.peer_catalogue.get() == Some(&catalogue) => Ok(()),
            Err(_) => Err(AssociationError::Catalogue(
                CatalogueError::ChangedAfterInstall,
            )),
        }
    }

    pub fn protocol_decision(
        &self,
        protocol_id: ProtocolId,
        fingerprint: ProtocolFingerprint,
    ) -> CatalogueDecision {
        self.peer_catalogue
            .get()
            .map_or(CatalogueDecision::Unsupported, |catalogue| {
                catalogue.compare(protocol_id, fingerprint)
            })
    }

    pub fn try_admit_interactive(&self, frame: Frame) -> Result<(), AssociationError> {
        self.prepare_data_lane(LaneKind::Interactive)?;
        self.try_admit(&self.interactive, frame)
    }

    pub(crate) fn try_reserve_bulk<F>(
        &self,
        update_route_hash: F,
        bytes: usize,
    ) -> Result<(usize, BulkAdmission<'_>), AssociationError>
    where
        F: FnOnce(&mut blake3::Hasher),
    {
        let stripe = self.bulk_stripe(update_route_hash)?;
        let admission = self.try_reserve_prepared_bulk(stripe, bytes)?;
        Ok((stripe, admission))
    }

    pub(crate) fn bulk_stripe<F>(&self, update_route_hash: F) -> Result<usize, AssociationError>
    where
        F: FnOnce(&mut blake3::Hasher),
    {
        self.ensure_active()?;
        Ok(if self.bulk.len() == 1 {
            0
        } else {
            let mut hasher = blake3::Hasher::new();
            update_route_hash(&mut hasher);
            stripe_from_hash(&hasher.finalize(), self.bulk.len())
        })
    }

    pub(crate) fn allocate_exact_target_dictionary_id(&self, stripe: usize) -> Option<u64> {
        let next = self.next_outbound_exact_target_ids.get(stripe)?;
        next.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (current
                < crate::messaging::target_dictionary::MAX_EXACT_TARGET_DICTIONARY_ENTRIES as u64)
                .then_some(current + 1)
        })
        .ok()
        .map(|previous| previous + 1)
    }

    pub(crate) fn bulk_lane_epoch(&self, stripe: usize) -> u64 {
        self.bulk_lane_epochs
            .get(stripe)
            .map_or(0, |epoch| epoch.load(Ordering::Acquire))
    }

    pub(crate) fn try_reserve_prepared_bulk(
        &self,
        stripe: usize,
        bytes: usize,
    ) -> Result<BulkAdmission<'_>, AssociationError> {
        if stripe >= self.bulk.len() {
            return Err(AssociationError::InvalidBulkStripe(
                u8::try_from(stripe).unwrap_or(u8::MAX),
            ));
        }
        self.prepare_data_lane(LaneKind::Bulk(stripe as u8))?;
        let permit = self.bulk[stripe].try_reserve().map_err(|_| {
            self.metrics.record_queue_rejection();
            AssociationError::QueueFull
        })?;
        self.reserve_bytes(bytes)?;
        Ok(BulkAdmission {
            association: self,
            permit: Some(permit),
            reserved_bytes: bytes,
        })
    }

    pub fn release_queued_bytes(&self, bytes: usize) {
        let _ = self
            .queued_bytes
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            });
        let _ = self
            .node_queued_bytes
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            });
    }

    pub fn begin_close(&self) {
        let mut inner = self.inner.lock().expect("association state poisoned");
        if self.state() != AssociationState::Closed {
            self.state
                .store(AssociationState::Closing as u8, Ordering::Release);
            inner.lanes.clear();
            self.attached_lanes.store(0, Ordering::Release);
            self.wake_pending_lanes.store(0, Ordering::Release);
        }
    }

    pub fn finish_close(&self) {
        let mut inner = self.inner.lock().expect("association state poisoned");
        self.state
            .store(AssociationState::Closed as u8, Ordering::Release);
        inner.lanes.clear();
        self.attached_lanes.store(0, Ordering::Release);
        self.wake_pending_lanes.store(0, Ordering::Release);
    }

    fn try_admit(
        &self,
        sender: &mpsc::Sender<Frame>,
        frame: Frame,
    ) -> Result<(), AssociationError> {
        self.ensure_active()?;
        let bytes = frame.payload_len();
        self.reserve_bytes(bytes)?;
        if sender.try_send(frame).is_err() {
            self.release_queued_bytes(bytes);
            return Err(AssociationError::QueueFull);
        }
        Ok(())
    }

    fn ensure_active(&self) -> Result<(), AssociationError> {
        if self.state() != AssociationState::Active {
            return Err(AssociationError::NotActive);
        }
        Ok(())
    }

    fn reserve_bytes(&self, bytes: usize) -> Result<(), AssociationError> {
        self.queued_bytes
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                let next = current.checked_add(bytes)?;
                (next <= self.config.max_outbound_bytes_per_association).then_some(next)
            })
            .map_err(|_| {
                self.metrics.record_association_byte_budget_rejection();
                AssociationError::ByteBudgetExceeded
            })?;
        if self
            .node_queued_bytes
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                let next = current.checked_add(bytes)?;
                (next <= self.config.max_outbound_bytes_per_node).then_some(next)
            })
            .is_err()
        {
            self.metrics.record_node_byte_budget_rejection();
            let _ = self
                .queued_bytes
                .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    Some(current.saturating_sub(bytes))
                });
            return Err(AssociationError::NodeByteBudgetExceeded);
        }
        Ok(())
    }

    pub(crate) fn record_outbound_write(&self, frames: usize, socket_writes: usize) {
        self.metrics.record_write_batch(frames, socket_writes);
    }

    pub(crate) fn record_exact_target_cache(&self, hits: u64, misses: u64) {
        self.metrics.record_exact_target_cache(hits, misses);
    }

    fn has_complete_lane_group(&self, lanes: &HashMap<LaneKind, u128>) -> bool {
        lanes.contains_key(&LaneKind::Control)
            && lanes.contains_key(&LaneKind::Interactive)
            && (0..self.config.bulk_stripes)
                .all(|index| lanes.contains_key(&LaneKind::Bulk(index as u8)))
    }
}

fn lane_mask(lane: LaneKind) -> u64 {
    let bit = match lane {
        LaneKind::Control => 0,
        LaneKind::Interactive => 1,
        LaneKind::Bulk(index) => u32::from(index) + 2,
    };
    1_u64 << bit
}

#[derive(Debug)]
struct AssociationReceiverSlots {
    control: Option<mpsc::Receiver<Frame>>,
    interactive: Option<mpsc::Receiver<Frame>>,
    bulk: Vec<Option<mpsc::Receiver<Frame>>>,
}

#[derive(Debug)]
pub struct AssociationManager {
    local_address: NodeAddress,
    local_incarnation: NodeIncarnation,
    config: RemotingConfig,
    associations: Mutex<HashMap<AssociationKey, Arc<Association>>>,
    remote_incarnations: Mutex<HashMap<NodeAddress, NodeIncarnation>>,
    queued_bytes: Arc<AtomicUsize>,
}

fn stripe_from_hash(hash: &blake3::Hash, stripes: usize) -> usize {
    debug_assert!(stripes > 0);
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(&hash.as_bytes()[..8]);
    (u64::from_be_bytes(prefix) as usize) % stripes
}

#[derive(Debug, Error)]
pub enum AssociationError {
    #[error("invalid remoting configuration")]
    InvalidConfig(#[source] RemotingConfigError),
    #[error("association registry is full")]
    AssociationLimit,
    #[error("lane attachment does not match association identity")]
    IdentityMismatch,
    #[error("bulk stripe {0} is outside the configured lane group")]
    InvalidBulkStripe(u8),
    #[error("association is not active")]
    NotActive,
    #[error("association is closed")]
    Closed,
    #[error("association lane queue is full")]
    QueueFull,
    #[error("association outbound byte budget is exhausted")]
    ByteBudgetExceeded,
    #[error("node-wide outbound byte budget is exhausted")]
    NodeByteBudgetExceeded,
    #[error("remote address is bound to another unreconciled or old incarnation")]
    OldOrUnreconciledIncarnation,
    #[error("incoming lanes name a conflicting AssociationId for the same peer incarnation")]
    IncomingAssociationConflict,
    #[error("association lane queue receiver is already owned")]
    LaneReceiverConflict,
    #[error("lane wake requested an invalid data lane")]
    InvalidLaneWake,
    #[error("peer protocol catalogue is invalid")]
    Catalogue(#[source] CatalogueError),
    #[error("association reliable control rejected the command")]
    ReliableControl(#[source] ReliableControlError),
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;

    use super::*;
    use crate::protocol::ProtocolFingerprint;
    use crate::wire::FrameKind;

    fn key() -> AssociationKey {
        AssociationKey {
            cluster_id: ClusterId::new("test").unwrap(),
            local_incarnation: NodeIncarnation::new(1).unwrap(),
            remote_address: NodeAddress::new("remote", 25520).unwrap(),
            remote_incarnation: NodeIncarnation::new(2).unwrap(),
        }
    }

    #[test]
    fn duplicate_lane_keeps_lowest_nonce_and_control_loss_closes_admission() {
        let association = Association::new(key(), RemotingConfig::default()).unwrap();
        for (lane, nonce) in [
            (LaneKind::Control, 20),
            (LaneKind::Interactive, 21),
            (LaneKind::Bulk(0), 22),
        ] {
            association
                .attach(LaneAttachment {
                    association_id: association.id(),
                    key: key(),
                    lane,
                    connection_nonce: nonce,
                })
                .unwrap();
        }
        assert_eq!(association.state(), AssociationState::Active);
        assert_eq!(
            association
                .attach(LaneAttachment {
                    association_id: association.id(),
                    key: key(),
                    lane: LaneKind::Control,
                    connection_nonce: 10,
                })
                .unwrap(),
            AttachmentDecision::ReplacedDuplicate
        );
        association.detach(LaneKind::Control, 10);
        assert_eq!(association.state(), AssociationState::Reconnecting);
        assert!(matches!(
            association.try_admit_interactive(Frame::new(FrameKind::Ask, bytes::Bytes::new())),
            Err(AssociationError::NotActive)
        ));
    }

    #[test]
    fn active_association_tolerates_a_transient_data_lane_disconnect() {
        let association = Association::new(key(), RemotingConfig::default()).unwrap();
        for (lane, nonce) in [
            (LaneKind::Control, 1),
            (LaneKind::Interactive, 2),
            (LaneKind::Bulk(0), 3),
        ] {
            association
                .attach(LaneAttachment {
                    association_id: association.id(),
                    key: key(),
                    lane,
                    connection_nonce: nonce,
                })
                .unwrap();
        }

        association.detach(LaneKind::Interactive, 2);

        assert_eq!(association.state(), AssociationState::Active);
        association
            .try_admit_interactive(Frame::new(FrameKind::Ask, bytes::Bytes::new()))
            .unwrap();
    }

    #[test]
    fn single_bulk_stripe_skips_route_hashing() {
        let association = Association::new(
            key(),
            RemotingConfig {
                bulk_queue_frames_per_stripe: 1,
                ..RemotingConfig::default()
            },
        )
        .unwrap();
        for (lane, nonce) in [
            (LaneKind::Control, 1),
            (LaneKind::Interactive, 2),
            (LaneKind::Bulk(0), 3),
        ] {
            association
                .attach(LaneAttachment {
                    association_id: association.id(),
                    key: key(),
                    lane,
                    connection_nonce: nonce,
                })
                .unwrap();
        }

        let frame = Frame::new(FrameKind::Tell, bytes::Bytes::from_static(b"message"));
        let (stripe, admission) = association
            .try_reserve_bulk(
                |_| panic!("single-stripe admission must not hash the route"),
                frame.payload_len(),
            )
            .unwrap();
        admission.send(frame);

        assert_eq!(stripe, 0);
        assert!(matches!(
            association.try_reserve_prepared_bulk(0, 1),
            Err(AssociationError::QueueFull)
        ));
        assert_eq!(association.metrics().outbound_queue_rejections, 1);
    }

    #[test]
    fn peer_catalogue_allows_idempotent_reinstall_but_rejects_changes() {
        let association = Association::new(key(), RemotingConfig::default()).unwrap();
        let protocol_id = ProtocolId::new(7).unwrap();
        let original = ProtocolFingerprint::digest(b"original");
        let changed = ProtocolFingerprint::digest(b"changed");
        let descriptor = |fingerprint| ProtocolDescriptor {
            protocol_id,
            fingerprint,
        };

        association
            .install_peer_catalogue([descriptor(original)])
            .unwrap();
        association
            .install_peer_catalogue([descriptor(original)])
            .unwrap();
        assert!(matches!(
            association.install_peer_catalogue([descriptor(changed)]),
            Err(AssociationError::Catalogue(
                CatalogueError::ChangedAfterInstall
            ))
        ));
        assert_eq!(
            association.protocol_decision(protocol_id, original),
            CatalogueDecision::Enabled
        );
        assert!(matches!(
            association.protocol_decision(protocol_id, changed),
            CatalogueDecision::FingerprintMismatch { actual } if actual == original
        ));
    }

    #[test]
    fn activation_is_reported_when_a_non_control_lane_completes_the_group() {
        let association = Association::new(key(), RemotingConfig::default()).unwrap();
        let (_, control_activated) = association
            .attach_with_activation(LaneAttachment {
                association_id: association.id(),
                key: key(),
                lane: LaneKind::Control,
                connection_nonce: 1,
            })
            .unwrap();
        let (_, interactive_activated) = association
            .attach_with_activation(LaneAttachment {
                association_id: association.id(),
                key: key(),
                lane: LaneKind::Interactive,
                connection_nonce: 2,
            })
            .unwrap();
        let (_, bulk_activated) = association
            .attach_with_activation(LaneAttachment {
                association_id: association.id(),
                key: key(),
                lane: LaneKind::Bulk(0),
                connection_nonce: 3,
            })
            .unwrap();
        let (_, duplicate_activated) = association
            .attach_with_activation(LaneAttachment {
                association_id: association.id(),
                key: key(),
                lane: LaneKind::Interactive,
                connection_nonce: 4,
            })
            .unwrap();

        assert!(!control_activated);
        assert!(!interactive_activated);
        assert!(bulk_activated);
        assert!(!duplicate_activated);
        assert_eq!(association.state(), AssociationState::Active);
    }

    #[test]
    fn queued_reliable_control_replays_when_a_non_control_lane_activates() {
        let association = Association::new(key(), RemotingConfig::default()).unwrap();
        association
            .admit_control_command(bytes::Bytes::from_static(b"queued"))
            .unwrap();
        for (lane, nonce) in [
            (LaneKind::Control, 1),
            (LaneKind::Interactive, 2),
            (LaneKind::Bulk(0), 3),
        ] {
            association
                .attach_and_replay(LaneAttachment {
                    association_id: association.id(),
                    key: key(),
                    lane,
                    connection_nonce: nonce,
                })
                .unwrap();
        }

        let mut control = association.take_lane_receiver(LaneKind::Control).unwrap();
        let envelope =
            crate::control::decode_control_envelope(&control.try_recv().unwrap()).unwrap();
        assert_eq!(envelope.sequence, 1);
        assert_eq!(envelope.payload, bytes::Bytes::from_static(b"queued"));
    }

    #[test]
    fn concurrent_reliable_admission_preserves_control_sequence_order() {
        let config = RemotingConfig {
            control_queue_frames: 1024,
            max_control_outbox_frames: 1024,
            ..RemotingConfig::default()
        };
        let association = Arc::new(Association::new(key(), config).unwrap());
        for (lane, nonce) in [
            (LaneKind::Control, 1),
            (LaneKind::Interactive, 2),
            (LaneKind::Bulk(0), 3),
        ] {
            association
                .attach(LaneAttachment {
                    association_id: association.id(),
                    key: key(),
                    lane,
                    connection_nonce: nonce,
                })
                .unwrap();
        }
        let mut control = association.take_lane_receiver(LaneKind::Control).unwrap();
        let barrier = Arc::new(Barrier::new(8));
        let workers = (0..8)
            .map(|_| {
                let association = association.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..64 {
                        association
                            .admit_control_command(bytes::Bytes::from_static(b"command"))
                            .unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }

        let sequences = (0..512)
            .map(|_| {
                let frame = control.try_recv().unwrap();
                crate::control::decode_control_envelope(&frame)
                    .unwrap()
                    .sequence
            })
            .collect::<Vec<_>>();
        assert_eq!(sequences, (1..=512).collect::<Vec<_>>());
    }

    #[test]
    fn reused_address_rejects_old_incarnation_after_explicit_replacement() {
        let config = RemotingConfig {
            max_associations: 1,
            ..RemotingConfig::default()
        };
        let manager = AssociationManager::new(
            NodeAddress::new("local", 25519).unwrap(),
            NodeIncarnation::new(1).unwrap(),
            config,
        )
        .unwrap();
        let address = NodeAddress::new("remote", 25520).unwrap();
        manager
            .get_or_create(
                ClusterId::new("test").unwrap(),
                address.clone(),
                NodeIncarnation::new(2).unwrap(),
            )
            .unwrap();
        assert_eq!(
            manager.replace_remote_incarnation(address.clone(), NodeIncarnation::new(3).unwrap()),
            1
        );
        assert!(matches!(
            manager.get_or_create(
                ClusterId::new("test").unwrap(),
                address,
                NodeIncarnation::new(2).unwrap(),
            ),
            Err(AssociationError::OldOrUnreconciledIncarnation)
        ));
    }

    #[test]
    fn node_byte_budget_is_shared_across_associations() {
        let config = RemotingConfig {
            max_associations: 2,
            max_outbound_bytes_per_association: 12,
            max_outbound_bytes_per_node: 12,
            ..RemotingConfig::default()
        };
        let manager = AssociationManager::new(
            NodeAddress::new("local", 25519).unwrap(),
            NodeIncarnation::new(1).unwrap(),
            config,
        )
        .unwrap();
        let cluster = ClusterId::new("test").unwrap();
        let first = manager
            .get_or_create(
                cluster.clone(),
                NodeAddress::new("first", 25520).unwrap(),
                NodeIncarnation::new(2).unwrap(),
            )
            .unwrap();
        let second = manager
            .get_or_create(
                cluster,
                NodeAddress::new("second", 25521).unwrap(),
                NodeIncarnation::new(3).unwrap(),
            )
            .unwrap();
        for association in [&first, &second] {
            for (lane, nonce) in [
                (LaneKind::Control, 1),
                (LaneKind::Interactive, 2),
                (LaneKind::Bulk(0), 3),
            ] {
                association
                    .attach(LaneAttachment {
                        association_id: association.id(),
                        key: association.key().clone(),
                        lane,
                        connection_nonce: nonce,
                    })
                    .unwrap();
            }
        }
        first
            .try_admit_interactive(Frame::new(
                FrameKind::Backpressure,
                bytes::Bytes::from_static(b"12345678"),
            ))
            .unwrap();
        assert!(matches!(
            second.try_admit_interactive(Frame::new(
                FrameKind::Backpressure,
                bytes::Bytes::from_static(b"12345678"),
            )),
            Err(AssociationError::NodeByteBudgetExceeded)
        ));
        assert_eq!(second.metrics().node_byte_budget_rejections, 1);
        first.release_queued_bytes(8);
        second
            .try_admit_interactive(Frame::new(
                FrameKind::Backpressure,
                bytes::Bytes::from_static(b"12345678"),
            ))
            .unwrap();
    }
}
