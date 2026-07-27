use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use lattice_core::actor_ref::{ClusterId, NodeAddress, NodeIncarnation};
use thiserror::Error;
use tokio::sync::{Notify, mpsc};

use crate::{
    config::{RemotingConfig, RemotingConfigError},
    control::{ReliableControl, ReliableControlError},
    protocol::{CatalogueError, ProtocolCatalogue},
    wire::Frame,
};

mod admission;
mod budget;
mod control_plane;
mod lanes;
mod manager;
pub mod metrics;
mod wake;

use budget::OutboundByteBudget;
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

impl LaneKind {
    pub(crate) fn fails_pending_asks(self) -> bool {
        !matches!(self, Self::Bulk(_))
    }
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
    ever_active: AtomicBool,
    state_changed: Notify,
    created_at: Instant,
    last_peer_activity_micros: AtomicU64,
    attached_lanes: AtomicU64,
    wake_pending_lanes: AtomicU64,
    inner: Mutex<AssociationInner>,
    control: mpsc::Sender<Frame>,
    interactive: mpsc::Sender<Frame>,
    bulk: Vec<mpsc::Sender<Frame>>,
    bulk_lane_epochs: Vec<AtomicU64>,
    next_outbound_exact_target_ids: Vec<AtomicU64>,
    receivers: Mutex<AssociationReceiverSlots>,
    queued_bytes: OutboundByteBudget,
    node_queued_bytes: Arc<OutboundByteBudget>,
    admission_changed: Notify,
    control_outbox_changed: Notify,
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
        Self::new_with_id_and_budget(key, id, config, Arc::new(OutboundByteBudget::new()))
    }

    fn new_with_id_and_budget(
        key: AssociationKey,
        id: AssociationId,
        config: RemotingConfig,
        node_queued_bytes: Arc<OutboundByteBudget>,
    ) -> Result<Self, AssociationError> {
        config.validate().map_err(AssociationError::InvalidConfig)?;
        let max_control_outbox_frames = config.max_control_outbox_frames;
        let max_control_outbox_bytes = config.max_control_outbox_bytes;
        let max_control_streams = config.max_control_streams;
        let max_control_outbox_frames_per_stream = config.max_control_outbox_frames_per_stream;
        let max_control_outbox_bytes_per_stream = config.max_control_outbox_bytes_per_stream;
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
            ever_active: AtomicBool::new(false),
            state_changed: Notify::new(),
            created_at: Instant::now(),
            last_peer_activity_micros: AtomicU64::new(0),
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
            queued_bytes: OutboundByteBudget::new(),
            node_queued_bytes,
            admission_changed: Notify::new(),
            control_outbox_changed: Notify::new(),
            peer_catalogue: OnceLock::new(),
            reliable_control: Mutex::new(
                ReliableControl::new_with_limits(
                    id,
                    max_control_outbox_frames,
                    max_control_outbox_bytes,
                    max_control_outbox_frames_per_stream,
                    max_control_outbox_bytes_per_stream,
                    max_control_streams,
                )
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

    pub fn has_activated(&self) -> bool {
        self.ever_active.load(Ordering::Acquire)
    }

    pub fn has_live_connection(&self) -> bool {
        !matches!(
            self.state(),
            AssociationState::Closing | AssociationState::Closed
        ) && self.attached_lanes.load(Ordering::Acquire) != 0
    }

    /// Records that bytes authored by the peer arrived on one of this association's lanes.
    ///
    /// A completed handshake and every socket read both count, so the recorded instant is
    /// the last moment the peer proved it still owns this association generation.
    pub(crate) fn record_peer_activity(&self) {
        let elapsed = u64::try_from(self.created_at.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.last_peer_activity_micros
            .fetch_max(elapsed, Ordering::AcqRel);
    }

    /// How long it has been since the peer last proved it owns this association generation.
    pub fn peer_silence(&self) -> Duration {
        let last = self.last_peer_activity_micros.load(Ordering::Acquire);
        self.created_at
            .elapsed()
            .saturating_sub(Duration::from_micros(last))
    }

    pub async fn wait_until_active(&self) -> Result<(), AssociationError> {
        loop {
            let changed = self.state_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self.state() {
                AssociationState::Active => return Ok(()),
                AssociationState::Closing | AssociationState::Closed => {
                    return Err(AssociationError::Closed);
                }
                _ => {}
            }
            changed.await;
        }
    }

    pub fn begin_close(&self) {
        let mut inner = self.inner.lock().expect("association state poisoned");
        if self.state() != AssociationState::Closed {
            self.state
                .store(AssociationState::Closing as u8, Ordering::Release);
            inner.lanes.clear();
            self.attached_lanes.store(0, Ordering::Release);
            self.wake_pending_lanes.store(0, Ordering::Release);
            self.admission_changed.notify_waiters();
            self.control_outbox_changed.notify_waiters();
            self.state_changed.notify_waiters();
        }
    }

    pub fn finish_close(&self) {
        let mut inner = self.inner.lock().expect("association state poisoned");
        self.state
            .store(AssociationState::Closed as u8, Ordering::Release);
        inner.lanes.clear();
        self.attached_lanes.store(0, Ordering::Release);
        self.wake_pending_lanes.store(0, Ordering::Release);
        self.admission_changed.notify_waiters();
        self.control_outbox_changed.notify_waiters();
        self.state_changed.notify_waiters();
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

    pub(crate) fn record_outbound_write(&self, frames: usize, socket_writes: usize) {
        self.metrics.record_write_batch(frames, socket_writes);
    }

    pub(crate) fn record_exact_target_cache(&self, hits: u64, misses: u64) {
        self.metrics.record_exact_target_cache(hits, misses);
    }

    pub(crate) fn record_discarded_reply(&self) {
        self.metrics.record_discarded_reply();
    }

    pub(crate) fn record_dropped_inbound_frame(&self) {
        self.metrics.record_dropped_inbound_frame();
    }

    pub(crate) fn record_control_apply_retry(&self) {
        self.metrics.record_control_apply_retry();
    }

    pub(crate) fn record_control_retry_exhaustion(&self) {
        self.metrics.record_control_retry_exhaustion();
    }

    pub(crate) fn record_rejected_control_command(&self) {
        self.metrics.record_rejected_control_command();
    }

    pub(crate) fn record_dropped_ephemeral_control(&self) {
        self.metrics.record_dropped_ephemeral_control();
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

/// `attached_lanes` is only ever the exact mask of the owned lane map, so the two can never
/// drift into a state where a lane looks attached with no connection behind it.
fn attached_lanes_mask(lanes: &HashMap<LaneKind, u128>) -> u64 {
    lanes
        .keys()
        .copied()
        .map(lane_mask)
        .fold(0, |mask, lane| mask | lane)
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
    queued_bytes: Arc<OutboundByteBudget>,
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
mod tests;
