//! Logical peer connections, lane registration and outbound admission.
//!
//! An [`Association`] identifies one connection generation between exact local and remote node
//! incarnations. Its Control, Interactive and Bulk sockets share that identity, byte budget and
//! reliable control history. Reconnecting a socket keeps the generation; replacing an association
//! creates a new [`AssociationId`].
//!
//! # Lifecycle
//!
//! ```text
//! Establishing -- all configured lanes attach --> Active
//! Active -- Control detaches --> Reconnecting -- all lanes attach --> Active
//! Establishing / Active / Reconnecting -- begin_close --> Closing -- finish_close --> Closed
//! ```
//!
//! An idle data lane detaches without leaving `Active`. The next send requests its wake through
//! Control. Control remains connected while the association is active; losing it disables new
//! admission until the configured lane group has been restored.
//!
//! # Ownership and concurrency
//!
//! Endpoint supervisors own connection attempts and parked queue receivers. Running I/O tasks
//! temporarily own a receiver and a connection permit, and return them in a completion event.
//! This module publishes state directly so message admission does not need an actor round trip.
//! The locked lane map records connection nonces; its atomic bitmask supports fast presence checks.
//! Notifications wake tasks to recheck state or capacity and are not themselves authoritative state.

#![deny(missing_docs)]

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use lattice_model::cluster::{ClusterId, NodeEndpoint, NodeIncarnation};
use thiserror::Error;
use tokio::sync::{Notify, mpsc};

use crate::{
    config::{RemotingConfig, RemotingConfigError},
    control::{ReliableControl, ReliableControlError},
    handshake::NodeIdentity,
    protocol::{CatalogueError, ProtocolCatalogue},
    wire::Frame,
};

mod admission;
mod authentication;
pub(crate) mod budget;
mod control_plane;
mod lanes;
mod manager;
pub mod metrics;
mod wake;

use budget::OutboundByteBudget;
use budget::QueuedBytes;
use metrics::{AssociationMetrics, AssociationMetricsSnapshot};

/// Identifies one association generation, independent of individual socket reconnections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AssociationId(u128);

impl AssociationId {
    /// Generates a random identifier for a new association generation.
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().as_u128())
    }

    /// Creates an identifier from its wire value, or returns `None` for zero.
    pub const fn new(value: u128) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    /// Returns the nonzero wire value.
    pub const fn get(self) -> u128 {
        self.0
    }
}

/// Identifies the exact node incarnations joined by an association.
///
/// A key can outlive several connection generations. Use [`AssociationId`] as well when an
/// asynchronous result must apply only to the generation that produced it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AssociationKey {
    /// Cluster shared by both nodes.
    pub cluster_id: ClusterId,
    /// Local process incarnation.
    pub local_incarnation: NodeIncarnation,
    /// Advertised endpoint of the peer.
    pub remote_address: NodeEndpoint,
    /// Remote process incarnation; restarting the peer changes this value.
    pub remote_incarnation: NodeIncarnation,
}

/// Selects a physical connection and its outbound queue within an association.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LaneKind {
    /// Always-on connection for heartbeats, reliable control and data-lane wake requests.
    Control,
    /// Connection for asks, replies and interactive traffic; may sleep when idle.
    Interactive,
    /// One Bulk connection, addressed by its zero-based stripe index; may sleep when idle.
    Bulk(u8),
}

impl LaneKind {
    pub(crate) fn fails_pending_asks(self) -> bool {
        !matches!(self, Self::Bulk(_))
    }
}

/// Registers one negotiated socket as a lane of an exact association generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaneAttachment {
    /// Generation negotiated during the handshake.
    pub association_id: AssociationId,
    /// Exact peer/incarnation pair negotiated during the handshake.
    pub key: AssociationKey,
    /// Queue and socket role to register.
    pub lane: LaneKind,
    /// Socket identity used for duplicate arbitration and conditional detach.
    pub connection_nonce: u128,
}

/// Outcome of registering a lane, including deterministic duplicate arbitration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachmentDecision {
    /// No connection was registered for the lane.
    Attached,
    /// The candidate's lower nonce replaced the registered connection.
    ReplacedDuplicate,
    /// The existing connection's nonce was lower than or equal to the candidate's.
    RejectedDuplicate,
}

/// Published lifecycle state used by admission and connection waiters.
///
/// This describes the logical association, not each socket. In particular, data lanes may be
/// asleep while the association remains [`Active`](Self::Active).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AssociationState {
    /// The configured lane group has not yet become active.
    Establishing = 0,
    /// Business frames may be admitted; individual data lanes can be sleeping.
    Active = 1,
    /// Control was lost or an incomplete lane group is being restored.
    Reconnecting = 2,
    /// Retirement has begun; new attachments and business admission are rejected.
    Closing = 3,
    /// The generation has been logically retired.
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
    /// Authoritative registration of each lane's current connection nonce.
    lanes: HashMap<LaneKind, u128>,
    /// TLS proof bound to a specific currently registered Control connection.
    authenticated_control_peer: Option<(u128, NodeIdentity)>,
    /// First verified full peer identity, retained across Control reconnections.
    #[cfg(any(feature = "tls", test))]
    authenticated_peer_identity: Option<NodeIdentity>,
}

/// Receivers transferred once to a supervisor or standalone lane runner.
///
/// An association keeps the matching senders. Each receiver must have exactly one consumer;
/// obtaining these receivers does not register or establish sockets.
#[derive(Debug)]
pub struct AssociationReceivers {
    /// Receives frames admitted to Control.
    pub control: mpsc::Receiver<Frame>,
    /// Receives frames admitted to Interactive.
    pub interactive: mpsc::Receiver<Frame>,
    /// Receives frames admitted to Bulk, in stripe order.
    pub bulk: Vec<mpsc::Receiver<Frame>>,
}

/// Shared identity, admission state and queues for one logical connection generation.
///
/// Creating an association allocates bounded queues, but performs no network I/O. An endpoint
/// supervisor establishes and owns its sockets. Sending paths consult this object directly.
#[derive(Debug)]
pub struct Association {
    /// Connection generation shared by all lanes and reliable control commands.
    id: AssociationId,
    /// Peer identity that every attachment must match.
    key: AssociationKey,
    /// Validated limits fixed for this generation.
    config: RemotingConfig,
    /// Lifecycle snapshot read by admission without acquiring the connection-state lock.
    state: AtomicU8,
    /// Sticky activation history; initial establishment expiry must not retire a reconnecting peer.
    ever_active: AtomicBool,
    /// Wakes activation and retirement waiters; they must recheck `state`.
    state_changed: Notify,
    /// Epoch for the relative peer-activity timestamp.
    created_at: Instant,
    /// Monotonic offset from creation, updated by peer traffic on any lane.
    last_peer_activity_micros: AtomicU64,
    /// Atomic presence snapshot of `inner.lanes`, using `lane_mask` bit positions.
    attached_lanes: AtomicU64,
    /// Data lanes with an outstanding wake request, coalescing repeated sends.
    wake_pending_lanes: AtomicU64,
    /// Serializes registration, retirement and publication of authenticated Control identity.
    inner: Mutex<AssociationInner>,
    /// Sender retained while the Control receiver moves between owners.
    control: mpsc::Sender<Frame>,
    /// Sender retained while the Interactive receiver moves between owners.
    interactive: mpsc::Sender<Frame>,
    /// One bounded sender per configured Bulk stripe.
    bulk: Vec<mpsc::Sender<Frame>>,
    /// Changes on Bulk attachment so prepared routes can detect a replaced socket dictionary.
    bulk_lane_epochs: Vec<AtomicU64>,
    /// Per-stripe allocation counters for outbound exact-target dictionary identifiers.
    next_outbound_exact_target_ids: Vec<AtomicU64>,
    /// Receivers not yet handed to a consumer; endpoint supervisors take them once.
    receivers: Mutex<AssociationReceiverSlots>,
    /// Payload bytes retained by this association's queued outbound frames.
    queued_bytes: Arc<OutboundByteBudget>,
    /// Shared byte budget across the manager's associations.
    node_queued_bytes: Arc<OutboundByteBudget>,
    /// Wakes queue/budget waiters when retirement invalidates further admission.
    admission_changed: Notify,
    /// Wakes reliable outbox waiters after acknowledgements or retirement.
    control_outbox_changed: Notify,
    /// Peer protocols installed once; reconnections must advertise the same catalogue.
    peer_catalogue: OnceLock<ProtocolCatalogue>,
    /// Replayable outbound commands and inbound per-stream application watermarks.
    reliable_control: Mutex<ReliableControl>,
    /// Coalescing wake signal consumed by the Interactive lane's supervisor waiter.
    interactive_wake: Notify,
    /// Coalescing wake signals consumed by Bulk supervisor waiters, in stripe order.
    bulk_wakes: Vec<Notify>,
    /// Transport counters updated independently of lifecycle events.
    metrics: AssociationMetrics,
}

/// Queue slot and payload reservation acquired before constructing a Bulk frame.
pub(crate) struct BulkAdmission<'a> {
    permit: mpsc::Permit<'a, Frame>,
    reservation: QueuedBytes,
}

impl BulkAdmission<'_> {
    /// Transfers the payload reservation to the frame and commits the reserved queue slot.
    ///
    /// The actual payload size must equal the size charged during reservation.
    pub(crate) fn send(self, mut frame: Frame) {
        debug_assert_eq!(frame.payload_len(), self.reservation.bytes);
        frame.outbound_budget = Some(self.reservation);
        self.permit.send(frame);
    }
}

impl Association {
    /// Allocates a new generation and its bounded queues without opening sockets.
    ///
    /// # Errors
    ///
    /// Returns [`AssociationError::InvalidConfig`] if `config` is unsupported.
    pub fn new(key: AssociationKey, config: RemotingConfig) -> Result<Self, AssociationError> {
        Self::new_with_id(key, AssociationId::generate(), config)
    }

    /// Allocates queues for a generation whose identifier was already negotiated.
    ///
    /// # Errors
    ///
    /// Returns [`AssociationError::InvalidConfig`] if `config` is unsupported.
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
                authenticated_control_peer: None,
                #[cfg(any(feature = "tls", test))]
                authenticated_peer_identity: None,
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
            queued_bytes: Arc::new(OutboundByteBudget::new()),
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

    /// Returns this connection generation's identifier.
    pub fn id(&self) -> AssociationId {
        self.id
    }

    /// Returns cumulative transport counters for this Association generation.
    pub fn metrics(&self) -> AssociationMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Returns the exact local/remote incarnation pair.
    pub fn key(&self) -> &AssociationKey {
        &self.key
    }

    /// Returns the currently published lifecycle state.
    pub fn state(&self) -> AssociationState {
        AssociationState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// Returns whether this generation has ever had its complete lane group attached.
    ///
    /// Remains `true` during reconnection, sleep and retirement.
    pub fn has_activated(&self) -> bool {
        self.ever_active.load(Ordering::Acquire)
    }

    /// Returns whether a non-retired generation has at least one registered lane.
    ///
    /// This checks local bookkeeping, not network reachability. Use [`Self::peer_silence`]
    /// when deciding whether a silent peer's registration can still be trusted.
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

    /// Waits until the association admits business messages.
    ///
    /// This wait has no built-in timeout. Dropping the future stops waiting without changing
    /// the association or cancelling its connection attempts.
    ///
    /// # Errors
    ///
    /// Returns [`AssociationError::Closed`] if retirement begins before activation is observed.
    pub async fn wait_until_active(&self) -> Result<(), AssociationError> {
        loop {
            let changed = self.state_changed.notified();
            tokio::pin!(changed);
            // Register before checking state so an activation between check and await is not lost.
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

    /// Waits for retirement to begin, including retirement that predates this call.
    ///
    /// Both `Closing` and `Closed` satisfy this wait. It is a shutdown trigger, not a join
    /// boundary proving that socket tasks have released their resources.
    pub(crate) async fn wait_closed(&self) {
        loop {
            let changed = self.state_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if matches!(
                self.state(),
                AssociationState::Closing | AssociationState::Closed
            ) {
                return;
            }
            changed.await;
        }
    }

    /// Begins retirement of this association and all of its lanes.
    ///
    /// Publishes `Closing`, invalidates lane registrations and wake requests, and wakes
    /// admission, outbox and lifecycle waiters. Receivers still stored here are discarded;
    /// consumers that already own receivers are responsible for releasing them.
    ///
    /// This operation performs no socket I/O and does not wait for tasks to exit. Endpoint
    /// supervision observes retirement and cancels the owned work. Repeated calls are harmless;
    /// an already `Closed` generation stays closed.
    pub fn begin_close(&self) {
        let mut inner = self.inner.lock().expect("association state poisoned");
        if self.state() != AssociationState::Closed {
            self.state
                .store(AssociationState::Closing as u8, Ordering::Release);
            // Clearing registration also makes late nonce-qualified detach operations harmless.
            inner.lanes.clear();
            self.attached_lanes.store(0, Ordering::Release);
            self.wake_pending_lanes.store(0, Ordering::Release);
            self.admission_changed.notify_waiters();
            self.control_outbox_changed.notify_waiters();
            self.state_changed.notify_waiters();
        }
        // Receiver destruction can release queued frames and byte budgets. Do it outside the
        // registration lock so cleanup does not extend the state publication critical section.
        drop(inner);
        self.discard_unowned_receivers();
    }

    /// Marks this generation as logically closed and discards locally retained receivers.
    ///
    /// Does not join socket tasks. Endpoint-owned cleanup supplies that separate boundary;
    /// observing `Closed` alone is not proof that every underlying resource has been released.
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
        drop(inner);
        self.discard_unowned_receivers();
    }

    /// Checks admission and transfers one payload reservation into a queued frame.
    ///
    /// Queue rejection drops the frame, automatically returning any acquired byte reservation.
    fn try_admit(
        &self,
        sender: &mpsc::Sender<Frame>,
        mut frame: Frame,
    ) -> Result<(), AssociationError> {
        self.ensure_active()?;
        let bytes = frame.payload_len();
        frame.outbound_budget = Some(self.reserve_bytes(bytes)?);
        if sender.try_send(frame).is_err() {
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

    /// Requires every configured lane for initial activation and restoration after Control loss.
    ///
    /// This predicate is not continuously enforced while active: idle data lanes may detach.
    fn has_complete_lane_group(&self, lanes: &HashMap<LaneKind, u128>) -> bool {
        lanes.contains_key(&LaneKind::Control)
            && lanes.contains_key(&LaneKind::Interactive)
            && (0..self.config.bulk_stripes)
                .all(|index| lanes.contains_key(&LaneKind::Bulk(index as u8)))
    }
}

/// Assigns Control bit 0, Interactive bit 1 and Bulk stripe `n` bit `n + 2`.
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

/// Receivers whose ownership has not yet been transferred to a lane consumer.
#[derive(Debug)]
struct AssociationReceiverSlots {
    control: Option<mpsc::Receiver<Frame>>,
    interactive: Option<mpsc::Receiver<Frame>>,
    bulk: Vec<Option<mpsc::Receiver<Frame>>>,
}

/// Registry of exact peers, connection generations and a shared outbound byte budget.
///
/// The endpoint uses this registry to reconcile incoming and outgoing lanes. Incarnation
/// bindings persist after association removal until explicitly forgotten or replaced.
#[derive(Debug)]
pub struct AssociationManager {
    local_address: NodeEndpoint,
    local_incarnation: NodeIncarnation,
    config: RemotingConfig,
    associations: Mutex<HashMap<AssociationKey, Arc<Association>>>,
    remote_incarnations: Mutex<HashMap<NodeEndpoint, NodeIncarnation>>,
    queued_bytes: Arc<OutboundByteBudget>,
}

/// Failure to allocate, register, admit work to or reconcile an association.
#[derive(Debug, Error)]
pub enum AssociationError {
    /// Resource limits or timeouts failed validation.
    #[error("invalid remoting configuration")]
    InvalidConfig(#[source] RemotingConfigError),
    /// The manager has reached its association limit.
    #[error("association registry is full")]
    AssociationLimit,
    /// The candidate names another generation or peer/incarnation pair.
    #[error("lane attachment does not match association identity")]
    IdentityMismatch,
    /// A zero-based Bulk stripe is outside the configured group.
    #[error("bulk stripe {0} is outside the configured lane group")]
    InvalidBulkStripe(u8),
    /// Business admission was attempted outside `Active`.
    #[error("association is not active")]
    NotActive,
    /// Verified TLS identity does not match the exact registered Control connection.
    #[error("authenticated control identity does not match the attached lane")]
    AuthenticatedPeerMismatch,
    /// The generation or one of its queue receivers has been closed.
    #[error("association is closed")]
    Closed,
    /// A bounded lane queue has no available frame slot.
    #[error("association lane queue is full")]
    QueueFull,
    /// The association has no available outbound payload budget.
    #[error("association outbound byte budget is exhausted")]
    ByteBudgetExceeded,
    /// Associations sharing the manager have exhausted the node payload budget.
    #[error("node-wide outbound byte budget is exhausted")]
    NodeByteBudgetExceeded,
    /// The remote address is still bound to a different incarnation.
    #[error("remote address is bound to another unreconciled or old incarnation")]
    OldOrUnreconciledIncarnation,
    /// A new identifier conflicts with a locally live, recently responsive generation.
    #[error("incoming lanes name a conflicting AssociationId for the same peer incarnation")]
    IncomingAssociationConflict,
    /// A receiver was returned to a slot that already has an owner.
    #[error("association lane queue receiver is already owned")]
    LaneReceiverConflict,
    /// A wake requested Control or an unsupported data lane.
    #[error("lane wake requested an invalid data lane")]
    InvalidLaneWake,
    /// Peer protocol descriptors are invalid or changed after installation.
    #[error("peer protocol catalogue is invalid")]
    Catalogue(#[source] CatalogueError),
    /// A reliable control command or acknowledgement violated its bounds or sequencing rules.
    #[error("association reliable control rejected the command")]
    ReliableControl(#[source] ReliableControlError),
}

#[cfg(test)]
mod tests;
