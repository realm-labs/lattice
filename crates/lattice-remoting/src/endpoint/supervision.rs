//! Local actor supervision for one association generation.
//!
//! Only lifecycle events enter this mailbox. Business frames still go directly through the
//! association's bounded lane queues. Negotiation performs I/O but does not publish attachments;
//! the actor adopts both inbound and outbound sockets after checking the current lane operation.
//!
//! # Ownership boundaries
//!
//! * The endpoint registry ensures one actor per association ID. Its lifetime owner is kept in
//!   the endpoint JoinSet and independently observes shutdown or actor termination.
//! * The actor owns parked receivers and lane phases. A dial task owns a connection permit;
//!   a running socket task owns both its permit and receiver in a lane lease.
//! * Completed I/O returns its resources through actor-scoped completion work. A full mailbox
//!   delays delivery instead of dropping it. Actor shutdown joins cancelled completion work
//!   before publishing termination, including resources held in already completed results.
//!
//! # Event ordering
//!
//! Each dial or retry gets a fresh attempt token. A running connection also records its wire
//! nonce. Results must match the current phase and tokens before attachment, replay or receiver
//! restoration. Dropping a stale result releases its candidate resources without affecting a
//! replacement connection.
//!
//! Detach is published by the socket path before `LaneStopped` enters the mailbox. A wake can
//! therefore arrive while the actor still considers the lane running. The actor retains that
//! intent and checks the returned receiver before allowing the lane to sleep.
//!
//! # Retirement
//!
//! The lifetime owner fences association admission and actor work before cancelling socket tasks.
//! Normal retirement joins actor termination and the I/O ledger before removing the generation.
//! Its Drop fallback cannot await, so timeout cleanup retains a ledger handle separately and joins
//! actual socket resource destruction. Logical `Closed` is not itself a resource join boundary.

#![cfg_attr(not(test), deny(clippy::missing_docs_in_private_items))]

use std::sync::{Arc, Weak};

use lattice_actor::{
    handle::ActorHandle,
    mailbox::MailboxConfig,
    runtime::{ActorExecutionPolicy, ActorSpawnOptions},
    traits::Message,
};
use tokio::{
    runtime::Handle,
    sync::{OwnedSemaphorePermit, oneshot},
    time::Instant,
};

use crate::{
    association::{Association, AssociationState, LaneKind},
    handshake::{Handshake, NodeIdentity},
    lane::{LaneError, LaneExit},
    protocol::ProtocolDescriptor,
};

use super::{EndpointError, RemotingEndpoint, connection::OpenedLane, stream::EndpointStream};

/// Serializes lifecycle events and dispatches synchronous lane decisions.
mod actor;
/// Launches socket work, schedules retries and adopts negotiated candidates.
mod operations;
/// Joins an association's actor and I/O work on behalf of the endpoint.
mod owner;
/// Defines per-lane phases, operation tokens and transferred resource leases.
mod state;
/// Tracks cancellable socket work on the endpoint's Tokio executor.
mod tasks;

use actor::AssociationSupervisor;
use owner::AssociationOwner;
use state::LaneLease;
use tasks::IoTasks;

/// Negotiated incoming candidate whose socket and permit have not yet been adopted.
pub(super) struct InboundConnection {
    /// Socket after TLS and remoting negotiation.
    pub(super) stream: EndpointStream,
    /// Validated peer identity, generation, lane and connection nonce.
    pub(super) handshake: Handshake,
    /// Protocol descriptors exchanged on Control, empty on data lanes.
    pub(super) peer_catalogue: Vec<ProtocolDescriptor>,
    /// Whether setup verified the peer's certificate identity.
    pub(super) authenticated: bool,
    /// Original accept deadline, including time spent waiting for the actor mailbox.
    pub(super) deadline: Instant,
    /// Capacity reservation transferred from the listener setup task.
    pub(super) permit: OwnedSemaphorePermit,
}

/// Completed dial plus its capacity reservation, retained even when negotiation fails.
struct DialOutcome {
    /// Candidate socket or negotiation error; dropped before the permit on disposal.
    opened: Result<Box<OpenedLane>, EndpointError>,
    /// Reservation owned by the actual I/O task until its result is delivered or discarded.
    permit: OwnedSemaphorePermit,
}

/// Lifecycle inputs; business frames never enter this mailbox.
enum ConnectionEvent {
    #[cfg(test)]
    Panic,
    #[cfg(test)]
    HoldTurn {
        entered: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    },
    /// Starts eligible lanes when this endpoint is the deterministic dialer.
    EnsureConnected,
    /// Transfers an incoming candidate and replies after actor adoption or rejection.
    Inbound {
        /// Socket, negotiated identity and capacity reservation.
        connection: Box<InboundConnection>,
        /// Setup caller's response channel; a closed channel invalidates a queued candidate.
        accepted: oneshot::Sender<Result<(), EndpointError>>,
    },
    /// Returns an outbound candidate for token validation and adoption.
    DialCompleted {
        /// Lane whose dial task completed.
        lane: LaneKind,
        /// Token assigned before launching the task.
        attempt: u64,
        /// Task outcome, including its permit; outer failure means the task could not return it.
        result: Result<DialOutcome, EndpointError>,
    },
    /// Returns a receiver lease and the socket's exit result for the current connection.
    LaneStopped {
        /// Lane whose socket task ended.
        lane: LaneKind,
        /// Operation token of the running task.
        attempt: u64,
        /// Nonce of the registered socket, independent of the local attempt token.
        nonce: u128,
        /// Returned resources and exit result, or a join failure that lost the lease.
        result: Result<(LaneLease, Result<LaneExit, LaneError>), EndpointError>,
    },
    /// Requests a data lane's restoration; may precede its queued stop event.
    Wake(LaneKind),
    /// Advances a matching backoff phase into another dial attempt.
    RetryDue {
        /// Lane whose backoff delay elapsed.
        lane: LaneKind,
        /// Token of that particular timer, allowing an obsolete tick to be ignored.
        attempt: u64,
    },
    /// Retires a generation only if it has never activated before the initial deadline.
    EstablishExpired,
}

impl Message for ConnectionEvent {}

/// Cloneable actor access plus a ledger retained for endpoint timeout cleanup.
#[derive(Clone)]
pub(super) struct SupervisorHandle {
    /// Bounded lifecycle mailbox and termination/admission controls.
    actor: ActorHandle<AssociationSupervisor>,
    /// Socket cleanup boundary independent of actor or lifetime-owner cancellation.
    tasks: Arc<IoTasks>,
}

impl SupervisorHandle {
    /// Cancels socket work and waits for its futures to release captured I/O resources.
    pub(super) async fn abort_and_join_io(&self) {
        self.tasks.abort_all();
        self.tasks.wait_empty().await;
    }

    /// Reliably queues a connection request; activation is awaited separately by the endpoint.
    pub(super) async fn ensure_connected(&self) -> Result<(), EndpointError> {
        self.actor
            .tell(ConnectionEvent::EnsureConnected)
            .await
            .map_err(|_| EndpointError::SupervisorUnavailable)
    }

    /// Waits for incoming adoption under the original setup deadline, including mailbox delay.
    ///
    /// Timing out drops the response receiver. A still-queued event is then rejected before
    /// adoption. If the actor already adopted the socket, its lifetime owner retains cleanup.
    pub(super) async fn accept(&self, connection: InboundConnection) -> Result<(), EndpointError> {
        let deadline = connection.deadline;
        let (accepted, completion) = oneshot::channel();
        tokio::time::timeout_at(deadline, async {
            self.actor
                .tell(ConnectionEvent::Inbound {
                    connection: Box::new(connection),
                    accepted,
                })
                .await
                .map_err(|_| EndpointError::SupervisorUnavailable)?;
            completion
                .await
                .map_err(|_| EndpointError::SupervisorUnavailable)?
        })
        .await
        .map_err(|_| EndpointError::InboundSetupTimeout)?
    }
}

impl RemotingEndpoint {
    /// Gets or creates the generation's sole supervisor and transfers all receivers to it.
    ///
    /// Registry locking makes concurrent incoming and outgoing callers share the actor. Spawn
    /// failure retires the generation because receiver ownership cannot be reconstructed. The
    /// lifetime owner is launched outside that lock: its Drop path also removes registry entries.
    ///
    /// Must be called on a Tokio runtime; its current handle is captured for socket work.
    pub(super) fn supervisor_for(
        self: &Arc<Self>,
        association: Arc<Association>,
        peer: NodeIdentity,
    ) -> Result<SupervisorHandle, EndpointError> {
        self.ensure_running()?;
        if matches!(
            association.state(),
            AssociationState::Closing | AssociationState::Closed
        ) {
            return Err(EndpointError::SupervisorUnavailable);
        }
        let (handle, owner) = {
            let mut supervisors = self
                .supervisors
                .lock()
                .expect("association supervisors poisoned");
            if let Some(handle) = supervisors.get(&association.id()) {
                return Ok(handle.clone());
            }
            let receivers = association
                .take_receivers()
                .ok_or(EndpointError::SupervisorUnavailable)?;
            let tasks = Arc::new(IoTasks::default());
            let spawned = self.actor_spawner.spawn_actor(
                AssociationSupervisor::new(
                    Arc::downgrade(self),
                    association.clone(),
                    peer,
                    receivers,
                    Handle::current(),
                    tasks.clone(),
                ),
                ActorSpawnOptions {
                    // Bound events independently of message frame queues. At most one I/O
                    // operation, retry timer, and wake waiter exist for each configured lane.
                    mailbox: MailboxConfig::with_lanes(32, 8).with_deferred_capacity(32),
                    execution: Some(ActorExecutionPolicy::TaskPerActor),
                    ..ActorSpawnOptions::default()
                },
            );
            let actor = match spawned {
                Ok(actor) => actor,
                Err(error) => {
                    self.associations
                        .remove(association.key(), association.id());
                    return Err(error.into());
                }
            };
            let handle = SupervisorHandle {
                actor: actor.clone(),
                tasks: tasks.clone(),
            };
            supervisors.insert(association.id(), handle.clone());
            let owner = AssociationOwner::new(Arc::downgrade(self), association, actor, tasks);
            (handle, owner)
        };
        // No registry lock is held while starting/dropping the lifetime owner.
        self.spawn(owner.run())?;
        Ok(handle)
    }
}

/// Upgrades a non-owning endpoint reference, treating owner disappearance as shutdown.
fn endpoint(endpoint: &Weak<RemotingEndpoint>) -> Result<Arc<RemotingEndpoint>, EndpointError> {
    endpoint.upgrade().ok_or(EndpointError::ShuttingDown)
}

#[cfg(test)]
mod tests;
