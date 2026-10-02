//! Local actor supervision for one association generation.
//!
//! Only lifecycle events enter this mailbox. Business frames still go directly through the
//! association's bounded lane queues. Negotiation performs I/O but does not publish attachments;
//! the actor adopts both inbound and outbound sockets after checking the current lane operation.

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

mod actor;
mod operations;
mod owner;
mod state;
mod tasks;

use actor::AssociationSupervisor;
use owner::AssociationOwner;
use state::LaneLease;
use tasks::IoTasks;

pub(super) struct InboundConnection {
    pub(super) stream: EndpointStream,
    pub(super) handshake: Handshake,
    pub(super) peer_catalogue: Vec<ProtocolDescriptor>,
    pub(super) authenticated: bool,
    pub(super) deadline: Instant,
    pub(super) permit: OwnedSemaphorePermit,
}

struct DialOutcome {
    opened: Result<Box<OpenedLane>, EndpointError>,
    permit: OwnedSemaphorePermit,
}

enum ConnectionEvent {
    #[cfg(test)]
    Panic,
    #[cfg(test)]
    HoldTurn {
        entered: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    },
    EnsureConnected,
    Inbound {
        connection: Box<InboundConnection>,
        accepted: oneshot::Sender<Result<(), EndpointError>>,
    },
    DialCompleted {
        lane: LaneKind,
        attempt: u64,
        result: Result<DialOutcome, EndpointError>,
    },
    LaneStopped {
        lane: LaneKind,
        attempt: u64,
        nonce: u128,
        result: Result<(LaneLease, Result<LaneExit, LaneError>), EndpointError>,
    },
    Wake(LaneKind),
    RetryDue {
        lane: LaneKind,
        attempt: u64,
    },
    EstablishExpired,
}

impl Message for ConnectionEvent {}

#[derive(Clone)]
pub(super) struct SupervisorHandle {
    actor: ActorHandle<AssociationSupervisor>,
    tasks: Arc<IoTasks>,
}

impl SupervisorHandle {
    pub(super) async fn abort_and_join_io(&self) {
        self.tasks.abort_all();
        self.tasks.wait_empty().await;
    }

    pub(super) async fn ensure_connected(&self) -> Result<(), EndpointError> {
        self.actor
            .tell(ConnectionEvent::EnsureConnected)
            .await
            .map_err(|_| EndpointError::SupervisorUnavailable)
    }

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

fn endpoint(endpoint: &Weak<RemotingEndpoint>) -> Result<Arc<RemotingEndpoint>, EndpointError> {
    endpoint.upgrade().ok_or(EndpointError::ShuttingDown)
}

#[cfg(test)]
mod tests;
