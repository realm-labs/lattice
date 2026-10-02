//! Endpoint-owned join boundary for an actor and all of its socket work.
//!
//! Normal completion waits for actor termination (including scoped completion outputs) and
//! socket-future destruction before registry removal. Drop is a non-awaiting fallback for task
//! cancellation; endpoint timeout cleanup retains its own ledger handle to finish joining I/O.

use std::sync::{Arc, Weak};

use lattice_actor::handle::ActorHandle;

use crate::association::Association;

use super::{
    EndpointError, RemotingEndpoint, actor::AssociationSupervisor, endpoint, tasks::IoTasks,
};
use crate::endpoint::lifecycle::wait_for_shutdown;

/// Lifetime guard retained by the endpoint, even when a connection wait is cancelled.
pub(super) struct AssociationOwner {
    /// Non-owning reference used for registry removal without retaining the endpoint forever.
    endpoint: Weak<RemotingEndpoint>,
    /// Exact generation whose admission is fenced and whose pending asks are failed on cleanup.
    association: Arc<Association>,
    /// Persistent admission fence and termination stream for the management actor.
    actor: ActorHandle<AssociationSupervisor>,
    /// Join boundary for socket futures that can outlive actor cancellation requests.
    tasks: Arc<IoTasks>,
}

impl AssociationOwner {
    /// Creates the guard before its run future is added to the endpoint JoinSet.
    pub(super) fn new(
        endpoint: Weak<RemotingEndpoint>,
        association: Arc<Association>,
        actor: ActorHandle<AssociationSupervisor>,
        tasks: Arc<IoTasks>,
    ) -> Self {
        Self {
            endpoint,
            association,
            actor,
            tasks,
        }
    }

    /// Waits for any retirement trigger, fences work, and joins actor plus socket cleanup.
    ///
    /// The guard's Drop removes only this generation, so a concurrently adopted replacement
    /// cannot be removed by delayed cleanup. Dropping the future invokes the same fencing
    /// fallback without an async join.
    pub(super) async fn run(self) -> Result<(), EndpointError> {
        let owner = endpoint(&self.endpoint)?;
        let mut shutdown = owner.shutdown_tx.subscribe();
        let mut terminated = self.actor.subscribe_terminated();
        drop(owner);
        let already_terminated = tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown) => false,
            () = self.association.wait_closed() => false,
            _ = terminated.recv() => true,
        };
        self.association.begin_close();
        // The actor's fence is persistent and does not depend on mailbox capacity.
        self.actor.fence_business_admission();
        self.tasks.abort_all();
        if !already_terminated {
            let _ = terminated.recv().await;
        }
        self.tasks.wait_empty().await;
        Ok(())
    }
}

impl Drop for AssociationOwner {
    fn drop(&mut self) {
        // This also runs when endpoint shutdown times out or its owning future is cancelled.
        self.association.begin_close();
        self.actor.fence_business_admission();
        self.tasks.abort_all();
        if let Some(endpoint) = self.endpoint.upgrade() {
            endpoint.messaging.fail_association(self.association.id());
            endpoint
                .associations
                .remove(self.association.key(), self.association.id());
            endpoint
                .supervisors
                .lock()
                .expect("association supervisors poisoned")
                .remove(&self.association.id());
        }
        self.association.finish_close();
    }
}
