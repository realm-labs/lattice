//! Endpoint-owned join boundary for an actor and all of its socket work.

use std::sync::{Arc, Weak};

use lattice_actor::handle::ActorHandle;

use crate::association::Association;

use super::{
    EndpointError, RemotingEndpoint, actor::AssociationSupervisor, endpoint, tasks::IoTasks,
};
use crate::endpoint::lifecycle::wait_for_shutdown;

pub(super) struct AssociationOwner {
    endpoint: Weak<RemotingEndpoint>,
    association: Arc<Association>,
    actor: ActorHandle<AssociationSupervisor>,
    tasks: Arc<IoTasks>,
}

impl AssociationOwner {
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
