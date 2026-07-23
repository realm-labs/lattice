use std::{
    future::Future,
    sync::{Arc, Mutex, RwLock},
};

use crate::{
    association::{AssociationError, AssociationId, AssociationManager},
    bootstrap::{AcceptBootstrap, BootstrapHandler},
    config::RemotingConfig,
    control::{ControlDispatch, RejectControlDispatch},
    handshake::NodeIdentity,
    lane::BidirectionalLaneConfig,
    messaging::{inbound::InboundDispatch, outbound::OutboundMessaging},
    protocol::ProtocolDescriptor,
};
use tokio::sync::{Mutex as AsyncMutex, Semaphore, broadcast, watch};

#[cfg(feature = "tls")]
use super::EndpointSecurity;
use super::{EndpointError, RemotingEndpoint, RemotingEndpointBuilder};

impl RemotingEndpointBuilder {
    pub fn control_dispatch(mut self, control_dispatch: Arc<dyn ControlDispatch>) -> Self {
        self.control_dispatch = control_dispatch;
        self
    }

    pub fn catalogue(mut self, catalogue: Vec<ProtocolDescriptor>) -> Self {
        self.catalogue = catalogue;
        self
    }

    #[cfg(feature = "tls")]
    pub fn security(mut self, security: EndpointSecurity) -> Self {
        self.security = Some(security);
        self
    }

    pub fn build(self) -> Result<RemotingEndpoint, EndpointError> {
        self.config
            .validate()
            .map_err(AssociationError::InvalidConfig)?;
        #[cfg(feature = "tls")]
        {
            if self
                .security
                .as_ref()
                .is_some_and(|security| security.server_name.is_empty())
            {
                return Err(EndpointError::InvalidSecurity);
            }
        }
        if self.catalogue.len() > self.config.max_protocols_per_peer {
            return Err(EndpointError::ProtocolLimit);
        }
        let connection_limit = self.config.required_socket_budget().saturating_sub(1);
        let (shutdown_tx, _) = watch::channel(false);
        let (disconnect_tx, _) = broadcast::channel(self.config.max_associations);
        Ok(RemotingEndpoint {
            local: self.local,
            config: self.config,
            associations: self.associations,
            messaging: self.messaging,
            dispatch: self.dispatch,
            control_dispatch: self.control_dispatch,
            catalogue: self.catalogue,
            connections: Arc::new(Semaphore::new(connection_limit)),
            shutdown_tx,
            disconnect_tx,
            tasks: Mutex::new(Vec::new()),
            #[cfg(feature = "tls")]
            security: self.security,
            connect_lock: AsyncMutex::new(()),
            bootstrap_handler: RwLock::new(Arc::new(AcceptBootstrap)),
        })
    }
}

impl RemotingEndpoint {
    pub fn builder(
        local: NodeIdentity,
        config: RemotingConfig,
        associations: Arc<AssociationManager>,
        messaging: Arc<OutboundMessaging>,
        dispatch: Arc<dyn InboundDispatch>,
    ) -> RemotingEndpointBuilder {
        RemotingEndpointBuilder {
            local,
            config,
            associations,
            messaging,
            dispatch,
            control_dispatch: Arc::new(RejectControlDispatch),
            catalogue: Vec::new(),
            #[cfg(feature = "tls")]
            security: None,
        }
    }

    pub fn local_identity(&self) -> &NodeIdentity {
        &self.local
    }

    pub fn open_connection_count(&self) -> usize {
        self.config
            .required_socket_budget()
            .saturating_sub(1)
            .saturating_sub(self.connections.available_permits())
    }

    pub fn install_bootstrap_handler(&self, handler: Arc<dyn BootstrapHandler>) {
        *self
            .bootstrap_handler
            .write()
            .expect("bootstrap handler lock poisoned") = handler;
    }

    pub fn disconnect_association(
        &self,
        association_id: AssociationId,
    ) -> Result<(), EndpointError> {
        self.disconnect_tx
            .send(association_id)
            .map(|_| ())
            .map_err(|_| EndpointError::NoActiveConnections)
    }

    pub(super) fn lane_config(&self) -> BidirectionalLaneConfig {
        BidirectionalLaneConfig {
            maximum_frame_size: self.config.max_frame_size,
            maximum_concurrent_inbound_asks: self.config.max_pending_asks,
            heartbeat_interval: self.config.heartbeat_interval,
            heartbeat_miss_limit: self.config.heartbeat_miss_limit,
            idle_data_connection_timeout: self.config.idle_data_connection_timeout,
            maximum_cached_exact_targets: self.config.max_cached_exact_targets_per_lane,
            socket_read_ahead_bytes: self.config.socket_read_ahead_bytes,
            maximum_ready_write_batch_frames: self.config.max_ready_write_batch_frames,
            maximum_ready_read_batch_frames: self.config.max_ready_read_batch_frames,
            maximum_coalesced_write_batch_bytes: self.config.max_coalesced_write_batch_bytes,
            maximum_pending_control_applies: self.config.control_queue_frames,
        }
    }

    pub(super) fn spawn<F>(self: &Arc<Self>, future: F) -> Result<(), EndpointError>
    where
        F: Future<Output = Result<(), EndpointError>> + Send + 'static,
    {
        let mut tasks = self.tasks.lock().expect("endpoint task list poisoned");
        self.ensure_running()?;
        tasks.retain(|task| !task.is_finished());
        if tasks.len() >= self.config.required_socket_budget() {
            return Err(EndpointError::TaskLimit);
        }
        tasks.push(tokio::spawn(future));
        Ok(())
    }
}
