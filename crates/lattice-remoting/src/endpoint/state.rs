//! Endpoint construction, shared resources and owned-task admission.
//!
//! A standalone endpoint owns a small actor runtime. Services inject their existing spawner and
//! retain runtime ownership. Socket work always uses the Tokio executor captured when a supervisor
//! is created, even if actor turns execute on the actor runtime's workers.

use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex, RwLock},
};

use lattice_actor::runtime::{ActorRuntime, ActorRuntimeConfig, spawner::ActorSpawner};

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
use tokio::{
    sync::{Semaphore, broadcast, watch},
    task::JoinSet,
};

#[cfg(feature = "tls")]
use super::EndpointSecurity;
use super::{
    EndpointError, RemotingEndpoint, RemotingEndpointBuilder, diagnostics::AcceptDiagnostics,
};

impl RemotingEndpointBuilder {
    /// Shares the owner's local actor runtime for connection supervision.
    ///
    /// The runtime must outlive the endpoint and its shutdown. Without an injected spawner,
    /// a standalone endpoint owns a one-worker actor runtime. Socket tasks always run on the
    /// Tokio executor that created the association supervisor.
    pub fn actor_spawner(mut self, spawner: ActorSpawner) -> Self {
        self.actor_spawner = Some(spawner);
        self
    }

    /// Sets the handler for reliable and ephemeral inbound control operations.
    ///
    /// The default handler rejects control application. Business message dispatch is separate.
    pub fn control_dispatch(mut self, control_dispatch: Arc<dyn ControlDispatch>) -> Self {
        self.control_dispatch = control_dispatch;
        self
    }

    /// Sets the local protocol descriptors advertised during Control negotiation.
    pub fn catalogue(mut self, catalogue: Vec<ProtocolDescriptor>) -> Self {
        self.catalogue = catalogue;
        self
    }

    /// Enables TLS using application-provided client/server settings and a default server name.
    #[cfg(feature = "tls")]
    pub fn security(mut self, security: EndpointSecurity) -> Self {
        self.security = Some(security);
        self
    }

    /// Validates settings and allocates an endpoint without binding or connecting.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid remoting limits, an empty TLS server name or an oversized
    /// local protocol catalogue. Peer catalogue compatibility is checked during negotiation.
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
        let connection_limit = self.config.connection_capacity();
        let (shutdown_tx, _) = watch::channel(false);
        let (disconnect_tx, _) = broadcast::channel(self.config.max_associations);
        let actor_runtime = self.actor_spawner.is_none().then(|| {
            ActorRuntime::new(ActorRuntimeConfig {
                task_worker_count: 1,
                ..ActorRuntimeConfig::default()
            })
        });
        let actor_spawner = self.actor_spawner.unwrap_or_else(|| {
            actor_runtime
                .as_ref()
                .expect("standalone actor runtime")
                .spawner()
        });
        Ok(RemotingEndpoint {
            local: self.local,
            config: self.config,
            associations: self.associations,
            messaging: self.messaging,
            dispatch: self.dispatch,
            control_dispatch: self.control_dispatch,
            catalogue: self.catalogue,
            connections: Arc::new(Semaphore::new(connection_limit)),
            accept_diagnostics: AcceptDiagnostics::default(),
            shutdown_tx,
            disconnect_tx,
            tasks: Mutex::new(JoinSet::new()),
            shutdown_lock: tokio::sync::Mutex::new(()),
            #[cfg(feature = "tls")]
            security: self.security,
            supervisors: Mutex::new(HashMap::new()),
            actor_spawner,
            _actor_runtime: actor_runtime,
            bootstrap_handler: RwLock::new(Arc::new(AcceptBootstrap)),
        })
    }
}

impl RemotingEndpoint {
    /// Creates a builder using the local identity and shared messaging components.
    ///
    /// The association manager and messaging layer should use the same identity and resource
    /// limits as the endpoint. TLS is disabled until security settings are supplied.
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
            actor_spawner: None,
            #[cfg(feature = "tls")]
            security: None,
        }
    }

    /// Returns the identity advertised by this endpoint's handshakes and bootstrap responses.
    pub fn local_identity(&self) -> &NodeIdentity {
        &self.local
    }

    /// Returns the number of reserved connection permits.
    ///
    /// Includes inbound setup, outbound dialing and reservations retained during reconnect
    /// backoff, so it may exceed the number of currently open sockets. Excludes the listener.
    pub fn open_connection_count(&self) -> usize {
        self.config
            .connection_capacity()
            .saturating_sub(self.connections.available_permits())
    }

    /// Returns the cumulative count of accepted sockets rejected for lack of connection permits.
    pub fn shed_connection_count(&self) -> u64 {
        self.accept_diagnostics.connection_limit_rejections()
    }

    /// Returns the cumulative count of listener accept failures, including recoverable failures.
    pub fn accept_failure_count(&self) -> u64 {
        self.accept_diagnostics.accept_failures()
    }

    /// Replaces the synchronous routing policy for future cluster bootstrap requests.
    ///
    /// Direct-peer reverse-dial requests use the endpoint's deterministic direction policy.
    pub fn install_bootstrap_handler(&self, handler: Arc<dyn BootstrapHandler>) {
        *self
            .bootstrap_handler
            .write()
            .expect("bootstrap handler lock poisoned") = handler;
    }

    /// Requests that current sockets for a generation disconnect, allowing normal reconnection.
    ///
    /// This is a fault-injection operation; it does not logically retire the association.
    /// Success reports broadcast delivery to a subscriber, not completion of disconnection.
    ///
    /// # Errors
    ///
    /// Returns [`EndpointError::NoActiveConnections`] when the broadcast has no subscribers.
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
            control_apply_retry_timeout: self.config.control_apply_retry_timeout,
            idle_data_connection_timeout: self.config.idle_data_connection_timeout,
            maximum_cached_exact_targets: self.config.max_cached_exact_targets_per_lane,
            socket_read_ahead_bytes: self.config.socket_read_ahead_bytes,
            maximum_ready_write_batch_frames: self.config.max_ready_write_batch_frames,
            maximum_ready_read_batch_frames: self.config.max_ready_read_batch_frames,
            maximum_coalesced_write_batch_bytes: self.config.max_coalesced_write_batch_bytes,
            maximum_pending_control_applies: self.config.control_queue_frames,
        }
    }

    pub(super) fn task_limit(&self) -> usize {
        // Include headroom for a retired generation while its replacement is being adopted.
        // Each association has one lifetime owner; socket work is bounded by connection permits.
        // Inbound connection tasks are owned by the accept loop's separate JoinSet.
        self.config.connection_capacity().saturating_add(1)
    }

    pub(super) fn spawn<F>(self: &Arc<Self>, future: F) -> Result<(), EndpointError>
    where
        F: Future<Output = Result<(), EndpointError>> + Send + 'static,
    {
        let mut tasks = self.tasks.lock().expect("endpoint task list poisoned");
        self.ensure_running()?;
        // Reap completed tasks before enforcing the cap. Background failures must not prevent
        // unrelated tasks from being admitted, but should not disappear without diagnostics.
        while let Some(result) = tasks.try_join_next() {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(error = ?error, "remoting endpoint task failed");
                }
                Err(error) if error.is_cancelled() => {}
                Err(error) => {
                    tracing::warn!(error = ?error, "remoting endpoint task join failed");
                }
            }
        }
        if tasks.len() >= self.task_limit() {
            return Err(EndpointError::TaskLimit);
        }
        tasks.spawn(future);
        Ok(())
    }
}
