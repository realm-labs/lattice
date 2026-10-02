//! TCP/TLS endpoint orchestration and association lifetime ownership.
//!
//! Build an endpoint, wrap it in [`Arc`], and call [`RemotingEndpoint::bind`] to accept peers.
//! [`RemotingEndpoint::connect_peer`] resolves an exact peer and waits for an active association.
//! Both directions share one local supervisor actor per association generation. Negotiation and
//! socket I/O run outside actor turns; business frames and replies use lane queues directly.
//!
//! # Connection flow
//!
//! ```text
//! outgoing: connect_peer -> supervisor -> dial task -> negotiated candidate -> actor adoption
//! incoming: listener -> setup task -> negotiated candidate -> supervisor -> actor adoption
//! adoption: register lane -> transfer receiver + permit -> socket task -> LaneStopped event
//! ```
//!
//! The lower peer identity tuple is the dialer for every lane. The other endpoint uses a short
//! bootstrap exchange to request reverse dialing. Control remains connected when data lanes
//! sleep. One failed socket can be replaced within its existing association generation.
//!
//! # Shutdown
//!
//! The endpoint owns listener and association-lifetime tasks. Each lifetime task observes endpoint
//! shutdown, association retirement and actor termination, then fences admission and cancels I/O.
//! [`RemotingEndpoint::shutdown`] joins this work; dropping a shutdown wait leaves unfinished
//! owners available for a later call. An injected actor runtime must outlive that cleanup.

#![deny(missing_docs)]

use crate::{
    association::{
        Association, AssociationError, AssociationId, AssociationManager, AssociationState,
        LaneKind,
    },
    bootstrap::{BootstrapError, BootstrapHandler},
    config::RemotingConfig,
    control::ControlDispatch,
    handshake::{HandshakeError, NodeIdentity},
    lane::LaneError,
    messaging::{inbound::InboundDispatch, outbound::OutboundMessaging},
    protocol::ProtocolDescriptor,
    transport::{NegotiationError, bind_tcp},
    wire::WireError,
};
use lattice_actor::{
    error::ActorSpawnError,
    runtime::{ActorRuntime, spawner::ActorSpawner},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};
use thiserror::Error;
use tokio::{
    sync::{Mutex as AsyncMutex, Semaphore, broadcast, watch},
    task::{JoinError, JoinSet},
};
#[cfg(feature = "tls")]
use tokio_rustls::rustls::{ClientConfig, ServerConfig};

mod bootstrap;
mod connection;
mod diagnostics;
mod lane_connection;
mod lifecycle;
mod listener;
mod reverse_dial;
mod state;
mod stream;
mod supervision;

use diagnostics::AcceptDiagnostics;
use lifecycle::wait_for_shutdown;
use supervision::SupervisorHandle;

const ACCEPT_BACKOFF_MIN: Duration = Duration::from_millis(10);
const ACCEPT_BACKOFF_MAX: Duration = Duration::from_secs(1);

/// Owns peer connection supervision, inbound listeners and bounded socket resources.
///
/// Construction performs no network I/O. Use [`Self::builder`] and keep the endpoint in an
/// [`Arc`] for binding or connecting. Call [`Self::shutdown`] to join owned work before stopping
/// an injected actor runtime.
pub struct RemotingEndpoint {
    local: NodeIdentity,
    config: RemotingConfig,
    associations: Arc<AssociationManager>,
    messaging: Arc<OutboundMessaging>,
    dispatch: Arc<dyn InboundDispatch>,
    control_dispatch: Arc<dyn ControlDispatch>,
    catalogue: Vec<ProtocolDescriptor>,
    /// Reservations cover setup, dialing, running sockets and selected reconnect backoff.
    connections: Arc<Semaphore>,
    accept_diagnostics: AcceptDiagnostics,
    /// Persistent endpoint fence, observed even by tasks created after it is set.
    shutdown_tx: watch::Sender<bool>,
    /// Fault-injection signal for dropping sockets without retiring their generation.
    disconnect_tx: broadcast::Sender<AssociationId>,
    /// Listener and lifetime owners retained here until joined, including cancelled shutdown waits.
    tasks: Mutex<JoinSet<Result<(), EndpointError>>>,
    /// Allows one shutdown join waiter at a time without taking ownership away from the endpoint.
    shutdown_lock: AsyncMutex<()>,
    #[cfg(feature = "tls")]
    security: Option<EndpointSecurity>,
    /// Serializes single-flight actor creation for each generation, across incoming/outgoing paths.
    supervisors: Mutex<HashMap<AssociationId, SupervisorHandle>>,
    /// Local actor runtime used exclusively for connection lifecycle decisions.
    actor_spawner: ActorSpawner,
    // Standalone endpoints own a small runtime; service endpoints share their injected runtime.
    _actor_runtime: Option<ActorRuntime>,
    bootstrap_handler: RwLock<Arc<dyn BootstrapHandler>>,
}

/// Application-provided TLS settings used for outbound and inbound remoting connections.
///
/// Configurations must support the application's certificate and peer-identity policy. This
/// crate does not install a process-wide rustls crypto provider.
#[cfg(feature = "tls")]
#[derive(Clone)]
pub struct EndpointSecurity {
    /// Client configuration used for outbound connections.
    pub client: Arc<ClientConfig>,
    /// Server configuration used by the listener; a peer certificate is required by setup.
    pub server: Arc<ServerConfig>,
    /// Default TLS server name for outbound connections, overridable by a bootstrap target.
    pub server_name: String,
}

/// Configures an endpoint before allocating its supervision and connection resources.
///
/// Created by [`RemotingEndpoint::builder`]. [`Self::build`] validates settings but does not bind
/// a listener or open sockets.
pub struct RemotingEndpointBuilder {
    local: NodeIdentity,
    config: RemotingConfig,
    associations: Arc<AssociationManager>,
    messaging: Arc<OutboundMessaging>,
    dispatch: Arc<dyn InboundDispatch>,
    control_dispatch: Arc<dyn ControlDispatch>,
    catalogue: Vec<ProtocolDescriptor>,
    actor_spawner: Option<ActorSpawner>,
    #[cfg(feature = "tls")]
    security: Option<EndpointSecurity>,
}

impl RemotingEndpoint {
    /// Binds the local advertised address and starts an owned accept loop.
    ///
    /// Returns once the listener is bound; it does not wait for incoming associations.
    ///
    /// # Errors
    ///
    /// Returns an error if shutdown has started, TCP binding fails, or the owned-task cap is reached.
    pub async fn bind(self: &Arc<Self>) -> Result<(), EndpointError> {
        self.ensure_running()?;
        let listener = bind_tcp(&self.local.address).await?;
        let endpoint = self.clone();
        self.spawn(async move { endpoint.accept_loop(listener).await })?;
        Ok(())
    }

    /// Gets or establishes an active association with the exact peer identity.
    ///
    /// Concurrent direct-dial requests share a generation's supervisor. The non-dialing side
    /// requests reverse dialing through bootstrap and waits for the incoming lane group.
    /// An already active association is returned even if its data lanes are sleeping.
    ///
    /// Dropping this wait does not cancel socket work already owned by a supervisor. A generation
    /// that never activates is eventually retired by its establishment timeout.
    ///
    /// # Errors
    ///
    /// Returns an error for shutdown, registry or supervision limits, a rejected reverse-dial
    /// exchange, or expiry of the configured connection wait. Individual socket failures on the
    /// direct-dial path are retried by supervision while that wait remains pending.
    pub async fn connect_peer(
        self: &Arc<Self>,
        peer: NodeIdentity,
    ) -> Result<Arc<Association>, EndpointError> {
        let mut shutdown = self.shutdown_tx.subscribe();
        self.ensure_running()?;
        if let Some(association) =
            self.associations
                .get_exact(&peer.cluster_id, &peer.address, peer.incarnation)
            && association.state() == AssociationState::Active
        {
            return Ok(association);
        }
        if !self
            .associations
            .should_dial(&peer.address, peer.incarnation)
        {
            return tokio::select! {
                biased;
                () = wait_for_shutdown(&mut shutdown) => Err(EndpointError::ShuttingDown),
                result = self.request_reverse_peer(peer) => result,
            };
        }
        let association = self.associations.get_or_create(
            peer.cluster_id.clone(),
            peer.address.clone(),
            peer.incarnation,
        )?;
        let supervisor = self.supervisor_for(association.clone(), peer)?;
        tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown) => Err(EndpointError::ShuttingDown),
            result = tokio::time::timeout(self.config.connect_timeout, async {
                supervisor.ensure_connected().await?;
                association.wait_until_active().await?;
                Ok::<_, EndpointError>(())
            }) => {
                result.map_err(|_| EndpointError::ConnectTimeout)??;
                Ok(association)
            }
        }
    }

    #[cfg(test)]
    fn supervisor_count(&self) -> usize {
        self.supervisors
            .lock()
            .expect("association supervisors poisoned")
            .len()
    }

    /// Enumerates lanes in the same order as receiver transfer and supervisor slot indexing.
    fn lanes(&self) -> impl Iterator<Item = LaneKind> {
        [LaneKind::Control, LaneKind::Interactive]
            .into_iter()
            .chain((0..self.config.bulk_stripes).map(|index| LaneKind::Bulk(index as u8)))
    }
}

/// Failure to build, connect, probe or shut down a remoting endpoint.
#[derive(Debug, Error)]
pub enum EndpointError {
    /// The local runtime could not spawn a connection supervisor.
    #[error("association supervisor could not start")]
    SupervisorSpawn(#[from] ActorSpawnError),
    /// The generation's supervisor or completion channel is unavailable.
    #[error("association supervisor is unavailable")]
    SupervisorUnavailable,
    /// Association registration or admission failed.
    #[error("association endpoint failed")]
    Association(#[from] AssociationError),
    /// Socket I/O, TLS or frame encoding failed.
    #[error("association endpoint wire failed")]
    Wire(#[from] WireError),
    /// Handshake or catalogue negotiation failed.
    #[error("association endpoint negotiation failed")]
    Negotiation(#[from] NegotiationError),
    /// Peer identity or advertised handshake limits failed validation.
    #[error("association endpoint handshake failed")]
    Handshake(#[from] HandshakeError),
    /// An established lane failed.
    #[error("association lane failed")]
    Lane(#[from] LaneError),
    /// The incoming peer dialed despite being the non-dialing side of the pair.
    #[error("only the stable lower node identity may dial")]
    WrongDialDirection,
    /// Bootstrap did not authorize the exact peer to reverse dial.
    #[error("the authoritative peer rejected a reverse-dial request")]
    ReverseDialRejected,
    /// No connection permit was available.
    #[error("association connection cap reached")]
    ConnectionLimit,
    /// An outbound attempt, probe or connection wait exceeded its timeout.
    #[error("association connection timed out")]
    ConnectTimeout,
    /// Inbound setup or actor adoption exceeded the shared deadline.
    #[error("inbound connection establishment timed out")]
    InboundSetupTimeout,
    /// An incoming candidate targeted a lane whose receiver is already in use.
    #[error("association lane {0:?} already owns its queue receiver")]
    LaneAlreadyRunning(LaneKind),
    /// The local catalogue contains more descriptors than configured.
    #[error("local actor protocol catalogue exceeds its configured bound")]
    ProtocolLimit,
    /// TLS settings contain an empty default server name.
    #[error("endpoint TLS configuration is invalid")]
    InvalidSecurity,
    /// The endpoint cannot admit another listener or lifetime task.
    #[error("association endpoint task cap reached")]
    TaskLimit,
    /// Endpoint shutdown has fenced further work.
    #[error("association endpoint is shutting down")]
    ShuttingDown,
    /// An owned Tokio task panicked or was unexpectedly cancelled.
    #[error("association endpoint task failed")]
    Join(#[source] JoinError),
    /// The shutdown deadline expired; remaining work was cancelled and cleanup was attempted.
    #[error("association endpoint shutdown timed out")]
    ShutdownTimeout,
    /// No connection task was subscribed to the requested disconnect signal.
    #[error("association endpoint has no active connections")]
    NoActiveConnections,
    /// A bootstrap request or response violated the protocol.
    #[error("association endpoint bootstrap protocol failed")]
    Bootstrap(#[from] BootstrapError),
    /// A bootstrap target specified an empty expected node ID or TLS server name.
    #[error("bootstrap probe target is invalid")]
    InvalidBootstrapTarget,
}

#[cfg(test)]
#[path = "endpoint/idle_tests.rs"]
mod idle_tests;

#[cfg(test)]
mod tests;
