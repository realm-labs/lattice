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

pub struct RemotingEndpoint {
    local: NodeIdentity,
    config: RemotingConfig,
    associations: Arc<AssociationManager>,
    messaging: Arc<OutboundMessaging>,
    dispatch: Arc<dyn InboundDispatch>,
    control_dispatch: Arc<dyn ControlDispatch>,
    catalogue: Vec<ProtocolDescriptor>,
    connections: Arc<Semaphore>,
    accept_diagnostics: AcceptDiagnostics,
    shutdown_tx: watch::Sender<bool>,
    disconnect_tx: broadcast::Sender<AssociationId>,
    tasks: Mutex<JoinSet<Result<(), EndpointError>>>,
    shutdown_lock: AsyncMutex<()>,
    #[cfg(feature = "tls")]
    security: Option<EndpointSecurity>,
    supervisors: Mutex<HashMap<AssociationId, SupervisorHandle>>,
    actor_spawner: ActorSpawner,
    // Standalone endpoints own a small runtime; service endpoints share their injected runtime.
    _actor_runtime: Option<ActorRuntime>,
    bootstrap_handler: RwLock<Arc<dyn BootstrapHandler>>,
}

#[cfg(feature = "tls")]
#[derive(Clone)]
pub struct EndpointSecurity {
    pub client: Arc<ClientConfig>,
    pub server: Arc<ServerConfig>,
    pub server_name: String,
}

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
    pub async fn bind(self: &Arc<Self>) -> Result<(), EndpointError> {
        self.ensure_running()?;
        let listener = bind_tcp(&self.local.address).await?;
        let endpoint = self.clone();
        self.spawn(async move { endpoint.accept_loop(listener).await })?;
        Ok(())
    }

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

    fn lanes(&self) -> impl Iterator<Item = LaneKind> {
        [LaneKind::Control, LaneKind::Interactive]
            .into_iter()
            .chain((0..self.config.bulk_stripes).map(|index| LaneKind::Bulk(index as u8)))
    }
}

#[derive(Debug, Error)]
pub enum EndpointError {
    #[error("association supervisor could not start")]
    SupervisorSpawn(#[from] ActorSpawnError),
    #[error("association supervisor is unavailable")]
    SupervisorUnavailable,
    #[error("association endpoint failed")]
    Association(#[from] AssociationError),
    #[error("association endpoint wire failed")]
    Wire(#[from] WireError),
    #[error("association endpoint negotiation failed")]
    Negotiation(#[from] NegotiationError),
    #[error("association endpoint handshake failed")]
    Handshake(#[from] HandshakeError),
    #[error("association lane failed")]
    Lane(#[from] LaneError),
    #[error("only the stable lower node identity may dial")]
    WrongDialDirection,
    #[error("the authoritative peer rejected a reverse-dial request")]
    ReverseDialRejected,
    #[error("association connection cap reached")]
    ConnectionLimit,
    #[error("association connection timed out")]
    ConnectTimeout,
    #[error("inbound connection establishment timed out")]
    InboundSetupTimeout,
    #[error("association lane {0:?} already owns its queue receiver")]
    LaneAlreadyRunning(LaneKind),
    #[error("local actor protocol catalogue exceeds its configured bound")]
    ProtocolLimit,
    #[error("endpoint TLS configuration is invalid")]
    InvalidSecurity,
    #[error("association endpoint task cap reached")]
    TaskLimit,
    #[error("association endpoint is shutting down")]
    ShuttingDown,
    #[error("association endpoint task failed")]
    Join(#[source] JoinError),
    #[error("association endpoint shutdown timed out")]
    ShutdownTimeout,
    #[error("association endpoint has no active connections")]
    NoActiveConnections,
    #[error("association endpoint bootstrap protocol failed")]
    Bootstrap(#[from] BootstrapError),
    #[error("bootstrap probe target is invalid")]
    InvalidBootstrapTarget,
}

#[cfg(test)]
#[path = "endpoint/idle_tests.rs"]
mod idle_tests;

#[cfg(test)]
mod tests;
