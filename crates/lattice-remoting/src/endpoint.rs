use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};

use lattice_core::actor_ref::{ClusterId, NodeAddress, NodeIncarnation};
use thiserror::Error;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Mutex as AsyncMutex, Semaphore, broadcast, mpsc::Receiver, watch},
    task::{JoinError, JoinHandle, JoinSet},
};
#[cfg(feature = "tls")]
use tokio_rustls::rustls::{ClientConfig, ServerConfig};

#[cfg(feature = "tls")]
use crate::transport::{connect_tls, verify_peer_certificate_identity};
use crate::{
    association::{
        Association, AssociationError, AssociationId, AssociationManager, AssociationState,
        LaneAttachment, LaneKind,
    },
    bootstrap::{BootstrapError, BootstrapHandler},
    config::RemotingConfig,
    control::ControlDispatch,
    handshake::{FeatureBits, Handshake, HandshakeError, HandshakeValidator, NodeIdentity},
    lane::{BidirectionalLane, LaneError, LaneExit, LaneServices},
    messaging::{inbound::InboundDispatch, outbound::OutboundMessaging},
    protocol::ProtocolDescriptor,
    transport::{
        FramedConnection, NegotiationError, bind_tcp, connect_tcp, negotiate_inbound_from_frame,
        negotiate_outbound,
    },
    wire::{Frame, FrameCodec, FrameKind, WireError},
};

mod bootstrap;
mod diagnostics;
mod lifecycle;
mod reverse_dial;
mod state;
mod stream;

#[cfg(test)]
use diagnostics::is_peer_disconnect;
use diagnostics::{
    AcceptDiagnostics, AcceptRecovery, classify_accept_failure, observe_connection_result,
    wait_for_disconnect,
};
use lifecycle::wait_for_shutdown;
use stream::EndpointStream;

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
    tasks: Mutex<Vec<JoinHandle<Result<(), EndpointError>>>>,
    #[cfg(feature = "tls")]
    security: Option<EndpointSecurity>,
    connect_locks: Mutex<HashMap<PeerConnectKey, Arc<AsyncMutex<()>>>>,
    bootstrap_handler: RwLock<Arc<dyn BootstrapHandler>>,
}

type PeerConnectKey = (ClusterId, NodeAddress, NodeIncarnation);

/// Serializes concurrent dials of one exact peer without serializing unrelated peers.
struct PeerConnectLease {
    endpoint: Arc<RemotingEndpoint>,
    key: PeerConnectKey,
    lock: Arc<AsyncMutex<()>>,
}

impl PeerConnectLease {
    fn acquire(endpoint: &Arc<RemotingEndpoint>, peer: &NodeIdentity) -> Self {
        let key = (
            peer.cluster_id.clone(),
            peer.address.clone(),
            peer.incarnation,
        );
        let lock = endpoint
            .connect_locks
            .lock()
            .expect("endpoint connect lock registry poisoned")
            .entry(key.clone())
            .or_default()
            .clone();
        Self {
            endpoint: endpoint.clone(),
            key,
            lock,
        }
    }
}

impl Drop for PeerConnectLease {
    fn drop(&mut self) {
        let mut locks = self
            .endpoint
            .connect_locks
            .lock()
            .expect("endpoint connect lock registry poisoned");
        if Arc::strong_count(&self.lock) == 2 {
            locks.remove(&self.key);
        }
    }
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
        let lease = PeerConnectLease::acquire(self, &peer);
        self.connect_peer_single_flight(&lease.lock, &peer, &mut shutdown)
            .await
    }

    async fn connect_peer_single_flight(
        self: &Arc<Self>,
        peer_lock: &AsyncMutex<()>,
        peer: &NodeIdentity,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<Arc<Association>, EndpointError> {
        let _connection_guard = tokio::select! {
            biased;
            () = wait_for_shutdown(shutdown) => {
                return Err(EndpointError::ShuttingDown);
            }
            guard = peer_lock.lock() => guard,
        };
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
                () = wait_for_shutdown(shutdown) => Err(EndpointError::ShuttingDown),
                result = self.request_reverse_peer(peer.clone()) => result,
            };
        }
        let association = self.associations.get_or_create(
            peer.cluster_id.clone(),
            peer.address.clone(),
            peer.incarnation,
        )?;
        for lane in self.lanes() {
            if association.lane_receiver_available(lane) {
                self.connect_lane(association.clone(), peer.clone(), lane)
                    .await?;
            }
        }
        tokio::select! {
            biased;
            () = wait_for_shutdown(shutdown) => {
                return Err(EndpointError::ShuttingDown);
            }
            result = tokio::time::timeout(
                self.config.connect_timeout,
                association.wait_until_active(),
            ) => {
                result.map_err(|_| EndpointError::ConnectTimeout)??;
            }
        }
        Ok(association)
    }

    #[cfg(test)]
    fn connect_lock_count(&self) -> usize {
        self.connect_locks
            .lock()
            .expect("endpoint connect lock registry poisoned")
            .len()
    }

    async fn connect_lane(
        self: &Arc<Self>,
        association: Arc<Association>,
        peer: NodeIdentity,
        lane: LaneKind,
    ) -> Result<(), EndpointError> {
        let mut shutdown = self.shutdown_tx.subscribe();
        self.ensure_running()?;
        let permit = self
            .connections
            .clone()
            .try_acquire_owned()
            .map_err(|_| EndpointError::ConnectionLimit)?;
        // The dial attaches the lane, so this task has to already own the lane receiver:
        // an attachment that no running lane can detach would pin the association forever.
        let mut receiver = association
            .take_lane_receiver(lane)
            .ok_or(EndpointError::LaneAlreadyRunning(lane))?;
        let opened = tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown) => Err(EndpointError::ShuttingDown),
            result = self.open_outbound_lane(&association, &peer, lane) => result,
        };
        let (stream, nonce) = match opened {
            Ok(opened) => opened,
            Err(error) => {
                let _ = association.return_lane_receiver(lane, receiver);
                return Err(error);
            }
        };
        let endpoint = self.clone();
        let mut disconnect = self.disconnect_tx.subscribe();
        self.spawn(async move {
            let mut connection_permit = Some(permit);
            let mut current = Some((stream, nonce));
            let mut backoff = endpoint.config.reconnect_backoff_min;
            let attached_at = Instant::now();
            loop {
                let (stream, nonce) = current.take().expect("lane connection is installed");
                let result = endpoint
                    .run_lane_connection(
                        association.clone(),
                        lane,
                        nonce,
                        &mut receiver,
                        stream,
                        &mut shutdown,
                    )
                    .await;
                if *shutdown.borrow() {
                    return Ok(());
                }
                if matches!(result, Ok(LaneExit::QueueClosed)) {
                    return Ok(());
                }
                if matches!(
                    association.state(),
                    AssociationState::Closing | AssociationState::Closed
                ) {
                    return Ok(());
                }
                if matches!(result, Ok(LaneExit::Idle)) && lane != LaneKind::Control {
                    connection_permit.take();
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                        () = wait_for_disconnect(&mut disconnect, association.id()) => {
                            if matches!(association.state(), AssociationState::Closing | AssociationState::Closed) {
                                return Ok(());
                            }
                        }
                        () = association.wait_for_lane_wake(lane) => {}
                    }
                    backoff = endpoint.config.reconnect_backoff_min;
                }
                loop {
                    if !association.has_activated()
                        && attached_at.elapsed() >= endpoint.config.establishing_timeout
                    {
                        endpoint.abandon_association(&association);
                        return Ok(());
                    }
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                        () = tokio::time::sleep(backoff) => {}
                    }
                    let acquired_for_attempt = connection_permit.is_none();
                    if acquired_for_attempt {
                        let Ok(permit) = endpoint.connections.clone().try_acquire_owned() else {
                            backoff = backoff
                                .saturating_mul(2)
                                .min(endpoint.config.reconnect_backoff_max);
                            continue;
                        };
                        connection_permit = Some(permit);
                    }
                    let connection = tokio::select! {
                        biased;
                        () = wait_for_shutdown(&mut shutdown) => return Ok(()),
                        result = endpoint.open_outbound_lane(&association, &peer, lane) => result,
                    };
                    match connection {
                        Ok(connection) => {
                            current = Some(connection);
                            backoff = endpoint.config.reconnect_backoff_min;
                            break;
                        }
                        Err(_) => {
                            if acquired_for_attempt {
                                connection_permit.take();
                            }
                            backoff = backoff
                                .saturating_mul(2)
                                .min(endpoint.config.reconnect_backoff_max);
                        }
                    }
                }
            }
        })?;
        Ok(())
    }

    async fn open_outbound_lane(
        &self,
        association: &Association,
        peer: &NodeIdentity,
        lane: LaneKind,
    ) -> Result<(EndpointStream, u128), EndpointError> {
        tokio::time::timeout(
            self.config.connect_timeout,
            self.open_outbound_lane_inner(association, peer, lane),
        )
        .await
        .map_err(|_| EndpointError::ConnectTimeout)?
    }

    async fn open_outbound_lane_inner(
        &self,
        association: &Association,
        peer: &NodeIdentity,
        lane: LaneKind,
    ) -> Result<(EndpointStream, u128), EndpointError> {
        let codec = FrameCodec::new(self.config.max_frame_size)?;
        #[cfg(feature = "tls")]
        let security = self.security.clone();
        let address = peer.address.clone();
        #[cfg(feature = "tls")]
        let expected_peer = peer.clone();
        #[cfg(feature = "tls")]
        let mut connection = match security {
            Some(security) => connect_tls(
                &address,
                security.server_name,
                security.client,
                &expected_peer,
                codec,
            )
            .await
            .map(|connection| {
                FramedConnection::new(
                    EndpointStream::TlsClient(connection.into_inner()),
                    FrameCodec::new(self.config.max_frame_size)
                        .expect("validated endpoint frame size"),
                )
            }),
            None => connect_tcp(&address, codec).await.map(|connection| {
                FramedConnection::new(
                    EndpointStream::Plain(connection.into_inner()),
                    FrameCodec::new(self.config.max_frame_size)
                        .expect("validated endpoint frame size"),
                )
            }),
        }?;
        #[cfg(not(feature = "tls"))]
        let mut connection = connect_tcp(&address, codec).await.map(|connection| {
            FramedConnection::new(
                EndpointStream::Plain(connection.into_inner()),
                FrameCodec::new(self.config.max_frame_size).expect("validated endpoint frame size"),
            )
        })?;
        let nonce = uuid::Uuid::new_v4().as_u128();
        let handshake = Handshake {
            source: self.local.clone(),
            expected_remote: peer.clone(),
            association_id: association.id(),
            lane,
            connection_nonce: nonce,
            maximum_frame_size: self.config.max_frame_size,
            features: FeatureBits::REQUIRED_V3,
        };
        let peer_catalogue = negotiate_outbound(
            &mut connection,
            &handshake,
            &self.catalogue,
            self.config.max_protocols_per_peer,
        )
        .await?;
        if lane == LaneKind::Control {
            association.install_peer_catalogue(peer_catalogue)?;
        }
        if let Err(error) = association.attach_and_replay(LaneAttachment {
            association_id: association.id(),
            key: association.key().clone(),
            lane,
            connection_nonce: nonce,
        }) {
            association.detach(lane, nonce);
            return Err(error.into());
        }
        Ok((connection.into_inner(), nonce))
    }

    async fn accept_loop(self: Arc<Self>, listener: TcpListener) -> Result<(), EndpointError> {
        let mut shutdown = self.shutdown_tx.subscribe();
        if *shutdown.borrow() {
            return Ok(());
        }
        let mut connections = JoinSet::new();
        let mut accept_backoff = ACCEPT_BACKOFF_MIN;
        loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                completed = connections.join_next(), if !connections.is_empty() => {
                    if let Some(result) = completed {
                        let connection_result = result.map_err(EndpointError::Join)?;
                        observe_connection_result(&connection_result);
                    }
                }
                accepted = listener.accept() => {
                    let (stream, peer) = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => {
                            let recovery = classify_accept_failure(error.kind());
                            self.accept_diagnostics.observe_accept_failure(&error, recovery);
                            if recovery == AcceptRecovery::Fatal {
                                return Err(WireError::Io(error).into());
                            }
                            if recovery == AcceptRecovery::Delayed {
                                tokio::select! {
                                    biased;
                                    changed = shutdown.changed() => {
                                        if changed.is_err() || *shutdown.borrow() {
                                            break;
                                        }
                                    }
                                    () = tokio::time::sleep(accept_backoff) => {}
                                }
                                accept_backoff =
                                    accept_backoff.saturating_mul(2).min(ACCEPT_BACKOFF_MAX);
                            }
                            continue;
                        }
                    };
                    accept_backoff = ACCEPT_BACKOFF_MIN;
                    let Ok(permit) = self.connections.clone().try_acquire_owned() else {
                        drop(stream);
                        self.accept_diagnostics.observe_connection_limit_rejection(peer);
                        continue;
                    };
                    let endpoint = self.clone();
                    connections.spawn(async move {
                        let _permit = permit;
                        endpoint.accept_connection(stream).await
                    });
                }
            }
        }
        // Every connection owns a receiver for the endpoint-wide shutdown watch and can leave
        // cleanly. Let those tasks observe the fence instead of aborting them immediately:
        // aborting synchronously drops their nested async state on a Tokio worker stack.
        while let Some(result) = connections.join_next().await {
            let connection_result = result.map_err(EndpointError::Join)?;
            observe_connection_result(&connection_result);
        }
        Ok(())
    }

    async fn accept_connection(self: Arc<Self>, stream: TcpStream) -> Result<(), EndpointError> {
        let mut shutdown = self.shutdown_tx.subscribe();
        if *shutdown.borrow() {
            return Ok(());
        }
        let validator = HandshakeValidator::new(
            self.local.clone(),
            self.config.max_frame_size,
            self.config.bulk_stripes,
        )?;
        stream.set_nodelay(true).map_err(WireError::Io)?;
        #[cfg(feature = "tls")]
        let (stream, peer_certificate) = if let Some(security) = &self.security {
            let stream = tokio::select! {
                biased;
                () = wait_for_shutdown(&mut shutdown) => return Ok(()),
                result = tokio_rustls::TlsAcceptor::from(security.server.clone()).accept(stream) => {
                    result.map_err(|_| WireError::Tls("server handshake failed"))?
                }
            };
            let certificate = stream
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|certificates| certificates.first())
                .map(|certificate| certificate.as_ref().to_vec())
                .ok_or(WireError::Tls("peer certificate missing"))?;
            (EndpointStream::TlsServer(stream), Some(certificate))
        } else {
            (EndpointStream::Plain(stream), None)
        };
        #[cfg(not(feature = "tls"))]
        let (stream, peer_certificate) = (EndpointStream::Plain(stream), Option::<Vec<u8>>::None);
        let mut connection =
            FramedConnection::new(stream, FrameCodec::new(self.config.max_frame_size)?);
        let first_frame = tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown) => return Ok(()),
            result = connection.read_frame() => result?,
        };
        if first_frame.kind == FrameKind::BootstrapRequest {
            return self
                .accept_bootstrap(connection, peer_certificate.as_deref(), first_frame)
                .await;
        }
        let (handshake, peer_catalogue) = tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown) => return Ok(()),
            result = negotiate_inbound_from_frame(
                &mut connection,
                first_frame,
                &validator,
                &self.catalogue,
                self.config.max_protocols_per_peer,
            ) => result?,
        };
        #[cfg(feature = "tls")]
        {
            if let Some(certificate) = peer_certificate {
                verify_peer_certificate_identity(&certificate, &handshake.source)?;
            }
        }
        if self
            .associations
            .should_dial(&handshake.source.address, handshake.source.incarnation)
        {
            return Err(EndpointError::WrongDialDirection);
        }
        let association = self.associations.get_or_accept(
            handshake.source.cluster_id.clone(),
            handshake.source.address.clone(),
            handshake.source.incarnation,
            handshake.association_id,
        )?;
        if handshake.lane == LaneKind::Control {
            association.install_peer_catalogue(peer_catalogue)?;
        }
        // Overlapping inbound connections for one lane are routine after a peer unfreezes,
        // because every dial it made while frozen is still queued in the accept backlog.
        // Only the connection that claims the lane receiver may attach, so a loser can
        // never leave behind an attachment that no running lane will ever detach.
        let mut receiver = association
            .attach_owned_lane(LaneAttachment {
                association_id: handshake.association_id,
                key: association.key().clone(),
                lane: handshake.lane,
                connection_nonce: handshake.connection_nonce,
            })
            .map_err(|error| match error {
                AssociationError::LaneReceiverConflict => {
                    EndpointError::LaneAlreadyRunning(handshake.lane)
                }
                error => EndpointError::Association(error),
            })?;
        let result = self
            .run_lane_connection(
                association.clone(),
                handshake.lane,
                handshake.connection_nonce,
                &mut receiver,
                connection.into_inner(),
                &mut shutdown,
            )
            .await;
        association.return_lane_receiver(handshake.lane, receiver)?;
        result?;
        Ok(())
    }

    fn lanes(&self) -> impl Iterator<Item = LaneKind> {
        [LaneKind::Control, LaneKind::Interactive]
            .into_iter()
            .chain((0..self.config.bulk_stripes).map(|index| LaneKind::Bulk(index as u8)))
    }

    async fn run_lane_connection(
        &self,
        association: Arc<Association>,
        lane: LaneKind,
        nonce: u128,
        receiver: &mut Receiver<Frame>,
        stream: EndpointStream,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<LaneExit, LaneError> {
        let association_id = association.id();
        let mut disconnect = self.disconnect_tx.subscribe();
        tokio::select! {
            result = BidirectionalLane::new(
                association.clone(),
                lane,
                nonce,
                LaneServices::new(
                    self.messaging.clone(),
                    self.dispatch.clone(),
                    self.control_dispatch.clone(),
                ),
                self.lane_config(),
            ).run(receiver, stream, shutdown) => result,
            () = wait_for_disconnect(&mut disconnect, association_id) => {
                association.detach(lane, nonce);
                if lane.fails_pending_asks() {
                    self.messaging.fail_association(association_id);
                }
                Ok(LaneExit::RemoteClose)
            }
        }
    }

    fn abandon_association(&self, association: &Association) {
        association.begin_close();
        association.finish_close();
        self.associations
            .remove(association.key(), association.id());
    }
}

#[derive(Debug, Error)]
pub enum EndpointError {
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
