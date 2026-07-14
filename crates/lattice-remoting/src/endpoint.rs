use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use thiserror::Error;
use tokio::sync::{Semaphore, broadcast, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::association::{
    Association, AssociationError, AssociationId, AssociationManager, LaneAttachment, LaneKind,
};
use crate::bootstrap::{
    AcceptBootstrap, BootstrapError, BootstrapHandler, BootstrapProbeTarget,
    BootstrapRejectionCode, BootstrapRequest, BootstrapResponse, BootstrapResult, BootstrapRoute,
};
use crate::config::RemotingConfig;
use crate::control::{ControlDispatch, RejectControlDispatch};
use crate::handshake::{FeatureBits, Handshake, HandshakeValidator, NodeIdentity};
use crate::lane::{BidirectionalLaneConfig, LaneExit, run_bidirectional_lane};
use crate::messaging::inbound::InboundDispatch;
use crate::messaging::outbound::OutboundMessaging;
use crate::protocol::ProtocolDescriptor;
use crate::transport::{
    FramedConnection, NegotiationError, bind_tcp, connect_tcp, connect_tls, connect_tls_candidate,
    negotiate_inbound_from_frame, negotiate_outbound, verify_peer_certificate_identity,
};
use crate::wire::{Frame, FrameCodec, WireError};

pub struct RemotingEndpoint {
    local: NodeIdentity,
    config: RemotingConfig,
    associations: Arc<AssociationManager>,
    messaging: Arc<OutboundMessaging>,
    dispatch: Arc<dyn InboundDispatch>,
    control_dispatch: Arc<dyn ControlDispatch>,
    catalogue: Vec<ProtocolDescriptor>,
    connections: Arc<Semaphore>,
    shutdown_tx: watch::Sender<bool>,
    disconnect_tx: broadcast::Sender<AssociationId>,
    tasks: Mutex<Vec<JoinHandle<Result<(), EndpointError>>>>,
    security: Option<EndpointSecurity>,
    connect_lock: tokio::sync::Mutex<()>,
    bootstrap_handler: std::sync::RwLock<Arc<dyn BootstrapHandler>>,
}

#[derive(Clone)]
pub struct EndpointSecurity {
    pub client: Arc<tokio_rustls::rustls::ClientConfig>,
    pub server: Arc<tokio_rustls::rustls::ServerConfig>,
    pub server_name: String,
}

enum EndpointStream {
    Plain(tokio::net::TcpStream),
    TlsClient(tokio_rustls::client::TlsStream<tokio::net::TcpStream>),
    TlsServer(tokio_rustls::server::TlsStream<tokio::net::TcpStream>),
}

impl tokio::io::AsyncRead for EndpointStream {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(context, buffer),
            Self::TlsClient(stream) => Pin::new(stream).poll_read(context, buffer),
            Self::TlsServer(stream) => Pin::new(stream).poll_read(context, buffer),
        }
    }
}

impl tokio::io::AsyncWrite for EndpointStream {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(context, buffer),
            Self::TlsClient(stream) => Pin::new(stream).poll_write(context, buffer),
            Self::TlsServer(stream) => Pin::new(stream).poll_write(context, buffer),
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(context),
            Self::TlsClient(stream) => Pin::new(stream).poll_flush(context),
            Self::TlsServer(stream) => Pin::new(stream).poll_flush(context),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(context),
            Self::TlsClient(stream) => Pin::new(stream).poll_shutdown(context),
            Self::TlsServer(stream) => Pin::new(stream).poll_shutdown(context),
        }
    }
}

impl RemotingEndpoint {
    pub fn new(
        local: NodeIdentity,
        config: RemotingConfig,
        associations: Arc<AssociationManager>,
        messaging: Arc<OutboundMessaging>,
        dispatch: Arc<dyn InboundDispatch>,
        catalogue: Vec<ProtocolDescriptor>,
    ) -> Result<Self, EndpointError> {
        Self::new_with_control(
            local,
            config,
            associations,
            messaging,
            dispatch,
            Arc::new(RejectControlDispatch),
            catalogue,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_control(
        local: NodeIdentity,
        config: RemotingConfig,
        associations: Arc<AssociationManager>,
        messaging: Arc<OutboundMessaging>,
        dispatch: Arc<dyn InboundDispatch>,
        control_dispatch: Arc<dyn ControlDispatch>,
        catalogue: Vec<ProtocolDescriptor>,
    ) -> Result<Self, EndpointError> {
        Self::new_with_control_and_security(
            local,
            config,
            associations,
            messaging,
            dispatch,
            control_dispatch,
            catalogue,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_control_and_security(
        local: NodeIdentity,
        config: RemotingConfig,
        associations: Arc<AssociationManager>,
        messaging: Arc<OutboundMessaging>,
        dispatch: Arc<dyn InboundDispatch>,
        control_dispatch: Arc<dyn ControlDispatch>,
        catalogue: Vec<ProtocolDescriptor>,
        security: Option<EndpointSecurity>,
    ) -> Result<Self, EndpointError> {
        config.validate().map_err(AssociationError::InvalidConfig)?;
        if security
            .as_ref()
            .is_some_and(|security| security.server_name.is_empty())
        {
            return Err(EndpointError::InvalidSecurity);
        }
        if catalogue.len() > config.max_protocols_per_peer {
            return Err(EndpointError::ProtocolLimit);
        }
        let connection_limit = config.required_socket_budget().saturating_sub(1);
        let (shutdown_tx, _) = watch::channel(false);
        let (disconnect_tx, _) = broadcast::channel(config.max_associations);
        Ok(Self {
            local,
            config,
            associations,
            messaging,
            dispatch,
            control_dispatch,
            catalogue,
            connections: Arc::new(Semaphore::new(connection_limit)),
            shutdown_tx,
            disconnect_tx,
            tasks: Mutex::new(Vec::new()),
            security,
            connect_lock: tokio::sync::Mutex::new(()),
            bootstrap_handler: std::sync::RwLock::new(Arc::new(AcceptBootstrap)),
        })
    }

    pub fn local_identity(&self) -> &NodeIdentity {
        &self.local
    }

    pub fn install_bootstrap_handler(&self, handler: Arc<dyn BootstrapHandler>) {
        *self
            .bootstrap_handler
            .write()
            .expect("bootstrap handler lock poisoned") = handler;
    }

    pub async fn bind(self: &Arc<Self>) -> Result<(), EndpointError> {
        let listener = bind_tcp(&self.local.address).await?;
        let endpoint = self.clone();
        self.spawn(async move { endpoint.accept_loop(listener).await })?;
        Ok(())
    }

    pub async fn connect_peer(
        self: &Arc<Self>,
        peer: NodeIdentity,
    ) -> Result<Arc<Association>, EndpointError> {
        let _connection_guard = self.connect_lock.lock().await;
        if !self
            .associations
            .should_dial(&peer.address, peer.incarnation)
        {
            return Err(EndpointError::WrongDialDirection);
        }
        let association = self.associations.get_or_create(
            peer.cluster_id.clone(),
            peer.address.clone(),
            peer.incarnation,
        )?;
        if association.state() == crate::association::AssociationState::Active {
            return Ok(association);
        }
        for lane in self.lanes() {
            self.connect_lane(association.clone(), peer.clone(), lane)
                .await?;
        }
        Ok(association)
    }

    pub async fn probe_candidate(
        self: &Arc<Self>,
        target: BootstrapProbeTarget,
    ) -> Result<BootstrapResponse, EndpointError> {
        if target
            .expected_node_id
            .as_ref()
            .is_some_and(String::is_empty)
            || target
                .tls_server_name
                .as_ref()
                .is_some_and(String::is_empty)
        {
            return Err(EndpointError::InvalidBootstrapTarget);
        }
        let permit = self
            .connections
            .clone()
            .try_acquire_owned()
            .map_err(|_| EndpointError::ConnectionLimit)?;
        let result = tokio::time::timeout(
            self.config.connect_timeout,
            self.probe_candidate_inner(target),
        )
        .await
        .map_err(|_| EndpointError::ConnectTimeout)?;
        drop(permit);
        result
    }

    async fn probe_candidate_inner(
        &self,
        target: BootstrapProbeTarget,
    ) -> Result<BootstrapResponse, EndpointError> {
        let codec = FrameCodec::new(self.config.max_frame_size)?;
        let (mut connection, peer_certificate) = match &self.security {
            Some(security) => {
                let server_name = target
                    .tls_server_name
                    .clone()
                    .unwrap_or_else(|| security.server_name.clone());
                let (connection, certificate) = connect_tls_candidate(
                    &target.address,
                    server_name,
                    security.client.clone(),
                    codec,
                )
                .await?;
                (
                    FramedConnection::new(
                        EndpointStream::TlsClient(connection.into_inner()),
                        FrameCodec::new(self.config.max_frame_size)?,
                    ),
                    Some(certificate),
                )
            }
            None => (
                FramedConnection::new(
                    EndpointStream::Plain(connect_tcp(&target.address, codec).await?.into_inner()),
                    FrameCodec::new(self.config.max_frame_size)?,
                ),
                None,
            ),
        };
        let request = BootstrapRequest::new(
            self.local.clone(),
            self.local.cluster_id.clone(),
            target.expected_node_id,
        );
        connection.write_frame(&request.to_frame()).await?;
        connection.flush().await?;
        let response = BootstrapResponse::from_frame(&connection.read_frame().await?)?;
        response.validate_for(&request)?;
        if let (Some(certificate), Some(remote)) =
            (peer_certificate.as_deref(), response.remote_identity())
        {
            verify_peer_certificate_identity(certificate, remote)?;
        }
        connection.close().await?;
        Ok(response)
    }

    pub async fn shutdown(&self) -> Result<(), EndpointError> {
        let _ = self.shutdown_tx.send(true);
        lattice_core::failpoint::hit(
            lattice_core::failpoint::Failpoint::ShutdownAfterFenceBeforeTaskJoin,
        );
        let tasks = {
            let mut tasks = self.tasks.lock().expect("endpoint task list poisoned");
            std::mem::take(&mut *tasks)
        };
        let deadline = tokio::time::Instant::now() + self.config.shutdown_timeout;
        let mut timed_out = false;
        for mut task in tasks {
            match tokio::time::timeout_at(deadline, &mut task).await {
                Ok(Ok(result)) => result?,
                Ok(Err(error)) if error.is_cancelled() => {}
                Ok(Err(error)) => return Err(EndpointError::Join(error)),
                Err(_) => {
                    timed_out = true;
                    task.abort();
                    let _ = task.await;
                }
            }
        }
        if timed_out {
            Err(EndpointError::ShutdownTimeout)
        } else {
            Ok(())
        }
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

    async fn connect_lane(
        self: &Arc<Self>,
        association: Arc<Association>,
        peer: NodeIdentity,
        lane: LaneKind,
    ) -> Result<(), EndpointError> {
        let permit = self
            .connections
            .clone()
            .try_acquire_owned()
            .map_err(|_| EndpointError::ConnectionLimit)?;
        let (stream, nonce) = self.open_outbound_lane(&association, &peer, lane).await?;
        let mut receiver = association
            .take_lane_receiver(lane)
            .ok_or(EndpointError::LaneAlreadyRunning(lane))?;
        let endpoint = self.clone();
        let mut shutdown = self.shutdown_tx.subscribe();
        self.spawn(async move {
            let _permit = permit;
            let mut current = Some((stream, nonce));
            let mut backoff = endpoint.config.reconnect_backoff_min;
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
                if matches!(result, Ok(crate::lane::LaneExit::QueueClosed)) {
                    return Ok(());
                }
                loop {
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                        () = tokio::time::sleep(backoff) => {}
                    }
                    match endpoint.open_outbound_lane(&association, &peer, lane).await {
                        Ok(connection) => {
                            current = Some(connection);
                            backoff = endpoint.config.reconnect_backoff_min;
                            break;
                        }
                        Err(_) => {
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
        let codec = FrameCodec::new(self.config.max_frame_size)?;
        let security = self.security.clone();
        let address = peer.address.clone();
        let expected_peer = peer.clone();
        let mut connection = tokio::time::timeout(self.config.connect_timeout, async move {
            match security {
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
            }
        })
        .await
        .map_err(|_| EndpointError::ConnectTimeout)??;
        let nonce = uuid::Uuid::new_v4().as_u128();
        let handshake = Handshake {
            source: self.local.clone(),
            expected_remote: peer.clone(),
            association_id: association.id(),
            lane,
            connection_nonce: nonce,
            maximum_frame_size: self.config.max_frame_size,
            features: FeatureBits::REQUIRED_V1,
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
        association.attach(LaneAttachment {
            association_id: association.id(),
            key: association.key().clone(),
            lane,
            connection_nonce: nonce,
        })?;
        if lane == LaneKind::Control
            && association.state() == crate::association::AssociationState::Active
        {
            for frame in association.replay_control_frames() {
                association.try_admit_control(frame)?;
            }
        }
        Ok((connection.into_inner(), nonce))
    }

    async fn accept_loop(
        self: Arc<Self>,
        listener: tokio::net::TcpListener,
    ) -> Result<(), EndpointError> {
        let mut shutdown = self.shutdown_tx.subscribe();
        let mut connections = JoinSet::new();
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
                    let (stream, _) = accepted.map_err(WireError::Io)?;
                    let permit = self.connections.clone().try_acquire_owned()
                        .map_err(|_| EndpointError::ConnectionLimit)?;
                    let endpoint = self.clone();
                    connections.spawn(async move {
                        let _permit = permit;
                        endpoint.accept_connection(stream).await
                    });
                }
            }
        }
        while let Some(result) = connections.join_next().await {
            let connection_result = result.map_err(EndpointError::Join)?;
            observe_connection_result(&connection_result);
        }
        Ok(())
    }

    async fn accept_connection(
        self: Arc<Self>,
        stream: tokio::net::TcpStream,
    ) -> Result<(), EndpointError> {
        let validator = HandshakeValidator::new(
            self.local.clone(),
            self.config.max_frame_size,
            self.config.bulk_stripes,
        )?;
        stream.set_nodelay(true).map_err(WireError::Io)?;
        let (stream, peer_certificate) = if let Some(security) = &self.security {
            let stream = tokio_rustls::TlsAcceptor::from(security.server.clone())
                .accept(stream)
                .await
                .map_err(|_| WireError::Tls("server handshake failed"))?;
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
        let mut connection =
            FramedConnection::new(stream, FrameCodec::new(self.config.max_frame_size)?);
        let first_frame = connection.read_frame().await?;
        if first_frame.kind == crate::wire::FrameKind::BootstrapRequest {
            return self
                .accept_bootstrap(connection, peer_certificate.as_deref(), first_frame)
                .await;
        }
        let (handshake, peer_catalogue) = negotiate_inbound_from_frame(
            &mut connection,
            first_frame,
            &validator,
            &self.catalogue,
            self.config.max_protocols_per_peer,
        )
        .await?;
        if let Some(certificate) = peer_certificate {
            verify_peer_certificate_identity(&certificate, &handshake.source)?;
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
        association.attach(LaneAttachment {
            association_id: handshake.association_id,
            key: association.key().clone(),
            lane: handshake.lane,
            connection_nonce: handshake.connection_nonce,
        })?;
        if handshake.lane == LaneKind::Control
            && association.state() == crate::association::AssociationState::Active
        {
            for frame in association.replay_control_frames() {
                association.try_admit_control(frame)?;
            }
        }
        let mut receiver = association
            .take_lane_receiver(handshake.lane)
            .ok_or(EndpointError::LaneAlreadyRunning(handshake.lane))?;
        let mut shutdown = self.shutdown_tx.subscribe();
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

    async fn accept_bootstrap(
        self: Arc<Self>,
        mut connection: FramedConnection<EndpointStream>,
        peer_certificate: Option<&[u8]>,
        first_frame: Frame,
    ) -> Result<(), EndpointError> {
        let request = BootstrapRequest::from_frame(&first_frame)?;
        let mut response = if let Some(code) = request.rejection(&self.local) {
            BootstrapResponse::rejected(request.nonce, code)
        } else if peer_certificate.is_some_and(|certificate| {
            verify_peer_certificate_identity(certificate, &request.local).is_err()
        }) {
            BootstrapResponse::rejected(
                request.nonce,
                BootstrapRejectionCode::AuthenticationFailure,
            )
        } else {
            self.bootstrap_response(&request)
        };
        if response.validate_for(&request).is_err() {
            response = BootstrapResponse::new(
                request.nonce,
                BootstrapResult::RetryAfter {
                    delay: std::time::Duration::from_secs(1),
                    reason: "bootstrap route is temporarily unavailable".to_string(),
                },
            );
        }
        let reverse_peer = match &response.result {
            BootstrapResult::ReverseDial { .. } => Some(request.local.clone()),
            _ => None,
        };
        connection.write_frame(&response.to_frame()).await?;
        connection.flush().await?;
        connection.close().await?;
        if let Some(peer) = reverse_peer {
            let endpoint = self.clone();
            self.spawn(async move {
                let _result = endpoint.connect_peer(peer).await;
                Ok(())
            })?;
        }
        Ok(())
    }

    fn bootstrap_response(&self, request: &BootstrapRequest) -> BootstrapResponse {
        let route = self
            .bootstrap_handler
            .read()
            .expect("bootstrap handler lock poisoned")
            .route(request);
        let result = match route {
            BootstrapRoute::Accept { leader } => {
                if self
                    .associations
                    .should_dial(&request.local.address, request.local.incarnation)
                {
                    BootstrapResult::ReverseDial {
                        remote: self.local.clone(),
                        leader,
                    }
                } else {
                    BootstrapResult::Identity {
                        remote: self.local.clone(),
                        leader,
                    }
                }
            }
            BootstrapRoute::Redirect { leader } => BootstrapResult::Redirect {
                remote: self.local.clone(),
                leader,
            },
            BootstrapRoute::RetryAfter { delay, reason } => {
                BootstrapResult::RetryAfter { delay, reason }
            }
            BootstrapRoute::Reject { code } => BootstrapResult::Rejected { code },
        };
        BootstrapResponse::new(request.nonce, result)
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
        receiver: &mut tokio::sync::mpsc::Receiver<Frame>,
        stream: EndpointStream,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<LaneExit, crate::lane::LaneError> {
        let association_id = association.id();
        let mut disconnect = self.disconnect_tx.subscribe();
        let result = tokio::select! {
            result = run_bidirectional_lane(
                association.clone(),
                lane,
                nonce,
                receiver,
                stream,
                self.messaging.clone(),
                self.dispatch.clone(),
                self.control_dispatch.clone(),
                self.lane_config(),
                shutdown,
            ) => result,
            () = wait_for_disconnect(&mut disconnect, association_id) => {
                Ok(LaneExit::RemoteClose)
            }
        };
        association.detach(lane, nonce);
        self.messaging.fail_association(association_id);
        result
    }

    fn lane_config(&self) -> BidirectionalLaneConfig {
        BidirectionalLaneConfig {
            maximum_frame_size: self.config.max_frame_size,
            maximum_concurrent_inbound_asks: self.config.max_pending_asks,
            heartbeat_interval: self.config.heartbeat_interval,
            heartbeat_miss_limit: self.config.heartbeat_miss_limit,
            idle_data_connection_timeout: self.config.idle_data_connection_timeout,
        }
    }

    fn spawn<F>(self: &Arc<Self>, future: F) -> Result<(), EndpointError>
    where
        F: Future<Output = Result<(), EndpointError>> + Send + 'static,
    {
        let mut tasks = self.tasks.lock().expect("endpoint task list poisoned");
        tasks.retain(|task| !task.is_finished());
        if tasks.len() >= self.config.required_socket_budget() {
            return Err(EndpointError::TaskLimit);
        }
        tasks.push(tokio::spawn(future));
        Ok(())
    }
}

fn observe_connection_result(result: &Result<(), EndpointError>) {
    static FAILURES: AtomicU64 = AtomicU64::new(0);
    let Err(error) = result else {
        return;
    };
    let count = FAILURES.fetch_add(1, Ordering::Relaxed).saturating_add(1);
    if count == 1 || count.is_multiple_of(100) {
        tracing::warn!(
            connection_failure_count = count,
            error = ?error,
            "inbound remoting connection task failed (subsequent failures are aggregated)"
        );
    }
}

use std::future::Future;

#[derive(Debug, Error)]
pub enum EndpointError {
    #[error("association endpoint failed")]
    Association(#[from] AssociationError),
    #[error("association endpoint wire failed")]
    Wire(#[from] WireError),
    #[error("association endpoint negotiation failed")]
    Negotiation(#[from] NegotiationError),
    #[error("association endpoint handshake failed")]
    Handshake(#[from] crate::handshake::HandshakeError),
    #[error("association lane failed")]
    Lane(#[from] crate::lane::LaneError),
    #[error("only the stable lower node identity may dial")]
    WrongDialDirection,
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
    #[error("association endpoint task failed")]
    Join(#[source] tokio::task::JoinError),
    #[error("association endpoint shutdown timed out")]
    ShutdownTimeout,
    #[error("association endpoint has no active connections")]
    NoActiveConnections,
    #[error("association endpoint bootstrap protocol failed")]
    Bootstrap(#[from] BootstrapError),
    #[error("bootstrap probe target is invalid")]
    InvalidBootstrapTarget,
}

async fn wait_for_disconnect(
    receiver: &mut broadcast::Receiver<AssociationId>,
    association_id: AssociationId,
) {
    loop {
        match receiver.recv().await {
            Ok(received) if received == association_id => return,
            Ok(_) => {}
            Err(broadcast::error::RecvError::Lagged(_))
            | Err(broadcast::error::RecvError::Closed) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bytes::Bytes;
    use lattice_core::actor_ref::{
        ActivationId, ActorPath, ActorRef, ClusterId, NodeAddress, NodeIncarnation, ProtocolId,
    };
    use std::time::{Duration, Instant};

    use crate::association::AssociationKey;
    use crate::control::{CommandId, ControlDispatchError, ControlGap};
    use crate::messaging::error::RemoteMessageError;
    use crate::messaging::target::{ExactActorTarget, SenderIdentity};
    use crate::protocol::ProtocolFingerprint;

    struct EchoDispatch;

    #[derive(Default)]
    struct RecordingControl {
        applied: Mutex<Vec<Bytes>>,
    }

    #[async_trait]
    impl ControlDispatch for RecordingControl {
        async fn apply(
            &self,
            _association: AssociationKey,
            _command_id: CommandId,
            payload: Bytes,
        ) -> Result<(), ControlDispatchError> {
            self.applied
                .lock()
                .expect("recording control poisoned")
                .push(payload);
            Ok(())
        }

        async fn reconcile(
            &self,
            _association: AssociationKey,
            _gap: Option<ControlGap>,
        ) -> Result<(), ControlDispatchError> {
            Ok(())
        }
    }

    #[async_trait]
    impl InboundDispatch for EchoDispatch {
        async fn tell(
            &self,
            _sender: Option<ActorRef>,
            _target: ExactActorTarget,
            _message_id: u64,
            _payload: Bytes,
        ) -> Result<(), RemoteMessageError> {
            Ok(())
        }

        async fn ask(
            &self,
            _target: ExactActorTarget,
            _message_id: u64,
            payload: Bytes,
            deadline: Instant,
        ) -> Result<Bytes, RemoteMessageError> {
            if Instant::now() >= deadline {
                return Err(RemoteMessageError::DeadlineExceeded);
            }
            Ok(payload)
        }
    }

    fn endpoint(identity: NodeIdentity, protocol: ProtocolDescriptor) -> Arc<RemotingEndpoint> {
        endpoint_with_control(identity, protocol, Arc::new(RejectControlDispatch))
    }

    fn endpoint_with_control(
        identity: NodeIdentity,
        protocol: ProtocolDescriptor,
        control: Arc<dyn ControlDispatch>,
    ) -> Arc<RemotingEndpoint> {
        let config = RemotingConfig {
            heartbeat_interval: Duration::from_millis(100),
            shutdown_timeout: Duration::from_secs(2),
            ..RemotingConfig::default()
        };
        let manager = Arc::new(
            AssociationManager::new(
                identity.address.clone(),
                identity.incarnation,
                config.clone(),
            )
            .unwrap(),
        );
        Arc::new(
            RemotingEndpoint::new_with_control(
                identity,
                config,
                manager,
                Arc::new(OutboundMessaging::new(32).unwrap()),
                Arc::new(EchoDispatch),
                control,
                vec![protocol],
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn real_tcp_endpoint_establishes_all_lanes_and_delivers_ask() {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_port = probe.local_addr().unwrap().port();
        drop(probe);
        let client_port = server_port.saturating_sub(1).max(1024);
        let cluster_id = ClusterId::new("endpoint-test").unwrap();
        let client_identity = NodeIdentity {
            cluster_id: cluster_id.clone(),
            node_id: "client".to_owned(),
            address: NodeAddress::new("127.0.0.1", client_port).unwrap(),
            incarnation: NodeIncarnation::new(1).unwrap(),
        };
        let server_identity = NodeIdentity {
            cluster_id: cluster_id.clone(),
            node_id: "server".to_owned(),
            address: NodeAddress::new("127.0.0.1", server_port).unwrap(),
            incarnation: NodeIncarnation::new(2).unwrap(),
        };
        assert!(
            (&client_identity.address, client_identity.incarnation.get())
                < (&server_identity.address, server_identity.incarnation.get())
        );
        let protocol_id = ProtocolId::new(7).unwrap();
        let fingerprint = ProtocolFingerprint::digest(b"endpoint-test/v1");
        let descriptor = ProtocolDescriptor {
            protocol_id,
            fingerprint,
        };
        let control = Arc::new(RecordingControl::default());
        let client = endpoint(client_identity.clone(), descriptor.clone());
        let server =
            endpoint_with_control(server_identity.clone(), descriptor.clone(), control.clone());
        server.bind().await.unwrap();
        let association = client.connect_peer(server_identity.clone()).await.unwrap();
        assert_eq!(
            association.state(),
            crate::association::AssociationState::Active
        );
        let target = ActorRef::new(
            cluster_id,
            server_identity.address.clone(),
            server_identity.incarnation,
            ActorPath::user(["user", "echo"]).unwrap(),
            ActivationId::new(server_identity.incarnation, 1).unwrap(),
            protocol_id,
        )
        .unwrap();
        let reply = client
            .messaging
            .ask(
                &association,
                &SenderIdentity::Process(9),
                &target,
                fingerprint,
                1,
                Bytes::from_static(b"hello"),
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(reply, Bytes::from_static(b"hello"));
        association
            .admit_control_command(Bytes::from_static(b"before-reconnect"))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while association.control_outbox_len() != 0
                || control
                    .applied
                    .lock()
                    .expect("recording control poisoned")
                    .len()
                    != 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        server.disconnect_association(association.id()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while association.state() != crate::association::AssociationState::Reconnecting {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while association.state() != crate::association::AssociationState::Active {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let reply = client
            .messaging
            .ask(
                &association,
                &SenderIdentity::Process(9),
                &target,
                fingerprint,
                1,
                Bytes::from_static(b"again"),
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(reply, Bytes::from_static(b"again"));
        association
            .admit_control_command(Bytes::from_static(b"after-reconnect"))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while association.control_outbox_len() != 0
                || control
                    .applied
                    .lock()
                    .expect("recording control poisoned")
                    .len()
                    != 2
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        client.shutdown().await.unwrap();
        server.shutdown().await.unwrap();
    }
}
