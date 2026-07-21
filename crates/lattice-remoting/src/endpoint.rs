use std::{
    io::ErrorKind,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use broadcast::error::RecvError;
use thiserror::Error;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Mutex as AsyncMutex, Semaphore, broadcast, mpsc::Receiver, watch},
    task::{JoinError, JoinHandle, JoinSet},
};
use tokio_rustls::rustls::{ClientConfig, ServerConfig};

use crate::{
    association::{
        Association, AssociationError, AssociationId, AssociationManager, AssociationState,
        LaneAttachment, LaneKind,
    },
    bootstrap::{
        AcceptBootstrap, BootstrapError, BootstrapHandler, BootstrapProbeTarget, BootstrapPurpose,
        BootstrapRejectionCode, BootstrapRequest, BootstrapResponse, BootstrapResult,
        BootstrapRoute,
    },
    config::RemotingConfig,
    control::{ControlDispatch, RejectControlDispatch},
    handshake::{FeatureBits, Handshake, HandshakeError, HandshakeValidator, NodeIdentity},
    lane::{BidirectionalLane, BidirectionalLaneConfig, LaneError, LaneExit, LaneServices},
    messaging::{inbound::InboundDispatch, outbound::OutboundMessaging},
    protocol::ProtocolDescriptor,
    transport::{
        FramedConnection, NegotiationError, bind_tcp, connect_tcp, connect_tls,
        connect_tls_candidate, negotiate_inbound_from_frame, negotiate_outbound,
        verify_peer_certificate_identity,
    },
    wire::{Frame, FrameCodec, FrameKind, WireError},
};

mod lifecycle;
mod reverse_dial;
mod stream;

use lifecycle::wait_for_shutdown;
use stream::EndpointStream;

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
    connect_lock: AsyncMutex<()>,
    bootstrap_handler: RwLock<Arc<dyn BootstrapHandler>>,
}

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
    security: Option<EndpointSecurity>,
}

impl RemotingEndpointBuilder {
    pub fn control_dispatch(mut self, control_dispatch: Arc<dyn ControlDispatch>) -> Self {
        self.control_dispatch = control_dispatch;
        self
    }

    pub fn catalogue(mut self, catalogue: Vec<ProtocolDescriptor>) -> Self {
        self.catalogue = catalogue;
        self
    }

    pub fn security(mut self, security: EndpointSecurity) -> Self {
        self.security = Some(security);
        self
    }

    pub fn build(self) -> Result<RemotingEndpoint, EndpointError> {
        self.config
            .validate()
            .map_err(AssociationError::InvalidConfig)?;
        if self
            .security
            .as_ref()
            .is_some_and(|security| security.server_name.is_empty())
        {
            return Err(EndpointError::InvalidSecurity);
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
        let _connection_guard = tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown) => {
                return Err(EndpointError::ShuttingDown);
            }
            guard = self.connect_lock.lock() => guard,
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
                () = wait_for_shutdown(&mut shutdown) => Err(EndpointError::ShuttingDown),
                result = self.request_reverse_peer(peer) => result,
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
            () = wait_for_shutdown(&mut shutdown) => {
                return Err(EndpointError::ShuttingDown);
            }
            result = tokio::time::timeout(self.config.connect_timeout, async {
                while association.state() != AssociationState::Active {
                    tokio::task::yield_now().await;
                }
            }) => {
                result.map_err(|_| EndpointError::ConnectTimeout)?;
            }
        }
        Ok(association)
    }

    pub async fn probe_candidate(
        self: &Arc<Self>,
        target: BootstrapProbeTarget,
    ) -> Result<BootstrapResponse, EndpointError> {
        let mut shutdown = self.shutdown_tx.subscribe();
        self.ensure_running()?;
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
        let result = tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown) => Err(EndpointError::ShuttingDown),
            result = tokio::time::timeout(
                self.config.connect_timeout,
                self.probe_candidate_inner(target),
            ) => result.map_err(|_| EndpointError::ConnectTimeout)?,
        };
        drop(permit);
        result
    }

    async fn probe_candidate_inner(
        &self,
        target: BootstrapProbeTarget,
    ) -> Result<BootstrapResponse, EndpointError> {
        let request = BootstrapRequest::new(
            target.scope,
            self.local.clone(),
            self.local.cluster_id.clone(),
            target.expected_node_id,
        );
        self.probe_request_inner(target.address, target.tls_server_name, request)
            .await
    }

    async fn probe_request_inner(
        &self,
        address: lattice_core::actor_ref::NodeAddress,
        tls_server_name: Option<String>,
        request: BootstrapRequest,
    ) -> Result<BootstrapResponse, EndpointError> {
        let codec = FrameCodec::new(self.config.max_frame_size)?;
        let (mut connection, peer_certificate) = match &self.security {
            Some(security) => {
                let server_name = tls_server_name.unwrap_or_else(|| security.server_name.clone());
                let (connection, certificate) =
                    connect_tls_candidate(&address, server_name, security.client.clone(), codec)
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
                    EndpointStream::Plain(connect_tcp(&address, codec).await?.into_inner()),
                    FrameCodec::new(self.config.max_frame_size)?,
                ),
                None,
            ),
        };
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
        let mut shutdown = self.shutdown_tx.subscribe();
        self.ensure_running()?;
        let permit = self
            .connections
            .clone()
            .try_acquire_owned()
            .map_err(|_| EndpointError::ConnectionLimit)?;
        let (stream, nonce) = tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown) => {
                return Err(EndpointError::ShuttingDown);
            }
            result = self.open_outbound_lane(&association, &peer, lane) => result?,
        };
        let mut receiver = association
            .take_lane_receiver(lane)
            .ok_or(EndpointError::LaneAlreadyRunning(lane))?;
        let endpoint = self.clone();
        let mut disconnect = self.disconnect_tx.subscribe();
        self.spawn(async move {
            let mut connection_permit = Some(permit);
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
                if matches!(result, Ok(LaneExit::QueueClosed)) {
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
        let security = self.security.clone();
        let address = peer.address.clone();
        let expected_peer = peer.clone();
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
        let nonce = uuid::Uuid::new_v4().as_u128();
        let handshake = Handshake {
            source: self.local.clone(),
            expected_remote: peer.clone(),
            association_id: association.id(),
            lane,
            connection_nonce: nonce,
            maximum_frame_size: self.config.max_frame_size,
            features: FeatureBits::REQUIRED_V2,
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
        association.attach_and_replay(LaneAttachment {
            association_id: association.id(),
            key: association.key().clone(),
            lane,
            connection_nonce: nonce,
        })?;
        Ok((connection.into_inner(), nonce))
    }

    async fn accept_loop(self: Arc<Self>, listener: TcpListener) -> Result<(), EndpointError> {
        let mut shutdown = self.shutdown_tx.subscribe();
        if *shutdown.borrow() {
            return Ok(());
        }
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
        connections.shutdown().await;
        Ok(())
    }

    async fn accept_connection(self: Arc<Self>, stream: TcpStream) -> Result<(), EndpointError> {
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
        if first_frame.kind == FrameKind::BootstrapRequest {
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
        association.attach_and_replay(LaneAttachment {
            association_id: handshake.association_id,
            key: association.key().clone(),
            lane: handshake.lane,
            connection_nonce: handshake.connection_nonce,
        })?;
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
                    delay: Duration::from_secs(1),
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
        if request.purpose == BootstrapPurpose::DirectPeer {
            let result = if self
                .associations
                .should_dial(&request.local.address, request.local.incarnation)
            {
                BootstrapResult::ReverseDial {
                    remote: self.local.clone(),
                    leader: None,
                }
            } else {
                BootstrapResult::Identity {
                    remote: self.local.clone(),
                    leader: None,
                }
            };
            return BootstrapResponse::new(request.nonce, result);
        }
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
                self.messaging.fail_association(association_id);
                Ok(LaneExit::RemoteClose)
            }
        }
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
        self.ensure_running()?;
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
    if is_peer_disconnect(error) {
        tracing::debug!(error = ?error, "inbound remoting peer disconnected");
        return;
    }
    let count = FAILURES.fetch_add(1, Ordering::Relaxed).saturating_add(1);
    if count == 1 || count.is_multiple_of(100) {
        tracing::warn!(
            connection_failure_count = count,
            error = ?error,
            "inbound remoting connection task failed (subsequent failures are aggregated)"
        );
    }
}

fn is_peer_disconnect(error: &EndpointError) -> bool {
    let io = match error {
        EndpointError::Wire(WireError::Io(io))
        | EndpointError::Lane(LaneError::Wire(WireError::Io(io))) => io,
        _ => return false,
    };
    matches!(
        io.kind(),
        ErrorKind::UnexpectedEof
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::BrokenPipe
    )
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

async fn wait_for_disconnect(
    receiver: &mut broadcast::Receiver<AssociationId>,
    association_id: AssociationId,
) {
    loop {
        match receiver.recv().await {
            Ok(received) if received == association_id => return,
            Ok(_) => {}
            Err(RecvError::Lagged(_)) | Err(RecvError::Closed) => return,
        }
    }
}

#[cfg(test)]
#[path = "endpoint/idle_tests.rs"]
mod idle_tests;

#[cfg(test)]
mod tests {
    use std::io::{Error, ErrorKind};

    use tokio::net::TcpListener;

    use super::*;
    use crate::{
        association::AssociationState, lane::LaneError, messaging::outbound::OutboundMessage,
    };

    #[test]
    fn classifies_normal_peer_disconnects_without_hiding_protocol_failures() {
        let disconnected = EndpointError::Lane(LaneError::Wire(WireError::Io(Error::from(
            ErrorKind::UnexpectedEof,
        ))));
        assert!(is_peer_disconnect(&disconnected));
        assert!(!is_peer_disconnect(&EndpointError::WrongDialDirection));
    }
    use std::time::{Duration, Instant};

    use async_trait::async_trait;
    use bytes::Bytes;
    use lattice_core::actor_ref::{
        ActivationId, ActorPath, ActorRef, ClusterId, NodeAddress, NodeIncarnation, ProtocolId,
    };

    use crate::{
        association::AssociationKey,
        control::{CommandId, ControlDispatchError, ControlGap},
        messaging::{
            error::RemoteMessageError,
            target::{ExactActorTarget, SenderIdentity},
        },
        protocol::ProtocolFingerprint,
    };

    struct EchoDispatch;

    #[derive(Default)]
    struct RecordingControl {
        applied: Mutex<Vec<Bytes>>,
    }

    #[derive(Default)]
    struct RejectInvalidControl {
        rejected: Mutex<bool>,
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
    impl ControlDispatch for RejectInvalidControl {
        async fn apply(
            &self,
            _association: AssociationKey,
            _command_id: CommandId,
            payload: Bytes,
        ) -> Result<(), ControlDispatchError> {
            if payload == Bytes::from_static(b"invalid") {
                *self.rejected.lock().expect("rejected flag poisoned") = true;
                return Err(ControlDispatchError::InvalidCommand);
            }
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
            RemotingEndpoint::builder(
                identity,
                config,
                manager,
                Arc::new(OutboundMessaging::new(32).unwrap()),
                Arc::new(EchoDispatch),
            )
            .control_dispatch(control)
            .catalogue(vec![protocol])
            .build()
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn real_tcp_endpoint_establishes_all_lanes_and_delivers_ask() {
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
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
        assert_eq!(association.state(), AssociationState::Active);
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
                OutboundMessage::new(fingerprint, 1, Bytes::from_static(b"hello")),
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
            while association.state() != AssociationState::Reconnecting {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while association.state() != AssociationState::Active {
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
                OutboundMessage::new(fingerprint, 1, Bytes::from_static(b"again")),
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

    #[tokio::test]
    async fn invalid_reliable_control_is_acknowledged_without_poisoning_later_commands() {
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_port = probe.local_addr().unwrap().port();
        drop(probe);
        let client_port = server_port.saturating_sub(1).max(1024);
        let cluster_id = ClusterId::new("invalid-control-test").unwrap();
        let client_identity = NodeIdentity {
            cluster_id: cluster_id.clone(),
            node_id: "client".to_owned(),
            address: NodeAddress::new("127.0.0.1", client_port).unwrap(),
            incarnation: NodeIncarnation::new(11).unwrap(),
        };
        let server_identity = NodeIdentity {
            cluster_id,
            node_id: "server".to_owned(),
            address: NodeAddress::new("127.0.0.1", server_port).unwrap(),
            incarnation: NodeIncarnation::new(12).unwrap(),
        };
        let descriptor = ProtocolDescriptor {
            protocol_id: ProtocolId::new(8).unwrap(),
            fingerprint: ProtocolFingerprint::digest(b"invalid-control-test/v1"),
        };
        let control = Arc::new(RejectInvalidControl::default());
        let client = endpoint(client_identity, descriptor.clone());
        let server = endpoint_with_control(server_identity.clone(), descriptor, control.clone());
        server.bind().await.unwrap();
        let association = client.connect_peer(server_identity).await.unwrap();

        association
            .admit_control_command(Bytes::from_static(b"invalid"))
            .unwrap();
        association
            .admit_control_command(Bytes::from_static(b"valid"))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let rejected = *control.rejected.lock().expect("rejected flag poisoned");
                let applied = control
                    .applied
                    .lock()
                    .expect("recording control poisoned")
                    .clone();
                if rejected
                    && applied == [Bytes::from_static(b"valid")]
                    && association.control_outbox_len() == 0
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(association.state(), AssociationState::Active);

        client.shutdown().await.unwrap();
        server.shutdown().await.unwrap();
    }
}
