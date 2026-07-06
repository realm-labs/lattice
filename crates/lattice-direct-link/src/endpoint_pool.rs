use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use lattice_core::{
    DirectLinkEndpoint, DirectLinkOpenRequest, DirectLinkSender, DirectLinkSession,
    LinkCloseReason, LinkDirection, LinkError, LinkId, LinkSendError, LinkSequence, LinkTarget,
    OutboundDirectLinkMessage,
};
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::codec::{DirectLinkFrame, DirectLinkFrameKind};
use crate::session::{DIRECT_LINK_PROTOCOL_VERSION, OpenLinkDirection, OpenLinkRequest};
use crate::transport::{DirectLinkConnection, DirectLinkTransport};

#[derive(Debug, Clone)]
pub struct DirectLinkEndpointPoolConfig {
    pub connections_per_endpoint: NonZeroUsize,
    pub max_links_per_connection: usize,
    pub max_links_per_endpoint: usize,
    pub connect_timeout: Duration,
    pub idle_timeout: Duration,
    pub reconnect_initial_backoff: Duration,
    pub reconnect_max_backoff: Duration,
}

impl Default for DirectLinkEndpointPoolConfig {
    fn default() -> Self {
        Self {
            connections_per_endpoint: NonZeroUsize::new(1).expect("one is non-zero"),
            max_links_per_connection: usize::MAX,
            max_links_per_endpoint: usize::MAX,
            connect_timeout: Duration::from_secs(3),
            idle_timeout: Duration::from_secs(30),
            reconnect_initial_backoff: Duration::from_millis(100),
            reconnect_max_backoff: Duration::from_secs(5),
        }
    }
}

impl DirectLinkEndpointPoolConfig {
    pub fn stripe_index_for_link(&self, link_id: &LinkId) -> usize {
        stable_stripe_index(link_id, self.connections_per_endpoint)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DirectLinkEndpointKey(String);

impl DirectLinkEndpointKey {
    pub fn new(endpoint: &DirectLinkEndpoint) -> Self {
        Self(endpoint.uri.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DirectLinkConnectionId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectLinkConnectionStripe {
    pub endpoint: DirectLinkEndpointKey,
    pub connection_id: DirectLinkConnectionId,
    pub stripe_index: usize,
}

#[derive(Debug, Clone)]
pub struct PooledDirectLinkSession {
    pub connection_id: DirectLinkConnectionId,
    pub endpoint: DirectLinkEndpoint,
    pub session: DirectLinkSession,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DirectLinkEndpointPoolMetricsSnapshot {
    pub physical_connections_opened: u64,
    pub physical_connections_closed: u64,
    pub active_physical_connections: u64,
    pub logical_links_opened: u64,
    pub logical_links_closed: u64,
    pub active_logical_links: u64,
    pub frames_written: u64,
    pub reconnects: u64,
    pub pool_rejections: u64,
}

#[derive(Debug, Default)]
struct DirectLinkEndpointPoolMetrics {
    physical_connections_opened: AtomicU64,
    physical_connections_closed: AtomicU64,
    active_physical_connections: AtomicU64,
    logical_links_opened: AtomicU64,
    logical_links_closed: AtomicU64,
    active_logical_links: AtomicU64,
    frames_written: AtomicU64,
    reconnects: AtomicU64,
    pool_rejections: AtomicU64,
}

impl DirectLinkEndpointPoolMetrics {
    fn snapshot(&self) -> DirectLinkEndpointPoolMetricsSnapshot {
        DirectLinkEndpointPoolMetricsSnapshot {
            physical_connections_opened: self.physical_connections_opened.load(Ordering::Relaxed),
            physical_connections_closed: self.physical_connections_closed.load(Ordering::Relaxed),
            active_physical_connections: self.active_physical_connections.load(Ordering::Relaxed),
            logical_links_opened: self.logical_links_opened.load(Ordering::Relaxed),
            logical_links_closed: self.logical_links_closed.load(Ordering::Relaxed),
            active_logical_links: self.active_logical_links.load(Ordering::Relaxed),
            frames_written: self.frames_written.load(Ordering::Relaxed),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            pool_rejections: self.pool_rejections.load(Ordering::Relaxed),
        }
    }

    fn record_connection_opened(&self) {
        self.physical_connections_opened
            .fetch_add(1, Ordering::Relaxed);
        self.active_physical_connections
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_connection_closed(&self) {
        self.physical_connections_closed
            .fetch_add(1, Ordering::Relaxed);
        self.active_physical_connections
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(1))
            })
            .ok();
    }

    fn record_link_opened(&self) {
        self.logical_links_opened.fetch_add(1, Ordering::Relaxed);
        self.active_logical_links.fetch_add(1, Ordering::Relaxed);
    }

    fn record_link_closed(&self) {
        self.logical_links_closed.fetch_add(1, Ordering::Relaxed);
        self.active_logical_links
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(1))
            })
            .ok();
    }

    fn record_frame_written(&self) {
        self.frames_written.fetch_add(1, Ordering::Relaxed);
    }

    fn record_pool_rejection(&self) {
        self.pool_rejections.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait]
pub trait DirectLinkEndpointPool: Clone + Send + Sync + 'static {
    async fn open_link(
        &self,
        request: DirectLinkOpenRequest,
    ) -> Result<PooledDirectLinkSession, LinkError>;

    async fn write_frame(
        &self,
        connection_id: DirectLinkConnectionId,
        frame: DirectLinkFrame,
    ) -> Result<(), LinkError>;
}

#[derive(Clone)]
pub struct PooledDirectLinkEndpointPool<T>
where
    T: DirectLinkTransport,
{
    inner: Arc<PooledDirectLinkEndpointPoolInner<T>>,
}

impl<T> fmt::Debug for PooledDirectLinkEndpointPool<T>
where
    T: DirectLinkTransport,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PooledDirectLinkEndpointPool")
            .field("config", &self.inner.config)
            .finish_non_exhaustive()
    }
}

impl<T> PooledDirectLinkEndpointPool<T>
where
    T: DirectLinkTransport,
{
    pub fn new(transport: T, config: DirectLinkEndpointPoolConfig) -> Self {
        Self {
            inner: Arc::new(PooledDirectLinkEndpointPoolInner {
                transport,
                config,
                state: Mutex::new(PoolState::default()),
                next_connection_id: AtomicU64::new(1),
                metrics: DirectLinkEndpointPoolMetrics::default(),
            }),
        }
    }

    pub fn config(&self) -> &DirectLinkEndpointPoolConfig {
        &self.inner.config
    }

    pub fn metrics_snapshot(&self) -> DirectLinkEndpointPoolMetricsSnapshot {
        self.inner.metrics.snapshot()
    }

    pub async fn active_links_for_endpoint(&self, endpoint: &DirectLinkEndpoint) -> usize {
        let state = self.inner.state.lock().await;
        state
            .endpoints
            .get(&DirectLinkEndpointKey::new(endpoint))
            .map(EndpointState::active_links)
            .unwrap_or_default()
    }
}

#[async_trait]
impl<T> DirectLinkEndpointPool for PooledDirectLinkEndpointPool<T>
where
    T: DirectLinkTransport,
{
    async fn open_link(
        &self,
        request: DirectLinkOpenRequest,
    ) -> Result<PooledDirectLinkSession, LinkError> {
        self.inner.clone().open_link(request).await
    }

    async fn write_frame(
        &self,
        connection_id: DirectLinkConnectionId,
        frame: DirectLinkFrame,
    ) -> Result<(), LinkError> {
        self.inner.write_frame(connection_id, frame).await
    }
}

struct PooledDirectLinkEndpointPoolInner<T>
where
    T: DirectLinkTransport,
{
    transport: T,
    config: DirectLinkEndpointPoolConfig,
    state: Mutex<PoolState>,
    next_connection_id: AtomicU64,
    metrics: DirectLinkEndpointPoolMetrics,
}

impl<T> PooledDirectLinkEndpointPoolInner<T>
where
    T: DirectLinkTransport,
{
    async fn open_link(
        self: Arc<Self>,
        request: DirectLinkOpenRequest,
    ) -> Result<PooledDirectLinkSession, LinkError> {
        let (endpoint, target) = endpoint_and_target(&request)?;
        let endpoint_key = DirectLinkEndpointKey::new(&endpoint);
        let stripe_index = self.config.stripe_index_for_link(&request.link_id);

        let stripe = {
            let mut state = self.state.lock().await;
            let endpoint_state = state
                .endpoints
                .entry(endpoint_key.clone())
                .or_insert_with(|| EndpointState::new(self.config.connections_per_endpoint.get()));

            if endpoint_state.active_links() >= self.config.max_links_per_endpoint {
                self.metrics.record_pool_rejection();
                return Err(LinkError::Overloaded);
            }

            let stripe_state = &mut endpoint_state.stripes[stripe_index];
            if let Some(stripe) = stripe_state
                && stripe.active_links >= self.config.max_links_per_connection
            {
                self.metrics.record_pool_rejection();
                return Err(LinkError::Overloaded);
            }

            if stripe_state.is_none() {
                let connection_id =
                    DirectLinkConnectionId(self.next_connection_id.fetch_add(1, Ordering::Relaxed));
                let connection = tokio::time::timeout(
                    self.config.connect_timeout,
                    self.transport.connect_physical(endpoint.clone()),
                )
                .await
                .map_err(|_| LinkError::Protocol("direct link connect timed out".to_string()))??;
                let writer = spawn_connection_task(connection, self.clone(), connection_id);
                self.metrics.record_connection_opened();
                *stripe_state = Some(PooledStripeState {
                    stripe: DirectLinkConnectionStripe {
                        endpoint: endpoint_key.clone(),
                        connection_id,
                        stripe_index,
                    },
                    writer,
                    active_links: 0,
                });
            }

            stripe_state
                .as_ref()
                .expect("stripe exists after connection creation")
                .clone()
        };

        let open_request = OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: request.link_id.clone(),
            source: request.source,
            target,
            mode: request.mode,
            source_to_target: OpenLinkDirection::from_stream(
                request.link_id.clone(),
                &request.source_to_target,
            ),
            target_to_source: request
                .target_to_source
                .as_ref()
                .map(|stream| OpenLinkDirection::from_stream(request.link_id.clone(), stream)),
            options: request.options,
        };
        let frame = DirectLinkFrame::open_link(&open_request)
            .map_err(|error| LinkError::Protocol(error.to_string()))?;
        let response = send_frame_for_response(&stripe.writer, frame).await?;

        match response.kind {
            DirectLinkFrameKind::OpenLinkAck => {
                let ack = response
                    .decode_open_link_ack()
                    .map_err(|error| LinkError::Protocol(error.to_string()))?;
                if ack.link_id != request.link_id {
                    return Err(LinkError::Protocol(format!(
                        "direct link OpenLinkAck link id mismatch: expected {}, got {}",
                        request.link_id, ack.link_id
                    )));
                }
                self.increment_link_count(&endpoint_key, stripe.stripe.connection_id)
                    .await;
                self.metrics.record_link_opened();
                let session = DirectLinkSession {
                    link_id: request.link_id.clone(),
                    direction: LinkDirection::SourceToTarget,
                    stream: request.source_to_target,
                    accepted_message_ids: ack.source_to_target.accepted_message_type_ids,
                    sender: Arc::new(PooledDirectLinkSender {
                        inner: self,
                        endpoint_key,
                        connection_id: stripe.stripe.connection_id,
                        link_id: request.link_id.clone(),
                        direction: LinkDirection::SourceToTarget,
                        next_sequence: AtomicU64::new(1),
                        closed: AtomicBool::new(false),
                    }),
                };
                Ok(PooledDirectLinkSession {
                    connection_id: stripe.stripe.connection_id,
                    endpoint,
                    session,
                })
            }
            DirectLinkFrameKind::OpenLinkReject => {
                let reject = response
                    .decode_open_link_reject()
                    .map_err(|error| LinkError::Protocol(error.to_string()))?;
                Err(reject_reason_to_error(reject.reason))
            }
            other => Err(LinkError::Protocol(format!(
                "expected OpenLinkAck/OpenLinkReject from direct link pool, got {other:?}"
            ))),
        }
    }

    async fn write_frame(
        &self,
        connection_id: DirectLinkConnectionId,
        frame: DirectLinkFrame,
    ) -> Result<(), LinkError> {
        let writer = {
            let state = self.state.lock().await;
            state.find_writer(connection_id)
        }
        .ok_or_else(|| {
            LinkError::Protocol(format!(
                "direct link connection {:?} is not in the endpoint pool",
                connection_id
            ))
        })?;
        send_frame(&writer, frame).await
    }

    async fn increment_link_count(
        &self,
        endpoint_key: &DirectLinkEndpointKey,
        connection_id: DirectLinkConnectionId,
    ) {
        let mut state = self.state.lock().await;
        if let Some(stripe) = state.find_stripe_mut(endpoint_key, connection_id) {
            stripe.active_links += 1;
        }
    }

    async fn release_link(
        &self,
        endpoint_key: &DirectLinkEndpointKey,
        connection_id: DirectLinkConnectionId,
    ) {
        let mut state = self.state.lock().await;
        if let Some(stripe) = state.find_stripe_mut(endpoint_key, connection_id)
            && stripe.active_links > 0
        {
            stripe.active_links -= 1;
            self.metrics.record_link_closed();
        }
    }

    async fn remove_connection(&self, connection_id: DirectLinkConnectionId) {
        let mut state = self.state.lock().await;
        for endpoint in state.endpoints.values_mut() {
            for stripe in &mut endpoint.stripes {
                if stripe
                    .as_ref()
                    .is_some_and(|stripe| stripe.stripe.connection_id == connection_id)
                {
                    *stripe = None;
                    self.metrics.record_connection_closed();
                    return;
                }
            }
        }
    }
}

#[derive(Debug, Default)]
struct PoolState {
    endpoints: HashMap<DirectLinkEndpointKey, EndpointState>,
}

impl PoolState {
    fn find_writer(
        &self,
        connection_id: DirectLinkConnectionId,
    ) -> Option<mpsc::Sender<ConnectionCommand>> {
        self.endpoints
            .values()
            .flat_map(|endpoint| endpoint.stripes.iter())
            .filter_map(Option::as_ref)
            .find(|stripe| stripe.stripe.connection_id == connection_id)
            .map(|stripe| stripe.writer.clone())
    }

    fn find_stripe_mut(
        &mut self,
        endpoint_key: &DirectLinkEndpointKey,
        connection_id: DirectLinkConnectionId,
    ) -> Option<&mut PooledStripeState> {
        self.endpoints
            .get_mut(endpoint_key)?
            .stripes
            .iter_mut()
            .filter_map(Option::as_mut)
            .find(|stripe| stripe.stripe.connection_id == connection_id)
    }
}

#[derive(Debug)]
struct EndpointState {
    stripes: Vec<Option<PooledStripeState>>,
}

impl EndpointState {
    fn new(stripe_count: usize) -> Self {
        Self {
            stripes: vec![None; stripe_count],
        }
    }

    fn active_links(&self) -> usize {
        self.stripes
            .iter()
            .filter_map(Option::as_ref)
            .map(|stripe| stripe.active_links)
            .sum()
    }
}

#[derive(Debug, Clone)]
struct PooledStripeState {
    stripe: DirectLinkConnectionStripe,
    writer: mpsc::Sender<ConnectionCommand>,
    active_links: usize,
}

enum ConnectionCommand {
    Write {
        frame: DirectLinkFrame,
        completion: Option<oneshot::Sender<Result<(), LinkError>>>,
    },
    WriteAndRead {
        frame: DirectLinkFrame,
        response: oneshot::Sender<Result<DirectLinkFrame, LinkError>>,
    },
}

impl fmt::Debug for ConnectionCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Write { frame, completion } => formatter
                .debug_struct("Write")
                .field("frame", frame)
                .field("expects_completion", &completion.is_some())
                .finish(),
            Self::WriteAndRead { frame, .. } => formatter
                .debug_struct("WriteAndRead")
                .field("frame", frame)
                .finish(),
        }
    }
}

struct PooledDirectLinkSender<T>
where
    T: DirectLinkTransport,
{
    inner: Arc<PooledDirectLinkEndpointPoolInner<T>>,
    endpoint_key: DirectLinkEndpointKey,
    connection_id: DirectLinkConnectionId,
    link_id: LinkId,
    direction: LinkDirection,
    next_sequence: AtomicU64,
    closed: AtomicBool,
}

impl<T> fmt::Debug for PooledDirectLinkSender<T>
where
    T: DirectLinkTransport,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PooledDirectLinkSender")
            .field("endpoint_key", &self.endpoint_key)
            .field("connection_id", &self.connection_id)
            .field("link_id", &self.link_id)
            .field("direction", &self.direction)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<T> DirectLinkSender for PooledDirectLinkSender<T>
where
    T: DirectLinkTransport,
{
    async fn tell(&self, message: OutboundDirectLinkMessage) -> Result<(), LinkSendError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(LinkSendError::Closed {
                reason: LinkCloseReason::Done,
            });
        }
        let frame = self.message_to_frame(message)?;
        self.inner
            .write_frame(self.connection_id, frame)
            .await
            .map_err(|error| LinkSendError::Protocol(error.to_string()))
    }

    fn try_tell(&self, message: OutboundDirectLinkMessage) -> Result<(), LinkSendError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(LinkSendError::Closed {
                reason: LinkCloseReason::Done,
            });
        }
        let frame = self.message_to_frame(message)?;
        let writer = self
            .inner
            .state
            .try_lock()
            .ok()
            .and_then(|state| state.find_writer(self.connection_id))
            .ok_or(LinkSendError::BackpressureFull)?;
        writer
            .try_send(ConnectionCommand::Write {
                frame,
                completion: None,
            })
            .map_err(|_| LinkSendError::BackpressureFull)
    }

    async fn close(&self, _reason: LinkCloseReason) -> Result<(), LinkSendError> {
        if self.closed.swap(true, Ordering::Relaxed) {
            return Ok(());
        }
        let frame = DirectLinkFrame {
            kind: DirectLinkFrameKind::CloseDirection,
            link_id: self.link_id.clone(),
            sequence: LinkSequence(0),
            message_id: None,
            flags: Default::default(),
            header: Vec::new(),
            payload: Vec::new(),
        };
        let _ = self.inner.write_frame(self.connection_id, frame).await;
        self.inner
            .release_link(&self.endpoint_key, self.connection_id)
            .await;
        Ok(())
    }
}

impl<T> PooledDirectLinkSender<T>
where
    T: DirectLinkTransport,
{
    fn message_to_frame(
        &self,
        message: OutboundDirectLinkMessage,
    ) -> Result<DirectLinkFrame, LinkSendError> {
        if message.direction != self.direction {
            return Err(LinkSendError::Protocol(
                "direct link sender used with the wrong direction".to_string(),
            ));
        }
        Ok(DirectLinkFrame::directed_message(
            message.link_id,
            message.direction,
            LinkSequence(self.next_sequence.fetch_add(1, Ordering::Relaxed)),
            message.message_id,
            message.payload,
        ))
    }
}

fn spawn_connection_task<T>(
    mut connection: T::Connection,
    pool: Arc<PooledDirectLinkEndpointPoolInner<T>>,
    connection_id: DirectLinkConnectionId,
) -> mpsc::Sender<ConnectionCommand>
where
    T: DirectLinkTransport,
{
    let (tx, mut rx) = mpsc::channel(1024);
    tokio::spawn(async move {
        while let Some(command) = rx.recv().await {
            match command {
                ConnectionCommand::Write { frame, completion } => {
                    let write_result = connection.write_frame(frame).await;
                    if let Err(error) = write_result {
                        if let Some(completion) = completion {
                            let _ = completion.send(Err(error));
                        }
                        break;
                    }
                    pool.metrics.record_frame_written();
                    if let Some(completion) = completion {
                        let _ = completion.send(Ok(()));
                    }
                }
                ConnectionCommand::WriteAndRead { frame, response } => {
                    let write_result = connection.write_frame(frame).await;
                    if let Err(error) = write_result {
                        let _ = response.send(Err(error));
                        break;
                    }
                    pool.metrics.record_frame_written();
                    let result = connection.read_frame().await;
                    let should_break = result.is_err();
                    let _ = response.send(result);
                    if should_break {
                        break;
                    }
                }
            }
        }
        let _ = connection.close().await;
        pool.remove_connection(connection_id).await;
    });
    tx
}

async fn send_frame(
    writer: &mpsc::Sender<ConnectionCommand>,
    frame: DirectLinkFrame,
) -> Result<(), LinkError> {
    let (tx, rx) = oneshot::channel();
    writer
        .send(ConnectionCommand::Write {
            frame,
            completion: Some(tx),
        })
        .await
        .map_err(|_| LinkError::Protocol("direct link pooled writer is closed".to_string()))?;
    rx.await
        .map_err(|_| LinkError::Protocol("direct link pooled writer stopped".to_string()))?
}

async fn send_frame_for_response(
    writer: &mpsc::Sender<ConnectionCommand>,
    frame: DirectLinkFrame,
) -> Result<DirectLinkFrame, LinkError> {
    let (tx, rx) = oneshot::channel();
    writer
        .send(ConnectionCommand::WriteAndRead {
            frame,
            response: tx,
        })
        .await
        .map_err(|_| LinkError::Protocol("direct link pooled writer is closed".to_string()))?;
    rx.await
        .map_err(|_| LinkError::Protocol("direct link pooled writer stopped".to_string()))?
}

fn endpoint_and_target(
    request: &DirectLinkOpenRequest,
) -> Result<(DirectLinkEndpoint, lattice_core::ActorRef), LinkError> {
    match &request.target {
        LinkTarget::Endpoint { endpoint, target } => Ok((endpoint.clone(), target.clone())),
        LinkTarget::Actor(_) => Err(LinkError::Protocol(
            "direct link ActorRef targets require placement endpoint resolution".to_string(),
        )),
    }
}

fn reject_reason_to_error(reason: crate::session::OpenLinkRejectReason) -> LinkError {
    match reason {
        crate::session::OpenLinkRejectReason::NotOwner => LinkError::NotOwner { redirect: None },
        crate::session::OpenLinkRejectReason::Fenced => LinkError::Fenced,
        crate::session::OpenLinkRejectReason::ActorUnavailable => LinkError::ActorUnavailable,
        crate::session::OpenLinkRejectReason::UnsupportedStream => LinkError::UnsupportedStream,
        crate::session::OpenLinkRejectReason::UnsupportedMessageType => {
            LinkError::UnsupportedMessageType
        }
        crate::session::OpenLinkRejectReason::Unauthorized => LinkError::Unauthorized,
        crate::session::OpenLinkRejectReason::Overloaded => LinkError::Overloaded,
        crate::session::OpenLinkRejectReason::ProtocolVersionMismatch => {
            LinkError::ProtocolVersionMismatch
        }
    }
}

fn stable_stripe_index(link_id: &LinkId, stripe_count: NonZeroUsize) -> usize {
    let mut hasher = DefaultHasher::new();
    link_id.hash(&mut hasher);
    (hasher.finish() as usize) % stripe_count.get()
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use lattice_core::{
        ActorId, DirectLinkMessageDescriptor, DirectLinkMessageId, DirectLinkMode,
        DirectLinkOptions, DirectLinkStreamDescriptor, InstanceId, LinkMessageFlags, actor_kind,
        service_kind,
    };

    use super::*;

    #[derive(Debug, Clone, Default)]
    struct FakeTransport {
        connects: Arc<StdMutex<Vec<DirectLinkEndpoint>>>,
        frames: Arc<StdMutex<Vec<DirectLinkFrame>>>,
    }

    #[async_trait]
    impl DirectLinkTransport for FakeTransport {
        type Listener = ();
        type Connection = FakeConnection;

        async fn bind(
            &self,
            _config: crate::transport::DirectLinkListenConfig,
        ) -> Result<Self::Listener, LinkError> {
            Ok(())
        }

        async fn connect_physical(
            &self,
            endpoint: DirectLinkEndpoint,
        ) -> Result<Self::Connection, LinkError> {
            self.connects.lock().unwrap().push(endpoint);
            Ok(FakeConnection {
                frames: self.frames.clone(),
            })
        }
    }

    #[derive(Debug)]
    struct FakeConnection {
        frames: Arc<StdMutex<Vec<DirectLinkFrame>>>,
    }

    #[async_trait]
    impl DirectLinkConnection for FakeConnection {
        async fn read_frame(&mut self) -> Result<DirectLinkFrame, LinkError> {
            let open = self
                .frames
                .lock()
                .unwrap()
                .last()
                .expect("open frame was written")
                .decode_open_link()
                .unwrap();
            let ack = crate::session::OpenLinkAck {
                link_id: open.link_id.clone(),
                source_to_target: crate::session::NegotiatedDirection {
                    direction: LinkDirection::SourceToTarget,
                    stream_name: open.source_to_target.stream_name,
                    accepted_message_type_ids: open.source_to_target.supported_message_type_ids,
                    next_receive_sequence: LinkSequence(1),
                    backpressure: open.options.backpressure,
                    closed: false,
                },
                target_to_source: None,
            };
            DirectLinkFrame::open_link_ack(&ack)
                .map_err(|error| LinkError::Protocol(error.to_string()))
        }

        async fn write_frame(&mut self, frame: DirectLinkFrame) -> Result<(), LinkError> {
            self.frames.lock().unwrap().push(frame);
            Ok(())
        }

        async fn close(&mut self) -> Result<(), LinkError> {
            Ok(())
        }
    }

    fn endpoint() -> DirectLinkEndpoint {
        DirectLinkEndpoint::new("tcp://127.0.0.1:9001".parse().unwrap())
    }

    fn stream() -> DirectLinkStreamDescriptor {
        DirectLinkStreamDescriptor {
            stream_name: "movement".to_string(),
            messages: vec![DirectLinkMessageDescriptor {
                message_id: DirectLinkMessageId(7),
                proto_full_name: "game.Position".to_string(),
                rust_type_name: "Position".to_string(),
            }],
        }
    }

    fn actor_ref(actor_id: u64) -> lattice_core::ActorRef {
        lattice_core::ActorRef::direct(
            service_kind!("Battle"),
            actor_kind!("BattleActor"),
            ActorId::U64(actor_id),
            InstanceId::new("battle-1"),
            "http://127.0.0.1:18080".parse().unwrap(),
            None,
        )
    }

    fn request(link_id: &str) -> DirectLinkOpenRequest {
        let endpoint = endpoint();
        DirectLinkOpenRequest {
            link_id: LinkId::new(link_id),
            source: actor_ref(1),
            target: LinkTarget::Endpoint {
                endpoint,
                target: actor_ref(2),
            },
            mode: DirectLinkMode::Unidirectional,
            source_to_target: stream(),
            target_to_source: None,
            options: DirectLinkOptions::default(),
            trace: Default::default(),
        }
    }

    #[tokio::test]
    async fn endpoint_pool_reuses_one_physical_connection_for_multiple_links() {
        let transport = FakeTransport::default();
        let pool = PooledDirectLinkEndpointPool::new(
            transport.clone(),
            DirectLinkEndpointPoolConfig::default(),
        );

        let first = pool.open_link(request("link-1")).await.unwrap();
        let second = pool.open_link(request("link-2")).await.unwrap();

        assert_eq!(first.connection_id, second.connection_id);
        assert_eq!(transport.connects.lock().unwrap().len(), 1);
        assert_eq!(pool.active_links_for_endpoint(&endpoint()).await, 2);
        let metrics = pool.metrics_snapshot();
        assert_eq!(metrics.physical_connections_opened, 1);
        assert_eq!(metrics.logical_links_opened, 2);
        assert_eq!(metrics.active_logical_links, 2);
    }

    #[tokio::test]
    async fn endpoint_pool_honors_stable_stripe_selection() {
        let config = DirectLinkEndpointPoolConfig {
            connections_per_endpoint: NonZeroUsize::new(4).unwrap(),
            ..DirectLinkEndpointPoolConfig::default()
        };
        let link_id = LinkId::new("stable-link");

        assert_eq!(
            config.stripe_index_for_link(&link_id),
            config.stripe_index_for_link(&link_id)
        );
        assert!(config.stripe_index_for_link(&link_id) < 4);
    }

    #[tokio::test]
    async fn max_links_per_connection_rejects_before_openlink() {
        let transport = FakeTransport::default();
        let config = DirectLinkEndpointPoolConfig {
            max_links_per_connection: 1,
            ..DirectLinkEndpointPoolConfig::default()
        };
        let pool = PooledDirectLinkEndpointPool::new(transport.clone(), config);

        pool.open_link(request("link-1")).await.unwrap();
        let error = pool.open_link(request("link-2")).await.unwrap_err();

        assert!(matches!(error, LinkError::Overloaded));
        assert_eq!(transport.connects.lock().unwrap().len(), 1);
        let open_frames = transport
            .frames
            .lock()
            .unwrap()
            .iter()
            .filter(|frame| frame.kind == DirectLinkFrameKind::OpenLink)
            .count();
        assert_eq!(open_frames, 1);
        assert_eq!(pool.metrics_snapshot().pool_rejections, 1);
    }

    #[tokio::test]
    async fn max_links_per_endpoint_rejects_before_openlink() {
        let transport = FakeTransport::default();
        let config = DirectLinkEndpointPoolConfig {
            max_links_per_endpoint: 1,
            connections_per_endpoint: NonZeroUsize::new(2).unwrap(),
            ..DirectLinkEndpointPoolConfig::default()
        };
        let pool = PooledDirectLinkEndpointPool::new(transport.clone(), config);

        pool.open_link(request("link-1")).await.unwrap();
        let error = pool.open_link(request("link-2")).await.unwrap_err();

        assert!(matches!(error, LinkError::Overloaded));
        let open_frames = transport
            .frames
            .lock()
            .unwrap()
            .iter()
            .filter(|frame| frame.kind == DirectLinkFrameKind::OpenLink)
            .count();
        assert_eq!(open_frames, 1);
        assert_eq!(pool.metrics_snapshot().pool_rejections, 1);
    }

    #[tokio::test]
    async fn pooled_sender_writes_message_frames_through_selected_connection() {
        let transport = FakeTransport::default();
        let pool = PooledDirectLinkEndpointPool::new(
            transport.clone(),
            DirectLinkEndpointPoolConfig::default(),
        );
        let session = pool.open_link(request("link-1")).await.unwrap();
        let sender = session.session.sender.clone();

        sender
            .tell(OutboundDirectLinkMessage {
                link_id: LinkId::new("link-1"),
                direction: LinkDirection::SourceToTarget,
                message_id: DirectLinkMessageId(7),
                proto_full_name: "game.Position",
                payload: b"abc".to_vec(),
                flags: LinkMessageFlags::EMPTY,
            })
            .await
            .unwrap();

        let frames = transport.frames.lock().unwrap();
        assert!(frames.iter().any(|frame| {
            frame.kind == DirectLinkFrameKind::Message
                && frame.link_id == LinkId::new("link-1")
                && frame.message_id == Some(DirectLinkMessageId(7))
        }));
    }
}
