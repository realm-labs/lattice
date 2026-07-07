// This module keeps endpoint-pool internals and their white-box tests together
// while Phase 9 connection pooling is still being assembled. The tests inspect
// private stripe/link tables so connection-loss and drain semantics can be
// covered before stable public inspection APIs exist.

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use lattice_core::{
    ActorRef, DirectLinkEndpoint, DirectLinkOpenRequest, DirectLinkSender, DirectLinkSession,
    LinkCloseReason, LinkClosed, LinkDirection, LinkDirectionClosed, LinkError, LinkId,
    LinkSendError, LinkSequence, LinkTarget, OutboundDirectLinkMessage,
};
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::protocol::{DirectLinkFrame, DirectLinkFrameKind};
use crate::session::{
    DIRECT_LINK_PROTOCOL_VERSION, OpenLinkAck, OpenLinkDirection, OpenLinkRequest,
};
use crate::transport::{DirectLinkConnection, DirectLinkTransport};

pub trait DirectLinkEndpointPoolLifecycle: Send + Sync + 'static {
    fn deliver_direction_closed(
        &self,
        actor_ref: &ActorRef,
        event: LinkDirectionClosed,
    ) -> Result<(), LinkError>;

    fn deliver_link_closed(&self, actor_ref: &ActorRef, event: LinkClosed)
    -> Result<(), LinkError>;
}

#[derive(Debug, Clone)]
pub struct DirectLinkEndpointPoolConfig {
    pub connections_per_endpoint: NonZeroUsize,
    pub max_frame_size: usize,
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
            max_frame_size: 256 * 1024,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
    pub pool_queue_backpressure_events: u64,
    pub links_per_connection: BTreeMap<DirectLinkConnectionId, usize>,
    pub frames_per_connection: BTreeMap<DirectLinkConnectionId, u64>,
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
    pool_queue_backpressure_events: AtomicU64,
    links_per_connection: std::sync::Mutex<BTreeMap<DirectLinkConnectionId, usize>>,
    frames_per_connection: std::sync::Mutex<BTreeMap<DirectLinkConnectionId, u64>>,
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
            pool_queue_backpressure_events: self
                .pool_queue_backpressure_events
                .load(Ordering::Relaxed),
            links_per_connection: self
                .links_per_connection
                .lock()
                .expect("direct link endpoint pool link metrics poisoned")
                .clone(),
            frames_per_connection: self
                .frames_per_connection
                .lock()
                .expect("direct link endpoint pool frame metrics poisoned")
                .clone(),
        }
    }

    fn record_connection_opened(&self, connection_id: DirectLinkConnectionId) {
        self.physical_connections_opened
            .fetch_add(1, Ordering::Relaxed);
        self.active_physical_connections
            .fetch_add(1, Ordering::Relaxed);
        self.links_per_connection
            .lock()
            .expect("direct link endpoint pool link metrics poisoned")
            .entry(connection_id)
            .or_insert(0);
        self.frames_per_connection
            .lock()
            .expect("direct link endpoint pool frame metrics poisoned")
            .entry(connection_id)
            .or_insert(0);
    }

    fn record_connection_closed(&self, connection_id: DirectLinkConnectionId) {
        self.physical_connections_closed
            .fetch_add(1, Ordering::Relaxed);
        self.active_physical_connections
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(1))
            })
            .ok();
        self.links_per_connection
            .lock()
            .expect("direct link endpoint pool link metrics poisoned")
            .remove(&connection_id);
    }

    fn record_link_opened(&self, connection_id: DirectLinkConnectionId) {
        self.logical_links_opened.fetch_add(1, Ordering::Relaxed);
        self.active_logical_links.fetch_add(1, Ordering::Relaxed);
        *self
            .links_per_connection
            .lock()
            .expect("direct link endpoint pool link metrics poisoned")
            .entry(connection_id)
            .or_insert(0) += 1;
    }

    fn record_link_closed(&self, connection_id: DirectLinkConnectionId) {
        self.logical_links_closed.fetch_add(1, Ordering::Relaxed);
        self.active_logical_links
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(1))
            })
            .ok();
        let mut links = self
            .links_per_connection
            .lock()
            .expect("direct link endpoint pool link metrics poisoned");
        if let Some(count) = links.get_mut(&connection_id) {
            *count = count.saturating_sub(1);
        }
    }

    fn record_frame_written(&self, connection_id: DirectLinkConnectionId) {
        self.frames_written.fetch_add(1, Ordering::Relaxed);
        *self
            .frames_per_connection
            .lock()
            .expect("direct link endpoint pool frame metrics poisoned")
            .entry(connection_id)
            .or_insert(0) += 1;
    }

    fn record_pool_rejection(&self) {
        self.pool_rejections.fetch_add(1, Ordering::Relaxed);
    }

    fn record_pool_queue_backpressure(&self) {
        self.pool_queue_backpressure_events
            .fetch_add(1, Ordering::Relaxed);
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
        Self::new_with_lifecycle(transport, config, None)
    }

    pub fn new_with_lifecycle(
        transport: T,
        config: DirectLinkEndpointPoolConfig,
        lifecycle: Option<Arc<dyn DirectLinkEndpointPoolLifecycle>>,
    ) -> Self {
        Self {
            inner: Arc::new(PooledDirectLinkEndpointPoolInner {
                transport,
                config,
                lifecycle,
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

    pub async fn close_all_logical_links(
        &self,
        reason: LinkCloseReason,
    ) -> Result<usize, LinkError> {
        self.inner.close_all_logical_links(reason).await
    }

    pub async fn closed_link_reasons(&self) -> BTreeMap<LinkId, LinkCloseReason> {
        let state = self.inner.state.lock().await;
        state.closed_links.clone()
    }

    pub async fn process_protocol_error_frame(
        &self,
        frame: DirectLinkFrame,
    ) -> Result<(), LinkError> {
        self.inner.process_protocol_error_frame(frame).await
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
    lifecycle: Option<Arc<dyn DirectLinkEndpointPoolLifecycle>>,
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
                    self.transport
                        .connect_physical(endpoint.clone(), self.config.max_frame_size),
                )
                .await
                .map_err(|_| LinkError::Protocol("direct link connect timed out".to_string()))??;
                let writer = spawn_connection_task(connection, self.clone(), connection_id);
                self.metrics.record_connection_opened(connection_id);
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
            source: request.source.clone(),
            target: target.clone(),
            mode: request.mode,
            source_to_target: OpenLinkDirection::from_stream(
                request.link_id.clone(),
                &request.source_to_target,
            ),
            target_to_source: request
                .target_to_source
                .as_ref()
                .map(|stream| OpenLinkDirection::from_stream(request.link_id.clone(), stream)),
            options: request.options.clone(),
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
                self.register_link(
                    request.link_id.clone(),
                    &endpoint_key,
                    stripe.stripe.connection_id,
                    PooledLinkState::from_open_ack(
                        endpoint_key.clone(),
                        stripe.stripe.connection_id,
                        &request,
                        &ack,
                    ),
                )
                .await;
                self.metrics.record_link_opened(stripe.stripe.connection_id);
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
                        next_sequence: StdMutex::new(1),
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
            DirectLinkFrameKind::ProtocolError => {
                let reason = String::from_utf8(response.payload)
                    .unwrap_or_else(|_| "remote protocol error".to_string());
                self.close_connection(
                    stripe.stripe.connection_id,
                    LinkCloseReason::ProtocolError(reason.clone()),
                )
                .await;
                Err(LinkError::Protocol(reason))
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

    async fn register_link(
        &self,
        link_id: LinkId,
        endpoint_key: &DirectLinkEndpointKey,
        connection_id: DirectLinkConnectionId,
        link_state: PooledLinkState,
    ) {
        let mut state = self.state.lock().await;
        if let Some(stripe) = state.find_stripe_mut(endpoint_key, connection_id) {
            stripe.active_links += 1;
            state.links.insert(link_id, link_state);
        }
    }

    async fn close_all_logical_links(&self, reason: LinkCloseReason) -> Result<usize, LinkError> {
        let (links, closures) = {
            let mut state = self.state.lock().await;
            let links = std::mem::take(&mut state.links);
            for link in links.values() {
                if let Some(stripe) = state.find_stripe_mut(&link.endpoint_key, link.connection_id)
                {
                    stripe.active_links = stripe.active_links.saturating_sub(1);
                    self.metrics.record_link_closed(link.connection_id);
                }
            }
            let closures = links
                .iter()
                .map(|(link_id, link)| link.close_all_event(link_id, reason.clone()))
                .collect::<Vec<_>>();
            let writers = links
                .into_iter()
                .filter_map(|(link_id, link)| {
                    state
                        .find_writer(link.connection_id)
                        .map(|writer| (link_id, writer))
                })
                .collect::<Vec<_>>();
            (writers, closures)
        };
        let count = links.len();
        for (link_id, writer) in links {
            send_frame(&writer, DirectLinkFrame::close(link_id, reason.clone())).await?;
        }
        for closure in closures {
            self.deliver_link_closure(closure);
        }
        Ok(count)
    }

    async fn process_protocol_error_frame(&self, frame: DirectLinkFrame) -> Result<(), LinkError> {
        if frame.kind != DirectLinkFrameKind::ProtocolError {
            return Err(LinkError::Protocol(format!(
                "expected ProtocolError frame, got {:?}",
                frame.kind
            )));
        }
        let reason = String::from_utf8(frame.payload)
            .unwrap_or_else(|_| "remote protocol error".to_string());
        if self
            .close_logical_link(
                &frame.link_id,
                LinkCloseReason::ProtocolError(reason.clone()),
            )
            .await
            .is_some()
        {
            Ok(())
        } else {
            Err(LinkError::Protocol(format!(
                "protocol error for unknown direct link {}: {reason}",
                frame.link_id
            )))
        }
    }

    async fn remove_connection(&self, connection_id: DirectLinkConnectionId) {
        self.close_connection(connection_id, LinkCloseReason::ConnectionLost)
            .await;
    }

    async fn close_connection(
        &self,
        connection_id: DirectLinkConnectionId,
        reason: LinkCloseReason,
    ) {
        let _ = self
            .close_logical_links_for_connection(connection_id, reason)
            .await;
        let mut state = self.state.lock().await;
        for endpoint in state.endpoints.values_mut() {
            for stripe in &mut endpoint.stripes {
                if stripe
                    .as_ref()
                    .is_some_and(|stripe| stripe.stripe.connection_id == connection_id)
                {
                    *stripe = None;
                    self.metrics.record_connection_closed(connection_id);
                    return;
                }
            }
        }
    }

    async fn close_logical_links_for_connection(
        &self,
        connection_id: DirectLinkConnectionId,
        reason: LinkCloseReason,
    ) -> usize {
        let (affected, closures) = {
            let mut state = self.state.lock().await;
            let affected = state
                .links
                .iter()
                .filter_map(|(link_id, link)| {
                    (link.connection_id == connection_id).then_some(link_id.clone())
                })
                .collect::<Vec<_>>();
            let mut closures = Vec::new();
            for link_id in &affected {
                if let Some(link) = state.links.remove(link_id) {
                    if let Some(stripe) =
                        state.find_stripe_mut(&link.endpoint_key, link.connection_id)
                    {
                        stripe.active_links = stripe.active_links.saturating_sub(1);
                    }
                    closures.push(link.close_all_event(link_id, reason.clone()));
                    state.closed_links.insert(link_id.clone(), reason.clone());
                    self.metrics.record_link_closed(link.connection_id);
                }
            }
            (affected, closures)
        };
        for closure in closures {
            self.deliver_link_closure(closure);
        }
        affected.len()
    }

    async fn close_logical_link(
        &self,
        link_id: &LinkId,
        reason: LinkCloseReason,
    ) -> Option<PooledLinkClosure> {
        let closure = {
            let mut state = self.state.lock().await;
            let link = state.links.remove(link_id)?;
            if let Some(stripe) = state.find_stripe_mut(&link.endpoint_key, link.connection_id) {
                stripe.active_links = stripe.active_links.saturating_sub(1);
            }
            let closure = link.close_all_event(link_id, reason.clone());
            state.closed_links.insert(link_id.clone(), reason);
            self.metrics.record_link_closed(link.connection_id);
            closure
        };
        self.deliver_link_closure(closure.clone());
        Some(closure)
    }

    async fn close_logical_direction(
        &self,
        link_id: &LinkId,
        direction: LinkDirection,
        reason: LinkCloseReason,
    ) -> Option<PooledLinkClosure> {
        let closure = {
            let mut state = self.state.lock().await;
            let link = state.links.get_mut(link_id)?;
            let closure = link.close_direction_event(link_id, direction, reason.clone())?;
            if link.is_fully_closed() {
                let link = state
                    .links
                    .remove(link_id)
                    .expect("link exists after get_mut");
                if let Some(stripe) = state.find_stripe_mut(&link.endpoint_key, link.connection_id)
                {
                    stripe.active_links = stripe.active_links.saturating_sub(1);
                }
                state.closed_links.insert(link_id.clone(), reason);
                self.metrics.record_link_closed(link.connection_id);
            }
            closure
        };
        self.deliver_link_closure(closure.clone());
        Some(closure)
    }

    fn deliver_link_closure(&self, closure: PooledLinkClosure) {
        let Some(lifecycle) = &self.lifecycle else {
            return;
        };
        for event in closure.direction_closed {
            if let Err(error) = lifecycle.deliver_direction_closed(&closure.source, event) {
                tracing::warn!(%error, "failed to deliver source direct-link direction close");
            }
        }
        if let Some(event) = closure.link_closed
            && let Err(error) = lifecycle.deliver_link_closed(&closure.source, event)
        {
            tracing::warn!(%error, "failed to deliver source direct-link close");
        }
    }
}

#[derive(Debug, Default)]
struct PoolState {
    endpoints: HashMap<DirectLinkEndpointKey, EndpointState>,
    links: BTreeMap<LinkId, PooledLinkState>,
    closed_links: BTreeMap<LinkId, LinkCloseReason>,
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

#[derive(Debug, Clone)]
struct PooledLinkState {
    endpoint_key: DirectLinkEndpointKey,
    connection_id: DirectLinkConnectionId,
    source: ActorRef,
    directions: BTreeMap<LinkDirection, PooledDirectionState>,
}

#[derive(Debug, Clone)]
struct PooledDirectionState {
    stream_name: String,
    closed: bool,
}

#[derive(Debug, Clone)]
struct PooledLinkClosure {
    source: ActorRef,
    direction_closed: Vec<LinkDirectionClosed>,
    link_closed: Option<LinkClosed>,
}

impl PooledLinkState {
    fn from_open_ack(
        endpoint_key: DirectLinkEndpointKey,
        connection_id: DirectLinkConnectionId,
        request: &DirectLinkOpenRequest,
        ack: &OpenLinkAck,
    ) -> Self {
        let mut directions = BTreeMap::from([(
            LinkDirection::SourceToTarget,
            PooledDirectionState {
                stream_name: ack.source_to_target.stream_name.clone(),
                closed: false,
            },
        )]);
        if let Some(direction) = &ack.target_to_source {
            directions.insert(
                LinkDirection::TargetToSource,
                PooledDirectionState {
                    stream_name: direction.stream_name.clone(),
                    closed: false,
                },
            );
        }
        Self {
            endpoint_key,
            connection_id,
            source: request.source.clone(),
            directions,
        }
    }

    fn close_direction_event(
        &mut self,
        link_id: &LinkId,
        direction: LinkDirection,
        reason: LinkCloseReason,
    ) -> Option<PooledLinkClosure> {
        let direction_state = self.directions.get_mut(&direction)?;
        if direction_state.closed {
            return None;
        }
        direction_state.closed = true;
        let direction_closed = source_observes_direction(direction).then(|| LinkDirectionClosed {
            link_id: link_id.clone(),
            direction,
            stream: direction_state.stream_name.clone(),
            reason: reason.clone(),
            last_sequence_seen: None,
        });
        let link_closed = self
            .is_fully_closed()
            .then(|| self.link_closed(link_id, reason));
        Some(PooledLinkClosure {
            source: self.source.clone(),
            direction_closed: direction_closed.into_iter().collect(),
            link_closed,
        })
    }

    fn close_all_event(&self, link_id: &LinkId, reason: LinkCloseReason) -> PooledLinkClosure {
        let direction_closed = self
            .directions
            .iter()
            .filter(|(_, state)| !state.closed)
            .filter_map(|(direction, state)| {
                source_observes_direction(*direction).then(|| LinkDirectionClosed {
                    link_id: link_id.clone(),
                    direction: *direction,
                    stream: state.stream_name.clone(),
                    reason: reason.clone(),
                    last_sequence_seen: None,
                })
            })
            .collect();
        PooledLinkClosure {
            source: self.source.clone(),
            direction_closed,
            link_closed: Some(self.link_closed(link_id, reason)),
        }
    }

    fn link_closed(&self, link_id: &LinkId, reason: LinkCloseReason) -> LinkClosed {
        LinkClosed {
            link_id: link_id.clone(),
            reason,
            closed_directions: self.directions.keys().copied().collect(),
            last_sequence_seen: None,
        }
    }

    fn is_fully_closed(&self) -> bool {
        self.directions.values().all(|direction| direction.closed)
    }
}

fn source_observes_direction(direction: LinkDirection) -> bool {
    direction == LinkDirection::TargetToSource
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
    next_sequence: StdMutex<u64>,
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
        let frame = self.next_message_frame(message)?;
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
        let writer = self
            .inner
            .state
            .try_lock()
            .ok()
            .and_then(|state| state.find_writer(self.connection_id))
            .ok_or_else(|| {
                self.inner.metrics.record_pool_queue_backpressure();
                LinkSendError::BackpressureFull
            })?;
        let mut next_sequence = self
            .next_sequence
            .lock()
            .expect("direct link pooled sender sequence poisoned");
        let frame = self.message_to_frame(message, LinkSequence(*next_sequence))?;
        match writer.try_send(ConnectionCommand::Write {
            frame,
            completion: None,
        }) {
            Ok(()) => {
                *next_sequence += 1;
                Ok(())
            }
            Err(_) => {
                self.inner.metrics.record_pool_queue_backpressure();
                Err(LinkSendError::BackpressureFull)
            }
        }
    }

    async fn close(&self, _reason: LinkCloseReason) -> Result<(), LinkSendError> {
        if self.closed.swap(true, Ordering::Relaxed) {
            return Ok(());
        }
        let frame =
            DirectLinkFrame::close_direction(self.link_id.clone(), self.direction, _reason.clone());
        let _ = self.inner.write_frame(self.connection_id, frame).await;
        self.inner
            .close_logical_direction(&self.link_id, self.direction, _reason)
            .await;
        Ok(())
    }
}

impl<T> PooledDirectLinkSender<T>
where
    T: DirectLinkTransport,
{
    fn next_message_frame(
        &self,
        message: OutboundDirectLinkMessage,
    ) -> Result<DirectLinkFrame, LinkSendError> {
        let mut next_sequence = self
            .next_sequence
            .lock()
            .expect("direct link pooled sender sequence poisoned");
        let frame = self.message_to_frame(message, LinkSequence(*next_sequence))?;
        *next_sequence += 1;
        Ok(frame)
    }

    fn message_to_frame(
        &self,
        message: OutboundDirectLinkMessage,
        sequence: LinkSequence,
    ) -> Result<DirectLinkFrame, LinkSendError> {
        if message.direction != self.direction {
            return Err(LinkSendError::Protocol(
                "direct link sender used with the wrong direction".to_string(),
            ));
        }
        if message.link_id != self.link_id {
            return Err(LinkSendError::Protocol(format!(
                "direct link sender for {} cannot send frame for {}",
                self.link_id, message.link_id
            )));
        }
        Ok(DirectLinkFrame::directed_message_with_header(
            message.link_id,
            message.direction,
            sequence,
            message.message_id,
            message.metadata,
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
        let mut pending_responses: HashMap<
            LinkId,
            oneshot::Sender<Result<DirectLinkFrame, LinkError>>,
        > = HashMap::new();
        let mut read_enabled = false;
        loop {
            tokio::select! {
                biased;
                command = rx.recv() => {
                    let Some(command) = command else {
                        break;
                    };
                    match command {
                        ConnectionCommand::Write { frame, completion } => {
                    let write_result = connection.write_frame(frame).await;
                    if let Err(error) = write_result {
                        if let Some(completion) = completion {
                            let _ = completion.send(Err(error));
                        }
                        break;
                    }
                    pool.metrics.record_frame_written(connection_id);
                    if let Some(completion) = completion {
                        let _ = completion.send(Ok(()));
                    }
                    read_enabled = true;
                        }
                        ConnectionCommand::WriteAndRead { frame, response } => {
                    let link_id = frame.link_id.clone();
                    let write_result = connection.write_frame(frame).await;
                    if let Err(error) = write_result {
                        let _ = response.send(Err(error));
                        break;
                    }
                    pool.metrics.record_frame_written(connection_id);
                    pending_responses.insert(link_id, response);
                    read_enabled = true;
                        }
                    }
                },
                frame = connection.read_frame(), if read_enabled => {
                    match frame {
                        Ok(frame) => {
                            let should_break = handle_connection_frame(
                                &mut connection,
                                &pool,
                                connection_id,
                                frame,
                                &mut pending_responses,
                            )
                            .await;
                            if should_break {
                                break;
                            }
                        }
                        Err(error) => {
                            for (_, response) in pending_responses.drain() {
                                let _ = response.send(Err(LinkError::Protocol(error.to_string())));
                            }
                            break;
                        }
                    }
                },
            }
        }
        let _ = connection.close().await;
        pool.remove_connection(connection_id).await;
    });
    tx
}

async fn handle_connection_frame<T>(
    connection: &mut T::Connection,
    pool: &Arc<PooledDirectLinkEndpointPoolInner<T>>,
    _connection_id: DirectLinkConnectionId,
    frame: DirectLinkFrame,
    pending_responses: &mut HashMap<LinkId, oneshot::Sender<Result<DirectLinkFrame, LinkError>>>,
) -> bool
where
    T: DirectLinkTransport,
{
    if matches!(
        frame.kind,
        DirectLinkFrameKind::OpenLinkAck | DirectLinkFrameKind::OpenLinkReject
    ) {
        if let Some(response) = pending_responses.remove(&frame.link_id) {
            let _ = response.send(Ok(frame));
            return false;
        }
        return true;
    }

    match frame.kind {
        DirectLinkFrameKind::ProtocolError => {
            let reason = String::from_utf8(frame.payload.clone())
                .unwrap_or_else(|_| "remote protocol error".to_string());
            if let Some(response) = pending_responses.remove(&frame.link_id) {
                let _ = response.send(Err(LinkError::Protocol(reason.clone())));
                pool.close_connection(_connection_id, LinkCloseReason::ProtocolError(reason))
                    .await;
                return true;
            }
            pool.process_protocol_error_frame(frame).await.is_err()
        }
        DirectLinkFrameKind::Close | DirectLinkFrameKind::CloseDirection => {
            let reason = frame.decode_close_reason();
            match frame.kind {
                DirectLinkFrameKind::CloseDirection => {
                    pool.close_logical_direction(&frame.link_id, frame.direction(), reason)
                        .await;
                }
                DirectLinkFrameKind::Close => {
                    pool.close_logical_link(&frame.link_id, reason).await;
                }
                _ => {}
            }
            false
        }
        DirectLinkFrameKind::Heartbeat => connection
            .write_frame(DirectLinkFrame::heartbeat_ack(frame.link_id))
            .await
            .is_err(),
        _ => false,
    }
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
mod tests;
