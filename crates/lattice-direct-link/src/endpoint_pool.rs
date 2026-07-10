pub(crate) mod connection;
pub(crate) mod helpers;
pub mod metrics;
pub(crate) mod sender;
pub(crate) mod state;

use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use lattice_core::actor_ref::ActorRef;
use lattice_core::direct_link::errors::LinkError;
use lattice_core::direct_link::ids::LinkId;
use lattice_core::direct_link::messages::{LinkClosed, LinkDirectionClosed};
use lattice_core::direct_link::options::{LinkCloseReason, LinkDirection};
use lattice_core::direct_link::runtime::{DirectLinkOpenRequest, DirectLinkSession};
use lattice_core::direct_link::target::DirectLinkEndpoint;
use tokio::sync::Mutex;

use crate::endpoint_pool::connection::{
    send_frame, send_frame_for_response, spawn_connection_task,
};
use crate::endpoint_pool::helpers::{
    endpoint_and_target, reject_reason_to_error, stable_stripe_index,
};
use crate::endpoint_pool::metrics::{
    DirectLinkEndpointPoolMetrics, DirectLinkEndpointPoolMetricsSnapshot,
};
use crate::endpoint_pool::sender::PooledDirectLinkSender;
use crate::endpoint_pool::state::{
    EndpointState, PoolState, PooledLinkClosure, PooledLinkState, PooledStripeState,
};
use crate::protocol::{DirectLinkFrame, DirectLinkFrameKind};
use crate::session::{
    DIRECT_LINK_PROTOCOL_VERSION, OpenLinkAck, OpenLinkDirection, OpenLinkRequest,
};
use crate::transport::DirectLinkTransport;

pub trait DirectLinkEndpointPoolLifecycle: Send + Sync + 'static {
    fn process_message_frame(&self, frame: DirectLinkFrame) -> Result<(), LinkError>;

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
    pub open_request: OpenLinkRequest,
    pub ack: OpenLinkAck,
    pub session: DirectLinkSession,
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

    pub async fn process_message_frame(&self, frame: DirectLinkFrame) -> Result<(), LinkError> {
        self.inner.process_message_frame(frame).await
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

pub(crate) struct PooledDirectLinkEndpointPoolInner<T>
where
    T: DirectLinkTransport,
{
    pub(crate) transport: T,
    pub(crate) config: DirectLinkEndpointPoolConfig,
    pub(crate) lifecycle: Option<Arc<dyn DirectLinkEndpointPoolLifecycle>>,
    pub(crate) state: Mutex<PoolState>,
    pub(crate) next_connection_id: AtomicU64,
    pub(crate) metrics: DirectLinkEndpointPoolMetrics,
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
                    accepted_message_ids: ack.source_to_target.accepted_message_type_ids.clone(),
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
                    open_request,
                    ack,
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

    async fn process_message_frame(&self, frame: DirectLinkFrame) -> Result<(), LinkError> {
        if frame.kind != DirectLinkFrameKind::Message {
            return Err(LinkError::Protocol(format!(
                "expected Message frame, got {:?}",
                frame.kind
            )));
        }
        let link = {
            let state = self.state.lock().await;
            state.links.get(&frame.link_id).cloned()
        }
        .ok_or_else(|| LinkError::Protocol(format!("unknown direct link {}", frame.link_id)))?;
        if !link.directions.contains_key(&frame.direction()) {
            return Err(LinkError::Protocol(format!(
                "direct link {} does not support direction {:?}",
                frame.link_id,
                frame.direction()
            )));
        }
        let Some(lifecycle) = &self.lifecycle else {
            return Err(LinkError::Protocol(
                "direct link endpoint pool has no lifecycle for inbound message frame".to_string(),
            ));
        };
        lifecycle.process_message_frame(frame)
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

#[cfg(test)]
mod tests;
