use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use http::Uri;
use lattice_core::actor_ref::{ActorRef, Epoch, RequestId};
use lattice_core::id::RouteKey;
use tokio::sync::OnceCell;
use tonic::transport::Channel;
use tonic::{Request, Status};
use tracing::{debug, warn};

use crate::error::RpcError;
use crate::metadata::RpcClientContextFactory;
use crate::security::RpcTransportSecurity;
use crate::traits::UnaryRpcTransport;
use crate::traits::{ActorRefRpcCore, RoutedRequest, RpcRequest, ShardedRpcCore};
use crate::types::RoutedEnvelope;

const DEFAULT_CHANNELS_PER_ENDPOINT: NonZeroUsize =
    NonZeroUsize::new(4).expect("default tonic channel stripe count is non-zero");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TonicEndpointChannelPoolConfig {
    channels_per_endpoint: NonZeroUsize,
}

impl TonicEndpointChannelPoolConfig {
    pub const fn new(channels_per_endpoint: NonZeroUsize) -> Self {
        Self {
            channels_per_endpoint,
        }
    }

    pub fn try_new(channels_per_endpoint: usize) -> Option<Self> {
        NonZeroUsize::new(channels_per_endpoint).map(Self::new)
    }

    pub fn channels_per_endpoint(self) -> NonZeroUsize {
        self.channels_per_endpoint
    }
}

impl Default for TonicEndpointChannelPoolConfig {
    fn default() -> Self {
        Self {
            channels_per_endpoint: DEFAULT_CHANNELS_PER_ENDPOINT,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TonicEndpointChannelPool {
    channels: Arc<DashMap<Uri, Arc<EndpointChannelStripes>>>,
    transport_security: RpcTransportSecurity,
    config: TonicEndpointChannelPoolConfig,
}

#[derive(Debug)]
struct EndpointChannelStripes {
    endpoint: Uri,
    channels: Vec<OnceCell<Channel>>,
}

impl EndpointChannelStripes {
    fn new(endpoint: Uri, config: TonicEndpointChannelPoolConfig) -> Self {
        let channels = (0..config.channels_per_endpoint().get())
            .map(|_| OnceCell::new())
            .collect();
        Self { endpoint, channels }
    }
}

impl Default for TonicEndpointChannelPool {
    fn default() -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
            transport_security: RpcTransportSecurity::default(),
            config: TonicEndpointChannelPoolConfig::default(),
        }
    }
}

impl TonicEndpointChannelPool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_transport_security(transport_security: RpcTransportSecurity) -> Self {
        Self::with_transport_config(
            transport_security,
            TonicEndpointChannelPoolConfig::default(),
        )
    }

    pub fn with_transport_config(
        transport_security: RpcTransportSecurity,
        config: TonicEndpointChannelPoolConfig,
    ) -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
            transport_security,
            config,
        }
    }

    pub fn config(&self) -> TonicEndpointChannelPoolConfig {
        self.config
    }

    pub async fn get_or_connect(&self, endpoint: &Uri) -> Result<Channel, RpcError> {
        self.get_or_connect_stripe(endpoint, 0).await
    }

    pub async fn get_or_connect_for_route_key(
        &self,
        endpoint: &Uri,
        route_key: &RouteKey,
    ) -> Result<Channel, RpcError> {
        let stripe = self.stripe_index_for(route_key);
        self.get_or_connect_stripe(endpoint, stripe).await
    }

    pub async fn get_or_connect_for_request_id(
        &self,
        endpoint: &Uri,
        request_id: &RequestId,
    ) -> Result<Channel, RpcError> {
        let stripe = self.stripe_index_for(request_id);
        self.get_or_connect_stripe(endpoint, stripe).await
    }

    pub fn stripe_index_for<T>(&self, key: &T) -> usize
    where
        T: Hash + ?Sized,
    {
        stripe_index_for_hash(key, self.config.channels_per_endpoint)
    }

    async fn get_or_connect_stripe(
        &self,
        endpoint: &Uri,
        stripe: usize,
    ) -> Result<Channel, RpcError> {
        let stripes = self.endpoint_stripes(endpoint);
        let stripe = stripe % stripes.channels.len();
        let channel = stripes.channels[stripe]
            .get_or_try_init(|| async { self.connect_channel(&stripes.endpoint, stripe).await })
            .await?;
        debug!(%endpoint, channel.stripe = stripe, "reusing tonic endpoint channel");
        Ok(channel.clone())
    }

    fn endpoint_stripes(&self, endpoint: &Uri) -> Arc<EndpointChannelStripes> {
        if let Some(stripes) = self.channels.get(endpoint).map(|entry| entry.clone()) {
            return stripes;
        }

        match self.channels.entry(endpoint.clone()) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => entry
                .insert(Arc::new(EndpointChannelStripes::new(
                    endpoint.clone(),
                    self.config,
                )))
                .clone(),
        }
    }

    async fn connect_channel(&self, endpoint: &Uri, stripe: usize) -> Result<Channel, RpcError> {
        debug!(%endpoint, channel.stripe = stripe, "connecting tonic endpoint channel");
        let mut builder = Channel::builder(endpoint.clone());
        if let Some(tls) = self
            .transport_security
            .client_tls_config(endpoint)
            .map_err(RpcError::Business)?
        {
            builder = builder.tls_config(tls).map_err(|error| {
                RpcError::Business(format!("configure TLS for {endpoint}: {error}"))
            })?;
        }
        builder.connect().await.map_err(|error| {
            warn!(%endpoint, %error, "failed to connect tonic endpoint channel");
            RpcError::Business(format!("connect {endpoint}: {error}"))
        })
    }
}

fn stripe_index_for_hash<T>(key: &T, stripe_count: NonZeroUsize) -> usize
where
    T: Hash + ?Sized,
{
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % stripe_count.get()
}

pub fn tonic_status_to_rpc_error_for_request(
    status: Status,
    method: &'static str,
    request_id: RequestId,
) -> RpcError {
    if status.code() == tonic::Code::FailedPrecondition
        && (status.message().contains("owner") || status.message().contains("epoch"))
    {
        return RpcError::Fenced {
            current_epoch: Epoch(0),
        };
    }

    if matches!(
        status.code(),
        tonic::Code::Cancelled
            | tonic::Code::Unknown
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Unavailable
    ) {
        return RpcError::UnknownResult {
            method,
            request_id,
            message: status.to_string(),
        };
    }

    if status.code() == tonic::Code::AlreadyExists
        && status.message().contains("duplicate request id")
    {
        return RpcError::UnknownResult {
            method,
            request_id,
            message: status.to_string(),
        };
    }

    RpcError::Business(status.to_string())
}

#[derive(Debug, Clone)]
pub struct MetadataInjectingRpcCore<T> {
    transport: T,
    context_factory: RpcClientContextFactory,
    route_epoch: Option<Epoch>,
}

impl<T> MetadataInjectingRpcCore<T> {
    pub fn new(transport: T, context_factory: RpcClientContextFactory) -> Self {
        Self {
            transport,
            context_factory,
            route_epoch: None,
        }
    }

    pub fn with_route_epoch(mut self, route_epoch: Epoch) -> Self {
        self.route_epoch = Some(route_epoch);
        self
    }
}

#[async_trait]
impl<T> ShardedRpcCore for MetadataInjectingRpcCore<T>
where
    T: UnaryRpcTransport,
{
    async fn call_routed<Req>(&self, envelope: RoutedEnvelope<Req>) -> Result<Req::Reply, RpcError>
    where
        Req: RpcRequest,
    {
        let route = envelope.route();
        let mut request = Request::new(envelope.req);
        let ctx = self.context_factory.next_context(self.route_epoch);
        debug!(
            rpc.method = Req::METHOD,
            actor.kind = route.actor_kind.as_str(),
            route.key = ?route.route_key,
            request.id = ctx.request_id.as_str(),
            "sending rpc request"
        );
        ctx.inject_metadata(request.metadata_mut())
            .map_err(|error| RpcError::Business(error.to_string()))?;
        route
            .inject_metadata(request.metadata_mut())
            .map_err(|error| RpcError::Business(error.to_string()))?;
        match self.transport.unary(request).await {
            Ok(response) => {
                debug!(
                    rpc.method = Req::METHOD,
                    request.id = ctx.request_id.as_str(),
                    "rpc request completed"
                );
                Ok(response.into_inner())
            }
            Err(error) => {
                warn!(
                    rpc.method = Req::METHOD,
                    request.id = ctx.request_id.as_str(),
                    %error,
                    "rpc request failed"
                );
                Err(error)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct TypedRpcClient<C> {
    core: C,
}

impl<C> TypedRpcClient<C> {
    pub fn new(core: C) -> Self {
        Self { core }
    }

    pub fn core(&self) -> &C {
        &self.core
    }
}

impl<C> TypedRpcClient<C>
where
    C: ShardedRpcCore,
{
    pub async fn call<Req>(&self, req: Req) -> Result<Req::Reply, RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        self.core.call(req).await
    }
}

#[derive(Debug, Clone)]
pub struct ActorRefRpcClient<C> {
    core: C,
}

impl<C> ActorRefRpcClient<C> {
    pub fn new(core: C) -> Self {
        Self { core }
    }

    pub fn core(&self) -> &C {
        &self.core
    }
}

impl<C> ActorRefRpcClient<C>
where
    C: ActorRefRpcCore,
{
    pub async fn call_ref<Req>(&self, actor_ref: ActorRef, req: Req) -> Result<Req::Reply, RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        self.core.call_ref(actor_ref, req).await
    }

    pub async fn tell_ref<Req>(&self, actor_ref: ActorRef, req: Req) -> Result<(), RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        self.core.tell_ref(actor_ref, req).await
    }
}
