use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use http::Uri;
use lattice_core::{ActorRef, Epoch, RequestId};
use tonic::transport::Channel;
use tonic::{Request, Status};
use tracing::{debug, warn};

use crate::metadata::RpcClientContextFactory;
use crate::security::RpcTransportSecurity;
use crate::traits::UnaryRpcTransport;
use crate::{ActorRefRpcCore, RoutedRequest, RpcError, RpcRequest, ShardedRpcCore};

#[derive(Debug, Default, Clone)]
pub struct TonicEndpointChannelPool {
    channels: Arc<DashMap<String, Channel>>,
    transport_security: RpcTransportSecurity,
}

impl TonicEndpointChannelPool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_transport_security(transport_security: RpcTransportSecurity) -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
            transport_security,
        }
    }

    pub async fn get_or_connect(&self, endpoint: &Uri) -> Result<Channel, RpcError> {
        let key = endpoint.to_string();
        if let Some(channel) = self.channels.get(&key).map(|entry| entry.clone()) {
            debug!(%endpoint, "reusing tonic endpoint channel");
            return Ok(channel);
        }

        debug!(%endpoint, "connecting tonic endpoint channel");
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
        let channel = builder.connect().await.map_err(|error| {
            warn!(%endpoint, %error, "failed to connect tonic endpoint channel");
            RpcError::Business(format!("connect {endpoint}: {error}"))
        })?;
        match self.channels.entry(key) {
            Entry::Occupied(entry) => {
                debug!(%endpoint, "using concurrently established tonic endpoint channel");
                Ok(entry.get().clone())
            }
            Entry::Vacant(entry) => {
                debug!(%endpoint, "tonic endpoint channel connected");
                Ok(entry.insert(channel).clone())
            }
        }
    }
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
    async fn call<Req>(&self, req: Req) -> Result<Req::Reply, RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        let actor_kind = req.actor_kind();
        let route_key = req.route_key();
        let mut request = Request::new(req);
        let ctx = self.context_factory.next_context(self.route_epoch);
        debug!(
            rpc.method = Req::METHOD,
            actor.kind = actor_kind.as_str(),
            route.key = ?route_key,
            request.id = ctx.request_id.as_str(),
            "sending rpc request"
        );
        ctx.inject_metadata(request.metadata_mut())
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
