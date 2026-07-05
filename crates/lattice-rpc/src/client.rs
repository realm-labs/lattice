use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use http::Uri;
use lattice_core::{ActorRef, Epoch};
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::metadata::RpcClientContextFactory;
use crate::{
    ActorRefRpcCore, RoutedRequest, RpcError, RpcRequest, ShardedRpcCore, UnaryRpcTransport,
};

#[derive(Debug, Default, Clone)]
pub struct TonicEndpointChannelPool {
    channels: Arc<DashMap<String, Channel>>,
}

impl TonicEndpointChannelPool {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn get_or_connect(&self, endpoint: &Uri) -> Result<Channel, RpcError> {
        let key = endpoint.to_string();
        if let Some(channel) = self.channels.get(&key).map(|entry| entry.clone()) {
            return Ok(channel);
        }

        let channel = Channel::builder(endpoint.clone())
            .connect()
            .await
            .map_err(|error| RpcError::Business(format!("connect {endpoint}: {error}")))?;
        match self.channels.entry(key) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => Ok(entry.insert(channel).clone()),
        }
    }
}

pub fn tonic_status_to_rpc_error(status: Status) -> RpcError {
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
        let mut request = Request::new(req);
        self.context_factory
            .next_context(self.route_epoch)
            .inject_metadata(request.metadata_mut())
            .map_err(|error| RpcError::Business(error.to_string()))?;
        self.transport
            .unary(request)
            .await
            .map(Response::into_inner)
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
