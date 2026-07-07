use std::collections::BTreeMap;

use async_trait::async_trait;
use lattice_core::actor_ref::{ActorRef, ActorRefTarget};
use lattice_core::id::{ActorId, RouteKey};
use lattice_core::kind::{ActorKind, ServiceKind};
use lattice_rpc::error::RpcError;
use lattice_rpc::metadata::{RpcClientContextFactory, RpcContext};
use lattice_rpc::traits::{ActorRefRpcCore, RoutedRequest, RpcRequest, ShardedRpcCore};
use lattice_rpc::types::{RouteTarget, RoutedEnvelope, RpcRoute};
use tonic::Response;
use tracing::{Instrument, debug, warn};

use crate::endpoint::{EndpointLease, EndpointPool};
use crate::error::PlacementError;
use crate::instance::{InMemoryInstanceRegistry, InstanceRegistry, InstanceState};
use crate::vshard::{VirtualShardAssignment, VirtualShardId, VirtualShardMapper};

#[derive(Debug, Clone)]
pub struct VirtualShardRouteTable {
    service_kind: ServiceKind,
    actor_kind: ActorKind,
    mapper: VirtualShardMapper,
    assignments: BTreeMap<VirtualShardId, VirtualShardAssignment>,
    instances: InMemoryInstanceRegistry,
}

impl VirtualShardRouteTable {
    pub fn new(
        service_kind: ServiceKind,
        actor_kind: ActorKind,
        mapper: VirtualShardMapper,
        assignments: Vec<VirtualShardAssignment>,
        instances: InMemoryInstanceRegistry,
    ) -> Self {
        Self {
            service_kind,
            actor_kind,
            mapper,
            assignments: assignments
                .into_iter()
                .map(|assignment| (assignment.shard_id, assignment))
                .collect(),
            instances,
        }
    }

    pub async fn resolve(&self, route_key: &RouteKey) -> Result<RouteTarget, PlacementError> {
        let shard_id = self.mapper.shard_for_route_key(route_key);
        let assignment = self
            .assignments
            .get(&shard_id)
            .ok_or(PlacementError::NoRoute)?;
        let instance = self
            .instances
            .get(&assignment.owner)
            .await?
            .ok_or_else(|| PlacementError::InstanceNotFound {
                instance_id: assignment.owner.clone(),
            })?;
        if instance.state != InstanceState::Ready {
            return Err(PlacementError::InstanceNotReady {
                instance_id: instance.instance_id,
                state: instance.state,
            });
        }
        Ok(RouteTarget {
            service_kind: self.service_kind.clone(),
            instance_id: instance.instance_id,
            advertised_endpoint: instance.advertised_endpoint,
            owner_epoch: Some(assignment.epoch),
        })
    }

    pub fn actor_kind(&self) -> &ActorKind {
        &self.actor_kind
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RouteCacheKey {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub route_key: RouteKey,
}

impl RouteCacheKey {
    pub fn new(service_kind: ServiceKind, actor_kind: ActorKind, route_key: RouteKey) -> Self {
        Self {
            service_kind,
            actor_kind,
            route_key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveRequest {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub route_key: RouteKey,
}

impl ResolveRequest {
    pub fn cache_key(&self) -> RouteCacheKey {
        RouteCacheKey::new(
            self.service_kind.clone(),
            self.actor_kind.clone(),
            self.route_key.clone(),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidateReason {
    NotOwner,
    Fenced,
    OwnerChanged,
    Manual,
}

#[async_trait]
pub trait RouteResolver: Clone + Send + Sync + 'static {
    async fn resolve(&self, request: ResolveRequest) -> Result<RouteTarget, PlacementError>;
    async fn invalidate(&self, key: RouteCacheKey, reason: InvalidateReason);
}

#[async_trait]
pub trait DynRouteResolver: Send + Sync + 'static {
    async fn resolve(&self, request: ResolveRequest) -> Result<RouteTarget, PlacementError>;
    async fn invalidate(&self, key: RouteCacheKey, reason: InvalidateReason);
}

#[async_trait]
impl<T> DynRouteResolver for T
where
    T: RouteResolver,
{
    async fn resolve(&self, request: ResolveRequest) -> Result<RouteTarget, PlacementError> {
        RouteResolver::resolve(self, request).await
    }

    async fn invalidate(&self, key: RouteCacheKey, reason: InvalidateReason) {
        RouteResolver::invalidate(self, key, reason).await;
    }
}

#[derive(Clone)]
pub struct BoxRouteResolver {
    inner: std::sync::Arc<dyn DynRouteResolver>,
}

impl std::fmt::Debug for BoxRouteResolver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BoxRouteResolver")
            .finish_non_exhaustive()
    }
}

impl BoxRouteResolver {
    pub fn new<R>(resolver: R) -> Self
    where
        R: RouteResolver,
    {
        Self {
            inner: std::sync::Arc::new(resolver),
        }
    }
}

#[async_trait]
impl RouteResolver for BoxRouteResolver {
    async fn resolve(&self, request: ResolveRequest) -> Result<RouteTarget, PlacementError> {
        self.inner.resolve(request).await
    }

    async fn invalidate(&self, key: RouteCacheKey, reason: InvalidateReason) {
        self.inner.invalidate(key, reason).await;
    }
}

#[async_trait]
pub trait EndpointRpcTransport: Clone + Send + Sync + 'static {
    async fn unary<Req>(
        &self,
        endpoint: EndpointLease,
        target: RouteTarget,
        route_key: &RouteKey,
        metadata: tonic::metadata::MetadataMap,
        request: Req,
    ) -> Result<Response<Req::Reply>, RpcError>
    where
        Req: RpcRequest;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RpcRetryPolicy {
    Disabled,
    #[default]
    RouteCorrection,
}

#[derive(Debug, Clone)]
pub struct ResolvingRpcCore<R, T> {
    service_kind: ServiceKind,
    resolver: R,
    endpoint_pool: EndpointPool,
    context_factory: RpcClientContextFactory,
    transport: T,
    retry_policy: RpcRetryPolicy,
}

impl<R, T> ResolvingRpcCore<R, T> {
    pub fn new(
        service_kind: ServiceKind,
        resolver: R,
        endpoint_pool: EndpointPool,
        context_factory: RpcClientContextFactory,
        transport: T,
    ) -> Self {
        Self {
            service_kind,
            resolver,
            endpoint_pool,
            context_factory,
            transport,
            retry_policy: RpcRetryPolicy::default(),
        }
    }

    pub fn with_retry_policy(mut self, retry_policy: RpcRetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }
}

#[async_trait]
impl<R, T> ShardedRpcCore for ResolvingRpcCore<R, T>
where
    R: RouteResolver,
    T: EndpointRpcTransport,
{
    async fn call_routed<Req>(&self, envelope: RoutedEnvelope<Req>) -> Result<Req::Reply, RpcError>
    where
        Req: RpcRequest,
    {
        let route = envelope.route();
        let span = tracing::info_span!(
            "rpc.client",
            otel.kind = "client",
            rpc.method = Req::METHOD,
            service.kind = self.service_kind.as_str(),
            actor.kind = route.actor_kind.as_str(),
            route.key = ?route.route_key
        );

        async {
            let resolve_request = ResolveRequest {
                service_kind: self.service_kind.clone(),
                actor_kind: route.actor_kind.clone(),
                route_key: route.route_key.clone(),
            };
            let key = resolve_request.cache_key();

            let target = self.resolve_rpc_target(resolve_request.clone()).await?;
            debug!(
                rpc.method = Req::METHOD,
                target.instance = target.instance_id.as_str(),
                target.endpoint = %target.advertised_endpoint,
                "resolved rpc target"
            );
            let ctx = self.context_factory.next_context(target.owner_epoch);
            if self.retry_policy == RpcRetryPolicy::Disabled {
                return self
                    .send_with_context(target, ctx, &route.route_key, &route, envelope.req)
                    .await;
            }

            let retry_request = <Req as prost::Message>::encode_to_vec(&envelope.req);
            match self
                .send_with_context(target, ctx.clone(), &route.route_key, &route, envelope.req)
                .await
            {
                Ok(reply) => Ok(reply),
                Err(RpcError::NotOwner { .. }) => {
                    warn!(
                        rpc.method = Req::METHOD,
                        retry.reason = "not_owner",
                        "rpc target rejected ownership; retrying"
                    );
                    let retry_span = tracing::info_span!(
                        "rpc.client.retry",
                        otel.kind = "client",
                        rpc.method = Req::METHOD,
                        retry.reason = "not_owner"
                    );
                    async {
                        self.resolver
                            .invalidate(key, InvalidateReason::NotOwner)
                            .await;
                        let retry_target = self.resolve_rpc_target(resolve_request).await?;
                        debug!(
                            rpc.method = Req::METHOD,
                            target.instance = retry_target.instance_id.as_str(),
                            target.endpoint = %retry_target.advertised_endpoint,
                            retry.reason = "not_owner",
                            "resolved retry rpc target"
                        );
                        let mut retry_ctx = ctx;
                        retry_ctx.route_epoch = retry_target.owner_epoch;
                        let retry_req = <Req as prost::Message>::decode(retry_request.as_slice())
                            .map_err(|error| RpcError::Business(error.to_string()))?;
                        self.send_with_context(
                            retry_target,
                            retry_ctx,
                            &route.route_key,
                            &route,
                            retry_req,
                        )
                        .await
                    }
                    .instrument(retry_span)
                    .await
                }
                Err(RpcError::Fenced { .. }) => {
                    warn!(
                        rpc.method = Req::METHOD,
                        retry.reason = "fenced",
                        "rpc target rejected route epoch; retrying"
                    );
                    let retry_span = tracing::info_span!(
                        "rpc.client.retry",
                        otel.kind = "client",
                        rpc.method = Req::METHOD,
                        retry.reason = "fenced"
                    );
                    async {
                        self.resolver
                            .invalidate(key, InvalidateReason::Fenced)
                            .await;
                        let retry_target = self.resolve_rpc_target(resolve_request).await?;
                        debug!(
                            rpc.method = Req::METHOD,
                            target.instance = retry_target.instance_id.as_str(),
                            target.endpoint = %retry_target.advertised_endpoint,
                            retry.reason = "fenced",
                            "resolved retry rpc target"
                        );
                        let mut retry_ctx = ctx;
                        retry_ctx.route_epoch = retry_target.owner_epoch;
                        let retry_req = <Req as prost::Message>::decode(retry_request.as_slice())
                            .map_err(|error| RpcError::Business(error.to_string()))?;
                        self.send_with_context(
                            retry_target,
                            retry_ctx,
                            &route.route_key,
                            &route,
                            retry_req,
                        )
                        .await
                    }
                    .instrument(retry_span)
                    .await
                }
                Err(error) => {
                    warn!(
                        rpc.method = Req::METHOD,
                        %error,
                        "rpc request failed without retry"
                    );
                    Err(error)
                }
            }
        }
        .instrument(span)
        .await
    }
}

impl<R, T> ResolvingRpcCore<R, T>
where
    R: RouteResolver,
    T: EndpointRpcTransport,
{
    async fn resolve_rpc_target(&self, request: ResolveRequest) -> Result<RouteTarget, RpcError> {
        self.resolver
            .resolve(request)
            .await
            .map_err(|error| RpcError::Business(error.to_string()))
    }

    async fn send_with_context<Req>(
        &self,
        target: RouteTarget,
        ctx: RpcContext,
        route_key: &RouteKey,
        route: &RpcRoute,
        req: Req,
    ) -> Result<Req::Reply, RpcError>
    where
        Req: RpcRequest,
    {
        let target_instance = target.instance_id.as_str().to_owned();
        let target_endpoint = target.advertised_endpoint.to_string();
        let endpoint = {
            let span = tracing::info_span!(
                "endpoint.pool.acquire",
                otel.kind = "internal",
                target.instance = target.instance_id.as_str(),
                target.endpoint = %target.advertised_endpoint
            );
            let _entered = span.enter();
            self.endpoint_pool.get_or_connect(&target)
        };
        let mut metadata = tonic::metadata::MetadataMap::new();
        ctx.inject_metadata(&mut metadata)
            .map_err(|error| RpcError::Business(error.to_string()))?;
        route
            .inject_metadata(&mut metadata)
            .map_err(|error| RpcError::Business(error.to_string()))?;
        debug!(
            rpc.method = Req::METHOD,
            request.id = ctx.request_id.as_str(),
            target.instance = target_instance.as_str(),
            target.endpoint = target_endpoint.as_str(),
            "sending routed rpc request"
        );
        match self
            .transport
            .unary(endpoint, target, route_key, metadata, req)
            .await
        {
            Ok(response) => {
                debug!(
                    rpc.method = Req::METHOD,
                    request.id = ctx.request_id.as_str(),
                    target.instance = target_instance.as_str(),
                    "routed rpc request completed"
                );
                Ok(response.into_inner())
            }
            Err(error) => {
                if matches!(&error, RpcError::NotOwner { .. } | RpcError::Fenced { .. }) {
                    debug!(
                        rpc.method = Req::METHOD,
                        request.id = ctx.request_id.as_str(),
                        target.instance = target_instance.as_str(),
                        %error,
                        "routed rpc request returned retryable error"
                    );
                } else {
                    warn!(
                        rpc.method = Req::METHOD,
                        request.id = ctx.request_id.as_str(),
                        target.instance = target_instance.as_str(),
                        %error,
                        "routed rpc request failed"
                    );
                }
                Err(error)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolvingActorRefRpcCore<R, T> {
    resolver: R,
    endpoint_pool: EndpointPool,
    context_factory: RpcClientContextFactory,
    transport: T,
}

impl<R, T> ResolvingActorRefRpcCore<R, T> {
    pub fn new(
        resolver: R,
        endpoint_pool: EndpointPool,
        context_factory: RpcClientContextFactory,
        transport: T,
    ) -> Self {
        Self {
            resolver,
            endpoint_pool,
            context_factory,
            transport,
        }
    }
}

#[async_trait]
impl<R, T> ActorRefRpcCore for ResolvingActorRefRpcCore<R, T>
where
    R: RouteResolver,
    T: EndpointRpcTransport,
{
    async fn call_ref<Req>(&self, actor_ref: ActorRef, req: Req) -> Result<Req::Reply, RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        validate_actor_ref_request(&actor_ref, &req)?;
        debug!(
            rpc.method = Req::METHOD,
            service.kind = actor_ref.service_kind.as_str(),
            actor.kind = actor_ref.actor_kind.as_str(),
            actor.id = ?actor_ref.actor_id,
            actor.ref.target = ?actor_ref.target,
            "calling actor ref"
        );
        let target = self.resolve_actor_ref_target(&actor_ref).await?;
        let ctx = self.context_factory.next_context(target.owner_epoch);
        self.send_with_context(target, ctx, req).await
    }
}

impl<R, T> ResolvingActorRefRpcCore<R, T>
where
    R: RouteResolver,
    T: EndpointRpcTransport,
{
    async fn resolve_actor_ref_target(
        &self,
        actor_ref: &ActorRef,
    ) -> Result<RouteTarget, RpcError> {
        match &actor_ref.target {
            ActorRefTarget::Direct {
                instance_id,
                endpoint,
                owner_epoch,
            } => Ok(RouteTarget {
                service_kind: actor_ref.service_kind.clone(),
                instance_id: instance_id.clone(),
                advertised_endpoint: endpoint.clone(),
                owner_epoch: *owner_epoch,
            }),
            ActorRefTarget::Routed => {
                let request = ResolveRequest {
                    service_kind: actor_ref.service_kind.clone(),
                    actor_kind: actor_ref.actor_kind.clone(),
                    route_key: actor_ref.actor_id.to_route_key(),
                };
                self.resolver
                    .resolve(request)
                    .await
                    .map_err(|error| RpcError::Business(error.to_string()))
            }
        }
    }

    async fn send_with_context<Req>(
        &self,
        target: RouteTarget,
        ctx: RpcContext,
        req: Req,
    ) -> Result<Req::Reply, RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        let target_instance = target.instance_id.as_str().to_owned();
        let target_endpoint = target.advertised_endpoint.to_string();
        let endpoint = self.endpoint_pool.get_or_connect(&target);
        let mut metadata = tonic::metadata::MetadataMap::new();
        ctx.inject_metadata(&mut metadata)
            .map_err(|error| RpcError::Business(error.to_string()))?;
        let route = RpcRoute::from_request(&req);
        route
            .inject_metadata(&mut metadata)
            .map_err(|error| RpcError::Business(error.to_string()))?;
        debug!(
            rpc.method = Req::METHOD,
            request.id = ctx.request_id.as_str(),
            target.instance = target_instance.as_str(),
            target.endpoint = target_endpoint.as_str(),
            "sending actor-ref rpc request"
        );
        let route_key = route.route_key;
        match self
            .transport
            .unary(endpoint, target, &route_key, metadata, req)
            .await
        {
            Ok(response) => {
                debug!(
                    rpc.method = Req::METHOD,
                    request.id = ctx.request_id.as_str(),
                    target.instance = target_instance.as_str(),
                    "actor-ref rpc request completed"
                );
                Ok(response.into_inner())
            }
            Err(error) => {
                warn!(
                    rpc.method = Req::METHOD,
                    request.id = ctx.request_id.as_str(),
                    target.instance = target_instance.as_str(),
                    %error,
                    "actor-ref rpc request failed"
                );
                Err(error)
            }
        }
    }
}

fn validate_actor_ref_request<Req>(actor_ref: &ActorRef, req: &Req) -> Result<(), RpcError>
where
    Req: RoutedRequest,
{
    if req.actor_kind() != actor_ref.actor_kind {
        return Err(RpcError::Business(format!(
            "actor ref kind {} does not match request kind {}",
            actor_ref.actor_kind.as_str(),
            req.actor_kind().as_str()
        )));
    }
    if !actor_id_matches_route_key(&actor_ref.actor_id, &req.route_key()) {
        return Err(RpcError::Business(format!(
            "actor ref id {:?} does not match request route key {:?}",
            actor_ref.actor_id,
            req.route_key()
        )));
    }
    Ok(())
}

fn actor_id_matches_route_key(actor_id: &ActorId, route_key: &RouteKey) -> bool {
    matches!(
        (actor_id, route_key),
        (ActorId::Str(left), RouteKey::Str(right)) if left == right
    ) || matches!(
        (actor_id, route_key),
        (ActorId::U64(left), RouteKey::U64(right)) if left == right
    ) || matches!(
        (actor_id, route_key),
        (ActorId::I64(left), RouteKey::I64(right)) if left == right
    ) || matches!(
        (actor_id, route_key),
        (ActorId::Bytes(left), RouteKey::Bytes(right)) if left == right
    )
}
