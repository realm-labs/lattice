use std::marker::PhantomData;
use std::sync::Arc;

use lattice_core::{ActorKind, RouteKey};
use lattice_rpc::{RoutedEnvelope, RoutedRequest, RpcRequest, ShardedRpcCore};
use prost::Message as ProstMessage;

use crate::{
    ClientFrame, GatewayError, GatewayRouteContext, GatewayRouteKeyPolicy, GatewayRouteSpec,
};

type RouteKeyExtractor<Req> =
    dyn Fn(&Req, &GatewayRouteContext) -> Result<RouteKey, GatewayError> + Send + Sync;

#[derive(Clone)]
pub struct ProstClientMessageBinding<Req> {
    msg_id: u32,
    actor_kind: ActorKind,
    route_key_policy: GatewayRouteKeyPolicy,
    route_key_extractor: Arc<RouteKeyExtractor<Req>>,
    _marker: PhantomData<Req>,
}

impl<Req> std::fmt::Debug for ProstClientMessageBinding<Req> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProstClientMessageBinding")
            .field("msg_id", &self.msg_id)
            .field("actor_kind", &self.actor_kind)
            .field("route_key_policy", &self.route_key_policy)
            .finish_non_exhaustive()
    }
}

impl<Req> ProstClientMessageBinding<Req> {
    pub fn with_route_extractor(
        msg_id: u32,
        actor_kind: ActorKind,
        route_key_policy: GatewayRouteKeyPolicy,
        route_key_extractor: impl Fn(&Req, &GatewayRouteContext) -> Result<RouteKey, GatewayError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            msg_id,
            actor_kind,
            route_key_policy,
            route_key_extractor: Arc::new(route_key_extractor),
            _marker: PhantomData,
        }
    }

    pub fn route_spec(&self) -> GatewayRouteSpec
    where
        Req: RpcRequest,
    {
        GatewayRouteSpec {
            msg_id: self.msg_id,
            actor_kind: self.actor_kind.clone(),
            route_key_policy: self.route_key_policy,
            method: <Req as RpcRequest>::METHOD,
        }
    }

    pub fn with_context_route_key(
        msg_id: u32,
        actor_kind: ActorKind,
        context_key: &'static str,
    ) -> Self {
        Self::with_route_extractor(
            msg_id,
            actor_kind,
            GatewayRouteKeyPolicy::context_key(context_key),
            move |_req, context| context.require_route_key(context_key),
        )
    }
}

impl<Req> ProstClientMessageBinding<Req>
where
    Req: RoutedRequest + RpcRequest,
{
    pub fn new(msg_id: u32) -> Self {
        let default_req = Req::default();
        Self::with_route_extractor(
            msg_id,
            default_req.actor_kind(),
            GatewayRouteKeyPolicy::request_field("<routed-request>"),
            |req, _context| Ok(req.route_key()),
        )
    }
}

impl<Req> ProstClientMessageBinding<Req>
where
    Req: RpcRequest,
{
    pub async fn decode_and_forward<C>(
        &self,
        frame: ClientFrame,
        core: C,
    ) -> Result<ClientFrame, GatewayError>
    where
        C: ShardedRpcCore,
    {
        self.decode_and_forward_with_context(frame, core, &GatewayRouteContext::new())
            .await
    }

    pub async fn decode_and_forward_with_context<C>(
        &self,
        frame: ClientFrame,
        core: C,
        context: &GatewayRouteContext,
    ) -> Result<ClientFrame, GatewayError>
    where
        C: ShardedRpcCore,
    {
        if frame.msg_id != self.msg_id {
            return Err(GatewayError::UnexpectedMessageId {
                expected: self.msg_id,
                actual: frame.msg_id,
            });
        }

        let req = Req::decode(frame.payload.as_slice())
            .map_err(|source| GatewayError::DecodePayload(source.to_string()))?;
        let route_key = self.route_key(&req, context)?;
        let reply = core
            .call_routed(RoutedEnvelope::new(req, self.actor_kind.clone(), route_key))
            .await
            .map_err(GatewayError::Rpc)?;
        Ok(ClientFrame {
            msg_id: self.msg_id,
            payload: reply.encode_to_vec(),
        })
    }

    fn route_key(
        &self,
        req: &Req,
        context: &GatewayRouteContext,
    ) -> Result<RouteKey, GatewayError> {
        (self.route_key_extractor)(req, context)
    }
}
