use std::marker::PhantomData;

use lattice_core::ActorKind;
use lattice_rpc::{RoutedEnvelope, RpcRequest, ShardedRpcCore};
use prost::Message as ProstMessage;

use crate::{ClientFrame, GatewayError, GatewayRouteContext, GatewayRouteSpec, MessageRouter};

#[derive(Clone)]
pub struct ProstClientMessageBinding<Req> {
    msg_id: u32,
    actor_kind: ActorKind,
    _marker: PhantomData<Req>,
}

impl<Req> std::fmt::Debug for ProstClientMessageBinding<Req> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProstClientMessageBinding")
            .field("msg_id", &self.msg_id)
            .field("actor_kind", &self.actor_kind)
            .finish_non_exhaustive()
    }
}

impl<Req> ProstClientMessageBinding<Req> {
    pub fn new(msg_id: u32, actor_kind: ActorKind) -> Self {
        Self {
            msg_id,
            actor_kind,
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
            method: <Req as RpcRequest>::METHOD,
        }
    }
}

impl<Req> ProstClientMessageBinding<Req>
where
    Req: RpcRequest,
{
    pub async fn decode_and_forward<C, R>(
        &self,
        frame: ClientFrame,
        core: C,
        router: &mut R,
        context: &GatewayRouteContext,
    ) -> Result<ClientFrame, GatewayError>
    where
        C: ShardedRpcCore,
        R: MessageRouter,
    {
        if frame.msg_id != self.msg_id {
            return Err(GatewayError::UnexpectedMessageId {
                expected: self.msg_id,
                actual: frame.msg_id,
            });
        }

        let route = self.route_spec();
        let decision = router.route(context, &route)?;
        let req = Req::decode(frame.payload.as_slice())
            .map_err(|source| GatewayError::DecodePayload(source.to_string()))?;
        let reply = core
            .call_routed(RoutedEnvelope::new(
                req,
                decision.actor_kind,
                decision.route_key,
            ))
            .await
            .map_err(GatewayError::Rpc)?;
        Ok(ClientFrame {
            msg_id: self.msg_id,
            payload: reply.encode_to_vec(),
        })
    }
}
