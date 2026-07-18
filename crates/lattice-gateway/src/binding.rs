use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    marker::PhantomData,
};

use async_trait::async_trait;
use lattice_actor::traits::Request;
use lattice_core::kind::ActorKind;
use prost::Message as ProstMessage;

use crate::{
    error::GatewayError,
    frame::ClientFrame,
    route::{GatewayRouteContext, GatewayRouteSpec, MessageRouter, RouteDecision},
};

#[async_trait]
pub trait GatewayRecipient<M>: Clone + Send + Sync + 'static
where
    M: Request,
{
    async fn ask(&self, route: RouteDecision, request: M) -> Result<M::Response, GatewayError>;
}

#[derive(Clone)]
pub struct ProstClientMessageBinding<M> {
    msg_id: u32,
    actor_kind: ActorKind,
    protocol_name: &'static str,
    _marker: PhantomData<fn() -> M>,
}

impl<M> Debug for ProstClientMessageBinding<M> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter
            .debug_struct("ProstClientMessageBinding")
            .field("msg_id", &self.msg_id)
            .field("actor_kind", &self.actor_kind)
            .field("protocol_name", &self.protocol_name)
            .finish_non_exhaustive()
    }
}

impl<M> ProstClientMessageBinding<M> {
    pub fn new(msg_id: u32, actor_kind: ActorKind, protocol_name: &'static str) -> Self {
        Self {
            msg_id,
            actor_kind,
            protocol_name,
            _marker: PhantomData,
        }
    }

    pub fn route_spec(&self) -> GatewayRouteSpec {
        GatewayRouteSpec {
            msg_id: self.msg_id,
            actor_kind: self.actor_kind.clone(),
            protocol_name: self.protocol_name,
        }
    }
}

impl<M> ProstClientMessageBinding<M>
where
    M: Request + ProstMessage + Default,
    M::Response: ProstMessage,
{
    pub async fn decode_and_forward<C, R>(
        &self,
        frame: ClientFrame,
        recipient: C,
        router: &mut R,
        context: &GatewayRouteContext,
    ) -> Result<ClientFrame, GatewayError>
    where
        C: GatewayRecipient<M>,
        R: MessageRouter,
    {
        if frame.msg_id != self.msg_id {
            return Err(GatewayError::UnexpectedMessageId {
                expected: self.msg_id,
                actual: frame.msg_id,
            });
        }
        let decision = router.route(context, &self.route_spec())?;
        let message = M::decode(frame.payload.as_slice())
            .map_err(|source| GatewayError::DecodePayload(source.to_string()))?;
        let reply = recipient.ask(decision, message).await?;
        Ok(ClientFrame {
            msg_id: self.msg_id,
            payload: reply.encode_to_vec(),
        })
    }
}
