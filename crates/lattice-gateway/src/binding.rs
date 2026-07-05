use std::marker::PhantomData;

use lattice_rpc::{RoutedRequest, RpcRequest, ShardedRpcCore};
use prost::Message as ProstMessage;

use crate::{ClientFrame, GatewayError, GatewayRouteSpec};

#[derive(Debug, Clone, Copy)]
pub struct ProstClientMessageBinding<Req> {
    msg_id: u32,
    _marker: PhantomData<Req>,
}

impl<Req> ProstClientMessageBinding<Req>
where
    Req: RoutedRequest + RpcRequest,
{
    pub const fn new(msg_id: u32) -> Self {
        Self {
            msg_id,
            _marker: PhantomData,
        }
    }

    pub fn route_spec(&self) -> GatewayRouteSpec {
        let default_req = Req::default();
        GatewayRouteSpec {
            msg_id: self.msg_id,
            actor_kind: default_req.actor_kind(),
            method: Req::METHOD,
        }
    }

    pub async fn decode_and_forward<C>(
        &self,
        frame: ClientFrame,
        core: C,
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
        let reply = core.call(req).await.map_err(GatewayError::Rpc)?;
        Ok(ClientFrame {
            msg_id: self.msg_id,
            payload: reply.encode_to_vec(),
        })
    }
}
