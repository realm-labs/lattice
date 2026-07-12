use async_trait::async_trait;
use lattice_actor::traits::Message;
use lattice_core::actor_kind;
use lattice_core::id::RouteKey;
use prost::Message as ProstMessage;

use crate::binding::{GatewayRecipient, ProstClientMessageBinding};
use crate::frame::{BinaryClientCodec, ClientCodec, ClientFrame};
use crate::route::{GatewayRouteContext, MessageRouter, RouteDecision};

#[derive(Clone, PartialEq, ProstMessage)]
struct Input {
    #[prost(uint64, tag = "1")]
    id: u64,
}

#[derive(Clone, PartialEq, ProstMessage)]
struct Output {
    #[prost(uint64, tag = "1")]
    id: u64,
}

impl Message for Input {
    type Reply = Output;
}

#[derive(Clone)]
struct FakeRecipient;

#[async_trait]
impl GatewayRecipient<Input> for FakeRecipient {
    async fn ask(
        &self,
        _route: RouteDecision,
        message: Input,
    ) -> Result<Output, crate::error::GatewayError> {
        Ok(Output { id: message.id + 1 })
    }
}

struct Router;

impl MessageRouter for Router {
    fn route(
        &mut self,
        context: &GatewayRouteContext,
        route: &crate::route::GatewayRouteSpec,
    ) -> Result<RouteDecision, crate::error::GatewayError> {
        Ok(RouteDecision::new(
            route.actor_kind.clone(),
            context.require_route_key("id")?,
        ))
    }
}

#[tokio::test]
async fn prost_binding_forwards_to_actor_recipient() {
    let binding = ProstClientMessageBinding::<Input>::new(7, actor_kind!("Target"), "target/v1");
    let frame = ClientFrame {
        msg_id: 7,
        payload: Input { id: 41 }.encode_to_vec(),
    };
    let reply = binding
        .decode_and_forward(
            frame,
            FakeRecipient,
            &mut Router,
            &GatewayRouteContext::new().with_route_key("id", RouteKey::U64(41)),
        )
        .await
        .unwrap();
    assert_eq!(Output::decode(reply.payload.as_slice()).unwrap().id, 42);
}

#[test]
fn binary_client_codec_round_trips() {
    let frame = ClientFrame {
        msg_id: 9,
        payload: vec![1, 2, 3],
    };
    let encoded = BinaryClientCodec.encode(frame.clone()).unwrap();
    assert_eq!(BinaryClientCodec.decode(&encoded).unwrap(), frame);
}
