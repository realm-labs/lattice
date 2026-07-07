use async_trait::async_trait;
use lattice_actor::error::ActorError;
use lattice_actor::traits::{Actor, Handler};
use lattice_core::actor_kind;
use lattice_core::id::RouteKey;
use lattice_core::kind::ActorKind;
use lattice_rpc::traits::{RoutedRequest, RpcRequest};
use lattice_rpc::types::Rpc;

#[derive(Clone, PartialEq, prost::Message)]
pub struct EnterWorldRequest {
    #[prost(uint64, tag = "1")]
    pub world_id: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct EnterWorldReply {
    #[prost(bool, tag = "1")]
    pub ok: bool,
}

impl RoutedRequest for EnterWorldRequest {
    fn actor_kind(&self) -> ActorKind {
        actor_kind!("World")
    }

    fn route_key(&self) -> RouteKey {
        RouteKey::U64(self.world_id)
    }
}

impl RpcRequest for EnterWorldRequest {
    type Reply = EnterWorldReply;
    const METHOD: &'static str = "world.WorldRpc/EnterWorld";
}

struct WorldActor;

#[async_trait]
impl Actor for WorldActor {
    type Error = ActorError;
}

fn assert_generated_adapter_bound<A>()
where
    A: Actor + Handler<Rpc<EnterWorldRequest>>,
{
}

fn main() {
    assert_generated_adapter_bound::<WorldActor>();
}
