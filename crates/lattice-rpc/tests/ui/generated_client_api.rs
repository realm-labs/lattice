use async_trait::async_trait;
use lattice_core::actor_kind;
use lattice_core::id::RouteKey;
use lattice_core::kind::ActorKind;
use lattice_rpc::client::TypedRpcClient;
use lattice_rpc::error::RpcError;
use lattice_rpc::traits::{RoutedRequest, RpcRequest, ShardedRpcCore};

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

#[derive(Clone)]
struct GeneratedCore;

#[async_trait]
impl ShardedRpcCore for GeneratedCore {
    async fn call<Req>(&self, _req: Req) -> Result<Req::Reply, RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        Ok(Req::Reply::default())
    }
}

#[derive(Debug)]
pub struct WorldClient<C> {
    inner: TypedRpcClient<C>,
}

impl<C> WorldClient<C>
where
    C: ShardedRpcCore,
{
    pub fn new(core: C) -> Self {
        Self {
            inner: TypedRpcClient::new(core),
        }
    }

    pub async fn enter_world(&self, world_id: u64) -> Result<EnterWorldReply, RpcError> {
        self.inner.call(EnterWorldRequest { world_id }).await
    }
}

fn main() {
    let _client = WorldClient::new(GeneratedCore);
}
