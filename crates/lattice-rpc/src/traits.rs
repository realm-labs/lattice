use async_trait::async_trait;
use lattice_core::{ActorKind, ActorRef, RouteKey};
use tonic::{Request, Response};

use crate::{RoutedEnvelope, RpcError, RpcRoute};

pub trait RoutedRequest {
    fn actor_kind(&self) -> ActorKind;
    fn route_key(&self) -> RouteKey;
}

pub trait RpcRequest: prost::Message + Default + Send + Sync + 'static {
    type Reply: prost::Message + Default + Send + Sync + 'static;
    const METHOD: &'static str;
}

#[async_trait]
pub trait ShardedRpcCore: Clone + Send + Sync + 'static {
    async fn call<Req>(&self, req: Req) -> Result<Req::Reply, RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        let route = RpcRoute::from_request(&req);
        self.call_routed(RoutedEnvelope::new(req, route.actor_kind, route.route_key))
            .await
    }

    async fn call_routed<Req>(&self, envelope: RoutedEnvelope<Req>) -> Result<Req::Reply, RpcError>
    where
        Req: RpcRequest,
    {
        let route = envelope.route();
        Err(RpcError::Business(format!(
            "rpc core does not support externally supplied route for {} actor {} key {:?}",
            Req::METHOD,
            route.actor_kind.as_str(),
            route.route_key
        )))
    }
}

#[async_trait]
pub trait ActorRefRpcCore: Clone + Send + Sync + 'static {
    async fn call_ref<Req>(&self, actor_ref: ActorRef, req: Req) -> Result<Req::Reply, RpcError>
    where
        Req: RoutedRequest + RpcRequest;

    async fn tell_ref<Req>(&self, actor_ref: ActorRef, req: Req) -> Result<(), RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        self.call_ref(actor_ref, req).await.map(|_| ())
    }
}

#[async_trait]
pub trait UnaryRpcTransport: Clone + Send + Sync + 'static {
    async fn unary<Req>(&self, request: Request<Req>) -> Result<Response<Req::Reply>, RpcError>
    where
        Req: RpcRequest;
}
