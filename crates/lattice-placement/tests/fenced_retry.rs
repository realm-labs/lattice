use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lattice_core::actor_ref::Epoch;
use lattice_core::id::RouteKey;
use lattice_core::instance::InstanceId;
use lattice_core::kind::ActorKind;
use lattice_core::{actor_kind, service_kind};
use lattice_placement::endpoint::{EndpointLease, EndpointPool};
use lattice_placement::error::PlacementError;
use lattice_placement::routing::resolver::{
    InvalidateReason, ResolveRequest, RouteCacheKey, RouteResolver,
};
use lattice_placement::routing::rpc::{EndpointRpcTransport, ResolvingRpcCore};
use lattice_rpc::error::RpcError;
use lattice_rpc::metadata::{RpcClientContextFactory, RpcContext};
use lattice_rpc::traits::{RoutedRequest, RpcRequest, ShardedRpcCore};
use lattice_rpc::types::RouteTarget;
use tonic::Response;

#[derive(Clone, PartialEq, prost::Message)]
struct EnterWorldRequest {
    #[prost(uint64, tag = "1")]
    world_id: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct EnterWorldReply {
    #[prost(bool, tag = "1")]
    ok: bool,
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
    const METHOD: &'static str = "WorldRpc/EnterWorld";
}

#[derive(Clone)]
struct SequencedResolver {
    targets: Arc<Mutex<VecDeque<RouteTarget>>>,
    invalidations: Arc<Mutex<Vec<InvalidateReason>>>,
}

#[async_trait]
impl RouteResolver for SequencedResolver {
    async fn resolve(&self, _request: ResolveRequest) -> Result<RouteTarget, PlacementError> {
        self.targets
            .lock()
            .unwrap()
            .pop_front()
            .ok_or(PlacementError::NoRoute)
    }

    async fn invalidate(&self, _key: RouteCacheKey, reason: InvalidateReason) {
        self.invalidations.lock().unwrap().push(reason);
    }
}

#[derive(Clone, Default)]
struct FencedThenOkTransport {
    request_ids: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl EndpointRpcTransport for FencedThenOkTransport {
    async fn unary<Req>(
        &self,
        _endpoint: EndpointLease,
        _target: RouteTarget,
        _route_key: &RouteKey,
        metadata: tonic::metadata::MetadataMap,
        _request: Req,
    ) -> Result<Response<Req::Reply>, RpcError>
    where
        Req: RpcRequest,
    {
        let ctx = RpcContext::from_metadata(&metadata)
            .map_err(|error| RpcError::Business(error.to_string()))?;
        let mut request_ids = self.request_ids.lock().unwrap();
        request_ids.push(ctx.request_id.as_str().to_string());
        if request_ids.len() == 1 {
            return Err(RpcError::Fenced {
                current_epoch: Epoch(2),
            });
        }
        Ok(Response::new(Req::Reply::default()))
    }
}

#[tokio::test]
async fn resolving_rpc_core_invalidates_fenced_owner_and_retries_same_request_id() {
    let resolver = SequencedResolver {
        targets: Arc::new(Mutex::new(VecDeque::from([
            target("world-a", 1),
            target("world-b", 2),
        ]))),
        invalidations: Arc::new(Mutex::new(Vec::new())),
    };
    let transport = FencedThenOkTransport::default();
    let request_ids = transport.request_ids.clone();
    let core = ResolvingRpcCore::new(
        service_kind!("World"),
        resolver.clone(),
        EndpointPool::new(),
        RpcClientContextFactory::new(service_kind!("Player"), InstanceId::new("player-0")),
        transport,
    );

    core.call(EnterWorldRequest { world_id: 7 }).await.unwrap();

    assert_eq!(
        *resolver.invalidations.lock().unwrap(),
        vec![InvalidateReason::Fenced]
    );
    let request_ids = request_ids.lock().unwrap();
    assert_eq!(request_ids.len(), 2);
    assert_eq!(request_ids[0], request_ids[1]);
}

fn target(instance_id: &str, epoch: u64) -> RouteTarget {
    RouteTarget {
        service_kind: service_kind!("World"),
        instance_id: InstanceId::new(instance_id),
        advertised_endpoint: format!("http://{instance_id}.world:18080").parse().unwrap(),
        owner_epoch: Some(Epoch(epoch)),
    }
}
