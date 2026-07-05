use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lattice_core::instance::InstanceCapacity;
use lattice_core::{ActorId, ActorKind, Epoch, InstanceId, RouteKey, actor_kind, service_kind};
use lattice_placement::coordinator::{
    ActivateActorRequest, ExplicitRouteResolver, FailoverReport, NoopLogicControl,
    PlacementCoordinator,
};
use lattice_placement::instance::{InstanceRecord, InstanceState};
use lattice_placement::store::{
    ActorPlacementKey, ActorPlacementRecord, InMemoryPlacementStore, LeaseId, PlacementPrefix,
    PlacementState, PlacementStore, SingletonKey, SingletonPlacementRecord,
};
use lattice_placement::{
    EndpointLease, EndpointPool, EndpointRpcTransport, PlacementError, ResolveRequest,
    ResolvingRpcCore, RouteResolver,
};
use lattice_rpc::{
    RouteTarget, RoutedRequest, RpcClientContextFactory, RpcContext, RpcError, RpcRequest,
    ShardedRpcCore,
};
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
    const METHOD: &'static str = "world.WorldRpc/EnterWorld";
}

#[tokio::test]
async fn node_crash_lease_expiry_reassigns_owned_actors_with_new_epoch() {
    let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/chaos"));
    store
        .upsert_instance(instance_record("world-a", InstanceState::Ready))
        .await
        .unwrap();
    store
        .upsert_instance(instance_record("world-b", InstanceState::Ready))
        .await
        .unwrap();
    let key = ActorPlacementKey {
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(7),
    };
    store
        .compare_and_put_actor(key.clone(), None, actor_record(7, "world-a", 3, LeaseId(9)))
        .await
        .unwrap();
    let singleton_key = SingletonKey {
        service_kind: service_kind!("World"),
        singleton_kind: actor_kind!("SeasonManager"),
        scope: "global".to_string(),
    };
    store
        .compare_and_put_singleton(
            singleton_key.clone(),
            None,
            singleton_record("global", "world-a", 5, LeaseId(11)),
        )
        .await
        .unwrap();
    let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);

    let report = coordinator
        .failover_expired_instance(service_kind!("World"), InstanceId::new("world-a"))
        .await
        .unwrap();
    let reassigned = store.get_actor(&key).await.unwrap().unwrap().1;
    let reassigned_singleton = store
        .get_singleton(&singleton_key)
        .await
        .unwrap()
        .unwrap()
        .1;

    assert_eq!(
        report,
        FailoverReport {
            failed_instance: InstanceId::new("world-a"),
            reassigned_actors: 1,
            reassigned_singletons: 1,
        }
    );
    assert_eq!(reassigned.owner, InstanceId::new("world-b"));
    assert_eq!(reassigned.epoch, Epoch(4));
    assert_eq!(reassigned.lease_id, LeaseId(10));
    assert_eq!(reassigned_singleton.owner, InstanceId::new("world-b"));
    assert_eq!(reassigned_singleton.epoch, Epoch(6));
    assert_eq!(
        store.instance_lease_keepalive_count(reassigned_singleton.lease_id),
        Some(0)
    );
}

#[tokio::test]
async fn stale_owner_recovery_after_lease_expiry_is_fenced_and_retried() {
    let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/chaos-retry"));
    store
        .upsert_instance(instance_record("world-a", InstanceState::Ready))
        .await
        .unwrap();
    store
        .upsert_instance(instance_record("world-b", InstanceState::Ready))
        .await
        .unwrap();
    let key = ActorPlacementKey {
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(7),
    };
    store
        .compare_and_put_actor(key.clone(), None, actor_record(7, "world-a", 3, LeaseId(9)))
        .await
        .unwrap();
    let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);
    let resolver = ExplicitRouteResolver::new(
        service_kind!("World"),
        store.clone(),
        coordinator.clone(),
        Default::default(),
    );
    let resolve_request = ResolveRequest {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        route_key: RouteKey::U64(7),
    };

    let cached = resolver.resolve(resolve_request.clone()).await.unwrap();
    assert_eq!(cached.instance_id, InstanceId::new("world-a"));
    assert_eq!(cached.owner_epoch, Some(Epoch(3)));

    coordinator
        .failover_expired_instance(service_kind!("World"), InstanceId::new("world-a"))
        .await
        .unwrap();
    let transport = FencingStoreTransport::new(store.clone(), key);
    let calls = transport.calls.clone();
    let core = ResolvingRpcCore::new(
        service_kind!("World"),
        resolver.clone(),
        EndpointPool::new(),
        RpcClientContextFactory::new(service_kind!("Player"), InstanceId::new("player-0")),
        transport,
    );

    core.call(EnterWorldRequest { world_id: 7 }).await.unwrap();

    assert_eq!(resolver.placement_lookups(), 2);
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].target_instance, InstanceId::new("world-a"));
    assert_eq!(calls[0].route_epoch, Some(Epoch(3)));
    assert_eq!(calls[1].target_instance, InstanceId::new("world-b"));
    assert_eq!(calls[1].route_epoch, Some(Epoch(4)));
    assert_eq!(calls[0].request_id, calls[1].request_id);
}

#[tokio::test]
async fn coordinator_leader_switch_rejects_stale_keepalive_and_continues_placement() {
    let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/chaos-leader"));
    store
        .upsert_instance(instance_record("world-a", InstanceState::Ready))
        .await
        .unwrap();
    store
        .upsert_instance(instance_record("world-b", InstanceState::Ready))
        .await
        .unwrap();
    let leader_a = store
        .campaign_coordinator_leader(InstanceId::new("coordinator-a"))
        .await
        .unwrap()
        .unwrap();
    let coordinator_a = PlacementCoordinator::new(store.clone(), NoopLogicControl);

    let first = coordinator_a
        .activate_actor(activate_request(7))
        .await
        .unwrap();
    store.resign_coordinator_leader(&leader_a).await.unwrap();
    let leader_b = store
        .campaign_coordinator_leader(InstanceId::new("coordinator-b"))
        .await
        .unwrap()
        .unwrap();
    let stale_keepalive = store.keepalive_coordinator_leader(&leader_a).await;
    store.keepalive_coordinator_leader(&leader_b).await.unwrap();
    let coordinator_b = PlacementCoordinator::new(store.clone(), NoopLogicControl);
    let second = coordinator_b
        .activate_actor(activate_request(8))
        .await
        .unwrap();

    assert_eq!(
        stale_keepalive,
        Err(PlacementError::CoordinatorLeadershipLost)
    );
    assert_eq!(store.coordinator_leader(), Some(leader_b));
    assert_eq!(first.owner, InstanceId::new("world-a"));
    assert_eq!(second.owner, InstanceId::new("world-a"));
    assert_eq!(store.list_actors().await.unwrap().len(), 2);
}

#[derive(Clone)]
struct FencingStoreTransport {
    store: InMemoryPlacementStore,
    key: ActorPlacementKey,
    calls: Arc<Mutex<Vec<ObservedCall>>>,
}

impl FencingStoreTransport {
    fn new(store: InMemoryPlacementStore, key: ActorPlacementKey) -> Self {
        Self {
            store,
            key,
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedCall {
    target_instance: InstanceId,
    route_epoch: Option<Epoch>,
    request_id: String,
}

#[async_trait]
impl EndpointRpcTransport for FencingStoreTransport {
    async fn unary<Req>(
        &self,
        _endpoint: EndpointLease,
        target: RouteTarget,
        metadata: tonic::metadata::MetadataMap,
        _request: &Req,
    ) -> Result<Response<Req::Reply>, RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        let ctx = RpcContext::from_metadata(&metadata)
            .map_err(|error| RpcError::Business(error.to_string()))?;
        self.calls.lock().unwrap().push(ObservedCall {
            target_instance: target.instance_id.clone(),
            route_epoch: ctx.route_epoch,
            request_id: ctx.request_id.as_str().to_string(),
        });
        let current = self
            .store
            .get_actor(&self.key)
            .await
            .map_err(|error| RpcError::Business(error.to_string()))?
            .map(|(_, record)| record)
            .ok_or_else(|| RpcError::Business("missing actor owner".to_string()))?;
        if target.instance_id != current.owner || ctx.route_epoch != Some(current.epoch) {
            return Err(RpcError::Fenced {
                current_epoch: current.epoch,
            });
        }
        Ok(Response::new(Req::Reply::default()))
    }
}

fn instance_record(instance_id: &str, state: InstanceState) -> InstanceRecord {
    InstanceRecord {
        service_kind: service_kind!("World"),
        instance_id: InstanceId::new(instance_id),
        lease_id: LeaseId(1),
        advertised_endpoint: format!("http://{instance_id}.world:18080").parse().unwrap(),
        control_endpoint: format!("http://{instance_id}.world:18081").parse().unwrap(),
        version: "test".to_string(),
        state,
        capacity: InstanceCapacity::default(),
        labels: BTreeMap::new(),
    }
}

fn actor_record(actor_id: u64, owner: &str, epoch: u64, lease_id: LeaseId) -> ActorPlacementRecord {
    ActorPlacementRecord {
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(actor_id),
        owner: InstanceId::new(owner),
        epoch: Epoch(epoch),
        lease_id,
        state: PlacementState::Running,
    }
}

fn activate_request(actor_id: u64) -> ActivateActorRequest {
    ActivateActorRequest {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(actor_id),
    }
}

fn singleton_record(
    scope: &str,
    owner: &str,
    epoch: u64,
    lease_id: LeaseId,
) -> SingletonPlacementRecord {
    SingletonPlacementRecord {
        service_kind: service_kind!("World"),
        singleton_kind: actor_kind!("SeasonManager"),
        scope: scope.to_string(),
        owner: InstanceId::new(owner),
        epoch: Epoch(epoch),
        lease_id,
        state: PlacementState::Running,
    }
}
