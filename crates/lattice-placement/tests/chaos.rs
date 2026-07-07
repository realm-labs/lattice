use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lattice_core::actor_ref::Epoch;
use lattice_core::id::{ActorId, RouteKey};
use lattice_core::instance::InstanceCapacity;
use lattice_core::instance::InstanceId;
use lattice_core::kind::ActorKind;
use lattice_core::{actor_kind, service_kind};
use lattice_placement::coordinator::{
    ActivateActorRequest, ExplicitRouteResolver, FailoverReport, NoopLogicControl,
    PlacementCoordinator,
};
use lattice_placement::endpoint::{EndpointLease, EndpointPool};
use lattice_placement::error::PlacementError;
use lattice_placement::etcd::{
    EtcdKv, EtcdPlacementStore, EtcdValue, EtcdWatch, InMemoryEtcdClient,
};
use lattice_placement::instance::{InstanceRecord, InstanceState};
use lattice_placement::route::{
    EndpointRpcTransport, ResolveRequest, ResolvingRpcCore, RouteResolver,
};
use lattice_placement::singleton::{SingletonCoordinator, SingletonRouteResolver};
use lattice_placement::store::{
    ActorPlacementKey, ActorPlacementRecord, InMemoryPlacementStore, LeaseId, PlacementPrefix,
    PlacementState, PlacementStore, PlacementVersion, SingletonKey, SingletonPlacementRecord,
};
use lattice_rpc::error::RpcError;
use lattice_rpc::metadata::{RpcClientContextFactory, RpcContext};
use lattice_rpc::traits::{RoutedRequest, RpcRequest, ShardedRpcCore};
use lattice_rpc::types::RouteTarget;
use tokio::sync::Semaphore;
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

#[derive(Clone, PartialEq, prost::Message)]
struct SingletonTickRequest {
    #[prost(string, tag = "1")]
    scope: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct SingletonTickReply {
    #[prost(bool, tag = "1")]
    ok: bool,
}

impl RoutedRequest for SingletonTickRequest {
    fn actor_kind(&self) -> ActorKind {
        actor_kind!("SeasonManager")
    }

    fn route_key(&self) -> RouteKey {
        RouteKey::Str(self.scope.clone())
    }
}

impl RpcRequest for SingletonTickRequest {
    type Reply = SingletonTickReply;
    const METHOD: &'static str = "world.SeasonRpc/Tick";
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
        service_kind: service_kind!("World"),
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
        service_kind: service_kind!("World"),
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

#[tokio::test]
async fn temporary_etcd_outage_during_activation_is_retryable_without_partial_owner() {
    let client = FlakyEtcdClient::new(InMemoryEtcdClient::new());
    let store =
        EtcdPlacementStore::new(PlacementPrefix::new("/lattice/chaos-etcd"), client.clone());
    store
        .upsert_instance(instance_record("world-a", InstanceState::Ready))
        .await
        .unwrap();
    client.fail_next_compare_and_put();
    let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);
    let key = ActorPlacementKey {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(7),
    };

    let failed = coordinator.activate_actor(activate_request(7)).await;
    let retry = coordinator
        .activate_actor(activate_request(7))
        .await
        .unwrap();

    assert!(matches!(failed, Err(PlacementError::Etcd { .. })));
    assert!(store.get_actor(&key).await.unwrap().is_some());
    assert_eq!(store.list_actors().await.unwrap().len(), 1);
    assert_eq!(retry.owner, InstanceId::new("world-a"));
    assert_eq!(retry.epoch, Epoch(1));
}

#[tokio::test]
async fn partial_placement_write_failure_can_be_retried_to_complete_failover() {
    let client = FlakyEtcdClient::new(InMemoryEtcdClient::new());
    let store = EtcdPlacementStore::new(
        PlacementPrefix::new("/lattice/chaos-partial-write"),
        client.clone(),
    );
    store
        .upsert_instance(instance_record("world-a", InstanceState::Ready))
        .await
        .unwrap();
    store
        .upsert_instance(instance_record("world-b", InstanceState::Ready))
        .await
        .unwrap();
    let first_key = ActorPlacementKey {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(7),
    };
    let second_key = ActorPlacementKey {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(8),
    };
    store
        .compare_and_put_actor(
            first_key.clone(),
            None,
            actor_record(7, "world-a", 1, LeaseId(9)),
        )
        .await
        .unwrap();
    store
        .compare_and_put_actor(
            second_key.clone(),
            None,
            actor_record(8, "world-a", 1, LeaseId(10)),
        )
        .await
        .unwrap();
    client.fail_on_future_compare_and_put(2);
    let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);

    let failed = coordinator
        .failover_expired_instance(service_kind!("World"), InstanceId::new("world-a"))
        .await;
    let first_after_failure = store.get_actor(&first_key).await.unwrap().unwrap().1;
    let second_after_failure = store.get_actor(&second_key).await.unwrap().unwrap().1;
    let retry = coordinator
        .failover_expired_instance(service_kind!("World"), InstanceId::new("world-a"))
        .await
        .unwrap();
    let first_after_retry = store.get_actor(&first_key).await.unwrap().unwrap().1;
    let second_after_retry = store.get_actor(&second_key).await.unwrap().unwrap().1;

    assert!(matches!(failed, Err(PlacementError::Etcd { .. })));
    let owners_after_failure = [
        first_after_failure.owner.clone(),
        second_after_failure.owner.clone(),
    ];
    assert_eq!(
        owners_after_failure
            .iter()
            .filter(|owner| **owner == InstanceId::new("world-b"))
            .count(),
        1
    );
    assert_eq!(
        owners_after_failure
            .iter()
            .filter(|owner| **owner == InstanceId::new("world-a"))
            .count(),
        1
    );
    assert_eq!(retry.reassigned_actors, 1);
    assert_eq!(first_after_retry.owner, InstanceId::new("world-b"));
    assert_eq!(first_after_retry.epoch, Epoch(2));
    assert_eq!(second_after_retry.owner, InstanceId::new("world-b"));
    assert_eq!(second_after_retry.epoch, Epoch(2));
}

#[tokio::test]
async fn singleton_failover_during_long_job_fences_old_owner_and_retries() {
    let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/chaos-singleton-job"));
    store
        .upsert_instance(instance_record("world-a", InstanceState::Ready))
        .await
        .unwrap();
    store
        .upsert_instance(instance_record("world-b", InstanceState::Ready))
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
    let singleton_coordinator =
        SingletonCoordinator::new(service_kind!("World"), store.clone(), NoopLogicControl);
    let resolver = SingletonRouteResolver::new(singleton_coordinator, Default::default());
    let transport = LongSingletonJobTransport::new(store.clone(), singleton_key);
    let calls = transport.calls.clone();
    let core = ResolvingRpcCore::new(
        service_kind!("World"),
        resolver,
        EndpointPool::new(),
        RpcClientContextFactory::new(service_kind!("Player"), InstanceId::new("player-0")),
        transport.clone(),
    );
    let call = tokio::spawn(async move {
        core.call(SingletonTickRequest {
            scope: "global".to_string(),
        })
        .await
    });

    transport
        .first_call_entered
        .acquire()
        .await
        .unwrap()
        .forget();
    PlacementCoordinator::new(store.clone(), NoopLogicControl)
        .failover_expired_instance(service_kind!("World"), InstanceId::new("world-a"))
        .await
        .unwrap();
    transport.release_first_call.add_permits(1);
    call.await.unwrap().unwrap();

    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].target_instance, InstanceId::new("world-a"));
    assert_eq!(calls[0].route_epoch, Some(Epoch(5)));
    assert_eq!(calls[1].target_instance, InstanceId::new("world-b"));
    assert_eq!(calls[1].route_epoch, Some(Epoch(6)));
    assert_eq!(calls[0].request_id, calls[1].request_id);
}

#[tokio::test]
async fn rolling_update_with_mixed_versions_drains_old_owner_to_ready_new_version() {
    let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/chaos-rolling"));
    store
        .upsert_instance(instance_record_with_version(
            "world-a",
            InstanceState::Ready,
            "1.0.0",
        ))
        .await
        .unwrap();
    store
        .upsert_instance(instance_record_with_version(
            "world-b",
            InstanceState::Ready,
            "2.0.0",
        ))
        .await
        .unwrap();
    let key = ActorPlacementKey {
        service_kind: service_kind!("World"),
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

    let report = coordinator
        .drain_instance(service_kind!("World"), InstanceId::new("world-a"))
        .await
        .unwrap();
    let old_instance = store
        .get_instance(&InstanceId::new("world-a"))
        .await
        .unwrap()
        .unwrap();
    let new_instance = store
        .get_instance(&InstanceId::new("world-b"))
        .await
        .unwrap()
        .unwrap();
    let migrated = store.get_actor(&key).await.unwrap().unwrap().1;
    let target = resolver
        .resolve(ResolveRequest {
            service_kind: service_kind!("World"),
            actor_kind: actor_kind!("World"),
            route_key: RouteKey::U64(7),
        })
        .await
        .unwrap();

    assert_eq!(report.migrated_actors, 1);
    assert_eq!(old_instance.state, InstanceState::Draining);
    assert_eq!(old_instance.version, "1.0.0");
    assert_eq!(new_instance.state, InstanceState::Ready);
    assert_eq!(new_instance.version, "2.0.0");
    assert_eq!(migrated.owner, InstanceId::new("world-b"));
    assert_eq!(migrated.epoch, Epoch(4));
    assert_eq!(target.instance_id, InstanceId::new("world-b"));
    assert_eq!(target.owner_epoch, Some(Epoch(4)));
}

#[derive(Debug, Clone)]
struct FlakyEtcdClient {
    inner: InMemoryEtcdClient,
    compare_and_put_calls: Arc<AtomicUsize>,
    fail_on_compare_and_put_call: Arc<AtomicUsize>,
}

impl FlakyEtcdClient {
    fn new(inner: InMemoryEtcdClient) -> Self {
        Self {
            inner,
            compare_and_put_calls: Arc::new(AtomicUsize::new(0)),
            fail_on_compare_and_put_call: Arc::new(AtomicUsize::new(usize::MAX)),
        }
    }

    fn fail_next_compare_and_put(&self) {
        self.fail_on_future_compare_and_put(1);
    }

    fn fail_on_future_compare_and_put(&self, future_call: usize) {
        let current = self.compare_and_put_calls.load(Ordering::SeqCst);
        self.fail_on_compare_and_put_call
            .store(current + future_call, Ordering::SeqCst);
    }

    fn check_compare_and_put(&self) -> Result<(), PlacementError> {
        let call = self.compare_and_put_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_on_compare_and_put_call.load(Ordering::SeqCst) == call {
            self.fail_on_compare_and_put_call
                .store(usize::MAX, Ordering::SeqCst);
            return Err(PlacementError::Etcd {
                message: "temporary etcd outage".to_string(),
            });
        }
        Ok(())
    }
}

#[async_trait]
impl EtcdKv for FlakyEtcdClient {
    async fn put(&self, key: String, value: EtcdValue) -> Result<(), PlacementError> {
        self.inner.put(key, value).await
    }

    async fn get(
        &self,
        key: &str,
    ) -> Result<Option<(PlacementVersion, EtcdValue)>, PlacementError> {
        self.inner.get(key).await
    }

    async fn list_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, PlacementVersion, EtcdValue)>, PlacementError> {
        self.inner.list_prefix(prefix).await
    }

    async fn compare_and_put(
        &self,
        key: String,
        expected: Option<PlacementVersion>,
        value: EtcdValue,
    ) -> Result<PlacementVersion, PlacementError> {
        self.check_compare_and_put()?;
        self.inner.compare_and_put(key, expected, value).await
    }

    async fn delete(&self, key: &str) -> Result<(), PlacementError> {
        self.inner.delete(key).await
    }

    async fn compare_and_delete(
        &self,
        key: String,
        expected: EtcdValue,
    ) -> Result<(), PlacementError> {
        self.inner.compare_and_delete(key, expected).await
    }

    async fn grant_instance_lease(&self) -> Result<LeaseId, PlacementError> {
        self.inner.grant_instance_lease().await
    }

    async fn keepalive_instance_lease(&self, lease_id: LeaseId) -> Result<(), PlacementError> {
        self.inner.keepalive_instance_lease(lease_id).await
    }

    async fn next_lease_id(&self) -> Result<LeaseId, PlacementError> {
        self.inner.next_lease_id().await
    }

    async fn watch_prefix(&self, prefix: &str) -> Result<EtcdWatch, PlacementError> {
        self.inner.watch_prefix(prefix).await
    }
}

#[derive(Clone)]
struct FencingStoreTransport {
    store: InMemoryPlacementStore,
    key: ActorPlacementKey,
    calls: Arc<Mutex<Vec<ObservedCall>>>,
}

#[derive(Clone)]
struct LongSingletonJobTransport {
    store: InMemoryPlacementStore,
    key: SingletonKey,
    first_call_entered: Arc<Semaphore>,
    release_first_call: Arc<Semaphore>,
    calls: Arc<Mutex<Vec<ObservedCall>>>,
}

impl LongSingletonJobTransport {
    fn new(store: InMemoryPlacementStore, key: SingletonKey) -> Self {
        Self {
            store,
            key,
            first_call_entered: Arc::new(Semaphore::new(0)),
            release_first_call: Arc::new(Semaphore::new(0)),
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl EndpointRpcTransport for LongSingletonJobTransport {
    async fn unary<Req>(
        &self,
        _endpoint: EndpointLease,
        target: RouteTarget,
        _route_key: &RouteKey,
        metadata: tonic::metadata::MetadataMap,
        _request: Req,
    ) -> Result<Response<Req::Reply>, RpcError>
    where
        Req: RpcRequest,
    {
        let ctx = RpcContext::from_metadata(&metadata)
            .map_err(|error| RpcError::Business(error.to_string()))?;
        let call_index = {
            let mut calls = self.calls.lock().unwrap();
            calls.push(ObservedCall {
                target_instance: target.instance_id.clone(),
                route_epoch: ctx.route_epoch,
                request_id: ctx.request_id.as_str().to_string(),
            });
            calls.len()
        };
        if call_index == 1 {
            self.first_call_entered.add_permits(1);
            self.release_first_call.acquire().await.unwrap().forget();
        }
        let current = self
            .store
            .get_singleton(&self.key)
            .await
            .map_err(|error| RpcError::Business(error.to_string()))?
            .map(|(_, record)| record)
            .ok_or_else(|| RpcError::Business("missing singleton owner".to_string()))?;
        if target.instance_id != current.owner || ctx.route_epoch != Some(current.epoch) {
            return Err(RpcError::Fenced {
                current_epoch: current.epoch,
            });
        }
        Ok(Response::new(Req::Reply::default()))
    }
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
        _route_key: &RouteKey,
        metadata: tonic::metadata::MetadataMap,
        _request: Req,
    ) -> Result<Response<Req::Reply>, RpcError>
    where
        Req: RpcRequest,
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
    instance_record_with_version(instance_id, state, "test")
}

fn instance_record_with_version(
    instance_id: &str,
    state: InstanceState,
    version: &str,
) -> InstanceRecord {
    InstanceRecord {
        service_kind: service_kind!("World"),
        instance_id: InstanceId::new(instance_id),
        lease_id: LeaseId(1),
        advertised_endpoint: format!("http://{instance_id}.world:18080").parse().unwrap(),
        control_endpoint: format!("http://{instance_id}.world:18081").parse().unwrap(),
        version: version.to_string(),
        state,
        capacity: InstanceCapacity::default(),
        labels: BTreeMap::new(),
    }
}

fn actor_record(actor_id: u64, owner: &str, epoch: u64, lease_id: LeaseId) -> ActorPlacementRecord {
    ActorPlacementRecord {
        service_kind: service_kind!("World"),
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
