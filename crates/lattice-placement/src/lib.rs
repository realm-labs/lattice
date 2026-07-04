mod coordinator;
mod instance;
mod store;
mod vshard;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use lattice_core::{ActorKind, InstanceId, RouteKey, ServiceKind};
use lattice_rpc::{
    RouteTarget, RoutedRequest, RpcClientContextFactory, RpcContext, RpcError, RpcRequest,
    ShardedRpcCore,
};
use tonic::{Request, Response};

pub use coordinator::*;
pub use instance::*;
pub use store::*;
pub use vshard::*;

#[derive(Debug, Clone)]
pub struct VirtualShardRouteTable {
    service_kind: ServiceKind,
    actor_kind: ActorKind,
    mapper: VirtualShardMapper,
    assignments: BTreeMap<VirtualShardId, VirtualShardAssignment>,
    instances: InMemoryInstanceRegistry,
}

impl VirtualShardRouteTable {
    pub fn new(
        service_kind: ServiceKind,
        actor_kind: ActorKind,
        mapper: VirtualShardMapper,
        assignments: Vec<VirtualShardAssignment>,
        instances: InMemoryInstanceRegistry,
    ) -> Self {
        Self {
            service_kind,
            actor_kind,
            mapper,
            assignments: assignments
                .into_iter()
                .map(|assignment| (assignment.shard_id, assignment))
                .collect(),
            instances,
        }
    }

    pub async fn resolve(&self, route_key: &RouteKey) -> Result<RouteTarget, PlacementError> {
        let shard_id = self.mapper.shard_for_route_key(route_key);
        let assignment = self
            .assignments
            .get(&shard_id)
            .ok_or(PlacementError::NoRoute)?;
        let instance = self
            .instances
            .get(&assignment.owner)
            .await?
            .ok_or_else(|| PlacementError::InstanceNotFound {
                instance_id: assignment.owner.clone(),
            })?;
        if instance.state != InstanceState::Ready {
            return Err(PlacementError::InstanceNotReady {
                instance_id: instance.instance_id,
                state: instance.state,
            });
        }
        Ok(RouteTarget {
            service_kind: self.service_kind.clone(),
            instance_id: instance.instance_id,
            advertised_endpoint: instance.advertised_endpoint,
            owner_epoch: Some(assignment.epoch),
        })
    }

    pub fn actor_kind(&self) -> &ActorKind {
        &self.actor_kind
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RouteCacheKey {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub route_key: RouteKey,
}

impl RouteCacheKey {
    pub fn new(service_kind: ServiceKind, actor_kind: ActorKind, route_key: RouteKey) -> Self {
        Self {
            service_kind,
            actor_kind,
            route_key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveRequest {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub route_key: RouteKey,
}

impl ResolveRequest {
    pub fn cache_key(&self) -> RouteCacheKey {
        RouteCacheKey::new(
            self.service_kind.clone(),
            self.actor_kind.clone(),
            self.route_key.clone(),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidateReason {
    NotOwner,
    Fenced,
    OwnerChanged,
    Manual,
}

#[async_trait]
pub trait RouteResolver: Clone + Send + Sync + 'static {
    async fn resolve(&self, request: ResolveRequest) -> Result<RouteTarget, PlacementError>;
    async fn invalidate(&self, key: RouteCacheKey, reason: InvalidateReason);
}

#[async_trait]
pub trait EndpointRpcTransport: Clone + Send + Sync + 'static {
    async fn unary<Req>(
        &self,
        endpoint: EndpointLease,
        target: RouteTarget,
        request: Request<Req>,
    ) -> Result<Response<Req::Reply>, RpcError>
    where
        Req: RoutedRequest + RpcRequest;
}

#[derive(Debug, Clone)]
pub struct ResolvingRpcCore<R, T> {
    service_kind: ServiceKind,
    resolver: R,
    endpoint_pool: EndpointPool,
    context_factory: RpcClientContextFactory,
    transport: T,
}

impl<R, T> ResolvingRpcCore<R, T> {
    pub fn new(
        service_kind: ServiceKind,
        resolver: R,
        endpoint_pool: EndpointPool,
        context_factory: RpcClientContextFactory,
        transport: T,
    ) -> Self {
        Self {
            service_kind,
            resolver,
            endpoint_pool,
            context_factory,
            transport,
        }
    }
}

#[async_trait]
impl<R, T> ShardedRpcCore for ResolvingRpcCore<R, T>
where
    R: RouteResolver,
    T: EndpointRpcTransport,
{
    async fn call<Req>(&self, req: Req) -> Result<Req::Reply, RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        let resolve_request = ResolveRequest {
            service_kind: self.service_kind.clone(),
            actor_kind: req.actor_kind(),
            route_key: req.route_key(),
        };
        let key = resolve_request.cache_key();
        let encoded = req.encode_to_vec();

        let target = self.resolve_rpc_target(resolve_request.clone()).await?;
        let ctx = self.context_factory.next_context(target.owner_epoch);
        match self
            .send_with_context(target, ctx.clone(), decode_request::<Req>(&encoded)?)
            .await
        {
            Ok(reply) => Ok(reply),
            Err(RpcError::NotOwner { .. }) => {
                self.resolver
                    .invalidate(key, InvalidateReason::NotOwner)
                    .await;
                let retry_target = self.resolve_rpc_target(resolve_request).await?;
                let mut retry_ctx = ctx;
                retry_ctx.route_epoch = retry_target.owner_epoch;
                self.send_with_context(retry_target, retry_ctx, decode_request::<Req>(&encoded)?)
                    .await
            }
            Err(RpcError::Fenced { .. }) => {
                self.resolver
                    .invalidate(key, InvalidateReason::Fenced)
                    .await;
                let retry_target = self.resolve_rpc_target(resolve_request).await?;
                let mut retry_ctx = ctx;
                retry_ctx.route_epoch = retry_target.owner_epoch;
                self.send_with_context(retry_target, retry_ctx, decode_request::<Req>(&encoded)?)
                    .await
            }
            Err(error) => Err(error),
        }
    }
}

impl<R, T> ResolvingRpcCore<R, T>
where
    R: RouteResolver,
    T: EndpointRpcTransport,
{
    async fn resolve_rpc_target(&self, request: ResolveRequest) -> Result<RouteTarget, RpcError> {
        self.resolver
            .resolve(request)
            .await
            .map_err(|error| RpcError::Business(error.to_string()))
    }

    async fn send_with_context<Req>(
        &self,
        target: RouteTarget,
        ctx: RpcContext,
        req: Req,
    ) -> Result<Req::Reply, RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        let endpoint = self.endpoint_pool.get_or_connect(&target);
        let mut request = Request::new(req);
        ctx.inject_metadata(request.metadata_mut())
            .map_err(|error| RpcError::Business(error.to_string()))?;
        self.transport
            .unary(endpoint, target, request)
            .await
            .map(Response::into_inner)
    }
}

fn decode_request<Req>(bytes: &[u8]) -> Result<Req, RpcError>
where
    Req: RpcRequest,
{
    Req::decode(bytes).map_err(|error| RpcError::Business(error.to_string()))
}

#[derive(Debug, Clone)]
pub struct RouteCacheConfig {
    pub soft_ttl: Duration,
    pub hard_ttl: Duration,
}

impl Default for RouteCacheConfig {
    fn default() -> Self {
        Self {
            soft_ttl: Duration::from_secs(5),
            hard_ttl: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocalRouteCache {
    config: RouteCacheConfig,
    entries: HashMap<RouteCacheKey, RouteCacheEntry>,
}

impl LocalRouteCache {
    pub fn new(config: RouteCacheConfig) -> Self {
        assert!(
            config.soft_ttl <= config.hard_ttl,
            "soft ttl must not exceed hard ttl"
        );
        Self {
            config,
            entries: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: RouteCacheKey, target: RouteTarget) {
        self.entries.insert(
            key,
            RouteCacheEntry {
                target,
                inserted_at: Instant::now(),
            },
        );
    }

    pub fn get(&mut self, key: &RouteCacheKey) -> CacheLookup {
        let Some(entry) = self.entries.get(key) else {
            return CacheLookup::Miss;
        };
        match entry.state(&self.config) {
            CacheEntryState::Fresh => CacheLookup::Fresh(entry.target.clone()),
            CacheEntryState::Stale => CacheLookup::Stale(entry.target.clone()),
            CacheEntryState::Expired => {
                self.entries.remove(key);
                CacheLookup::Miss
            }
        }
    }

    pub fn invalidate(&mut self, key: &RouteCacheKey) {
        self.entries.remove(key);
    }
}

impl Default for LocalRouteCache {
    fn default() -> Self {
        Self::new(RouteCacheConfig::default())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheLookup {
    Fresh(RouteTarget),
    Stale(RouteTarget),
    Miss,
}

#[derive(Debug, Clone)]
struct RouteCacheEntry {
    target: RouteTarget,
    inserted_at: Instant,
}

impl RouteCacheEntry {
    fn state(&self, config: &RouteCacheConfig) -> CacheEntryState {
        let age = self.inserted_at.elapsed();
        if age < config.soft_ttl {
            CacheEntryState::Fresh
        } else if age < config.hard_ttl {
            CacheEntryState::Stale
        } else {
            CacheEntryState::Expired
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CacheEntryState {
    Fresh,
    Stale,
    Expired,
}

#[derive(Debug, Clone)]
pub struct StaticPlacementConfig {
    pub ranges: Vec<StaticRouteRange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticRouteRange {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub start_inclusive: u64,
    pub end_exclusive: u64,
    pub target: RouteTarget,
}

#[derive(Debug, Clone)]
pub struct StaticRouteResolver {
    config: StaticPlacementConfig,
    cache: Arc<std::sync::Mutex<LocalRouteCache>>,
    placement_lookups: Arc<AtomicU64>,
}

impl StaticRouteResolver {
    pub fn new(config: StaticPlacementConfig, cache_config: RouteCacheConfig) -> Self {
        Self {
            config,
            cache: Arc::new(std::sync::Mutex::new(LocalRouteCache::new(cache_config))),
            placement_lookups: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn placement_lookups(&self) -> u64 {
        self.placement_lookups.load(Ordering::SeqCst)
    }

    fn resolve_from_static_config(
        &self,
        request: &ResolveRequest,
    ) -> Result<RouteTarget, PlacementError> {
        self.placement_lookups.fetch_add(1, Ordering::SeqCst);
        let RouteKey::U64(value) = request.route_key else {
            return Err(PlacementError::UnsupportedRouteKey);
        };

        self.config
            .ranges
            .iter()
            .find(|range| {
                range.service_kind == request.service_kind
                    && range.actor_kind == request.actor_kind
                    && range.start_inclusive <= value
                    && value < range.end_exclusive
            })
            .map(|range| range.target.clone())
            .ok_or(PlacementError::NoRoute)
    }
}

#[async_trait]
impl RouteResolver for StaticRouteResolver {
    async fn resolve(&self, request: ResolveRequest) -> Result<RouteTarget, PlacementError> {
        let key = request.cache_key();
        {
            let mut cache = self.cache.lock().expect("route cache mutex poisoned");
            match cache.get(&key) {
                CacheLookup::Fresh(target) | CacheLookup::Stale(target) => return Ok(target),
                CacheLookup::Miss => {}
            }
        }

        let target = self.resolve_from_static_config(&request)?;
        self.cache
            .lock()
            .expect("route cache mutex poisoned")
            .insert(key, target.clone());
        Ok(target)
    }

    async fn invalidate(&self, key: RouteCacheKey, _reason: InvalidateReason) {
        self.cache
            .lock()
            .expect("route cache mutex poisoned")
            .invalidate(&key);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EndpointPoolKey {
    pub instance_id: InstanceId,
    pub advertised_endpoint: String,
}

impl EndpointPoolKey {
    pub fn from_target(target: &RouteTarget) -> Self {
        Self {
            instance_id: target.instance_id.clone(),
            advertised_endpoint: target.advertised_endpoint.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointLease {
    pub key: EndpointPoolKey,
    pub connection_id: u64,
}

#[derive(Debug, Default, Clone)]
pub struct EndpointPool {
    connections: Arc<std::sync::Mutex<HashMap<EndpointPoolKey, EndpointLease>>>,
    next_connection_id: Arc<AtomicU64>,
}

impl EndpointPool {
    pub fn new() -> Self {
        Self {
            connections: Arc::new(std::sync::Mutex::new(HashMap::new())),
            next_connection_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn get_or_connect(&self, target: &RouteTarget) -> EndpointLease {
        let key = EndpointPoolKey::from_target(target);
        let mut connections = self
            .connections
            .lock()
            .expect("endpoint pool mutex poisoned");
        connections
            .entry(key.clone())
            .or_insert_with(|| EndpointLease {
                key,
                connection_id: self.next_connection_id.fetch_add(1, Ordering::SeqCst),
            })
            .clone()
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PlacementError {
    #[error("no route found")]
    NoRoute,
    #[error("static placement supports only u64 route keys in phase 3")]
    UnsupportedRouteKey,
    #[error("virtual shard count must be greater than zero")]
    InvalidShardCount,
    #[error("no ready instances are available for placement")]
    NoReadyInstances,
    #[error("duplicate virtual shard assigner {name}")]
    DuplicateAssigner { name: &'static str },
    #[error("instance {instance_id} was not found")]
    InstanceNotFound { instance_id: InstanceId },
    #[error("instance {instance_id} is not ready: {state:?}")]
    InstanceNotReady {
        instance_id: InstanceId,
        state: InstanceState,
    },
    #[error("placement compare-and-put failed")]
    CompareAndPutFailed,
    #[error("activation lock is already held for actor")]
    ActivationLockHeld,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, VecDeque};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use lattice_core::{
        ActorId, Epoch, InstanceCapacity, InstanceId, RouteKey, actor_kind, service_kind,
    };
    use lattice_rpc::{AuthContext, RpcContext};

    use super::*;

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
        fn actor_kind(&self) -> lattice_core::ActorKind {
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
        resolves: Arc<AtomicU64>,
        invalidations: Arc<Mutex<Vec<(RouteCacheKey, InvalidateReason)>>>,
    }

    #[async_trait]
    impl RouteResolver for SequencedResolver {
        async fn resolve(&self, _request: ResolveRequest) -> Result<RouteTarget, PlacementError> {
            self.resolves.fetch_add(1, Ordering::SeqCst);
            self.targets
                .lock()
                .unwrap()
                .pop_front()
                .ok_or(PlacementError::NoRoute)
        }

        async fn invalidate(&self, key: RouteCacheKey, reason: InvalidateReason) {
            self.invalidations.lock().unwrap().push((key, reason));
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Attempt {
        request_id: String,
        route_epoch: Option<Epoch>,
        instance_id: InstanceId,
        connection_id: u64,
    }

    #[derive(Clone, Default)]
    struct NotOwnerThenOkTransport {
        attempts: Arc<Mutex<Vec<Attempt>>>,
    }

    #[async_trait]
    impl EndpointRpcTransport for NotOwnerThenOkTransport {
        async fn unary<Req>(
            &self,
            endpoint: EndpointLease,
            target: RouteTarget,
            request: Request<Req>,
        ) -> Result<Response<Req::Reply>, RpcError>
        where
            Req: RoutedRequest + RpcRequest,
        {
            let ctx = RpcContext::from_metadata(request.metadata())
                .map_err(|error| RpcError::Business(error.to_string()))?;
            let mut attempts = self.attempts.lock().unwrap();
            attempts.push(Attempt {
                request_id: ctx.request_id.as_str().to_string(),
                route_epoch: ctx.route_epoch,
                instance_id: target.instance_id,
                connection_id: endpoint.connection_id,
            });
            if attempts.len() == 1 {
                return Err(RpcError::NotOwner {
                    expected_epoch: Some(Epoch(2)),
                });
            }

            Ok(Response::new(Req::Reply::default()))
        }
    }

    #[test]
    fn local_route_cache_reports_fresh_stale_and_hard_expired_entries() {
        let mut cache = LocalRouteCache::new(RouteCacheConfig {
            soft_ttl: Duration::from_millis(5),
            hard_ttl: Duration::from_millis(25),
        });
        let key = RouteCacheKey::new(
            service_kind!("World"),
            actor_kind!("World"),
            RouteKey::U64(1),
        );
        let target = route_target("world-0", 1);

        cache.insert(key.clone(), target.clone());
        assert_eq!(cache.get(&key), CacheLookup::Fresh(target.clone()));

        std::thread::sleep(Duration::from_millis(8));
        assert_eq!(cache.get(&key), CacheLookup::Stale(target));

        std::thread::sleep(Duration::from_millis(25));
        assert_eq!(cache.get(&key), CacheLookup::Miss);
    }

    #[tokio::test]
    async fn static_route_resolver_uses_cache_after_first_lookup() {
        let resolver = static_resolver();
        let request = ResolveRequest {
            service_kind: service_kind!("World"),
            actor_kind: actor_kind!("World"),
            route_key: RouteKey::U64(7),
        };

        let first = resolver.resolve(request.clone()).await.unwrap();
        let second = resolver.resolve(request).await.unwrap();

        assert_eq!(first.instance_id, InstanceId::new("world-a"));
        assert_eq!(second.instance_id, InstanceId::new("world-a"));
        assert_eq!(resolver.placement_lookups(), 1);
    }

    #[tokio::test]
    async fn static_route_resolver_maps_ranges_to_different_instances() {
        let resolver = static_resolver();

        let low = resolver
            .resolve(ResolveRequest {
                service_kind: service_kind!("World"),
                actor_kind: actor_kind!("World"),
                route_key: RouteKey::U64(40),
            })
            .await
            .unwrap();
        let high = resolver
            .resolve(ResolveRequest {
                service_kind: service_kind!("World"),
                actor_kind: actor_kind!("World"),
                route_key: RouteKey::U64(60),
            })
            .await
            .unwrap();

        assert_eq!(low.instance_id, InstanceId::new("world-a"));
        assert_eq!(high.instance_id, InstanceId::new("world-b"));
    }

    #[test]
    fn endpoint_pool_reuses_by_instance_and_endpoint() {
        let pool = EndpointPool::new();
        let first = pool.get_or_connect(&route_target("world-a", 1));
        let same = pool.get_or_connect(&route_target("world-a", 2));
        let other = pool.get_or_connect(&route_target("world-b", 1));

        assert_eq!(first.connection_id, same.connection_id);
        assert_ne!(first.connection_id, other.connection_id);
    }

    #[test]
    fn virtual_shard_hash_is_stable_for_route_key() {
        let mapper = VirtualShardMapper::new(128).unwrap();

        let first = mapper.shard_for_route_key(&RouteKey::U64(42));
        let second = mapper.shard_for_route_key(&RouteKey::U64(42));
        let different_type = mapper.shard_for_route_key(&RouteKey::Str("42".to_string()));

        assert_eq!(first, second);
        assert_ne!(first, different_type);
        assert!(first.0 < 128);
    }

    #[tokio::test]
    async fn round_robin_assigner_plans_deterministic_shard_owners() {
        let plan = RoundRobinShardAssigner
            .plan(assign_input(
                4,
                vec![InstanceId::new("a"), InstanceId::new("b")],
                Vec::new(),
                BTreeSet::new(),
                usize::MAX,
            ))
            .await
            .unwrap();

        assert_eq!(
            plan.assignments
                .iter()
                .map(|assignment| assignment.owner.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "a", "b"]
        );
        assert!(
            plan.assignments
                .iter()
                .all(|assignment| assignment.epoch == Epoch(1))
        );
    }

    #[tokio::test]
    async fn assigner_registry_returns_registered_assigner_by_stable_name() {
        let registry = VirtualShardAssignerRegistry::new();

        registry.register(RoundRobinShardAssigner).unwrap();
        let assigner = registry.get("round_robin").unwrap();
        let duplicate = registry.register(RoundRobinShardAssigner);

        assert_eq!(assigner.name(), "round_robin");
        assert_eq!(
            duplicate,
            Err(PlacementError::DuplicateAssigner {
                name: "round_robin"
            })
        );
    }

    #[tokio::test]
    async fn gradual_rebalance_moves_only_eligible_limited_shards_and_increments_epoch() {
        let previous = vec![
            VirtualShardAssignment {
                shard_id: VirtualShardId(0),
                owner: InstanceId::new("a"),
                epoch: Epoch(1),
            },
            VirtualShardAssignment {
                shard_id: VirtualShardId(1),
                owner: InstanceId::new("a"),
                epoch: Epoch(1),
            },
            VirtualShardAssignment {
                shard_id: VirtualShardId(2),
                owner: InstanceId::new("a"),
                epoch: Epoch(1),
            },
            VirtualShardAssignment {
                shard_id: VirtualShardId(3),
                owner: InstanceId::new("a"),
                epoch: Epoch(1),
            },
        ];
        let plan = GradualRebalanceShardAssigner
            .plan(assign_input(
                4,
                vec![InstanceId::new("a"), InstanceId::new("b")],
                previous,
                BTreeSet::from([VirtualShardId(1), VirtualShardId(3)]),
                1,
            ))
            .await
            .unwrap();

        assert_eq!(
            plan.owner_of(VirtualShardId(1)).unwrap().owner,
            InstanceId::new("b")
        );
        assert_eq!(plan.owner_of(VirtualShardId(1)).unwrap().epoch, Epoch(2));
        assert_eq!(
            plan.owner_of(VirtualShardId(3)).unwrap().owner,
            InstanceId::new("a")
        );
        assert_eq!(plan.owner_of(VirtualShardId(3)).unwrap().epoch, Epoch(1));
    }

    #[tokio::test]
    async fn in_memory_instance_registry_lists_ready_instances_for_service() {
        let registry = InMemoryInstanceRegistry::new();

        registry
            .upsert(instance_record("world-a", InstanceState::Ready))
            .await
            .unwrap();
        registry
            .upsert(instance_record("world-b", InstanceState::Draining))
            .await
            .unwrap();

        let ready = registry.list_ready(&service_kind!("World")).await.unwrap();

        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].instance_id, InstanceId::new("world-a"));
    }

    #[tokio::test]
    async fn virtual_shard_route_table_resolves_actor_key_to_shard_owner() {
        let registry = InMemoryInstanceRegistry::new();
        registry
            .upsert(instance_record("world-a", InstanceState::Ready))
            .await
            .unwrap();
        registry
            .upsert(instance_record("world-b", InstanceState::Ready))
            .await
            .unwrap();
        let mapper = VirtualShardMapper::new(2).unwrap();
        let shard = mapper.shard_for_route_key(&RouteKey::U64(7));
        let owner = if shard.0 == 0 { "world-a" } else { "world-b" };
        let table = VirtualShardRouteTable::new(
            service_kind!("World"),
            actor_kind!("World"),
            mapper,
            vec![VirtualShardAssignment {
                shard_id: shard,
                owner: InstanceId::new(owner),
                epoch: Epoch(9),
            }],
            registry,
        );

        let target = table.resolve(&RouteKey::U64(7)).await.unwrap();

        assert_eq!(target.instance_id, InstanceId::new(owner));
        assert_eq!(target.owner_epoch, Some(Epoch(9)));
        assert_eq!(table.actor_kind(), &actor_kind!("World"));
    }

    #[tokio::test]
    async fn in_memory_placement_store_compare_and_puts_actor_records() {
        let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test"));
        let key = actor_key(7);
        let record = actor_record(7, "world-a", 1, LeaseId(10));

        let version = store
            .compare_and_put_actor(key.clone(), None, record.clone())
            .await
            .unwrap();
        let stale = store
            .compare_and_put_actor(key.clone(), None, record.clone())
            .await;
        let updated = ActorPlacementRecord {
            epoch: Epoch(2),
            ..record
        };
        let next = store
            .compare_and_put_actor(key.clone(), Some(version), updated.clone())
            .await
            .unwrap();

        assert_eq!(version, PlacementVersion(1));
        assert_eq!(stale, Err(PlacementError::CompareAndPutFailed));
        assert_eq!(next, PlacementVersion(2));
        assert_eq!(store.get_actor(&key).await.unwrap().unwrap().1, updated);
    }

    #[tokio::test]
    async fn in_memory_placement_store_activation_lock_is_exclusive_until_release() {
        let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test"));
        let key = actor_key(7);

        let first = store.acquire_activation_lock(key.clone()).await.unwrap();
        let second = store.acquire_activation_lock(key.clone()).await;
        store.release_activation_lock(&key).await.unwrap();
        let third = store.acquire_activation_lock(key).await.unwrap();

        assert_eq!(first, LeaseId(1));
        assert_eq!(second, Err(PlacementError::ActivationLockHeld));
        assert_eq!(third, LeaseId(2));
    }

    #[tokio::test]
    async fn in_memory_placement_store_isolates_records_by_cluster_prefix() {
        let first = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/cluster-a"));
        let second = InMemoryPlacementStore::with_shared_inner(
            PlacementPrefix::new("/lattice/cluster-b"),
            &first,
        );
        let key = actor_key(7);

        first
            .upsert_instance(instance_record("world-a", InstanceState::Ready))
            .await
            .unwrap();
        first
            .compare_and_put_actor(
                key.clone(),
                None,
                actor_record(7, "world-a", 1, LeaseId(10)),
            )
            .await
            .unwrap();

        assert_eq!(
            first
                .list_instances(&service_kind!("World"))
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            second
                .list_instances(&service_kind!("World"))
                .await
                .unwrap()
                .len(),
            0
        );
        assert!(second.get_actor(&key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn resolving_rpc_core_invalidates_not_owner_and_retries_same_request_id() {
        let resolver = SequencedResolver {
            targets: Arc::new(Mutex::new(VecDeque::from([
                route_target("world-a", 1),
                route_target("world-b", 2),
            ]))),
            resolves: Arc::new(AtomicU64::new(0)),
            invalidations: Arc::new(Mutex::new(Vec::new())),
        };
        let transport = NotOwnerThenOkTransport::default();
        let attempts = transport.attempts.clone();
        let context_factory =
            RpcClientContextFactory::new(service_kind!("Player"), InstanceId::new("player-0"))
                .with_auth(AuthContext {
                    authorization: "Bearer internal".into(),
                });
        let core = ResolvingRpcCore::new(
            service_kind!("World"),
            resolver.clone(),
            EndpointPool::new(),
            context_factory,
            transport,
        );

        let reply = core.call(EnterWorldRequest { world_id: 7 }).await.unwrap();

        assert!(!reply.ok);
        assert_eq!(resolver.resolves.load(Ordering::SeqCst), 2);
        assert_eq!(
            resolver.invalidations.lock().unwrap()[0].1,
            InvalidateReason::NotOwner
        );
        let attempts = attempts.lock().unwrap();
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0].request_id, attempts[1].request_id);
        assert_eq!(attempts[0].route_epoch, Some(Epoch(1)));
        assert_eq!(attempts[1].route_epoch, Some(Epoch(2)));
        assert_eq!(attempts[0].instance_id, InstanceId::new("world-a"));
        assert_eq!(attempts[1].instance_id, InstanceId::new("world-b"));
    }

    fn static_resolver() -> StaticRouteResolver {
        StaticRouteResolver::new(
            StaticPlacementConfig {
                ranges: vec![
                    StaticRouteRange {
                        service_kind: service_kind!("World"),
                        actor_kind: actor_kind!("World"),
                        start_inclusive: 0,
                        end_exclusive: 50,
                        target: route_target("world-a", 1),
                    },
                    StaticRouteRange {
                        service_kind: service_kind!("World"),
                        actor_kind: actor_kind!("World"),
                        start_inclusive: 50,
                        end_exclusive: 100,
                        target: route_target("world-b", 1),
                    },
                ],
            },
            RouteCacheConfig {
                soft_ttl: Duration::from_secs(30),
                hard_ttl: Duration::from_secs(60),
            },
        )
    }

    fn route_target(instance_id: &str, epoch: u64) -> RouteTarget {
        RouteTarget {
            service_kind: service_kind!("World"),
            instance_id: InstanceId::new(instance_id),
            advertised_endpoint: format!("http://{instance_id}.world:18080").parse().unwrap(),
            owner_epoch: Some(Epoch(epoch)),
        }
    }

    fn assign_input(
        shard_count: u32,
        instances: Vec<InstanceId>,
        previous: Vec<VirtualShardAssignment>,
        eligible_shards: BTreeSet<VirtualShardId>,
        max_migrations: usize,
    ) -> VirtualShardAssignInput {
        VirtualShardAssignInput {
            service_kind: service_kind!("World"),
            actor_kind: actor_kind!("World"),
            shard_count,
            instances,
            previous,
            eligible_shards,
            max_migrations,
        }
    }

    fn instance_record(instance_id: &str, state: InstanceState) -> InstanceRecord {
        InstanceRecord {
            service_kind: service_kind!("World"),
            instance_id: InstanceId::new(instance_id),
            advertised_endpoint: format!("http://{instance_id}.world:18080").parse().unwrap(),
            control_endpoint: format!("http://{instance_id}.world:18081").parse().unwrap(),
            version: "test".to_string(),
            state,
            capacity: InstanceCapacity::default(),
            labels: Default::default(),
        }
    }

    fn actor_key(actor_id: u64) -> ActorPlacementKey {
        ActorPlacementKey {
            actor_kind: actor_kind!("World"),
            actor_id: ActorId::U64(actor_id),
        }
    }

    fn actor_record(
        actor_id: u64,
        owner: &str,
        epoch: u64,
        lease_id: LeaseId,
    ) -> ActorPlacementRecord {
        ActorPlacementRecord {
            actor_kind: actor_kind!("World"),
            actor_id: ActorId::U64(actor_id),
            owner: InstanceId::new(owner),
            epoch: Epoch(epoch),
            lease_id,
            state: PlacementState::Running,
        }
    }
}
