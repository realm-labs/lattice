use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use lattice_core::{ActorKind, Epoch, InstanceId, RouteKey, ServiceKind};
use lattice_rpc::RouteTarget;

use crate::{
    InstanceRecord, InstanceState, LeaseId, LocalRouteCache, PlacementError, PlacementStore,
    ResolveRequest, RouteCacheConfig, RouteCacheKey, RouteResolver,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SingletonKey {
    pub singleton_kind: ActorKind,
    pub scope: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonPlacementRecord {
    pub singleton_kind: ActorKind,
    pub scope: String,
    pub owner: InstanceId,
    pub epoch: Epoch,
    pub lease_id: LeaseId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivateSingletonRequest {
    pub service_kind: ServiceKind,
    pub singleton_kind: ActorKind,
    pub scope: String,
}

#[async_trait]
pub trait SingletonControl: Clone + Send + Sync + 'static {
    async fn activate_singleton(
        &self,
        instance: &InstanceRecord,
        key: &SingletonKey,
        epoch: Epoch,
    ) -> Result<(), PlacementError>;
}

#[derive(Debug, Clone, Default)]
pub struct NoopSingletonControl;

#[async_trait]
impl SingletonControl for NoopSingletonControl {
    async fn activate_singleton(
        &self,
        _instance: &InstanceRecord,
        _key: &SingletonKey,
        _epoch: Epoch,
    ) -> Result<(), PlacementError> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SingletonCoordinator<S, C> {
    service_kind: ServiceKind,
    store: S,
    control: C,
    records: Arc<std::sync::Mutex<HashMap<SingletonKey, SingletonPlacementRecord>>>,
    locks: Arc<std::sync::Mutex<HashMap<SingletonKey, LeaseId>>>,
    next_lease_id: Arc<AtomicU64>,
}

impl<S, C> SingletonCoordinator<S, C> {
    pub fn new(service_kind: ServiceKind, store: S, control: C) -> Self {
        Self {
            service_kind,
            store,
            control,
            records: Arc::new(std::sync::Mutex::new(HashMap::new())),
            locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
            next_lease_id: Arc::new(AtomicU64::new(1)),
        }
    }
}

impl<S, C> SingletonCoordinator<S, C>
where
    S: PlacementStore,
    C: SingletonControl,
{
    pub async fn activate_singleton(
        &self,
        request: ActivateSingletonRequest,
    ) -> Result<SingletonPlacementRecord, PlacementError> {
        let key = SingletonKey {
            singleton_kind: request.singleton_kind,
            scope: request.scope,
        };
        if let Some(record) = self.records.lock().unwrap().get(&key).cloned() {
            return Ok(record);
        }
        let lease_id = match self.acquire_lock(key.clone()) {
            Ok(lease_id) => lease_id,
            Err(PlacementError::SingletonLockHeld) => {
                return self.wait_for_existing_owner(&key).await;
            }
            Err(error) => return Err(error),
        };
        let result = self.activate_with_lock(key.clone(), lease_id).await;
        self.locks.lock().unwrap().remove(&key);
        result
    }

    pub async fn failover(
        &self,
        key: &SingletonKey,
        new_owner: InstanceId,
    ) -> Result<SingletonPlacementRecord, PlacementError> {
        let mut records = self.records.lock().unwrap();
        let current = records.get(key).cloned().ok_or(PlacementError::NoRoute)?;
        let record = SingletonPlacementRecord {
            owner: new_owner,
            epoch: Epoch(current.epoch.0 + 1),
            lease_id: LeaseId(current.lease_id.0 + 1),
            ..current
        };
        records.insert(key.clone(), record.clone());
        Ok(record)
    }

    pub fn get(&self, key: &SingletonKey) -> Option<SingletonPlacementRecord> {
        self.records.lock().unwrap().get(key).cloned()
    }

    async fn activate_with_lock(
        &self,
        key: SingletonKey,
        lease_id: LeaseId,
    ) -> Result<SingletonPlacementRecord, PlacementError> {
        if let Some(record) = self.records.lock().unwrap().get(&key).cloned() {
            return Ok(record);
        }
        let instance = self
            .store
            .list_instances(&self.service_kind)
            .await?
            .into_iter()
            .filter(|instance| instance.state == InstanceState::Ready)
            .min_by_key(|instance| instance.instance_id.clone())
            .ok_or(PlacementError::NoReadyInstances)?;
        let record = SingletonPlacementRecord {
            singleton_kind: key.singleton_kind.clone(),
            scope: key.scope.clone(),
            owner: instance.instance_id.clone(),
            epoch: Epoch(1),
            lease_id,
        };
        self.control
            .activate_singleton(&instance, &key, record.epoch)
            .await?;
        self.records.lock().unwrap().insert(key, record.clone());
        Ok(record)
    }

    fn acquire_lock(&self, key: SingletonKey) -> Result<LeaseId, PlacementError> {
        let mut locks = self.locks.lock().unwrap();
        if locks.contains_key(&key) {
            return Err(PlacementError::SingletonLockHeld);
        }
        let lease_id = LeaseId(self.next_lease_id.fetch_add(1, Ordering::SeqCst));
        locks.insert(key, lease_id);
        Ok(lease_id)
    }

    async fn wait_for_existing_owner(
        &self,
        key: &SingletonKey,
    ) -> Result<SingletonPlacementRecord, PlacementError> {
        for _ in 0..50 {
            if let Some(record) = self.records.lock().unwrap().get(key).cloned() {
                return Ok(record);
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        Err(PlacementError::SingletonLockHeld)
    }
}

#[derive(Debug, Clone)]
pub struct SingletonRouteResolver<S, C> {
    coordinator: SingletonCoordinator<S, C>,
    cache: Arc<LocalRouteCache>,
}

impl<S, C> SingletonRouteResolver<S, C> {
    pub fn new(coordinator: SingletonCoordinator<S, C>, cache_config: RouteCacheConfig) -> Self {
        Self {
            coordinator,
            cache: Arc::new(LocalRouteCache::new(cache_config)),
        }
    }
}

#[async_trait]
impl<S, C> RouteResolver for SingletonRouteResolver<S, C>
where
    S: PlacementStore,
    C: SingletonControl,
{
    async fn resolve(&self, request: ResolveRequest) -> Result<RouteTarget, PlacementError> {
        let cache_key = request.cache_key();
        match self.cache.get(&cache_key) {
            crate::CacheLookup::Fresh(target) | crate::CacheLookup::Stale(target) => {
                return Ok(target);
            }
            crate::CacheLookup::Miss => {}
        }

        let scope = match request.route_key {
            RouteKey::Str(scope) => scope,
            other => format!("{other:?}"),
        };
        let record = self
            .coordinator
            .activate_singleton(ActivateSingletonRequest {
                service_kind: request.service_kind.clone(),
                singleton_kind: request.actor_kind,
                scope,
            })
            .await?;
        let instance = self
            .coordinator
            .store
            .get_instance(&record.owner)
            .await?
            .ok_or_else(|| PlacementError::InstanceNotFound {
                instance_id: record.owner.clone(),
            })?;
        let target = RouteTarget {
            service_kind: request.service_kind,
            instance_id: instance.instance_id,
            advertised_endpoint: instance.advertised_endpoint,
            owner_epoch: Some(record.epoch),
        };
        self.cache.insert(cache_key, target.clone());
        Ok(target)
    }

    async fn invalidate(&self, key: RouteCacheKey, _reason: crate::InvalidateReason) {
        self.cache.invalidate(&key);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use lattice_core::{InstanceCapacity, actor_kind, service_kind};

    use super::*;
    use crate::{InMemoryPlacementStore, InstanceState, PlacementPrefix};

    #[derive(Debug, Clone, Default)]
    struct CountingSingletonControl {
        calls: Arc<AtomicU64>,
        delay: Duration,
    }

    #[async_trait]
    impl SingletonControl for CountingSingletonControl {
        async fn activate_singleton(
            &self,
            _instance: &InstanceRecord,
            _key: &SingletonKey,
            _epoch: Epoch,
        ) -> Result<(), PlacementError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn singleton_activation_race_creates_one_owner() {
        let store = ready_store().await;
        let control = CountingSingletonControl {
            calls: Arc::new(AtomicU64::new(0)),
            delay: Duration::from_millis(10),
        };
        let coordinator = Arc::new(SingletonCoordinator::new(
            service_kind!("Control"),
            store,
            control.clone(),
        ));

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let coordinator = coordinator.clone();
            tasks.push(tokio::spawn(async move {
                coordinator
                    .activate_singleton(ActivateSingletonRequest {
                        service_kind: service_kind!("Control"),
                        singleton_kind: actor_kind!("SeasonManager"),
                        scope: "global".to_string(),
                    })
                    .await
            }));
        }

        for task in tasks {
            let record = task.await.unwrap().unwrap();
            assert_eq!(record.owner, InstanceId::new("control-a"));
        }
        assert_eq!(control.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn singleton_failover_increments_epoch() {
        let store = ready_store().await;
        store
            .upsert_instance(instance_record("control-b", InstanceState::Ready))
            .await
            .unwrap();
        let coordinator =
            SingletonCoordinator::new(service_kind!("Control"), store, NoopSingletonControl);
        let key = SingletonKey {
            singleton_kind: actor_kind!("SeasonManager"),
            scope: "global".to_string(),
        };

        coordinator
            .activate_singleton(ActivateSingletonRequest {
                service_kind: service_kind!("Control"),
                singleton_kind: key.singleton_kind.clone(),
                scope: key.scope.clone(),
            })
            .await
            .unwrap();
        let failed_over = coordinator
            .failover(&key, InstanceId::new("control-b"))
            .await
            .unwrap();

        assert_eq!(failed_over.owner, InstanceId::new("control-b"));
        assert_eq!(failed_over.epoch, Epoch(2));
    }

    #[tokio::test]
    async fn singleton_route_resolver_returns_owner_epoch_for_generated_client() {
        let store = ready_store().await;
        let coordinator =
            SingletonCoordinator::new(service_kind!("Control"), store, NoopSingletonControl);
        let resolver = SingletonRouteResolver::new(coordinator, RouteCacheConfig::default());

        let target = resolver
            .resolve(ResolveRequest {
                service_kind: service_kind!("Control"),
                actor_kind: actor_kind!("SeasonManager"),
                route_key: RouteKey::Str("global".to_string()),
            })
            .await
            .unwrap();

        assert_eq!(target.instance_id, InstanceId::new("control-a"));
        assert_eq!(target.owner_epoch, Some(Epoch(1)));
    }

    async fn ready_store() -> InMemoryPlacementStore {
        let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test"));
        store
            .upsert_instance(instance_record("control-a", InstanceState::Ready))
            .await
            .unwrap();
        store
    }

    fn instance_record(instance_id: &str, state: InstanceState) -> InstanceRecord {
        InstanceRecord {
            service_kind: service_kind!("Control"),
            instance_id: InstanceId::new(instance_id),
            advertised_endpoint: format!("http://{instance_id}.control:18080")
                .parse()
                .unwrap(),
            control_endpoint: format!("http://{instance_id}.control:18081")
                .parse()
                .unwrap(),
            version: "test".to_string(),
            state,
            capacity: InstanceCapacity::default(),
            labels: BTreeMap::new(),
        }
    }
}
