use std::sync::Arc;

use async_trait::async_trait;
use lattice_core::actor_ref::Epoch;
use lattice_core::id::RouteKey;
use lattice_core::instance::InstanceId;
use lattice_core::kind::{ActorKind, ServiceKind};
use lattice_rpc::types::RouteTarget;

use crate::authority::PlacementAuthority;
use crate::error::PlacementError;
use crate::registry::{InstanceRecord, InstanceState};
use crate::routing::cache::{CacheLookup, LocalRouteCache, RouteCacheConfig};
use crate::routing::resolver::{InvalidateReason, ResolveRequest, RouteCacheKey, RouteResolver};
use crate::storage::{
    LeaseId, PlacementState, PlacementStore, SingletonKey, SingletonPlacementRecord,
};

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
    store: S,
    control: C,
}

impl<S, C> SingletonCoordinator<S, C> {
    pub fn new(_service_kind: ServiceKind, store: S, control: C) -> Self {
        Self::from_store(store, control)
    }

    pub fn from_store(store: S, control: C) -> Self {
        Self { store, control }
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
            service_kind: request.service_kind,
            singleton_kind: request.singleton_kind,
            scope: request.scope,
        };
        if let Some((_version, record)) = self.store.get_singleton(&key).await? {
            return Ok(record);
        }
        let lock_lease_id = match self.store.acquire_singleton_lock(key.clone()).await {
            Ok(lease_id) => lease_id,
            Err(PlacementError::SingletonLockHeld) => {
                return self.wait_for_existing_owner(&key).await;
            }
            Err(error) => return Err(error),
        };
        let result = self.activate_with_lock(key.clone(), lock_lease_id).await;
        self.store
            .release_singleton_lock(&key, lock_lease_id)
            .await?;
        result
    }

    pub async fn failover(
        &self,
        key: &SingletonKey,
        new_owner: InstanceId,
    ) -> Result<SingletonPlacementRecord, PlacementError> {
        let lock_lease_id = self.store.acquire_singleton_lock(key.clone()).await?;
        let result = self.failover_with_lock(key, new_owner, lock_lease_id).await;
        self.store
            .release_singleton_lock(key, lock_lease_id)
            .await?;
        result
    }

    pub async fn get(
        &self,
        key: &SingletonKey,
    ) -> Result<Option<SingletonPlacementRecord>, PlacementError> {
        Ok(self
            .store
            .get_singleton(key)
            .await?
            .map(|(_version, record)| record))
    }

    async fn failover_with_lock(
        &self,
        key: &SingletonKey,
        new_owner: InstanceId,
        lock_lease_id: LeaseId,
    ) -> Result<SingletonPlacementRecord, PlacementError> {
        let (version, current) = self
            .store
            .get_singleton(key)
            .await?
            .ok_or(PlacementError::NoRoute)?;
        let instance = self.store.get_instance(&new_owner).await?.ok_or_else(|| {
            PlacementError::InstanceNotFound {
                instance_id: new_owner.clone(),
            }
        })?;
        if instance.state != InstanceState::Ready {
            return Err(PlacementError::InstanceNotReady {
                instance_id: instance.instance_id,
                state: instance.state,
            });
        }
        let lease_id = self.store.grant_instance_lease().await?;
        let reservation = self
            .store
            .reserve_singleton_epoch(key.clone(), Some(version), Some(lock_lease_id))
            .await?;
        let record = SingletonPlacementRecord {
            owner: instance.instance_id.clone(),
            epoch: reservation.epoch(),
            lease_id,
            state: PlacementState::Running,
            ..current
        };
        self.control
            .activate_singleton(&instance, key, record.epoch)
            .await?;
        self.store
            .commit_singleton_epoch(reservation, record.clone())
            .await?;
        Ok(record)
    }

    async fn activate_with_lock(
        &self,
        key: SingletonKey,
        lock_lease_id: LeaseId,
    ) -> Result<SingletonPlacementRecord, PlacementError> {
        if let Some((_version, record)) = self.store.get_singleton(&key).await? {
            return Ok(record);
        }
        let instance = self
            .store
            .list_instances(&key.service_kind)
            .await?
            .into_iter()
            .filter(|instance| instance.state == InstanceState::Ready)
            .min_by_key(|instance| instance.instance_id.clone())
            .ok_or(PlacementError::NoReadyInstances)?;
        let lease_id = self.store.grant_instance_lease().await?;
        let reservation = self
            .store
            .reserve_singleton_epoch(key.clone(), None, Some(lock_lease_id))
            .await?;
        let record = SingletonPlacementRecord {
            service_kind: key.service_kind.clone(),
            singleton_kind: key.singleton_kind.clone(),
            scope: key.scope.clone(),
            owner: instance.instance_id.clone(),
            epoch: reservation.epoch(),
            lease_id,
            state: PlacementState::Running,
        };
        self.control
            .activate_singleton(&instance, &key, record.epoch)
            .await?;
        self.store
            .commit_singleton_epoch(reservation, record.clone())
            .await?;
        Ok(record)
    }

    async fn wait_for_existing_owner(
        &self,
        key: &SingletonKey,
    ) -> Result<SingletonPlacementRecord, PlacementError> {
        for _ in 0..50 {
            if let Some((_version, record)) = self.store.get_singleton(key).await? {
                return Ok(record);
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        Err(PlacementError::SingletonLockHeld)
    }
}

#[derive(Clone)]
pub struct SingletonRouteResolver<S> {
    store: S,
    authority: Arc<dyn PlacementAuthority>,
    cache: Arc<LocalRouteCache>,
}

impl<S> std::fmt::Debug for SingletonRouteResolver<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SingletonRouteResolver")
            .finish_non_exhaustive()
    }
}

impl<S> SingletonRouteResolver<S> {
    pub fn new(
        store: S,
        authority: Arc<dyn PlacementAuthority>,
        cache_config: RouteCacheConfig,
    ) -> Self {
        Self {
            store,
            authority,
            cache: Arc::new(LocalRouteCache::new(cache_config)),
        }
    }
}

#[async_trait]
impl<S> RouteResolver for SingletonRouteResolver<S>
where
    S: PlacementStore,
{
    async fn resolve(&self, request: ResolveRequest) -> Result<RouteTarget, PlacementError> {
        let cache_key = request.cache_key();
        match self.cache.get(&cache_key) {
            CacheLookup::Fresh(target) | CacheLookup::Stale(target) => {
                return Ok(target);
            }
            CacheLookup::Miss => {}
        }

        let scope = match request.route_key {
            RouteKey::Str(scope) => scope,
            other => format!("{other:?}"),
        };
        let singleton_key = SingletonKey {
            service_kind: request.service_kind.clone(),
            singleton_kind: request.actor_kind.clone(),
            scope: scope.clone(),
        };
        let record = match self.store.get_singleton(&singleton_key).await? {
            Some((_version, record)) => record,
            None => {
                self.authority
                    .activate_singleton(ActivateSingletonRequest {
                        service_kind: singleton_key.service_kind.clone(),
                        singleton_kind: singleton_key.singleton_kind.clone(),
                        scope: singleton_key.scope.clone(),
                    })
                    .await?
            }
        };
        if record.service_kind != singleton_key.service_kind
            || record.singleton_kind != singleton_key.singleton_kind
            || record.scope != singleton_key.scope
            || record.state != PlacementState::Running
        {
            return Err(PlacementError::NoRoute);
        }
        let instance = self
            .store
            .get_instance(&record.owner)
            .await?
            .ok_or_else(|| PlacementError::InstanceNotFound {
                instance_id: record.owner.clone(),
            })?;
        // Singleton records intentionally use a dedicated owner lease rather
        // than the instance lease. Record presence proves that lease has not
        // expired; incarnation-bound renewal is enforced by the later
        // singleton lifecycle cutover.
        if instance.service_kind != request.service_kind || instance.state != InstanceState::Ready {
            return Err(PlacementError::NoRoute);
        }
        let target = RouteTarget {
            service_kind: request.service_kind,
            instance_id: instance.instance_id,
            advertised_endpoint: instance.advertised_endpoint,
            owner_epoch: Some(record.epoch),
        };
        self.cache.insert(cache_key, target.clone());
        Ok(target)
    }

    async fn invalidate(&self, key: RouteCacheKey, _reason: InvalidateReason) {
        self.cache.invalidate(&key);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use lattice_core::instance::InstanceCapacity;
    use lattice_core::{actor_kind, service_kind};

    use super::*;
    use crate::authority::DevelopmentInProcessPlacementAuthority;
    use crate::coordination::logic::NoopLogicControl;
    use crate::registry::InstanceState;
    use crate::storage::memory::InMemoryPlacementStore;
    use crate::storage::{LeaseId, PlacementPrefix};

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
    async fn singleton_recreation_after_record_deletion_uses_the_durable_epoch_floor() {
        let store = ready_store().await;
        let request = ActivateSingletonRequest {
            service_kind: service_kind!("Control"),
            singleton_kind: actor_kind!("SeasonManager"),
            scope: "global".to_string(),
        };
        let first = SingletonCoordinator::new(
            service_kind!("Control"),
            store.clone(),
            NoopSingletonControl,
        )
        .activate_singleton(request.clone())
        .await
        .unwrap();
        let key = SingletonKey {
            service_kind: request.service_kind.clone(),
            singleton_kind: request.singleton_kind.clone(),
            scope: request.scope.clone(),
        };

        assert!(store.remove_singleton_for_test(&key).is_some());
        let recreated =
            SingletonCoordinator::new(service_kind!("Control"), store, NoopSingletonControl)
                .activate_singleton(request)
                .await
                .unwrap();

        assert_eq!(first.epoch, Epoch(1));
        assert_eq!(recreated.epoch, Epoch(2));
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
            service_kind: service_kind!("Control"),
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
    async fn singleton_owner_record_is_persisted_in_store() {
        let store = ready_store().await;
        let coordinator = SingletonCoordinator::new(
            service_kind!("Control"),
            store.clone(),
            NoopSingletonControl,
        );
        let key = SingletonKey {
            service_kind: service_kind!("Control"),
            singleton_kind: actor_kind!("SeasonManager"),
            scope: "global".to_string(),
        };

        let record = coordinator
            .activate_singleton(ActivateSingletonRequest {
                service_kind: service_kind!("Control"),
                singleton_kind: key.singleton_kind.clone(),
                scope: key.scope.clone(),
            })
            .await
            .unwrap();
        let (stored_version, stored) = store.get_singleton(&key).await.unwrap().unwrap();

        assert_eq!(stored, record);
        assert_eq!(stored.state, PlacementState::Running);
        assert_eq!(stored.service_kind, service_kind!("Control"));
        assert_eq!(
            store.list_singletons().await.unwrap(),
            vec![(stored_version, stored)]
        );
    }

    #[tokio::test]
    async fn singleton_route_resolver_returns_owner_epoch_for_generated_client() {
        let store = ready_store().await;
        let authority =
            DevelopmentInProcessPlacementAuthority::new(store.clone(), NoopLogicControl).shared();
        let resolver = SingletonRouteResolver::new(store, authority, RouteCacheConfig::default());

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

    #[tokio::test]
    async fn singleton_route_resolver_rejects_nonrunning_and_nonready_but_accepts_dedicated_lease()
    {
        let store = ready_store().await;
        let instance = store
            .get_instance(&InstanceId::new("control-a"))
            .await
            .unwrap()
            .unwrap();
        let key = SingletonKey {
            service_kind: service_kind!("Control"),
            singleton_kind: actor_kind!("SeasonManager"),
            scope: "global".to_string(),
        };
        let version = store
            .compare_and_put_singleton(
                key.clone(),
                None,
                SingletonPlacementRecord {
                    service_kind: key.service_kind.clone(),
                    singleton_kind: key.singleton_kind.clone(),
                    scope: key.scope.clone(),
                    owner: instance.instance_id.clone(),
                    epoch: Epoch(1),
                    lease_id: instance.lease_id,
                    state: PlacementState::Draining,
                },
            )
            .await
            .unwrap();
        let authority =
            DevelopmentInProcessPlacementAuthority::new(store.clone(), NoopLogicControl).shared();
        let resolver = SingletonRouteResolver::new(
            store.clone(),
            authority.clone(),
            RouteCacheConfig::default(),
        );
        let request = ResolveRequest {
            service_kind: service_kind!("Control"),
            actor_kind: actor_kind!("SeasonManager"),
            route_key: RouteKey::Str("global".to_string()),
        };
        assert_eq!(
            resolver.resolve(request.clone()).await.unwrap_err(),
            PlacementError::NoRoute
        );

        store
            .compare_and_put_singleton(
                key.clone(),
                Some(version),
                SingletonPlacementRecord {
                    service_kind: key.service_kind.clone(),
                    singleton_kind: key.singleton_kind.clone(),
                    scope: key.scope.clone(),
                    owner: instance.instance_id.clone(),
                    epoch: Epoch(2),
                    lease_id: LeaseId(instance.lease_id.0 + 100),
                    state: PlacementState::Running,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            resolver.resolve(request.clone()).await.unwrap().instance_id,
            instance.instance_id
        );

        let mut not_ready = instance.clone();
        not_ready.state = InstanceState::Draining;
        store.upsert_instance(not_ready).await.unwrap();
        let resolver = SingletonRouteResolver::new(store, authority, RouteCacheConfig::default());
        assert_eq!(
            resolver.resolve(request).await.unwrap_err(),
            PlacementError::NoRoute
        );
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
            lease_id: LeaseId(1),
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
