use std::sync::Arc;

use async_trait::async_trait;
use lattice_config::store::{ConfigStore, ConfigStoreError, ConfigWatch};
use lattice_core::instance::InstanceId;
use lattice_core::kind::ServiceKind;
use lattice_core::service_context::ServiceContext;
use lattice_eventbus::error::EventBusError;
use lattice_eventbus::local::{EventBus, EventHandler, EventSubscriptionHandle};
use lattice_eventbus::publisher::ServiceEvents;
use lattice_eventbus::types::{EventEnvelope, EventSubscription};
use lattice_ops::scheduler::ServiceScheduler;
use lattice_placement::control::TonicLogicControl;
use lattice_placement::coordination::actor::PlacementCoordinator;
use lattice_placement::coordination::reports::DrainReport;
use lattice_placement::error::PlacementError;
use lattice_placement::registry::InstanceRecord;
use lattice_placement::storage::{
    ActorPlacementKey, ActorPlacementRecord, LeaseId, PlacementPrefix, PlacementStore,
    PlacementVersion, PlacementWatch, SingletonKey, SingletonPlacementRecord,
    VirtualShardPlacementKey, VirtualShardPlacementRecord,
};
use tokio::sync::Mutex;

#[async_trait]
pub trait DynPlacementStore: Send + Sync + 'static {
    async fn grant_instance_lease(&self) -> Result<LeaseId, PlacementError>;
    async fn keepalive_instance_lease(&self, lease_id: LeaseId) -> Result<(), PlacementError>;
    async fn upsert_instance(&self, record: InstanceRecord) -> Result<(), PlacementError>;
    async fn get_instance(
        &self,
        instance_id: &InstanceId,
    ) -> Result<Option<InstanceRecord>, PlacementError>;
    async fn list_instances(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<InstanceRecord>, PlacementError>;
    async fn list_all_instances(&self) -> Result<Vec<InstanceRecord>, PlacementError>;
    async fn drain_instance(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
    ) -> Result<DrainReport, PlacementError>;
    async fn get_actor(
        &self,
        key: &ActorPlacementKey,
    ) -> Result<Option<(PlacementVersion, ActorPlacementRecord)>, PlacementError>;
    async fn list_actors(
        &self,
    ) -> Result<Vec<(PlacementVersion, ActorPlacementRecord)>, PlacementError>;
    async fn compare_and_put_actor(
        &self,
        key: ActorPlacementKey,
        expected: Option<PlacementVersion>,
        value: ActorPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError>;
    async fn get_virtual_shard(
        &self,
        key: &VirtualShardPlacementKey,
    ) -> Result<Option<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError>;
    async fn list_virtual_shards(
        &self,
        service_kind: &ServiceKind,
        actor_kind: &lattice_core::kind::ActorKind,
    ) -> Result<Vec<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError>;
    async fn list_virtual_shards_for_service(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError>;
    async fn compare_and_put_virtual_shard(
        &self,
        key: VirtualShardPlacementKey,
        expected: Option<PlacementVersion>,
        value: VirtualShardPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError>;
    async fn get_singleton(
        &self,
        key: &SingletonKey,
    ) -> Result<Option<(PlacementVersion, SingletonPlacementRecord)>, PlacementError>;
    async fn acquire_activation_lock(
        &self,
        key: ActorPlacementKey,
    ) -> Result<LeaseId, PlacementError>;
    async fn validate_activation_lock(
        &self,
        key: &ActorPlacementKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError>;
    async fn release_activation_lock(
        &self,
        key: &ActorPlacementKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError>;
    async fn watch(&self, prefix: PlacementPrefix) -> Result<PlacementWatch, PlacementError>;
    fn prefix(&self) -> PlacementPrefix;
}

#[async_trait]
impl<T> DynPlacementStore for T
where
    T: PlacementStore,
{
    async fn grant_instance_lease(&self) -> Result<LeaseId, PlacementError> {
        PlacementStore::grant_instance_lease(self).await
    }

    async fn keepalive_instance_lease(&self, lease_id: LeaseId) -> Result<(), PlacementError> {
        PlacementStore::keepalive_instance_lease(self, lease_id).await
    }

    async fn upsert_instance(&self, record: InstanceRecord) -> Result<(), PlacementError> {
        PlacementStore::upsert_instance(self, record).await
    }

    async fn get_instance(
        &self,
        instance_id: &InstanceId,
    ) -> Result<Option<InstanceRecord>, PlacementError> {
        PlacementStore::get_instance(self, instance_id).await
    }

    async fn list_instances(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<InstanceRecord>, PlacementError> {
        PlacementStore::list_instances(self, service_kind).await
    }

    async fn list_all_instances(&self) -> Result<Vec<InstanceRecord>, PlacementError> {
        PlacementStore::list_all_instances(self).await
    }

    async fn drain_instance(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
    ) -> Result<DrainReport, PlacementError> {
        PlacementCoordinator::new(self.clone(), TonicLogicControl)
            .drain_instance(service_kind, instance_id)
            .await
    }

    async fn get_actor(
        &self,
        key: &ActorPlacementKey,
    ) -> Result<Option<(PlacementVersion, ActorPlacementRecord)>, PlacementError> {
        PlacementStore::get_actor(self, key).await
    }

    async fn list_actors(
        &self,
    ) -> Result<Vec<(PlacementVersion, ActorPlacementRecord)>, PlacementError> {
        PlacementStore::list_actors(self).await
    }

    async fn compare_and_put_actor(
        &self,
        key: ActorPlacementKey,
        expected: Option<PlacementVersion>,
        value: ActorPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        PlacementStore::compare_and_put_actor(self, key, expected, value).await
    }

    async fn get_virtual_shard(
        &self,
        key: &VirtualShardPlacementKey,
    ) -> Result<Option<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError> {
        PlacementStore::get_virtual_shard(self, key).await
    }

    async fn list_virtual_shards(
        &self,
        service_kind: &ServiceKind,
        actor_kind: &lattice_core::kind::ActorKind,
    ) -> Result<Vec<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError> {
        PlacementStore::list_virtual_shards(self, service_kind, actor_kind).await
    }

    async fn list_virtual_shards_for_service(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError> {
        PlacementStore::list_virtual_shards_for_service(self, service_kind).await
    }

    async fn compare_and_put_virtual_shard(
        &self,
        key: VirtualShardPlacementKey,
        expected: Option<PlacementVersion>,
        value: VirtualShardPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        PlacementStore::compare_and_put_virtual_shard(self, key, expected, value).await
    }

    async fn get_singleton(
        &self,
        key: &SingletonKey,
    ) -> Result<Option<(PlacementVersion, SingletonPlacementRecord)>, PlacementError> {
        PlacementStore::get_singleton(self, key).await
    }

    async fn acquire_activation_lock(
        &self,
        key: ActorPlacementKey,
    ) -> Result<LeaseId, PlacementError> {
        PlacementStore::acquire_activation_lock(self, key).await
    }

    async fn validate_activation_lock(
        &self,
        key: &ActorPlacementKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError> {
        PlacementStore::validate_activation_lock(self, key, lease_id).await
    }

    async fn release_activation_lock(
        &self,
        key: &ActorPlacementKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError> {
        PlacementStore::release_activation_lock(self, key, lease_id).await
    }

    async fn watch(&self, prefix: PlacementPrefix) -> Result<PlacementWatch, PlacementError> {
        PlacementStore::watch(self, prefix).await
    }

    fn prefix(&self) -> PlacementPrefix {
        PlacementStore::prefix(self).clone()
    }
}

#[async_trait]
pub trait DynEventBus: Send + Sync + 'static {
    async fn publish(&self, event: EventEnvelope) -> Result<(), EventBusError>;
    async fn subscribe_boxed(
        &self,
        subscription: EventSubscription,
        handler: Arc<dyn EventHandler>,
    ) -> Result<EventSubscriptionHandle, EventBusError>;
}

#[async_trait]
impl<T> DynEventBus for T
where
    T: EventBus,
{
    async fn publish(&self, event: EventEnvelope) -> Result<(), EventBusError> {
        EventBus::publish(self, event).await
    }

    async fn subscribe_boxed(
        &self,
        subscription: EventSubscription,
        handler: Arc<dyn EventHandler>,
    ) -> Result<EventSubscriptionHandle, EventBusError> {
        EventBus::subscribe(self, subscription, move |event| {
            let handler = handler.clone();
            async move { handler.handle(event).await }
        })
        .await
    }
}

#[async_trait]
pub trait DynConfigStore: Send + Sync + 'static {
    async fn get(&self, key: &str) -> Result<Option<serde_json::Value>, ConfigStoreError>;
    async fn put(&self, key: String, value: serde_json::Value) -> Result<(), ConfigStoreError>;
    async fn watch(&self, key: &str) -> Result<ConfigWatch, ConfigStoreError>;
}

#[async_trait]
impl<T> DynConfigStore for T
where
    T: ConfigStore,
{
    async fn get(&self, key: &str) -> Result<Option<serde_json::Value>, ConfigStoreError> {
        ConfigStore::get(self, key).await
    }

    async fn put(&self, key: String, value: serde_json::Value) -> Result<(), ConfigStoreError> {
        ConfigStore::put(self, key, value).await
    }

    async fn watch(&self, key: &str) -> Result<ConfigWatch, ConfigStoreError> {
        ConfigStore::watch(self, key).await
    }
}

#[derive(Clone)]
pub struct PlacementStoreComponent {
    inner: Arc<dyn DynPlacementStore>,
}

impl PlacementStoreComponent {
    pub fn new<T>(store: T) -> Self
    where
        T: PlacementStore,
    {
        Self {
            inner: Arc::new(store),
        }
    }

    pub fn inner(&self) -> Arc<dyn DynPlacementStore> {
        self.inner.clone()
    }
}

impl std::fmt::Debug for PlacementStoreComponent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PlacementStoreComponent")
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct ServiceEventBus {
    inner: Arc<dyn DynEventBus>,
    subscriptions: EventSubscriptionRegistry,
}

impl ServiceEventBus {
    fn new(inner: Arc<dyn DynEventBus>, subscriptions: EventSubscriptionRegistry) -> Self {
        Self {
            inner,
            subscriptions,
        }
    }
}

impl std::fmt::Debug for ServiceEventBus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServiceEventBus")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl EventBus for ServiceEventBus {
    async fn publish(&self, event: EventEnvelope) -> Result<(), EventBusError> {
        self.inner.publish(event).await
    }

    async fn subscribe<H>(
        &self,
        subscription: EventSubscription,
        handler: H,
    ) -> Result<EventSubscriptionHandle, EventBusError>
    where
        H: EventHandler,
    {
        let handle = self
            .inner
            .subscribe_boxed(subscription, Arc::new(handler))
            .await?;
        self.subscriptions.own(handle.clone()).await;
        Ok(handle)
    }
}

#[derive(Debug, Clone, Default)]
struct EventSubscriptionRegistry {
    handles: Arc<Mutex<Vec<EventSubscriptionHandle>>>,
}

impl EventSubscriptionRegistry {
    async fn own(&self, handle: EventSubscriptionHandle) {
        self.handles.lock().await.push(handle);
    }

    pub(crate) async fn cancel_all(&self) -> usize {
        let mut handles = self.handles.lock().await;
        let count = handles.len();
        for handle in handles.drain(..) {
            handle.cancel();
        }
        count
    }
}

#[derive(Clone)]
pub struct ClusterEventBusComponent {
    inner: Arc<dyn DynEventBus>,
    subscriptions: EventSubscriptionRegistry,
}

impl ClusterEventBusComponent {
    pub fn new<T>(event_bus: T) -> Self
    where
        T: EventBus,
    {
        Self {
            inner: Arc::new(event_bus),
            subscriptions: EventSubscriptionRegistry::default(),
        }
    }

    pub fn bus(&self) -> ServiceEventBus {
        ServiceEventBus::new(self.inner.clone(), self.subscriptions.clone())
    }

    pub(crate) async fn cancel_owned_subscriptions(&self) -> usize {
        self.subscriptions.cancel_all().await
    }
}

impl std::fmt::Debug for ClusterEventBusComponent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClusterEventBusComponent")
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct LocalEventBusComponent {
    inner: Arc<dyn DynEventBus>,
    subscriptions: EventSubscriptionRegistry,
}

impl LocalEventBusComponent {
    pub fn new<T>(event_bus: T) -> Self
    where
        T: EventBus,
    {
        Self {
            inner: Arc::new(event_bus),
            subscriptions: EventSubscriptionRegistry::default(),
        }
    }

    pub fn bus(&self) -> ServiceEventBus {
        ServiceEventBus::new(self.inner.clone(), self.subscriptions.clone())
    }

    pub(crate) async fn cancel_owned_subscriptions(&self) -> usize {
        self.subscriptions.cancel_all().await
    }
}

impl std::fmt::Debug for LocalEventBusComponent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalEventBusComponent")
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct ConfigStoreComponent {
    inner: Arc<dyn DynConfigStore>,
}

impl ConfigStoreComponent {
    pub fn new<T>(store: T) -> Self
    where
        T: ConfigStore,
    {
        Self {
            inner: Arc::new(store),
        }
    }

    pub fn inner(&self) -> Arc<dyn DynConfigStore> {
        self.inner.clone()
    }
}

impl std::fmt::Debug for ConfigStoreComponent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConfigStoreComponent")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub struct ServiceSchedulerComponent {
    scheduler: ServiceScheduler,
}

impl ServiceSchedulerComponent {
    pub fn new(scheduler: ServiceScheduler) -> Self {
        Self { scheduler }
    }

    pub fn scheduler(&self) -> ServiceScheduler {
        self.scheduler.clone()
    }
}

pub trait ServiceContextExt {
    fn placement_store(&self) -> Arc<dyn DynPlacementStore>;
    fn cluster_event_bus(&self) -> ServiceEventBus;
    fn local_event_bus(&self) -> ServiceEventBus;
    fn cluster_events(&self) -> ServiceEvents<ServiceEventBus>;
    fn local_events(&self) -> ServiceEvents<ServiceEventBus>;
    fn scheduler(&self) -> ServiceScheduler;
    fn config_store(&self) -> Arc<dyn DynConfigStore>;
}

impl ServiceContextExt for ServiceContext {
    fn placement_store(&self) -> Arc<dyn DynPlacementStore> {
        self.extension::<PlacementStoreComponent>()
            .map(|component| component.inner())
            .expect("placement_store should be registered in ServiceContext")
    }

    fn cluster_event_bus(&self) -> ServiceEventBus {
        self.extension::<ClusterEventBusComponent>()
            .map(|component| component.bus())
            .expect("cluster_event_bus should be registered in ServiceContext")
    }

    fn local_event_bus(&self) -> ServiceEventBus {
        self.extension::<LocalEventBusComponent>()
            .map(|component| component.bus())
            .or_else(|| {
                self.extension::<ClusterEventBusComponent>()
                    .map(|component| component.bus())
            })
            .expect("local_event_bus should be registered in ServiceContext")
    }

    fn cluster_events(&self) -> ServiceEvents<ServiceEventBus> {
        ServiceEvents::new(self.cluster_event_bus())
    }

    fn local_events(&self) -> ServiceEvents<ServiceEventBus> {
        ServiceEvents::new(self.local_event_bus())
    }

    fn scheduler(&self) -> ServiceScheduler {
        self.extension::<ServiceSchedulerComponent>()
            .map(|component| component.scheduler())
            .expect("service scheduler should be registered in ServiceContext")
    }

    fn config_store(&self) -> Arc<dyn DynConfigStore> {
        self.extension::<ConfigStoreComponent>()
            .map(|component| component.inner())
            .expect("config_store should be registered in ServiceContext")
    }
}
