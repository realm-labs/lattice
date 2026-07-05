use std::sync::Arc;

use async_trait::async_trait;
use lattice_config::{ConfigStore, ConfigStoreError, ConfigWatch};
use lattice_core::{InstanceId, ServiceContext, ServiceKind};
use lattice_eventbus::{
    EventBus, EventBusError, EventEnvelope, EventHandler, EventSubscription,
    EventSubscriptionHandle,
};
use lattice_placement::PlacementError;
use lattice_placement::instance::InstanceRecord;
use lattice_placement::store::{
    ActorPlacementKey, ActorPlacementRecord, LeaseId, PlacementPrefix, PlacementStore,
    PlacementVersion, PlacementWatch,
};

#[async_trait]
pub trait DynPlacementStore: Send + Sync + 'static {
    async fn upsert_instance(&self, record: InstanceRecord) -> Result<(), PlacementError>;
    async fn get_instance(
        &self,
        instance_id: &InstanceId,
    ) -> Result<Option<InstanceRecord>, PlacementError>;
    async fn list_instances(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<InstanceRecord>, PlacementError>;
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
    async fn acquire_activation_lock(
        &self,
        key: ActorPlacementKey,
    ) -> Result<LeaseId, PlacementError>;
    async fn release_activation_lock(&self, key: &ActorPlacementKey) -> Result<(), PlacementError>;
    async fn watch(&self, prefix: PlacementPrefix) -> Result<PlacementWatch, PlacementError>;
    fn prefix(&self) -> PlacementPrefix;
}

#[async_trait]
impl<T> DynPlacementStore for T
where
    T: PlacementStore,
{
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

    async fn acquire_activation_lock(
        &self,
        key: ActorPlacementKey,
    ) -> Result<LeaseId, PlacementError> {
        PlacementStore::acquire_activation_lock(self, key).await
    }

    async fn release_activation_lock(&self, key: &ActorPlacementKey) -> Result<(), PlacementError> {
        PlacementStore::release_activation_lock(self, key).await
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
pub struct EventBusComponent {
    inner: Arc<dyn DynEventBus>,
}

impl EventBusComponent {
    pub fn new<T>(event_bus: T) -> Self
    where
        T: EventBus,
    {
        Self {
            inner: Arc::new(event_bus),
        }
    }

    pub fn inner(&self) -> Arc<dyn DynEventBus> {
        self.inner.clone()
    }
}

impl std::fmt::Debug for EventBusComponent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EventBusComponent")
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct LocalEventBusComponent {
    inner: Arc<dyn DynEventBus>,
}

impl LocalEventBusComponent {
    pub fn new<T>(event_bus: T) -> Self
    where
        T: EventBus,
    {
        Self {
            inner: Arc::new(event_bus),
        }
    }

    pub fn inner(&self) -> Arc<dyn DynEventBus> {
        self.inner.clone()
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

pub trait ServiceContextExt {
    fn placement_store(&self) -> Arc<dyn DynPlacementStore>;
    fn event_bus(&self) -> Arc<dyn DynEventBus>;
    fn local_event_bus(&self) -> Arc<dyn DynEventBus>;
    fn config_store(&self) -> Arc<dyn DynConfigStore>;
}

impl ServiceContextExt for ServiceContext {
    fn placement_store(&self) -> Arc<dyn DynPlacementStore> {
        self.extension::<PlacementStoreComponent>()
            .map(|component| component.inner())
            .expect("placement_store should be registered in ServiceContext")
    }

    fn event_bus(&self) -> Arc<dyn DynEventBus> {
        self.extension::<EventBusComponent>()
            .map(|component| component.inner())
            .expect("event_bus should be registered in ServiceContext")
    }

    fn local_event_bus(&self) -> Arc<dyn DynEventBus> {
        self.extension::<LocalEventBusComponent>()
            .map(|component| component.inner())
            .or_else(|| {
                self.extension::<EventBusComponent>()
                    .map(|component| component.inner())
            })
            .expect("local_event_bus should be registered in ServiceContext")
    }

    fn config_store(&self) -> Arc<dyn DynConfigStore> {
        self.extension::<ConfigStoreComponent>()
            .map(|component| component.inner())
            .expect("config_store should be registered in ServiceContext")
    }
}
