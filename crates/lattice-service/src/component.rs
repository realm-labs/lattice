use async_trait::async_trait;
use lattice_config::BootstrapConfig;
use lattice_core::service_context::ConfiguredComponentBuilder;
use lattice_core::{ConfiguredComponent, InstanceId, ServiceContextBuilder, ServiceKind};
use lattice_placement::PlacementError;
use lattice_placement::cache::RouteCacheConfig;
use lattice_placement::control::TonicLogicControl;
use lattice_placement::coordinator::{
    DrainReport, PlacementCoordinator, PlacementRouteResolver, PlacementWatchStarter,
    PlacementWatchTask,
};
use lattice_placement::instance::InstanceRecord;
use lattice_placement::singleton::SingletonRouteResolver;
use lattice_placement::store::{LeaseId, PlacementStore};

use crate::LatticeServiceError;
use crate::framework::{
    ClusterEventBusComponent, ConfigStoreComponent, LocalEventBusComponent, PlacementStoreComponent,
};

#[derive(Debug)]
pub struct ServiceComponentContext {
    pub service_kind: ServiceKind,
    pub instance_id: InstanceId,
    pub bootstrap_config: BootstrapConfig,
}

#[async_trait]
pub trait ServiceComponent<T>: Send + Sync + 'static
where
    T: Send + Sync + 'static,
{
    async fn build(
        self: Box<Self>,
        ctx: &ServiceComponentContext,
    ) -> Result<T, LatticeServiceError>;
}

#[derive(Debug)]
pub struct ReadyComponent<T> {
    value: T,
}

impl<T> ReadyComponent<T> {
    pub fn new(value: T) -> Self {
        Self { value }
    }
}

#[async_trait]
impl<T> ServiceComponent<T> for ReadyComponent<T>
where
    T: Send + Sync + 'static,
{
    async fn build(
        self: Box<Self>,
        _ctx: &ServiceComponentContext,
    ) -> Result<T, LatticeServiceError> {
        Ok(self.value)
    }
}

#[derive(Debug)]
pub struct ConfiguredServiceComponent<T> {
    builder: ConfiguredComponentBuilder<T>,
}

impl<T> ConfiguredServiceComponent<T>
where
    T: Send + Sync + 'static,
{
    fn new(component: ConfiguredComponent<T>) -> Self {
        Self {
            builder: component.into_builder(),
        }
    }
}

#[async_trait]
impl<T> ServiceComponent<T> for ConfiguredServiceComponent<T>
where
    T: Send + Sync + 'static,
{
    async fn build(
        self: Box<Self>,
        ctx: &ServiceComponentContext,
    ) -> Result<T, LatticeServiceError> {
        self.builder
            .build(&ctx.bootstrap_config)
            .await
            .map_err(|error| LatticeServiceError::ComponentBuild {
                slot: self.builder.section().to_string(),
                message: error.to_string(),
            })
    }
}

pub trait IntoServiceComponent<T>
where
    T: Send + Sync + 'static,
{
    type Component: ServiceComponent<T>;

    fn into_service_component(self) -> Self::Component;
}

impl<T> IntoServiceComponent<T> for T
where
    T: Send + Sync + 'static,
{
    type Component = ReadyComponent<T>;

    fn into_service_component(self) -> Self::Component {
        ReadyComponent::new(self)
    }
}

impl<T> IntoServiceComponent<T> for ConfiguredComponent<T>
where
    T: Send + Sync + 'static,
{
    type Component = ConfiguredServiceComponent<T>;

    fn into_service_component(self) -> Self::Component {
        ConfiguredServiceComponent::new(self)
    }
}

#[async_trait]
pub(crate) trait ErasedServiceComponent: Send + Sync {
    fn target_name(&self) -> &'static str;
    fn type_name(&self) -> &'static str;

    async fn build(
        self: Box<Self>,
        ctx: &ServiceComponentContext,
        service: &mut ServiceContextBuilder,
    ) -> Result<(), LatticeServiceError>;
}

#[async_trait]
pub(crate) trait ErasedPlacementStore: std::fmt::Debug + Send + Sync {
    async fn grant_instance_lease(&self) -> Result<LeaseId, PlacementError>;
    async fn keepalive_instance_lease(&self, lease_id: LeaseId) -> Result<(), PlacementError>;
    async fn upsert_instance(&self, record: InstanceRecord) -> Result<(), PlacementError>;
    async fn keepalive_singleton_owner_leases(
        &self,
        service_kind: &ServiceKind,
        instance_id: &InstanceId,
    ) -> Result<usize, PlacementError>;
    async fn drain_instance(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
    ) -> Result<DrainReport, PlacementError>;
    async fn placement_route_resolver(
        &self,
        service_kind: ServiceKind,
    ) -> Result<(lattice_placement::BoxRouteResolver, PlacementWatchTask), PlacementError>;
    async fn singleton_route_resolver(
        &self,
    ) -> Result<(lattice_placement::BoxRouteResolver, PlacementWatchTask), PlacementError>;
}

#[async_trait]
pub(crate) trait ErasedPlacementStoreComponent: Send + Sync {
    fn target_name(&self) -> &'static str;
    fn type_name(&self) -> &'static str;

    async fn build(
        self: Box<Self>,
        ctx: &ServiceComponentContext,
        service: &mut ServiceContextBuilder,
    ) -> Result<Box<dyn ErasedPlacementStore>, LatticeServiceError>;
}

pub(crate) struct PlacementStoreHandle<T>
where
    T: PlacementStore,
{
    store: T,
}

impl<T> std::fmt::Debug for PlacementStoreHandle<T>
where
    T: PlacementStore,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PlacementStoreHandle")
            .field("store_type", &std::any::type_name::<T>())
            .finish()
    }
}

impl<T> PlacementStoreHandle<T>
where
    T: PlacementStore,
{
    fn new(store: T) -> Self {
        Self { store }
    }
}

#[async_trait]
impl<T> ErasedPlacementStore for PlacementStoreHandle<T>
where
    T: PlacementStore,
{
    async fn grant_instance_lease(&self) -> Result<LeaseId, PlacementError> {
        self.store.grant_instance_lease().await
    }

    async fn keepalive_instance_lease(&self, lease_id: LeaseId) -> Result<(), PlacementError> {
        self.store.keepalive_instance_lease(lease_id).await
    }

    async fn upsert_instance(&self, record: InstanceRecord) -> Result<(), PlacementError> {
        self.store.upsert_instance(record).await
    }

    async fn keepalive_singleton_owner_leases(
        &self,
        service_kind: &ServiceKind,
        instance_id: &InstanceId,
    ) -> Result<usize, PlacementError> {
        let mut kept_alive = 0;
        for (_version, record) in self.store.list_singletons().await? {
            if &record.service_kind == service_kind && &record.owner == instance_id {
                self.store.keepalive_instance_lease(record.lease_id).await?;
                kept_alive += 1;
            }
        }
        Ok(kept_alive)
    }

    async fn drain_instance(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
    ) -> Result<DrainReport, PlacementError> {
        PlacementCoordinator::new(self.store.clone(), TonicLogicControl)
            .drain_instance(service_kind, instance_id)
            .await
    }

    async fn placement_route_resolver(
        &self,
        service_kind: ServiceKind,
    ) -> Result<(lattice_placement::BoxRouteResolver, PlacementWatchTask), PlacementError> {
        let coordinator = PlacementCoordinator::new(self.store.clone(), TonicLogicControl);
        let resolver = PlacementRouteResolver::new(
            service_kind,
            self.store.clone(),
            coordinator,
            RouteCacheConfig::default(),
        );
        let watch = resolver.start_placement_watch().await?;
        Ok((lattice_placement::BoxRouteResolver::new(resolver), watch))
    }

    async fn singleton_route_resolver(
        &self,
    ) -> Result<(lattice_placement::BoxRouteResolver, PlacementWatchTask), PlacementError> {
        let coordinator = lattice_placement::singleton::SingletonCoordinator::from_store(
            self.store.clone(),
            TonicLogicControl,
        );
        let resolver = SingletonRouteResolver::new(coordinator, RouteCacheConfig::default());
        Ok((
            lattice_placement::BoxRouteResolver::new(resolver),
            PlacementWatchTask::noop(),
        ))
    }
}

pub(crate) struct PlacementStoreRegistration<T>
where
    T: PlacementStore,
{
    component: Box<dyn ServiceComponent<T>>,
}

impl<T> PlacementStoreRegistration<T>
where
    T: PlacementStore,
{
    pub(crate) fn new<C>(component: C) -> Self
    where
        C: IntoServiceComponent<T>,
    {
        Self {
            component: Box::new(component.into_service_component()),
        }
    }
}

#[async_trait]
impl<T> ErasedPlacementStoreComponent for PlacementStoreRegistration<T>
where
    T: PlacementStore,
{
    fn target_name(&self) -> &'static str {
        "placement_store"
    }

    fn type_name(&self) -> &'static str {
        std::any::type_name::<T>()
    }

    async fn build(
        self: Box<Self>,
        ctx: &ServiceComponentContext,
        service: &mut ServiceContextBuilder,
    ) -> Result<Box<dyn ErasedPlacementStore>, LatticeServiceError> {
        let store = self.component.build(ctx).await?;
        service
            .insert_extension(PlacementStoreComponent::new(store.clone()))
            .map_err(|component| LatticeServiceError::DuplicateServiceComponent {
                component: component.to_string(),
            })?;
        Ok(Box::new(PlacementStoreHandle::new(store)))
    }
}

pub(crate) struct ServiceComponentRegistration<T>
where
    T: Send + Sync + 'static,
{
    target_name: &'static str,
    insert: fn(&mut ServiceContextBuilder, T) -> Result<(), &'static str>,
    component: Box<dyn ServiceComponent<T>>,
}

impl<T> ServiceComponentRegistration<T>
where
    T: Send + Sync + 'static,
{
    pub(crate) fn cluster_event_bus<C>(component: C) -> Self
    where
        T: lattice_eventbus::EventBus,
        C: IntoServiceComponent<T>,
    {
        Self {
            target_name: "cluster_event_bus",
            insert: insert_cluster_event_bus_component::<T>,
            component: Box::new(component.into_service_component()),
        }
    }

    pub(crate) fn local_event_bus<C>(component: C) -> Self
    where
        T: lattice_eventbus::EventBus,
        C: IntoServiceComponent<T>,
    {
        Self {
            target_name: "local_event_bus",
            insert: insert_local_event_bus_component::<T>,
            component: Box::new(component.into_service_component()),
        }
    }

    pub(crate) fn config_store<C>(component: C) -> Self
    where
        T: lattice_config::ConfigStore,
        C: IntoServiceComponent<T>,
    {
        Self {
            target_name: "config_store",
            insert: insert_config_store_component::<T>,
            component: Box::new(component.into_service_component()),
        }
    }

    pub(crate) fn extension<C>(component: C) -> Self
    where
        C: IntoServiceComponent<T>,
    {
        Self {
            target_name: "extension",
            insert: |service, value| service.insert_extension(value),
            component: Box::new(component.into_service_component()),
        }
    }
}

fn insert_cluster_event_bus_component<T>(
    service: &mut ServiceContextBuilder,
    event_bus: T,
) -> Result<(), &'static str>
where
    T: lattice_eventbus::EventBus,
{
    service.insert_extension(ClusterEventBusComponent::new(event_bus))
}

fn insert_local_event_bus_component<T>(
    service: &mut ServiceContextBuilder,
    event_bus: T,
) -> Result<(), &'static str>
where
    T: lattice_eventbus::EventBus,
{
    service.insert_extension(LocalEventBusComponent::new(event_bus))
}

fn insert_config_store_component<T>(
    service: &mut ServiceContextBuilder,
    store: T,
) -> Result<(), &'static str>
where
    T: lattice_config::ConfigStore,
{
    service.insert_extension(ConfigStoreComponent::new(store))
}

#[async_trait]
impl<T> ErasedServiceComponent for ServiceComponentRegistration<T>
where
    T: Send + Sync + 'static,
{
    fn target_name(&self) -> &'static str {
        self.target_name
    }

    fn type_name(&self) -> &'static str {
        std::any::type_name::<T>()
    }

    async fn build(
        self: Box<Self>,
        ctx: &ServiceComponentContext,
        service: &mut ServiceContextBuilder,
    ) -> Result<(), LatticeServiceError> {
        let target_name = self.target_name;
        let insert = self.insert;
        let value = self.component.build(ctx).await?;
        insert(service, value).map_err(|duplicate| {
            if target_name == "extension" {
                LatticeServiceError::DuplicateServiceExtension {
                    type_name: duplicate.to_string(),
                }
            } else {
                LatticeServiceError::DuplicateServiceComponent {
                    component: duplicate.to_string(),
                }
            }
        })
    }
}
