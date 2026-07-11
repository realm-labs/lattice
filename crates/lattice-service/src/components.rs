use std::sync::Arc;

use async_trait::async_trait;
use lattice_config::bootstrap::BootstrapConfig;
use lattice_core::instance::{InstanceId, InstanceIncarnation};
use lattice_core::kind::ServiceKind;
use lattice_core::service_context::ConfiguredComponentBuilder;
use lattice_core::service_context::{ConfiguredComponent, ServiceContextBuilder};
use lattice_placement::authority::{MAX_SINGLETON_RENEWAL_CLAIMS, PlacementAuthority};
use lattice_placement::coordination::singleton::SingletonRouteResolver;
use lattice_placement::error::PlacementError;
use lattice_placement::registry::InstanceRecord;
use lattice_placement::routing::cache::RouteCacheConfig;
use lattice_placement::routing::placement::{
    PlacementRouteResolver, PlacementWatchStarter, PlacementWatchTask,
};
use lattice_placement::storage::{
    ActorPlacementRecord, PlacementReadStore, PlacementVersion, SingletonPlacementRecord,
    VirtualShardPlacementRecord,
};

use crate::error::LatticeServiceError;
use crate::framework::config_store::ConfigStoreComponent;
use crate::framework::event_bus::{ClusterEventBusComponent, LocalEventBusComponent};
use crate::framework::placement::PlacementStoreComponent;

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

pub(crate) trait ErasedPlacementAuthorityComponent: Send + Sync {
    fn type_name(&self) -> &'static str;

    fn build(self: Box<Self>) -> Arc<dyn PlacementAuthority>;
}

pub(crate) struct PlacementAuthorityRegistration<A>
where
    A: PlacementAuthority,
{
    authority: A,
}

impl<A> PlacementAuthorityRegistration<A>
where
    A: PlacementAuthority,
{
    pub(crate) fn new(authority: A) -> Self {
        Self { authority }
    }
}

impl<A> ErasedPlacementAuthorityComponent for PlacementAuthorityRegistration<A>
where
    A: PlacementAuthority,
{
    fn type_name(&self) -> &'static str {
        std::any::type_name::<A>()
    }

    fn build(self: Box<Self>) -> Arc<dyn PlacementAuthority> {
        Arc::new(self.authority)
    }
}

#[async_trait]
pub(crate) trait ErasedPlacementStore: std::fmt::Debug + Send + Sync {
    async fn list_instances(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<InstanceRecord>, PlacementError>;
    async fn list_actors(
        &self,
    ) -> Result<Vec<(PlacementVersion, ActorPlacementRecord)>, PlacementError>;
    async fn list_virtual_shards_for_service(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError>;
    async fn list_singletons(
        &self,
    ) -> Result<Vec<(PlacementVersion, SingletonPlacementRecord)>, PlacementError>;
    async fn singleton_owner_lease_claims(
        &self,
        service_kind: &ServiceKind,
        instance_id: &InstanceId,
        instance_incarnation: &InstanceIncarnation,
    ) -> Result<Vec<SingletonPlacementRecord>, PlacementError>;
    async fn placement_route_resolver(
        &self,
        service_kind: ServiceKind,
        authority: Arc<dyn PlacementAuthority>,
    ) -> Result<
        (
            lattice_placement::routing::resolver::BoxRouteResolver,
            PlacementWatchTask,
        ),
        PlacementError,
    >;
    async fn singleton_route_resolver(
        &self,
        authority: Arc<dyn PlacementAuthority>,
    ) -> Result<
        (
            lattice_placement::routing::resolver::BoxRouteResolver,
            PlacementWatchTask,
        ),
        PlacementError,
    >;
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
    T: PlacementReadStore,
{
    store: T,
}

impl<T> std::fmt::Debug for PlacementStoreHandle<T>
where
    T: PlacementReadStore,
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
    T: PlacementReadStore,
{
    fn new(store: T) -> Self {
        Self { store }
    }
}

#[async_trait]
impl<T> ErasedPlacementStore for PlacementStoreHandle<T>
where
    T: PlacementReadStore,
{
    async fn list_instances(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<InstanceRecord>, PlacementError> {
        self.store.list_instances(service_kind).await
    }

    async fn list_actors(
        &self,
    ) -> Result<Vec<(PlacementVersion, ActorPlacementRecord)>, PlacementError> {
        self.store.list_actors().await
    }

    async fn list_virtual_shards_for_service(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError> {
        self.store
            .list_virtual_shards_for_service(service_kind)
            .await
    }

    async fn list_singletons(
        &self,
    ) -> Result<Vec<(PlacementVersion, SingletonPlacementRecord)>, PlacementError> {
        self.store.list_singletons().await
    }

    async fn singleton_owner_lease_claims(
        &self,
        service_kind: &ServiceKind,
        instance_id: &InstanceId,
        instance_incarnation: &InstanceIncarnation,
    ) -> Result<Vec<SingletonPlacementRecord>, PlacementError> {
        let mut claims = Vec::new();
        for (_version, record) in self.store.list_singletons().await? {
            if &record.service_kind == service_kind
                && &record.owner == instance_id
                && &record.owner_incarnation == instance_incarnation
            {
                if claims.len() == MAX_SINGLETON_RENEWAL_CLAIMS {
                    return Err(PlacementError::SingletonRenewalLimitExceeded {
                        limit: MAX_SINGLETON_RENEWAL_CLAIMS,
                    });
                }
                claims.push(record);
            }
        }
        Ok(claims)
    }

    async fn placement_route_resolver(
        &self,
        service_kind: ServiceKind,
        authority: Arc<dyn PlacementAuthority>,
    ) -> Result<
        (
            lattice_placement::routing::resolver::BoxRouteResolver,
            PlacementWatchTask,
        ),
        PlacementError,
    > {
        let resolver = PlacementRouteResolver::new(
            service_kind,
            self.store.clone(),
            authority,
            RouteCacheConfig::default(),
        );
        let watch = resolver.start_placement_watch().await?;
        Ok((
            lattice_placement::routing::resolver::BoxRouteResolver::new(resolver),
            watch,
        ))
    }

    async fn singleton_route_resolver(
        &self,
        authority: Arc<dyn PlacementAuthority>,
    ) -> Result<
        (
            lattice_placement::routing::resolver::BoxRouteResolver,
            PlacementWatchTask,
        ),
        PlacementError,
    > {
        let resolver =
            SingletonRouteResolver::new(self.store.clone(), authority, RouteCacheConfig::default());
        Ok((
            lattice_placement::routing::resolver::BoxRouteResolver::new(resolver),
            PlacementWatchTask::noop(),
        ))
    }
}

pub(crate) struct PlacementStoreRegistration<T>
where
    T: PlacementReadStore,
{
    component: Box<dyn ServiceComponent<T>>,
}

impl<T> PlacementStoreRegistration<T>
where
    T: PlacementReadStore,
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
    T: PlacementReadStore,
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
        T: lattice_eventbus::local::EventBus,
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
        T: lattice_eventbus::local::EventBus,
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
        T: lattice_config::store::ConfigStore,
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
    T: lattice_eventbus::local::EventBus,
{
    service.insert_extension(ClusterEventBusComponent::new(event_bus))
}

fn insert_local_event_bus_component<T>(
    service: &mut ServiceContextBuilder,
    event_bus: T,
) -> Result<(), &'static str>
where
    T: lattice_eventbus::local::EventBus,
{
    service.insert_extension(LocalEventBusComponent::new(event_bus))
}

fn insert_config_store_component<T>(
    service: &mut ServiceContextBuilder,
    store: T,
) -> Result<(), &'static str>
where
    T: lattice_config::store::ConfigStore,
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

#[cfg(test)]
mod singleton_renewal_tests {
    use lattice_core::actor_ref::Epoch;
    use lattice_core::instance::{InstanceId, InstanceIncarnation};
    use lattice_core::{actor_kind, service_kind};
    use lattice_placement::storage::memory::InMemoryPlacementStore;
    use lattice_placement::storage::{
        PlacementPrefix, PlacementState, PlacementStore, SingletonKey, SingletonPlacementRecord,
    };

    use super::{ErasedPlacementStore, PlacementStoreHandle};

    #[tokio::test]
    async fn singleton_claim_discovery_ignores_records_from_a_previous_owner_boot() {
        let store =
            InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/service-singleton-renewal"));
        let current_lease = store.grant_instance_lease().await.unwrap();
        let stale_lease = store.grant_instance_lease().await.unwrap();
        for (scope, incarnation, lease_id) in [
            ("current", "world-a-current-boot", current_lease),
            ("stale", "world-a-previous-boot", stale_lease),
        ] {
            let key = SingletonKey {
                service_kind: service_kind!("World"),
                singleton_kind: actor_kind!("SeasonManager"),
                scope: scope.to_string(),
            };
            store
                .compare_and_put_singleton(
                    key.clone(),
                    None,
                    SingletonPlacementRecord {
                        service_kind: key.service_kind,
                        singleton_kind: key.singleton_kind,
                        scope: key.scope,
                        owner: InstanceId::new("world-a"),
                        owner_incarnation: InstanceIncarnation::new(incarnation),
                        epoch: Epoch(1),
                        lease_id,
                        state: PlacementState::Running,
                    },
                )
                .await
                .unwrap();
        }
        let handle = PlacementStoreHandle::new(store.clone());

        let claims = handle
            .singleton_owner_lease_claims(
                &service_kind!("World"),
                &InstanceId::new("world-a"),
                &InstanceIncarnation::new("world-a-current-boot"),
            )
            .await
            .unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].scope, "current");
        assert_eq!(store.instance_lease_keepalive_count(current_lease), Some(0));
        assert_eq!(store.instance_lease_keepalive_count(stale_lease), Some(0));
    }
}
