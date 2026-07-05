use std::any::{TypeId, type_name};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;

use lattice_actor::Actor;
use lattice_config::{BootstrapConfig, ConfigSource};
use lattice_config::{ConfigStore, LocalConfigStore};
use lattice_core::{ActorKind, InstanceId, ServiceContext, ServiceKind};
use lattice_eventbus::{EventBus, LocalEventBus};
use lattice_placement::store::{InMemoryPlacementStore, PlacementPrefix, PlacementStore};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tracing::{debug, info};

use crate::actor::ActorRegistration;
use crate::actor::ErasedActorRegistration;
use crate::component::{
    ErasedPlacementStore, ErasedPlacementStoreComponent, ErasedServiceComponent,
    IntoServiceComponent, PlacementStoreRegistration, ServiceComponentContext,
    ServiceComponentRegistration,
};
use crate::config::InstanceConfig;
use crate::context::ServiceBuildContext;
use crate::{LatticeService, LatticeServiceError, RpcClientBinding, RpcServiceBinding};

pub struct LatticeServiceBuilder {
    service_kind: ServiceKind,
    instance: Option<InstanceConfig>,
    listener: Option<TcpListener>,
    ready: Option<oneshot::Sender<SocketAddr>>,
    actor_registrations: Vec<Box<dyn ErasedActorRegistration>>,
    rpc_services: Vec<Box<dyn RpcServiceBinding>>,
    client_bindings: Vec<String>,
    config: Option<ConfigSource>,
    placement_store: Option<Box<dyn ErasedPlacementStoreComponent>>,
    event_bus: Option<Box<dyn ErasedServiceComponent>>,
    local_event_bus: Option<Box<dyn ErasedServiceComponent>>,
    config_store: Option<Box<dyn ErasedServiceComponent>>,
    duplicate_framework_component: Option<&'static str>,
    duplicate_extension: Option<&'static str>,
    extensions: HashMap<TypeId, Box<dyn ErasedServiceComponent>>,
}

impl fmt::Debug for LatticeServiceBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LatticeServiceBuilder")
            .field("service_kind", &self.service_kind)
            .field("instance", &self.instance)
            .field(
                "listener",
                &self.listener.as_ref().map(TcpListener::local_addr),
            )
            .field("has_ready_signal", &self.ready.is_some())
            .field("actor_registration_count", &self.actor_registrations.len())
            .field("rpc_service_count", &self.rpc_services.len())
            .field("client_bindings", &self.client_bindings)
            .field("has_config", &self.config.is_some())
            .field("has_placement_store", &self.placement_store.is_some())
            .field("has_event_bus", &self.event_bus.is_some())
            .field("has_local_event_bus", &self.local_event_bus.is_some())
            .field("has_config_store", &self.config_store.is_some())
            .field("extension_count", &self.extensions.len())
            .finish()
    }
}

impl LatticeServiceBuilder {
    pub fn new(service_kind: ServiceKind) -> Self {
        Self {
            service_kind,
            instance: None,
            listener: None,
            ready: None,
            actor_registrations: Vec::new(),
            rpc_services: Vec::new(),
            client_bindings: Vec::new(),
            config: None,
            placement_store: None,
            event_bus: None,
            local_event_bus: None,
            config_store: None,
            duplicate_framework_component: None,
            duplicate_extension: None,
            extensions: HashMap::new(),
        }
    }

    pub fn service_kind(&self) -> &ServiceKind {
        &self.service_kind
    }

    pub fn instance_config(&self) -> Option<&InstanceConfig> {
        self.instance.as_ref()
    }

    pub fn instance(mut self, instance: InstanceConfig) -> Self {
        self.instance = Some(instance);
        self
    }

    pub fn instance_id(self, instance_id: InstanceId) -> Self {
        self.instance(InstanceConfig::new(instance_id))
    }

    pub fn listen(mut self, listener: TcpListener) -> Self {
        self.listener = Some(listener);
        self
    }

    pub fn ready_signal(mut self, ready: oneshot::Sender<SocketAddr>) -> Self {
        self.ready = Some(ready);
        self
    }

    pub fn config(mut self, config: ConfigSource) -> Self {
        self.config = Some(config);
        self
    }

    pub fn placement_store<T, C>(mut self, store: C) -> Self
    where
        T: PlacementStore,
        C: IntoServiceComponent<T>,
    {
        if self.placement_store.is_some() {
            self.duplicate_framework_component
                .get_or_insert("placement_store");
        } else {
            self.placement_store = Some(Box::new(PlacementStoreRegistration::<T>::new(store)));
        }
        self
    }

    pub fn event_bus<T, C>(mut self, event_bus: C) -> Self
    where
        T: EventBus,
        C: IntoServiceComponent<T>,
    {
        if self.event_bus.is_some() {
            self.duplicate_framework_component
                .get_or_insert("event_bus");
        } else {
            self.event_bus = Some(Box::new(ServiceComponentRegistration::<T>::event_bus(
                event_bus,
            )));
        }
        self
    }

    pub fn local_event_bus<T, C>(mut self, event_bus: C) -> Self
    where
        T: EventBus,
        C: IntoServiceComponent<T>,
    {
        if self.local_event_bus.is_some() {
            self.duplicate_framework_component
                .get_or_insert("local_event_bus");
        } else {
            self.local_event_bus = Some(Box::new(
                ServiceComponentRegistration::<T>::local_event_bus(event_bus),
            ));
        }
        self
    }

    pub fn config_store<T, C>(mut self, store: C) -> Self
    where
        T: ConfigStore,
        C: IntoServiceComponent<T>,
    {
        if self.config_store.is_some() {
            self.duplicate_framework_component
                .get_or_insert("config_store");
        } else {
            self.config_store = Some(Box::new(ServiceComponentRegistration::<T>::config_store(
                store,
            )));
        }
        self
    }

    pub fn extension<T, C>(mut self, extension: C) -> Self
    where
        T: Send + Sync + 'static,
        C: IntoServiceComponent<T>,
    {
        let type_id = TypeId::of::<T>();
        match self.extensions.entry(type_id) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(Box::new(ServiceComponentRegistration::<T>::extension(
                    extension,
                )));
            }
            std::collections::hash_map::Entry::Occupied(_) => {
                self.duplicate_extension.get_or_insert(type_name::<T>());
            }
        }
        self
    }

    pub fn register_actor<A>(mut self, registration: ActorRegistration<A>) -> Self
    where
        A: Actor + Sync,
    {
        self.actor_registrations.push(Box::new(registration));
        self
    }

    pub fn register_sharded_rpc<B>(mut self, binding: B) -> Self
    where
        B: RpcServiceBinding,
    {
        self.rpc_services.push(Box::new(binding));
        self
    }

    pub fn register_client<B>(mut self) -> Self
    where
        B: RpcClientBinding,
    {
        self.client_bindings.push(B::SERVICE_KIND.to_string());
        self
    }

    pub async fn build(self) -> Result<LatticeService, LatticeServiceError> {
        let listener = self.listener.ok_or(LatticeServiceError::MissingListener)?;
        let instance = self
            .instance
            .ok_or(LatticeServiceError::MissingInstanceConfig)?;
        let bootstrap_config = match self.config {
            Some(source) => source.load().map_err(|error| LatticeServiceError::Config {
                message: error.to_string(),
            })?,
            None => BootstrapConfig::default(),
        };
        let component_context = ServiceComponentContext {
            service_kind: self.service_kind.clone(),
            instance_id: instance.instance_id.clone(),
            bootstrap_config: bootstrap_config.clone(),
        };
        if let Some(component) = self.duplicate_framework_component {
            return Err(LatticeServiceError::DuplicateServiceComponent {
                component: component.to_string(),
            });
        }
        if let Some(type_name) = self.duplicate_extension {
            return Err(LatticeServiceError::DuplicateServiceExtension {
                type_name: type_name.to_string(),
            });
        }
        let mut service_context = ServiceContext::builder(
            self.service_kind.clone(),
            instance.instance_id.clone(),
            bootstrap_config,
        );
        let placement_store = build_placement_store_or_default(
            self.placement_store,
            Box::new(PlacementStoreRegistration::<InMemoryPlacementStore>::new(
                InMemoryPlacementStore::new(PlacementPrefix::new(format!(
                    "/lattice/{}/placement",
                    self.service_kind.as_str()
                ))),
            )),
            &component_context,
            &mut service_context,
            self.service_kind.as_str(),
        )
        .await?;
        match (self.event_bus, self.local_event_bus) {
            (None, None) => {
                build_service_component(
                    Box::new(ServiceComponentRegistration::<LocalEventBus>::event_bus(
                        LocalEventBus::default(),
                    )),
                    &component_context,
                    &mut service_context,
                    self.service_kind.as_str(),
                )
                .await?;
            }
            (event_bus, local_event_bus) => {
                build_framework_component_or_default(
                    event_bus,
                    Box::new(ServiceComponentRegistration::<LocalEventBus>::event_bus(
                        LocalEventBus::default(),
                    )),
                    &component_context,
                    &mut service_context,
                    self.service_kind.as_str(),
                )
                .await?;
                if let Some(local_event_bus) = local_event_bus {
                    build_service_component(
                        local_event_bus,
                        &component_context,
                        &mut service_context,
                        self.service_kind.as_str(),
                    )
                    .await?;
                }
            }
        }
        build_framework_component_or_default(
            self.config_store,
            Box::new(
                ServiceComponentRegistration::<LocalConfigStore>::config_store(
                    LocalConfigStore::default(),
                ),
            ),
            &component_context,
            &mut service_context,
            self.service_kind.as_str(),
        )
        .await?;
        for extension in self.extensions.into_values() {
            build_service_component(
                extension,
                &component_context,
                &mut service_context,
                self.service_kind.as_str(),
            )
            .await?;
        }
        let service_context = service_context.build();

        info!(
            service.kind = self.service_kind.as_str(),
            instance.id = instance.instance_id.as_str(),
            actor.registrations = self.actor_registrations.len(),
            rpc.services = self.rpc_services.len(),
            rpc.clients = self.client_bindings.len(),
            service.extensions = service_context.extension_count(),
            "building lattice service"
        );
        let mut context = ServiceBuildContext::new(service_context.clone());
        let mut actor_kinds = HashSet::<ActorKind>::new();

        for registration in self.actor_registrations {
            let actor_kind = registration.actor_kind().clone();
            if !actor_kinds.insert(actor_kind.clone()) {
                return Err(LatticeServiceError::DuplicateActorRegistration { actor_kind });
            }
            debug!(
                service.kind = self.service_kind.as_str(),
                actor.kind = actor_kind.as_str(),
                "registering actor"
            );
            registration.register(&mut context)?;
        }

        let mut rpc_services = HashSet::<String>::new();
        for binding in self.rpc_services {
            let service_name = binding.service_name();
            if !rpc_services.insert(service_name.to_string()) {
                return Err(LatticeServiceError::DuplicateRpcService {
                    service_name: service_name.to_string(),
                });
            }
            debug!(
                service.kind = self.service_kind.as_str(),
                rpc.service = service_name,
                "registering rpc service"
            );
            binding.register(&mut context)?;
        }

        for service_kind in &self.client_bindings {
            debug!(
                service.kind = self.service_kind.as_str(),
                rpc.client.service = service_kind,
                "registered rpc client binding"
            );
        }

        let router = context.router.ok_or(LatticeServiceError::NoRpcServices)?;
        Ok(LatticeService::new(
            self.service_kind,
            instance,
            listener,
            router,
            service_context,
            placement_store,
            self.ready,
        ))
    }
}

async fn build_placement_store_or_default(
    configured: Option<Box<dyn ErasedPlacementStoreComponent>>,
    default: Box<dyn ErasedPlacementStoreComponent>,
    component_context: &ServiceComponentContext,
    service_context: &mut lattice_core::ServiceContextBuilder,
    service_kind: &str,
) -> Result<Box<dyn ErasedPlacementStore>, LatticeServiceError> {
    let component = configured.unwrap_or(default);
    debug!(
        service.kind = service_kind,
        component.target = component.target_name(),
        component.type = component.type_name(),
        "building service component"
    );
    component.build(component_context, service_context).await
}

async fn build_framework_component_or_default(
    configured: Option<Box<dyn ErasedServiceComponent>>,
    default: Box<dyn ErasedServiceComponent>,
    component_context: &ServiceComponentContext,
    service_context: &mut lattice_core::ServiceContextBuilder,
    service_kind: &str,
) -> Result<(), LatticeServiceError> {
    build_service_component(
        configured.unwrap_or(default),
        component_context,
        service_context,
        service_kind,
    )
    .await
}

async fn build_service_component(
    component: Box<dyn ErasedServiceComponent>,
    component_context: &ServiceComponentContext,
    service_context: &mut lattice_core::ServiceContextBuilder,
    service_kind: &str,
) -> Result<(), LatticeServiceError> {
    debug!(
        service.kind = service_kind,
        component.target = component.target_name(),
        component.type = component.type_name(),
        "building service component"
    );
    component.build(component_context, service_context).await
}
