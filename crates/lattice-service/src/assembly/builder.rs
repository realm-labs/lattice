use std::any::{TypeId, type_name};
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

use lattice_actor::traits::{Actor, Handler};
use lattice_config::source::ConfigSource;
use lattice_config::store::ConfigStore;
use lattice_core::direct_link::messages::{
    LinkBackpressure, LinkClosed, LinkDirectionClosed, LinkOpened,
};
use lattice_core::direct_link::stream::DirectLinkMetadata;
use lattice_core::instance::InstanceId;
use lattice_core::kind::ServiceKind;
use lattice_direct_link::delivery::DirectLinkDispatch;
use lattice_direct_link::stream::DirectLinkActorBinding;
use lattice_eventbus::local::EventBus;
use lattice_ops::ops_config::AdminHttpConfig;
use lattice_placement::authority::{DevelopmentInProcessPlacementAuthority, PlacementAuthority};
use lattice_placement::routing::placement::PlacementWatchStarter;
use lattice_placement::routing::rpc::RpcRetryPolicy;
use lattice_placement::storage::{PlacementReadStore, PlacementStore};
use lattice_rpc::client::TonicEndpointChannelPoolConfig;
use lattice_rpc::security::{RpcSecurityPolicy, RpcServerSecurity, RpcTransportSecurity};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::actors::registration::ActorRegistration;
use crate::actors::registration::ErasedActorRegistration;
use crate::assembly::placement_watch::{ErasedPlacementWatchStarter, PlacementWatchRegistration};
use crate::clients::{ErasedRpcClientBinding, RpcClientRegistration};
use crate::clients::{RpcClientBinding, RpcServiceBinding};
use crate::components::{
    ErasedPlacementAuthorityComponent, ErasedPlacementStoreComponent, ErasedServiceComponent,
    IntoServiceComponent, PlacementAuthorityRegistration, PlacementStoreRegistration,
    ServiceComponentRegistration,
};
use crate::config::{DirectLinkConfig, InstanceConfig};
use crate::direct_links::{DirectLinkBindingRegistration, ErasedDirectLinkBinding};

pub struct LatticeServiceBuilder {
    pub(crate) service_kind: ServiceKind,
    pub(crate) instance: Option<InstanceConfig>,
    pub(crate) listener: Option<TcpListener>,
    pub(crate) ready: Option<oneshot::Sender<SocketAddr>>,
    pub(crate) instance_lease_keepalive_interval: Duration,
    pub(crate) actor_registrations: Vec<Box<dyn ErasedActorRegistration>>,
    pub(crate) rpc_services: Vec<Box<dyn RpcServiceBinding>>,
    pub(crate) client_bindings: Vec<Box<dyn ErasedRpcClientBinding>>,
    pub(crate) direct_link_bindings: Vec<Box<dyn ErasedDirectLinkBinding>>,
    pub(crate) config: Option<ConfigSource>,
    pub(crate) placement_store: Option<Box<dyn ErasedPlacementStoreComponent>>,
    pub(crate) placement_authority: Option<Box<dyn ErasedPlacementAuthorityComponent>>,
    pub(crate) cluster_event_bus: Option<Box<dyn ErasedServiceComponent>>,
    pub(crate) local_event_bus: Option<Box<dyn ErasedServiceComponent>>,
    pub(crate) config_store: Option<Box<dyn ErasedServiceComponent>>,
    pub(crate) admin_http: Option<AdminHttpConfig>,
    pub(crate) rpc_security: RpcServerSecurity,
    pub(crate) rpc_transport_security: RpcTransportSecurity,
    pub(crate) rpc_client_transport: TonicEndpointChannelPoolConfig,
    pub(crate) rpc_retry_policy: RpcRetryPolicy,
    pub(crate) direct_link: Option<DirectLinkConfig>,
    pub(crate) placement_watchers: Vec<Box<dyn ErasedPlacementWatchStarter>>,
    pub(crate) duplicate_framework_component: Option<&'static str>,
    pub(crate) duplicate_extension: Option<&'static str>,
    pub(crate) extensions: HashMap<TypeId, Box<dyn ErasedServiceComponent>>,
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
            .field(
                "instance_lease_keepalive_interval",
                &self.instance_lease_keepalive_interval,
            )
            .field("actor_registration_count", &self.actor_registrations.len())
            .field("rpc_service_count", &self.rpc_services.len())
            .field("client_binding_count", &self.client_bindings.len())
            .field(
                "direct_link_binding_count",
                &self.direct_link_bindings.len(),
            )
            .field("has_config", &self.config.is_some())
            .field("has_placement_store", &self.placement_store.is_some())
            .field(
                "has_placement_authority",
                &self.placement_authority.is_some(),
            )
            .field("has_cluster_event_bus", &self.cluster_event_bus.is_some())
            .field("has_local_event_bus", &self.local_event_bus.is_some())
            .field("has_config_store", &self.config_store.is_some())
            .field("has_admin_http", &self.admin_http.is_some())
            .field("rpc_security", &self.rpc_security)
            .field("rpc_transport_security", &self.rpc_transport_security)
            .field("rpc_client_transport", &self.rpc_client_transport)
            .field("rpc_retry_policy", &self.rpc_retry_policy)
            .field("direct_link", &self.direct_link)
            .field("placement_watch_count", &self.placement_watchers.len())
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
            instance_lease_keepalive_interval: Duration::from_secs(10),
            actor_registrations: Vec::new(),
            rpc_services: Vec::new(),
            client_bindings: Vec::new(),
            direct_link_bindings: Vec::new(),
            config: None,
            placement_store: None,
            placement_authority: None,
            cluster_event_bus: None,
            local_event_bus: None,
            config_store: None,
            admin_http: None,
            rpc_security: RpcServerSecurity::disabled(),
            rpc_transport_security: RpcTransportSecurity::plaintext(),
            rpc_client_transport: TonicEndpointChannelPoolConfig::default(),
            rpc_retry_policy: RpcRetryPolicy::default(),
            direct_link: None,
            placement_watchers: Vec::new(),
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

    pub fn instance_lease_keepalive_interval(mut self, interval: Duration) -> Self {
        if !interval.is_zero() {
            self.instance_lease_keepalive_interval = interval;
        }
        self
    }

    pub fn config(mut self, config: ConfigSource) -> Self {
        self.config = Some(config);
        self
    }

    pub fn placement_store<T, C>(mut self, store: C) -> Self
    where
        T: PlacementReadStore,
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

    pub fn placement_authority<A>(mut self, authority: A) -> Self
    where
        A: PlacementAuthority,
    {
        if self.placement_authority.is_some() {
            self.duplicate_framework_component
                .get_or_insert("placement_authority");
        } else {
            self.placement_authority =
                Some(Box::new(PlacementAuthorityRegistration::new(authority)));
        }
        self
    }

    /// Configures an in-process writable placement authority for development.
    ///
    /// Production services must use [`Self::placement_authority`] with a remote
    /// semantic authority so they never receive placement-writer credentials.
    pub fn dangerously_use_in_process_placement<S, L>(self, store: S, logic: L) -> Self
    where
        S: PlacementStore,
        L: Clone,
        DevelopmentInProcessPlacementAuthority<S, L>: PlacementAuthority,
    {
        let authority = DevelopmentInProcessPlacementAuthority::new(store.clone(), logic);
        self.placement_store::<S, _>(store)
            .placement_authority(authority)
    }

    pub fn cluster_event_bus<T, C>(mut self, event_bus: C) -> Self
    where
        T: EventBus,
        C: IntoServiceComponent<T>,
    {
        if self.cluster_event_bus.is_some() {
            self.duplicate_framework_component
                .get_or_insert("cluster_event_bus");
        } else {
            self.cluster_event_bus = Some(Box::new(
                ServiceComponentRegistration::<T>::cluster_event_bus(event_bus),
            ));
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

    pub fn admin_http(mut self, config: AdminHttpConfig) -> Self {
        self.admin_http = Some(config);
        self
    }

    pub fn rpc_security(mut self, policy: RpcSecurityPolicy) -> Self {
        self.rpc_security = RpcServerSecurity::new(policy);
        self
    }

    pub fn rpc_transport_security(mut self, security: RpcTransportSecurity) -> Self {
        self.rpc_transport_security = security;
        self
    }

    pub fn rpc_client_transport(mut self, config: TonicEndpointChannelPoolConfig) -> Self {
        self.rpc_client_transport = config;
        self
    }

    pub fn rpc_retry_policy(mut self, policy: RpcRetryPolicy) -> Self {
        self.rpc_retry_policy = policy;
        self
    }

    pub fn direct_links(mut self, config: DirectLinkConfig) -> Self {
        self.direct_link = Some(config);
        self
    }

    pub fn register_direct_link<A, Messages, Metadata>(
        mut self,
        binding: DirectLinkActorBinding<A, Messages, Metadata>,
    ) -> Self
    where
        A: Actor + Sync,
        A: Handler<LinkOpened>
            + Handler<LinkDirectionClosed>
            + Handler<LinkClosed>
            + Handler<LinkBackpressure>,
        Metadata: DirectLinkMetadata,
        Messages: DirectLinkDispatch<A, Metadata>,
    {
        self.direct_link_bindings
            .push(Box::new(DirectLinkBindingRegistration::new(binding)));
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
        self.client_bindings
            .push(Box::new(RpcClientRegistration::<B>::new()));
        self
    }

    pub fn placement_watch<W>(mut self, watcher: W) -> Self
    where
        W: PlacementWatchStarter,
    {
        self.placement_watchers
            .push(Box::new(PlacementWatchRegistration { watcher }));
        self
    }
}
