use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use lattice_config::bootstrap::BootstrapConfig;
use lattice_config::store::LocalConfigStore;
use lattice_core::direct_link::runtime::{
    DirectLinkLifecycleRuntimeHandle, DirectLinkRuntimeHandle,
};
use lattice_core::kind::ActorKind;
use lattice_core::service_context::ServiceContext;
use lattice_eventbus::local::LocalEventBus;
use lattice_ops::scheduler::ServiceScheduler;
use lattice_placement::storage::PlacementPrefix;
use lattice_placement::storage::memory::InMemoryPlacementStore;
use tracing::{debug, info};

use crate::assembly::admin::build_admin_http;
use crate::assembly::builder::LatticeServiceBuilder;
use crate::assembly::components::{
    build_framework_component_or_default, build_placement_store_or_default, build_service_component,
};
use crate::assembly::placement_watch::start_placement_watchers;
use crate::clients::RpcClientPlacement;
use crate::components::{
    PlacementStoreRegistration, ServiceComponentContext, ServiceComponentRegistration,
};
use crate::context::ServiceBuildContext;
use crate::control::ServiceLogicControlHandler;
use crate::direct_links::{
    DeferredDirectLinkLifecycleRuntime, DeferredDirectLinkRuntime, build_direct_link_runtime,
};
use crate::error::LatticeServiceError;
use crate::framework::scheduler::ServiceSchedulerComponent;
use crate::runtime::service::{LatticeService, LatticeServiceParts};

impl LatticeServiceBuilder {
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
        let mut service_context =
            ServiceContext::builder(self.service_kind.clone(), instance.instance_id.clone());
        let admin_actor_kinds = self
            .actor_registrations
            .iter()
            .map(|registration| registration.actor_kind().clone())
            .collect();
        let admin_http = build_admin_http(self.admin_http, admin_actor_kinds).await?;
        let placement_watchers = self.placement_watchers;
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
        match (self.cluster_event_bus, self.local_event_bus) {
            (None, None) => {
                build_service_component(
                    Box::new(
                        ServiceComponentRegistration::<LocalEventBus>::cluster_event_bus(
                            LocalEventBus::default(),
                        ),
                    ),
                    &component_context,
                    &mut service_context,
                    self.service_kind.as_str(),
                )
                .await?;
            }
            (cluster_event_bus, local_event_bus) => {
                build_framework_component_or_default(
                    cluster_event_bus,
                    Box::new(
                        ServiceComponentRegistration::<LocalEventBus>::cluster_event_bus(
                            LocalEventBus::default(),
                        ),
                    ),
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
        service_context
            .insert_extension(ServiceSchedulerComponent::new(ServiceScheduler::new()))
            .map_err(|component| LatticeServiceError::DuplicateServiceComponent {
                component: component.to_string(),
            })?;
        for extension in self.extensions.into_values() {
            build_service_component(
                extension,
                &component_context,
                &mut service_context,
                self.service_kind.as_str(),
            )
            .await?;
        }
        let rpc_client_count = self.client_bindings.len();
        let mut placement_watch_tasks =
            start_placement_watchers(placement_watchers, self.service_kind.as_str()).await?;
        for binding in self.client_bindings {
            let client_service_kind = binding.service_kind();
            debug!(
                service.kind = self.service_kind.as_str(),
                rpc.client.service = client_service_kind.as_str(),
                rpc.client.core = binding.core_type(),
                "registering rpc client binding"
            );
            let (default_resolver, watch_task) = match binding.placement() {
                RpcClientPlacement::Actor => {
                    placement_store
                        .placement_route_resolver(client_service_kind)
                        .await?
                }
                RpcClientPlacement::Singleton => placement_store.singleton_route_resolver().await?,
            };
            placement_watch_tasks.push(watch_task);
            let context_factory = self
                .rpc_security
                .client_context_factory(self.service_kind.clone(), instance.instance_id.clone());
            binding.register(
                &mut service_context,
                Some(default_resolver),
                context_factory,
                self.rpc_retry_policy,
                self.rpc_transport_security.clone(),
                self.rpc_client_transport,
            )?;
        }
        let direct_link_enabled =
            self.direct_link.is_some() || !self.direct_link_bindings.is_empty();
        let direct_link_runtime_handle = if direct_link_enabled {
            let runtime = Arc::new(DeferredDirectLinkRuntime::default());
            service_context
                .insert_extension(DirectLinkRuntimeHandle::new(runtime.clone()))
                .map_err(|component| LatticeServiceError::DuplicateServiceComponent {
                    component: component.to_string(),
                })?;
            Some(runtime)
        } else {
            None
        };
        let direct_link_lifecycle_runtime = if direct_link_enabled {
            let runtime = Arc::new(DeferredDirectLinkLifecycleRuntime::default());
            service_context
                .insert_extension(DirectLinkLifecycleRuntimeHandle::new(runtime.clone()))
                .map_err(|component| LatticeServiceError::DuplicateServiceComponent {
                    component: component.to_string(),
                })?;
            Some(runtime)
        } else {
            None
        };
        let service_context = service_context.build();

        info!(
            service.kind = self.service_kind.as_str(),
            instance.id = instance.instance_id.as_str(),
            actor.registrations = self.actor_registrations.len(),
            rpc.services = self.rpc_services.len(),
            rpc.clients = rpc_client_count,
            placement.watches = placement_watch_tasks.len(),
            service.extensions = service_context.extension_count(),
            "building lattice service"
        );
        let mut context = ServiceBuildContext::with_rpc_security_and_transport(
            service_context.clone(),
            self.rpc_security,
            self.rpc_transport_security,
        )?;
        let actor_ref_endpoint: http::Uri = format!("http://{}", listener.local_addr()?)
            .parse()
            .map_err(|error| LatticeServiceError::ComponentBuild {
                slot: "actor_ref".to_string(),
                message: format!("failed to build actor self endpoint: {error}"),
            })?;
        context.set_actor_ref_endpoint(actor_ref_endpoint);
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
        let direct_link_runtime =
            build_direct_link_runtime(self.direct_link_bindings, &context, direct_link_enabled)?;
        if let (Some(runtime), Some(direct_link_config)) =
            (direct_link_runtime.as_ref(), self.direct_link.as_ref())
        {
            let max_active_links = direct_link_config.max_active_links_config();
            let max_open_links_per_second = direct_link_config.max_open_links_per_second_config();
            let max_messages_per_second = direct_link_config.max_messages_per_second_config();
            runtime
                .session_manager()
                .update_validation_policy(|policy| {
                    let mut policy = policy;
                    if let Some(max_active_links) = max_active_links {
                        policy = policy.max_active_links(max_active_links);
                    }
                    if let Some(max_open_links) = max_open_links_per_second {
                        policy = policy.open_rate_limit(max_open_links, Duration::from_secs(1));
                    }
                    if let Some(max_messages) = max_messages_per_second {
                        policy = policy.message_rate_limit(max_messages, Duration::from_secs(1));
                    }
                    policy
                });
        }
        if let (Some(deferred), Some(runtime)) = (
            direct_link_lifecycle_runtime.as_ref(),
            direct_link_runtime.clone(),
        ) {
            deferred.set_runtime(runtime);
        }
        if let (Some(deferred), Some(runtime)) = (
            direct_link_runtime_handle.as_ref(),
            direct_link_runtime.clone(),
        ) {
            deferred.set_runtime(runtime);
        }
        if !context.logic_actors.is_empty() {
            let handler = ServiceLogicControlHandler::new(
                context.logic_actors.clone(),
                direct_link_runtime.clone(),
            );
            context.add_rpc_service(
                lattice_placement::control::proto::logic_control_server::LogicControlServer::new(
                    lattice_placement::control::LogicControlService::new(handler),
                ),
            );
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

        let logic_actors = context.logic_actors.values().cloned().collect();
        let router = context.router.ok_or(LatticeServiceError::NoRpcServices)?;
        Ok(LatticeService::new(LatticeServiceParts {
            service_kind: self.service_kind,
            instance,
            listener,
            router,
            service_context,
            logic_actors,
            placement_store,
            placement_watch_tasks,
            admin_http,
            instance_lease_keepalive_interval: self.instance_lease_keepalive_interval,
            direct_link: self.direct_link,
            direct_link_runtime,
            ready: self.ready,
        }))
    }
}
