use std::collections::HashSet;
use std::fmt;
use std::net::SocketAddr;

use lattice_actor::Actor;
use lattice_core::{ActorKind, InstanceId, ServiceKind};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tracing::{debug, info};

use crate::actor::ActorRegistration;
use crate::actor::ErasedActorRegistration;
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
    component_labels: Vec<&'static str>,
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
            .field("component_labels", &self.component_labels)
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
            component_labels: Vec::new(),
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

    pub fn config<T>(mut self, _config: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        self.component_labels.push("config");
        self
    }

    pub fn placement_store<T>(mut self, _store: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        self.component_labels.push("placement_store");
        self
    }

    pub fn event_bus<T>(mut self, _event_bus: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        self.component_labels.push("event_bus");
        self
    }

    pub fn local_event_bus<T>(mut self, _event_bus: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        self.component_labels.push("local_event_bus");
        self
    }

    pub fn config_store<T>(mut self, _store: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        self.component_labels.push("config_store");
        self
    }

    pub fn telemetry<T>(mut self, _telemetry: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        self.component_labels.push("telemetry");
        self
    }

    pub fn admin_http<T>(mut self, _admin_http: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        self.component_labels.push("admin_http");
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
        info!(
            service.kind = self.service_kind.as_str(),
            instance.id = instance.instance_id.as_str(),
            actor.registrations = self.actor_registrations.len(),
            rpc.services = self.rpc_services.len(),
            rpc.clients = self.client_bindings.len(),
            "building lattice service"
        );
        let mut context = ServiceBuildContext::new(self.service_kind.clone());
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
            self.ready,
        ))
    }
}
