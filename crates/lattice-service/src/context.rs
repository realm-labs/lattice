use std::any::Any;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

use lattice_actor::Actor;
use lattice_core::{ActorKind, ServiceContext};
use lattice_rpc::RpcServerSecurity;
use tonic::body::Body;
use tonic::codegen::Service;
use tonic::server::NamedService;
use tonic::transport::Server;
use tonic::transport::server::Router;

use crate::LatticeServiceError;
use crate::actor::{ErasedLogicActor, RegisteredActor};

pub struct ServiceBuildContext {
    service: ServiceContext,
    rpc_security: RpcServerSecurity,
    pub(crate) actors: HashMap<ActorKind, Box<dyn Any + Send>>,
    pub(crate) logic_actors: HashMap<ActorKind, Arc<dyn ErasedLogicActor>>,
    pub(crate) router: Option<Router>,
}

impl std::fmt::Debug for ServiceBuildContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServiceBuildContext")
            .field("service", &self.service)
            .field("rpc_security", &self.rpc_security)
            .field("actor_count", &self.actors.len())
            .field("logic_actor_count", &self.logic_actors.len())
            .field("has_router", &self.router.is_some())
            .finish()
    }
}

impl ServiceBuildContext {
    pub fn new(service: ServiceContext) -> Self {
        Self::with_rpc_security(service, RpcServerSecurity::disabled())
    }

    pub(crate) fn with_rpc_security(
        service: ServiceContext,
        rpc_security: RpcServerSecurity,
    ) -> Self {
        Self {
            service,
            rpc_security,
            actors: HashMap::new(),
            logic_actors: HashMap::new(),
            router: None,
        }
    }

    pub fn service_context(&self) -> ServiceContext {
        self.service.clone()
    }

    pub fn rpc_security(&self) -> RpcServerSecurity {
        self.rpc_security.clone()
    }

    pub fn add_rpc_service<S>(&mut self, service: S)
    where
        S: Service<http::Request<Body>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Response: axum::response::IntoResponse,
        S::Future: Send + 'static,
    {
        self.router = Some(match self.router.take() {
            Some(router) => router.add_service(service),
            None => Server::builder().add_service(service),
        });
    }

    pub fn actor<A>(
        &self,
        actor_kind: &ActorKind,
    ) -> Result<RegisteredActor<A>, LatticeServiceError>
    where
        A: Actor,
    {
        let registered = self.actors.get(actor_kind).ok_or_else(|| {
            LatticeServiceError::MissingActorRegistration {
                actor_kind: actor_kind.clone(),
            }
        })?;
        registered
            .downcast_ref::<RegisteredActor<A>>()
            .cloned()
            .ok_or_else(|| LatticeServiceError::ActorTypeMismatch {
                actor_kind: actor_kind.clone(),
                expected_type: std::any::type_name::<A>(),
            })
    }
}
