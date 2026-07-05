use std::any::Any;
use std::collections::HashMap;
use std::convert::Infallible;

use lattice_actor::Actor;
use lattice_core::{ActorKind, ServiceKind};
use tonic::body::Body;
use tonic::codegen::Service;
use tonic::server::NamedService;
use tonic::transport::Server;
use tonic::transport::server::Router;

use crate::LatticeServiceError;
use crate::actor::RegisteredActor;

pub struct ServiceContext {
    service_kind: ServiceKind,
}

impl ServiceContext {
    pub fn service_kind(&self) -> &ServiceKind {
        &self.service_kind
    }
}

pub struct ServiceBuildContext {
    service_kind: ServiceKind,
    pub(crate) actors: HashMap<ActorKind, Box<dyn Any + Send>>,
    pub(crate) router: Option<Router>,
}

impl ServiceBuildContext {
    pub(crate) fn new(service_kind: ServiceKind) -> Self {
        Self {
            service_kind,
            actors: HashMap::new(),
            router: None,
        }
    }

    pub fn service_context(&self) -> ServiceContext {
        ServiceContext {
            service_kind: self.service_kind.clone(),
        }
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
