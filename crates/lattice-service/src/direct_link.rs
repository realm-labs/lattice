use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use lattice_actor::{Actor, Handler};
use lattice_core::{LinkBackpressure, LinkClosed, LinkDirectionClosed, LinkOpened};
use lattice_direct_link::{
    DirectLinkActorBinding, DirectLinkDispatch, DirectLinkInboundRouter,
    DirectLinkInboundRouterBuilder, DirectLinkSessionManager,
};

use crate::LatticeServiceError;
use crate::context::ServiceBuildContext;

#[derive(Clone)]
pub struct DirectLinkServiceRuntime {
    session_manager: Arc<DirectLinkSessionManager>,
    inbound_router: Arc<DirectLinkInboundRouter>,
}

impl fmt::Debug for DirectLinkServiceRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DirectLinkServiceRuntime")
            .field("session_manager", &self.session_manager)
            .field("inbound_router", &self.inbound_router)
            .finish()
    }
}

impl DirectLinkServiceRuntime {
    pub fn session_manager(&self) -> Arc<DirectLinkSessionManager> {
        self.session_manager.clone()
    }

    pub(crate) fn inbound_router(&self) -> Arc<DirectLinkInboundRouter> {
        self.inbound_router.clone()
    }
}

pub(crate) trait ErasedDirectLinkBinding: Send + Sync + 'static {
    fn register(
        self: Box<Self>,
        context: &ServiceBuildContext,
        session_manager: &DirectLinkSessionManager,
        router: DirectLinkInboundRouterBuilder,
    ) -> Result<DirectLinkInboundRouterBuilder, LatticeServiceError>;
}

pub(crate) struct DirectLinkBindingRegistration<A, Messages>
where
    A: Actor,
{
    binding: DirectLinkActorBinding<A, Messages>,
    _actor: PhantomData<fn() -> A>,
}

impl<A, Messages> DirectLinkBindingRegistration<A, Messages>
where
    A: Actor,
{
    pub fn new(binding: DirectLinkActorBinding<A, Messages>) -> Self {
        Self {
            binding,
            _actor: PhantomData,
        }
    }
}

impl<A, Messages> ErasedDirectLinkBinding for DirectLinkBindingRegistration<A, Messages>
where
    A: Actor + Sync,
    A: Handler<LinkOpened>
        + Handler<LinkDirectionClosed>
        + Handler<LinkClosed>
        + Handler<LinkBackpressure>,
    Messages: DirectLinkDispatch<A>,
{
    fn register(
        self: Box<Self>,
        context: &ServiceBuildContext,
        session_manager: &DirectLinkSessionManager,
        router: DirectLinkInboundRouterBuilder,
    ) -> Result<DirectLinkInboundRouterBuilder, LatticeServiceError> {
        let actor_kind = self.binding.actor_kind().clone();
        session_manager
            .register_binding(actor_kind, self.binding.stream().clone())
            .map_err(|error| LatticeServiceError::ComponentBuild {
                slot: "direct_links".to_string(),
                message: error.to_string(),
            })?;

        let registered = context.actor::<A>(self.binding.actor_kind())?;
        let registry = registered.registry();
        Ok(router.bind_actor(self.binding, move |actor_ref| {
            registry.get_running(&actor_ref.actor_id)
        }))
    }
}

pub(crate) fn build_direct_link_runtime(
    bindings: Vec<Box<dyn ErasedDirectLinkBinding>>,
    context: &ServiceBuildContext,
) -> Result<Option<DirectLinkServiceRuntime>, LatticeServiceError> {
    if bindings.is_empty() {
        return Ok(None);
    }

    let session_manager = Arc::new(DirectLinkSessionManager::new());
    let mut router = DirectLinkInboundRouter::builder(session_manager.clone());
    for binding in bindings {
        router = binding.register(context, &session_manager, router)?;
    }

    Ok(Some(DirectLinkServiceRuntime {
        session_manager,
        inbound_router: Arc::new(router.build()),
    }))
}
