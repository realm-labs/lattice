use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use lattice_actor::{Actor, Handler};
use lattice_core::{
    ActorRef, DirectLinkLifecycleRuntime, DirectLinkOpenRequest, DirectLinkRuntime,
    DirectLinkSession, LinkBackpressure, LinkCloseReason, LinkClosed, LinkDirectionClosed,
    LinkError, LinkId, LinkOpened,
};
use lattice_direct_link::{
    DirectLinkActorBinding, DirectLinkDispatch, DirectLinkEndpointPool, DirectLinkInboundRouter,
    DirectLinkInboundRouterBuilder, DirectLinkSessionManager, PooledDirectLinkEndpointPool,
    TcpDirectLinkTransport,
};

use crate::LatticeServiceError;
use crate::context::ServiceBuildContext;

#[derive(Clone)]
pub struct DirectLinkServiceRuntime {
    session_manager: Arc<DirectLinkSessionManager>,
    inbound_router: Arc<DirectLinkInboundRouter>,
    endpoint_pool: PooledDirectLinkEndpointPool<TcpDirectLinkTransport>,
}

impl fmt::Debug for DirectLinkServiceRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DirectLinkServiceRuntime")
            .field("session_manager", &self.session_manager)
            .field("inbound_router", &self.inbound_router)
            .field("endpoint_pool", &self.endpoint_pool)
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

#[derive(Debug, Default)]
pub(crate) struct DeferredDirectLinkRuntime {
    runtime: Mutex<Option<DirectLinkServiceRuntime>>,
}

impl DeferredDirectLinkRuntime {
    pub(crate) fn set_runtime(&self, runtime: DirectLinkServiceRuntime) {
        *self
            .runtime
            .lock()
            .expect("deferred direct-link runtime poisoned") = Some(runtime);
    }
}

#[async_trait]
impl DirectLinkRuntime for DeferredDirectLinkRuntime {
    async fn open_link(
        &self,
        request: DirectLinkOpenRequest,
    ) -> Result<DirectLinkSession, LinkError> {
        let runtime = self
            .runtime
            .lock()
            .expect("deferred direct-link runtime poisoned")
            .clone()
            .ok_or(LinkError::Unavailable)?;
        runtime.open_link(request).await
    }

    async fn get_outbound(
        &self,
        link_id: LinkId,
        stream: lattice_core::DirectLinkStreamDescriptor,
    ) -> Result<DirectLinkSession, LinkError> {
        let runtime = self
            .runtime
            .lock()
            .expect("deferred direct-link runtime poisoned")
            .clone()
            .ok_or(LinkError::Unavailable)?;
        runtime.get_outbound(link_id, stream).await
    }

    async fn close_all(&self, link_id: LinkId, reason: LinkCloseReason) -> Result<(), LinkError> {
        let runtime = self
            .runtime
            .lock()
            .expect("deferred direct-link runtime poisoned")
            .clone()
            .ok_or(LinkError::Unavailable)?;
        runtime.close_all(link_id, reason).await
    }
}

#[derive(Debug, Default)]
pub(crate) struct DeferredDirectLinkLifecycleRuntime {
    runtime: Mutex<Option<DirectLinkServiceRuntime>>,
}

impl DeferredDirectLinkLifecycleRuntime {
    pub(crate) fn set_runtime(&self, runtime: DirectLinkServiceRuntime) {
        *self
            .runtime
            .lock()
            .expect("deferred direct-link lifecycle runtime poisoned") = Some(runtime);
    }
}

#[async_trait]
impl DirectLinkLifecycleRuntime for DeferredDirectLinkLifecycleRuntime {
    async fn close_for_actor(
        &self,
        actor: ActorRef,
        reason: LinkCloseReason,
    ) -> Result<usize, LinkError> {
        let runtime = self
            .runtime
            .lock()
            .expect("deferred direct-link lifecycle runtime poisoned")
            .clone();
        let Some(runtime) = runtime else {
            return Ok(0);
        };
        runtime.close_for_actor(actor, reason).await
    }
}

#[async_trait]
impl DirectLinkLifecycleRuntime for DirectLinkServiceRuntime {
    async fn close_for_actor(
        &self,
        actor: ActorRef,
        reason: LinkCloseReason,
    ) -> Result<usize, LinkError> {
        self.inbound_router
            .close_active_links_for_actor(&actor.actor_kind, &actor.actor_id, reason)
            .map_err(|error| LinkError::Protocol(error.to_string()))
    }
}

#[async_trait]
impl DirectLinkRuntime for DirectLinkServiceRuntime {
    async fn open_link(
        &self,
        request: DirectLinkOpenRequest,
    ) -> Result<DirectLinkSession, LinkError> {
        Ok(self.endpoint_pool.open_link(request).await?.session)
    }

    async fn get_outbound(
        &self,
        _link_id: LinkId,
        _stream: lattice_core::DirectLinkStreamDescriptor,
    ) -> Result<DirectLinkSession, LinkError> {
        Err(LinkError::Unavailable)
    }

    async fn close_all(&self, link_id: LinkId, reason: LinkCloseReason) -> Result<(), LinkError> {
        self.inbound_router
            .close_all(&link_id, reason)
            .map_err(|error| LinkError::Protocol(error.to_string()))
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
    enable_outbound: bool,
) -> Result<Option<DirectLinkServiceRuntime>, LatticeServiceError> {
    if bindings.is_empty() && !enable_outbound {
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
        endpoint_pool: PooledDirectLinkEndpointPool::new(
            TcpDirectLinkTransport::new(),
            Default::default(),
        ),
    }))
}
