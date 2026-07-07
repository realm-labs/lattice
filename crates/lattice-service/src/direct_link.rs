use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use lattice_actor::traits::{Actor, Handler};
use lattice_core::actor_ref::ActorRef;
use lattice_core::direct_link::errors::LinkError;
use lattice_core::direct_link::ids::LinkId;
use lattice_core::direct_link::messages::{
    LinkBackpressure, LinkClosed, LinkDirectionClosed, LinkOpened,
};
use lattice_core::direct_link::options::LinkCloseReason;
use lattice_core::direct_link::runtime::{
    DirectLinkLifecycleRuntime, DirectLinkOpenRequest, DirectLinkRuntime, DirectLinkSession,
};
use lattice_core::direct_link::stream::DirectLinkMetadata;
use lattice_core::direct_link::target::{DirectLinkEndpoint, LinkTarget};
use lattice_direct_link::delivery::DirectLinkDispatch;
use lattice_direct_link::endpoint_pool::{
    DirectLinkEndpointPool, DirectLinkEndpointPoolLifecycle, PooledDirectLinkEndpointPool,
};
use lattice_direct_link::inbound::{DirectLinkInboundRouter, DirectLinkInboundRouterBuilder};
use lattice_direct_link::session::{DirectLinkSessionManager, OpenLinkValidationPolicy};
use lattice_direct_link::stream::DirectLinkActorBinding;
use lattice_direct_link::transport::TcpDirectLinkTransport;
use lattice_placement::instance::InstanceState;
use lattice_placement::store::{ActorPlacementKey, PlacementState};

use crate::context::ServiceBuildContext;
use crate::error::LatticeServiceError;
use crate::framework::{DynPlacementStore, PlacementStoreComponent};

#[derive(Clone)]
pub struct DirectLinkServiceRuntime {
    session_manager: Arc<DirectLinkSessionManager>,
    inbound_router: Arc<DirectLinkInboundRouter>,
    endpoint_pool: PooledDirectLinkEndpointPool<TcpDirectLinkTransport>,
    placement_store: Option<Arc<dyn DynPlacementStore>>,
}

impl fmt::Debug for DirectLinkServiceRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DirectLinkServiceRuntime")
            .field("session_manager", &self.session_manager)
            .field("inbound_router", &self.inbound_router)
            .field("endpoint_pool", &self.endpoint_pool)
            .field("has_placement_store", &self.placement_store.is_some())
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
        stream: lattice_core::direct_link::stream::DirectLinkStreamDescriptor,
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
        let request = self.resolve_open_request(request).await?;
        Ok(self.endpoint_pool.open_link(request).await?.session)
    }

    async fn get_outbound(
        &self,
        link_id: LinkId,
        stream: lattice_core::direct_link::stream::DirectLinkStreamDescriptor,
    ) -> Result<DirectLinkSession, LinkError> {
        self.inbound_router.outbound_session(link_id, stream)
    }

    async fn close_all(&self, link_id: LinkId, reason: LinkCloseReason) -> Result<(), LinkError> {
        self.inbound_router
            .close_all(&link_id, reason)
            .map_err(|error| LinkError::Protocol(error.to_string()))
    }
}

impl DirectLinkServiceRuntime {
    async fn resolve_open_request(
        &self,
        mut request: DirectLinkOpenRequest,
    ) -> Result<DirectLinkOpenRequest, LinkError> {
        let LinkTarget::Actor(actor_ref) = &request.target else {
            return Ok(request);
        };
        let placement_store = self.placement_store.as_ref().ok_or_else(|| {
            LinkError::Protocol(
                "direct link ActorRef target resolution requires a placement store".to_string(),
            )
        })?;
        let key = ActorPlacementKey {
            service_kind: actor_ref.service_kind.clone(),
            actor_kind: actor_ref.actor_kind.clone(),
            actor_id: actor_ref.actor_id.clone(),
        };
        let Some((_version, placement)) = placement_store
            .get_actor(&key)
            .await
            .map_err(|error| LinkError::Protocol(error.to_string()))?
        else {
            return Err(LinkError::ActorUnavailable);
        };
        if placement.state != PlacementState::Running {
            return Err(LinkError::ActorUnavailable);
        }
        let Some(instance) = placement_store
            .get_instance(&placement.owner)
            .await
            .map_err(|error| LinkError::Protocol(error.to_string()))?
        else {
            return Err(LinkError::ActorUnavailable);
        };
        if instance.state != InstanceState::Ready {
            return Err(LinkError::ActorUnavailable);
        }
        let endpoint = instance
            .labels
            .get("direct_link_endpoint")
            .ok_or_else(|| {
                LinkError::Protocol(format!(
                    "instance {} has no direct_link_endpoint label",
                    instance.instance_id
                ))
            })?
            .parse()
            .map_err(|error| {
                LinkError::Protocol(format!(
                    "invalid direct_link_endpoint for instance {}: {error}",
                    instance.instance_id
                ))
            })?;
        let target = ActorRef::direct(
            actor_ref.service_kind.clone(),
            actor_ref.actor_kind.clone(),
            actor_ref.actor_id.clone(),
            instance.instance_id,
            instance.advertised_endpoint,
            Some(placement.epoch),
        );
        request.target = LinkTarget::Endpoint {
            endpoint: DirectLinkEndpoint::new(endpoint),
            target,
        };
        Ok(request)
    }

    pub(crate) async fn close_for_node_drain(&self) -> Result<usize, LinkError> {
        let inbound = self
            .inbound_router
            .close_active_links(LinkCloseReason::NodeDraining)
            .map_err(|error| LinkError::Protocol(error.to_string()))?;
        let outbound = self
            .endpoint_pool
            .close_all_logical_links(LinkCloseReason::NodeDraining)
            .await?;
        Ok(inbound + outbound)
    }
}

#[derive(Debug)]
struct DirectLinkSourceLifecycle {
    inbound_router: Arc<DirectLinkInboundRouter>,
}

impl DirectLinkEndpointPoolLifecycle for DirectLinkSourceLifecycle {
    fn deliver_direction_closed(
        &self,
        actor_ref: &ActorRef,
        event: LinkDirectionClosed,
    ) -> Result<(), LinkError> {
        self.inbound_router
            .deliver_direction_closed_to_actor(actor_ref, event)
            .map_err(|error| LinkError::Protocol(error.to_string()))
    }

    fn deliver_link_closed(
        &self,
        actor_ref: &ActorRef,
        event: LinkClosed,
    ) -> Result<(), LinkError> {
        self.inbound_router
            .deliver_link_closed_to_actor(actor_ref, event)
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

pub(crate) struct DirectLinkBindingRegistration<A, Messages, Metadata = ()>
where
    A: Actor,
{
    binding: DirectLinkActorBinding<A, Messages, Metadata>,
    _actor: PhantomData<fn() -> (A, Metadata)>,
}

impl<A, Messages, Metadata> DirectLinkBindingRegistration<A, Messages, Metadata>
where
    A: Actor,
{
    pub fn new(binding: DirectLinkActorBinding<A, Messages, Metadata>) -> Self {
        Self {
            binding,
            _actor: PhantomData,
        }
    }
}

impl<A, Messages, Metadata> ErasedDirectLinkBinding
    for DirectLinkBindingRegistration<A, Messages, Metadata>
where
    A: Actor + Sync,
    A: Handler<LinkOpened>
        + Handler<LinkDirectionClosed>
        + Handler<LinkClosed>
        + Handler<LinkBackpressure>,
    Metadata: DirectLinkMetadata,
    Messages: DirectLinkDispatch<A, Metadata>,
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
    session_manager.set_validation_policy(OpenLinkValidationPolicy::hosted(
        context.service_context().service_kind().clone(),
    ));
    let mut router = DirectLinkInboundRouter::builder(session_manager.clone());
    for binding in bindings {
        router = binding.register(context, &session_manager, router)?;
    }

    let inbound_router = Arc::new(router.build());
    let source_lifecycle = Arc::new(DirectLinkSourceLifecycle {
        inbound_router: inbound_router.clone(),
    });

    Ok(Some(DirectLinkServiceRuntime {
        session_manager,
        inbound_router,
        endpoint_pool: PooledDirectLinkEndpointPool::new_with_lifecycle(
            TcpDirectLinkTransport::new(),
            Default::default(),
            Some(source_lifecycle),
        ),
        placement_store: context
            .service_context()
            .extension::<PlacementStoreComponent>()
            .map(|component| component.inner()),
    }))
}
