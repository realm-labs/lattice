//! Distributed capabilities exposed through a local Actor context.

use std::sync::{Arc, OnceLock};

use lattice_actor::{context::ActorContext, traits::Actor};
use lattice_model::actor::{ActorAddress, EntityAddress, RecipientAddress, SingletonAddress};

use crate::{
    protocol::Protocol,
    recipient::{ActorSystem, RecipientError},
    reference::{ActorRef, EntityRef, Recipient, SingletonRef},
};

/// Service-scoped access to the distributed Actor system.
#[derive(Clone)]
pub(crate) struct DistributedActorRuntime {
    actor_system: Arc<OnceLock<ActorSystem>>,
}

impl DistributedActorRuntime {
    pub(crate) fn new(actor_system: Arc<OnceLock<ActorSystem>>) -> Self {
        Self { actor_system }
    }
}

/// Exact distributed identity belonging to one Actor activation.
#[derive(Clone)]
pub(crate) struct DistributedActorIdentity {
    self_address: Option<ActorAddress>,
}

impl DistributedActorIdentity {
    pub(crate) fn new(self_address: Option<ActorAddress>) -> Self {
        Self { self_address }
    }
}

#[derive(Clone)]
pub struct DistributedActorContext {
    self_address: Option<ActorAddress>,
    actor_system: Arc<OnceLock<ActorSystem>>,
}

impl DistributedActorContext {
    #[doc(hidden)]
    pub fn new(
        self_address: Option<ActorAddress>,
        actor_system: Arc<OnceLock<ActorSystem>>,
    ) -> Self {
        Self {
            self_address,
            actor_system,
        }
    }

    pub fn self_address(&self) -> Option<&ActorAddress> {
        self.self_address.as_ref()
    }

    pub fn actor_system(&self) -> Result<&ActorSystem, RecipientError> {
        self.actor_system
            .get()
            .ok_or(RecipientError::ActorSystemUnavailable)
    }

    pub fn self_ref<P: Protocol>(&self) -> Result<Option<ActorRef<P>>, RecipientError> {
        let Some(address) = self.self_address.as_ref() else {
            return Ok(None);
        };
        let address = address.try_typed::<P>()?;
        self.actor_system()?.bind_actor(address).map(Some)
    }
}

pub trait DistributedActorContextExt<A: Actor> {
    fn distributed(&self) -> Option<Arc<DistributedActorContext>>;

    fn require_distributed(&self) -> Result<Arc<DistributedActorContext>, RecipientError> {
        self.distributed()
            .ok_or(RecipientError::ActorSystemUnavailable)
    }

    fn bind_actor<P: Protocol>(
        &self,
        address: ActorAddress<P>,
    ) -> Result<ActorRef<P>, RecipientError> {
        self.require_distributed()?
            .actor_system()?
            .bind_actor(address)
    }

    fn bind_entity<P: Protocol>(
        &self,
        address: EntityAddress<P>,
    ) -> Result<EntityRef<P>, RecipientError> {
        self.require_distributed()?
            .actor_system()?
            .bind_entity(address)
    }

    fn bind_singleton<P: Protocol>(
        &self,
        address: SingletonAddress<P>,
    ) -> Result<SingletonRef<P>, RecipientError> {
        self.require_distributed()?
            .actor_system()?
            .bind_singleton(address)
    }

    fn bind<P: Protocol>(
        &self,
        address: RecipientAddress<P>,
    ) -> Result<Recipient<P>, RecipientError> {
        self.require_distributed()?.actor_system()?.bind(address)
    }

    fn self_ref<P: Protocol>(&self) -> Result<Option<ActorRef<P>>, RecipientError> {
        self.require_distributed()?.self_ref::<P>()
    }
}

impl<A: Actor> DistributedActorContextExt<A> for ActorContext<A> {
    fn distributed(&self) -> Option<Arc<DistributedActorContext>> {
        let runtime = self.service().extension::<DistributedActorRuntime>()?;
        let self_address = self
            .runtime_attachment::<DistributedActorIdentity>()
            .and_then(|identity| identity.self_address.clone());
        Some(Arc::new(DistributedActorContext::new(
            self_address,
            runtime.actor_system.clone(),
        )))
    }
}
