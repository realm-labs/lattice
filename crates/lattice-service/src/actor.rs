use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use lattice_actor::error::ActorError;
use lattice_actor::mailbox::MailboxConfig;
use lattice_actor::registry::{
    ActorCreateContext, ActorFactory, ActorLoader, ActorRegistry, ActorRegistryConfig,
};
use lattice_actor::runtime::PassivationPolicy;
use lattice_actor::traits::Actor;
use lattice_core::ActorKind;

use crate::LatticeServiceError;
use crate::context::ServiceBuildContext;

type ActorCreateFuture<A> = Pin<Box<dyn Future<Output = Result<A, ActorError>> + Send>>;
type ActorCreateFn<A> = dyn Fn(ActorCreateContext) -> ActorCreateFuture<A> + Send + Sync;

pub struct ActorRegistration<A>
where
    A: Actor,
{
    pub(crate) actor_kind: ActorKind,
    pub(crate) config: ActorRegistryConfig,
    pub(crate) loader: ServiceActorLoader<A>,
}

impl<A> ActorRegistration<A>
where
    A: Actor,
{
    pub fn builder(actor_kind: ActorKind) -> ActorRegistrationBuilder<A, NoFactory> {
        ActorRegistrationBuilder {
            actor_kind,
            config: ActorRegistryConfig::default(),
            factory: NoFactory,
            _actor: PhantomData,
        }
    }
}

pub struct ActorRegistrationBuilder<A, F>
where
    A: Actor,
{
    actor_kind: ActorKind,
    config: ActorRegistryConfig,
    factory: F,
    _actor: PhantomData<fn() -> A>,
}

impl<A, F> ActorRegistrationBuilder<A, F>
where
    A: Actor,
{
    pub fn mailbox(mut self, mailbox: MailboxConfig) -> Self {
        self.config.mailbox = mailbox;
        self
    }

    pub fn passivation(mut self, passivation: PassivationPolicy) -> Self {
        self.config.passivation = passivation;
        self
    }

    pub fn registry_config(mut self, config: ActorRegistryConfig) -> Self {
        self.config = config;
        self
    }

    pub fn factory<N>(self, factory: N) -> ActorRegistrationBuilder<A, N>
    where
        N: ActorFactory<A>,
    {
        ActorRegistrationBuilder {
            actor_kind: self.actor_kind,
            config: self.config,
            factory,
            _actor: PhantomData,
        }
    }
}

impl<A, F> ActorRegistrationBuilder<A, F>
where
    A: Actor,
    F: ActorFactory<A>,
{
    pub fn build(self) -> ActorRegistration<A> {
        ActorRegistration {
            actor_kind: self.actor_kind,
            config: self.config,
            loader: ServiceActorLoader::from_factory(self.factory),
        }
    }
}

pub struct NoFactory;

pub struct ServiceActorLoader<A>
where
    A: Actor,
{
    create: Arc<ActorCreateFn<A>>,
}

impl<A> Clone for ServiceActorLoader<A>
where
    A: Actor,
{
    fn clone(&self) -> Self {
        Self {
            create: self.create.clone(),
        }
    }
}

impl<A> ServiceActorLoader<A>
where
    A: Actor,
{
    pub fn from_factory<F>(factory: F) -> Self
    where
        F: ActorFactory<A>,
    {
        Self {
            create: Arc::new(move |ctx| {
                let factory = factory.clone();
                Box::pin(async move { factory.create(ctx).await })
            }),
        }
    }
}

#[async_trait]
impl<A> ActorLoader<A> for ServiceActorLoader<A>
where
    A: Actor,
{
    async fn load(&self, ctx: ActorCreateContext) -> Result<A, ActorError> {
        (self.create)(ctx).await
    }
}

pub struct RegisteredActor<A>
where
    A: Actor,
{
    registry: Arc<ActorRegistry<A>>,
    loader: ServiceActorLoader<A>,
}

impl<A> Clone for RegisteredActor<A>
where
    A: Actor,
{
    fn clone(&self) -> Self {
        Self {
            registry: self.registry.clone(),
            loader: self.loader.clone(),
        }
    }
}

impl<A> RegisteredActor<A>
where
    A: Actor,
{
    pub fn registry(&self) -> Arc<ActorRegistry<A>> {
        self.registry.clone()
    }

    pub fn loader(&self) -> ServiceActorLoader<A> {
        self.loader.clone()
    }
}

pub(crate) trait ErasedActorRegistration: Send + Sync {
    fn actor_kind(&self) -> &ActorKind;
    fn register(
        self: Box<Self>,
        context: &mut ServiceBuildContext,
    ) -> Result<(), LatticeServiceError>;
}

impl<A> ErasedActorRegistration for ActorRegistration<A>
where
    A: Actor + Sync,
{
    fn actor_kind(&self) -> &ActorKind {
        &self.actor_kind
    }

    fn register(
        self: Box<Self>,
        context: &mut ServiceBuildContext,
    ) -> Result<(), LatticeServiceError> {
        let registry = Arc::new(ActorRegistry::new(self.actor_kind.clone(), self.config));
        let registered = RegisteredActor {
            registry,
            loader: self.loader,
        };
        context
            .actors
            .insert(self.actor_kind.clone(), Box::new(registered));
        Ok(())
    }
}
