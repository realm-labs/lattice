use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use lattice_actor::error::ActorActivationError;
use lattice_actor::mailbox::MailboxConfig;
use lattice_actor::registry::{
    ActorCreateContext, ActorFactory, ActorLoader, ActorRegistry, ActorRegistryConfig,
};
use lattice_actor::runtime::{PassivationPolicy, ShardMigrationPolicy};
use lattice_actor::traits::{Actor, PassivationReason};
use lattice_core::{ActorId, ActorKind, RouteKey};
use lattice_placement::vshard::{VirtualShardId, VirtualShardMapper};

use crate::LatticeServiceError;
use crate::context::ServiceBuildContext;

type ActorCreateFuture<A> = Pin<Box<dyn Future<Output = Result<A, <A as Actor>::Error>> + Send>>;
type ActorCreateFn<A> = dyn Fn(ActorCreateContext) -> ActorCreateFuture<A> + Send + Sync;

#[derive(Debug)]
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

#[derive(Debug)]
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

    pub fn shard_migration(mut self, policy: ShardMigrationPolicy) -> Self {
        self.config.shard_migration = policy;
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

#[derive(Debug)]
pub struct NoFactory;

pub struct ServiceActorLoader<A>
where
    A: Actor,
{
    create: Arc<ActorCreateFn<A>>,
}

impl<A> fmt::Debug for ServiceActorLoader<A>
where
    A: Actor,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServiceActorLoader")
            .finish_non_exhaustive()
    }
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
    async fn load(&self, ctx: ActorCreateContext) -> Result<A, A::Error> {
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

impl<A> fmt::Debug for RegisteredActor<A>
where
    A: Actor,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredActor")
            .field("actor_kind", self.registry.kind())
            .finish_non_exhaustive()
    }
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

#[async_trait]
pub(crate) trait ErasedLogicActor: Send + Sync + fmt::Debug {
    async fn activate(&self, actor_id: ActorId) -> Result<(), ActorActivationError>;
    async fn drain(&self) -> usize;
    async fn prepare_virtual_shard_migration(
        &self,
        shard_id: VirtualShardId,
        shard_count: u32,
    ) -> VirtualShardPreparation;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VirtualShardPreparation {
    pub eligible: bool,
    pub running_actors: usize,
    pub passivated_actors: usize,
}

#[async_trait]
impl<A> ErasedLogicActor for RegisteredActor<A>
where
    A: Actor + Sync,
{
    async fn activate(&self, actor_id: ActorId) -> Result<(), ActorActivationError> {
        self.registry
            .get_or_load(actor_id, self.loader.clone())
            .await?;
        Ok(())
    }

    async fn drain(&self) -> usize {
        self.registry.drain().await
    }

    async fn prepare_virtual_shard_migration(
        &self,
        shard_id: VirtualShardId,
        shard_count: u32,
    ) -> VirtualShardPreparation {
        let mapper = match VirtualShardMapper::new(shard_count) {
            Ok(mapper) => mapper,
            Err(_) => {
                return VirtualShardPreparation {
                    eligible: false,
                    running_actors: 0,
                    passivated_actors: 0,
                };
            }
        };
        let running_actor_ids = self
            .registry
            .running_actor_ids()
            .into_iter()
            .filter(|actor_id| {
                mapper.shard_for_route_key(&route_key_from_actor_id(actor_id)) == shard_id
            })
            .collect::<Vec<_>>();
        let running_actors = running_actor_ids.len();
        if running_actors == 0 {
            return VirtualShardPreparation {
                eligible: true,
                running_actors,
                passivated_actors: 0,
            };
        }

        match self.registry.shard_migration_policy() {
            ShardMigrationPolicy::BlockRunningActors => VirtualShardPreparation {
                eligible: false,
                running_actors,
                passivated_actors: 0,
            },
            ShardMigrationPolicy::PassivateRunningActors => {
                let passivated = self
                    .registry
                    .passivate_actor_ids(running_actor_ids, PassivationReason::Migrate)
                    .await;
                VirtualShardPreparation {
                    eligible: true,
                    running_actors,
                    passivated_actors: passivated,
                }
            }
        }
    }
}

fn route_key_from_actor_id(actor_id: &ActorId) -> RouteKey {
    match actor_id {
        ActorId::Str(value) => RouteKey::Str(value.clone()),
        ActorId::U64(value) => RouteKey::U64(*value),
        ActorId::I64(value) => RouteKey::I64(*value),
        ActorId::Bytes(value) => RouteKey::Bytes(value.clone()),
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
        let mut config = self.config;
        config.service = context.service_context();
        let registry = Arc::new(ActorRegistry::new(self.actor_kind.clone(), config));
        let registered = RegisteredActor {
            registry,
            loader: self.loader,
        };
        context
            .logic_actors
            .insert(self.actor_kind.clone(), Arc::new(registered.clone()));
        context
            .actors
            .insert(self.actor_kind.clone(), Box::new(registered));
        Ok(())
    }
}
