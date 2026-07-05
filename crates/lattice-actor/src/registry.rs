use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use http::Uri;
use lattice_core::{ActorId, ActorKind, ActorRef, Epoch, InstanceId, ServiceContext, ServiceKind};
use tokio::sync::{Semaphore, watch};

use crate::error::{ActorActivationError, ActorError};
use crate::handle::ActorHandle;
use crate::mailbox::MailboxConfig;
use crate::runtime::{PassivationPolicy, spawn_actor_with_self_ref};
use crate::traits::Actor;

#[derive(Debug, Clone)]
pub struct ActorRegistryConfig {
    pub mailbox: MailboxConfig,
    pub passivation: PassivationPolicy,
    pub waiter_capacity: usize,
    pub waiter_timeout: Duration,
    pub actor_ref: Option<ActorRefConfig>,
    pub service: ServiceContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorRefConfig {
    pub service_kind: ServiceKind,
    pub instance_id: InstanceId,
    pub endpoint: Uri,
    pub owner_epoch: Option<Epoch>,
}

impl Default for ActorRegistryConfig {
    fn default() -> Self {
        Self {
            mailbox: MailboxConfig::default(),
            passivation: PassivationPolicy::Disabled,
            waiter_capacity: 1024,
            waiter_timeout: Duration::from_secs(5),
            actor_ref: None,
            service: ServiceContext::empty(),
        }
    }
}

pub struct ActorRegistry<A: Actor> {
    kind: ActorKind,
    config: ActorRegistryConfig,
    entries: DashMap<ActorId, RegistryEntry<A>>,
}

impl<A: Actor> fmt::Debug for ActorRegistry<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorRegistry")
            .field("kind", &self.kind)
            .field("config", &self.config)
            .field("entry_count", &self.entries.len())
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct ActorCreateContext {
    pub actor_kind: ActorKind,
    pub actor_id: ActorId,
    pub service: ServiceContext,
}

#[async_trait]
pub trait ActorFactory<A>: Clone + Send + Sync + 'static
where
    A: Actor,
{
    async fn create(&self, ctx: ActorCreateContext) -> Result<A, A::Error>;
}

#[async_trait]
pub trait ActorLoader<A>: Clone + Send + Sync + 'static
where
    A: Actor,
{
    async fn load(&self, ctx: ActorCreateContext) -> Result<A, A::Error>;
}

impl<A: Actor> ActorRegistry<A> {
    pub fn new(kind: ActorKind, config: ActorRegistryConfig) -> Self {
        Self {
            kind,
            config,
            entries: DashMap::new(),
        }
    }

    pub fn kind(&self) -> &ActorKind {
        &self.kind
    }

    pub async fn get(&self, actor_id: &ActorId) -> Option<ActorHandle<A>> {
        match self.entries.get(actor_id).as_deref() {
            Some(RegistryEntry::Running(handle)) => Some(handle.clone()),
            Some(RegistryEntry::Activating(_)) | None => None,
        }
    }

    pub async fn remove(&self, actor_id: &ActorId) -> Option<ActorHandle<A>> {
        match self.entries.remove(actor_id).map(|(_, entry)| entry) {
            Some(RegistryEntry::Running(handle)) => Some(handle),
            Some(RegistryEntry::Activating(activation)) => {
                activation.publish(Err(ActorActivationError::ActivationFailed(
                    ActorError::new("actor registry entry removed during activation"),
                )));
                None
            }
            None => None,
        }
    }

    pub async fn start(
        &self,
        actor_id: ActorId,
        actor: A,
    ) -> Result<ActorHandle<A>, ActorActivationError> {
        match self.entries.entry(actor_id.clone()) {
            Entry::Occupied(_) => Err(ActorActivationError::AlreadyExists),
            Entry::Vacant(entry) => {
                let handle = self.spawn_actor(actor_id, actor);
                entry.insert(RegistryEntry::Running(handle.clone()));
                Ok(handle)
            }
        }
    }

    pub async fn get_or_activate<F, Fut>(
        &self,
        actor_id: ActorId,
        activate: F,
    ) -> Result<ActorHandle<A>, ActorActivationError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<A, A::Error>>,
    {
        let lookup = match self.entries.entry(actor_id.clone()) {
            Entry::Occupied(entry) => match entry.get() {
                RegistryEntry::Running(handle) => return Ok(handle.clone()),
                RegistryEntry::Activating(activation) => RegistryLookup::Wait(activation.clone()),
            },
            Entry::Vacant(entry) => {
                let activation = ActivationState::new(self.config.waiter_capacity);
                entry.insert(RegistryEntry::Activating(activation.clone()));
                RegistryLookup::Activate(activation)
            }
        };

        let activation = match lookup {
            RegistryLookup::Wait(activation) => {
                return self.wait_for_activation(activation).await;
            }
            RegistryLookup::Activate(activation) => activation,
        };

        let result = match activate().await {
            Ok(actor) => {
                let handle = self.spawn_actor(actor_id.clone(), actor);
                self.entries
                    .insert(actor_id, RegistryEntry::Running(handle.clone()));
                Ok(handle)
            }
            Err(error) => {
                self.entries.remove_if(&actor_id, |_, entry| {
                    matches!(entry, RegistryEntry::Activating(existing) if Arc::ptr_eq(existing, &activation))
                });
                Err(ActorActivationError::ActivationFailed(
                    ActorError::from_error(error),
                ))
            }
        };

        activation.publish(result.clone());
        result
    }

    pub async fn get_or_create<F>(
        &self,
        actor_id: ActorId,
        factory: F,
    ) -> Result<ActorHandle<A>, ActorActivationError>
    where
        F: ActorFactory<A>,
    {
        let ctx = ActorCreateContext {
            actor_kind: self.kind.clone(),
            actor_id: actor_id.clone(),
            service: self.config.service.clone(),
        };
        self.get_or_activate(actor_id, || async move { factory.create(ctx).await })
            .await
    }

    pub async fn get_or_load<L>(
        &self,
        actor_id: ActorId,
        loader: L,
    ) -> Result<ActorHandle<A>, ActorActivationError>
    where
        L: ActorLoader<A>,
    {
        let ctx = ActorCreateContext {
            actor_kind: self.kind.clone(),
            actor_id: actor_id.clone(),
            service: self.config.service.clone(),
        };
        self.get_or_activate(actor_id, || async move { loader.load(ctx).await })
            .await
    }

    async fn wait_for_activation(
        &self,
        activation: Arc<ActivationState<A>>,
    ) -> Result<ActorHandle<A>, ActorActivationError> {
        let permit = activation
            .waiter_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| ActorActivationError::WaiterCapacityExceeded)?;
        let mut result_rx = activation.result_tx.subscribe();

        if let Some(result) = result_rx.borrow().clone() {
            drop(permit);
            return result;
        }

        let wait = async {
            loop {
                result_rx.changed().await.map_err(|_| {
                    ActorActivationError::ActivationFailed(ActorError::new(
                        "activation result channel closed",
                    ))
                })?;
                if let Some(result) = result_rx.borrow().clone() {
                    return result;
                }
            }
        };

        let result = tokio::time::timeout(self.config.waiter_timeout, wait)
            .await
            .map_err(|_| ActorActivationError::WaiterTimeout {
                timeout: self.config.waiter_timeout,
            })?;
        drop(permit);
        result
    }

    fn spawn_actor(&self, actor_id: ActorId, actor: A) -> ActorHandle<A> {
        let self_ref = self.actor_ref_for(actor_id);
        spawn_actor_with_self_ref(
            actor,
            self.config.mailbox,
            self.config.passivation,
            self_ref,
            self.config.service.clone(),
        )
    }

    fn actor_ref_for(&self, actor_id: ActorId) -> Option<ActorRef> {
        let config = self.config.actor_ref.as_ref()?;
        Some(ActorRef::direct(
            config.service_kind.clone(),
            self.kind.clone(),
            actor_id,
            config.instance_id.clone(),
            config.endpoint.clone(),
            config.owner_epoch,
        ))
    }
}

enum RegistryEntry<A: Actor> {
    Running(ActorHandle<A>),
    Activating(Arc<ActivationState<A>>),
}

enum RegistryLookup<A: Actor> {
    Activate(Arc<ActivationState<A>>),
    Wait(Arc<ActivationState<A>>),
}

struct ActivationState<A: Actor> {
    result_tx: watch::Sender<Option<Result<ActorHandle<A>, ActorActivationError>>>,
    waiter_slots: Arc<Semaphore>,
}

impl<A: Actor> ActivationState<A> {
    fn new(waiter_capacity: usize) -> Arc<Self> {
        let (result_tx, _result_rx) = watch::channel(None);
        Arc::new(Self {
            result_tx,
            waiter_slots: Arc::new(Semaphore::new(waiter_capacity)),
        })
    }

    fn publish(&self, result: Result<ActorHandle<A>, ActorActivationError>) {
        self.result_tx.send_replace(Some(result));
    }
}
