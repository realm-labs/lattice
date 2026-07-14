use std::fmt;
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use lattice_core::actor_ref::{
    ActorPath, ActorRef, ClusterId, NodeAddress, NodeIncarnation, ProtocolId,
};
use lattice_core::id::ActorId;
use lattice_core::kind::ActorKind;
use lattice_core::service_context::ServiceContext;
use tokio::sync::{Semaphore, watch};

use crate::error::{ActorActivationError, ActorError};
use crate::handle::ActorHandle;
use crate::mailbox::MailboxConfig;
use crate::recipient::ActorSystem;
use crate::runtime::{PassivationPolicy, ShardMigrationPolicy, spawn_actor_with_self_ref};
use crate::traits::{Actor, ActorLifecycleState, PassivationReason, StopReason};

#[derive(Debug, Clone)]
pub struct ActorRegistryConfig {
    pub mailbox: MailboxConfig,
    pub passivation: PassivationPolicy,
    pub shard_migration: ShardMigrationPolicy,
    pub waiter_capacity: usize,
    pub waiter_timeout: Duration,
    pub actor_ref: Option<ActorRefConfig>,
    pub service: ServiceContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorRefConfig {
    pub cluster_id: ClusterId,
    pub node_address: NodeAddress,
    pub node_incarnation: NodeIncarnation,
    pub protocol_id: ProtocolId,
}

impl Default for ActorRegistryConfig {
    fn default() -> Self {
        Self {
            mailbox: MailboxConfig::default(),
            passivation: PassivationPolicy::Disabled,
            shard_migration: ShardMigrationPolicy::BlockRunningActors,
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
    entries: Arc<DashMap<ActorId, RegistryEntry<A>>>,
    actor_system: Arc<OnceLock<ActorSystem>>,
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
            entries: Arc::new(DashMap::new()),
            actor_system: Arc::new(OnceLock::new()),
        }
    }

    #[doc(hidden)]
    pub fn install_actor_system(&self, actor_system: ActorSystem) -> Result<(), ActorSystem> {
        self.actor_system.set(actor_system)
    }

    pub fn kind(&self) -> &ActorKind {
        &self.kind
    }

    pub fn shard_migration_policy(&self) -> ShardMigrationPolicy {
        self.config.shard_migration
    }

    pub fn running_actor_ids(&self) -> Vec<ActorId> {
        self.entries
            .iter()
            .filter_map(|entry| match entry.value() {
                RegistryEntry::Running(_) => Some(entry.key().clone()),
                RegistryEntry::Activating(_) => None,
            })
            .collect()
    }

    pub async fn get(&self, actor_id: &ActorId) -> Option<ActorHandle<A>> {
        self.get_running(actor_id)
    }

    pub fn get_running(&self, actor_id: &ActorId) -> Option<ActorHandle<A>> {
        match self.entries.get(actor_id).as_deref() {
            Some(RegistryEntry::Running(handle)) if !is_terminal(handle.lifecycle_state()) => {
                Some(handle.clone())
            }
            Some(RegistryEntry::Running(_)) => {
                self.entries.remove(actor_id);
                None
            }
            Some(RegistryEntry::Activating(_)) | None => None,
        }
    }

    pub fn get_exact(&self, actor_ref: &ActorRef) -> Option<ActorHandle<A>> {
        if self.config.actor_ref.as_ref().is_none_or(|config| {
            config.cluster_id != *actor_ref.cluster_id()
                || config.node_address != *actor_ref.node_address()
                || config.node_incarnation != actor_ref.node_incarnation()
                || config.protocol_id != actor_ref.protocol_id()
        }) {
            return None;
        }
        if let Some(directory) = self
            .config
            .service
            .extension::<crate::directory::ActivationDirectory>()
            && let Some(handle) = directory.resolve(actor_ref)
        {
            return Some(handle);
        }
        self.entries.iter().find_map(|entry| match entry.value() {
            RegistryEntry::Running(handle)
                if !is_terminal(handle.lifecycle_state())
                    && handle
                        .actor_ref()
                        .is_some_and(|current| current.same_activation(actor_ref)) =>
            {
                Some(handle.clone())
            }
            RegistryEntry::Running(_) | RegistryEntry::Activating(_) => None,
        })
    }

    pub async fn remove(&self, actor_id: &ActorId) -> Option<ActorHandle<A>> {
        match self.entries.remove(actor_id).map(|(_, entry)| entry) {
            Some(RegistryEntry::Running(handle)) => {
                if let Some(directory) = self
                    .config
                    .service
                    .extension::<crate::directory::ActivationDirectory>()
                    && let Some(reference) = handle.actor_ref()
                {
                    directory.remove(&reference.erase());
                }
                Some(handle)
            }
            Some(RegistryEntry::Activating(activation)) => {
                activation.publish(Err(ActorActivationError::ActivationFailed(
                    ActorError::new("actor registry entry removed during activation"),
                )));
                None
            }
            None => None,
        }
    }

    pub async fn drain(&self) -> usize {
        let actor_ids = self
            .entries
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        let mut drained = 0;
        for actor_id in actor_ids {
            if let Some(handle) = self.remove(&actor_id).await {
                let _ = handle
                    .stop(StopReason::Passivated(PassivationReason::Drain))
                    .await;
                drained += 1;
            }
        }
        drained
    }

    pub async fn passivate_actor_ids<I>(&self, actor_ids: I, reason: PassivationReason) -> usize
    where
        I: IntoIterator<Item = ActorId>,
    {
        let mut passivated = 0;
        for actor_id in actor_ids {
            if let Some(handle) = self.remove(&actor_id).await {
                let _ = handle.stop(StopReason::Passivated(reason)).await;
                passivated += 1;
            }
        }
        passivated
    }

    pub async fn start(
        &self,
        actor_id: ActorId,
        actor: A,
    ) -> Result<ActorHandle<A>, ActorActivationError> {
        self.remove_stopped_running_entry(&actor_id);
        match self.entries.entry(actor_id.clone()) {
            Entry::Occupied(_) => Err(ActorActivationError::AlreadyExists),
            Entry::Vacant(entry) => {
                let handle = self
                    .spawn_actor(actor_id, actor)
                    .map_err(ActorActivationError::ActivationFailed)?;
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
        self.remove_stopped_running_entry(&actor_id);
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
                let still_activating = self.entries.remove_if(&actor_id, |_, entry| {
                    matches!(entry, RegistryEntry::Activating(existing) if Arc::ptr_eq(existing, &activation))
                });
                if still_activating.is_none() {
                    return Err(ActorActivationError::ActivationFailed(ActorError::new(
                        "actor registry entry removed during activation",
                    )));
                }
                match self.spawn_actor(actor_id.clone(), actor) {
                    Ok(handle) => {
                        self.entries
                            .insert(actor_id, RegistryEntry::Running(handle.clone()));
                        Ok(handle)
                    }
                    Err(error) => Err(ActorActivationError::ActivationFailed(error)),
                }
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

    fn remove_stopped_running_entry(&self, actor_id: &ActorId) {
        let removed = self.entries.remove_if(actor_id, |_, entry| {
            matches!(
                entry,
                RegistryEntry::Running(handle)
                    if is_terminal(handle.lifecycle_state())
            )
        });
        if let Some((_, RegistryEntry::Running(handle))) = removed
            && let Some(directory) = self
                .config
                .service
                .extension::<crate::directory::ActivationDirectory>()
            && let Some(reference) = handle.actor_ref()
        {
            directory.remove(&reference.erase());
        }
    }

    fn spawn_actor(&self, actor_id: ActorId, actor: A) -> Result<ActorHandle<A>, ActorError> {
        let self_ref = self
            .actor_ref_for(actor_id.clone())
            .map(|actor_ref| actor_ref.erase());
        let handle = spawn_actor_with_self_ref(
            actor,
            self.config.mailbox,
            self.config.passivation,
            self_ref,
            Some(self.actor_system.clone()),
            self.config.service.clone(),
        );
        if let Some(directory) = self
            .config
            .service
            .extension::<crate::directory::ActivationDirectory>()
            && let Err(error) = directory.register(&handle)
        {
            let _ = handle.try_stop_internal(StopReason::StartFailed);
            return Err(ActorError::new(error.to_string()));
        }
        Ok(handle)
    }

    fn actor_ref_for(&self, actor_id: ActorId) -> Option<ActorRef> {
        let config = self.config.actor_ref.as_ref()?;
        let path = ActorPath::user([
            "user".to_owned(),
            encode_segment(self.kind.as_str().as_bytes()),
            encode_actor_id(&actor_id),
        ])
        .expect("registry-generated actor path is canonical");
        ActorRef::new(
            config.cluster_id.clone(),
            config.node_address.clone(),
            config.node_incarnation,
            path,
            crate::runtime::next_activation_id(config.node_incarnation),
            config.protocol_id,
        )
        .ok()
    }
}

fn encode_actor_id(actor_id: &ActorId) -> String {
    match actor_id {
        ActorId::Str(value) => format!("s-{}", encode_segment(value.as_bytes())),
        ActorId::U64(value) => format!("u-{value}"),
        ActorId::I64(value) => format!("i-{value}"),
        ActorId::Bytes(value) => format!("b-{}", encode_segment(value)),
    }
}

fn encode_segment(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(HEX[usize::from(byte >> 4)] as char);
        output.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    output
}

fn is_terminal(state: ActorLifecycleState) -> bool {
    matches!(
        state,
        ActorLifecycleState::Stopped | ActorLifecycleState::StopFailed
    )
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
