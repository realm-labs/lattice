use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use lattice_core::{ActorId, ActorKind};
use tokio::sync::{Mutex, Semaphore, watch};

use crate::{Actor, ActorActivationError, ActorError, ActorHandle, MailboxConfig, spawn_actor};

#[derive(Debug, Clone, Copy)]
pub struct ActorRegistryConfig {
    pub mailbox: MailboxConfig,
    pub waiter_capacity: usize,
    pub waiter_timeout: Duration,
}

impl Default for ActorRegistryConfig {
    fn default() -> Self {
        Self {
            mailbox: MailboxConfig::default(),
            waiter_capacity: 1024,
            waiter_timeout: Duration::from_secs(5),
        }
    }
}

pub struct ActorRegistry<A: Actor> {
    kind: ActorKind,
    config: ActorRegistryConfig,
    entries: Mutex<HashMap<ActorId, RegistryEntry<A>>>,
}

impl<A: Actor> ActorRegistry<A> {
    pub fn new(kind: ActorKind, config: ActorRegistryConfig) -> Self {
        Self {
            kind,
            config,
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub fn kind(&self) -> &ActorKind {
        &self.kind
    }

    pub async fn get(&self, actor_id: &ActorId) -> Option<ActorHandle<A>> {
        match self.entries.lock().await.get(actor_id) {
            Some(RegistryEntry::Running(handle)) => Some(handle.clone()),
            Some(RegistryEntry::Activating(_)) | None => None,
        }
    }

    pub async fn start(
        &self,
        actor_id: ActorId,
        actor: A,
    ) -> Result<ActorHandle<A>, ActorActivationError> {
        let mut entries = self.entries.lock().await;
        if entries.contains_key(&actor_id) {
            return Err(ActorActivationError::AlreadyExists);
        }

        let handle = spawn_actor(actor, self.config.mailbox);
        entries.insert(actor_id, RegistryEntry::Running(handle.clone()));
        Ok(handle)
    }

    pub async fn get_or_activate<F, Fut>(
        &self,
        actor_id: ActorId,
        activate: F,
    ) -> Result<ActorHandle<A>, ActorActivationError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<A, ActorError>>,
    {
        let lookup = {
            let mut entries = self.entries.lock().await;
            match entries.get(&actor_id) {
                Some(RegistryEntry::Running(handle)) => return Ok(handle.clone()),
                Some(RegistryEntry::Activating(activation)) => {
                    RegistryLookup::Wait(activation.clone())
                }
                None => {
                    let activation = ActivationState::new(self.config.waiter_capacity);
                    entries.insert(
                        actor_id.clone(),
                        RegistryEntry::Activating(activation.clone()),
                    );
                    RegistryLookup::Activate(activation)
                }
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
                let handle = spawn_actor(actor, self.config.mailbox);
                let mut entries = self.entries.lock().await;
                entries.insert(actor_id, RegistryEntry::Running(handle.clone()));
                Ok(handle)
            }
            Err(error) => {
                let mut entries = self.entries.lock().await;
                entries.remove(&actor_id);
                Err(ActorActivationError::ActivationFailed(error))
            }
        };

        activation.publish(result.clone());
        result
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
