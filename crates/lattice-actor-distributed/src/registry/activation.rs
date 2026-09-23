use super::ActorDefinition;
use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};

use crate::ActorId;
use dashmap::{DashMap, mapref::entry::Entry};
use lattice_actor::{
    handle::ActorHandle,
    traits::{Actor, ActorLifecycleState},
};
use tokio::sync::{Semaphore, watch};

use super::{
    ActorActivationError, ActorFencingToken, ActorRegistry, RegistryEntry, RegistryLookup,
    authority_error, is_business_admitted,
};
use crate::entity::EntityActivationState;

pub(super) struct ActivationState<A: Actor> {
    result_tx: watch::Sender<Option<Result<ActorHandle<A>, ActorActivationError>>>,
    pub(super) waiter_slots: Arc<Semaphore>,
    state: AtomicU8,
    pub(super) fencing_token: Option<ActorFencingToken>,
}

pub(super) struct ActivationCleanup<A: Actor> {
    pub(super) entries: Arc<DashMap<ActorId, RegistryEntry<A>>>,
    pub(super) actor_id: ActorId,
    pub(super) activation: Arc<ActivationState<A>>,
}

impl<A: Actor> Drop for ActivationCleanup<A> {
    fn drop(&mut self) {
        self.entries.remove_if(&self.actor_id, |_, entry| {
            matches!(entry, RegistryEntry::Activating(existing) if Arc::ptr_eq(existing, &self.activation))
        });
        self.activation
            .publish(Err(ActorActivationError::Cancelled));
    }
}

impl<A: Actor> ActivationState<A> {
    pub(super) fn new(
        waiter_capacity: usize,
        fencing_token: Option<ActorFencingToken>,
    ) -> Arc<Self> {
        let (result_tx, _result_rx) = watch::channel(None);
        Arc::new(Self {
            result_tx,
            waiter_slots: Arc::new(Semaphore::new(waiter_capacity)),
            state: AtomicU8::new(0),
            fencing_token,
        })
    }

    pub(super) fn publish(&self, result: Result<ActorHandle<A>, ActorActivationError>) {
        self.result_tx.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(result);
            true
        });
    }

    pub(super) async fn result(&self) -> Result<ActorHandle<A>, ActorActivationError> {
        let mut receiver = self.result_tx.subscribe();
        loop {
            if let Some(result) = receiver.borrow_and_update().clone() {
                return result;
            }
            if receiver.changed().await.is_err() {
                return Err(ActorActivationError::Cancelled);
            }
        }
    }

    pub(super) fn set_loading(&self) {
        self.state.store(1, Ordering::Release);
    }

    pub(super) fn state(&self) -> EntityActivationState {
        match self.state.load(Ordering::Acquire) {
            0 => EntityActivationState::Activating,
            _ => EntityActivationState::Loading,
        }
    }
}

impl<D: ActorDefinition, A: Actor> ActorRegistry<D, A> {
    // Called under the resolver's authority lock. A current grant can supersede an old
    // activation even if disconnect/snapshot recovery lost the old session's stop effect.
    // Fence it before admitting the replacement; retain any failed persistence in quarantine.
    pub(super) fn lookup_activation(
        &self,
        actor_id: &ActorId,
        fencing_token: Option<ActorFencingToken>,
    ) -> Result<RegistryLookup<A>, ActorActivationError> {
        loop {
            match self.entries.entry(actor_id.clone()) {
                Entry::Occupied(entry) => {
                    let existing_token = match entry.get() {
                        RegistryEntry::Running(_, token) => *token,
                        RegistryEntry::Activating(activation) => activation.fencing_token,
                    };
                    if existing_token != fencing_token {
                        self.fence_entry(entry).map_err(|error| {
                            ActorActivationError::ActivationFailed(
                                lattice_actor::error::ActorFailure::from_error(error),
                            )
                        })?;
                        continue;
                    }
                    return match entry.get() {
                        RegistryEntry::Running(handle, _) => {
                            if handle.lifecycle_state() == ActorLifecycleState::StopFailed {
                                return Err(ActorActivationError::RetainedStopFailure);
                            }
                            if !is_business_admitted(handle.lifecycle_state())
                                || handle.business_admission_fenced()
                            {
                                return Err(authority_error("actor is stopping"));
                            }
                            Ok(RegistryLookup::Running(handle.clone()))
                        }
                        RegistryEntry::Activating(activation) => {
                            Ok(RegistryLookup::Wait(activation.clone()))
                        }
                    };
                }
                Entry::Vacant(entry) => {
                    let activation =
                        ActivationState::new(self.config.waiter_capacity, fencing_token);
                    entry.insert(RegistryEntry::Activating(activation.clone()));
                    return Ok(RegistryLookup::Activate(activation));
                }
            }
        }
    }

    pub(super) async fn wait_for_activation(
        &self,
        activation: Arc<ActivationState<A>>,
    ) -> Result<ActorHandle<A>, ActorActivationError> {
        let permit = activation
            .waiter_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| ActorActivationError::WaiterCapacityExceeded)?;
        let result = tokio::time::timeout(self.config.waiter_timeout, activation.result())
            .await
            .map_err(|_| ActorActivationError::WaiterTimeout {
                timeout: self.config.waiter_timeout,
            })?;
        drop(permit);
        result
    }

    // The entry lock orders invalidation against loader publication. Removing a placeholder
    // invalidates its producer, even when that caller has not yet been polled to drop its loader.
    // The producer's cleanup guard uses identity, so it cannot remove a later activation.
    pub(super) fn cancel_loading_or_handle(
        &self,
        actor_id: &ActorId,
    ) -> Option<Option<ActorHandle<A>>> {
        let Entry::Occupied(entry) = self.entries.entry(actor_id.clone()) else {
            return None;
        };
        match entry.get() {
            RegistryEntry::Running(handle, _) => Some(Some(handle.clone())),
            RegistryEntry::Activating(activation) => {
                activation.publish(Err(ActorActivationError::Cancelled));
                entry.remove();
                Some(None)
            }
        }
    }
}
