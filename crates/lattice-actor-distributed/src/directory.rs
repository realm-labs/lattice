use std::any::Any;
use std::sync::Mutex;

use dashmap::DashMap;
use lattice_core::actor_address::{ActorAddress, ActorPath, ProtocolTag};
use thiserror::Error;

use lattice_actor::{
    handle::ActorHandle,
    traits::{Actor, ActorLifecycleState},
};

struct DirectoryEntry {
    reference: ActorAddress,
    handle: Box<dyn Any + Send + Sync>,
}

pub struct ActivationDirectory {
    maximum: usize,
    entries: DashMap<ActorPath, DirectoryEntry>,
    mutations: Mutex<()>,
}

impl ActivationDirectory {
    pub fn new(maximum: usize) -> Result<Self, ActivationDirectoryError> {
        if maximum == 0 {
            return Err(ActivationDirectoryError::ZeroLimit);
        }
        Ok(Self {
            maximum,
            entries: DashMap::new(),
            mutations: Mutex::new(()),
        })
    }

    pub fn register<A: Actor>(
        &self,
        reference: &ActorAddress,
        handle: &ActorHandle<A>,
    ) -> Result<(), ActivationDirectoryError> {
        // Serialize capacity changes while retaining independent read lookups.
        // Checking DashMap::len() before insertion is not an atomic reservation.
        let _mutation = self
            .mutations
            .lock()
            .expect("activation directory mutations poisoned");
        if self.entries.len() >= self.maximum && !self.entries.contains_key(reference.actor_path())
        {
            return Err(ActivationDirectoryError::Capacity);
        }
        self.entries.insert(
            reference.actor_path().clone(),
            DirectoryEntry {
                reference: reference.clone(),
                handle: Box::new(handle.clone()),
            },
        );
        Ok(())
    }

    pub fn resolve<A: Actor, P: ProtocolTag>(
        &self,
        reference: &ActorAddress<P>,
    ) -> Option<ActorHandle<A>> {
        let entry = self.entries.get(reference.actor_path())?;
        if !entry.reference.same_activation(&reference.erase()) {
            return None;
        }
        let handle = entry.handle.downcast_ref::<ActorHandle<A>>()?;
        if !matches!(
            handle.lifecycle_state(),
            ActorLifecycleState::Starting | ActorLifecycleState::Running
        ) {
            return None;
        }
        Some(handle.clone())
    }

    pub fn remove(&self, reference: &ActorAddress) -> bool {
        let _mutation = self
            .mutations
            .lock()
            .expect("activation directory mutations poisoned");
        self.entries
            .remove_if(reference.actor_path(), |_, entry| {
                entry.reference.same_activation(reference)
            })
            .is_some()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ActivationDirectoryError {
    #[error("activation directory limit must be nonzero")]
    ZeroLimit,
    #[error("activation directory capacity reached")]
    Capacity,
}
