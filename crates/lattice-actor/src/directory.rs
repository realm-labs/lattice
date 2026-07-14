use std::any::Any;

use dashmap::DashMap;
use lattice_core::actor_ref::{ActorPath, ActorRef, ProtocolTag};
use thiserror::Error;

use crate::handle::ActorHandle;
use crate::traits::{Actor, ActorLifecycleState};

struct DirectoryEntry {
    reference: ActorRef,
    handle: Box<dyn Any + Send + Sync>,
}

pub struct ActivationDirectory {
    maximum: usize,
    entries: DashMap<ActorPath, DirectoryEntry>,
}

impl ActivationDirectory {
    pub fn new(maximum: usize) -> Result<Self, ActivationDirectoryError> {
        if maximum == 0 {
            return Err(ActivationDirectoryError::ZeroLimit);
        }
        Ok(Self {
            maximum,
            entries: DashMap::new(),
        })
    }

    pub fn register<A: Actor>(
        &self,
        handle: &ActorHandle<A>,
    ) -> Result<(), ActivationDirectoryError> {
        let Some(reference) = handle.actor_ref() else {
            return Ok(());
        };
        if self.entries.len() == self.maximum && !self.entries.contains_key(reference.actor_path())
        {
            return Err(ActivationDirectoryError::Capacity);
        }
        self.entries.insert(
            reference.actor_path().clone(),
            DirectoryEntry {
                reference: reference.erase(),
                handle: Box::new(handle.clone()),
            },
        );
        Ok(())
    }

    pub fn resolve<A: Actor, P: ProtocolTag>(
        &self,
        reference: &ActorRef<P>,
    ) -> Option<ActorHandle<A>> {
        let entry = self.entries.get(reference.actor_path())?;
        if !entry.reference.same_activation(&reference.erase()) {
            return None;
        }
        let handle = entry.handle.downcast_ref::<ActorHandle<A>>()?;
        if matches!(
            handle.lifecycle_state(),
            ActorLifecycleState::Stopped | ActorLifecycleState::StopFailed
        ) {
            return None;
        }
        Some(handle.clone())
    }

    pub fn remove(&self, reference: &ActorRef) -> bool {
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
