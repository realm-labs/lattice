use std::collections::HashMap;

use lattice_core::{ActorId, ActorKind, Epoch, InstanceId};
use tokio::sync::mpsc;

use crate::PassivationReason;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RemoteActorRef {
    pub actor_kind: ActorKind,
    pub actor_id: ActorId,
    pub owner: InstanceId,
    pub epoch: Epoch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteWatchEvent {
    Stopped {
        target: RemoteActorRef,
    },
    Passivated {
        target: RemoteActorRef,
        reason: PassivationReason,
    },
    Migrated {
        target: RemoteActorRef,
        new_owner: InstanceId,
        new_epoch: Epoch,
    },
    Fenced {
        target: RemoteActorRef,
        current_epoch: Epoch,
    },
    NodeDown {
        target: RemoteActorRef,
    },
}

#[derive(Debug, Default)]
pub struct CrossNodeWatchRegistry {
    watchers: HashMap<RemoteActorRef, Vec<mpsc::UnboundedSender<RemoteWatchEvent>>>,
}

impl CrossNodeWatchRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn watch(&mut self, target: RemoteActorRef) -> mpsc::UnboundedReceiver<RemoteWatchEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.watchers.entry(target).or_default().push(tx);
        rx
    }

    pub fn unwatch(&mut self, target: &RemoteActorRef) -> bool {
        self.watchers.remove(target).is_some()
    }

    pub fn notify(&mut self, event: RemoteWatchEvent) {
        let target = event.target().clone();
        if let Some(watchers) = self.watchers.get_mut(&target) {
            watchers.retain(|watcher| watcher.send(event.clone()).is_ok());
        }
    }

    pub fn watcher_count(&self, target: &RemoteActorRef) -> usize {
        self.watchers.get(target).map_or(0, Vec::len)
    }
}

impl RemoteWatchEvent {
    pub fn target(&self) -> &RemoteActorRef {
        match self {
            Self::Stopped { target }
            | Self::Passivated { target, .. }
            | Self::Migrated { target, .. }
            | Self::Fenced { target, .. }
            | Self::NodeDown { target } => target,
        }
    }
}

#[cfg(test)]
mod tests {
    use lattice_core::{ActorId, Epoch, InstanceId, actor_kind};

    use super::*;

    #[tokio::test]
    async fn remote_watch_delivers_migration_and_fence_notifications_by_epoch() {
        let target = target_ref(Epoch(1));
        let mut registry = CrossNodeWatchRegistry::new();
        let mut rx = registry.watch(target.clone());

        registry.notify(RemoteWatchEvent::Migrated {
            target: target.clone(),
            new_owner: InstanceId::new("world-b"),
            new_epoch: Epoch(2),
        });
        registry.notify(RemoteWatchEvent::Fenced {
            target: target.clone(),
            current_epoch: Epoch(2),
        });

        assert_eq!(
            rx.recv().await.unwrap(),
            RemoteWatchEvent::Migrated {
                target: target.clone(),
                new_owner: InstanceId::new("world-b"),
                new_epoch: Epoch(2)
            }
        );
        assert_eq!(
            rx.recv().await.unwrap(),
            RemoteWatchEvent::Fenced {
                target,
                current_epoch: Epoch(2)
            }
        );
    }

    #[tokio::test]
    async fn remote_watch_unwatch_removes_registered_target() {
        let target = target_ref(Epoch(1));
        let mut registry = CrossNodeWatchRegistry::new();
        let _rx = registry.watch(target.clone());

        assert_eq!(registry.watcher_count(&target), 1);
        assert!(registry.unwatch(&target));
        assert_eq!(registry.watcher_count(&target), 0);
    }

    fn target_ref(epoch: Epoch) -> RemoteActorRef {
        RemoteActorRef {
            actor_kind: actor_kind!("World"),
            actor_id: ActorId::U64(7),
            owner: InstanceId::new("world-a"),
            epoch,
        }
    }
}
