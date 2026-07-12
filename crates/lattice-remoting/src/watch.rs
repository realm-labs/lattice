use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use lattice_core::actor_ref::{ActorRef, EntityRef, NodeIncarnation, SingletonRef};
use thiserror::Error;

use crate::association::AssociationId;
use crate::messaging::ExactActorTarget;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct WatchId {
    watcher_boot: u128,
    sequence: u64,
}

impl WatchId {
    pub const fn new(watcher_boot: u128, sequence: u64) -> Option<Self> {
        if watcher_boot == 0 || sequence == 0 {
            None
        } else {
            Some(Self {
                watcher_boot,
                sequence,
            })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TerminatedReason {
    Stopped,
    Passivated,
    Handoff,
    ClaimLost,
    NodeDown,
    ActivationChanged,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum WatchCommand {
    Watch {
        watch_id: WatchId,
        target: ExactActorTarget,
    },
    WatchAck {
        watch_id: WatchId,
        target: ExactActorTarget,
    },
    Unwatch {
        watch_id: WatchId,
    },
    Terminated {
        watch_id: WatchId,
        target: ExactActorTarget,
        reason: TerminatedReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchStatus {
    Pending,
    Active,
    Terminated,
    Unknown,
}

const WATCH_CONTROL_MAGIC: &[u8; 4] = b"LWCH";
pub const WATCH_CONTROL_GENERATION: u32 = 1;

#[derive(serde::Serialize, serde::Deserialize)]
struct WatchControlEnvelope {
    generation: u32,
    command: WatchCommand,
}

pub fn is_watch_control(payload: &[u8]) -> bool {
    payload.starts_with(WATCH_CONTROL_MAGIC)
}

pub fn encode_watch_command(
    command: &WatchCommand,
    maximum_payload: usize,
) -> Result<Bytes, WatchError> {
    if maximum_payload <= WATCH_CONTROL_MAGIC.len() {
        return Err(WatchError::ZeroLimit);
    }
    let encoded = serde_json::to_vec(&WatchControlEnvelope {
        generation: WATCH_CONTROL_GENERATION,
        command: command.clone(),
    })
    .map_err(|_| WatchError::InvalidCommand)?;
    if encoded.len().saturating_add(WATCH_CONTROL_MAGIC.len()) > maximum_payload {
        return Err(WatchError::PayloadTooLarge);
    }
    let mut payload = Vec::with_capacity(WATCH_CONTROL_MAGIC.len() + encoded.len());
    payload.extend_from_slice(WATCH_CONTROL_MAGIC);
    payload.extend_from_slice(&encoded);
    Ok(Bytes::from(payload))
}

pub fn decode_watch_command(
    payload: &[u8],
    maximum_payload: usize,
) -> Result<WatchCommand, WatchError> {
    if maximum_payload <= WATCH_CONTROL_MAGIC.len() || payload.len() > maximum_payload {
        return Err(WatchError::PayloadTooLarge);
    }
    let encoded = payload
        .strip_prefix(WATCH_CONTROL_MAGIC)
        .ok_or(WatchError::InvalidCommand)?;
    let envelope: WatchControlEnvelope =
        serde_json::from_slice(encoded).map_err(|_| WatchError::InvalidCommand)?;
    if envelope.generation != WATCH_CONTROL_GENERATION {
        return Err(WatchError::GenerationMismatch);
    }
    Ok(envelope.command)
}

#[derive(Debug, Clone)]
struct DesiredWatch {
    association_id: AssociationId,
    target: ExactActorTarget,
    acknowledged: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TargetWatchKey {
    association_id: AssociationId,
    watch_id: WatchId,
}

pub struct WatchRegistry {
    boot_id: u128,
    next_watch: AtomicU64,
    maximum_desired: usize,
    maximum_target: usize,
    desired: BTreeMap<WatchId, DesiredWatch>,
    target_watches: BTreeMap<String, BTreeMap<TargetWatchKey, ExactActorTarget>>,
    terminal_delivered: BTreeSet<WatchId>,
}

impl WatchRegistry {
    pub fn new(maximum_desired: usize, maximum_target: usize) -> Result<Self, WatchError> {
        if maximum_desired == 0 || maximum_target == 0 {
            return Err(WatchError::ZeroLimit);
        }
        Ok(Self {
            boot_id: uuid::Uuid::new_v4().as_u128(),
            next_watch: AtomicU64::new(1),
            maximum_desired,
            maximum_target,
            desired: BTreeMap::new(),
            target_watches: BTreeMap::new(),
            terminal_delivered: BTreeSet::new(),
        })
    }

    pub fn watch<A>(
        &mut self,
        association_id: AssociationId,
        target: &ActorRef<A>,
    ) -> Result<(WatchId, WatchCommand), WatchError> {
        if self.desired.len() == self.maximum_desired {
            return Err(WatchError::DesiredCapacity);
        }
        let sequence = self.next_watch.fetch_add(1, Ordering::Relaxed);
        let watch_id = WatchId::new(self.boot_id, sequence).ok_or(WatchError::IdExhausted)?;
        let target = ExactActorTarget::from(target);
        self.desired.insert(
            watch_id,
            DesiredWatch {
                association_id,
                target: target.clone(),
                acknowledged: false,
            },
        );
        Ok((watch_id, WatchCommand::Watch { watch_id, target }))
    }

    pub fn receive_watch<F>(
        &mut self,
        association_id: AssociationId,
        watch_id: WatchId,
        target: ExactActorTarget,
        is_current: F,
    ) -> Result<WatchCommand, WatchError>
    where
        F: FnOnce(&ExactActorTarget) -> bool,
    {
        if !is_current(&target) {
            return Ok(WatchCommand::Terminated {
                watch_id,
                target,
                reason: TerminatedReason::ActivationChanged,
            });
        }
        if self.target_count() == self.maximum_target {
            return Err(WatchError::TargetCapacity);
        }
        let path = target.actor_path.to_string();
        self.target_watches.entry(path).or_default().insert(
            TargetWatchKey {
                association_id,
                watch_id,
            },
            target.clone(),
        );
        Ok(WatchCommand::WatchAck { watch_id, target })
    }

    pub fn receive_ack(&mut self, watch_id: WatchId, target: &ExactActorTarget) -> bool {
        let Some(desired) = self.desired.get_mut(&watch_id) else {
            return false;
        };
        if desired.target != *target {
            return false;
        }
        desired.acknowledged = true;
        true
    }

    pub fn unwatch(&mut self, watch_id: WatchId) -> Option<(AssociationId, WatchCommand)> {
        self.desired
            .remove(&watch_id)
            .map(|desired| (desired.association_id, WatchCommand::Unwatch { watch_id }))
    }

    pub fn receive_unwatch(&mut self, association_id: AssociationId, watch_id: WatchId) -> bool {
        let key = TargetWatchKey {
            association_id,
            watch_id,
        };
        let mut removed = false;
        self.target_watches.retain(|_, watches| {
            removed |= watches.remove(&key).is_some();
            !watches.is_empty()
        });
        removed
    }

    pub fn target_terminated(
        &mut self,
        target: &ExactActorTarget,
        reason: TerminatedReason,
    ) -> Vec<(AssociationId, WatchCommand)> {
        let Some(watches) = self.target_watches.remove(&target.actor_path.to_string()) else {
            return Vec::new();
        };
        watches
            .into_iter()
            .filter(|(_, watched)| watched == target)
            .map(|(key, watched)| {
                (
                    key.association_id,
                    WatchCommand::Terminated {
                        watch_id: key.watch_id,
                        target: watched,
                        reason,
                    },
                )
            })
            .collect()
    }

    pub fn receive_terminated(&mut self, watch_id: WatchId, target: &ExactActorTarget) -> bool {
        if self.terminal_delivered.contains(&watch_id) {
            return false;
        }
        let Some(desired) = self.desired.get(&watch_id) else {
            return false;
        };
        if desired.target != *target {
            return false;
        }
        self.desired.remove(&watch_id);
        self.remember_terminal(watch_id);
        true
    }

    pub fn reconcile_association(&self, association_id: AssociationId) -> Vec<WatchCommand> {
        self.desired
            .iter()
            .filter_map(|(watch_id, desired)| {
                (desired.association_id == association_id).then_some(WatchCommand::Watch {
                    watch_id: *watch_id,
                    target: desired.target.clone(),
                })
            })
            .collect()
    }

    pub fn node_down(&mut self, incarnation: NodeIncarnation) -> Vec<(WatchId, ExactActorTarget)> {
        let ids = self
            .desired
            .iter()
            .filter_map(|(id, desired)| {
                (desired.target.node_incarnation == incarnation).then_some(*id)
            })
            .collect::<Vec<_>>();
        ids.into_iter()
            .filter_map(|id| {
                let desired = self.desired.remove(&id)?;
                self.remember_terminal(id);
                Some((id, desired.target))
            })
            .collect()
    }

    fn remember_terminal(&mut self, watch_id: WatchId) {
        self.terminal_delivered.insert(watch_id);
        while self.terminal_delivered.len() > self.maximum_desired {
            if let Some(oldest) = self.terminal_delivered.pop_first()
                && oldest == watch_id
            {
                self.terminal_delivered.insert(oldest);
                break;
            }
        }
    }

    pub fn target_count(&self) -> usize {
        self.target_watches.values().map(BTreeMap::len).sum()
    }

    pub fn desired_count(&self) -> usize {
        self.desired.len()
    }

    pub fn is_acknowledged(&self, watch_id: WatchId) -> bool {
        self.desired
            .get(&watch_id)
            .is_some_and(|watch| watch.acknowledged)
    }

    pub fn terminal_was_delivered(&self, watch_id: WatchId) -> bool {
        self.terminal_delivered.contains(&watch_id)
    }

    pub fn status(&self, watch_id: WatchId) -> WatchStatus {
        if self.terminal_delivered.contains(&watch_id) {
            WatchStatus::Terminated
        } else {
            match self.desired.get(&watch_id) {
                Some(watch) if watch.acknowledged => WatchStatus::Active,
                Some(_) => WatchStatus::Pending,
                None => WatchStatus::Unknown,
            }
        }
    }
}

#[async_trait]
pub trait CurrentActivationResolver: Send + Sync {
    async fn resolve_entity_current<A>(
        &self,
        reference: &EntityRef<A>,
    ) -> Result<Option<ActorRef<A>>, WatchError>;

    async fn resolve_singleton_current<A>(
        &self,
        reference: &SingletonRef<A>,
    ) -> Result<Option<ActorRef<A>>, WatchError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WatchError {
    #[error("watch limits must be nonzero")]
    ZeroLimit,
    #[error("desired watch registry is full")]
    DesiredCapacity,
    #[error("target watch registry is full")]
    TargetCapacity,
    #[error("watch ID sequence is exhausted")]
    IdExhausted,
    #[error("logical entity has no current activation")]
    NotActive,
    #[error("singleton has no currently available activation")]
    Unavailable,
    #[error("watch command is invalid for current state")]
    InvalidCommand,
    #[error("watch control command exceeds its payload bound")]
    PayloadTooLarge,
    #[error("watch control schema generation differs")]
    GenerationMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;
    use lattice_core::actor_ref::{ActivationId, ActorPath, ClusterId, NodeAddress, ProtocolId};

    fn actor(sequence: u64) -> ActorRef<()> {
        let node = NodeIncarnation::new(2).unwrap();
        ActorRef::new(
            ClusterId::new("test").unwrap(),
            NodeAddress::new("remote", 25520).unwrap(),
            node,
            ActorPath::user(["user", "actor"]).unwrap(),
            ActivationId::new(node, sequence).unwrap(),
            ProtocolId::new(7).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn reconnect_reinstalls_exact_activation_and_replacement_terminates_old_watch() {
        let association = AssociationId::new(9).unwrap();
        let mut watcher = WatchRegistry::new(8, 8).unwrap();
        let old = actor(1);
        let (watch_id, _) = watcher.watch(association, &old).unwrap();
        assert_eq!(watcher.reconcile_association(association).len(), 1);

        let mut target = WatchRegistry::new(8, 8).unwrap();
        let command = target
            .receive_watch(
                association,
                watch_id,
                ExactActorTarget::from(&old),
                |candidate| candidate.activation_id == old.activation_id(),
            )
            .unwrap();
        assert!(matches!(command, WatchCommand::WatchAck { .. }));

        let replacement = actor(2);
        let stale = target
            .receive_watch(
                association,
                watch_id,
                ExactActorTarget::from(&old),
                |candidate| candidate.activation_id == replacement.activation_id(),
            )
            .unwrap();
        assert!(matches!(
            stale,
            WatchCommand::Terminated {
                reason: TerminatedReason::ActivationChanged,
                ..
            }
        ));
    }

    #[test]
    fn watch_control_codec_is_bounded_and_generation_tagged() {
        let target = actor(1);
        let command = WatchCommand::Watch {
            watch_id: WatchId::new(7, 9).unwrap(),
            target: ExactActorTarget::from(&target),
        };
        let encoded = encode_watch_command(&command, 4096).unwrap();
        assert!(is_watch_control(&encoded));
        assert_eq!(decode_watch_command(&encoded, 4096).unwrap(), command);
        assert_eq!(
            decode_watch_command(&encoded, 4).unwrap_err(),
            WatchError::PayloadTooLarge
        );
    }

    #[test]
    fn coordinator_node_down_is_terminal_once_for_exact_incarnation() {
        let association = AssociationId::new(9).unwrap();
        let target = actor(1);
        let mut registry = WatchRegistry::new(8, 8).unwrap();
        let (watch_id, _) = registry.watch(association, &target).unwrap();
        registry.receive_ack(watch_id, &ExactActorTarget::from(&target));

        assert_eq!(registry.node_down(target.node_incarnation()).len(), 1);
        assert_eq!(registry.status(watch_id), WatchStatus::Terminated);
        assert!(registry.node_down(target.node_incarnation()).is_empty());
    }
}
