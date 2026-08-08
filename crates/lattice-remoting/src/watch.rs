use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::{AtomicU64, Ordering},
};

use async_trait::async_trait;
use bytes::Bytes;
use lattice_core::{
    actor_ref::{
        ActivationId, ActorPath, ActorRef, ClusterId, EntityRef, NodeAddress, NodeIncarnation,
        ProtocolId, ProtocolTag, SingletonRef,
    },
    failpoint::{Failpoint, FailpointAction},
};
use prost::{Enumeration, Message};
use thiserror::Error;
use tokio::{
    sync::{oneshot, watch},
    task::AbortHandle,
};

use crate::{association::AssociationId, messaging::target::ExactActorTarget};

pub use lattice_core::watch::{TerminatedReason, WatchId, WatchStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
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

const WATCH_CONTROL_MAGIC: &[u8; 4] = b"LWCH";
pub const WATCH_CONTROL_GENERATION: u32 = 3;

#[derive(Clone, PartialEq, Message)]
struct WatchControlEnvelopeWire {
    #[prost(uint32, tag = "1")]
    generation: u32,
    #[prost(oneof = "watch_control_envelope_wire::Command", tags = "2, 3, 4, 5")]
    command: Option<watch_control_envelope_wire::Command>,
}

mod watch_control_envelope_wire {
    use super::{TerminatedWire, UnwatchWire, WatchAckWire, WatchWire};
    use prost::Oneof;

    #[derive(Clone, PartialEq, Oneof)]
    pub enum Command {
        #[prost(message, tag = "2")]
        Watch(WatchWire),
        #[prost(message, tag = "3")]
        WatchAck(WatchAckWire),
        #[prost(message, tag = "4")]
        Unwatch(UnwatchWire),
        #[prost(message, tag = "5")]
        Terminated(TerminatedWire),
    }
}

#[derive(Clone, PartialEq, Message)]
struct WatchIdWire {
    #[prost(bytes = "bytes", tag = "1")]
    watcher_boot: Bytes,
    #[prost(uint64, tag = "2")]
    sequence: u64,
}

#[derive(Clone, PartialEq, Message)]
struct ExactActorTargetWire {
    #[prost(string, tag = "1")]
    cluster_id: String,
    #[prost(string, tag = "2")]
    host: String,
    #[prost(uint32, tag = "3")]
    port: u32,
    #[prost(bytes = "bytes", tag = "4")]
    node_incarnation: Bytes,
    #[prost(string, tag = "5")]
    actor_path: String,
    #[prost(uint64, tag = "6")]
    activation_sequence: u64,
    #[prost(uint64, tag = "7")]
    protocol_id: u64,
}

#[derive(Clone, PartialEq, Message)]
struct WatchWire {
    #[prost(message, optional, tag = "1")]
    watch_id: Option<WatchIdWire>,
    #[prost(message, optional, tag = "2")]
    target: Option<ExactActorTargetWire>,
}

#[derive(Clone, PartialEq, Message)]
struct WatchAckWire {
    #[prost(message, optional, tag = "1")]
    watch_id: Option<WatchIdWire>,
    #[prost(message, optional, tag = "2")]
    target: Option<ExactActorTargetWire>,
}

#[derive(Clone, PartialEq, Message)]
struct UnwatchWire {
    #[prost(message, optional, tag = "1")]
    watch_id: Option<WatchIdWire>,
}

#[derive(Clone, PartialEq, Message)]
struct TerminatedWire {
    #[prost(message, optional, tag = "1")]
    watch_id: Option<WatchIdWire>,
    #[prost(message, optional, tag = "2")]
    target: Option<ExactActorTargetWire>,
    #[prost(enumeration = "TerminatedReasonWire", tag = "3")]
    reason: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
enum TerminatedReasonWire {
    Unspecified = 0,
    Stopped = 1,
    Panicked = 2,
    Passivated = 3,
    Migrated = 4,
    Fenced = 5,
    NodeDown = 6,
    ActivationChanged = 7,
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
    let encoded = command_to_wire(command).encode_to_vec();
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
    let envelope =
        WatchControlEnvelopeWire::decode(encoded).map_err(|_| WatchError::InvalidCommand)?;
    if envelope.generation != WATCH_CONTROL_GENERATION {
        return Err(WatchError::GenerationMismatch);
    }
    command_from_wire(envelope)
}

fn command_to_wire(command: &WatchCommand) -> WatchControlEnvelopeWire {
    use watch_control_envelope_wire::Command;

    let command = match command {
        WatchCommand::Watch { watch_id, target } => Command::Watch(WatchWire {
            watch_id: Some(watch_id_to_wire(*watch_id)),
            target: Some(target_to_wire(target)),
        }),
        WatchCommand::WatchAck { watch_id, target } => Command::WatchAck(WatchAckWire {
            watch_id: Some(watch_id_to_wire(*watch_id)),
            target: Some(target_to_wire(target)),
        }),
        WatchCommand::Unwatch { watch_id } => Command::Unwatch(UnwatchWire {
            watch_id: Some(watch_id_to_wire(*watch_id)),
        }),
        WatchCommand::Terminated {
            watch_id,
            target,
            reason,
        } => Command::Terminated(TerminatedWire {
            watch_id: Some(watch_id_to_wire(*watch_id)),
            target: Some(target_to_wire(target)),
            reason: reason_to_wire(*reason) as i32,
        }),
    };
    WatchControlEnvelopeWire {
        generation: WATCH_CONTROL_GENERATION,
        command: Some(command),
    }
}

fn command_from_wire(envelope: WatchControlEnvelopeWire) -> Result<WatchCommand, WatchError> {
    use watch_control_envelope_wire::Command;

    match envelope.command.ok_or(WatchError::InvalidCommand)? {
        Command::Watch(wire) => Ok(WatchCommand::Watch {
            watch_id: watch_id_from_wire(wire.watch_id)?,
            target: target_from_wire(wire.target)?,
        }),
        Command::WatchAck(wire) => Ok(WatchCommand::WatchAck {
            watch_id: watch_id_from_wire(wire.watch_id)?,
            target: target_from_wire(wire.target)?,
        }),
        Command::Unwatch(wire) => Ok(WatchCommand::Unwatch {
            watch_id: watch_id_from_wire(wire.watch_id)?,
        }),
        Command::Terminated(wire) => Ok(WatchCommand::Terminated {
            watch_id: watch_id_from_wire(wire.watch_id)?,
            target: target_from_wire(wire.target)?,
            reason: reason_from_wire(wire.reason)?,
        }),
    }
}

fn watch_id_to_wire(watch_id: WatchId) -> WatchIdWire {
    WatchIdWire {
        watcher_boot: Bytes::copy_from_slice(&watch_id.watcher_boot().to_be_bytes()),
        sequence: watch_id.sequence(),
    }
}

fn watch_id_from_wire(wire: Option<WatchIdWire>) -> Result<WatchId, WatchError> {
    let wire = wire.ok_or(WatchError::InvalidCommand)?;
    let watcher_boot = u128::from_be_bytes(
        wire.watcher_boot
            .as_ref()
            .try_into()
            .map_err(|_| WatchError::InvalidCommand)?,
    );
    WatchId::new(watcher_boot, wire.sequence).ok_or(WatchError::InvalidCommand)
}

fn target_to_wire(target: &ExactActorTarget) -> ExactActorTargetWire {
    ExactActorTargetWire {
        cluster_id: target.cluster_id.as_str().to_owned(),
        host: target.node_address.host().to_owned(),
        port: u32::from(target.node_address.port()),
        node_incarnation: Bytes::copy_from_slice(&target.node_incarnation.get().to_be_bytes()),
        actor_path: target.actor_path.to_string(),
        activation_sequence: target.activation_id.local_sequence(),
        protocol_id: target.protocol_id.get(),
    }
}

fn target_from_wire(wire: Option<ExactActorTargetWire>) -> Result<ExactActorTarget, WatchError> {
    let wire = wire.ok_or(WatchError::InvalidCommand)?;
    let node_incarnation = NodeIncarnation::new(u128::from_be_bytes(
        wire.node_incarnation
            .as_ref()
            .try_into()
            .map_err(|_| WatchError::InvalidCommand)?,
    ))
    .map_err(|_| WatchError::InvalidCommand)?;
    let port = u16::try_from(wire.port).map_err(|_| WatchError::InvalidCommand)?;
    Ok(ExactActorTarget {
        cluster_id: ClusterId::new(wire.cluster_id).map_err(|_| WatchError::InvalidCommand)?,
        node_address: NodeAddress::new(wire.host, port).map_err(|_| WatchError::InvalidCommand)?,
        node_incarnation,
        actor_path: ActorPath::try_from(wire.actor_path).map_err(|_| WatchError::InvalidCommand)?,
        activation_id: ActivationId::new(node_incarnation, wire.activation_sequence)
            .map_err(|_| WatchError::InvalidCommand)?,
        protocol_id: ProtocolId::new(wire.protocol_id).map_err(|_| WatchError::InvalidCommand)?,
    })
}

const fn reason_to_wire(reason: TerminatedReason) -> TerminatedReasonWire {
    match reason {
        TerminatedReason::Stopped => TerminatedReasonWire::Stopped,
        TerminatedReason::Panicked => TerminatedReasonWire::Panicked,
        TerminatedReason::Passivated => TerminatedReasonWire::Passivated,
        TerminatedReason::Migrated => TerminatedReasonWire::Migrated,
        TerminatedReason::Fenced => TerminatedReasonWire::Fenced,
        TerminatedReason::NodeDown => TerminatedReasonWire::NodeDown,
        TerminatedReason::ActivationChanged => TerminatedReasonWire::ActivationChanged,
    }
}

fn reason_from_wire(value: i32) -> Result<TerminatedReason, WatchError> {
    match TerminatedReasonWire::try_from(value).map_err(|_| WatchError::InvalidCommand)? {
        TerminatedReasonWire::Stopped => Ok(TerminatedReason::Stopped),
        TerminatedReasonWire::Panicked => Ok(TerminatedReason::Panicked),
        TerminatedReasonWire::Passivated => Ok(TerminatedReason::Passivated),
        TerminatedReasonWire::Migrated => Ok(TerminatedReason::Migrated),
        TerminatedReasonWire::Fenced => Ok(TerminatedReason::Fenced),
        TerminatedReasonWire::NodeDown => Ok(TerminatedReason::NodeDown),
        TerminatedReasonWire::ActivationChanged => Ok(TerminatedReason::ActivationChanged),
        TerminatedReasonWire::Unspecified => Err(WatchError::InvalidCommand),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchTermination {
    pub watch_id: WatchId,
    pub target: ExactActorTarget,
    pub reason: TerminatedReason,
}

pub struct RegisteredWatch {
    watch_id: WatchId,
    status: watch::Receiver<WatchStatus>,
    terminated: oneshot::Receiver<WatchTermination>,
}

impl RegisteredWatch {
    pub const fn id(&self) -> WatchId {
        self.watch_id
    }

    pub fn status(&self) -> WatchStatus {
        *self.status.borrow()
    }

    pub async fn status_changed(&mut self) -> Result<WatchStatus, WatchError> {
        self.status
            .changed()
            .await
            .map_err(|_| WatchError::Closed)?;
        Ok(*self.status.borrow_and_update())
    }

    pub async fn recv(&mut self) -> Result<WatchTermination, WatchError> {
        (&mut self.terminated).await.map_err(|_| WatchError::Closed)
    }
}

struct DesiredWatch {
    association_id: AssociationId,
    target: ExactActorTarget,
    acknowledged: bool,
    cancelling: bool,
    status: watch::Sender<WatchStatus>,
    terminated: oneshot::Sender<WatchTermination>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TargetWatchKey {
    association_id: AssociationId,
    watch_id: WatchId,
}

struct TargetWatch {
    target: ExactActorTarget,
    task: Option<AbortHandle>,
}

pub struct WatchRegistry {
    boot_id: u128,
    next_watch: AtomicU64,
    maximum_desired: usize,
    maximum_target: usize,
    desired: BTreeMap<WatchId, DesiredWatch>,
    target_watches: BTreeMap<String, BTreeMap<TargetWatchKey, TargetWatch>>,
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

    pub fn watch<A: ProtocolTag>(
        &mut self,
        association_id: AssociationId,
        target: &ActorRef<A>,
    ) -> Result<(RegisteredWatch, WatchCommand), WatchError> {
        if self.desired.len() == self.maximum_desired {
            return Err(WatchError::DesiredCapacity);
        }
        let sequence = self.next_watch.fetch_add(1, Ordering::Relaxed);
        let watch_id = WatchId::new(self.boot_id, sequence).ok_or(WatchError::IdExhausted)?;
        let target = ExactActorTarget::from(target);
        let (status, status_rx) = watch::channel(WatchStatus::Pending);
        let (terminated, terminated_rx) = oneshot::channel();
        self.desired.insert(
            watch_id,
            DesiredWatch {
                association_id,
                target: target.clone(),
                acknowledged: false,
                cancelling: false,
                status,
                terminated,
            },
        );
        Ok((
            RegisteredWatch {
                watch_id,
                status: status_rx,
                terminated: terminated_rx,
            },
            WatchCommand::Watch { watch_id, target },
        ))
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
        let key = TargetWatchKey {
            association_id,
            watch_id,
        };
        let replay = self
            .target_watches
            .values()
            .find_map(|watches| watches.get(&key));
        if replay.is_some_and(|existing| existing.target != target) {
            return Err(WatchError::ConflictingReplay);
        }
        if !is_current(&target) {
            return Ok(WatchCommand::Terminated {
                watch_id,
                target,
                reason: TerminatedReason::ActivationChanged,
            });
        }
        if replay.is_some() {
            return Ok(WatchCommand::WatchAck { watch_id, target });
        }
        if self.target_count() >= self.maximum_target {
            return Err(WatchError::TargetCapacity);
        }
        let path = target.actor_path.to_string();
        self.target_watches.entry(path).or_default().insert(
            key,
            TargetWatch {
                target: target.clone(),
                task: None,
            },
        );
        lattice_core::failpoint::hit(Failpoint::WatchAfterInstallBeforeAck);
        Ok(WatchCommand::WatchAck { watch_id, target })
    }

    pub fn attach_target_task(
        &mut self,
        association_id: AssociationId,
        watch_id: WatchId,
        task: AbortHandle,
    ) -> bool {
        let key = TargetWatchKey {
            association_id,
            watch_id,
        };
        for watches in self.target_watches.values_mut() {
            if let Some(watch) = watches.get_mut(&key) {
                if watch.task.is_some() {
                    task.abort();
                    return false;
                }
                watch.task = Some(task);
                return true;
            }
        }
        task.abort();
        false
    }

    pub fn receive_ack(&mut self, watch_id: WatchId, target: &ExactActorTarget) -> bool {
        let Some(desired) = self.desired.get_mut(&watch_id) else {
            return false;
        };
        if desired.target != *target {
            return false;
        }
        desired.acknowledged = true;
        desired.status.send_replace(WatchStatus::Active);
        true
    }

    pub fn begin_unwatch(
        &mut self,
        watch_id: WatchId,
    ) -> Option<(AssociationId, ExactActorTarget, WatchCommand)> {
        let desired = self.desired.get_mut(&watch_id)?;
        desired.cancelling = true;
        Some((
            desired.association_id,
            desired.target.clone(),
            WatchCommand::Unwatch { watch_id },
        ))
    }

    pub fn complete_unwatch(&mut self, watch_id: WatchId) -> bool {
        self.desired.remove(&watch_id).is_some()
    }

    pub fn receive_unwatch(&mut self, association_id: AssociationId, watch_id: WatchId) -> bool {
        let key = TargetWatchKey {
            association_id,
            watch_id,
        };
        let mut removed = false;
        self.target_watches.retain(|_, watches| {
            if let Some(watch) = watches.remove(&key) {
                if let Some(task) = watch.task {
                    task.abort();
                }
                removed = true;
            }
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
        if lattice_core::failpoint::hit_decision(Failpoint::WatchAfterTerminatedBeforeAck)
            == FailpointAction::Drop
        {
            return Vec::new();
        }
        watches
            .into_iter()
            .filter(|(_, watched)| watched.target == *target)
            .map(|(key, watched)| {
                (
                    key.association_id,
                    WatchCommand::Terminated {
                        watch_id: key.watch_id,
                        target: watched.target,
                        reason,
                    },
                )
            })
            .collect()
    }

    pub fn receive_terminated(
        &mut self,
        watch_id: WatchId,
        target: &ExactActorTarget,
        reason: TerminatedReason,
    ) -> bool {
        if self.terminal_delivered.contains(&watch_id) {
            return false;
        }
        let Some(desired) = self.desired.get(&watch_id) else {
            return false;
        };
        if desired.target != *target {
            return false;
        }
        let Some(desired) = self.desired.remove(&watch_id) else {
            return false;
        };
        desired.status.send_replace(WatchStatus::Terminated);
        let _ = desired.terminated.send(WatchTermination {
            watch_id,
            target: target.clone(),
            reason,
        });
        self.remember_terminal(watch_id);
        true
    }

    pub fn reconcile_association(&self, association_id: AssociationId) -> Vec<WatchCommand> {
        self.desired
            .iter()
            .filter(|(_, desired)| desired.association_id == association_id)
            .map(|(watch_id, desired)| {
                if desired.cancelling {
                    WatchCommand::Unwatch {
                        watch_id: *watch_id,
                    }
                } else {
                    WatchCommand::Watch {
                        watch_id: *watch_id,
                        target: desired.target.clone(),
                    }
                }
            })
            .collect()
    }

    pub fn node_down(&mut self, incarnation: NodeIncarnation) -> Vec<WatchTermination> {
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
                desired.status.send_replace(WatchStatus::Terminated);
                let termination = WatchTermination {
                    watch_id: id,
                    target: desired.target,
                    reason: TerminatedReason::NodeDown,
                };
                let _ = desired.terminated.send(termination.clone());
                self.remember_terminal(id);
                Some(termination)
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

    pub fn contains_desired(&self, watch_id: WatchId) -> bool {
        self.desired.contains_key(&watch_id)
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
    async fn resolve_entity_current<A: ProtocolTag>(
        &self,
        reference: &EntityRef<A>,
    ) -> Result<Option<ActorRef<A>>, WatchError>;

    async fn resolve_singleton_current<A: ProtocolTag>(
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
    #[error("watch replay changed the original target")]
    ConflictingReplay,
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
    #[error("watch subscription closed before a terminal event")]
    Closed,
}

#[cfg(test)]
mod tests {
    use lattice_core::actor_ref::{ActivationId, ActorPath, ClusterId, NodeAddress, ProtocolId};

    use super::*;

    fn actor(sequence: u64) -> ActorRef {
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
        let (registered, _) = watcher.watch(association, &old).unwrap();
        let watch_id = registered.id();
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
    fn idempotent_watch_replay_succeeds_when_target_registry_is_full() {
        let association = AssociationId::new(9).unwrap();
        let target = actor(1);
        let exact = ExactActorTarget::from(&target);
        let watch_id = WatchId::new(7, 9).unwrap();
        let mut registry = WatchRegistry::new(8, 1).unwrap();

        assert!(matches!(
            registry
                .receive_watch(association, watch_id, exact.clone(), |_| true)
                .unwrap(),
            WatchCommand::WatchAck { .. }
        ));
        assert_eq!(registry.target_count(), 1);
        assert!(matches!(
            registry
                .receive_watch(association, watch_id, exact, |_| true)
                .unwrap(),
            WatchCommand::WatchAck { .. }
        ));
        assert_eq!(registry.target_count(), 1);

        assert_eq!(
            registry
                .receive_watch(
                    association,
                    WatchId::new(7, 10).unwrap(),
                    ExactActorTarget::from(&actor(2)),
                    |_| true,
                )
                .unwrap_err(),
            WatchError::TargetCapacity
        );
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
    fn panicked_termination_requires_watch_generation_three() {
        assert_eq!(WATCH_CONTROL_GENERATION, 3);
        let target = actor(1);
        let command = WatchCommand::Terminated {
            watch_id: WatchId::new(7, 9).unwrap(),
            target: ExactActorTarget::from(&target),
            reason: TerminatedReason::Panicked,
        };
        let encoded = encode_watch_command(&command, 4096).unwrap();
        assert_eq!(decode_watch_command(&encoded, 4096).unwrap(), command);

        let mut envelope = command_to_wire(&command);
        envelope.generation = 2;
        let mut legacy = WATCH_CONTROL_MAGIC.to_vec();
        legacy.extend_from_slice(&envelope.encode_to_vec());
        assert_eq!(
            decode_watch_command(&legacy, 4096).unwrap_err(),
            WatchError::GenerationMismatch
        );
    }

    #[tokio::test]
    async fn coordinator_node_down_is_terminal_once_for_exact_incarnation() {
        let association = AssociationId::new(9).unwrap();
        let target = actor(1);
        let mut registry = WatchRegistry::new(8, 8).unwrap();
        let (mut registered, _) = registry.watch(association, &target).unwrap();
        let watch_id = registered.id();
        registry.receive_ack(watch_id, &ExactActorTarget::from(&target));

        assert_eq!(registry.node_down(target.node_incarnation()).len(), 1);
        assert_eq!(registry.status(watch_id), WatchStatus::Terminated);
        assert_eq!(
            registered.recv().await.unwrap().reason,
            TerminatedReason::NodeDown
        );
        assert!(registry.node_down(target.node_incarnation()).is_empty());
    }

    #[tokio::test]
    async fn unwatch_aborts_the_target_side_termination_task() {
        let association = AssociationId::new(9).unwrap();
        let target = actor(1);
        let mut registry = WatchRegistry::new(8, 8).unwrap();
        let (registered, _) = registry.watch(association, &target).unwrap();
        let watch_id = registered.id();
        registry
            .receive_watch(
                association,
                watch_id,
                ExactActorTarget::from(&target),
                |_| true,
            )
            .unwrap();
        let task = tokio::spawn(std::future::pending::<()>());
        assert!(registry.attach_target_task(association, watch_id, task.abort_handle()));

        let (unwatch_association, _, _) = registry.begin_unwatch(watch_id).unwrap();
        assert!(registry.receive_unwatch(unwatch_association, watch_id));
        assert!(registry.complete_unwatch(watch_id));
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(registry.target_count(), 0);
    }
}
