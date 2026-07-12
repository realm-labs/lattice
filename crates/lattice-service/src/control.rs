use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use lattice_actor::ProtocolHostRegistry;
use lattice_remoting::{
    Association, AssociationKey, AssociationManager, CommandId, ControlDispatch,
    ControlDispatchError, ControlGap, TerminatedReason, WatchCommand, WatchRegistry,
    decode_watch_command, encode_watch_command, is_watch_control,
};

use crate::supervisor::TaskSupervisor;

pub(crate) struct ServiceControlDispatch {
    application: Arc<dyn ControlDispatch>,
    associations: Arc<AssociationManager>,
    hosts: Arc<ProtocolHostRegistry>,
    watches: Arc<Mutex<WatchRegistry>>,
    supervisor: Arc<TaskSupervisor>,
    maximum_payload: usize,
    application_scope: Option<AssociationKey>,
}

impl ServiceControlDispatch {
    pub fn new(
        application: Arc<dyn ControlDispatch>,
        associations: Arc<AssociationManager>,
        hosts: Arc<ProtocolHostRegistry>,
        watches: Arc<Mutex<WatchRegistry>>,
        supervisor: Arc<TaskSupervisor>,
        maximum_payload: usize,
        application_scope: Option<AssociationKey>,
    ) -> Result<Self, ControlDispatchError> {
        if maximum_payload <= 4 {
            return Err(ControlDispatchError::InvalidCommand);
        }
        Ok(Self {
            application,
            associations,
            hosts,
            watches,
            supervisor,
            maximum_payload,
            application_scope,
        })
    }

    fn send(
        &self,
        association: &Association,
        command: &WatchCommand,
    ) -> Result<(), ControlDispatchError> {
        let payload = encode_watch_command(command, self.maximum_payload)
            .map_err(|_| ControlDispatchError::InvalidCommand)?;
        association
            .admit_control_command(payload)
            .map(|_| ())
            .map_err(|_| ControlDispatchError::Unavailable)
    }

    fn supervise_termination(
        &self,
        target: lattice_remoting::ExactActorTarget,
        mut terminated: tokio::sync::broadcast::Receiver<lattice_actor::watch::ActorTerminated>,
    ) -> Result<(), ControlDispatchError> {
        let watches = self.watches.clone();
        let associations = self.associations.clone();
        let maximum_payload = self.maximum_payload;
        self.supervisor
            .spawn(async move {
                let Ok(terminated) = terminated.recv().await else {
                    return;
                };
                let reason = match terminated.reason {
                    lattice_actor::watch::TerminatedReason::Stopped => TerminatedReason::Stopped,
                    lattice_actor::watch::TerminatedReason::Passivated => {
                        TerminatedReason::Passivated
                    }
                    lattice_actor::watch::TerminatedReason::Migrated => TerminatedReason::Handoff,
                    lattice_actor::watch::TerminatedReason::Fenced => TerminatedReason::ClaimLost,
                    lattice_actor::watch::TerminatedReason::NodeDown => TerminatedReason::NodeDown,
                };
                let commands = watches
                    .lock()
                    .expect("watch registry poisoned")
                    .target_terminated(&target, reason);
                for (association_id, command) in commands {
                    let Some(association) = associations.get_by_id(association_id) else {
                        continue;
                    };
                    let Ok(payload) = encode_watch_command(&command, maximum_payload) else {
                        continue;
                    };
                    let _ = association.admit_control_command(payload);
                }
            })
            .map_err(|_| ControlDispatchError::Unavailable)
    }

    async fn apply_watch(
        &self,
        association_key: AssociationKey,
        command: WatchCommand,
    ) -> Result<(), ControlDispatchError> {
        let association = self
            .associations
            .get(&association_key)
            .ok_or(ControlDispatchError::Unavailable)?;
        match command {
            WatchCommand::Watch { watch_id, target } => {
                let terminated = self.hosts.subscribe_terminated(&target);
                let response = self
                    .watches
                    .lock()
                    .expect("watch registry poisoned")
                    .receive_watch(association.id(), watch_id, target.clone(), |candidate| {
                        self.hosts.is_current(candidate)
                    })
                    .map_err(|_| ControlDispatchError::Unavailable)?;
                if matches!(response, WatchCommand::WatchAck { .. })
                    && let Some(terminated) = terminated
                {
                    self.supervise_termination(target, terminated)?;
                }
                self.send(&association, &response)
            }
            WatchCommand::WatchAck { watch_id, target } => {
                if self
                    .watches
                    .lock()
                    .expect("watch registry poisoned")
                    .receive_ack(watch_id, &target)
                {
                    Ok(())
                } else {
                    Err(ControlDispatchError::InvalidCommand)
                }
            }
            WatchCommand::Unwatch { watch_id } => {
                self.watches
                    .lock()
                    .expect("watch registry poisoned")
                    .receive_unwatch(association.id(), watch_id);
                Ok(())
            }
            WatchCommand::Terminated {
                watch_id, target, ..
            } => {
                if self
                    .watches
                    .lock()
                    .expect("watch registry poisoned")
                    .receive_terminated(watch_id, &target)
                {
                    Ok(())
                } else {
                    Err(ControlDispatchError::InvalidCommand)
                }
            }
        }
    }
}

#[async_trait]
impl ControlDispatch for ServiceControlDispatch {
    async fn apply(
        &self,
        association: AssociationKey,
        command_id: CommandId,
        payload: Bytes,
    ) -> Result<(), ControlDispatchError> {
        if is_watch_control(&payload) {
            let command = decode_watch_command(&payload, self.maximum_payload)
                .map_err(|_| ControlDispatchError::InvalidCommand)?;
            self.apply_watch(association, command).await
        } else {
            if self
                .application_scope
                .as_ref()
                .is_some_and(|scope| scope != &association)
            {
                return Err(ControlDispatchError::InvalidCommand);
            }
            self.application
                .apply(association, command_id, payload)
                .await
        }
    }

    async fn reconcile(
        &self,
        association: AssociationKey,
        gap: Option<ControlGap>,
    ) -> Result<(), ControlDispatchError> {
        if self
            .application_scope
            .as_ref()
            .is_none_or(|scope| scope == &association)
        {
            match self.application.reconcile(association.clone(), gap).await {
                Ok(()) | Err(ControlDispatchError::Unsupported) => {}
                Err(error) => return Err(error),
            }
        }
        let target = self
            .associations
            .get(&association)
            .ok_or(ControlDispatchError::Unavailable)?;
        let commands = self
            .watches
            .lock()
            .expect("watch registry poisoned")
            .reconcile_association(target.id());
        for command in commands {
            self.send(&target, &command)?;
        }
        Ok(())
    }
}
