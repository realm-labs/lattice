use lattice_model::{
    cluster::CoordinatorScope,
    run::{ClusterLifecycle, ControlOperationId, RunEpoch, RunPhase},
};
use lattice_remoting::association::AssociationKey;

use super::{CoordinatorHost, CoordinatorRuntimeError};
use crate::{
    administration::AdminAction,
    control::{PlacementControlCommand, control_stream_id, encode_control_command_for_term},
    coordinator::ClusterLeaderGuard,
    shutdown::{ClusterShutdown, ShutdownReport, ShutdownRequestRejection},
    storage::{ActorGroupStore, MembershipStore, ScopedElectionStore, StorageError},
    types::NodeKey,
};

impl<S: ScopedElectionStore + MembershipStore + ActorGroupStore> CoordinatorHost<S> {
    pub(super) async fn accept_node_stopped(
        &mut self,
        association: &AssociationKey,
        epoch: RunEpoch,
        node: NodeKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        self.validate_stop_peer(association, &node)?;
        let leader = self
            .membership
            .as_ref()
            .ok_or(CoordinatorRuntimeError::NotLeader)?;
        if epoch != leader.leader().epoch {
            return Err(StorageError::RunMismatch.into());
        }
        let guard = ClusterLeaderGuard::new(leader.leader().clone())
            .map_err(CoordinatorRuntimeError::Coordinator)?;
        self.store.record_node_stopped(&guard, &node).await?;
        let payload = encode_control_command_for_term(
            &CoordinatorScope::Cluster,
            guard.term().get(),
            &PlacementControlCommand::NodeStopConfirmed { epoch, node },
            self.config.group.maximum_control_payload,
        )
        .map_err(CoordinatorRuntimeError::Control)?;
        let connection = self
            .associations
            .get(association)
            .ok_or(CoordinatorRuntimeError::UnauthorizedCommand)?;
        connection
            .admit_control_command_in(control_stream_id(&CoordinatorScope::Cluster), payload)?;
        Ok(())
    }
    fn validate_stop_peer(
        &self,
        association: &AssociationKey,
        node: &NodeKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        let connection = self
            .associations
            .get(association)
            .ok_or(CoordinatorRuntimeError::UnauthorizedCommand)?;
        if association.remote_incarnation != node.incarnation
            || association.remote_address != node.address
            || connection.authenticated_control_peer().is_some_and(|peer| {
                peer.node_id != node.node_id
                    || peer.incarnation != node.incarnation
                    || peer.address != node.address
            })
        {
            return Err(CoordinatorRuntimeError::UnauthorizedCommand);
        }
        // The store additionally requires this exact admitted NodeKey in the
        // persistent stop-obligation set. A claimed node name alone is not proof.
        Ok(())
    }

    pub(super) async fn resume_shutdown_hello(
        &mut self,
        association: &AssociationKey,
        node: &NodeKey,
    ) -> Result<bool, CoordinatorRuntimeError> {
        let lifecycle = self.store.lifecycle().await?;
        let command = match &lifecycle.phase {
            RunPhase::Closing { operation, .. } => {
                let driver = self.shutdown_driver()?;
                // No shutdown side effects while manifest construction is incomplete.
                let manifest = self
                    .store
                    .build_shutdown_manifest(
                        &ClusterLeaderGuard::new(
                            self.membership
                                .as_ref()
                                .ok_or(CoordinatorRuntimeError::NotLeader)?
                                .leader()
                                .clone(),
                        )
                        .map_err(CoordinatorRuntimeError::Coordinator)?,
                        operation,
                    )
                    .await?;
                if !manifest.sealed {
                    return Err(StorageError::ShutdownBlocked.into());
                }
                let mut after = None;
                let mut admitted = false;
                loop {
                    let page = driver.participants(operation, after.as_deref()).await?;
                    if page.iter().any(|(_, participant)| participant == node) {
                        admitted = true;
                        break;
                    }
                    let Some((last, _)) = page.last() else {
                        break;
                    };
                    after = Some(last.clone());
                }
                if !admitted {
                    return Err(CoordinatorRuntimeError::UnauthorizedCommand);
                }
                self.validate_stop_peer(association, node)?;
                PlacementControlCommand::ClusterClosing {
                    epoch: lifecycle.epoch,
                    operation: operation.clone(),
                }
            }
            RunPhase::Closed { .. } => PlacementControlCommand::ClusterClosed { lifecycle },
            RunPhase::Running => return Ok(false),
            RunPhase::Resetting { .. } => return Err(StorageError::RunNotRunning.into()),
        };
        let leader = self
            .membership
            .as_ref()
            .ok_or(CoordinatorRuntimeError::NotLeader)?;
        let payload = encode_control_command_for_term(
            &CoordinatorScope::Cluster,
            leader.leader().term.get(),
            &command,
            self.config.group.maximum_control_payload,
        )
        .map_err(CoordinatorRuntimeError::Control)?;
        self.associations
            .get(association)
            .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?
            .admit_control_command_in(control_stream_id(&CoordinatorScope::Cluster), payload)?;
        self.membership_associations
            .insert(node.incarnation, association.clone());
        Ok(true)
    }

    fn shutdown_driver(&self) -> Result<ClusterShutdown<S>, CoordinatorRuntimeError> {
        let leader = self
            .membership
            .as_ref()
            .ok_or(CoordinatorRuntimeError::NotLeader)?;
        let guard = ClusterLeaderGuard::new(leader.leader().clone())
            .map_err(CoordinatorRuntimeError::Coordinator)?;
        Ok(ClusterShutdown::new(self.store.clone(), guard))
    }

    pub(super) async fn respond_shutdown_request(
        &self,
        association: &AssociationKey,
        requested_term: Option<u64>,
        epoch: RunEpoch,
        operation: ControlOperationId,
    ) -> Result<(), CoordinatorRuntimeError> {
        let result = self
            .request_cluster_shutdown(association, epoch, operation.clone())
            .await;
        let outcome = result.map_err(|error| match error {
            CoordinatorRuntimeError::UnauthorizedCommand => ShutdownRequestRejection::Unauthorized,
            CoordinatorRuntimeError::Storage(StorageError::RunMismatch) => {
                ShutdownRequestRejection::StaleRun
            }
            _ => ShutdownRequestRejection::Unavailable,
        });
        let term = self
            .membership
            .as_ref()
            .map(|leader| leader.leader().term.get())
            .or(requested_term)
            .ok_or(CoordinatorRuntimeError::NotLeader)?;
        let payload = encode_control_command_for_term(
            &CoordinatorScope::Cluster,
            term,
            &PlacementControlCommand::ClusterShutdownResult {
                request: operation,
                outcome,
            },
            self.config.group.maximum_control_payload,
        )
        .map_err(CoordinatorRuntimeError::Control)?;
        let peer = self
            .associations
            .get(association)
            .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
        peer.admit_control_command_in(control_stream_id(&CoordinatorScope::Cluster), payload)?;
        Ok(())
    }

    pub(super) async fn request_cluster_shutdown(
        &self,
        association: &AssociationKey,
        epoch: RunEpoch,
        operation: ControlOperationId,
    ) -> Result<ClusterLifecycle, CoordinatorRuntimeError> {
        let peer = self
            .associations
            .get(association)
            .ok_or(CoordinatorRuntimeError::UnauthorizedCommand)?;
        self.config
            .administrators
            .as_ref()
            .ok_or(CoordinatorRuntimeError::UnauthorizedCommand)?
            .authorize(&peer, AdminAction::ShutdownCluster)
            .map_err(|_| CoordinatorRuntimeError::UnauthorizedCommand)?;
        if self.store.lifecycle().await?.epoch != epoch {
            return Err(StorageError::RunMismatch.into());
        }
        Ok(self.shutdown_driver()?.request_shutdown(operation).await?)
    }

    pub(super) async fn accept_stop_report(
        &mut self,
        association: &AssociationKey,
        report: ShutdownReport,
    ) -> Result<(), CoordinatorRuntimeError> {
        self.validate_stop_peer(association, &report.node)?;
        self.shutdown_driver()?.report(report.clone()).await?;
        self.membership_associations
            .insert(report.node.incarnation, association.clone());
        Ok(())
    }

    pub(super) async fn drive_cluster_shutdown(&mut self) -> Result<(), CoordinatorRuntimeError> {
        let lifecycle = self.store.lifecycle().await?;
        if matches!(lifecycle.phase, RunPhase::Closed { .. }) {
            if self.membership.is_some() {
                self.broadcast_shutdown(PlacementControlCommand::ClusterClosed { lifecycle })?;
            }
            return Ok(());
        }
        let RunPhase::Closing { operation, .. } = &lifecycle.phase else {
            return Ok(());
        };
        if self.membership.is_none() {
            return Ok(());
        }
        let driver = self.shutdown_driver()?;
        let progress = driver.advance(operation).await;
        match progress {
            Ok(progress) if progress.complete => {
                self.broadcast_shutdown(PlacementControlCommand::ClusterClosed {
                    lifecycle: progress.lifecycle,
                })?
            }
            Ok(progress)
                if progress
                    .manifest
                    .as_ref()
                    .is_some_and(|manifest| manifest.sealed) =>
            {
                self.broadcast_shutdown(PlacementControlCommand::ClusterClosing {
                    epoch: lifecycle.epoch,
                    operation: operation.clone(),
                })?;
            }
            Err(StorageError::ShutdownBlocked) => {
                self.broadcast_shutdown(PlacementControlCommand::ClusterClosing {
                    epoch: lifecycle.epoch,
                    operation: operation.clone(),
                })?;
            }
            Err(error) => return Err(error.into()),
            _ => {}
        }
        Ok(())
    }

    fn broadcast_shutdown(
        &self,
        command: PlacementControlCommand,
    ) -> Result<(), CoordinatorRuntimeError> {
        let leader = self
            .membership
            .as_ref()
            .ok_or(CoordinatorRuntimeError::NotLeader)?;
        let payload = encode_control_command_for_term(
            &CoordinatorScope::Cluster,
            leader.leader().term.get(),
            &command,
            self.config.group.maximum_control_payload,
        )
        .map_err(CoordinatorRuntimeError::Control)?;
        for key in self.membership_associations.values() {
            if let Some(association) = self.associations.get(key) {
                // Retry on every bounded progress pass. Reliable control delivery
                // preserves application acknowledgement, not just socket writes.
                let _ = association.admit_control_command_in(
                    control_stream_id(&CoordinatorScope::Cluster),
                    payload.clone(),
                );
            }
        }
        Ok(())
    }
}
