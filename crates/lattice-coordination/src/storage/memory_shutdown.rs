use super::{InMemoryCoordinationStore, MemoryState, StorageError, validate_leader};
use crate::{
    coordinator::ClusterLeaderGuard,
    shutdown::{
        ClusterShutdownStore, NodeStopOutcome, ShutdownManifest, ShutdownProgress, ShutdownReport,
        extend_digest, participant_key,
    },
    types::NodeKey,
};
use async_trait::async_trait;
use lattice_model::run::{
    ClosingStage, ClusterLifecycle, ControlOperationId, RunCompletion, RunPhase,
};

fn closing(
    state: &MemoryState,
    operation: &ControlOperationId,
    stage: ClosingStage,
) -> Result<ClusterLifecycle, StorageError> {
    let run = state
        .lifecycle
        .clone()
        .ok_or(StorageError::RunNotInitialized)?;
    if !matches!(&run.phase,RunPhase::Closing {operation:active,stage:actual} if active==operation && *actual==stage)
    {
        return Err(StorageError::RunNotRunning);
    }
    Ok(run)
}

#[async_trait]
impl ClusterShutdownStore for InMemoryCoordinationStore {
    async fn record_node_stopped(
        &self,
        guard: &ClusterLeaderGuard,
        node: &NodeKey,
    ) -> Result<(), StorageError> {
        let mut state = self.inner.lock().expect("memory store poisoned");
        validate_leader(&state, guard)?;
        let run = state
            .lifecycle
            .as_ref()
            .ok_or(StorageError::RunNotInitialized)?;
        let running = run.is_running();
        if !running
            && !matches!(
                run.phase,
                RunPhase::Closing {
                    stage: ClosingStage::Draining,
                    ..
                }
            )
        {
            return Err(StorageError::RunNotRunning);
        }
        if running && state.members.contains_key(&node.node_id) {
            return Err(StorageError::CompareFailed);
        }
        let id = participant_key(node);
        if let Some(expected) = state.shutdown_obligations.get(&id) {
            if expected != node {
                return Err(StorageError::CompareFailed);
            }
        } else {
            return if running {
                Ok(())
            } else {
                Err(StorageError::CompareFailed)
            };
        }
        if running {
            state.shutdown_obligations.remove(&id);
        } else {
            state.stopped_nodes.insert(id, node.clone());
        }
        Ok(())
    }
    async fn build_shutdown_manifest(
        &self,
        guard: &ClusterLeaderGuard,
        operation: &ControlOperationId,
    ) -> Result<ShutdownManifest, StorageError> {
        let mut state = self.inner.lock().expect("memory store poisoned");
        validate_leader(&state, guard)?;
        let run = closing(&state, operation, ClosingStage::Draining)?;
        if let Some(manifest) = &state.shutdown_manifest {
            if manifest.sealed {
                return Ok(manifest.clone());
            }
        }
        let mut manifest = state.shutdown_manifest.clone().unwrap_or(ShutdownManifest {
            epoch: run.epoch,
            operation: operation.clone(),
            source_revision: 1,
            cursor: None,
            participant_count: 0,
            digest: [0; 32],
            sealed: false,
        });
        let mut nodes = state
            .shutdown_obligations
            .iter()
            .filter(|(id, _)| manifest.cursor.as_ref().is_none_or(|cursor| *id > cursor))
            .take(33)
            .map(|(id, node)| (id.clone(), node.clone()))
            .collect::<Vec<_>>();
        manifest.sealed = nodes.len() <= 32;
        nodes.truncate(32);
        for (id, node) in nodes {
            manifest.digest = extend_digest(manifest.digest, &id, &node)?;
            manifest.participant_count += 1;
            manifest.cursor = Some(id);
        }
        state.shutdown_manifest = Some(manifest.clone());
        Ok(manifest)
    }
    async fn shutdown_participants(
        &self,
        operation: &ControlOperationId,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, NodeKey)>, StorageError> {
        if limit == 0 || limit > 32 {
            return Err(StorageError::InvalidConfig);
        }
        let state = self.inner.lock().expect("memory store poisoned");
        closing(&state, operation, ClosingStage::Draining)?;
        if state
            .shutdown_manifest
            .as_ref()
            .is_none_or(|manifest| !manifest.sealed || manifest.operation != *operation)
        {
            return Err(StorageError::CompareFailed);
        }
        Ok(state
            .shutdown_obligations
            .iter()
            .filter(|(id, _)| after.is_none_or(|cursor| id.as_str() > cursor))
            .take(limit)
            .map(|(id, node)| (id.clone(), node.clone()))
            .collect())
    }
    async fn record_shutdown_report(
        &self,
        guard: &ClusterLeaderGuard,
        report: ShutdownReport,
    ) -> Result<(), StorageError> {
        let mut state = self.inner.lock().expect("memory store poisoned");
        validate_leader(&state, guard)?;
        let run = closing(&state, &report.operation, ClosingStage::Draining)?;
        if run.epoch != report.epoch {
            return Err(StorageError::RunMismatch);
        }
        if matches!(&report.outcome,NodeStopOutcome::Blocked {reason} if reason.len()>1024) {
            return Err(StorageError::InvalidRecord);
        }
        let id = participant_key(&report.node);
        if state
            .shutdown_manifest
            .as_ref()
            .is_none_or(|manifest| !manifest.sealed)
            || state.shutdown_obligations.get(&id) != Some(&report.node)
        {
            return Err(StorageError::CompareFailed);
        }
        if let Some(previous) = state.shutdown_reports.get(&id) {
            if previous.outcome == NodeStopOutcome::Stopped {
                return if previous == &report {
                    Ok(())
                } else {
                    Err(StorageError::CompareFailed)
                };
            }
        }
        state.shutdown_reports.insert(id, report);
        Ok(())
    }
    async fn begin_shutdown_cleanup(
        &self,
        guard: &ClusterLeaderGuard,
        operation: &ControlOperationId,
    ) -> Result<(), StorageError> {
        let mut state = self.inner.lock().expect("memory store poisoned");
        validate_leader(&state, guard)?;
        let run = closing(&state, operation, ClosingStage::Draining)?;
        let manifest = state
            .shutdown_manifest
            .as_ref()
            .filter(|m| m.sealed)
            .ok_or(StorageError::CompareFailed)?;
        let mut digest = [0; 32];
        let mut count = 0;
        for (id, node) in &state.shutdown_obligations {
            let expected = ShutdownReport {
                epoch: run.epoch,
                operation: operation.clone(),
                node: node.clone(),
                outcome: NodeStopOutcome::Stopped,
            };
            if state.shutdown_reports.get(id) != Some(&expected)
                && state.stopped_nodes.get(id) != Some(node)
            {
                return Err(StorageError::ShutdownBlocked);
            }
            digest = extend_digest(digest, id, node)?;
            count += 1;
        }
        if digest != manifest.digest || count != manifest.participant_count {
            return Err(StorageError::InvalidRecord);
        }
        state.lifecycle = Some(ClusterLifecycle {
            epoch: run.epoch,
            phase: RunPhase::Closing {
                operation: operation.clone(),
                stage: ClosingStage::Cleaning,
            },
        });
        Ok(())
    }
    async fn shutdown_cleanup_batch(
        &self,
        guard: &ClusterLeaderGuard,
        operation: &ControlOperationId,
        maximum_keys: usize,
    ) -> Result<ShutdownProgress, StorageError> {
        if maximum_keys == 0 || maximum_keys > 32 {
            return Err(StorageError::InvalidConfig);
        }
        let mut state = self.inner.lock().expect("memory store poisoned");
        if let Some(run) = &state.lifecycle {
            if run.epoch == guard.record().epoch
                && matches!(&run.phase,RunPhase::Closed{operation:active,completion:RunCompletion::Graceful}if active==operation)
            {
                return Ok(ShutdownProgress {
                    lifecycle: run.clone(),
                    manifest: None,
                    complete: true,
                });
            }
        }
        validate_leader(&state, guard)?;
        let run = closing(&state, operation, ClosingStage::Cleaning)?;
        let mut remaining = maximum_keys;
        macro_rules! clean {
            ($field:ident) => {
                while remaining > 0 && !state.$field.is_empty() {
                    state.$field.pop_first();
                    remaining -= 1;
                }
            };
        }
        clean!(members);
        clean!(group_members);
        clean!(slots);
        clean!(claims);
        clean!(plans);
        clean!(transfer_reservations);
        clean!(transfer_barriers);
        clean!(admin_operations);
        clean!(shutdown_reports);
        clean!(shutdown_obligations);
        clean!(stopped_nodes);
        let complete = remaining > 0;
        if complete {
            state.shutdown_manifest = None;
            state.leaders.clear();
            state.candidate_registrations.clear();
            state.placement_revisions.clear();
            state.lifecycle = Some(ClusterLifecycle {
                epoch: run.epoch,
                phase: RunPhase::Closed {
                    operation: operation.clone(),
                    completion: RunCompletion::Graceful,
                },
            });
        }
        Ok(ShutdownProgress {
            lifecycle: state.lifecycle.clone().unwrap(),
            manifest: state.shutdown_manifest.clone(),
            complete,
        })
    }
}
