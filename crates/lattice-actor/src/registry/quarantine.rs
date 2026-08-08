use lattice_core::{actor_ref::ActorRef, id::ActorId};

use crate::{
    directory::ActivationDirectory,
    handle::ActorHandle,
    traits::{Actor, ActorLifecycleState, StopReason},
    watch::LocalActorRef,
};

use super::{
    ActorCellDiagnostics, ActorQuarantineError, ActorRegistry, ActorRegistryMetricsSnapshot,
    QuarantineDiagnostics, QuarantinedEntry, RegistryEntry, RetainedActorFailure, is_terminal,
};

impl<A: Actor> ActorRegistry<A> {
    pub fn retained_stop_failures(&self) -> Vec<RetainedActorFailure> {
        let mut failures = self
            .entries
            .iter()
            .filter_map(|entry| match entry.value() {
                RegistryEntry::Running(handle)
                    if handle.lifecycle_state() == ActorLifecycleState::StopFailed =>
                {
                    handle
                        .inspect_stop_failure()
                        .map(|failure| RetainedActorFailure {
                            actor_id: entry.key().clone(),
                            local_ref: handle.local_ref(),
                            failure,
                        })
                }
                RegistryEntry::Running(_) | RegistryEntry::Activating(_) => None,
            })
            .collect::<Vec<_>>();
        failures.sort_by(|left, right| left.actor_id.cmp(&right.actor_id));
        failures
    }

    pub fn quarantine_len(&self) -> usize {
        self.quarantined.len()
    }

    pub fn lifecycle_metrics(&self) -> ActorRegistryMetricsSnapshot {
        ActorRegistryMetricsSnapshot {
            retained_stop_failures: self.retained_stop_failures().len(),
            quarantine_used: self.quarantined.len(),
            quarantine_capacity: self.config.quarantine_capacity,
        }
    }

    pub fn live_cells(&self) -> Vec<ActorCellDiagnostics> {
        let mut cells = self
            .entries
            .iter()
            .filter_map(|entry| match entry.value() {
                RegistryEntry::Running(handle) if !is_terminal(handle.lifecycle_state()) => {
                    Some(ActorCellDiagnostics {
                        actor_id: entry.key().clone(),
                        local_ref: handle.local_ref(),
                        lifecycle: handle.lifecycle_state(),
                        quarantined: handle.lifecycle_state() == ActorLifecycleState::Quarantined,
                        stop_failure: handle.inspect_stop_failure(),
                    })
                }
                RegistryEntry::Running(_) | RegistryEntry::Activating(_) => None,
            })
            .chain(self.quarantined.iter().filter_map(|entry| {
                let handle = &entry.value().handle;
                (!is_terminal(handle.lifecycle_state())).then(|| ActorCellDiagnostics {
                    actor_id: entry.value().actor_id.clone(),
                    local_ref: handle.local_ref(),
                    lifecycle: handle.lifecycle_state(),
                    quarantined: true,
                    stop_failure: handle.inspect_stop_failure(),
                })
            }))
            .collect::<Vec<_>>();
        cells.sort_by_key(|cell| cell.local_ref.id());
        cells
    }

    pub fn inspect_quarantined(&self, actor_id: &ActorId) -> Option<QuarantineDiagnostics> {
        if let Some(entry) = self
            .quarantined
            .iter()
            .filter(|entry| &entry.value().actor_id == actor_id)
            .max_by_key(|entry| entry.key().id())
        {
            return self.quarantine_diagnostics(entry.value());
        }
        let handle = self.entry_handle(actor_id)?;
        (handle.lifecycle_state() == ActorLifecycleState::Quarantined)
            .then(|| self.handle_quarantine_diagnostics(actor_id.clone(), &handle))
            .flatten()
    }

    pub fn inspect_quarantined_exact(
        &self,
        local_ref: LocalActorRef,
    ) -> Option<QuarantineDiagnostics> {
        if let Some(entry) = self.quarantined.get(&local_ref) {
            return self.quarantine_diagnostics(entry.value());
        }
        self.entries.iter().find_map(|entry| match entry.value() {
            RegistryEntry::Running(handle)
                if handle.local_ref() == local_ref
                    && handle.lifecycle_state() == ActorLifecycleState::Quarantined =>
            {
                self.handle_quarantine_diagnostics(entry.key().clone(), handle)
            }
            RegistryEntry::Running(_) | RegistryEntry::Activating(_) => None,
        })
    }

    pub fn quarantined_activations(&self, actor_id: &ActorId) -> Vec<QuarantineDiagnostics> {
        let mut diagnostics = self
            .quarantined
            .iter()
            .filter(|entry| &entry.value().actor_id == actor_id)
            .filter_map(|entry| self.quarantine_diagnostics(entry.value()))
            .chain(self.entries.iter().filter_map(|entry| match entry.value() {
                RegistryEntry::Running(handle)
                    if entry.key() == actor_id
                        && handle.lifecycle_state() == ActorLifecycleState::Quarantined =>
                {
                    self.handle_quarantine_diagnostics(entry.key().clone(), handle)
                }
                RegistryEntry::Running(_) | RegistryEntry::Activating(_) => None,
            }))
            .collect::<Vec<_>>();
        diagnostics.sort_by_key(|entry| entry.local_ref.id());
        diagnostics
    }

    fn quarantine_diagnostics(&self, entry: &QuarantinedEntry<A>) -> Option<QuarantineDiagnostics> {
        self.handle_quarantine_diagnostics(entry.actor_id.clone(), &entry.handle)
    }

    fn handle_quarantine_diagnostics(
        &self,
        actor_id: ActorId,
        handle: &ActorHandle<A>,
    ) -> Option<QuarantineDiagnostics> {
        Some(QuarantineDiagnostics {
            actor_id,
            local_ref: handle.local_ref(),
            actor_ref: handle.actor_ref().map(ActorRef::erase),
            failure: handle.inspect_stop_failure()?,
        })
    }

    pub fn export_quarantine_diagnostics(&self, actor_id: &ActorId) -> Option<String> {
        self.inspect_quarantined(actor_id)
            .map(|diagnostics| format!("{diagnostics:#?}"))
    }

    pub async fn quarantine_after_authority_loss(
        &self,
        actor_id: &ActorId,
    ) -> Result<QuarantineDiagnostics, ActorQuarantineError> {
        self.fence_after_authority_loss(actor_id).await?;
        self.inspect_quarantined(actor_id)
            .ok_or(ActorQuarantineError::NotRetained)
    }

    pub async fn fence_after_authority_loss(
        &self,
        actor_id: &ActorId,
    ) -> Result<(), ActorQuarantineError> {
        let handle = self
            .entry_handle(actor_id)
            .ok_or(ActorQuarantineError::NotRetained)?;
        let capacity_exhausted = self.quarantined.len() >= self.config.quarantine_capacity;
        let previous = handle.mark_external_authority_lost();
        if !capacity_exhausted {
            self.entries.remove_if(actor_id, |_, entry| {
                matches!(entry, RegistryEntry::Running(current) if current.local_ref() == handle.local_ref())
            });
        }
        if let Some(directory) = self.config.service.extension::<ActivationDirectory>()
            && let Some(reference) = handle.actor_ref()
        {
            directory.remove(&reference.erase());
        }
        self.remove_exact(&handle);
        let local_ref = handle.local_ref();
        if !capacity_exhausted {
            self.quarantined.insert(
                local_ref,
                QuarantinedEntry {
                    actor_id: actor_id.clone(),
                    handle: handle.clone(),
                },
            );
        }
        handle
            .begin_fenced_stop(previous, StopReason::AuthorityLost)
            .map_err(ActorQuarantineError::Admin)?;
        if capacity_exhausted {
            tracing::error!(
                actor.id = ?actor_id,
                actor.local_ref = local_ref.id(),
                quarantine.capacity = self.config.quarantine_capacity,
                quarantine.used = self.quarantined.len(),
                "external authority was fully fenced but retained as a registry overflow blocker; operator intervention is mandatory"
            );
            return Err(ActorQuarantineError::Capacity {
                capacity: self.config.quarantine_capacity,
            });
        }
        Ok(())
    }

    pub async fn retry_quarantined(&self, actor_id: &ActorId) -> Result<(), ActorQuarantineError> {
        let handle = self
            .quarantined
            .iter()
            .filter(|entry| &entry.value().actor_id == actor_id)
            .max_by_key(|entry| entry.key().id())
            .map(|entry| entry.value().handle.clone())
            .or_else(|| {
                self.entry_handle(actor_id)
                    .filter(|handle| handle.lifecycle_state() == ActorLifecycleState::Quarantined)
            })
            .ok_or(ActorQuarantineError::NotRetained)?;
        handle.retry_stop().await?;
        Ok(())
    }

    pub async fn retry_quarantined_exact(
        &self,
        local_ref: LocalActorRef,
    ) -> Result<(), ActorQuarantineError> {
        let handle = self
            .local_handle(local_ref)
            .filter(|handle| handle.lifecycle_state() == ActorLifecycleState::Quarantined)
            .ok_or(ActorQuarantineError::NotRetained)?;
        handle.retry_stop().await?;
        Ok(())
    }

    pub async fn retry_stop_exact(
        &self,
        local_ref: LocalActorRef,
    ) -> Result<(), ActorQuarantineError> {
        let handle = self
            .local_handle(local_ref)
            .ok_or(ActorQuarantineError::NotRetained)?;
        handle.retry_stop().await?;
        Ok(())
    }

    pub async fn force_discard_quarantined(
        &self,
        actor_id: &ActorId,
        reason: impl Into<String>,
        ticket: impl Into<String>,
    ) -> Result<(), ActorQuarantineError> {
        let handle = self
            .quarantined
            .iter()
            .filter(|entry| &entry.value().actor_id == actor_id)
            .max_by_key(|entry| entry.key().id())
            .map(|entry| entry.value().handle.clone())
            .or_else(|| {
                self.entry_handle(actor_id)
                    .filter(|handle| handle.lifecycle_state() == ActorLifecycleState::Quarantined)
            })
            .ok_or(ActorQuarantineError::NotRetained)?;
        handle.force_stop(reason, ticket).await?;
        Ok(())
    }

    pub async fn force_discard_quarantined_exact(
        &self,
        local_ref: LocalActorRef,
        reason: impl Into<String>,
        ticket: impl Into<String>,
    ) -> Result<(), ActorQuarantineError> {
        let handle = self
            .local_handle(local_ref)
            .filter(|handle| handle.lifecycle_state() == ActorLifecycleState::Quarantined)
            .ok_or(ActorQuarantineError::NotRetained)?;
        handle.force_stop(reason, ticket).await?;
        Ok(())
    }

    pub async fn force_stop_exact(
        &self,
        local_ref: LocalActorRef,
        reason: impl Into<String>,
        ticket: impl Into<String>,
    ) -> Result<(), ActorQuarantineError> {
        let handle = self
            .local_handle(local_ref)
            .ok_or(ActorQuarantineError::NotRetained)?;
        handle.force_stop(reason, ticket).await?;
        Ok(())
    }
}
