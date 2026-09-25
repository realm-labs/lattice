use lattice_model::cluster::CoordinatorScope;
use lattice_remoting::association::AssociationKey;

use super::{CoordinatorHost, CoordinatorHostScopeState, CoordinatorRuntimeError};
use crate::{
    administration::{AdminAction, AdminOperationError, change_candidate},
    candidates::{CandidateChange, CandidateRegistration},
    control::{PlacementControlCommand, control_stream_id, encode_control_command},
    storage::{ActorGroupStore, MembershipStore, ScopedElectionStore},
};

impl<S> CoordinatorHost<S>
where
    S: ScopedElectionStore + MembershipStore + ActorGroupStore,
{
    pub(super) async fn change_remote_candidate(
        &self,
        key: &AssociationKey,
        request: CandidateChange,
    ) -> Result<(), CoordinatorRuntimeError> {
        let association = self
            .associations
            .get(key)
            .ok_or(CoordinatorRuntimeError::UnauthorizedCommand)?;
        let authorization = self
            .config
            .administrators
            .as_ref()
            .ok_or(CoordinatorRuntimeError::UnauthorizedCommand)?
            .authorize(
                &association,
                AdminAction::ManageCandidates(request.scope.clone()),
            )
            .map_err(|_| CoordinatorRuntimeError::UnauthorizedCommand)?;
        let scope = request.scope.clone();
        let operation = request.operation.clone();
        let result = change_candidate(self.store.as_ref(), authorization, request)
            .await
            .map_err(|error| match error {
                AdminOperationError::Storage(error) => CoordinatorRuntimeError::Storage(error),
                AdminOperationError::Authorization(_) => {
                    CoordinatorRuntimeError::UnauthorizedCommand
                }
            })?;
        let payload = encode_control_command(
            &scope,
            &PlacementControlCommand::CandidateChanged { operation, result },
            self.config.group.maximum_control_payload,
        )
        .map_err(CoordinatorRuntimeError::Control)?;
        association.admit_control_command_in(control_stream_id(&scope), payload)?;
        Ok(())
    }

    /// Capability is configured locally; eligibility is administered durably.
    /// Removing eligibility does not remove capability, so a later explicit
    /// promotion can resume campaigning without restarting the process.
    pub(super) async fn refresh_candidate_registrations(
        &mut self,
    ) -> Result<(), CoordinatorRuntimeError> {
        let epoch = self.store.ensure_framework().await?;
        let running = self.store.lifecycle().await?.is_running();
        let scopes = std::iter::once(CoordinatorScope::Cluster)
            .chain(
                self.groups
                    .keys()
                    .filter(|_| running)
                    .cloned()
                    .map(CoordinatorScope::Group),
            )
            .collect::<Vec<_>>();
        for scope in scopes {
            let authorization = self
                .store
                .candidate_authorization(&scope, &self.node.node_id)
                .await?;
            let previous = self.candidate_registrations.get(&scope).cloned();
            if let Some(previous) = previous {
                if authorization.as_ref() == Some(&previous.authorization)
                    && self
                        .store
                        .online_candidates(&scope)
                        .await?
                        .contains(&previous)
                    && self.store.keep_lease_alive(previous.lease_id).await.is_ok()
                {
                    continue;
                }
                self.candidate_registrations.remove(&scope);
                // Revocation is already enforced in every storage commit. This
                // local step stops advertising and relinquishes promptly.
                match &scope {
                    CoordinatorScope::Cluster => {
                        if let Some(leader) = self.membership.take() {
                            let _ = leader.shutdown().await;
                        }
                        self.membership_events = None;
                        self.membership_state = CoordinatorHostScopeState::Standby;
                    }
                    CoordinatorScope::Group(group) => {
                        if let Some(hosted) = self.groups.get_mut(group) {
                            if let Some(stop) = hosted.shutdown.take() {
                                let _ = stop.send(true);
                            }
                            hosted.handle = None;
                            hosted.state = CoordinatorHostScopeState::Standby;
                        }
                    }
                }
                let _ = self.store.unregister_candidate(&previous).await;
                let _ = self.store.revoke_lease(previous.lease_id).await;
                self.publish_directory();
            }
            let Some(authorization) = authorization else {
                continue;
            };
            let lease_id = self
                .store
                .grant_lease(self.config.cluster.leader_lease_ttl)
                .await?;
            let registration = CandidateRegistration {
                epoch,
                authorization,
                node: self.node.clone(),
                lease_id,
            };
            if let Err(error) = self.store.register_candidate(&registration).await {
                let _ = self.store.revoke_lease(lease_id).await;
                return Err(error.into());
            }
            self.candidate_registrations.insert(scope, registration);
        }
        Ok(())
    }
}
