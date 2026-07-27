use lattice_core::{actor_ref::NodeIncarnation, coordinator::CoordinatorScope};
use lattice_remoting::{
    association::AssociationKey,
    control::{ControlDispatchError, ControlRetryReason},
};
use tokio::sync::oneshot;

use super::{
    CoordinatorHost, CoordinatorHostScopeState, CoordinatorRuntimeError, helpers::dispatch_error,
};
use crate::{
    control::{PlacementControlCommand, PlacementControlEvent, PlacementControlEventKind},
    coordinator::{MemberHello, MemberRemovalReason, MemberStatus},
    storage::{CoordinatorLeaseStore, MembershipStore, PlacementDomainStore, ScopedElectionStore},
    types::MembershipVersion,
};

impl<S> CoordinatorHost<S>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + PlacementDomainStore,
{
    pub(super) async fn route_control(&mut self, event: PlacementControlEvent) {
        match event.kind {
            PlacementControlEventKind::Command(inbound) => {
                match (inbound.coordinator_term, self.active_term(&inbound.scope)) {
                    (Some(received_term), Some(expected_term))
                        if expected_term == received_term => {}
                    (Some(_), Some(_)) | (None, _) => {
                        let _ = event
                            .completion
                            .send(Err(ControlDispatchError::InvalidCommand));
                        return;
                    }
                    (Some(_), None) => {
                        // This host no longer owns the scope. Retrying an old-term command on
                        // this association can permanently head-of-line block commands for
                        // other scopes multiplexed over the same control lane. Fence and
                        // acknowledge it; discovery/session reconciliation will target the
                        // current leader and send a fresh hello under its term.
                        let _ = event
                            .completion
                            .send(Err(ControlDispatchError::InvalidCommand));
                        return;
                    }
                }
                match (&inbound.scope, &inbound.command) {
                    (CoordinatorScope::Membership, PlacementControlCommand::MemberHello(hello)) => {
                        let result = self.admit_member(hello.clone()).await;
                        if result.is_ok() {
                            self.pending_member_hellos
                                .insert(inbound.association.remote_incarnation, hello.clone());
                            self.membership_associations.insert(
                                inbound.association.remote_incarnation,
                                inbound.association.clone(),
                            );
                        }
                        let result = match result {
                            Ok(()) => self.send_membership_snapshot(&inbound.association).await,
                            Err(error) => Err(error),
                        };
                        let _ = event.completion.send(result.map_err(dispatch_error));
                    }
                    (
                        CoordinatorScope::Membership,
                        PlacementControlCommand::NodeHeartbeat {
                            incarnation,
                            sequence,
                        },
                    ) => {
                        let result = if *incarnation != inbound.association.remote_incarnation
                            || *sequence == 0
                        {
                            Err(CoordinatorRuntimeError::UnauthorizedCommand)
                        } else if let Some(hello) =
                            self.pending_member_hellos.get(incarnation).cloned()
                        {
                            self.admit_member(hello).await
                        } else {
                            Err(CoordinatorRuntimeError::UnknownSession)
                        };
                        let _ = event.completion.send(result.map_err(dispatch_error));
                    }
                    (
                        CoordinatorScope::Membership,
                        PlacementControlCommand::JoinReady { snapshot_version },
                    ) => {
                        let result = self
                            .complete_member_join(
                                inbound.association.remote_incarnation,
                                *snapshot_version,
                                &inbound.association,
                            )
                            .await;
                        let _ = event.completion.send(result.map_err(dispatch_error));
                    }
                    (
                        CoordinatorScope::Membership,
                        PlacementControlCommand::MembershipDrainComplete {
                            operation_id,
                            expected_incarnation,
                        },
                    ) => {
                        let result = self
                            .complete_membership_drain(
                                operation_id,
                                *expected_incarnation,
                                &inbound.association,
                            )
                            .await;
                        let _ = event.completion.send(result.map_err(dispatch_error));
                    }
                    (
                        CoordinatorScope::Placement(domain),
                        PlacementControlCommand::PlacementDomainHello(hello),
                    ) => {
                        let Some(hosted) = self.domains.get(domain) else {
                            let _ = event.completion.send(Err(ControlDispatchError::RetryLater(
                                ControlRetryReason::AssociationStarting,
                            )));
                            return;
                        };
                        let Some(sender) = hosted.sender.clone() else {
                            let _ = event.completion.send(Err(ControlDispatchError::RetryLater(
                                ControlRetryReason::AssociationStarting,
                            )));
                            return;
                        };
                        let member_is_up = self
                            .store
                            .get_member(&hello.node.node_id)
                            .await
                            .ok()
                            .flatten()
                            .filter(|member| {
                                member.node == hello.node
                                    && member.status == MemberStatus::Up
                                    && hello.node.incarnation
                                        == inbound.association.remote_incarnation
                                    && hello.node.address == inbound.association.remote_address
                            })
                            .is_some();
                        if !member_is_up {
                            let _ = event
                                .completion
                                .send(Err(ControlDispatchError::InvalidCommand));
                            return;
                        }
                        if sender
                            .send(PlacementControlEvent {
                                kind: PlacementControlEventKind::Command(inbound),
                                completion: event.completion,
                            })
                            .await
                            .is_err()
                        {
                            // The original completion is dropped on a closed queue and the
                            // remoting caller observes Unavailable.
                        }
                    }
                    (CoordinatorScope::Placement(domain), _) => {
                        if let Some(sender) = self
                            .domains
                            .get(domain)
                            .and_then(|entry| entry.sender.clone())
                        {
                            let _ = sender
                                .send(PlacementControlEvent {
                                    kind: PlacementControlEventKind::Command(inbound),
                                    completion: event.completion,
                                })
                                .await;
                        } else {
                            let error = ControlDispatchError::RetryLater(
                                ControlRetryReason::AssociationStarting,
                            );
                            let _ = event.completion.send(Err(error));
                        }
                    }
                    _ => {
                        let _ = event
                            .completion
                            .send(Err(ControlDispatchError::InvalidCommand));
                    }
                }
            }
            PlacementControlEventKind::Reconcile { association, gap } => {
                for hosted in self.domains.values() {
                    if let Some(sender) = &hosted.sender {
                        let (completion, _) = oneshot::channel();
                        let _ = sender
                            .send(PlacementControlEvent {
                                kind: PlacementControlEventKind::Reconcile {
                                    association: association.clone(),
                                    gap,
                                },
                                completion,
                            })
                            .await;
                    }
                }
                let _ = event.completion.send(Ok(()));
            }
            PlacementControlEventKind::GlobalMemberRemoved { .. } => {
                let _ = event
                    .completion
                    .send(Err(ControlDispatchError::InvalidCommand));
            }
        }
    }

    async fn admit_member(&mut self, hello: MemberHello) -> Result<(), CoordinatorRuntimeError> {
        if let Some(membership) = self.membership.as_mut() {
            let member = membership.join(hello).await?;
            match member.status {
                MemberStatus::Joining | MemberStatus::Up => {}
                MemberStatus::Leaving => return Err(CoordinatorRuntimeError::StaleMember),
            }
            return Ok(());
        }
        let current = self
            .store
            .get_member(&hello.node.node_id)
            .await?
            .filter(|member| {
                member.node == hello.node
                    && member.hello == hello
                    && member.status == MemberStatus::Up
            })
            .ok_or(CoordinatorRuntimeError::NotLeader)?;
        self.store.keep_lease_alive(current.lease_id).await?;
        Ok(())
    }

    pub(super) fn active_term(&self, scope: &CoordinatorScope) -> Option<u64> {
        let state = match scope {
            CoordinatorScope::Membership => &self.membership_state,
            CoordinatorScope::Placement(domain) => &self.domains.get(domain)?.state,
        };
        match state {
            CoordinatorHostScopeState::Active(leader) => Some(leader.term.get()),
            CoordinatorHostScopeState::Standby | CoordinatorHostScopeState::Failed => None,
        }
    }

    async fn complete_member_join(
        &mut self,
        incarnation: NodeIncarnation,
        snapshot_version: MembershipVersion,
        association: &AssociationKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        if association.remote_incarnation != incarnation
            || self.membership_associations.get(&incarnation) != Some(association)
        {
            return Err(CoordinatorRuntimeError::StaleMember);
        }
        let hello = self
            .pending_member_hellos
            .get(&incarnation)
            .cloned()
            .filter(|hello| {
                hello.node.incarnation == incarnation
                    && hello.node.address == association.remote_address
            })
            .ok_or(CoordinatorRuntimeError::StaleMember)?;
        let membership = self
            .membership
            .as_mut()
            .ok_or(CoordinatorRuntimeError::NotLeader)?;
        if !membership.version().satisfies(snapshot_version) {
            return Err(CoordinatorRuntimeError::StaleMember);
        }
        let member = self
            .store
            .get_member(&hello.node.node_id)
            .await?
            .filter(|member| member.node == hello.node && member.hello == hello)
            .ok_or(CoordinatorRuntimeError::StaleMember)?;
        match member.status {
            MemberStatus::Joining => {
                membership.mark_up(&member.node).await?;
            }
            MemberStatus::Up => {}
            MemberStatus::Leaving => return Err(CoordinatorRuntimeError::StaleMember),
        }
        Ok(())
    }

    async fn complete_membership_drain(
        &mut self,
        operation_id: &str,
        expected_incarnation: NodeIncarnation,
        association: &AssociationKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        if operation_id.is_empty()
            || operation_id.len() > 256
            || association.remote_incarnation != expected_incarnation
        {
            return Err(CoordinatorRuntimeError::StaleMember);
        }
        let hello = self
            .pending_member_hellos
            .get(&expected_incarnation)
            .cloned()
            .ok_or(CoordinatorRuntimeError::StaleMember)?;
        if self.membership_associations.get(&expected_incarnation) != Some(association) {
            return Err(CoordinatorRuntimeError::StaleMember);
        }
        let membership = self
            .membership
            .as_mut()
            .ok_or(CoordinatorRuntimeError::NotLeader)?;
        let member = self
            .store
            .get_member(&hello.node.node_id)
            .await?
            .filter(|member| {
                member.node == hello.node && member.node.incarnation == expected_incarnation
            })
            .ok_or(CoordinatorRuntimeError::StaleMember)?;
        match member.status {
            MemberStatus::Joining => return Err(CoordinatorRuntimeError::StaleMember),
            MemberStatus::Up => {
                membership.begin_leave(&member.node).await?;
            }
            MemberStatus::Leaving => {}
        }
        let removed = membership
            .remove(&member.node, MemberRemovalReason::GracefulLeave)
            .await?;
        self.fanout_global_member_removal(removed.node, MemberRemovalReason::GracefulLeave)
            .await?;
        Ok(())
    }
}
