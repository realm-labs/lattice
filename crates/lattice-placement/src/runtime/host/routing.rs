use lattice_model::{cluster::CoordinatorScope, cluster::NodeIncarnation};
use lattice_remoting::{
    association::AssociationKey,
    control::{ControlDispatchError, ControlRetryReason},
};
use tokio::sync::{mpsc, oneshot};

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
                // Hello is the re-bootstrap path after a coordinator election. The session may
                // have discovered this host just before the domain term advanced, so fencing a
                // stale hello here would leave the member retrying the same stale term forever.
                // Authentication and association checks still happen in the normal dispatch
                // path; all steady-state commands remain exact-term fenced below.
                let is_bootstrap = matches!(
                    (&inbound.scope, &inbound.command),
                    (
                        CoordinatorScope::Membership,
                        PlacementControlCommand::MemberHello(_)
                    ) | (
                        CoordinatorScope::Placement(_),
                        PlacementControlCommand::PlacementDomainHello(_)
                    )
                );
                if !is_bootstrap {
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
                        match result {
                            Ok(()) => self.spawn_membership_snapshot(
                                inbound.association.clone(),
                                Some(event.completion),
                            ),
                            Err(error) => {
                                let _ = event.completion.send(Err(dispatch_error(error)));
                            }
                        }
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
                            node_id,
                            expected_incarnation,
                        },
                    ) => {
                        let result = self
                            .complete_membership_drain(
                                operation_id,
                                node_id,
                                *expected_incarnation,
                                &inbound.association,
                            )
                            .await;
                        let _ = event.completion.send(result.map_err(dispatch_error));
                    }
                    (
                        CoordinatorScope::Placement(domain),
                        PlacementControlCommand::PlacementDomainHello(_),
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
                        route_to_domain(
                            &sender,
                            PlacementControlEvent {
                                kind: PlacementControlEventKind::Command(inbound),
                                completion: event.completion,
                            },
                        );
                    }
                    (CoordinatorScope::Placement(domain), _) => {
                        if let Some(sender) = self
                            .domains
                            .get(domain)
                            .and_then(|entry| entry.sender.clone())
                        {
                            route_to_domain(
                                &sender,
                                PlacementControlEvent {
                                    kind: PlacementControlEventKind::Command(inbound),
                                    completion: event.completion,
                                },
                            );
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
                for (domain, hosted) in &self.domains {
                    if gap.is_some_and(|gap| {
                        crate::control::control_stream_id(&CoordinatorScope::Placement(
                            domain.clone(),
                        )) != gap.stream_id
                    }) {
                        continue;
                    }
                    if let Some(sender) = &hosted.sender {
                        let (completion, _) = oneshot::channel();
                        if !route_to_domain(
                            sender,
                            PlacementControlEvent {
                                kind: PlacementControlEventKind::Reconcile {
                                    association: association.clone(),
                                    gap,
                                },
                                completion,
                            },
                        ) {
                            let _ = event.completion.send(Err(ControlDispatchError::RetryLater(
                                ControlRetryReason::ConsumerBusy,
                            )));
                            return;
                        }
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
        node_id: &str,
        expected_incarnation: NodeIncarnation,
        association: &AssociationKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        if operation_id.is_empty()
            || operation_id.len() > 256
            || association.remote_incarnation != expected_incarnation
        {
            return Err(CoordinatorRuntimeError::StaleMember);
        }
        let node = crate::types::NodeKey {
            node_id: node_id.to_owned(),
            address: association.remote_address.clone(),
            incarnation: expected_incarnation,
        };
        if node.validate().is_err() {
            return Err(CoordinatorRuntimeError::StaleMember);
        }
        let membership = self
            .membership
            .as_mut()
            .ok_or(CoordinatorRuntimeError::NotLeader)?;
        if let Some(member) = self
            .store
            .get_member(node_id)
            .await?
            .filter(|member| member.node.incarnation == expected_incarnation)
        {
            if member.node != node
                || self.membership_associations.get(&expected_incarnation) != Some(association)
            {
                return Err(CoordinatorRuntimeError::StaleMember);
            }
            match member.status {
                MemberStatus::Joining => return Err(CoordinatorRuntimeError::MemberNotReady),
                MemberStatus::Up => {
                    membership.begin_leave(&member.node).await?;
                }
                MemberStatus::Leaving => {}
            }
            membership
                .remove(&member.node, MemberRemovalReason::GracefulLeave)
                .await?;
        }
        let leader = membership.leader().clone();
        // Do not let a heartbeat already queued behind the completion request re-create the
        // member while the removal event is waiting to be fanned out by the host loop.
        self.pending_member_hellos.remove(&expected_incarnation);
        self.membership_associations.remove(&expected_incarnation);
        // The removed record is also the idempotency witness after a commit whose response was
        // lost. A replacement incarnation is never removed by this request.
        if self
            .store
            .get_leader(&CoordinatorScope::Membership)
            .await?
            .as_ref()
            != Some(&leader)
        {
            return Err(CoordinatorRuntimeError::NotLeader);
        }
        let peer = self
            .associations
            .get(association)
            .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
        let payload = crate::control::encode_control_command_for_term(
            &CoordinatorScope::Membership,
            leader.term.get(),
            &PlacementControlCommand::DrainCommitted {
                operation_id: operation_id.to_owned(),
                expected_incarnation,
            },
            self.config.placement.maximum_control_payload,
        )
        .map_err(CoordinatorRuntimeError::Control)?;
        peer.admit_control_command_in_wait(
            crate::control::control_stream_id(&CoordinatorScope::Membership),
            payload,
            crate::session::CONTROL_ADMISSION_TIMEOUT,
        )
        .await?;
        if let Err(error) = self
            .fanout_global_member_removal(node, MemberRemovalReason::GracefulLeave)
            .await
        {
            tracing::warn!(
                target: "lattice.cluster.membership",
                %error,
                "graceful member removal fanout deferred to reconciliation"
            );
        }
        Ok(())
    }
}

fn route_to_domain(
    sender: &mpsc::Sender<PlacementControlEvent>,
    event: PlacementControlEvent,
) -> bool {
    match sender.try_send(event) {
        Ok(()) => true,
        Err(error) => {
            complete_route_error(error);
            false
        }
    }
}

fn complete_route_error(error: mpsc::error::TrySendError<PlacementControlEvent>) {
    let (event, error) = match error {
        mpsc::error::TrySendError::Full(event) => (
            event,
            ControlDispatchError::RetryLater(ControlRetryReason::ConsumerBusy),
        ),
        mpsc::error::TrySendError::Closed(event) => {
            (event, ControlDispatchError::consumer_closed())
        }
    };
    let _ = event.completion.send(Err(error));
}

#[cfg(test)]
mod routing_backpressure_tests {
    use lattice_model::cluster::{NodeEndpoint, NodeIncarnation};

    use super::*;
    use crate::types::NodeKey;

    fn removal_event() -> (
        PlacementControlEvent,
        oneshot::Receiver<Result<(), ControlDispatchError>>,
    ) {
        let (completion, completed) = oneshot::channel();
        (
            PlacementControlEvent {
                kind: PlacementControlEventKind::GlobalMemberRemoved {
                    node: NodeKey {
                        node_id: "node".to_owned(),
                        address: NodeEndpoint::new("127.0.0.1", 25520).unwrap(),
                        incarnation: NodeIncarnation::new(1).unwrap(),
                    },
                    reason: MemberRemovalReason::FailureDetected,
                },
                completion,
            },
            completed,
        )
    }

    #[tokio::test]
    async fn a_full_domain_mailbox_returns_retry_without_waiting_on_that_domain() {
        let (sender, _receiver) = mpsc::channel(1);
        let (first, _first_completed) = removal_event();
        sender.try_send(first).unwrap();
        let (second, second_completed) = removal_event();

        assert!(!route_to_domain(&sender, second));
        assert!(matches!(
            second_completed.await.unwrap(),
            Err(ControlDispatchError::RetryLater(
                ControlRetryReason::ConsumerBusy
            ))
        ));
    }

    #[tokio::test]
    async fn membership_commit_is_confirmed_again_after_response_loss_and_leader_replacement() {
        use crate::{
            control::decode_control_command, runtime::host::CoordinatorHostConfig,
            storage::InMemoryPlacementStore,
        };
        use lattice_model::{cluster::ClusterId, cluster::ReleaseManifest};
        use lattice_remoting::{
            association::AssociationManager, config::RemotingConfig,
            control::decode_control_envelope,
        };
        use std::{collections::BTreeSet, sync::Arc};

        let store = Arc::new(InMemoryPlacementStore::new(16, 16).unwrap());
        let local = NodeKey {
            node_id: "leader".to_owned(),
            address: NodeEndpoint::new("127.0.0.1", 34701).unwrap(),
            incarnation: NodeIncarnation::new(1).unwrap(),
        };
        let departing = NodeKey {
            node_id: "departing".to_owned(),
            address: NodeEndpoint::new("127.0.0.1", 34702).unwrap(),
            incarnation: NodeIncarnation::new(2).unwrap(),
        };
        let associations = Arc::new(
            AssociationManager::new(
                local.address.clone(),
                local.incarnation,
                RemotingConfig::default(),
            )
            .unwrap(),
        );
        let peer = associations
            .get_or_create(
                ClusterId::new("commit-replay").unwrap(),
                departing.address.clone(),
                departing.incarnation,
            )
            .unwrap();
        let mut host = CoordinatorHost::elect(
            store.clone(),
            associations.clone(),
            local.clone(),
            BTreeSet::new(),
            CoordinatorHostConfig::default(),
        )
        .await
        .unwrap();
        let hello = MemberHello {
            node: departing.clone(),
            release: ReleaseManifest::development(1),
            rollout_participant: true,
            roles: Default::default(),
            failure_domains: Default::default(),
            protocols: Vec::new(),
            remoting_capabilities: Default::default(),
        };
        host.membership
            .as_mut()
            .unwrap()
            .join(hello.clone())
            .await
            .unwrap();
        host.membership
            .as_mut()
            .unwrap()
            .mark_up(&departing)
            .await
            .unwrap();
        host.pending_member_hellos
            .insert(departing.incarnation, hello);
        host.membership_associations
            .insert(departing.incarnation, peer.key().clone());
        host.complete_membership_drain(
            "leave",
            &departing.node_id,
            departing.incarnation,
            peer.key(),
        )
        .await
        .unwrap();
        assert!(
            store
                .get_member(&departing.node_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            !host
                .pending_member_hellos
                .contains_key(&departing.incarnation)
        );
        // No response is delivered to the departing node before the leader restarts.
        host.membership.take().unwrap().shutdown().await.unwrap();
        let mut replacement = CoordinatorHost::elect(
            store.clone(),
            associations,
            local,
            BTreeSet::new(),
            CoordinatorHostConfig::default(),
        )
        .await
        .unwrap();
        replacement
            .complete_membership_drain(
                "leave",
                &departing.node_id,
                departing.incarnation,
                peer.key(),
            )
            .await
            .unwrap();
        let responses = peer.replay_control_frames().into_iter().filter_map(|frame| {
            let envelope = decode_control_envelope(&frame).ok()?;
            decode_control_command(&envelope.payload, crate::control::DEFAULT_MAX_CONTROL_PAYLOAD).ok()
        }).filter(|command| matches!(&command.command, PlacementControlCommand::DrainCommitted { operation_id, expected_incarnation } if operation_id == "leave" && *expected_incarnation == departing.incarnation)).collect::<Vec<_>>();
        assert_eq!(responses.len(), 2);
        assert!(responses[1].coordinator_term > responses[0].coordinator_term);
        replacement
            .membership
            .take()
            .unwrap()
            .shutdown()
            .await
            .unwrap();
    }
}
