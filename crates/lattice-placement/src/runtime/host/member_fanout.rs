use bytes::Bytes;
use lattice_core::coordinator::CoordinatorScope;
use lattice_remoting::{
    association::{AssociationKey, AssociationManager},
    control::ControlDispatchError,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot};

use super::{CoordinatorHost, CoordinatorRuntimeError, HostBackgroundCompletion};
use crate::{
    control::{
        PlacementControlCommand, PlacementControlEvent, PlacementControlEventKind,
        encode_control_command_for_term,
    },
    coordinator::{
        MemberChange, MemberEvent, MemberRemovalReason, MemberStatus, SnapshotRecord,
        SnapshotVersion, build_snapshot,
    },
    storage::{CoordinatorLeaseStore, MembershipStore, PlacementDomainStore, ScopedElectionStore},
    types::{MembershipVersion, NodeKey},
};

impl<S> CoordinatorHost<S>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + PlacementDomainStore,
{
    /// Removal is fanned out from the ordered membership stream so a departed member reaches every
    /// domain immediately; the periodic full scan only closes gaps a dropped event would leave.
    pub(super) async fn apply_membership_event(
        &mut self,
        event: MemberEvent,
    ) -> Result<(), CoordinatorRuntimeError> {
        let removed = match &event.change {
            MemberChange::Removed { node, reason } => Some((node.clone(), *reason)),
            MemberChange::Upsert(_) => None,
        };
        self.broadcast_membership_event(event)?;
        if let Some((node, reason)) = removed {
            if let Err(error) = self.fanout_global_member_removal(node, reason).await {
                tracing::warn!(
                    target: "lattice.cluster.membership",
                    %error,
                    "global member removal fanout deferred to reconciliation"
                );
            }
        }
        Ok(())
    }

    pub(super) fn spawn_membership_snapshot(
        &mut self,
        association_key: AssociationKey,
        completion: Option<oneshot::Sender<Result<(), ControlDispatchError>>>,
    ) {
        let Some(version) = self
            .membership
            .as_ref()
            .map(|membership| membership.version())
        else {
            if let Some(completion) = completion {
                let _ = completion.send(Err(super::helpers::dispatch_error(
                    CoordinatorRuntimeError::NotLeader,
                )));
            }
            return;
        };
        if !self
            .snapshotting_associations
            .insert(association_key.clone())
        {
            self.pending_snapshot_replays
                .insert(association_key.clone());
            if let Some(completion) = completion {
                let _ = completion.send(Ok(()));
            }
            return;
        }
        let store = self.store.clone();
        let associations = self.associations.clone();
        let config = self.config.clone();
        self.background_tasks.spawn(async move {
            let result = Self::send_membership_snapshot(
                store,
                associations,
                config,
                version,
                &association_key,
            )
            .await;
            if let Some(completion) = completion {
                let _ = completion.send(result.map_err(super::helpers::dispatch_error));
            } else if let Err(error) = result {
                tracing::warn!(
                    target: "lattice.cluster.membership",
                    %error,
                    remote = %association_key.remote_address,
                    "membership snapshot replay failed"
                );
            }
            HostBackgroundCompletion::MembershipSnapshot(association_key)
        });
    }

    async fn send_membership_snapshot(
        store: Arc<S>,
        associations: Arc<AssociationManager>,
        config: super::CoordinatorHostConfig,
        version: MembershipVersion,
        association_key: &AssociationKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        let records = store
            .list_members()
            .await?
            .into_iter()
            .map(|member| {
                Ok(SnapshotRecord {
                    key: format!("member/{}", member.node.node_id),
                    value: Bytes::from(
                        serde_json::to_vec(&member).map_err(|_| CoordinatorRuntimeError::Codec)?,
                    ),
                })
            })
            .collect::<Result<Vec<_>, CoordinatorRuntimeError>>()?;
        let (begin, chunks, end) = build_snapshot(
            SnapshotVersion::Membership(version),
            records,
            &config.placement.snapshot_limits,
        )
        .map_err(CoordinatorRuntimeError::Coordinator)?;
        let association = associations
            .get(association_key)
            .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
        let wait_timeout =
            Duration::from_millis(config.placement.snapshot_limits.staging_timeout_millis);
        for command in std::iter::once(PlacementControlCommand::SnapshotBegin(begin))
            .chain(
                chunks
                    .into_iter()
                    .map(PlacementControlCommand::SnapshotChunk),
            )
            .chain(std::iter::once(PlacementControlCommand::SnapshotEnd(end)))
        {
            let payload = encode_control_command_for_term(
                &CoordinatorScope::Membership,
                version.term.get(),
                &command,
                config.placement.maximum_control_payload,
            )
            .map_err(CoordinatorRuntimeError::Control)?;
            association
                .admit_control_command_in_wait(
                    crate::control::control_stream_id(&CoordinatorScope::Membership),
                    payload,
                    wait_timeout,
                )
                .await?;
        }
        Ok(())
    }

    fn broadcast_membership_event(
        &mut self,
        event: MemberEvent,
    ) -> Result<(), CoordinatorRuntimeError> {
        let removed = match &event.change {
            MemberChange::Removed { node, .. } => Some(node.incarnation),
            MemberChange::Upsert(_) => None,
        };
        let coordinator_term = event.version.term.get();
        let payload = encode_control_command_for_term(
            &CoordinatorScope::Membership,
            coordinator_term,
            &PlacementControlCommand::MemberDelta(event),
            self.config.placement.maximum_control_payload,
        )
        .map_err(CoordinatorRuntimeError::Control)?;
        let mut stale = Vec::new();
        let mut reconcile = Vec::new();
        for (incarnation, key) in &self.membership_associations {
            // A snapshot is one atomic logical replacement even though it is transported as
            // several reliable frames. Never interleave a delta between Begin and End; remember
            // that the in-flight snapshot became stale and cut a fresh one after it completes.
            if self.snapshotting_associations.contains(key) {
                self.pending_snapshot_replays.insert(key.clone());
                continue;
            }
            let Some(association) = self.associations.get(key) else {
                stale.push(*incarnation);
                continue;
            };
            if association
                .admit_control_command_in(
                    crate::control::control_stream_id(&CoordinatorScope::Membership),
                    payload.clone(),
                )
                .is_err()
            {
                reconcile.push(key.clone());
            }
        }
        for incarnation in stale {
            self.membership_associations.remove(&incarnation);
            self.pending_member_hellos.remove(&incarnation);
        }
        for association in reconcile {
            self.spawn_membership_snapshot(association, None);
        }
        if let Some(incarnation) = removed {
            self.membership_associations.remove(&incarnation);
            self.pending_member_hellos.remove(&incarnation);
        }
        Ok(())
    }

    pub(super) fn spawn_global_member_reconciliation(&mut self) {
        for (domain, hosted) in &self.domains {
            let Some(sender) = &hosted.sender else {
                continue;
            };
            if !self.reconciling_domains.insert(domain.clone()) {
                continue;
            }
            let store = self.store.clone();
            let domain = domain.clone();
            let completed_domain = domain.clone();
            let sender = sender.clone();
            let maximum_work = self.config.placement.maximum_reconciliation_work_per_pass;
            self.background_tasks.spawn(async move {
                let result = async {
                    let participants = store.list_domain_members(&domain).await?;
                    for participant in participants.into_iter().take(maximum_work) {
                        let globally_up = store
                            .get_member(&participant.node.node_id)
                            .await?
                            .is_some_and(|member| {
                                member.node == participant.node && member.status == MemberStatus::Up
                            });
                        if globally_up {
                            continue;
                        }
                        try_remove_global_member_from_domain(
                            &sender,
                            participant.node,
                            MemberRemovalReason::FailureDetected,
                        )?;
                    }
                    Ok::<(), CoordinatorRuntimeError>(())
                }
                .await;
                if let Err(error) = result {
                    tracing::warn!(
                        target: "lattice.cluster.membership",
                        domain = %domain.as_str(),
                        %error,
                        "global member reconciliation deferred"
                    );
                }
                HostBackgroundCompletion::DomainReconciliation(completed_domain)
            });
        }
    }

    pub(super) async fn fanout_global_member_removal(
        &self,
        node: NodeKey,
        reason: MemberRemovalReason,
    ) -> Result<(), CoordinatorRuntimeError> {
        for hosted in self.domains.values() {
            let Some(sender) = &hosted.sender else {
                continue;
            };
            try_remove_global_member_from_domain(sender, node.clone(), reason)?;
        }
        Ok(())
    }
}

fn try_remove_global_member_from_domain(
    sender: &mpsc::Sender<PlacementControlEvent>,
    node: NodeKey,
    reason: MemberRemovalReason,
) -> Result<(), CoordinatorRuntimeError> {
    let (completion, _completed) = oneshot::channel();
    sender
        .try_send(PlacementControlEvent {
            kind: PlacementControlEventKind::GlobalMemberRemoved { node, reason },
            completion,
        })
        .map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => CoordinatorRuntimeError::ControlBackpressure,
            mpsc::error::TrySendError::Closed(_) => CoordinatorRuntimeError::ControlClosed,
        })
}

#[cfg(test)]
mod snapshot_backpressure_tests {
    use std::sync::Arc;

    use lattice_core::actor_ref::{ClusterId, NodeAddress, NodeIncarnation};
    use lattice_remoting::{
        association::{Association, LaneAttachment, LaneKind},
        config::RemotingConfig,
        control::{ControlAck, decode_control_envelope},
    };

    use super::*;
    use crate::{
        control::{DEFAULT_MAX_CONTROL_PAYLOAD, decode_control_command},
        coordinator::SnapshotLimits,
        types::{CoordinatorTerm, MembershipVersion, Revision},
    };

    #[tokio::test]
    async fn snapshot_larger_than_the_outbox_resumes_at_the_next_chunk_after_each_ack() {
        let key = AssociationKey {
            cluster_id: ClusterId::new("snapshot-backpressure").unwrap(),
            local_incarnation: NodeIncarnation::new(1).unwrap(),
            remote_address: NodeAddress::new("127.0.0.1", 25520).unwrap(),
            remote_incarnation: NodeIncarnation::new(2).unwrap(),
        };
        let config = RemotingConfig {
            max_control_outbox_frames: 2,
            max_control_outbox_frames_per_stream: 2,
            ..RemotingConfig::default()
        };
        let association = Arc::new(Association::new(key.clone(), config).unwrap());
        for (lane, nonce) in [
            (LaneKind::Control, 1_u128),
            (LaneKind::Interactive, 2),
            (LaneKind::Bulk(0), 3),
        ] {
            association
                .attach(LaneAttachment {
                    association_id: association.id(),
                    key: key.clone(),
                    lane,
                    connection_nonce: nonce,
                })
                .unwrap();
        }
        let mut outbound = association.take_lane_receiver(LaneKind::Control).unwrap();
        let limits = SnapshotLimits {
            maximum_records: 32,
            maximum_bytes: 4096,
            maximum_chunks: 32,
            maximum_chunk_bytes: 64,
            staging_timeout_millis: 1000,
        };
        let version = MembershipVersion {
            term: CoordinatorTerm::new(1).unwrap(),
            revision: Revision::new(1).unwrap(),
        };
        let (begin, chunks, end) = build_snapshot(
            SnapshotVersion::Membership(version),
            (0..24)
                .map(|index| SnapshotRecord {
                    key: format!("member/{index:02}"),
                    value: Bytes::from(vec![u8::try_from(index).unwrap(); 48]),
                })
                .collect(),
            &limits,
        )
        .unwrap();
        let snapshot_id = begin.snapshot_id;
        let expected_frames = chunks.len() + 2;
        assert!(expected_frames > 2);
        let commands = std::iter::once(PlacementControlCommand::SnapshotBegin(begin))
            .chain(
                chunks
                    .into_iter()
                    .map(PlacementControlCommand::SnapshotChunk),
            )
            .chain(std::iter::once(PlacementControlCommand::SnapshotEnd(end)))
            .collect::<Vec<_>>();
        let sender = {
            let association = association.clone();
            tokio::spawn(async move {
                for command in commands {
                    let payload = encode_control_command_for_term(
                        &CoordinatorScope::Membership,
                        version.term.get(),
                        &command,
                        DEFAULT_MAX_CONTROL_PAYLOAD,
                    )
                    .unwrap();
                    association
                        .admit_control_command_in_wait(
                            crate::control::control_stream_id(&CoordinatorScope::Membership),
                            payload,
                            Duration::from_secs(1),
                        )
                        .await
                        .unwrap();
                }
            })
        };

        let mut received = Vec::new();
        for _ in 0..expected_frames {
            let envelope = decode_control_envelope(&outbound.recv().await.unwrap()).unwrap();
            let command =
                decode_control_command(&envelope.payload, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
            received.push(command.command);
            association
                .acknowledge_control(ControlAck {
                    association_epoch: association.id(),
                    stream_id: envelope.stream_id,
                    cumulative_sequence: envelope.sequence,
                })
                .unwrap();
        }
        sender.await.unwrap();

        assert!(matches!(
            received.first(),
            Some(PlacementControlCommand::SnapshotBegin(begin))
                if begin.snapshot_id == snapshot_id
        ));
        assert!(matches!(
            received.last(),
            Some(PlacementControlCommand::SnapshotEnd(end))
                if end.snapshot_id == snapshot_id
        ));
        let indexes = received
            .iter()
            .filter_map(|command| match command {
                PlacementControlCommand::SnapshotChunk(chunk) => Some(chunk.index),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(indexes, (0..indexes.len()).collect::<Vec<_>>());
    }
}
