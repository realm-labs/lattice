use bytes::Bytes;
use lattice_core::coordinator::CoordinatorScope;
use lattice_remoting::association::AssociationKey;
use tokio::sync::{mpsc, oneshot};

use super::{CoordinatorHost, CoordinatorRuntimeError};
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
    types::NodeKey,
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
            self.fanout_global_member_removal(node, reason).await?;
        }
        Ok(())
    }

    pub(super) async fn send_membership_snapshot(
        &self,
        association_key: &AssociationKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        let membership = self
            .membership
            .as_ref()
            .ok_or(CoordinatorRuntimeError::NotLeader)?;
        let records = self
            .store
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
            SnapshotVersion::Membership(membership.version()),
            records,
            &self.config.placement.snapshot_limits,
        )
        .map_err(CoordinatorRuntimeError::Coordinator)?;
        let association = self
            .associations
            .get(association_key)
            .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
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
                membership.version().term.get(),
                &command,
                self.config.placement.maximum_control_payload,
            )
            .map_err(CoordinatorRuntimeError::Control)?;
            association.admit_control_command(payload)?;
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
        for (incarnation, key) in &self.membership_associations {
            let Some(association) = self.associations.get(key) else {
                stale.push(*incarnation);
                continue;
            };
            if association.admit_control_command(payload.clone()).is_err() {
                stale.push(*incarnation);
            }
        }
        for incarnation in stale {
            self.membership_associations.remove(&incarnation);
            self.pending_member_hellos.remove(&incarnation);
        }
        if let Some(incarnation) = removed {
            self.membership_associations.remove(&incarnation);
            self.pending_member_hellos.remove(&incarnation);
        }
        Ok(())
    }

    pub(super) async fn fanout_global_member_removals(
        &self,
    ) -> Result<(), CoordinatorRuntimeError> {
        for (domain, hosted) in &self.domains {
            let Some(sender) = &hosted.sender else {
                continue;
            };
            let participants = self.store.list_domain_members(domain).await?;
            for participant in participants
                .into_iter()
                .take(self.config.placement.maximum_reconciliation_work_per_pass)
            {
                let globally_up = self
                    .store
                    .get_member(&participant.node.node_id)
                    .await?
                    .is_some_and(|member| {
                        member.node == participant.node && member.status == MemberStatus::Up
                    });
                if globally_up {
                    continue;
                }
                self.remove_global_member_from_domain(
                    sender,
                    participant.node,
                    MemberRemovalReason::FailureDetected,
                )
                .await?;
            }
        }
        Ok(())
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
            self.remove_global_member_from_domain(sender, node.clone(), reason)
                .await?;
        }
        Ok(())
    }

    async fn remove_global_member_from_domain(
        &self,
        sender: &mpsc::Sender<PlacementControlEvent>,
        node: NodeKey,
        reason: MemberRemovalReason,
    ) -> Result<(), CoordinatorRuntimeError> {
        let (completion, completed) = oneshot::channel();
        sender
            .send(PlacementControlEvent {
                kind: PlacementControlEventKind::GlobalMemberRemoved { node, reason },
                completion,
            })
            .await
            .map_err(|_| CoordinatorRuntimeError::ControlClosed)?;
        completed
            .await
            .map_err(|_| CoordinatorRuntimeError::ControlClosed)?
            .map_err(|_| CoordinatorRuntimeError::ControlClosed)
    }
}
