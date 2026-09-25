use lattice_remoting::association::AssociationKey;

use super::{
    ActorGroupStore, CoordinatorLeaseStore, CoordinatorRuntimeError, GroupCoordinator,
    MembershipStore, ScopedElectionStore,
};
use crate::{
    authority::clock::{AuthorityClock, MAXIMUM_GRANT_INTERVAL},
    control::{PlacementControlCommand, encode_control_command_for_term},
    storage::{
        StorageError,
        records::{AdoptAuthority, LeasedClaim},
    },
    types::{AssignmentGeneration, PlacementSlotKey, PlacementSlotState},
};

impl<S> GroupCoordinator<S>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + ActorGroupStore,
{
    /// Called only after observing the durable Fenced state, which prevents any
    /// subsequent old-generation issuance commit. No process-local timestamp is
    /// persisted: recovery begins another full interval.
    pub(super) fn replacement_wait_complete(
        &mut self,
        key: &PlacementSlotKey,
        generation: AssignmentGeneration,
    ) -> bool {
        let Some(clock) = AuthorityClock::new() else {
            return false;
        };
        let entry = self
            .reconciliation
            .replacement_waits
            .entry(key.clone())
            .or_insert_with(|| (generation, clock, clock.now()));
        if entry.0 != generation {
            *entry = (generation, clock, clock.now());
        }
        entry.1.elapsed_since(entry.2).is_some_and(|elapsed| {
            elapsed >= AuthorityClock::takeover_interval(MAXIMUM_GRANT_INTERVAL)
        })
    }

    /// A lease keepalive never directly creates serving permission. The exact claim,
    /// owner, session/member state and leader authorization are compared in the
    /// issuance transaction; only its committed result may answer this request.
    pub(super) async fn issue_requested_claim(
        &mut self,
        association: &AssociationKey,
        request_id: u128,
        key: PlacementSlotKey,
        generation: AssignmentGeneration,
    ) -> Result<(), CoordinatorRuntimeError> {
        if request_id == 0 || key.group() != &self.version.group {
            return Err(CoordinatorRuntimeError::UnauthorizedCommand);
        }
        let session = self
            .sessions
            .get(&association.remote_incarnation)
            .filter(|session| &session.association == association)
            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
        let owner = session.hello.node.clone();
        let Some(slot) = self.store.get_slot(&key).await? else {
            return Ok(());
        };
        let Some(previous) = self.store.get_claim(&key).await? else {
            return Ok(());
        };
        if slot.owner.as_ref() != Some(&owner)
            || matches!(
                slot.state,
                PlacementSlotState::Fenced | PlacementSlotState::Unallocated
            )
            || slot.assignment_generation != generation
            || previous.grant.owner != owner
            || previous.grant.assignment_generation != generation
            || previous.grant.coordinator_term != self.leader.term
        {
            return Ok(());
        }
        let clock = AuthorityClock::new().ok_or(StorageError::InvalidConfig)?;
        let started = clock.now();
        self.store.keep_lease_alive(previous.lease_id).await?;
        let ttl = self
            .store
            .lease_time_to_live(previous.lease_id)
            .await?
            .ok_or(StorageError::CompareFailed)?;
        let elapsed = clock
            .elapsed_since(started)
            .ok_or(StorageError::InvalidConfig)?;
        let usable = AuthorityClock::usable_duration(
            ttl.min(self.config.claim_ttl)
                .min(MAXIMUM_GRANT_INTERVAL)
                .saturating_sub(elapsed),
        );
        if usable.is_zero() {
            return Ok(());
        }
        let (expected_global_member, expected_group_member) =
            self.assignment_members(&owner).await?;
        let mut grant = previous.grant.clone();
        grant.request_id = request_id;
        grant.ttl = usable;
        grant.grant_sequence = grant
            .grant_sequence
            .next()
            .map_err(|_| CoordinatorRuntimeError::ClaimSequence)?;
        let committed = self
            .store
            .adopt_authority(
                &self.leader_guard,
                AdoptAuthority {
                    expected_global_member,
                    expected_group_member,
                    expected_slot: slot,
                    expected_claim: previous.grant,
                    claim: LeasedClaim {
                        grant,
                        lease_id: previous.lease_id,
                    },
                },
            )
            .await?;
        self.remember_claim(committed.lease_id, committed.grant.clone());
        let association = self
            .associations
            .get(association)
            .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
        let scope = self.leader.scope.clone();
        let payload = encode_control_command_for_term(
            &scope,
            self.leader.term.get(),
            &PlacementControlCommand::ClaimGranted(committed.grant),
            self.config.maximum_control_payload,
        )
        .map_err(CoordinatorRuntimeError::Control)?;
        // Ephemeral delivery: only a fresh owner request can retry. Outbox replay cannot
        // turn a previously issued duration into a new receipt-relative lease.
        association.admit_ephemeral_control(payload)?;
        Ok(())
    }
}
