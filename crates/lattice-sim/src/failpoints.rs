//! Complete failpoint catalogue used by deterministic simulation.

use lattice_failpoint::FailpointId;

macro_rules! catalogue {
    ($($variant:ident => $id:path),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum Failpoint {
            $($variant),+
        }

        impl Failpoint {
            pub const ALL: [Self; 27] = [$(Self::$variant),+];

            pub const fn id(self) -> FailpointId {
                match self {
                    $(Self::$variant => $id),+
                }
            }

            pub const fn name(self) -> &'static str {
                self.id().name()
            }

            pub fn from_id(id: FailpointId) -> Option<Self> {
                Self::ALL.into_iter().find(|point| point.id() == id)
            }
        }

        impl From<Failpoint> for FailpointId {
            fn from(point: Failpoint) -> Self {
                point.id()
            }
        }
    };
}

catalogue! {
    AssociationAfterHandshakeBeforeCatalogue => lattice_remoting::failpoints::ASSOCIATION_AFTER_HANDSHAKE_BEFORE_CATALOGUE,
    ControlAfterOutboxBeforeSocketWrite => lattice_remoting::failpoints::CONTROL_AFTER_OUTBOX_BEFORE_SOCKET_WRITE,
    ControlAfterRemoteApplyBeforeAck => lattice_remoting::failpoints::CONTROL_AFTER_REMOTE_APPLY_BEFORE_ACK,
    CoordinatorAfterEtcdCommitBeforeDelta => lattice_placement::failpoints::COORDINATOR_AFTER_ETCD_COMMIT_BEFORE_DELTA,
    MemberBeforeGuardedCommit => lattice_placement::failpoints::MEMBER_BEFORE_GUARDED_COMMIT,
    PlanBeforeGuardedCommit => lattice_placement::failpoints::PLAN_BEFORE_GUARDED_COMMIT,
    AuthorityBeforeGuardedCommit => lattice_placement::failpoints::AUTHORITY_BEFORE_GUARDED_COMMIT,
    AdminBeforeGuardedCommit => lattice_placement::failpoints::ADMIN_BEFORE_GUARDED_COMMIT,
    InitialAuthorityAfterCommitBeforeEffect => lattice_placement::failpoints::INITIAL_AUTHORITY_AFTER_COMMIT_BEFORE_EFFECT,
    FenceAuthorityAfterCommitBeforeEffect => lattice_placement::failpoints::FENCE_AUTHORITY_AFTER_COMMIT_BEFORE_EFFECT,
    AdminAfterCommitBeforeResponse => lattice_placement::failpoints::ADMIN_AFTER_COMMIT_BEFORE_RESPONSE,
    ReconciliationAfterCommitBeforeEffect => lattice_placement::failpoints::RECONCILIATION_AFTER_COMMIT_BEFORE_EFFECT,
    MigrationAfterCommitBeforeProgress => lattice_placement::failpoints::MIGRATION_AFTER_COMMIT_BEFORE_PROGRESS,
    MigrationBeforeFinalize => lattice_placement::failpoints::MIGRATION_BEFORE_FINALIZE,
    SnapshotAfterStageBeforeInstall => lattice_placement::failpoints::SNAPSHOT_AFTER_STAGE_BEFORE_INSTALL,
    RebalanceAfterPlanPersist => lattice_placement::failpoints::REBALANCE_AFTER_PLAN_PERSIST,
    RebalanceAfterReservationBeforeHandoff => lattice_placement::failpoints::REBALANCE_AFTER_RESERVATION_BEFORE_HANDOFF,
    HandoffAfterBeginPersist => lattice_placement::failpoints::HANDOFF_AFTER_BEGIN_PERSIST,
    HandoffAfterPartialBarrier => lattice_placement::failpoints::HANDOFF_AFTER_PARTIAL_BARRIER,
    HandoffAfterDrainSend => lattice_placement::failpoints::HANDOFF_AFTER_DRAIN_SEND,
    HandoffAfterShardDrainedBeforeClaimRevoke => lattice_placement::failpoints::HANDOFF_AFTER_SHARD_DRAINED_BEFORE_CLAIM_REVOKE,
    HandoffAfterNewClaimBeforeGrantSend => lattice_placement::failpoints::HANDOFF_AFTER_NEW_CLAIM_BEFORE_GRANT_SEND,
    HandoffAfterGrantBeforeShardReady => lattice_placement::failpoints::HANDOFF_AFTER_GRANT_BEFORE_SHARD_READY,
    HandoffAfterActivePersistBeforeDelta => lattice_placement::failpoints::HANDOFF_AFTER_ACTIVE_PERSIST_BEFORE_DELTA,
    WatchAfterInstallBeforeAck => lattice_remoting::failpoints::WATCH_AFTER_INSTALL_BEFORE_ACK,
    WatchAfterTerminatedBeforeAck => lattice_remoting::failpoints::WATCH_AFTER_TERMINATED_BEFORE_ACK,
    ShutdownAfterFenceBeforeTaskJoin => lattice_remoting::failpoints::SHUTDOWN_AFTER_FENCE_BEFORE_TASK_JOIN,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::Failpoint;

    #[test]
    fn catalogue_ids_are_unique_and_reversible() {
        let ids = Failpoint::ALL
            .into_iter()
            .map(Failpoint::id)
            .collect::<BTreeSet<_>>();

        assert_eq!(ids.len(), Failpoint::ALL.len());
        for point in Failpoint::ALL {
            assert_eq!(Failpoint::from_id(point.id()), Some(point));
        }
    }
}
