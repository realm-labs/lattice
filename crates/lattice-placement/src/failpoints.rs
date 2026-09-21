//! Failpoint IDs owned by the placement subsystem.

use lattice_failpoint::FailpointId;

macro_rules! failpoint {
    ($name:ident, $value:literal) => {
        pub const $name: FailpointId = FailpointId::new($value);
    };
}

failpoint!(
    COORDINATOR_AFTER_ETCD_COMMIT_BEFORE_DELTA,
    "coordinator_after_etcd_commit_before_delta"
);
failpoint!(MEMBER_BEFORE_GUARDED_COMMIT, "member_before_guarded_commit");
failpoint!(PLAN_BEFORE_GUARDED_COMMIT, "plan_before_guarded_commit");
failpoint!(
    AUTHORITY_BEFORE_GUARDED_COMMIT,
    "authority_before_guarded_commit"
);
failpoint!(ADMIN_BEFORE_GUARDED_COMMIT, "admin_before_guarded_commit");
failpoint!(
    INITIAL_AUTHORITY_AFTER_COMMIT_BEFORE_EFFECT,
    "initial_authority_after_commit_before_effect"
);
failpoint!(
    FENCE_AUTHORITY_AFTER_COMMIT_BEFORE_EFFECT,
    "fence_authority_after_commit_before_effect"
);
failpoint!(
    ADMIN_AFTER_COMMIT_BEFORE_RESPONSE,
    "admin_after_commit_before_response"
);
failpoint!(
    RECONCILIATION_AFTER_COMMIT_BEFORE_EFFECT,
    "reconciliation_after_commit_before_effect"
);
failpoint!(
    MIGRATION_AFTER_COMMIT_BEFORE_PROGRESS,
    "migration_after_commit_before_progress"
);
failpoint!(MIGRATION_BEFORE_FINALIZE, "migration_before_finalize");
failpoint!(
    SNAPSHOT_AFTER_STAGE_BEFORE_INSTALL,
    "snapshot_after_stage_before_install"
);
failpoint!(REBALANCE_AFTER_PLAN_PERSIST, "rebalance_after_plan_persist");
failpoint!(
    REBALANCE_AFTER_RESERVATION_BEFORE_HANDOFF,
    "rebalance_after_reservation_before_handoff"
);
failpoint!(HANDOFF_AFTER_BEGIN_PERSIST, "handoff_after_begin_persist");
failpoint!(
    HANDOFF_AFTER_PARTIAL_BARRIER,
    "handoff_after_partial_barrier"
);
failpoint!(HANDOFF_AFTER_DRAIN_SEND, "handoff_after_drain_send");
failpoint!(
    HANDOFF_AFTER_SHARD_DRAINED_BEFORE_CLAIM_REVOKE,
    "handoff_after_shard_drained_before_claim_revoke"
);
failpoint!(
    HANDOFF_AFTER_NEW_CLAIM_BEFORE_GRANT_SEND,
    "handoff_after_new_claim_before_grant_send"
);
failpoint!(
    HANDOFF_AFTER_GRANT_BEFORE_SHARD_READY,
    "handoff_after_grant_before_shard_ready"
);
failpoint!(
    HANDOFF_AFTER_ACTIVE_PERSIST_BEFORE_DELTA,
    "handoff_after_active_persist_before_delta"
);
