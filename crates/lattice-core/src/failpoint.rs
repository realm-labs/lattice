#[cfg(feature = "test-failpoints")]
use std::sync::{Arc, OnceLock, RwLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Failpoint {
    AssociationAfterHandshakeBeforeCatalogue,
    ControlAfterOutboxBeforeSocketWrite,
    ControlAfterRemoteApplyBeforeAck,
    CoordinatorAfterEtcdCommitBeforeDelta,
    SnapshotAfterStageBeforeInstall,
    RebalanceAfterPlanPersist,
    RebalanceAfterReservationBeforeHandoff,
    HandoffAfterBeginPersist,
    HandoffAfterPartialBarrier,
    HandoffAfterDrainSend,
    HandoffAfterShardDrainedBeforeClaimRevoke,
    HandoffAfterNewClaimBeforeGrantSend,
    HandoffAfterGrantBeforeShardReady,
    HandoffAfterActivePersistBeforeDelta,
    WatchAfterInstallBeforeAck,
    WatchAfterTerminatedBeforeAck,
    ShutdownAfterFenceBeforeTaskJoin,
}

impl Failpoint {
    pub const ALL: [Self; 17] = [
        Self::AssociationAfterHandshakeBeforeCatalogue,
        Self::ControlAfterOutboxBeforeSocketWrite,
        Self::ControlAfterRemoteApplyBeforeAck,
        Self::CoordinatorAfterEtcdCommitBeforeDelta,
        Self::SnapshotAfterStageBeforeInstall,
        Self::RebalanceAfterPlanPersist,
        Self::RebalanceAfterReservationBeforeHandoff,
        Self::HandoffAfterBeginPersist,
        Self::HandoffAfterPartialBarrier,
        Self::HandoffAfterDrainSend,
        Self::HandoffAfterShardDrainedBeforeClaimRevoke,
        Self::HandoffAfterNewClaimBeforeGrantSend,
        Self::HandoffAfterGrantBeforeShardReady,
        Self::HandoffAfterActivePersistBeforeDelta,
        Self::WatchAfterInstallBeforeAck,
        Self::WatchAfterTerminatedBeforeAck,
        Self::ShutdownAfterFenceBeforeTaskJoin,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::AssociationAfterHandshakeBeforeCatalogue => {
                "association_after_handshake_before_catalogue"
            }
            Self::ControlAfterOutboxBeforeSocketWrite => "control_after_outbox_before_socket_write",
            Self::ControlAfterRemoteApplyBeforeAck => "control_after_remote_apply_before_ack",
            Self::CoordinatorAfterEtcdCommitBeforeDelta => {
                "coordinator_after_etcd_commit_before_delta"
            }
            Self::SnapshotAfterStageBeforeInstall => "snapshot_after_stage_before_install",
            Self::RebalanceAfterPlanPersist => "rebalance_after_plan_persist",
            Self::RebalanceAfterReservationBeforeHandoff => {
                "rebalance_after_reservation_before_handoff"
            }
            Self::HandoffAfterBeginPersist => "handoff_after_begin_persist",
            Self::HandoffAfterPartialBarrier => "handoff_after_partial_barrier",
            Self::HandoffAfterDrainSend => "handoff_after_drain_send",
            Self::HandoffAfterShardDrainedBeforeClaimRevoke => {
                "handoff_after_shard_drained_before_claim_revoke"
            }
            Self::HandoffAfterNewClaimBeforeGrantSend => {
                "handoff_after_new_claim_before_grant_send"
            }
            Self::HandoffAfterGrantBeforeShardReady => "handoff_after_grant_before_shard_ready",
            Self::HandoffAfterActivePersistBeforeDelta => {
                "handoff_after_active_persist_before_delta"
            }
            Self::WatchAfterInstallBeforeAck => "watch_after_install_before_ack",
            Self::WatchAfterTerminatedBeforeAck => "watch_after_terminated_before_ack",
            Self::ShutdownAfterFenceBeforeTaskJoin => "shutdown_after_fence_before_task_join",
        }
    }
}

#[cfg(feature = "test-failpoints")]
type Hook = Arc<dyn Fn(Failpoint) + Send + Sync>;

#[cfg(feature = "test-failpoints")]
fn hook() -> &'static RwLock<Option<Hook>> {
    static HOOK: OnceLock<RwLock<Option<Hook>>> = OnceLock::new();
    HOOK.get_or_init(|| RwLock::new(None))
}

pub fn hit(point: Failpoint) {
    #[cfg(feature = "test-failpoints")]
    if let Some(hook) = hook().read().expect("failpoint hook poisoned").clone() {
        hook(point);
    }
    #[cfg(not(feature = "test-failpoints"))]
    let _ = point;
}

#[cfg(feature = "test-failpoints")]
pub struct FailpointGuard {
    previous: Option<Hook>,
}

#[cfg(feature = "test-failpoints")]
impl Drop for FailpointGuard {
    fn drop(&mut self) {
        *hook().write().expect("failpoint hook poisoned") = self.previous.take();
    }
}

#[cfg(feature = "test-failpoints")]
pub fn install_hook(hook_fn: impl Fn(Failpoint) + Send + Sync + 'static) -> FailpointGuard {
    let mut active = hook().write().expect("failpoint hook poisoned");
    let previous = active.replace(Arc::new(hook_fn));
    FailpointGuard { previous }
}
