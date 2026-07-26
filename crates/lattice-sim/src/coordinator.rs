//! Fault injection against a real placement domain leader.
//!
//! [`Scenario`](crate::scenario::Scenario) drives production reducers under a simulated executor,
//! which can only prove that a boundary exists, never that the coordinator honours a decision at
//! it. This harness closes that gap: it installs the injector over a genuine
//! `PlacementDomainLeader` — durable store, association manager, member sessions and all — and
//! walks it through allocation, handoff, reconciliation and admin work so an armed failpoint has
//! to change what the leader persists or sends.

use lattice_core::failpoint::Failpoint;
use lattice_placement::runtime::{CoordinatorRuntimeError, harness::DomainHarness};

use crate::fault::{
    FailAction, FaultEvidence, FaultOrigin, FaultOutcome, FaultTarget, InstalledFaultInjector,
    SharedFaultInjector,
};

pub const HARNESS_SHARD: u32 = 1;
pub const SUCCESSOR_TERM: u64 = 2;

const RELOCATION_OPERATION: &str = "harness-relocate";
const PAUSE_OPERATION: &str = "harness-pause";
const HARNESS_DOMAIN: &str = "sim-leader";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaderStep {
    AllocateShard,
    CompleteReady,
    Relocate,
    ApplyBarrier,
    SourceDrained,
    TargetReady,
    PauseAutomatic,
    Reelect,
}

pub struct LeaderHarness {
    leader: DomainHarness,
    faults: SharedFaultInjector,
    plan_id: Option<u128>,
    /// Field order is load bearing: the hook has to outlive every leader operation this harness
    /// performs, so it is declared last and therefore dropped last.
    #[allow(dead_code)]
    installed: InstalledFaultInjector,
}

impl LeaderHarness {
    pub async fn start(
        cluster: &str,
        port_base: u16,
        incarnation_base: u128,
    ) -> Result<Self, CoordinatorRuntimeError> {
        let faults = SharedFaultInjector::default();
        let installed = faults.install();
        let leader =
            DomainHarness::start(cluster, HARNESS_DOMAIN, port_base, incarnation_base).await?;
        Ok(Self {
            leader,
            faults,
            plan_id: None,
            installed,
        })
    }

    pub fn arm(&self, point: Failpoint, action: FailAction) {
        self.faults.arm(point, action);
    }

    pub fn leader(&self) -> &DomainHarness {
        &self.leader
    }

    pub fn leader_mut(&mut self) -> &mut DomainHarness {
        &mut self.leader
    }

    pub fn plan_id(&self) -> Option<u128> {
        self.plan_id
    }

    pub async fn advance(&mut self, steps: &[LeaderStep]) -> Result<(), CoordinatorRuntimeError> {
        for step in steps {
            self.step(*step).await?;
        }
        Ok(())
    }

    pub async fn step(&mut self, step: LeaderStep) -> Result<(), CoordinatorRuntimeError> {
        match step {
            LeaderStep::AllocateShard => self.leader.allocate_shard(HARNESS_SHARD).await,
            LeaderStep::CompleteReady => self.leader.complete_ready(HARNESS_SHARD).await,
            LeaderStep::Relocate => {
                self.plan_id = Some(
                    self.leader
                        .relocate(RELOCATION_OPERATION, HARNESS_SHARD)
                        .await?,
                );
                Ok(())
            }
            LeaderStep::ApplyBarrier => self.leader.apply_barrier(HARNESS_SHARD).await,
            LeaderStep::SourceDrained => self.leader.source_drained(HARNESS_SHARD).await,
            LeaderStep::TargetReady => self.leader.target_ready(HARNESS_SHARD).await,
            LeaderStep::PauseAutomatic => {
                self.leader
                    .set_automatic_paused(PAUSE_OPERATION, true)
                    .await
            }
            LeaderStep::Reelect => self.leader.reelect(SUCCESSOR_TERM).await,
        }
    }

    /// Records one evidence row per target the assertions have just proven. The injector refuses
    /// any row whose action never fired, so a boundary that silently ignored its decision cannot
    /// contribute coverage.
    pub fn record(
        &self,
        point: Failpoint,
        action: FailAction,
        targets: &[FaultTarget],
    ) -> Vec<FaultEvidence> {
        let outcome = outcome_of(action);
        targets
            .iter()
            .map(|target| FaultEvidence {
                point,
                target: *target,
                action,
                outcome,
                origin: FaultOrigin::ProductionCallSite,
            })
            .filter(|evidence| self.faults.record(*evidence))
            .collect()
    }

    pub fn observed(&self, point: Failpoint) -> bool {
        self.faults.observed(point)
    }

    pub fn evidence(&self) -> Vec<FaultEvidence> {
        self.faults.evidence()
    }
}

fn outcome_of(action: FailAction) -> FaultOutcome {
    match action {
        FailAction::Crash => FaultOutcome::ProcessCrashed,
        FailAction::Pause => FaultOutcome::ProcessPaused,
        FailAction::Drop => FaultOutcome::MessageLost,
        FailAction::Duplicate => FaultOutcome::MessageDuplicated,
        FailAction::Continue | FailAction::StoreFailure => FaultOutcome::CommitRejected,
    }
}
