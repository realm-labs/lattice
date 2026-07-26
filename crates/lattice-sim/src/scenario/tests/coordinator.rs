//! Fault injection against the production placement domain leader.
//!
//! Every case here arms one failpoint, drives the real leader through the operation that reaches
//! it, and asserts the divergence the decision was supposed to cause — a refused commit, durable
//! truth the leader no longer tracks, or a control command that never reached a member lane. Only
//! then is evidence recorded, so the matrix cannot count a boundary the coordinator ignored.

use lattice_core::failpoint::Failpoint;
use lattice_placement::{
    control::PlacementControlCommand,
    handoff::HandoffPhase,
    plan::PlanStatus,
    runtime::CoordinatorRuntimeError,
    types::{PlacementSlotState, PlacementVersion},
};

use crate::{
    coordinator::{HARNESS_SHARD, LeaderHarness, LeaderStep, SUCCESSOR_TERM},
    fault::{FailAction, FaultEvidence, FaultTarget},
};

const RUNNING: &[LeaderStep] = &[LeaderStep::AllocateShard, LeaderStep::CompleteReady];
const RESERVED: &[LeaderStep] = &[
    LeaderStep::AllocateShard,
    LeaderStep::CompleteReady,
    LeaderStep::Relocate,
];
const DRAINED: &[LeaderStep] = &[
    LeaderStep::AllocateShard,
    LeaderStep::CompleteReady,
    LeaderStep::Relocate,
    LeaderStep::ApplyBarrier,
];
const REPLACED: &[LeaderStep] = &[
    LeaderStep::AllocateShard,
    LeaderStep::CompleteReady,
    LeaderStep::Relocate,
    LeaderStep::ApplyBarrier,
    LeaderStep::SourceDrained,
];
const COMPLETED: &[LeaderStep] = &[
    LeaderStep::AllocateShard,
    LeaderStep::CompleteReady,
    LeaderStep::Relocate,
    LeaderStep::ApplyBarrier,
    LeaderStep::SourceDrained,
    LeaderStep::TargetReady,
];

async fn harness(cluster: &str, port_base: u16) -> LeaderHarness {
    LeaderHarness::start(cluster, port_base, u128::from(port_base))
        .await
        .expect("the leader harness elects over an in-memory store")
}

fn storage_failure(error: &CoordinatorRuntimeError) -> bool {
    matches!(error, CoordinatorRuntimeError::Storage(_))
}

fn deltas(commands: &[PlacementControlCommand]) -> usize {
    commands
        .iter()
        .filter(|command| matches!(command, PlacementControlCommand::StateDelta(_)))
        .count()
}

fn grants(commands: &[PlacementControlCommand]) -> usize {
    commands
        .iter()
        .filter(|command| matches!(command, PlacementControlCommand::ClaimGranted(_)))
        .count()
}

fn drains(commands: &[PlacementControlCommand]) -> usize {
    commands
        .iter()
        .filter(|command| matches!(command, PlacementControlCommand::DrainSlot { .. }))
        .count()
}

fn carries(commands: &[PlacementControlCommand], version: &PlacementVersion) -> bool {
    commands.iter().any(|command| {
        matches!(command, PlacementControlCommand::StateDelta(delta) if &delta.version == version)
    })
}

fn silence(harness: &mut LeaderHarness) {
    let _ = harness.leader_mut().commands(0);
    let _ = harness.leader_mut().commands(1);
}

async fn owner_index(harness: &LeaderHarness) -> usize {
    harness
        .leader()
        .owner_index(HARNESS_SHARD)
        .await
        .unwrap()
        .expect("an allocated shard has an owning host")
}

fn recorded(
    harness: &LeaderHarness,
    point: Failpoint,
    action: FailAction,
    targets: &[FaultTarget],
) -> Vec<FaultEvidence> {
    let evidence = harness.record(point, action, targets);
    assert_eq!(
        evidence.len(),
        targets.len(),
        "{} was never honoured at its call site",
        point.name()
    );
    evidence
}

async fn allocation_commit_is_refused() -> Vec<FaultEvidence> {
    let mut harness = harness("alloc-refused", 27100).await;
    harness.arm(
        Failpoint::AuthorityBeforeGuardedCommit,
        FailAction::StoreFailure,
    );
    let error = harness.step(LeaderStep::AllocateShard).await.unwrap_err();
    assert!(storage_failure(&error), "{error}");
    assert!(
        harness
            .leader()
            .slot(HARNESS_SHARD)
            .await
            .unwrap()
            .is_none()
    );
    assert!(!harness.leader().tracks_claim(HARNESS_SHARD));
    let evidence = recorded(
        &harness,
        Failpoint::AuthorityBeforeGuardedCommit,
        FailAction::StoreFailure,
        &[FaultTarget::Coordinator, FaultTarget::Store],
    );
    harness.step(LeaderStep::AllocateShard).await.unwrap();
    assert_eq!(
        harness.leader().slot_state(HARNESS_SHARD).await.unwrap(),
        Some(PlacementSlotState::Allocating),
        "the retry after a refused commit never allocated"
    );
    evidence
}

async fn initial_authority_effect_is_lost() -> Vec<FaultEvidence> {
    let mut harness = harness("alloc-effect-lost", 27110).await;
    harness.arm(
        Failpoint::InitialAuthorityAfterCommitBeforeEffect,
        FailAction::Crash,
    );
    let error = harness.step(LeaderStep::AllocateShard).await.unwrap_err();
    assert!(storage_failure(&error), "{error}");
    assert_eq!(
        harness.leader().slot_state(HARNESS_SHARD).await.unwrap(),
        Some(PlacementSlotState::Allocating)
    );
    assert!(
        harness
            .leader()
            .stored_claim(HARNESS_SHARD)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        !harness.leader().tracks_claim(HARNESS_SHARD),
        "the leader adopted a claim the failpoint cut off"
    );
    let owner = owner_index(&harness).await;
    let commands = harness.leader_mut().commands(owner);
    assert_eq!(
        grants(&commands),
        0,
        "the owner was granted authority anyway"
    );
    assert_eq!(deltas(&commands), 0);
    recorded(
        &harness,
        Failpoint::InitialAuthorityAfterCommitBeforeEffect,
        FailAction::Crash,
        &[
            FaultTarget::Coordinator,
            FaultTarget::Store,
            FaultTarget::Target,
            FaultTarget::Network,
        ],
    )
}

async fn slot_delta_is_dropped() -> Vec<FaultEvidence> {
    let mut harness = harness("delta-dropped", 27120).await;
    harness.advance(RESERVED).await.unwrap();
    let source = owner_index(&harness).await;
    silence(&mut harness);
    harness.arm(
        Failpoint::CoordinatorAfterEtcdCommitBeforeDelta,
        FailAction::Drop,
    );
    harness.step(LeaderStep::ApplyBarrier).await.unwrap();
    let drained = harness.leader_mut().commands(source);
    let observer = harness.leader_mut().commands(1 - source);
    assert_eq!(
        deltas(&drained),
        0,
        "the dropped delta still reached a lane"
    );
    assert_eq!(deltas(&observer), 0);
    assert_eq!(drains(&drained), 1, "the drain command was dropped as well");
    let evidence = recorded(
        &harness,
        Failpoint::CoordinatorAfterEtcdCommitBeforeDelta,
        FailAction::Drop,
        &[
            FaultTarget::Network,
            FaultTarget::Source,
            FaultTarget::Target,
        ],
    );
    harness.step(LeaderStep::SourceDrained).await.unwrap();
    assert!(
        deltas(&harness.leader_mut().commands(1 - source)) > 0,
        "a one-shot drop silenced the delta path for good"
    );
    evidence
}

async fn drain_command_is_dropped() -> Vec<FaultEvidence> {
    let mut harness = harness("drain-dropped", 27130).await;
    harness.advance(RESERVED).await.unwrap();
    let source = owner_index(&harness).await;
    silence(&mut harness);
    harness.arm(Failpoint::HandoffAfterDrainSend, FailAction::Drop);
    harness.step(LeaderStep::ApplyBarrier).await.unwrap();
    let commands = harness.leader_mut().commands(source);
    assert_eq!(drains(&commands), 0, "the drain command reached the source");
    assert!(
        deltas(&commands) > 0,
        "dropping the drain command also silenced the delta"
    );
    assert_eq!(
        harness.leader().slot_state(HARNESS_SHARD).await.unwrap(),
        Some(PlacementSlotState::Stopping)
    );
    recorded(
        &harness,
        Failpoint::HandoffAfterDrainSend,
        FailAction::Drop,
        &[FaultTarget::Network, FaultTarget::Source],
    )
}

async fn reservation_persists_without_a_handoff(
    cluster: &str,
    port_base: u16,
    point: Failpoint,
    action: FailAction,
) -> Vec<FaultEvidence> {
    let mut harness = harness(cluster, port_base).await;
    harness.advance(RUNNING).await.unwrap();
    silence(&mut harness);
    harness.arm(point, action);
    let error = harness.step(LeaderStep::Relocate).await.unwrap_err();
    assert!(storage_failure(&error), "{error}");
    assert_eq!(
        harness.leader().slot_state(HARNESS_SHARD).await.unwrap(),
        Some(PlacementSlotState::BeginHandoff)
    );
    assert_eq!(harness.leader().stored_plans().await.unwrap(), 1);
    assert!(
        harness.leader().handoff_phase(HARNESS_SHARD).is_none(),
        "a handoff machine survived a cut-off reservation"
    );
    assert_eq!(deltas(&harness.leader_mut().commands(0)), 0);
    assert_eq!(deltas(&harness.leader_mut().commands(1)), 0);
    recorded(
        &harness,
        point,
        action,
        &[
            FaultTarget::Coordinator,
            FaultTarget::Store,
            FaultTarget::Source,
            FaultTarget::Target,
            FaultTarget::Network,
        ],
    )
}

async fn partial_barrier_stops_before_its_effects() -> Vec<FaultEvidence> {
    let mut harness = harness("partial-barrier", 27160).await;
    harness.advance(RUNNING).await.unwrap();
    silence(&mut harness);
    harness.arm(Failpoint::HandoffAfterPartialBarrier, FailAction::Crash);
    let error = harness.step(LeaderStep::Relocate).await.unwrap_err();
    assert!(storage_failure(&error), "{error}");
    assert_eq!(
        harness.leader().handoff_phase(HARNESS_SHARD),
        Some(HandoffPhase::Invalidating),
        "the barrier failpoint fired at the reservation boundary instead"
    );
    assert!(
        deltas(&harness.leader_mut().commands(0)) > 0,
        "the barrier delta never reached the members"
    );
    let evidence = recorded(
        &harness,
        Failpoint::HandoffAfterPartialBarrier,
        FailAction::Crash,
        &[FaultTarget::Coordinator, FaultTarget::Store],
    );
    harness.step(LeaderStep::ApplyBarrier).await.unwrap();
    assert_eq!(
        harness.leader().slot_state(HARNESS_SHARD).await.unwrap(),
        Some(PlacementSlotState::Stopping),
        "the installed barrier could not be applied afterwards"
    );
    evidence
}

async fn claim_revoke_is_refused() -> Vec<FaultEvidence> {
    let mut harness = harness("revoke-refused", 27170).await;
    harness.advance(DRAINED).await.unwrap();
    let source = harness.leader().host(owner_index(&harness).await).cloned();
    harness.arm(
        Failpoint::HandoffAfterShardDrainedBeforeClaimRevoke,
        FailAction::StoreFailure,
    );
    let error = harness.step(LeaderStep::SourceDrained).await.unwrap_err();
    assert!(storage_failure(&error), "{error}");
    assert_eq!(
        harness.leader().slot_state(HARNESS_SHARD).await.unwrap(),
        Some(PlacementSlotState::Stopping)
    );
    assert_eq!(
        harness
            .leader()
            .stored_claim(HARNESS_SHARD)
            .await
            .unwrap()
            .map(|grant| grant.owner),
        source,
        "the retiring owner lost its claim before the fence committed"
    );
    assert!(harness.leader().tracks_claim(HARNESS_SHARD));
    recorded(
        &harness,
        Failpoint::HandoffAfterShardDrainedBeforeClaimRevoke,
        FailAction::StoreFailure,
        &[
            FaultTarget::Coordinator,
            FaultTarget::Store,
            FaultTarget::Source,
        ],
    )
}

async fn fence_effect_is_lost() -> Vec<FaultEvidence> {
    let mut harness = harness("fence-effect-lost", 27180).await;
    harness.advance(DRAINED).await.unwrap();
    harness.arm(
        Failpoint::FenceAuthorityAfterCommitBeforeEffect,
        FailAction::Crash,
    );
    let error = harness.step(LeaderStep::SourceDrained).await.unwrap_err();
    assert!(storage_failure(&error), "{error}");
    assert_eq!(
        harness.leader().slot_state(HARNESS_SHARD).await.unwrap(),
        Some(PlacementSlotState::Fenced)
    );
    assert!(
        harness
            .leader()
            .stored_claim(HARNESS_SHARD)
            .await
            .unwrap()
            .is_none(),
        "the fence commit left the retired claim behind"
    );
    assert!(
        harness.leader().tracks_claim(HARNESS_SHARD),
        "the leader released a claim the failpoint cut off"
    );
    recorded(
        &harness,
        Failpoint::FenceAuthorityAfterCommitBeforeEffect,
        FailAction::Crash,
        &[
            FaultTarget::Coordinator,
            FaultTarget::Store,
            FaultTarget::Source,
        ],
    )
}

async fn new_claim_is_never_granted() -> Vec<FaultEvidence> {
    let mut harness = harness("grant-never-sent", 27190).await;
    harness.advance(DRAINED).await.unwrap();
    let target = 1 - owner_index(&harness).await;
    silence(&mut harness);
    harness.arm(
        Failpoint::HandoffAfterNewClaimBeforeGrantSend,
        FailAction::StoreFailure,
    );
    let error = harness.step(LeaderStep::SourceDrained).await.unwrap_err();
    assert!(storage_failure(&error), "{error}");
    assert_eq!(owner_index(&harness).await, target);
    assert!(
        harness
            .leader()
            .stored_claim(HARNESS_SHARD)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        !harness.leader().tracks_claim(HARNESS_SHARD),
        "the leader adopted a claim the failpoint cut off"
    );
    let installed = harness
        .leader()
        .slot(HARNESS_SHARD)
        .await
        .unwrap()
        .unwrap()
        .version;
    let commands = harness.leader_mut().commands(target);
    assert_eq!(grants(&commands), 0, "the new owner was granted authority");
    assert!(
        !carries(&commands, &installed),
        "the installed authority was published anyway"
    );
    recorded(
        &harness,
        Failpoint::HandoffAfterNewClaimBeforeGrantSend,
        FailAction::StoreFailure,
        &[
            FaultTarget::Coordinator,
            FaultTarget::Store,
            FaultTarget::Target,
            FaultTarget::Network,
        ],
    )
}

async fn granted_target_never_reaches_ready() -> Vec<FaultEvidence> {
    let mut harness = harness("ready-never-reached", 27200).await;
    harness.advance(DRAINED).await.unwrap();
    let target = 1 - owner_index(&harness).await;
    silence(&mut harness);
    harness.arm(
        Failpoint::HandoffAfterGrantBeforeShardReady,
        FailAction::Crash,
    );
    let error = harness.step(LeaderStep::SourceDrained).await.unwrap_err();
    assert!(storage_failure(&error), "{error}");
    assert_eq!(owner_index(&harness).await, target);
    assert!(harness.leader().tracks_claim(HARNESS_SHARD));
    assert_eq!(
        grants(&harness.leader_mut().commands(target)),
        1,
        "the target never received the grant this boundary follows"
    );
    assert_eq!(
        harness.leader().handoff_phase(HARNESS_SHARD),
        Some(HandoffPhase::ReplacingAuthority),
        "the handoff advanced past the cut-off boundary"
    );
    recorded(
        &harness,
        Failpoint::HandoffAfterGrantBeforeShardReady,
        FailAction::Crash,
        &[FaultTarget::Coordinator, FaultTarget::Target],
    )
}

async fn active_slot_is_never_published() -> Vec<FaultEvidence> {
    let mut harness = harness("active-not-published", 27210).await;
    harness.advance(REPLACED).await.unwrap();
    let plan_id = harness.plan_id().unwrap();
    silence(&mut harness);
    harness.arm(
        Failpoint::HandoffAfterActivePersistBeforeDelta,
        FailAction::StoreFailure,
    );
    let error = harness.step(LeaderStep::TargetReady).await.unwrap_err();
    assert!(storage_failure(&error), "{error}");
    assert_eq!(
        harness.leader().slot_state(HARNESS_SHARD).await.unwrap(),
        Some(PlacementSlotState::Running)
    );
    assert_eq!(
        harness.leader().stored_plan_status(plan_id).await.unwrap(),
        Some(PlanStatus::Completed)
    );
    assert_eq!(
        harness.leader().handoff_phase(HARNESS_SHARD),
        Some(HandoffPhase::Completed),
        "the leader retired a handoff the failpoint cut off"
    );
    assert_eq!(deltas(&harness.leader_mut().commands(0)), 0);
    assert_eq!(deltas(&harness.leader_mut().commands(1)), 0);
    recorded(
        &harness,
        Failpoint::HandoffAfterActivePersistBeforeDelta,
        FailAction::StoreFailure,
        &[
            FaultTarget::Coordinator,
            FaultTarget::Store,
            FaultTarget::Source,
            FaultTarget::Target,
            FaultTarget::Network,
        ],
    )
}

async fn adopted_claim_effect_is_lost() -> Vec<FaultEvidence> {
    let mut harness = harness("adopt-effect-lost", 27220).await;
    harness.advance(COMPLETED).await.unwrap();
    assert_eq!(
        harness
            .leader()
            .stored_claim(HARNESS_SHARD)
            .await
            .unwrap()
            .map(|grant| grant.coordinator_term.get()),
        Some(1)
    );
    harness.arm(
        Failpoint::ReconciliationAfterCommitBeforeEffect,
        FailAction::Crash,
    );
    let error = harness.step(LeaderStep::Reelect).await.unwrap_err();
    assert!(storage_failure(&error), "{error}");
    assert_eq!(
        harness
            .leader()
            .stored_claim(HARNESS_SHARD)
            .await
            .unwrap()
            .map(|grant| grant.coordinator_term.get()),
        Some(SUCCESSOR_TERM),
        "the successor never adopted the claim it committed"
    );
    assert_eq!(
        harness.leader().version().term.get(),
        1,
        "the successor was installed despite a cut-off reconciliation"
    );
    recorded(
        &harness,
        Failpoint::ReconciliationAfterCommitBeforeEffect,
        FailAction::Crash,
        &[FaultTarget::Coordinator, FaultTarget::Store],
    )
}

async fn admin_commit_is_refused() -> Vec<FaultEvidence> {
    let mut harness = harness("admin-refused", 27230).await;
    harness.arm(
        Failpoint::AdminBeforeGuardedCommit,
        FailAction::StoreFailure,
    );
    let error = harness.step(LeaderStep::PauseAutomatic).await.unwrap_err();
    assert!(storage_failure(&error), "{error}");
    assert!(!harness.leader().stored_automatic_paused().await.unwrap());
    assert!(!harness.leader().automatic_paused());
    assert_eq!(harness.leader().stored_admin_operations().await.unwrap(), 0);
    let evidence = recorded(
        &harness,
        Failpoint::AdminBeforeGuardedCommit,
        FailAction::StoreFailure,
        &[FaultTarget::Coordinator, FaultTarget::Store],
    );
    harness.step(LeaderStep::PauseAutomatic).await.unwrap();
    assert!(
        harness.leader().automatic_paused(),
        "the retry after a refused admin commit never applied"
    );
    evidence
}

async fn admin_response_is_lost() -> Vec<FaultEvidence> {
    let mut harness = harness("admin-response-lost", 27240).await;
    harness.arm(Failpoint::AdminAfterCommitBeforeResponse, FailAction::Crash);
    let error = harness.step(LeaderStep::PauseAutomatic).await.unwrap_err();
    assert!(storage_failure(&error), "{error}");
    assert!(
        harness.leader().stored_automatic_paused().await.unwrap(),
        "the admin commit never landed"
    );
    assert!(
        !harness.leader().automatic_paused(),
        "the leader applied an effect the failpoint cut off"
    );
    assert_eq!(harness.leader().stored_admin_operations().await.unwrap(), 1);
    recorded(
        &harness,
        Failpoint::AdminAfterCommitBeforeResponse,
        FailAction::Crash,
        &[FaultTarget::Coordinator, FaultTarget::Store],
    )
}

async fn plan_commit_is_refused() -> Vec<FaultEvidence> {
    let mut harness = harness("plan-refused", 27250).await;
    harness.advance(RUNNING).await.unwrap();
    harness.arm(Failpoint::PlanBeforeGuardedCommit, FailAction::StoreFailure);
    let error = harness.step(LeaderStep::Relocate).await.unwrap_err();
    assert!(storage_failure(&error), "{error}");
    assert_eq!(harness.leader().stored_plans().await.unwrap(), 0);
    assert_eq!(harness.leader().tracked_plans(), 0);
    assert_eq!(
        harness.leader().slot_state(HARNESS_SHARD).await.unwrap(),
        Some(PlacementSlotState::Running)
    );
    recorded(
        &harness,
        Failpoint::PlanBeforeGuardedCommit,
        FailAction::StoreFailure,
        &[FaultTarget::Coordinator, FaultTarget::Store],
    )
}

async fn persisted_plan_is_never_started() -> Vec<FaultEvidence> {
    let mut harness = harness("plan-not-started", 27260).await;
    harness.advance(RUNNING).await.unwrap();
    harness.arm(Failpoint::RebalanceAfterPlanPersist, FailAction::Crash);
    let error = harness.step(LeaderStep::Relocate).await.unwrap_err();
    assert!(storage_failure(&error), "{error}");
    assert_eq!(harness.leader().stored_plans().await.unwrap(), 1);
    assert_eq!(
        harness.leader().tracked_plans(),
        0,
        "the leader tracked a plan the failpoint cut off"
    );
    assert_eq!(
        harness.leader().slot_state(HARNESS_SHARD).await.unwrap(),
        Some(PlacementSlotState::Running),
        "a move started despite the cut-off plan"
    );
    recorded(
        &harness,
        Failpoint::RebalanceAfterPlanPersist,
        FailAction::Crash,
        &[FaultTarget::Coordinator, FaultTarget::Store],
    )
}

pub(super) async fn injected_leader_evidence() -> Vec<FaultEvidence> {
    let mut evidence = Vec::new();
    evidence.extend(allocation_commit_is_refused().await);
    evidence.extend(initial_authority_effect_is_lost().await);
    evidence.extend(slot_delta_is_dropped().await);
    evidence.extend(drain_command_is_dropped().await);
    evidence.extend(
        reservation_persists_without_a_handoff(
            "begin-persist",
            27140,
            Failpoint::HandoffAfterBeginPersist,
            FailAction::StoreFailure,
        )
        .await,
    );
    evidence.extend(
        reservation_persists_without_a_handoff(
            "reservation-cut",
            27150,
            Failpoint::RebalanceAfterReservationBeforeHandoff,
            FailAction::Crash,
        )
        .await,
    );
    evidence.extend(partial_barrier_stops_before_its_effects().await);
    evidence.extend(claim_revoke_is_refused().await);
    evidence.extend(fence_effect_is_lost().await);
    evidence.extend(new_claim_is_never_granted().await);
    evidence.extend(granted_target_never_reaches_ready().await);
    evidence.extend(active_slot_is_never_published().await);
    evidence.extend(adopted_claim_effect_is_lost().await);
    evidence.extend(admin_commit_is_refused().await);
    evidence.extend(admin_response_is_lost().await);
    evidence.extend(plan_commit_is_refused().await);
    evidence.extend(persisted_plan_is_never_started().await);
    evidence
}

#[tokio::test]
async fn an_unarmed_leader_completes_allocation_handoff_and_admin_work() {
    let mut harness = harness("unarmed-leader", 27000).await;
    harness.advance(COMPLETED).await.unwrap();
    harness.step(LeaderStep::PauseAutomatic).await.unwrap();
    assert_eq!(
        harness.leader().slot_state(HARNESS_SHARD).await.unwrap(),
        Some(PlacementSlotState::Running)
    );
    assert!(harness.leader().handoff_phase(HARNESS_SHARD).is_none());
    assert!(harness.leader().tracks_claim(HARNESS_SHARD));
    assert!(harness.leader().automatic_paused());
    assert!(harness.evidence().is_empty());
}

#[tokio::test]
async fn allocation_boundaries_honour_their_injected_decisions() {
    assert_eq!(allocation_commit_is_refused().await.len(), 2);
    assert_eq!(initial_authority_effect_is_lost().await.len(), 4);
}

#[tokio::test]
async fn handoff_boundaries_honour_their_injected_decisions() {
    assert_eq!(slot_delta_is_dropped().await.len(), 3);
    assert_eq!(drain_command_is_dropped().await.len(), 2);
    assert_eq!(
        reservation_persists_without_a_handoff(
            "begin-persist-case",
            27141,
            Failpoint::HandoffAfterBeginPersist,
            FailAction::StoreFailure,
        )
        .await
        .len(),
        5
    );
    assert_eq!(
        reservation_persists_without_a_handoff(
            "reservation-cut-case",
            27151,
            Failpoint::RebalanceAfterReservationBeforeHandoff,
            FailAction::Crash,
        )
        .await
        .len(),
        5
    );
    assert_eq!(partial_barrier_stops_before_its_effects().await.len(), 2);
    assert_eq!(claim_revoke_is_refused().await.len(), 3);
    assert_eq!(fence_effect_is_lost().await.len(), 3);
    assert_eq!(new_claim_is_never_granted().await.len(), 4);
    assert_eq!(granted_target_never_reaches_ready().await.len(), 2);
    assert_eq!(active_slot_is_never_published().await.len(), 5);
}

#[tokio::test]
async fn reconciliation_and_admin_boundaries_honour_their_injected_decisions() {
    assert_eq!(adopted_claim_effect_is_lost().await.len(), 2);
    assert_eq!(admin_commit_is_refused().await.len(), 2);
    assert_eq!(admin_response_is_lost().await.len(), 2);
    assert_eq!(plan_commit_is_refused().await.len(), 2);
    assert_eq!(persisted_plan_is_never_started().await.len(), 2);
}
