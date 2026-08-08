//! Scenario tests.
//!
//! The shared fixtures build either a seeded workload or a fully controlled schedule so that a
//! test can assert on one boundary at a time.

use std::collections::BTreeSet;

use lattice_core::{failpoint::Failpoint, watch::TerminatedReason};
use lattice_placement::{
    coordinator::MemberHello,
    runtime::membership_plane::{MembershipLeader, MembershipLeaderConfig},
    storage::InMemoryPlacementStore,
};
use lattice_remoting::watch::WatchRegistry;

use super::*;
use crate::{
    explorer::StateExplorer,
    fault::{
        FailAction, FaultEvidence, FaultMatrix, FaultOrigin, FaultOutcome, FaultTarget,
        SharedFaultInjector,
    },
    store::{SimEtcd, SimWatchEvent},
};

mod coordinator;
mod exploration;

use exploration::HandoffExploration;

fn run(seed: u64) -> Scenario {
    let mut scenario = Scenario::standard(ScenarioConfig {
        seed,
        maximum_events: 256,
    })
    .unwrap();
    scenario.schedule_standard_workload().unwrap();
    scenario.run().unwrap();
    scenario
}

fn controlled(seed: u64) -> Scenario {
    let mut scenario = Scenario::standard(ScenarioConfig {
        seed,
        maximum_events: 256,
    })
    .unwrap();
    scenario.schedule(1, ScenarioEvent::InstallWatch);
    scenario.schedule(
        2,
        ScenarioEvent::Handoff(HandoffStep::ApplyBarrier(incarnation(1)), MAXIMUM_ATTEMPTS),
    );
    scenario.schedule(
        3,
        ScenarioEvent::Handoff(HandoffStep::ApplyBarrier(incarnation(2)), MAXIMUM_ATTEMPTS),
    );
    scenario.schedule(
        4,
        ScenarioEvent::Handoff(HandoffStep::SourceInvalid, MAXIMUM_ATTEMPTS),
    );
    scenario.schedule(
        5,
        ScenarioEvent::Handoff(HandoffStep::TargetClaimInstalled, MAXIMUM_ATTEMPTS),
    );
    scenario.schedule(
        6,
        ScenarioEvent::Handoff(HandoffStep::TargetReady, MAXIMUM_ATTEMPTS),
    );
    scenario.schedule(7, ScenarioEvent::SendControl(1));
    scenario.schedule(40, ScenarioEvent::ReplayControl);
    scenario.schedule(60, ScenarioEvent::TargetTerminated);
    scenario
}

fn signature(scenario: &Scenario) -> Vec<String> {
    scenario
        .trace
        .events
        .iter()
        .map(|event| format!("{}@{}", event.kind, event.time_millis))
        .collect()
}

#[test]
fn same_seed_replays_identical_production_reducer_trace() {
    for seed in [1, 44, 4096, u64::MAX] {
        let first = run(seed);
        let second = run(seed);
        assert_eq!(first.state(), second.state());
        assert_eq!(first.trace, second.trace);
        assert_eq!(first.evidence(), second.evidence());
    }
}

#[test]
fn seeded_workloads_explore_distinct_interleavings() {
    let traces = (1..=32)
        .map(|seed| signature(&run(seed)))
        .collect::<BTreeSet<_>>();
    assert!(traces.len() >= 24, "only {} distinct traces", traces.len());
}

#[test]
fn seeded_workloads_arm_distinct_fault_sets() {
    let armed = (1..=32)
        .map(|seed| {
            let mut scenario = Scenario::standard(ScenarioConfig {
                seed,
                maximum_events: 256,
            })
            .unwrap();
            scenario.schedule_standard_workload().unwrap();
            scenario.faults.with(|injector| {
                Failpoint::ALL
                    .into_iter()
                    .filter(|point| injector.is_armed(*point))
                    .map(Failpoint::name)
                    .collect::<Vec<_>>()
            })
        })
        .collect::<BTreeSet<_>>();
    assert!(
        armed.len() >= 16,
        "only {} distinct fault sets",
        armed.len()
    );
}

#[test]
fn every_seed_preserves_handoff_and_control_safety() {
    let mut injected = 0;
    let mut completed = 0;
    for seed in 1..=192 {
        let scenario = run(seed);
        scenario.check_invariants().unwrap();
        injected += scenario.state().injected_faults;
        completed += usize::from(scenario.state().running);
    }
    assert!(injected > 0, "no seeded workload injected a fault");
    assert!(completed > 0, "no seeded workload completed the handoff");
}

#[test]
fn unarmed_workloads_inject_nothing() {
    let mut scenario = controlled(7);
    scenario.run().unwrap();
    assert_eq!(scenario.state().injected_faults, 0);
    assert!(scenario.evidence().is_empty());
    assert!(scenario.state().running);
    assert_eq!(scenario.state().applied_control_commands, 1);
    assert_eq!(scenario.state().terminal_watches, 1);
    assert!(scenario.state().watch_acknowledged);
}

#[test]
fn armed_store_failure_at_active_persist_is_observed_and_recovered() {
    let mut scenario = controlled(7);
    scenario.faults.arm(
        Failpoint::HandoffAfterActivePersistBeforeDelta,
        FailAction::StoreFailure,
    );
    scenario.run().unwrap();
    assert_eq!(
        scenario.evidence(),
        vec![FaultEvidence {
            point: Failpoint::HandoffAfterActivePersistBeforeDelta,
            target: FaultTarget::Store,
            action: FailAction::StoreFailure,
            outcome: FaultOutcome::CommitRejected,
            origin: FaultOrigin::SimulatedExecutor,
        }]
    );
    assert_eq!(scenario.state().injected_faults, 1);
    assert!(scenario.state().running, "recovery retry never republished");
}

#[test]
fn armed_claim_revoke_crash_delays_but_never_dual_owns_the_slot() {
    let mut scenario = controlled(7);
    scenario.faults.arm(
        Failpoint::HandoffAfterShardDrainedBeforeClaimRevoke,
        FailAction::Crash,
    );
    scenario.run().unwrap();
    assert!(
        scenario
            .evidence()
            .iter()
            .any(|evidence| evidence.outcome == FaultOutcome::ProcessCrashed)
    );
    let crashed = scenario
        .trace
        .events
        .iter()
        .position(|event| event.kind.contains("SourceInvalid"))
        .unwrap();
    assert!(scenario.trace.events[crashed].next == "draining");
    assert!(scenario.state().running);
}

#[test]
fn armed_control_ack_drop_forces_replay_and_deduplicated_apply() {
    let mut scenario = controlled(7);
    scenario.faults.arm(
        Failpoint::ControlAfterRemoteApplyBeforeAck,
        FailAction::Drop,
    );
    scenario.run().unwrap();
    assert_eq!(
        scenario.evidence(),
        vec![FaultEvidence {
            point: Failpoint::ControlAfterRemoteApplyBeforeAck,
            target: FaultTarget::Network,
            action: FailAction::Drop,
            outcome: FaultOutcome::MessageLost,
            origin: FaultOrigin::SimulatedExecutor,
        }]
    );
    assert_eq!(scenario.state().applied_control_commands, 1);
    assert!(scenario.state().duplicate_control_commands >= 1);
}

#[test]
fn armed_outbox_duplicate_is_rejected_by_reliable_control() {
    let mut scenario = controlled(7);
    scenario.faults.arm(
        Failpoint::ControlAfterOutboxBeforeSocketWrite,
        FailAction::Duplicate,
    );
    scenario.run().unwrap();
    assert_eq!(scenario.state().applied_control_commands, 1);
    assert_eq!(scenario.state().duplicate_control_commands, 1);
}

#[test]
fn production_watch_registry_honours_a_dropped_terminal_notification() {
    let injector = SharedFaultInjector::default();
    let installed = injector.install();
    assert_eq!(terminal_notifications(), 1);
    injector.arm(Failpoint::WatchAfterTerminatedBeforeAck, FailAction::Drop);
    assert_eq!(terminal_notifications(), 0);
    assert_eq!(terminal_notifications(), 1);
    drop(installed);
}

fn terminal_notifications() -> usize {
    let source = node("source", 1, 28001);
    let actor = actor_ref(&source);
    let mut registry = WatchRegistry::new(4, 4).unwrap();
    let (registered_watch, _) = registry
        .watch(AssociationId::new(1).unwrap(), &actor)
        .unwrap();
    let watch_id = registered_watch.id();
    let target = ExactActorTarget::from(&actor);
    registry
        .receive_watch(
            AssociationId::new(1).unwrap(),
            watch_id,
            target.clone(),
            |_| true,
        )
        .unwrap();
    registry
        .target_terminated(&target, TerminatedReason::Migrated)
        .len()
}

#[tokio::test]
async fn production_membership_commit_fails_under_an_injected_store_failure() {
    let injector = SharedFaultInjector::default();
    let installed = injector.install();
    let store = std::sync::Arc::new(InMemoryPlacementStore::new(8, 8).unwrap());
    let mut leader = MembershipLeader::elect(
        store,
        node("coordinator", 5, 29500),
        CoordinatorTerm::new(1).unwrap(),
        MembershipLeaderConfig::default(),
    )
    .await
    .unwrap();
    injector.arm(
        Failpoint::MemberBeforeGuardedCommit,
        FailAction::StoreFailure,
    );
    let hello = member_hello();
    let error = leader.join(hello.clone()).await.unwrap_err();
    assert!(
        format!("{error}").contains("durable store failed"),
        "{error}"
    );
    assert!(injector.observed(Failpoint::MemberBeforeGuardedCommit));
    let record = leader.join(hello).await.unwrap();
    assert_eq!(record.node.node_id, "member");
    drop(installed);
}

fn member_hello() -> MemberHello {
    MemberHello {
        node: node("member", 11, 29301),
        release: lattice_core::release::ReleaseManifest::development(1),
        rollout_participant: true,
        roles: Default::default(),
        failure_domains: Default::default(),
        protocols: Vec::new(),
        remoting_capabilities: Default::default(),
    }
}

#[test]
fn trace_shrinking_preserves_one_command_reproduction() {
    let scenario = run(9);
    let shrunk = scenario.trace.shrink(|events| {
        events
            .iter()
            .any(|event| event.kind.contains("TargetReady"))
    });
    assert_eq!(shrunk.events.len(), 1);
    assert!(shrunk.events[0].kind.contains("TargetReady"));
}

#[test]
fn bounded_state_explorer_checks_every_production_handoff_transition() {
    let scenario = Scenario::standard(ScenarioConfig {
        seed: 1,
        maximum_events: 8,
    })
    .unwrap();
    let report = StateExplorer {
        maximum_states: 20_000,
        maximum_depth: 8,
    }
    .explore(HandoffExploration {
        machine: scenario.handoff,
        published: false,
        stop_failed: false,
        completed_seen: false,
    })
    .unwrap();
    assert_eq!(report.maximum_depth_reached, 5);
    assert!(report.visited_states >= 10, "{report:?}");
    assert!(report.explored_transitions > 100, "{report:?}");
}

#[test]
fn simulated_etcd_cas_leases_watch_and_compaction_are_revisioned() {
    let mut etcd = SimEtcd::new(8).unwrap();
    let lease = etcd.grant_lease(0, 10).unwrap();
    let revision = etcd
        .compare_and_put(
            "claim".to_owned(),
            None,
            bytes::Bytes::from_static(b"one"),
            Some(lease),
        )
        .unwrap();
    assert_eq!(revision, 1);
    assert!(
        etcd.compare_and_put("claim".to_owned(), None, bytes::Bytes::new(), None)
            .is_err()
    );
    assert_eq!(etcd.expire_leases(10).unwrap(), vec!["claim"]);
    etcd.compact(2);
    assert!(matches!(
        etcd.watch_from(1).as_slice(),
        [SimWatchEvent::Compacted { compacted: 2, .. }]
    ));
}

#[tokio::test]
async fn fault_matrix_records_only_injected_and_observed_boundaries() {
    let mut matrix = FaultMatrix::required_default();
    for seed in 1..=192 {
        for evidence in run(seed).evidence() {
            assert!(matrix.record(evidence));
        }
    }
    for evidence in injected_membership_evidence().await {
        assert!(matrix.record(evidence));
    }
    for evidence in coordinator::injected_leader_evidence().await {
        assert!(matrix.record(evidence));
    }
    let covered = matrix
        .covered()
        .map(|(pair, origin)| format!("{} {:?} {origin:?}", pair.0.name(), pair.1))
        .collect::<Vec<_>>();
    assert_eq!(covered, EXPECTED_COVERAGE);
    assert_eq!(
        matrix.missing().count(),
        matrix.required().count() - EXPECTED_COVERAGE.len()
    );
}

const EXPECTED_COVERAGE: [&str; 60] = [
    "control_after_outbox_before_socket_write Network SimulatedExecutor",
    "control_after_remote_apply_before_ack Target SimulatedExecutor",
    "control_after_remote_apply_before_ack Network SimulatedExecutor",
    "coordinator_after_etcd_commit_before_delta Source ProductionCallSite",
    "coordinator_after_etcd_commit_before_delta Target ProductionCallSite",
    "coordinator_after_etcd_commit_before_delta Network ProductionCallSite",
    "member_before_guarded_commit Coordinator ProductionCallSite",
    "member_before_guarded_commit Store ProductionCallSite",
    "plan_before_guarded_commit Coordinator ProductionCallSite",
    "plan_before_guarded_commit Store ProductionCallSite",
    "authority_before_guarded_commit Coordinator ProductionCallSite",
    "authority_before_guarded_commit Store ProductionCallSite",
    "admin_before_guarded_commit Coordinator ProductionCallSite",
    "admin_before_guarded_commit Store ProductionCallSite",
    "initial_authority_after_commit_before_effect Coordinator ProductionCallSite",
    "initial_authority_after_commit_before_effect Target ProductionCallSite",
    "initial_authority_after_commit_before_effect Store ProductionCallSite",
    "initial_authority_after_commit_before_effect Network ProductionCallSite",
    "fence_authority_after_commit_before_effect Coordinator ProductionCallSite",
    "fence_authority_after_commit_before_effect Source ProductionCallSite",
    "fence_authority_after_commit_before_effect Store ProductionCallSite",
    "admin_after_commit_before_response Coordinator ProductionCallSite",
    "admin_after_commit_before_response Store ProductionCallSite",
    "reconciliation_after_commit_before_effect Coordinator ProductionCallSite",
    "reconciliation_after_commit_before_effect Store ProductionCallSite",
    "rebalance_after_plan_persist Coordinator ProductionCallSite",
    "rebalance_after_plan_persist Store ProductionCallSite",
    "rebalance_after_reservation_before_handoff Coordinator ProductionCallSite",
    "rebalance_after_reservation_before_handoff Source ProductionCallSite",
    "rebalance_after_reservation_before_handoff Target ProductionCallSite",
    "rebalance_after_reservation_before_handoff Store ProductionCallSite",
    "rebalance_after_reservation_before_handoff Network ProductionCallSite",
    "handoff_after_begin_persist Coordinator ProductionCallSite",
    "handoff_after_begin_persist Source ProductionCallSite",
    "handoff_after_begin_persist Target ProductionCallSite",
    "handoff_after_begin_persist Store ProductionCallSite",
    "handoff_after_begin_persist Network ProductionCallSite",
    "handoff_after_partial_barrier Coordinator ProductionCallSite",
    "handoff_after_partial_barrier Store ProductionCallSite",
    "handoff_after_partial_barrier Network SimulatedExecutor",
    "handoff_after_drain_send Source ProductionCallSite",
    "handoff_after_drain_send Network ProductionCallSite",
    "handoff_after_shard_drained_before_claim_revoke Coordinator ProductionCallSite",
    "handoff_after_shard_drained_before_claim_revoke Source ProductionCallSite",
    "handoff_after_shard_drained_before_claim_revoke Target SimulatedExecutor",
    "handoff_after_shard_drained_before_claim_revoke Store ProductionCallSite",
    "handoff_after_new_claim_before_grant_send Coordinator ProductionCallSite",
    "handoff_after_new_claim_before_grant_send Target ProductionCallSite",
    "handoff_after_new_claim_before_grant_send Store ProductionCallSite",
    "handoff_after_new_claim_before_grant_send Network ProductionCallSite",
    "handoff_after_grant_before_shard_ready Coordinator ProductionCallSite",
    "handoff_after_grant_before_shard_ready Target ProductionCallSite",
    "handoff_after_grant_before_shard_ready Network SimulatedExecutor",
    "handoff_after_active_persist_before_delta Coordinator ProductionCallSite",
    "handoff_after_active_persist_before_delta Source ProductionCallSite",
    "handoff_after_active_persist_before_delta Target ProductionCallSite",
    "handoff_after_active_persist_before_delta Store ProductionCallSite",
    "handoff_after_active_persist_before_delta Network ProductionCallSite",
    "watch_after_install_before_ack Network SimulatedExecutor",
    "watch_after_terminated_before_ack Network ProductionCallSite",
];

async fn injected_membership_evidence() -> Vec<FaultEvidence> {
    let injector = SharedFaultInjector::default();
    let installed = injector.install();
    let store = std::sync::Arc::new(InMemoryPlacementStore::new(8, 8).unwrap());
    let mut leader = MembershipLeader::elect(
        store,
        node("coordinator", 5, 29500),
        CoordinatorTerm::new(1).unwrap(),
        MembershipLeaderConfig::default(),
    )
    .await
    .unwrap();
    let mut evidence = Vec::new();
    for (action, target, outcome) in [
        (
            FailAction::StoreFailure,
            FaultTarget::Store,
            FaultOutcome::CommitRejected,
        ),
        (
            FailAction::Crash,
            FaultTarget::Coordinator,
            FaultOutcome::ProcessCrashed,
        ),
    ] {
        injector.arm(Failpoint::MemberBeforeGuardedCommit, action);
        assert!(leader.join(member_hello()).await.is_err());
        let record = FaultEvidence {
            point: Failpoint::MemberBeforeGuardedCommit,
            target,
            action,
            outcome,
            origin: FaultOrigin::ProductionCallSite,
        };
        assert!(injector.record(record));
        evidence.push(record);
    }
    assert!(leader.join(member_hello()).await.is_ok());
    drop(installed);
    evidence
}
