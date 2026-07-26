use std::path::{Path, PathBuf};

use lattice_core::failpoint::Failpoint;

const PRODUCTION_CRATES: [&str; 3] = ["lattice-remoting", "lattice-placement", "lattice-service"];

/// Every entry is a production call site that turns an injected decision into an outcome the
/// caller can observe: a refused commit, an unknown outcome after the commit landed, or a control
/// command that never leaves the coordinator. A call site that only announces the boundary belongs
/// nowhere on this list.
const DECISION_CAPABLE: [&str; 19] = [
    "coordinator_after_etcd_commit_before_delta",
    "member_before_guarded_commit",
    "plan_before_guarded_commit",
    "authority_before_guarded_commit",
    "admin_before_guarded_commit",
    "initial_authority_after_commit_before_effect",
    "fence_authority_after_commit_before_effect",
    "admin_after_commit_before_response",
    "reconciliation_after_commit_before_effect",
    "rebalance_after_plan_persist",
    "rebalance_after_reservation_before_handoff",
    "handoff_after_begin_persist",
    "handoff_after_partial_barrier",
    "handoff_after_drain_send",
    "handoff_after_shard_drained_before_claim_revoke",
    "handoff_after_new_claim_before_grant_send",
    "handoff_after_grant_before_shard_ready",
    "handoff_after_active_persist_before_delta",
    "watch_after_terminated_before_ack",
];

#[test]
fn every_named_failpoint_is_called_from_production_code() {
    let production = production_sources();
    for point in Failpoint::ALL {
        assert!(
            calls(&production, "hit", point) || decision_call(&production, point),
            "production code never calls {}",
            point.name()
        );
    }
}

#[test]
fn failpoints_that_can_inject_a_decision_are_an_explicit_ledger() {
    let production = production_sources();
    let capable = Failpoint::ALL
        .into_iter()
        .filter(|point| decision_call(&production, *point))
        .map(Failpoint::name)
        .collect::<Vec<_>>();
    assert_eq!(
        capable, DECISION_CAPABLE,
        "the set of failpoints whose production call site honours an injected action changed"
    );
}

fn decision_call(production: &str, point: Failpoint) -> bool {
    [
        "hit_decision",
        "guarded_commit_failpoint",
        "post_commit_failpoint",
        "dropped_by_failpoint",
    ]
    .into_iter()
    .any(|function| calls(production, function, point))
}

fn calls(production: &str, function: &str, point: Failpoint) -> bool {
    [")", ","].into_iter().any(|terminator| {
        production.contains(&format!("{function}(Failpoint::{point:?}{terminator}"))
    })
}

fn production_sources() -> String {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let mut production = String::new();
    for crate_name in PRODUCTION_CRATES {
        collect_rust(
            &root.join("crates").join(crate_name).join("src"),
            &mut production,
        );
    }
    production.retain(|character| !character.is_whitespace());
    production
}

fn collect_rust(directory: &Path, output: &mut String) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect_rust(&path, output);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            output.push_str(&std::fs::read_to_string(path).unwrap());
        }
    }
}
