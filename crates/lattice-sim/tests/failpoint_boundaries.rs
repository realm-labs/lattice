use std::path::{Path, PathBuf};

use lattice_core::failpoint::Failpoint;

const PRODUCTION_CRATES: [&str; 3] = ["lattice-remoting", "lattice-placement", "lattice-service"];

const DECISION_CAPABLE: [&str; 2] = [
    "member_before_guarded_commit",
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
    calls(production, "hit_decision", point) || calls(production, "guarded_commit_failpoint", point)
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
