use std::{
    path::Path,
    time::{Duration, Instant},
};

use lattice_sim::{
    domains::{MultiDomainScenario, MultiDomainScenarioConfig},
    lifecycle::{LifecycleScenario, LifecycleScenarioConfig},
    scenario::{Scenario, ScenarioConfig},
    trace::TraceJournal,
};

use super::{testctl_artifacts::ResourceSample, testctl_commands::cargo, testctl_resources};

pub(super) fn simulate(seed: u64, artifacts: &Path) -> Result<(), String> {
    simulate_to(seed, &artifacts.join(format!("trace-{seed}.json")), true)
}

fn simulate_to(seed: u64, trace_path: &Path, retain_companion_traces: bool) -> Result<(), String> {
    let mut scenario = Scenario::standard(ScenarioConfig {
        seed,
        maximum_events: 256,
    })
    .map_err(|error| error.to_string())?;
    scenario
        .schedule_standard_workload()
        .map_err(|error| error.to_string())?;
    let result = scenario
        .run()
        .map(|_| ())
        .map_err(|error| error.to_string());
    scenario
        .trace
        .write_json(trace_path)
        .map_err(|error| error.to_string())?;
    let mut lifecycle = LifecycleScenario::standard(LifecycleScenarioConfig {
        seed,
        maximum_events: 64,
    })
    .map_err(|error| error.to_string())?;
    lifecycle.schedule_acceptance();
    lifecycle.run().map_err(|error| error.to_string())?;
    if retain_companion_traces {
        lifecycle
            .trace
            .write_json(&trace_path.with_file_name(format!("lifecycle-trace-{seed}.json")))
            .map_err(|error| error.to_string())?;
    }
    let mut domains = MultiDomainScenario::standard(MultiDomainScenarioConfig {
        seed,
        maximum_events: 64,
    })
    .map_err(|error| error.to_string())?;
    domains.schedule_acceptance();
    domains.run().map_err(|error| error.to_string())?;
    if retain_companion_traces {
        domains
            .trace
            .write_json(&trace_path.with_file_name(format!("domain-trace-{seed}.json")))
            .map_err(|error| error.to_string())?;
    }
    result
}

pub(super) fn soak(
    seed: u64,
    duration_seconds: u64,
    artifacts: &Path,
    timer: Instant,
    samples: &mut Vec<ResourceSample>,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(duration_seconds.max(1));
    let mut current = seed;
    let rolling_trace = artifacts.join("soak-latest.json");
    let mut next_sample = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        simulate_to(current, &rolling_trace, false)?;
        current = current.saturating_add(1);
        if Instant::now() >= next_sample {
            samples.push(testctl_resources::sample(timer.elapsed()));
            next_sample = Instant::now() + Duration::from_secs(1);
        }
    }
    let final_trace = artifacts.join(format!("trace-{}.json", current.saturating_sub(1)));
    std::fs::copy(&rolling_trace, final_trace).map_err(|error| error.to_string())?;
    testctl_resources::assert_growth(samples, &testctl_resources::sample(timer.elapsed()))
}

pub(super) fn replay(path: &Path) -> Result<(), String> {
    let expected = TraceJournal::read_json(path).map_err(|error| error.to_string())?;
    let actual = match expected.scenario.as_str() {
        "standard-handoff" => {
            let mut scenario = Scenario::standard(ScenarioConfig {
                seed: expected.seed,
                maximum_events: 256,
            })
            .map_err(|error| error.to_string())?;
            scenario
                .schedule_standard_workload()
                .map_err(|error| error.to_string())?;
            scenario.run().map_err(|error| error.to_string())?;
            scenario.trace
        }
        "cluster-member-lifecycle" => {
            let mut scenario = LifecycleScenario::standard(LifecycleScenarioConfig {
                seed: expected.seed,
                maximum_events: 64,
            })
            .map_err(|error| error.to_string())?;
            scenario.schedule_acceptance();
            scenario.run().map_err(|error| error.to_string())?;
            scenario.trace
        }
        "multi-domain-isolation" => {
            let mut scenario = MultiDomainScenario::standard(MultiDomainScenarioConfig {
                seed: expected.seed,
                maximum_events: 64,
            })
            .map_err(|error| error.to_string())?;
            scenario.schedule_acceptance();
            scenario.run().map_err(|error| error.to_string())?;
            scenario.trace
        }
        scenario => return Err(format!("unsupported replay scenario {scenario}")),
    };
    if actual == expected {
        Ok(())
    } else {
        Err("replayed trace differs from artifact".to_owned())
    }
}

pub(super) fn distributed_node(mode: &str, reference: &Path) -> Result<(), String> {
    let reference = reference
        .to_str()
        .ok_or_else(|| "distributed fixture reference path is not UTF-8".to_owned())?;
    cargo(&[
        "run",
        "-p",
        "lattice-sim",
        "--bin",
        "distributed-node",
        "--",
        mode,
        "--reference",
        reference,
    ])
}
