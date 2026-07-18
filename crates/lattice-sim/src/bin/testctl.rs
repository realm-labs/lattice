#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

use std::collections::BTreeSet;

#[path = "testctl/artifacts.rs"]
mod testctl_artifacts;
#[path = "testctl/chaos.rs"]
mod testctl_chaos;
#[path = "testctl/commands.rs"]
mod testctl_commands;
#[path = "testctl/discovery.rs"]
mod testctl_discovery;
#[path = "testctl/outcomes.rs"]
mod testctl_outcomes;
#[path = "testctl/resources.rs"]
mod testctl_resources;
#[path = "testctl/scenarios.rs"]
mod testctl_scenarios;

use std::{
    path::{Path, PathBuf},
    process::{Command, ExitCode},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use clap::{Parser, Subcommand, ValueEnum};
use lattice_sim::{
    domains::{MultiDomainScenario, MultiDomainScenarioConfig},
    lifecycle::{LifecycleScenario, LifecycleScenarioConfig},
    scenario::{Scenario, ScenarioConfig},
    trace::TraceJournal,
};
use serde::Serialize;
use testctl_artifacts::{
    Manifest, MonitorCommand, MonitorResult, MultiDomainHostArtifact, MultiDomainLogicArtifact,
    ResourceSample, ScopedLeadershipArtifact, write_json, write_json_atomic, write_junit,
};
use testctl_commands::{cargo, cargo_test_exact, command, output};
use testctl_outcomes::ScenarioRunner;
use testctl_scenarios::{wait_for_host_scope, wait_for_scope_across_hosts};

#[derive(Parser)]
#[command(name = "testctl")]
struct Cli {
    #[command(subcommand)]
    command: TestCommand,
}

#[derive(Subcommand)]
enum TestCommand {
    Run {
        #[arg(value_enum)]
        profile: Profile,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value = "target/test-artifacts/local")]
        artifacts: PathBuf,
        #[arg(long, default_value_t = 30)]
        duration_seconds: u64,
    },
    Replay {
        #[arg(long)]
        artifact: PathBuf,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Profile {
    Quality,
    Sim,
    Model,
    E2e,
    E2eHaEtcd,
    Chaos,
    K8s,
    Soak,
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("testctl: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), String> {
    match cli.command {
        TestCommand::Replay { artifact } => replay(&artifact),
        TestCommand::Run {
            profile,
            seed,
            artifacts,
            duration_seconds,
        } => run_profile(profile, seed, &artifacts, duration_seconds),
    }
}

fn run_profile(
    profile: Profile,
    seed: u64,
    artifacts: &Path,
    duration_seconds: u64,
) -> Result<(), String> {
    std::fs::create_dir_all(artifacts).map_err(|error| error.to_string())?;
    let started = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_millis();
    let timer = Instant::now();
    let mut resource_samples = vec![testctl_resources::sample(timer.elapsed())];
    let scenario_names = testctl_scenarios::for_profile(profile);
    let mut runner = ScenarioRunner::new(&scenario_names);
    match profile {
        Profile::Quality => {
            runner.run("structure", || command("scripts/check-structure.sh", &[]));
            runner.run("fmt", || cargo(&["fmt", "--all", "--", "--check"]));
            runner.run("clippy", || {
                cargo(&[
                    "clippy",
                    "--workspace",
                    "--all-targets",
                    "--all-features",
                    "--",
                    "-D",
                    "warnings",
                ])
            });
            runner.run("workspace-tests", || {
                cargo(&["test", "--workspace", "--all-features"])
            });
        }
        Profile::Sim => runner.run("seeded-simulation-suite", || simulate(seed, artifacts)),
        Profile::Model => {
            runner.run("bounded-state-explorer", || {
                cargo(&[
                    "test",
                    "-p",
                    "lattice-sim",
                    "scenario::tests::bounded_state_explorer_checks_every_transition",
                ])
            });
            runner.run("multi-domain-bounded-state-explorer", || {
                cargo(&[
                    "test",
                    "-p",
                    "lattice-sim",
                    "domains::tests::multi_domain_bounded_state_explorer_checks_every_transition",
                ])
            });
        }
        Profile::E2e => {
            runner.run("exact-actor-ref-child-watch", || {
                distributed_node("client", &artifacts.join("server-ref.json"))
            });
            runner.run("gateway-entity-ref-remote-shard", || {
                distributed_node("gateway", &artifacts.join("entity-ref.json"))
            });
            runner.run("static-discovery-lifecycle", || {
                testctl_discovery::verify_case(artifacts, "discovery-static.json", "static")
            });
            runner.run("config-store-discovery-lifecycle", || {
                testctl_discovery::verify_case(artifacts, "discovery-config.json", "config-store")
            });
            runner.run("multi-domain-failover", || multi_domain_real(artifacts));
            runner.run("single-member-etcd", || {
                cargo(&[
                    "test",
                    "-p",
                    "lattice-placement",
                    "--test",
                    "etcd_acceptance",
                    "--",
                    "--nocapture",
                ])
            });
            runner.run("tcp", || {
                cargo(&[
                    "test",
                    "-p",
                    "lattice-remoting",
                    "real_tcp_endpoint_establishes_all_lanes_and_delivers_ask",
                ])
            });
            runner.run("mutual-tls", || {
                cargo(&[
                    "test",
                    "-p",
                    "lattice-remoting",
                    "real_mutual_tls_socket_verifies_both_node_identities",
                ])
            });
            runner.run("claimed-entity-ref", || {
                cargo(&[
                    "test",
                    "-p",
                    "lattice-service",
                    "remote_entity_ask_reaches_only_claimed_owner",
                ])
            });
        }
        Profile::E2eHaEtcd => {
            runner.run("etcd-coordinator-failover", || ha_etcd_real(artifacts));
            runner.run("leader-recovery-resume", || {
                cargo_test_exact(
                    "lattice-placement",
                    "runtime::tests::recovery_tests::leader_recovery_resumes_persisted_handoff",
                )
            });
            runner.run("singleton-forward-recovery", || {
                cargo_test_exact(
                    "lattice-placement",
                    "runtime::tests::recovery_tests::singleton_owner_loss_recovers_forward_after_leader_restart",
                )
            });
        }
        Profile::Chaos => {
            runner.run("multi-domain-failover", || multi_domain_real(artifacts));
            runner.run("docker-fault-sequence", || testctl_chaos::verify(artifacts));
            runner.run("one-domain-coordinator-loss", || {
                cargo(&[
                    "test",
                    "-p",
                    "lattice-service",
                    "one_domain_coordinator_loss_leaves_other_domain_ready",
                ])
            });
            runner.run("membership-loss", || {
                cargo(&[
                    "test",
                    "-p",
                    "lattice-service",
                    "membership_loss_revokes_node_readiness_until_a_new_snapshot",
                ])
            });
            runner.run("drain-force-remove", || {
                cargo(&[
                    "test",
                    "-p",
                    "lattice-placement",
                    "join_drain_and_force_remove_are_revisioned_idempotent_and_fenced",
                ])
            });
            runner.run("etcd-lease-expiry", || {
                cargo(&[
                    "test",
                    "-p",
                    "lattice-placement",
                    "--test",
                    "etcd_acceptance",
                    "real_etcd_guarded_domain_commits_and_lease_expiry",
                    "--",
                    "--nocapture",
                ])
            });
            runner.run("multi-domain-trace-replay", || {
                cargo(&[
                    "test",
                    "-p",
                    "lattice-sim",
                    "multi_domain_trace_replays_independent_elections_and_handoffs",
                ])
            });
            runner.run("seed-corpus", || {
                for current in seed..seed.saturating_add(32) {
                    simulate(current, artifacts)?;
                }
                Ok(())
            });
        }
        Profile::K8s => runner.run("k8s-lifecycle", || {
            command("sh", &["tests/distributed/k8s/verify.sh"])
        }),
        Profile::Soak => runner.run("bounded-seeded-soak", || {
            soak(
                seed,
                duration_seconds,
                artifacts,
                timer,
                &mut resource_samples,
            )
        }),
    }
    let scenario_result = runner.finish();
    let cleanup_result = if matches!(profile, Profile::E2eHaEtcd | Profile::Chaos | Profile::K8s)
        && Path::new("/var/run/docker.sock").exists()
    {
        command("sh", &["scripts/docker-image-lifecycle.sh", "cleanup"])
    } else {
        Ok(())
    };
    let infrastructure_error = cleanup_result.as_ref().err().cloned();
    let result = match (scenario_result, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(scenarios), Err(cleanup)) => {
            Err(format!("{scenarios}; infrastructure cleanup: {cleanup}"))
        }
    };
    resource_samples.push(testctl_resources::sample(timer.elapsed()));
    let success = result.is_ok();
    let replay_artifact = if matches!(profile, Profile::Soak) {
        "soak-latest.json".to_owned()
    } else {
        format!("trace-{seed}.json")
    };
    let manifest = Manifest {
        profile,
        seed,
        source_commit: output("git", &["rev-parse", "HEAD"])
            .unwrap_or_else(|_| "unknown".to_owned()),
        source_status: output("git", &["status", "--porcelain=v1"])
            .unwrap_or_else(|_| "unavailable".to_owned()),
        source_fingerprint: output(
            "sh",
            &[
                "-c",
                "git ls-files --cached --others --exclude-standard -z | sort -z | xargs -0 sha256sum | sha256sum",
            ],
        )
        .unwrap_or_else(|_| "unavailable".to_owned()),
        started_unix_millis: started,
        elapsed_millis: timer.elapsed().as_millis(),
        success,
        replay: format!(
            "./scripts/test-docker.sh replay --artifact {}/{}",
            artifacts.display(),
            replay_artifact
        ),
        platform: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
        pinned_images: std::fs::read_to_string("tests/distributed/images.lock")
            .unwrap_or_else(|_| "unavailable".to_owned()),
        scenarios: runner.outcomes().to_vec(),
        infrastructure_error: infrastructure_error.clone(),
        configuration: serde_json::json!({
            "duration_seconds": duration_seconds,
            "artifact_directory": artifacts,
        }),
    };
    write_json(&artifacts.join("manifest.json"), &manifest)?;
    write_junit(
        &artifacts.join("junit.xml"),
        profile,
        runner.outcomes(),
        infrastructure_error.as_deref(),
    )?;
    write_json(&artifacts.join("resource-samples.json"), &resource_samples)?;
    result
}

fn multi_domain_real(artifacts: &Path) -> Result<(), String> {
    let run_id = std::env::var("LATTICE_RUN_ID")
        .map_err(|_| "multi-domain e2e requires LATTICE_RUN_ID".to_owned())?;
    let containers = labeled_containers(&run_id)?;
    let container = |needle: &str| {
        containers
            .lines()
            .find(|name| name.contains(needle) && !name.contains("runner"))
            .ok_or_else(|| format!("missing labeled {needle} container"))
    };
    let membership_container = container("domain-membership")?;
    let alpha_container = container("domain-alpha")?;
    let beta_container = container("domain-beta")?;
    let gamma_container = container("domain-gamma")?;
    let standby_container = container("domain-standby")?;
    for name in [
        membership_container,
        alpha_container,
        beta_container,
        gamma_container,
        standby_container,
    ] {
        require_label("container", name, &run_id)?;
    }

    let membership = wait_for_host_scope(
        &artifacts.join("domain-membership.json"),
        "membership",
        0,
        Duration::from_secs(120),
    )?;
    if membership.node_id != "domain-membership" {
        return Err("dedicated membership host did not retain membership leadership".to_owned());
    }
    let alpha = wait_for_host_scope(
        &artifacts.join("domain-alpha.json"),
        "placement:domain-alpha",
        0,
        Duration::from_secs(120),
    )?;
    let beta = wait_for_host_scope(
        &artifacts.join("domain-beta.json"),
        "placement:domain-beta",
        0,
        Duration::from_secs(120),
    )?;
    let gamma = wait_for_host_scope(
        &artifacts.join("domain-gamma.json"),
        "placement:domain-gamma",
        0,
        Duration::from_secs(120),
    )?;
    let delta = wait_for_host_scope(
        &artifacts.join("domain-alpha.json"),
        "placement:domain-delta",
        0,
        Duration::from_secs(120),
    )?;
    if delta.node_id != alpha.node_id {
        return Err("domain alpha host did not initially lead both alpha and delta".to_owned());
    }
    let leaders = [
        alpha.node_id.as_str(),
        beta.node_id.as_str(),
        gamma.node_id.as_str(),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    if leaders.len() != 3 {
        return Err(format!(
            "expected three independently distributed domain leaders, found {leaders:?}"
        ));
    }
    for name in ["domain-logic-a.json", "domain-logic-b.json"] {
        wait_for_logic_ready(&artifacts.join(name), Duration::from_secs(30))?;
    }

    command("docker", &["stop", "--time", "1", alpha_container])?;
    let replacement = wait_for_host_scope_while_checking_logic(
        artifacts,
        "placement:domain-alpha",
        alpha.term,
        Duration::from_secs(60),
    );
    let restart_result = (|| {
        command("docker", &["start", alpha_container])?;
        wait_for_running_container(alpha_container, Duration::from_secs(30))
    })();
    let replacement = replacement?;
    restart_result?;
    if replacement.node_id != "domain-standby" {
        return Err(format!(
            "domain alpha failed over to {}, expected domain-standby",
            replacement.node_id
        ));
    }
    let delta_replacement = wait_for_host_scope(
        &artifacts.join("domain-standby.json"),
        "placement:domain-delta",
        delta.term,
        Duration::from_secs(60),
    )?;
    if delta_replacement.node_id != "domain-standby" {
        return Err(format!(
            "domain delta failed over to {}, expected domain-standby",
            delta_replacement.node_id
        ));
    }
    let beta_after = wait_for_host_scope(
        &artifacts.join("domain-beta.json"),
        "placement:domain-beta",
        0,
        Duration::from_secs(2),
    )?;
    let gamma_after = wait_for_host_scope(
        &artifacts.join("domain-gamma.json"),
        "placement:domain-gamma",
        0,
        Duration::from_secs(2),
    )?;
    if beta_after.node_id != beta.node_id
        || beta_after.term != beta.term
        || gamma_after.node_id != gamma.node_id
        || gamma_after.term != gamma.term
    {
        return Err("domain alpha failure changed beta or gamma leadership".to_owned());
    }
    for name in ["domain-logic-a.json", "domain-logic-b.json"] {
        wait_for_logic_ready(&artifacts.join(name), Duration::from_secs(30))?;
    }
    command("docker", &["stop", "--time", "1", membership_container])?;
    let membership_replacement = wait_for_scope_across_hosts(
        artifacts,
        &[
            "domain-alpha.json",
            "domain-beta.json",
            "domain-gamma.json",
            "domain-standby.json",
        ],
        "membership",
        membership.term,
        Duration::from_secs(60),
    );
    let membership_restart = (|| {
        command("docker", &["start", membership_container])?;
        wait_for_running_container(membership_container, Duration::from_secs(30))
    })();
    let membership_replacement = membership_replacement?;
    membership_restart?;
    if membership_replacement.node_id == membership.node_id {
        return Err(format!(
            "membership did not fail away from the stopped host: {}",
            membership_replacement.node_id
        ));
    }
    for name in ["domain-logic-a.json", "domain-logic-b.json"] {
        wait_for_logic_ready(&artifacts.join(name), Duration::from_secs(30))?;
    }
    write_json(
        &artifacts.join("multi-domain-failover.json"),
        &serde_json::json!({
            "membership": {
                "node_id": membership.node_id,
                "term": membership.term,
                "incarnation": membership.incarnation.to_string(),
            },
            "initial": {
                "alpha": {
                    "node_id": alpha.node_id,
                    "term": alpha.term,
                    "incarnation": alpha.incarnation.to_string(),
                },
                "beta": {
                    "node_id": beta.node_id,
                    "term": beta.term,
                    "incarnation": beta.incarnation.to_string(),
                },
                "gamma": {
                    "node_id": gamma.node_id,
                    "term": gamma.term,
                    "incarnation": gamma.incarnation.to_string(),
                },
                "delta": {
                    "node_id": delta.node_id,
                    "term": delta.term,
                    "incarnation": delta.incarnation.to_string(),
                },
            },
            "replacement": {
                "node_id": replacement.node_id,
                "term": replacement.term,
                "incarnation": replacement.incarnation.to_string(),
            },
            "delta_replacement": {
                "node_id": delta_replacement.node_id,
                "term": delta_replacement.term,
                "incarnation": delta_replacement.incarnation.to_string(),
            },
            "membership_replacement": {
                "node_id": membership_replacement.node_id,
                "term": membership_replacement.term,
                "incarnation": membership_replacement.incarnation.to_string(),
            },
            "unrelated_terms_unchanged": true,
            "logic_nodes": ["domain-logic-a", "domain-logic-b"],
        }),
    )
}

fn wait_for_host_scope_while_checking_logic(
    artifacts: &Path,
    scope: &str,
    minimum_term: u64,
    timeout: Duration,
) -> Result<ScopedLeadershipArtifact, String> {
    let deadline = Instant::now() + timeout;
    loop {
        for name in ["domain-logic-a.json", "domain-logic-b.json"] {
            let logic = read_logic(&artifacts.join(name))?;
            if logic.lifecycle != "Ready"
                || logic.domains.get("domain-beta").map(String::as_str) != Some("Ready")
                || logic.domains.get("domain-gamma").map(String::as_str) != Some("Ready")
            {
                return Err(format!(
                    "unrelated domain degraded during alpha failover: {logic:?}"
                ));
            }
        }
        if let Ok(leader) = wait_for_host_scope(
            &artifacts.join("domain-standby.json"),
            scope,
            minimum_term,
            Duration::from_millis(10),
        ) {
            return Ok(leader);
        }
        if Instant::now() >= deadline {
            return Err("standby did not acquire failed placement domain".to_owned());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn read_logic(path: &Path) -> Result<MultiDomainLogicArtifact, String> {
    serde_json::from_slice(&std::fs::read(path).map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())
}

fn require_logic_ready(path: &Path) -> Result<(), String> {
    let logic = read_logic(path)?;
    if logic.lifecycle == "Ready"
        && [
            "domain-alpha",
            "domain-beta",
            "domain-gamma",
            "domain-delta",
        ]
        .into_iter()
        .all(|domain| logic.domains.get(domain).map(String::as_str) == Some("Ready"))
    {
        Ok(())
    } else {
        Err(format!("multi-domain logic node is not ready: {logic:?}"))
    }
}

fn wait_for_logic_ready(path: &Path, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        if require_logic_ready(path).is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return require_logic_ready(path);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn ha_etcd_real(artifacts: &Path) -> Result<(), String> {
    let run_id = std::env::var("LATTICE_RUN_ID")
        .map_err(|_| "HA etcd requires LATTICE_RUN_ID".to_owned())?;
    let containers = labeled_containers(&run_id)?;
    let members = containers
        .lines()
        .filter(|name| name.contains("etcd") && !name.contains("runner"))
        .collect::<Vec<_>>();
    if members.len() != 3 {
        return Err(format!(
            "expected three labeled etcd members, found {}",
            members.len()
        ));
    }
    for member in &members {
        require_label("container", member, &run_id)?;
    }
    let coordinators = containers
        .lines()
        .filter(|name| name.contains("coordinator-") && !name.contains("runner"))
        .collect::<Vec<_>>();
    if coordinators.len() != 2 {
        return Err(format!(
            "expected two labeled Coordinator processes, found {}",
            coordinators.len()
        ));
    }
    let initial_coordinator =
        wait_for_coordinator_leadership(artifacts, None, 0, Duration::from_secs(120))?;

    run_etcd_acceptance()?;
    let (leader, stopped_member_id) = members
        .iter()
        .find_map(|member| match etcd_member_status(member) {
            Ok((member_id, leader_id)) if member_id == leader_id => Some(Ok((*member, member_id))),
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .transpose()?
        .ok_or_else(|| "etcd leader was not discoverable".to_owned())?;
    require_label("container", leader, &run_id)?;
    command("docker", &["stop", "--time", "5", leader])?;
    wait_for_etcd_failover(&members, leader, stopped_member_id, Duration::from_secs(30))?;
    let surviving_endpoints = ["etcd1", "etcd2", "etcd3"]
        .into_iter()
        .filter(|name| !leader.contains(name))
        .map(|name| format!("http://{name}:2379"))
        .collect::<Vec<_>>()
        .join(",");
    let quorum_result = run_etcd_acceptance_with_endpoints(&surviving_endpoints);
    require_label("container", leader, &run_id)?;
    command("docker", &["start", leader])?;
    wait_for_healthy_container(leader, Duration::from_secs(60))?;
    quorum_result?;

    let coordinator_container = coordinators
        .iter()
        .find(|container| container.contains(&initial_coordinator.node_id))
        .copied()
        .ok_or_else(|| "elected Coordinator container was not discoverable".to_owned())?;
    require_label("container", coordinator_container, &run_id)?;
    command("docker", &["stop", "--time", "1", coordinator_container])?;
    let replacement = wait_for_coordinator_leadership(
        artifacts,
        Some(&initial_coordinator.node_id),
        initial_coordinator.term,
        Duration::from_secs(60),
    )?;
    require_label("container", coordinator_container, &run_id)?;
    command("docker", &["start", coordinator_container])?;
    wait_for_running_container(coordinator_container, Duration::from_secs(60))?;
    assert_coordinator_not_displaced(leader, &run_id, &replacement, Duration::from_secs(2))?;
    write_json(
        &artifacts.join("coordinator-failover.json"),
        &serde_json::json!({
            "stopped": {
                "node_id": initial_coordinator.node_id,
                "term": initial_coordinator.term,
                "incarnation": initial_coordinator.incarnation.to_string(),
            },
            "replacement": {
                "node_id": replacement.node_id,
                "term": replacement.term,
                "incarnation": replacement.incarnation.to_string(),
            },
            "restarted_container": coordinator_container,
        }),
    )?;

    let status = output(
        "docker",
        &[
            "exec",
            leader,
            "etcdctl",
            "--endpoints=http://127.0.0.1:2379",
            "--write-out=json",
            "endpoint",
            "status",
        ],
    )?;
    std::fs::write(artifacts.join("etcd-status.json"), status)
        .map_err(|error| error.to_string())?;
    let keys = output(
        "docker",
        &[
            "exec",
            leader,
            "etcdctl",
            "--endpoints=http://127.0.0.1:2379",
            "--write-out=json",
            "get",
            "",
            "--prefix",
            "--keys-only",
        ],
    )?;
    std::fs::write(artifacts.join("etcd-keys-redacted.json"), keys)
        .map_err(|error| error.to_string())
}

fn wait_for_coordinator_leadership(
    artifacts: &Path,
    excluded_node: Option<&str>,
    minimum_term: u64,
    timeout: Duration,
) -> Result<ScopedLeadershipArtifact, String> {
    let deadline = Instant::now() + timeout;
    loop {
        for name in ["coordinator-a.json", "coordinator-b.json"] {
            let path = artifacts.join(name);
            let Ok(bytes) = std::fs::read(path) else {
                continue;
            };
            let Ok(state) = serde_json::from_slice::<ScopedLeadershipArtifact>(&bytes) else {
                continue;
            };
            if state.term > minimum_term
                && excluded_node.is_none_or(|excluded| state.node_id != excluded)
            {
                return Ok(state);
            }
        }
        if Instant::now() >= deadline {
            return Err("Coordinator leadership did not reach the required term".to_owned());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn assert_coordinator_not_displaced(
    etcd_member: &str,
    run_id: &str,
    expected: &ScopedLeadershipArtifact,
    stable_period: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + stable_period;
    let mut observed = false;
    while Instant::now() < deadline {
        let Ok((node_id, term)) = coordinator_leader_from_etcd(etcd_member, run_id) else {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        };
        observed = true;
        if node_id != expected.node_id || term < expected.term {
            return Err(format!(
                "Coordinator leader changed from {} term {} to {node_id} term {term}",
                expected.node_id, expected.term
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    if observed {
        Ok(())
    } else {
        Err("Coordinator leader was not observable during the stability window".to_owned())
    }
}

fn coordinator_leader_from_etcd(etcd_member: &str, run_id: &str) -> Result<(String, u64), String> {
    let key = format!("/lattice-ha/{run_id}/domains/distributed-simulation/leader");
    let encoded = output(
        "docker",
        &[
            "exec",
            etcd_member,
            "etcdctl",
            "--endpoints=http://127.0.0.1:2379",
            "get",
            &key,
            "--print-value-only",
        ],
    )?;
    let value: serde_json::Value =
        serde_json::from_str(&encoded).map_err(|error| error.to_string())?;
    let node_id = value
        .pointer("/node/node_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "Coordinator leader record is missing node_id".to_owned())?
        .to_owned();
    let term = json_u64(
        value
            .get("term")
            .ok_or_else(|| "Coordinator leader record is missing term".to_owned())?,
    )?;
    Ok((node_id, term))
}

fn wait_for_etcd_failover(
    members: &[&str],
    stopped: &str,
    stopped_member_id: u64,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        for member in members.iter().copied().filter(|member| *member != stopped) {
            if let Ok((_, leader_id)) = etcd_member_status(member)
                && leader_id != 0
                && leader_id != stopped_member_id
            {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            return Err("surviving etcd quorum did not elect a new leader".to_owned());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn run_etcd_acceptance() -> Result<(), String> {
    run_etcd_acceptance_command(Command::new("cargo"))
}

fn run_etcd_acceptance_with_endpoints(endpoints: &str) -> Result<(), String> {
    let mut command = Command::new("cargo");
    command.env("LATTICE_ETCD_ENDPOINTS", endpoints);
    run_etcd_acceptance_command(command)
}

fn run_etcd_acceptance_command(mut command: Command) -> Result<(), String> {
    let status = command
        .args([
            "test",
            "-p",
            "lattice-placement",
            "--test",
            "etcd_acceptance",
            "--",
            "--nocapture",
        ])
        .status()
        .map_err(|error| format!("failed to start cargo: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("cargo exited with {status}"))
    }
}

fn labeled_containers(run_id: &str) -> Result<String, String> {
    output(
        "docker",
        &[
            "ps",
            "--filter",
            &format!("label=io.lattice.test-run={run_id}"),
            "--format",
            "{{.Names}}",
        ],
    )
}

fn etcd_member_status(container: &str) -> Result<(u64, u64), String> {
    let encoded = output(
        "docker",
        &[
            "exec",
            container,
            "etcdctl",
            "--endpoints=http://127.0.0.1:2379",
            "--write-out=json",
            "endpoint",
            "status",
        ],
    )?;
    let value: serde_json::Value =
        serde_json::from_str(&encoded).map_err(|error| error.to_string())?;
    let status = value
        .as_array()
        .and_then(|items| items.first())
        .and_then(|item| item.get("Status"))
        .ok_or_else(|| format!("missing etcd status for {container}"))?;
    let member = json_u64(
        status
            .pointer("/header/member_id")
            .ok_or_else(|| "missing etcd member ID".to_owned())?,
    )?;
    let leader = json_u64(
        status
            .get("leader")
            .ok_or_else(|| "missing etcd leader ID".to_owned())?,
    )?;
    Ok((member, leader))
}

fn json_u64(value: &serde_json::Value) -> Result<u64, String> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        .ok_or_else(|| format!("expected integer in etcd status, found {value}"))
}

fn wait_for_healthy_container(container: &str, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        let status = output(
            "docker",
            &[
                "container",
                "inspect",
                "--format",
                "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
                container,
            ],
        )?;
        if status == "healthy" {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("container {container} did not become healthy"));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_running_container(container: &str, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        let status = output(
            "docker",
            &[
                "container",
                "inspect",
                "--format",
                "{{.State.Status}}",
                container,
            ],
        )?;
        if status == "running" {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("container {container} did not enter running state"));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_file(path: &Path, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        if path.is_file() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{} was not published within {timeout:?}",
                path.display()
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn require_label(kind: &str, name: &str, run_id: &str) -> Result<(), String> {
    let template = if kind == "container" {
        "{{ index .Config.Labels \"io.lattice.test-run\" }}"
    } else {
        "{{ index .Labels \"io.lattice.test-run\" }}"
    };
    let actual = output("docker", &[kind, "inspect", "--format", template, name])?;
    if actual == run_id {
        Ok(())
    } else {
        Err(format!("refusing to mutate unlabeled {kind} {name}"))
    }
}

fn simulate(seed: u64, artifacts: &Path) -> Result<(), String> {
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

fn soak(
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

fn replay(path: &Path) -> Result<(), String> {
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

fn distributed_node(mode: &str, reference: &Path) -> Result<(), String> {
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
