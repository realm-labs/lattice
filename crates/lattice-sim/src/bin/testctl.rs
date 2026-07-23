#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

use std::collections::BTreeSet;

#[path = "testctl/artifacts.rs"]
mod testctl_artifacts;
#[path = "testctl/chaos.rs"]
mod testctl_chaos;
#[path = "testctl/commands.rs"]
mod testctl_commands;
#[path = "testctl/crashes.rs"]
mod testctl_crashes;
#[path = "testctl/discovery.rs"]
mod testctl_discovery;
#[path = "testctl/etcd.rs"]
mod testctl_etcd;
#[path = "testctl/outcomes.rs"]
mod testctl_outcomes;
#[path = "testctl/resources.rs"]
mod testctl_resources;
#[path = "testctl/scale.rs"]
mod testctl_scale;
#[path = "testctl/scenarios.rs"]
mod testctl_scenarios;
#[path = "testctl/simulation.rs"]
mod testctl_simulation;

use std::{
    path::{Path, PathBuf},
    process::ExitCode,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use testctl_artifacts::{
    Manifest, MonitorCommand, MonitorResult, MultiDomainHostArtifact, MultiDomainLogicArtifact,
    ScopedLeadershipArtifact, write_json, write_json_atomic, write_junit,
};
use testctl_commands::{cargo, cargo_test_exact, command, output};
use testctl_etcd::{
    assert_coordinator_not_displaced, coordinator_leader_from_etcd, find_etcd_leader,
    labeled_containers, require_label, run_etcd_acceptance, run_etcd_acceptance_with_endpoints,
    wait_for_etcd_failover, wait_for_file, wait_for_healthy_container, wait_for_running_container,
};
use testctl_outcomes::ScenarioRunner;
use testctl_scenarios::{wait_for_host_scope, wait_for_scope_across_hosts};
use testctl_simulation::{distributed_node, replay, simulate, soak};

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
    Scale,
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
        Profile::Scale => runner.run("sixty-four-node-convergence", || {
            testctl_scale::run(artifacts)
        }),
        Profile::Chaos => {
            runner.run("multi-domain-failover", || multi_domain_real(artifacts));
            runner.run("control-plane-store-outage-recovery", || {
                control_plane_store_outage_real(artifacts)
            });
            runner.run("membership-leader-hard-crash-recovery", || {
                testctl_crashes::membership_leader(artifacts)
            });
            runner.run("member-hard-crash-recovery", || {
                testctl_crashes::member(artifacts)
            });
            runner.run("etcd-hard-crash-recovery", || {
                testctl_crashes::etcd(artifacts)
            });
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
            "scale_expected_members": std::env::var("LATTICE_SCALE_EXPECTED_MEMBERS").ok(),
            "scale_startup_window_seconds": std::env::var("LATTICE_SCALE_STARTUP_WINDOW_SECONDS").ok(),
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
    let deadline = Instant::now() + Duration::from_millis(250);
    loop {
        match std::fs::read(path)
            .map_err(|error| error.to_string())
            .and_then(|encoded| serde_json::from_slice(&encoded).map_err(|error| error.to_string()))
        {
            Ok(logic) => return Ok(logic),
            Err(error) if Instant::now() >= deadline => return Err(error),
            Err(_) => std::thread::sleep(Duration::from_millis(10)),
        }
    }
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

fn control_plane_store_outage_real(artifacts: &Path) -> Result<(), String> {
    let run_id = std::env::var("LATTICE_RUN_ID")
        .map_err(|_| "control-plane outage requires LATTICE_RUN_ID".to_owned())?;
    let containers = labeled_containers(&run_id)?;
    let container = |needle: &str| {
        containers
            .lines()
            .find(|name| name.contains(needle) && !name.contains("runner"))
            .ok_or_else(|| format!("missing labeled {needle} container"))
    };
    let coordinator_names = [
        "domain-membership",
        "domain-alpha",
        "domain-beta",
        "domain-gamma",
        "domain-standby",
    ];
    let coordinators = coordinator_names
        .iter()
        .map(|name| container(name))
        .collect::<Result<Vec<_>, _>>()?;
    let etcd = container("etcd-single")?;
    for name in coordinators.iter().copied().chain(std::iter::once(etcd)) {
        require_label("container", name, &run_id)?;
    }

    let logic_paths = [
        artifacts.join("domain-logic-a.json"),
        artifacts.join("domain-logic-b.json"),
    ];
    for path in &logic_paths {
        wait_for_logic_ready(path, Duration::from_secs(30))?;
    }
    let before = logic_paths
        .iter()
        .map(|path| read_logic(path))
        .collect::<Result<Vec<_>, _>>()?;
    let before_term = before
        .iter()
        .filter_map(|logic| logic.membership_version.map(|version| version.term))
        .max()
        .ok_or_else(|| "logic nodes did not publish an initial membership term".to_owned())?;
    write_json(
        &artifacts.join("control-plane-store-outage-schedule.json"),
        &serde_json::json!({
            "coordinators": coordinator_names,
            "etcd": "etcd-single",
            "before_membership_term": before_term,
            "sequence": [
                "pause-all-coordinators",
                "wait-for-all-logic-to-revoke-ready",
                "hold-until-leader-leases-expire",
                "pause-etcd",
                "resume-coordinators-while-etcd-is-unavailable",
                "hold-through-store-operation-deadline",
                "resume-etcd",
                "wait-for-higher-term-ready",
            ],
        }),
    )?;

    let mut paused = Vec::new();
    let scenario = (|| {
        for coordinator in &coordinators {
            command("docker", &["pause", coordinator])?;
            paused.push(*coordinator);
        }
        let degraded = wait_for_logic_unready(&logic_paths, Duration::from_secs(20))?;

        // The leader lease TTL is ten seconds. By the time both logic nodes have revoked Ready,
        // six seconds have normally elapsed; retain the CPU stall long enough for the durable
        // leases to expire before introducing the store outage.
        std::thread::sleep(Duration::from_secs(6));
        command("docker", &["pause", etcd])?;
        paused.push(etcd);
        for coordinator in &coordinators {
            command("docker", &["unpause", coordinator])?;
            paused.retain(|paused| paused != coordinator);
        }

        // Etcd operations have a five-second deadline. Keep the store unavailable long enough for
        // resumed hosts to abandon their old in-memory leaders and sessions.
        std::thread::sleep(Duration::from_secs(6));
        command("docker", &["unpause", etcd])?;
        paused.retain(|paused| *paused != etcd);

        for path in &logic_paths {
            wait_for_logic_ready(path, Duration::from_secs(90))?;
        }
        let recovered = logic_paths
            .iter()
            .map(|path| read_logic(path))
            .collect::<Result<Vec<_>, _>>()?;
        for logic in &recovered {
            let version = logic.membership_version.ok_or_else(|| {
                format!("{} recovered without a membership version", logic.node_id)
            })?;
            if version.term <= before_term {
                return Err(format!(
                    "{} returned to Ready without a new Coordinator term: before={before_term}, after={}",
                    logic.node_id, version.term
                ));
            }
            if logic.members.is_empty() || logic.members.iter().any(|member| member.status != "Up")
            {
                return Err(format!(
                    "{} returned to Ready with an invalid member directory: {:?}",
                    logic.node_id, logic.members
                ));
            }
        }
        write_json(
            &artifacts.join("control-plane-store-outage-recovery.json"),
            &serde_json::json!({
                "before_membership_term": before_term,
                "degraded": degraded,
                "recovered": recovered,
                "faults": [
                    "pause-all-coordinators-past-heartbeat-timeout",
                    "expire-leader-leases",
                    "resume-coordinators-while-etcd-paused",
                    "restore-etcd",
                ],
            }),
        )
    })();

    let mut cleanup_errors = Vec::new();
    for container in paused.into_iter().rev() {
        if let Err(error) = command("docker", &["unpause", container]) {
            cleanup_errors.push(format!("{container}: {error}"));
        }
    }
    match (scenario, cleanup_errors.is_empty()) {
        (Ok(()), true) => Ok(()),
        (Err(error), true) => Err(error),
        (Ok(()), false) => Err(format!(
            "control-plane outage cleanup failed: {}",
            cleanup_errors.join("; ")
        )),
        (Err(error), false) => Err(format!(
            "{error}; cleanup failed: {}",
            cleanup_errors.join("; ")
        )),
    }
}

fn wait_for_logic_unready(
    paths: &[PathBuf],
    timeout: Duration,
) -> Result<Vec<MultiDomainLogicArtifact>, String> {
    let deadline = Instant::now() + timeout;
    loop {
        let snapshots = match paths
            .iter()
            .map(|path| read_logic(path))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(snapshots) => snapshots,
            Err(error) => {
                if Instant::now() >= deadline {
                    return Err(format!(
                        "logic artifacts remained unreadable during the control-plane outage: {error}"
                    ));
                }
                std::thread::sleep(Duration::from_millis(50));
                continue;
            }
        };
        if snapshots.iter().all(|logic| {
            logic.lifecycle != "Ready" || logic.domains.values().any(|state| state != "Ready")
        }) {
            return Ok(snapshots);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "logic nodes did not revoke Ready during the control-plane outage: {snapshots:?}"
            ));
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
    run_etcd_acceptance()?;
    let (leader, stopped_member_id) = find_etcd_leader(&members)?;
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

    let (hard_killed_leader, hard_killed_member_id) = find_etcd_leader(&members)?;
    require_label("container", hard_killed_leader, &run_id)?;
    command("docker", &["kill", hard_killed_leader])?;
    wait_for_etcd_failover(
        &members,
        hard_killed_leader,
        hard_killed_member_id,
        Duration::from_secs(30),
    )?;
    let hard_kill_surviving_endpoints = ["etcd1", "etcd2", "etcd3"]
        .into_iter()
        .filter(|name| !hard_killed_leader.contains(name))
        .map(|name| format!("http://{name}:2379"))
        .collect::<Vec<_>>()
        .join(",");
    let hard_kill_quorum_result =
        run_etcd_acceptance_with_endpoints(&hard_kill_surviving_endpoints);
    require_label("container", hard_killed_leader, &run_id)?;
    command("docker", &["start", hard_killed_leader])?;
    wait_for_healthy_container(hard_killed_leader, Duration::from_secs(60))?;
    hard_kill_quorum_result?;
    write_json(
        &artifacts.join("etcd-failover.json"),
        &serde_json::json!({
            "gracefully_stopped_leader": leader,
            "hard_killed_leader": hard_killed_leader,
            "quorum_remained_writable": true,
            "restarted_members_healthy": true,
        }),
    )?;

    // Coordinator artifact files are diagnostic snapshots and may lag the durable lease
    // record. Select and observe failover through etcd so the test always stops the leader
    // that is current after the preceding etcd disruptions.
    let initial_coordinator =
        wait_for_coordinator_leadership(leader, &run_id, None, 0, Duration::from_secs(120))?;
    let coordinator_container = coordinators
        .iter()
        .find(|container| container.contains(&initial_coordinator.node_id))
        .copied()
        .ok_or_else(|| "elected Coordinator container was not discoverable".to_owned())?;
    require_label("container", coordinator_container, &run_id)?;
    command("docker", &["stop", "--time", "1", coordinator_container])?;
    let replacement = wait_for_coordinator_leadership(
        leader,
        &run_id,
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
    etcd_member: &str,
    run_id: &str,
    excluded_node: Option<&str>,
    minimum_term: u64,
    timeout: Duration,
) -> Result<ScopedLeadershipArtifact, String> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(state) = coordinator_leader_from_etcd(etcd_member, run_id)
            && state.term > minimum_term
            && excluded_node.is_none_or(|excluded| state.node_id != excluded)
        {
            return Ok(state);
        }
        if Instant::now() >= deadline {
            return Err("Coordinator leadership did not reach the required term".to_owned());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}
