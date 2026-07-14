#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

#[path = "testctl/discovery.rs"]
mod testctl_discovery;

use std::fs::OpenOptions;
use std::io::Write;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand, ValueEnum};
use lattice_sim::lifecycle::{LifecycleScenario, LifecycleScenarioConfig};
use lattice_sim::scenario::Scenario;
use lattice_sim::scenario::ScenarioConfig;
use lattice_sim::trace::TraceJournal;
use serde::{Deserialize, Serialize};

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

#[derive(Serialize)]
struct Manifest {
    profile: Profile,
    seed: u64,
    source_commit: String,
    source_status: String,
    source_fingerprint: String,
    started_unix_millis: u128,
    elapsed_millis: u128,
    success: bool,
    replay: String,
    platform: String,
    pinned_images: String,
    scenarios: Vec<&'static str>,
    configuration: serde_json::Value,
}

#[derive(Serialize)]
struct ResourceSample {
    elapsed_millis: u128,
    open_file_descriptors: Option<usize>,
    resident_memory_kib: Option<u64>,
    threads: Option<u64>,
    process_status: Option<String>,
}

#[derive(Serialize)]
struct MonitorCommand {
    sequence: u64,
    stop: bool,
}

#[derive(serde::Deserialize)]
struct MonitorResult {
    success: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct CoordinatorLeadershipArtifact {
    node_id: String,
    term: u64,
    incarnation: u128,
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
    let mut resource_samples = vec![resource_sample(timer.elapsed())];
    let result = match profile {
        Profile::Quality => (|| {
            command("scripts/check-structure.sh", &[])?;
            commands(&[
                &["fmt", "--all", "--", "--check"],
                &[
                    "clippy",
                    "--workspace",
                    "--all-targets",
                    "--all-features",
                    "--",
                    "-D",
                    "warnings",
                ],
                &["test", "--workspace", "--all-features"],
            ])
        })(),
        Profile::Sim => simulate(seed, artifacts),
        Profile::Model => cargo(&[
            "test",
            "-p",
            "lattice-sim",
            "bounded_state_explorer_checks_every_transition",
        ]),
        Profile::E2e => (|| {
            testctl_discovery::verify(artifacts)?;
            commands(&[
                &[
                    "test",
                    "-p",
                    "lattice-placement",
                    "--test",
                    "etcd_acceptance",
                    "--",
                    "--nocapture",
                ],
                &[
                    "test",
                    "-p",
                    "lattice-remoting",
                    "real_tcp_endpoint_establishes_all_lanes_and_delivers_ask",
                ],
                &[
                    "test",
                    "-p",
                    "lattice-remoting",
                    "real_mutual_tls_socket_verifies_both_node_identities",
                ],
                &[
                    "test",
                    "-p",
                    "lattice-service",
                    "remote_entity_ask_reaches_only_claimed_owner",
                ],
            ])
        })(),
        Profile::E2eHaEtcd => ha_etcd_real(artifacts),
        Profile::Chaos => (|| {
            chaos_real(artifacts)?;
            for current in seed..seed.saturating_add(32) {
                simulate(current, artifacts)?;
            }
            Ok(())
        })(),
        Profile::K8s => command("sh", &["tests/distributed/k8s/verify.sh"]),
        Profile::Soak => soak(
            seed,
            duration_seconds,
            artifacts,
            timer,
            &mut resource_samples,
        ),
    };
    let cleanup_result = if matches!(profile, Profile::E2eHaEtcd | Profile::Chaos | Profile::K8s)
        && Path::new("/var/run/docker.sock").exists()
    {
        command("sh", &["scripts/docker-image-lifecycle.sh", "cleanup"])
    } else {
        Ok(())
    };
    let result = match (result, cleanup_result) {
        (Err(error), _) => Err(error),
        (Ok(()), cleanup) => cleanup,
    };
    resource_samples.push(resource_sample(timer.elapsed()));
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
        scenarios: profile_scenarios(profile),
        configuration: serde_json::json!({
            "duration_seconds": duration_seconds,
            "artifact_directory": artifacts,
        }),
    };
    write_json(&artifacts.join("manifest.json"), &manifest)?;
    write_junit(&artifacts.join("junit.xml"), profile, success)?;
    write_json(&artifacts.join("resource-samples.json"), &resource_samples)?;
    result
}

fn profile_scenarios(profile: Profile) -> Vec<&'static str> {
    match profile {
        Profile::Quality => vec!["fmt", "clippy", "workspace-tests"],
        Profile::Sim => vec!["seeded-production-reducers"],
        Profile::Model => vec!["bounded-state-explorer"],
        Profile::E2e => vec![
            "exact-actor-ref-child-watch",
            "gateway-entity-ref-remote-shard",
            "single-member-etcd",
            "tcp",
            "mutual-tls",
            "claimed-entity-ref",
            "static-discovery-cluster-join",
            "config-store-discovery-cluster-join",
            "graceful-member-leave",
        ],
        Profile::E2eHaEtcd => vec!["etcd-leader-failover", "coordinator-plan-recovery"],
        Profile::Chaos => vec![
            "pause-resume",
            "netem-delay-loss",
            "same-incarnation-reconnect",
            "network-partition-heal",
            "kill-start",
            "same-address-restart",
            "stale-reference-rejection",
            "seed-corpus",
        ],
        Profile::K8s => vec!["probes", "dns", "rollout", "pdb-eviction"],
        Profile::Soak => vec!["bounded-seeded-soak"],
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
        Duration::from_secs(30),
    )?;
    require_label("container", coordinator_container, &run_id)?;
    command("docker", &["start", coordinator_container])?;
    wait_for_running_container(coordinator_container, Duration::from_secs(30))?;
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

    commands(&[
        &[
            "test",
            "-p",
            "lattice-placement",
            "leader_recovery_resumes_handoff_and_cancels_stale_pending_move",
        ],
        &[
            "test",
            "-p",
            "lattice-placement",
            "singleton_owner_loss_recovers_forward_after_leader_restart",
        ],
    ])?;
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
) -> Result<CoordinatorLeadershipArtifact, String> {
    let deadline = Instant::now() + timeout;
    loop {
        for name in ["coordinator-a.json", "coordinator-b.json"] {
            let path = artifacts.join(name);
            let Ok(bytes) = std::fs::read(path) else {
                continue;
            };
            let Ok(state) = serde_json::from_slice::<CoordinatorLeadershipArtifact>(&bytes) else {
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
        std::thread::yield_now();
    }
}

fn assert_coordinator_not_displaced(
    etcd_member: &str,
    run_id: &str,
    expected: &CoordinatorLeadershipArtifact,
    stable_period: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + stable_period;
    while Instant::now() < deadline {
        let (node_id, term) = coordinator_leader_from_etcd(etcd_member, run_id)?;
        if node_id != expected.node_id || term < expected.term {
            return Err(format!(
                "Coordinator leader changed from {} term {} to {node_id} term {term}",
                expected.node_id, expected.term
            ));
        }
        std::thread::yield_now();
    }
    Ok(())
}

fn coordinator_leader_from_etcd(etcd_member: &str, run_id: &str) -> Result<(String, u64), String> {
    let key = format!("/lattice-ha/{run_id}/coordinator/leader");
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
        std::thread::yield_now();
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
        std::thread::yield_now();
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
        std::thread::yield_now();
    }
}

fn chaos_real(artifacts: &Path) -> Result<(), String> {
    let run_id =
        std::env::var("LATTICE_RUN_ID").map_err(|_| "chaos requires LATTICE_RUN_ID".to_owned())?;
    let network = std::env::var("LATTICE_DOCKER_NETWORK")
        .map_err(|_| "chaos requires LATTICE_DOCKER_NETWORK".to_owned())?;
    let containers = labeled_containers(&run_id)?;
    let server = containers
        .lines()
        .find(|name| name.contains("fixture-server"))
        .ok_or_else(|| "labeled fixture server is absent".to_owned())?;
    require_label("container", server, &run_id)?;
    require_label("network", &network, &run_id)?;
    write_json(
        &artifacts.join("fault-schedule.json"),
        &serde_json::json!({
            "run_id": run_id,
            "operations": [
                "pause-server",
                "resume-server",
                "netem-delay-150ms",
                "netem-loss-100-percent",
                "disconnect-network",
                "assert-partition-failure",
                "reconnect-same-address",
                "kill-server",
                "start-server-new-incarnation",
                "assert-killed-reference-stale",
                "restart-new-incarnation",
                "assert-stale-reference-failure",
                "assert-new-reference-recovery"
            ]
        }),
    )?;

    let mut monitor = ChaosMonitor::start()?;
    command("docker", &["pause", server])?;
    command("docker", &["unpause", server])?;
    monitor.probe(Some(true))?;

    require_label("container", server, &run_id)?;
    command(
        "docker",
        &[
            "exec", server, "tc", "qdisc", "add", "dev", "eth0", "root", "netem", "delay", "150ms",
        ],
    )?;
    monitor.probe(None)?;
    require_label("container", server, &run_id)?;
    command(
        "docker",
        &["exec", server, "tc", "qdisc", "del", "dev", "eth0", "root"],
    )?;
    monitor.recover(Duration::from_secs(30))?;

    require_label("container", server, &run_id)?;
    command(
        "docker",
        &[
            "exec", server, "tc", "qdisc", "add", "dev", "eth0", "root", "netem", "loss", "100%",
        ],
    )?;
    monitor.probe(Some(false))?;
    require_label("container", server, &run_id)?;
    command(
        "docker",
        &["exec", server, "tc", "qdisc", "del", "dev", "eth0", "root"],
    )?;
    monitor.recover(Duration::from_secs(30))?;

    let server_address = pin_container_host(server, "fixture-server", &run_id)?.to_string();
    command("docker", &["network", "disconnect", &network, server])?;
    monitor.probe(Some(false))?;
    command(
        "docker",
        &[
            "network",
            "connect",
            "--ip",
            &server_address,
            "--alias",
            "fixture-server",
            &network,
            server,
        ],
    )?;
    monitor.recover(Duration::from_secs(30))?;
    monitor.stop()?;

    let killed = artifacts.join("killed-server-ref.json");
    std::fs::copy("/artifacts/server-ref.json", &killed).map_err(|error| error.to_string())?;
    let old = std::fs::read(&killed).map_err(|error| error.to_string())?;
    require_label("container", server, &run_id)?;
    command("docker", &["kill", server])?;
    require_label("container", server, &run_id)?;
    command("docker", &["start", server])?;
    wait_for_new_incarnation(&old)?;
    distributed_client(&killed, true)?;
    distributed_client(Path::new("/artifacts/server-ref.json"), false)?;

    let stale = artifacts.join("stale-server-ref.json");
    std::fs::copy("/artifacts/server-ref.json", &stale).map_err(|error| error.to_string())?;
    let old = std::fs::read(&stale).map_err(|error| error.to_string())?;
    require_label("container", server, &run_id)?;
    command("docker", &["restart", server])?;
    wait_for_new_incarnation(&old)?;
    distributed_client(&stale, true)?;
    distributed_client(Path::new("/artifacts/server-ref.json"), false)
}

fn wait_for_new_incarnation(old: &[u8]) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if std::fs::read("/artifacts/server-ref.json").is_ok_and(|current| current != old) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("server restart did not publish a new incarnation".to_owned());
        }
        std::thread::yield_now();
    }
}

fn pin_container_host(container: &str, host: &str, run_id: &str) -> Result<IpAddr, String> {
    let address = output(
        "docker",
        &[
            "container",
            "inspect",
            "--format",
            "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
            container,
        ],
    )?;
    let address = address
        .parse::<IpAddr>()
        .map_err(|error| format!("invalid fixture address {address}: {error}"))?;
    let mut hosts = OpenOptions::new()
        .append(true)
        .open("/etc/hosts")
        .map_err(|error| format!("failed to pin fixture host: {error}"))?;
    writeln!(hosts, "{address} {host} # lattice test run {run_id}")
        .map_err(|error| format!("failed to pin fixture host: {error}"))?;
    Ok(address)
}

fn distributed_client(reference: &Path, expect_failure: bool) -> Result<(), String> {
    let mut arguments = vec![
        "run",
        "-p",
        "lattice-sim",
        "--bin",
        "distributed-node",
        "--",
        "client",
        "--reference",
        reference
            .to_str()
            .ok_or_else(|| "reference path is not UTF-8".to_owned())?,
    ];
    if expect_failure {
        arguments.push("--expect-failure");
    }
    cargo(&arguments)
}

struct ChaosMonitor {
    child: Option<Child>,
    next_sequence: u64,
}

impl ChaosMonitor {
    fn start() -> Result<Self, String> {
        for path in [
            "/artifacts/monitor-ready.json",
            "/artifacts/monitor-command.json",
        ] {
            let _ = std::fs::remove_file(path);
        }
        for sequence in 1..=16 {
            let _ = std::fs::remove_file(format!("/artifacts/monitor-result-{sequence}.json"));
        }
        let child = Command::new("cargo")
            .args([
                "run",
                "-p",
                "lattice-sim",
                "--bin",
                "distributed-node",
                "--",
                "monitor",
                "--reference",
                "/artifacts/server-ref.json",
            ])
            .spawn()
            .map_err(|error| format!("failed to start chaos monitor: {error}"))?;
        wait_for_file(
            Path::new("/artifacts/monitor-ready.json"),
            Duration::from_secs(60),
        )?;
        Ok(Self {
            child: Some(child),
            next_sequence: 1,
        })
    }

    fn probe(&mut self, expected: Option<bool>) -> Result<bool, String> {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        write_json_atomic(
            Path::new("/artifacts/monitor-command.json"),
            &MonitorCommand {
                sequence,
                stop: false,
            },
        )?;
        let path = PathBuf::from(format!("/artifacts/monitor-result-{sequence}.json"));
        wait_for_file(&path, Duration::from_secs(15))?;
        let result: MonitorResult =
            serde_json::from_slice(&std::fs::read(&path).map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())?;
        if expected.is_some_and(|expected| expected != result.success) {
            return Err(format!(
                "chaos monitor probe {sequence} success={} did not match expected={expected:?}",
                result.success
            ));
        }
        Ok(result.success)
    }

    fn recover(&mut self, timeout: Duration) -> Result<(), String> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.probe(None)? {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!("chaos monitor did not recover within {timeout:?}"));
            }
            std::thread::yield_now();
        }
    }

    fn stop(&mut self) -> Result<(), String> {
        write_json_atomic(
            Path::new("/artifacts/monitor-command.json"),
            &MonitorCommand {
                sequence: u64::MAX,
                stop: true,
            },
        )?;
        let status = self
            .child
            .take()
            .ok_or_else(|| "chaos monitor already stopped".to_owned())?
            .wait()
            .map_err(|error| error.to_string())?;
        if status.success() {
            Ok(())
        } else {
            Err(format!("chaos monitor exited with {status}"))
        }
    }
}

impl Drop for ChaosMonitor {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
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
        std::thread::yield_now();
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
    simulate_to(seed, &artifacts.join(format!("trace-{seed}.json")))
}

fn simulate_to(seed: u64, trace_path: &Path) -> Result<(), String> {
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
    lifecycle
        .trace
        .write_json(&trace_path.with_file_name(format!("lifecycle-trace-{seed}.json")))
        .map_err(|error| error.to_string())?;
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
        simulate_to(current, &rolling_trace)?;
        current = current.saturating_add(1);
        if Instant::now() >= next_sample {
            samples.push(resource_sample(timer.elapsed()));
            next_sample = Instant::now() + Duration::from_secs(1);
        }
    }
    let final_trace = artifacts.join(format!("trace-{}.json", current.saturating_sub(1)));
    std::fs::copy(&rolling_trace, final_trace).map_err(|error| error.to_string())?;
    assert_resource_growth(samples, &resource_sample(timer.elapsed()))
}

fn resource_sample(elapsed: Duration) -> ResourceSample {
    let process_status = std::fs::read_to_string("/proc/self/status").ok();
    ResourceSample {
        elapsed_millis: elapsed.as_millis(),
        open_file_descriptors: std::fs::read_dir("/proc/self/fd")
            .ok()
            .map(|entries| entries.count()),
        resident_memory_kib: status_value(&process_status, "VmRSS:"),
        threads: status_value(&process_status, "Threads:"),
        process_status,
    }
}

fn status_value(status: &Option<String>, key: &str) -> Option<u64> {
    status.as_deref()?.lines().find_map(|line| {
        line.strip_prefix(key)?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    })
}

fn assert_resource_growth(
    samples: &[ResourceSample],
    final_sample: &ResourceSample,
) -> Result<(), String> {
    let initial = samples
        .first()
        .ok_or_else(|| "soak has no initial resource sample".to_owned())?;
    if let (Some(before), Some(after)) = (
        initial.open_file_descriptors,
        final_sample.open_file_descriptors,
    ) && after > before.saturating_add(32)
    {
        return Err(format!(
            "soak file descriptors grew from {before} to {after}"
        ));
    }
    if let (Some(before), Some(after)) = (
        initial.resident_memory_kib,
        final_sample.resident_memory_kib,
    ) && after > before.saturating_add(128 * 1024)
    {
        return Err(format!("soak RSS grew from {before} KiB to {after} KiB"));
    }
    if let (Some(before), Some(after)) = (initial.threads, final_sample.threads)
        && after > before.saturating_add(16)
    {
        return Err(format!("soak threads grew from {before} to {after}"));
    }
    Ok(())
}

fn replay(path: &Path) -> Result<(), String> {
    let expected = TraceJournal::read_json(path).map_err(|error| error.to_string())?;
    let directory = path.parent().unwrap_or_else(|| Path::new("."));
    simulate(expected.seed, directory)?;
    let actual = TraceJournal::read_json(&directory.join(format!("trace-{}.json", expected.seed)))
        .map_err(|error| error.to_string())?;
    if actual == expected {
        Ok(())
    } else {
        Err("replayed trace differs from artifact".to_owned())
    }
}

fn commands(commands: &[&[&str]]) -> Result<(), String> {
    for arguments in commands {
        cargo(arguments)?;
    }
    Ok(())
}

fn cargo(arguments: &[&str]) -> Result<(), String> {
    command("cargo", arguments)
}

fn command(program: &str, arguments: &[&str]) -> Result<(), String> {
    let status = Command::new(program)
        .args(arguments)
        .status()
        .map_err(|error| format!("failed to start {program}: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{program} exited with {status}"))
    }
}

fn output(program: &str, arguments: &[&str]) -> Result<String, String> {
    let output = Command::new(program)
        .args(arguments)
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!("{program} exited with {}", output.status));
    }
    String::from_utf8(output.stdout)
        .map(|value| value.trim().to_owned())
        .map_err(|error| error.to_string())
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let encoded = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    std::fs::write(path, encoded).map_err(|error| error.to_string())
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let encoded = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    std::fs::write(&temporary, encoded).map_err(|error| error.to_string())?;
    std::fs::rename(temporary, path).map_err(|error| error.to_string())
}

fn write_junit(path: &Path, profile: Profile, success: bool) -> Result<(), String> {
    let failure = if success {
        String::new()
    } else {
        "<failure message=\"profile failed\"/>".to_owned()
    };
    let xml = format!(
        "<testsuite name=\"lattice-{profile:?}\" tests=\"1\" failures=\"{}\"><testcase name=\"{profile:?}\">{failure}</testcase></testsuite>\n",
        usize::from(!success)
    );
    std::fs::write(path, xml).map_err(|error| error.to_string())
}
