use std::fs::OpenOptions;
use std::io::Write;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use super::{
    MonitorCommand, MonitorResult, command, labeled_containers, output, require_label,
    wait_for_file, write_json, write_json_atomic,
};

pub(super) fn verify(artifacts: &Path) -> Result<(), String> {
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
        "client",
        "--reference",
        reference
            .to_str()
            .ok_or_else(|| "reference path is not UTF-8".to_owned())?,
    ];
    if expect_failure {
        arguments.push("--expect-failure");
    }
    let status = Command::new(distributed_node_executable()?)
        .args(arguments)
        .status()
        .map_err(|error| format!("failed to start distributed-node client: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("distributed-node client exited with {status}"))
    }
}

fn distributed_node_executable() -> Result<PathBuf, String> {
    let executable = std::env::current_exe()
        .map_err(|error| format!("failed to locate testctl executable: {error}"))?;
    let sibling =
        executable.with_file_name(format!("distributed-node{}", std::env::consts::EXE_SUFFIX));
    if sibling.is_file() {
        Ok(sibling)
    } else {
        Err(format!(
            "distributed-node is absent beside testctl at {}",
            sibling.display()
        ))
    }
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
        let child = Command::new(distributed_node_executable()?)
            .args(["monitor", "--reference", "/artifacts/server-ref.json"])
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
