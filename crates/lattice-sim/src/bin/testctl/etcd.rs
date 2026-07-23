use std::{
    path::Path,
    process::Command,
    time::{Duration, Instant},
};

use serde::Deserialize;

use super::{testctl_artifacts::ScopedLeadershipArtifact, testctl_commands::output};

pub(super) fn assert_coordinator_not_displaced(
    etcd_member: &str,
    run_id: &str,
    expected: &ScopedLeadershipArtifact,
    stable_period: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + stable_period;
    let mut observed = false;
    while Instant::now() < deadline {
        let Ok(current) = coordinator_leader_from_etcd(etcd_member, run_id) else {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        };
        observed = true;
        if current.node_id != expected.node_id || current.term < expected.term {
            return Err(format!(
                "Coordinator leader changed from {} term {} to {} term {}",
                expected.node_id, expected.term, current.node_id, current.term
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

pub(super) fn coordinator_leader_from_etcd(
    etcd_member: &str,
    run_id: &str,
) -> Result<ScopedLeadershipArtifact, String> {
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
    decode_coordinator_leader(&encoded)
}

#[derive(Deserialize)]
struct DurableNode {
    node_id: String,
    incarnation: u128,
}

#[derive(Deserialize)]
struct DurableLeader {
    node: DurableNode,
    term: u64,
}

fn decode_coordinator_leader(encoded: &str) -> Result<ScopedLeadershipArtifact, String> {
    let value: DurableLeader = serde_json::from_str(encoded).map_err(|error| error.to_string())?;
    Ok(ScopedLeadershipArtifact {
        node_id: value.node.node_id,
        term: value.term,
        incarnation: value.node.incarnation,
    })
}

pub(super) fn wait_for_etcd_failover(
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

pub(super) fn find_etcd_leader<'a>(members: &[&'a str]) -> Result<(&'a str, u64), String> {
    members
        .iter()
        .find_map(|member| match etcd_member_status(member) {
            Ok((member_id, leader_id)) if member_id == leader_id => Some(Ok((*member, member_id))),
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .transpose()?
        .ok_or_else(|| "etcd leader was not discoverable".to_owned())
}

pub(super) fn run_etcd_acceptance() -> Result<(), String> {
    run_etcd_acceptance_command(Command::new("cargo"))
}

pub(super) fn run_etcd_acceptance_with_endpoints(endpoints: &str) -> Result<(), String> {
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

pub(super) fn labeled_containers(run_id: &str) -> Result<String, String> {
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

pub(super) fn wait_for_healthy_container(container: &str, timeout: Duration) -> Result<(), String> {
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

pub(super) fn wait_for_running_container(container: &str, timeout: Duration) -> Result<(), String> {
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

pub(super) fn wait_for_file(path: &Path, timeout: Duration) -> Result<(), String> {
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

pub(super) fn require_label(kind: &str, name: &str, run_id: &str) -> Result<(), String> {
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

#[cfg(test)]
mod tests {
    use super::decode_coordinator_leader;

    #[test]
    fn etcd_leader_record_accepts_a_full_width_numeric_incarnation() {
        let decoded = decode_coordinator_leader(
            r#"{"node":{"node_id":"coordinator","incarnation":340282366920938463463374607431768211455},"term":9}"#,
        )
        .unwrap();
        assert_eq!(decoded.node_id, "coordinator");
        assert_eq!(decoded.term, 9);
        assert_eq!(decoded.incarnation, u128::MAX);
    }
}
