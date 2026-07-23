use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use super::testctl_artifacts::{
    MembershipVersionArtifact, MultiDomainHostArtifact, MultiDomainLogicArtifact,
    ScopedLeadershipArtifact,
};
use super::{
    command, labeled_containers, output, read_logic, require_label, wait_for_healthy_container,
    wait_for_logic_unready, wait_for_running_container, write_json,
};

const HOSTS: [(&str, &str); 5] = [
    ("domain-membership", "domain-membership.json"),
    ("domain-alpha", "domain-alpha.json"),
    ("domain-beta", "domain-beta.json"),
    ("domain-gamma", "domain-gamma.json"),
    ("domain-standby", "domain-standby.json"),
];

const LOGIC_ARTIFACTS: [&str; 2] = ["domain-logic-a.json", "domain-logic-b.json"];

pub(super) fn membership_leader(artifacts: &Path) -> Result<(), String> {
    let run_id = run_id("membership leader hard crash")?;
    let containers = labeled_containers(&run_id)?;
    let initial = wait_for_scope(artifacts, "membership", 0, None, Duration::from_secs(30))?;
    let container = find_container(&containers, &initial.node_id)?;
    require_label("container", &container, &run_id)?;
    let host_artifact = host_artifact(artifacts, &initial.node_id)?;
    let old_process_incarnation = read_host(&host_artifact)?.incarnation;
    let logic_paths = logic_paths(artifacts);
    wait_for_valid_cluster(&logic_paths, Duration::from_secs(30))?;
    write_json(
        &artifacts.join("membership-leader-hard-crash-schedule.json"),
        &serde_json::json!({
            "leader": initial,
            "container": container,
            "sequence": [
                "docker-kill-current-membership-leader",
                "wait-for-higher-term-replacement",
                "restart-killed-container",
                "require-new-process-incarnation",
                "require-replacement-leader-stability",
                "require-all-logic-ready",
            ],
        }),
    )?;

    let mut stopped = false;
    let scenario = (|| {
        command("docker", &["kill", &container])?;
        stopped = true;
        let replacement = wait_for_scope(
            artifacts,
            "membership",
            initial.term,
            Some(&initial.node_id),
            Duration::from_secs(60),
        )?;

        require_label("container", &container, &run_id)?;
        command("docker", &["start", &container])?;
        wait_for_running_container(&container, Duration::from_secs(30))?;
        stopped = false;
        let restarted = wait_for_host_incarnation(
            &host_artifact,
            &initial.node_id,
            old_process_incarnation,
            Duration::from_secs(30),
        )?;
        assert_scope_stable(
            artifacts,
            "membership",
            &replacement,
            Duration::from_secs(3),
        )?;
        let recovered = wait_for_valid_cluster(&logic_paths, Duration::from_secs(90))?;
        write_json(
            &artifacts.join("membership-leader-hard-crash-recovery.json"),
            &serde_json::json!({
                "killed": initial,
                "replacement": replacement,
                "restarted_host": restarted,
                "recovered": recovered,
            }),
        )
    })();
    finish_with_restore(scenario, stopped, &container, &run_id)
}

pub(super) fn member(artifacts: &Path) -> Result<(), String> {
    let run_id = run_id("member hard crash")?;
    let containers = labeled_containers(&run_id)?;
    let container = find_container(&containers, "domain-logic-a")?;
    require_label("container", &container, &run_id)?;
    let logic_paths = logic_paths(artifacts);
    let before = wait_for_valid_cluster(&logic_paths, Duration::from_secs(30))?;
    let killed = before
        .iter()
        .find(|logic| logic.node_id == "domain-logic-a")
        .cloned()
        .ok_or_else(|| "domain-logic-a did not publish cluster state".to_owned())?;
    let observer_path = artifacts.join("domain-logic-b.json");
    let before_version = killed
        .membership_version
        .ok_or_else(|| "domain-logic-a did not publish a membership version".to_owned())?;
    write_json(
        &artifacts.join("member-hard-crash-schedule.json"),
        &serde_json::json!({
            "node_id": killed.node_id,
            "incarnation": killed.incarnation.to_string(),
            "container": container,
            "before_membership_version": before_version,
            "sequence": [
                "docker-kill-member",
                "wait-for-old-incarnation-removal",
                "restart-member",
                "require-new-incarnation",
                "require-single-up-record-for-new-incarnation",
                "require-all-logic-ready",
            ],
        }),
    )?;

    let mut stopped = false;
    let scenario = (|| {
        command("docker", &["kill", &container])?;
        stopped = true;
        let removed = wait_for_member_absent(
            &observer_path,
            "domain-logic-a",
            before_version,
            Duration::from_secs(45),
        )?;
        let removed_version = removed.membership_version.ok_or_else(|| {
            "observer removed the crashed member without publishing a version".to_owned()
        })?;

        require_label("container", &container, &run_id)?;
        command("docker", &["start", &container])?;
        wait_for_running_container(&container, Duration::from_secs(30))?;
        stopped = false;
        let restarted = wait_for_logic_incarnation(
            &artifacts.join("domain-logic-a.json"),
            killed.incarnation,
            Duration::from_secs(90),
        )?;
        let recovered = wait_for_member_rejoin(
            &logic_paths,
            "domain-logic-a",
            restarted.incarnation,
            removed_version,
            Duration::from_secs(90),
        )?;
        write_json(
            &artifacts.join("member-hard-crash-recovery.json"),
            &serde_json::json!({
                "killed": killed,
                "removed": removed,
                "restarted": restarted,
                "recovered": recovered,
            }),
        )
    })();
    finish_with_restore(scenario, stopped, &container, &run_id)
}

pub(super) fn etcd(artifacts: &Path) -> Result<(), String> {
    let run_id = run_id("etcd hard crash")?;
    let containers = labeled_containers(&run_id)?;
    let container = find_container(&containers, "etcd-single")?;
    require_label("container", &container, &run_id)?;
    let logic_paths = logic_paths(artifacts);
    let before = wait_for_valid_cluster(&logic_paths, Duration::from_secs(30))?;
    let before_term = before
        .iter()
        .filter_map(|logic| logic.membership_version.map(|version| version.term))
        .max()
        .ok_or_else(|| "logic nodes did not publish a membership term".to_owned())?;
    write_json(
        &artifacts.join("etcd-hard-crash-schedule.json"),
        &serde_json::json!({
            "container": "etcd-single",
            "before_membership_term": before_term,
            "sequence": [
                "docker-kill-etcd",
                "require-all-logic-to-revoke-ready",
                "hold-through-store-operation-deadline",
                "restart-etcd-with-existing-data-volume",
                "require-etcd-healthy",
                "require-higher-term-membership",
                "require-all-logic-ready",
            ],
        }),
    )?;

    let mut stopped = false;
    let scenario = (|| {
        command("docker", &["kill", &container])?;
        stopped = true;
        let degraded = wait_for_logic_unready(&logic_paths, Duration::from_secs(20))?;

        // This is part of the injected fault, not synchronization: the store stays down beyond
        // the five-second operation deadline so in-memory leaders must abandon stale sessions.
        std::thread::sleep(Duration::from_secs(6));
        require_label("container", &container, &run_id)?;
        command("docker", &["start", &container])?;
        wait_for_healthy_container(&container, Duration::from_secs(60))?;
        stopped = false;

        let recovered = wait_for_valid_cluster(&logic_paths, Duration::from_secs(90))?;
        for logic in &recovered {
            let version = logic.membership_version.ok_or_else(|| {
                format!("{} recovered without a membership version", logic.node_id)
            })?;
            if version.term <= before_term {
                return Err(format!(
                    "{} recovered after etcd restart without a new Coordinator term: before={before_term}, after={}",
                    logic.node_id, version.term
                ));
            }
        }
        write_json(
            &artifacts.join("etcd-hard-crash-recovery.json"),
            &serde_json::json!({
                "before_membership_term": before_term,
                "before": before,
                "degraded": degraded,
                "recovered": recovered,
                "data_volume_reused": true,
            }),
        )
    })();
    finish_with_restore(scenario, stopped, &container, &run_id)
}

fn run_id(scenario: &str) -> Result<String, String> {
    std::env::var("LATTICE_RUN_ID").map_err(|_| format!("{scenario} requires LATTICE_RUN_ID"))
}

fn find_container(containers: &str, needle: &str) -> Result<String, String> {
    containers
        .lines()
        .find(|name| name.contains(needle) && !name.contains("runner"))
        .map(str::to_owned)
        .ok_or_else(|| format!("missing labeled {needle} container"))
}

fn logic_paths(artifacts: &Path) -> Vec<PathBuf> {
    LOGIC_ARTIFACTS
        .iter()
        .map(|name| artifacts.join(name))
        .collect()
}

fn host_artifact(artifacts: &Path, node_id: &str) -> Result<PathBuf, String> {
    HOSTS
        .iter()
        .find(|(candidate, _)| *candidate == node_id)
        .map(|(_, artifact)| artifacts.join(artifact))
        .ok_or_else(|| format!("membership leader {node_id} has no host artifact"))
}

fn read_host(path: &Path) -> Result<MultiDomainHostArtifact, String> {
    serde_json::from_slice(&std::fs::read(path).map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())
}

fn latest_scope(artifacts: &Path, scope: &str) -> Result<ScopedLeadershipArtifact, String> {
    let mut leaders = HOSTS
        .iter()
        .filter_map(|(_, name)| read_host(&artifacts.join(name)).ok())
        .filter_map(|host| host.scopes.get(scope).cloned())
        .collect::<Vec<_>>();
    leaders.sort_by_key(|leader| leader.term);
    let latest = leaders
        .last()
        .cloned()
        .ok_or_else(|| format!("no host published {scope} leadership"))?;
    if leaders.iter().any(|leader| {
        leader.term == latest.term
            && (leader.node_id != latest.node_id || leader.incarnation != latest.incarnation)
    }) {
        return Err(format!(
            "hosts published conflicting {scope} leaders for term {}",
            latest.term
        ));
    }
    Ok(latest)
}

fn wait_for_scope(
    artifacts: &Path,
    scope: &str,
    minimum_term: u64,
    excluded_node: Option<&str>,
    timeout: Duration,
) -> Result<ScopedLeadershipArtifact, String> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(leader) = latest_scope(artifacts, scope)
            && leader.term > minimum_term
            && excluded_node.is_none_or(|excluded| leader.node_id != excluded)
        {
            return Ok(leader);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{scope} leadership did not advance above term {minimum_term}"
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_host_incarnation(
    path: &Path,
    node_id: &str,
    old_incarnation: u128,
    timeout: Duration,
) -> Result<MultiDomainHostArtifact, String> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(host) = read_host(path)
            && host.node_id == node_id
            && host.incarnation != old_incarnation
        {
            return Ok(host);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{node_id} did not publish a new process incarnation after restart"
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn assert_scope_stable(
    artifacts: &Path,
    scope: &str,
    expected: &ScopedLeadershipArtifact,
    duration: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        let current = latest_scope(artifacts, scope)?;
        if current.node_id != expected.node_id
            || current.incarnation != expected.incarnation
            || current.term != expected.term
        {
            return Err(format!(
                "{scope} leader changed after the killed host restarted: expected {expected:?}, found {current:?}"
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}

fn wait_for_valid_cluster(
    paths: &[PathBuf],
    timeout: Duration,
) -> Result<Vec<MultiDomainLogicArtifact>, String> {
    let deadline = Instant::now() + timeout;
    loop {
        let snapshots = paths
            .iter()
            .map(|path| read_logic(path))
            .collect::<Result<Vec<_>, _>>()?;
        if cluster_is_valid(&snapshots) {
            return Ok(snapshots);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "logic nodes did not converge to valid Ready membership: {snapshots:?}"
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn cluster_is_valid(snapshots: &[MultiDomainLogicArtifact]) -> bool {
    let Some(first) = snapshots.first() else {
        return false;
    };
    snapshots.len() == LOGIC_ARTIFACTS.len()
        && snapshots.iter().all(logic_is_valid)
        && snapshots.iter().all(|snapshot| {
            snapshot.membership_version == first.membership_version
                && snapshot.members == first.members
                && snapshot.members.iter().any(|member| {
                    member.node_id == snapshot.node_id
                        && member.incarnation == snapshot.incarnation
                        && member.status == "Up"
                })
        })
        && ["domain-logic-a", "domain-logic-b"]
            .into_iter()
            .all(|node_id| {
                first
                    .members
                    .iter()
                    .filter(|member| member.node_id == node_id)
                    .count()
                    == 1
            })
        && first.members.len() == LOGIC_ARTIFACTS.len()
}

fn logic_is_valid(logic: &MultiDomainLogicArtifact) -> bool {
    logic.lifecycle == "Ready"
        && [
            "domain-alpha",
            "domain-beta",
            "domain-gamma",
            "domain-delta",
        ]
        .into_iter()
        .all(|domain| logic.domains.get(domain).map(String::as_str) == Some("Ready"))
        && logic.membership_version.is_some()
        && !logic.members.is_empty()
        && logic.members.iter().all(|member| member.status == "Up")
}

fn wait_for_member_absent(
    observer: &Path,
    node_id: &str,
    minimum_version: MembershipVersionArtifact,
    timeout: Duration,
) -> Result<MultiDomainLogicArtifact, String> {
    let deadline = Instant::now() + timeout;
    loop {
        let snapshot = read_logic(observer)?;
        if logic_is_valid(&snapshot)
            && snapshot
                .membership_version
                .is_some_and(|version| version > minimum_version)
            && snapshot
                .members
                .iter()
                .all(|member| member.node_id != node_id)
        {
            return Ok(snapshot);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{node_id} was not removed from membership after its hard crash: {snapshot:?}"
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_logic_incarnation(
    path: &Path,
    old_incarnation: u128,
    timeout: Duration,
) -> Result<MultiDomainLogicArtifact, String> {
    let deadline = Instant::now() + timeout;
    loop {
        let snapshot = read_logic(path)?;
        if snapshot.incarnation != old_incarnation && logic_is_valid(&snapshot) {
            return Ok(snapshot);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{} did not return Ready with a new incarnation: {snapshot:?}",
                snapshot.node_id
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_member_rejoin(
    paths: &[PathBuf],
    node_id: &str,
    incarnation: u128,
    minimum_version: MembershipVersionArtifact,
    timeout: Duration,
) -> Result<Vec<MultiDomainLogicArtifact>, String> {
    let deadline = Instant::now() + timeout;
    loop {
        let snapshots = paths
            .iter()
            .map(|path| read_logic(path))
            .collect::<Result<Vec<_>, _>>()?;
        let converged = cluster_is_valid(&snapshots)
            && snapshots.iter().all(|snapshot| {
                let members = snapshot
                    .members
                    .iter()
                    .filter(|member| member.node_id == node_id)
                    .collect::<Vec<_>>();
                logic_is_valid(snapshot)
                    && snapshot
                        .membership_version
                        .is_some_and(|version| version > minimum_version)
                    && members.len() == 1
                    && members[0].incarnation == incarnation
                    && members[0].status == "Up"
            });
        if converged {
            return Ok(snapshots);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{node_id} did not rejoin once with incarnation {incarnation}: {snapshots:?}"
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn finish_with_restore(
    scenario: Result<(), String>,
    stopped: bool,
    container: &str,
    run_id: &str,
) -> Result<(), String> {
    let restore = if stopped {
        restore_container(container, run_id)
    } else {
        Ok(())
    };
    match (scenario, restore) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(restore)) => Err(format!("container restore failed: {restore}")),
        (Err(error), Err(restore)) => Err(format!("{error}; container restore failed: {restore}")),
    }
}

fn restore_container(container: &str, run_id: &str) -> Result<(), String> {
    require_label("container", container, run_id)?;
    let running = output(
        "docker",
        &[
            "container",
            "inspect",
            "--format",
            "{{.State.Running}}",
            container,
        ],
    )?;
    if running != "true" {
        command("docker", &["start", container])?;
    }
    wait_for_running_container(container, Duration::from_secs(30))?;
    let has_healthcheck = output(
        "docker",
        &[
            "container",
            "inspect",
            "--format",
            "{{if .State.Health}}true{{else}}false{{end}}",
            container,
        ],
    )?;
    if has_healthcheck == "true" {
        wait_for_healthy_container(container, Duration::from_secs(60))
    } else {
        Ok(())
    }
}
