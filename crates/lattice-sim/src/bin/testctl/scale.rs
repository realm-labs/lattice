use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::{Duration, Instant},
};

use serde::Serialize;

use super::testctl_artifacts::{
    MemberArtifact, MembershipVersionArtifact, ScaleNodeArtifact, write_json,
};
#[cfg(test)]
use super::testctl_artifacts::{ProcessResourceArtifact, RingArtifact};

#[derive(Serialize)]
struct ConvergenceArtifact {
    expected_members: usize,
    membership_coordinator: &'static str,
    logic_nodes: usize,
    membership_term: u64,
    membership_revision: u64,
    convergence_millis: u128,
    startup_window_seconds: u64,
    metrics: ScaleMetricsArtifact,
    members: Vec<MemberArtifact>,
}

#[derive(Serialize)]
struct ScaleMetricsArtifact {
    maximum_join_millis: u128,
    maximum_ring_millis: u128,
    total_resident_memory_kib: u64,
    maximum_resident_memory_kib: u64,
    maximum_threads: u64,
    maximum_open_file_descriptors: usize,
    total_associations: usize,
    maximum_associations: usize,
    total_attached_lanes: usize,
    maximum_attached_lanes: usize,
}

pub(super) fn run(artifacts: &Path) -> Result<(), String> {
    let expected_members = std::env::var("LATTICE_SCALE_EXPECTED_MEMBERS")
        .unwrap_or_else(|_| "64".to_owned())
        .parse::<usize>()
        .map_err(|error| format!("invalid LATTICE_SCALE_EXPECTED_MEMBERS: {error}"))?;
    if expected_members == 0 {
        return Err("scale topology needs at least one member".to_owned());
    }
    let expected_logic = expected_members;
    let startup_window_seconds = std::env::var("LATTICE_SCALE_STARTUP_WINDOW_SECONDS")
        .unwrap_or_else(|_| "0".to_owned())
        .parse::<u64>()
        .map_err(|error| format!("invalid LATTICE_SCALE_STARTUP_WINDOW_SECONDS: {error}"))?;
    let directory = artifacts.join("scale");
    let started = Instant::now();
    let deadline = started + Duration::from_secs(600);
    loop {
        let nodes = read_nodes(&directory)?;
        let observation = observation(&nodes, expected_logic, expected_members);
        if let Some((version, members)) =
            evaluate_convergence(&nodes, expected_logic, expected_members)?
        {
            write_json(
                &artifacts.join("scale-convergence.json"),
                &ConvergenceArtifact {
                    expected_members,
                    membership_coordinator: "domain-membership",
                    logic_nodes: expected_logic,
                    membership_term: version.term,
                    membership_revision: version.revision,
                    convergence_millis: started.elapsed().as_millis(),
                    startup_window_seconds,
                    metrics: scale_metrics(&nodes),
                    members,
                },
            )?;
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{expected_members}-node cluster did not converge within 600s; {observation}"
            ));
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn read_nodes(directory: &Path) -> Result<Vec<ScaleNodeArtifact>, String> {
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.to_string()),
    };
    let mut nodes_by_artifact = BTreeMap::new();
    for entry in entries {
        let entry = entry.map_err(|error| error.to_string())?;
        if entry.path().extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        if let Ok(encoded) = std::fs::read(entry.path())
            && let Ok(node) = serde_json::from_slice::<ScaleNodeArtifact>(&encoded)
        {
            // Windows bind mounts may surface the same directory entry twice while an atomic
            // replacement is in flight. Preserve distinct artifact files so the convergence
            // oracle can still detect duplicate node identities, but collapse duplicate reads
            // of the same path.
            nodes_by_artifact.insert(entry.file_name(), node);
        }
    }
    let mut nodes = nodes_by_artifact.into_values().collect::<Vec<_>>();
    nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(nodes)
}

fn evaluate_convergence(
    nodes: &[ScaleNodeArtifact],
    expected_logic: usize,
    expected_members: usize,
) -> Result<Option<(MembershipVersionArtifact, Vec<MemberArtifact>)>, String> {
    if nodes.len() > expected_logic {
        return Err(format!(
            "scale topology published {} logic nodes, expected {expected_logic}",
            nodes.len()
        ));
    }
    if nodes.len() < expected_logic {
        return Ok(None);
    }
    let identities = nodes
        .iter()
        .map(|node| (node.node_id.clone(), node.incarnation))
        .collect::<BTreeSet<_>>();
    if identities.len() != expected_logic {
        return Err("scale logic artifacts contain duplicate node identities".to_owned());
    }
    if nodes
        .iter()
        .map(|node| node.node_id.as_str())
        .collect::<BTreeSet<_>>()
        .len()
        != expected_logic
    {
        return Err("scale logic artifacts contain duplicate node IDs".to_owned());
    }

    let mut expected_version = None;
    let mut expected_directory = None;
    for (index, node) in nodes.iter().enumerate() {
        if node.lifecycle != "Ready" || !node.domains.is_empty() {
            return Ok(None);
        }
        let Some(version) = node.membership_version else {
            return Ok(None);
        };
        let directory = node.members.iter().cloned().collect::<BTreeSet<_>>();
        if node.members.len() != expected_members
            || directory.len() != expected_members
            || directory.iter().any(|member| member.status != "Up")
            || !directory.iter().any(|member| {
                member.node_id == node.node_id && member.incarnation == node.incarnation
            })
        {
            return Ok(None);
        }
        let Some(ring) = &node.ring else {
            return Ok(None);
        };
        let expected_peer = &nodes[(index + 1) % nodes.len()].node_id;
        if &ring.peer_node_id != expected_peer
            || ring.request != index as u64
            || ring.reply != ring.request + 1
            || !ring.data_lanes_slept
            || node.join_millis.is_none()
            || node
                .resources
                .resident_memory_kib
                .is_none_or(|value| value == 0)
            || node.resources.threads.is_none_or(|value| value == 0)
            || node
                .resources
                .open_file_descriptors
                .is_none_or(|value| value == 0)
            || node.associations == 0
            || node.associations > 3
            || node.attached_lanes != node.associations
        {
            return Ok(None);
        }
        if expected_version.is_some_and(|expected| expected != version)
            || expected_directory
                .as_ref()
                .is_some_and(|expected| expected != &directory)
        {
            return Ok(None);
        }
        expected_version = Some(version);
        expected_directory = Some(directory);
    }
    let version = expected_version.expect("non-empty scale topology has a version");
    let directory = expected_directory.expect("non-empty scale topology has a directory");
    for (node_id, incarnation) in identities {
        if !directory
            .iter()
            .any(|member| member.node_id == node_id && member.incarnation == incarnation)
        {
            return Err(format!(
                "converged membership is missing logic node {node_id}/{incarnation}"
            ));
        }
    }
    Ok(Some((version, directory.into_iter().collect())))
}

fn scale_metrics(nodes: &[ScaleNodeArtifact]) -> ScaleMetricsArtifact {
    ScaleMetricsArtifact {
        maximum_join_millis: nodes
            .iter()
            .filter_map(|node| node.join_millis)
            .max()
            .unwrap_or_default(),
        maximum_ring_millis: nodes
            .iter()
            .filter_map(|node| node.ring.as_ref().map(|ring| ring.elapsed_millis))
            .max()
            .unwrap_or_default(),
        total_resident_memory_kib: nodes
            .iter()
            .filter_map(|node| node.resources.resident_memory_kib)
            .sum(),
        maximum_resident_memory_kib: nodes
            .iter()
            .filter_map(|node| node.resources.resident_memory_kib)
            .max()
            .unwrap_or_default(),
        maximum_threads: nodes
            .iter()
            .filter_map(|node| node.resources.threads)
            .max()
            .unwrap_or_default(),
        maximum_open_file_descriptors: nodes
            .iter()
            .filter_map(|node| node.resources.open_file_descriptors)
            .max()
            .unwrap_or_default(),
        total_associations: nodes.iter().map(|node| node.associations).sum(),
        maximum_associations: nodes
            .iter()
            .map(|node| node.associations)
            .max()
            .unwrap_or_default(),
        total_attached_lanes: nodes.iter().map(|node| node.attached_lanes).sum(),
        maximum_attached_lanes: nodes
            .iter()
            .map(|node| node.attached_lanes)
            .max()
            .unwrap_or_default(),
    }
}

fn observation(
    nodes: &[ScaleNodeArtifact],
    expected_logic: usize,
    expected_members: usize,
) -> String {
    let ready = nodes
        .iter()
        .filter(|node| node.lifecycle == "Ready" && node.domains.is_empty())
        .count();
    let complete_membership = nodes
        .iter()
        .filter(|node| node.members.len() == expected_members)
        .count();
    let ring_complete = nodes.iter().filter(|node| node.ring.is_some()).count();
    format!(
        "logic artifacts={}/{expected_logic}, ready={ready}/{expected_logic}, full membership={complete_membership}/{expected_logic}, ring={ring_complete}/{expected_logic}",
        nodes.len()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn converged_nodes() -> Vec<ScaleNodeArtifact> {
        let version = MembershipVersionArtifact {
            term: 3,
            revision: 64,
        };
        let mut members = (0..64)
            .map(|index| MemberArtifact {
                node_id: format!("logic-{index:02}"),
                incarnation: index as u128 + 100,
                status: "Up".to_owned(),
            })
            .collect::<Vec<_>>();
        members.sort();
        (0..64)
            .map(|index| ScaleNodeArtifact {
                node_id: format!("logic-{index:02}"),
                incarnation: index as u128 + 100,
                lifecycle: "Ready".to_owned(),
                domains: Default::default(),
                membership_version: Some(version),
                members: members.clone(),
                join_millis: Some(100),
                ring: Some(RingArtifact {
                    peer_node_id: format!("logic-{:02}", (index + 1) % 64),
                    request: index as u64,
                    reply: index as u64 + 1,
                    elapsed_millis: 10,
                    data_lanes_slept: true,
                }),
                resources: ProcessResourceArtifact {
                    resident_memory_kib: Some(1024),
                    threads: Some(2),
                    open_file_descriptors: Some(8),
                },
                associations: 3,
                attached_lanes: 3,
            })
            .collect()
    }

    #[test]
    fn sixty_four_node_oracle_requires_identical_membership() {
        let nodes = converged_nodes();
        let converged = evaluate_convergence(&nodes, 64, 64)
            .expect("valid scale evidence")
            .expect("all nodes should converge");
        assert_eq!(converged.0.revision, 64);
        assert_eq!(converged.1.len(), 64);
    }

    #[test]
    fn sixty_four_node_oracle_waits_for_revision_convergence() {
        let mut nodes = converged_nodes();
        nodes[0].membership_version = Some(MembershipVersionArtifact {
            term: 3,
            revision: 63,
        });
        assert!(
            evaluate_convergence(&nodes, 64, 64)
                .expect("revision skew is transient")
                .is_none()
        );
    }

    #[test]
    fn sixty_four_node_oracle_rejects_duplicate_logic_identity() {
        let mut nodes = converged_nodes();
        nodes[1].node_id = nodes[0].node_id.clone();
        nodes[1].incarnation = nodes[0].incarnation;
        let error = evaluate_convergence(&nodes, 64, 64)
            .expect_err("duplicate logic identity must be rejected");
        assert!(error.contains("duplicate"));
    }

    #[test]
    fn convergence_artifact_streams_full_u128_incarnations() {
        let artifact = ConvergenceArtifact {
            expected_members: 1,
            membership_coordinator: "domain-membership",
            logic_nodes: 1,
            membership_term: 1,
            membership_revision: 2,
            convergence_millis: 3,
            startup_window_seconds: 0,
            metrics: scale_metrics(&converged_nodes()),
            members: vec![MemberArtifact {
                node_id: "logic".to_owned(),
                incarnation: u128::MAX,
                status: "Up".to_owned(),
            }],
        };
        let value = serde_json::json!(artifact);
        let encoded = serde_json::to_vec(&value).expect("string incarnation must fit JSON Value");
        assert!(
            encoded
                .windows(39)
                .any(|window| window == b"340282366920938463463374607431768211455")
        );
    }
}
