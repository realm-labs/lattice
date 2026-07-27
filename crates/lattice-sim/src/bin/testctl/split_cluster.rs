use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::{labeled_containers, require_label};

/// A fenced owner is only believed to have stopped once nothing has been served for long enough to
/// rule out a probe that was already in flight.
pub(super) const QUIET_PERIOD: Duration = Duration::from_millis(1_500);

/// The hosts that keep an independent, timestamped record of which activation served the shared
/// entity. A spare host exists in the same profile but stays out of the cluster until a scenario
/// gives it a release, so it is deliberately not one of these observers.
pub(super) const NODES: [&str; 2] = ["split-node-a", "split-node-b"];

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub(super) struct ActivationIdentity {
    pub node_id: String,
    pub incarnation: String,
    pub activation: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct ProbeRecord {
    pub sequence: u64,
    pub requested_unix_millis: u128,
    pub unix_millis: u128,
    pub outcome: String,
    pub served_by: Option<ActivationIdentity>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ServingWindow {
    pub activation: ActivationIdentity,
    pub first_served_millis: u128,
    pub last_served_millis: u128,
    pub observers: Vec<String>,
    pub samples: usize,
}

/// The release composition a host observes across the live, lease-backed members, exactly as the
/// admission guard would derive it. `invalid` can only be published by a host whose cluster has a
/// combination of releases no guard should ever have admitted.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(super) enum ClusterReleaseArtifact {
    Absent,
    Empty,
    Stable { release_id: u64 },
    Rolling { from: u64, to: u64 },
    Invalid { error: String },
}

impl ClusterReleaseArtifact {
    /// Every release the host can see, which is what a scenario compares against the releases it
    /// asked for.
    pub fn releases(&self) -> Vec<u64> {
        match self {
            Self::Absent | Self::Empty | Self::Invalid { .. } => Vec::new(),
            Self::Stable { release_id } => vec![*release_id],
            Self::Rolling { from, to } => vec![*from, *to],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
pub(super) struct RolloutMemberArtifact {
    pub node_id: String,
    pub status: String,
    pub release_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(super) struct StartupErrorArtifact {
    pub kind: String,
    pub detail: String,
}

/// The release a host was told to run. A code-only upgrade changes nothing here but `release_id`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
pub(super) struct ReleaseIdentity {
    pub release_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_fingerprint: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_generation: Option<u64>,
}

impl ReleaseIdentity {
    pub fn code_only(release_id: u64) -> Self {
        Self {
            release_id,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct SplitHostArtifact {
    pub node_id: String,
    pub unix_millis: u128,
    pub lifecycle: String,
    #[serde(default)]
    pub release: Option<ReleaseIdentity>,
    #[serde(default)]
    pub cluster_release: Option<ClusterReleaseArtifact>,
    #[serde(default)]
    pub rollout_members: Vec<RolloutMemberArtifact>,
    #[serde(default)]
    pub startup_error: Option<StartupErrorArtifact>,
}

impl SplitHostArtifact {
    pub fn release_of(&self, node_id: &str) -> Option<u64> {
        self.rollout_members
            .iter()
            .find(|member| member.node_id == node_id)
            .map(|member| member.release_id)
    }

    pub fn cluster_release(&self) -> ClusterReleaseArtifact {
        self.cluster_release
            .clone()
            .unwrap_or(ClusterReleaseArtifact::Absent)
    }
}

pub(super) struct SplitCluster {
    pub run_id: String,
    pub directory: PathBuf,
    containers: BTreeMap<String, String>,
}

impl SplitCluster {
    pub fn resolve(artifacts: &Path) -> Result<Self, String> {
        let run_id = std::env::var("LATTICE_RUN_ID")
            .map_err(|_| "split-brain scenarios require LATTICE_RUN_ID".to_owned())?;
        let labeled = labeled_containers(&run_id)?;
        let mut containers = BTreeMap::new();
        for node in NODES {
            let container = labeled
                .lines()
                .find(|name| name.contains(node) && !name.contains("runner"))
                .ok_or_else(|| format!("missing labeled {node} container"))?;
            require_label("container", container, &run_id)?;
            containers.insert(node.to_owned(), container.to_owned());
        }
        Ok(Self {
            run_id,
            directory: artifacts.join("partition"),
            containers,
        })
    }

    pub fn container(&self, node: &str) -> Result<&str, String> {
        self.containers
            .get(node)
            .map(String::as_str)
            .ok_or_else(|| format!("{node} has no labeled container"))
    }

    pub fn peer(node: &str) -> &'static str {
        if node == NODES[0] { NODES[1] } else { NODES[0] }
    }

    /// Where a host publishes what it is doing, including the releases it can see.
    pub fn host_artifact(&self, node: &str) -> PathBuf {
        self.directory.join(format!("{node}.json"))
    }

    /// Where a scenario tells a host which release to run. Rewriting it is the upgrade.
    pub fn release_file(&self, node: &str) -> PathBuf {
        self.directory.join(format!("{node}-release.json"))
    }

    pub fn probes(&self, node: &str, since: u128) -> Result<Vec<ProbeRecord>, String> {
        read_probe_journal(&self.directory.join(format!("{node}-probes.jsonl")), since)
    }

    pub fn latest(&self, node: &str, since: u128) -> Result<Option<ProbeRecord>, String> {
        let mut records = self.probes(node, since)?;
        Ok(records.pop())
    }

    /// Waits until every node's most recent probe was answered by the same activation, which is the
    /// only steady state a single-shard entity type may reach: one host activates the entity and
    /// the other proxies to it.
    pub fn wait_for_agreed_activation(
        &self,
        since: u128,
        timeout: Duration,
    ) -> Result<ActivationIdentity, String> {
        let deadline = Instant::now() + timeout;
        loop {
            let observed = NODES
                .iter()
                .map(|node| self.latest(node, since))
                .collect::<Result<Vec<_>, _>>()?;
            if let [Some(first), Some(second)] = observed.as_slice()
                && let (Some(left), Some(right)) = (&first.served_by, &second.served_by)
                && left == right
            {
                return Ok(left.clone());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "the entity did not converge on one activation within {timeout:?}: {observed:?}"
                ));
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    /// Waits for the fenced activation to stop answering anywhere in the cluster. A paused owner
    /// records nothing while it is frozen, so quiescence is measured from the last observation
    /// rather than from the presence of a refusal.
    pub fn wait_until_fenced(
        &self,
        activation: &ActivationIdentity,
        since: u128,
        budget: Duration,
    ) -> Result<u128, String> {
        let deadline = Instant::now() + budget + QUIET_PERIOD;
        loop {
            let last = NODES
                .iter()
                .map(|node| {
                    Ok(self
                        .probes(node, since)?
                        .into_iter()
                        .filter(|record| record.served_by.as_ref() == Some(activation))
                        .map(|record| record.unix_millis)
                        .max())
                })
                .collect::<Result<Vec<_>, String>>()?
                .into_iter()
                .flatten()
                .max();
            let now = now_millis()?;
            match last {
                Some(last) if now.saturating_sub(last) >= QUIET_PERIOD.as_millis() => {
                    return Ok(last);
                }
                None if Instant::now() >= deadline => return Ok(since),
                _ => {}
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "{:?} was still serving {budget:?} after the fault began",
                    activation
                ));
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    /// Waits for a surviving node to be served by an activation other than `fenced`, which is the
    /// observable proof that the Coordinator reassigned the slot after the claim expired.
    pub fn wait_for_replacement_activation(
        &self,
        node: &str,
        fenced: &ActivationIdentity,
        since: u128,
        timeout: Duration,
    ) -> Result<ActivationIdentity, String> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(record) = self.latest(node, since)?
                && let Some(activation) = record.served_by
                && &activation != fenced
            {
                return Ok(activation);
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "{node} was not served by a replacement activation within {timeout:?}"
                ));
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }
}

pub(super) fn now_millis() -> Result<u128, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .map_err(|error| error.to_string())
}

fn read_probe_journal(path: &Path, since: u128) -> Result<Vec<ProbeRecord>, String> {
    let encoded = match std::fs::read_to_string(path) {
        Ok(encoded) => encoded,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(format!("{}: {error}", path.display())),
    };
    let mut records = Vec::new();
    let mut lines = encoded.lines().filter(|line| !line.is_empty()).peekable();
    while let Some(line) = lines.next() {
        match serde_json::from_str::<ProbeRecord>(line) {
            Ok(record) if record.unix_millis >= since => records.push(record),
            Ok(_) => {}
            // A concurrent append can be observed mid-write, but only ever as the final line.
            Err(error) if lines.peek().is_some() => {
                return Err(format!("{}: corrupt probe record: {error}", path.display()));
            }
            Err(_) => {}
        }
    }
    Ok(records)
}

/// Collapses every observation of a served probe into one interval per activation. Two intervals
/// that overlap mean two activations answered for the same entity at the same time.
pub(super) fn serving_windows(
    cluster: &SplitCluster,
    since: u128,
) -> Result<Vec<ServingWindow>, String> {
    let mut windows: BTreeMap<ActivationIdentity, ServingWindow> = BTreeMap::new();
    for node in NODES {
        for record in cluster.probes(node, since)? {
            match (record.outcome.as_str(), record.served_by) {
                ("served", Some(activation)) => {
                    let window =
                        windows
                            .entry(activation.clone())
                            .or_insert_with(|| ServingWindow {
                                activation,
                                first_served_millis: record.unix_millis,
                                last_served_millis: record.unix_millis,
                                observers: Vec::new(),
                                samples: 0,
                            });
                    window.first_served_millis = window.first_served_millis.min(record.unix_millis);
                    window.last_served_millis = window.last_served_millis.max(record.unix_millis);
                    window.samples += 1;
                    if !window.observers.iter().any(|name| name == node) {
                        window.observers.push(node.to_owned());
                    }
                }
                ("served", None) => {
                    return Err(format!(
                        "{node} recorded a served probe without an activation"
                    ));
                }
                ("rejected", _) if record.error.as_deref().is_none_or(str::is_empty) => {
                    return Err(format!(
                        "{node} rejected probe {} without an explicit failure",
                        record.sequence
                    ));
                }
                ("rejected", _) => {}
                (outcome, _) => {
                    return Err(format!(
                        "{node} recorded an unknown probe outcome {outcome}"
                    ));
                }
            }
        }
    }
    let mut windows = windows.into_values().collect::<Vec<_>>();
    windows.sort_by_key(|window| (window.first_served_millis, window.last_served_millis));
    Ok(windows)
}

pub(super) fn require_disjoint_activations(windows: &[ServingWindow]) -> Result<(), String> {
    for (index, earlier) in windows.iter().enumerate() {
        for later in &windows[index + 1..] {
            if later.first_served_millis <= earlier.last_served_millis
                && earlier.first_served_millis <= later.last_served_millis
            {
                return Err(format!(
                    "two activations served the same entity concurrently: {:?} served [{}, {}] and {:?} served [{}, {}]",
                    earlier.activation,
                    earlier.first_served_millis,
                    earlier.last_served_millis,
                    later.activation,
                    later.first_served_millis,
                    later.last_served_millis
                ));
            }
        }
    }
    Ok(())
}

/// Fails as soon as any node reports that a request it issued after `since` was answered by an
/// activation that is supposed to be finished. A retired activation may still complete work it
/// accepted while it was the owner, so the request rather than the reply decides the violation.
pub(super) fn require_retired(
    cluster: &SplitCluster,
    activation: &ActivationIdentity,
    since: u128,
    settle: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + settle;
    loop {
        for node in NODES {
            if let Some(record) = cluster.probes(node, since)?.into_iter().find(|record| {
                record.served_by.as_ref() == Some(activation)
                    && record.requested_unix_millis >= since
            }) {
                return Err(format!(
                    "{node} asked at {} and was answered by the fenced activation {activation:?}",
                    record.requested_unix_millis
                ));
            }
        }
        if Instant::now() >= deadline {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

pub(super) fn refusals_since(
    cluster: &SplitCluster,
    node: &str,
    since: u128,
) -> Result<usize, String> {
    Ok(cluster
        .probes(node, since)?
        .into_iter()
        .filter(|record| record.outcome == "rejected")
        .count())
}

/// The stretch an observer spent with nothing to talk to, from the last probe the outgoing
/// activation answered to the first probe its replacement answered.
pub(super) fn gap_without_an_owner(
    cluster: &SplitCluster,
    node: &str,
    outgoing: &ActivationIdentity,
    replacement: &ActivationIdentity,
    since: u128,
) -> Result<u128, String> {
    let records = cluster.probes(node, since)?;
    let served_by = |wanted: &ActivationIdentity| {
        records
            .iter()
            .filter(|record| record.served_by.as_ref() == Some(wanted))
            .map(|record| record.unix_millis)
            .collect::<Vec<_>>()
    };
    let last = served_by(outgoing)
        .into_iter()
        .max()
        .ok_or_else(|| format!("{node} never observed {outgoing:?}"))?;
    let first = served_by(replacement)
        .into_iter()
        .min()
        .ok_or_else(|| format!("{node} never observed {replacement:?}"))?;
    first
        .checked_sub(last)
        .ok_or_else(|| format!("{node} was served by {replacement:?} before {outgoing:?} stopped"))
}

/// Every probe a node sent must have been answered or explicitly refused, and the recorded
/// sequence numbers must be contiguous: a gap would be a request that vanished without an outcome.
pub(super) fn require_conserved_probes(
    cluster: &SplitCluster,
    node: &str,
    since: u128,
) -> Result<usize, String> {
    let records = cluster.probes(node, since)?;
    let mut expected = None;
    for record in &records {
        if !matches!(record.outcome.as_str(), "served" | "rejected") {
            return Err(format!("{node} probe {} has no outcome", record.sequence));
        }
        if let Some(expected) = expected
            && record.sequence != expected
        {
            return Err(format!(
                "{node} lost probe outcomes between {} and {}",
                expected - 1,
                record.sequence
            ));
        }
        expected = Some(record.sequence + 1);
    }
    Ok(records.len())
}
