use std::collections::BTreeMap;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::{command, labeled_containers, output, require_label, write_json};

/// Placement grants a claim for `claim_ttl` and the logic side fences itself `claim_safety_margin`
/// early, so an owner that stops hearing from its Coordinator must refuse traffic this long after
/// the fault at the latest. The tolerance covers the grant that was already in flight, the
/// hundred-millisecond authority tick, and one probe round trip.
const CLAIM_TTL: Duration = Duration::from_secs(15);
const CLAIM_SAFETY_MARGIN: Duration = Duration::from_secs(2);
const LOCAL_FENCE_TOLERANCE: Duration = Duration::from_secs(4);
/// A member that stays silent this long is declared dead and its claim is left to expire, which is
/// what moves a slot to another host. Faults that only prove local fencing must heal before it.
const MEMBER_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(25);
/// A leaving owner keeps renewing a valid claim while it hands the slot back, so the claim deadline
/// bounds nothing about it and the whole handoff has to be bounded by something else. The failure
/// detector is that bound: a graceful leave exists to move the slot sooner than a crash would, so a
/// drain that has not finished by the time a silent member would have been declared dead has bought
/// nothing over the path it replaces.
const DRAIN_HANDOFF_BUDGET: Duration = MEMBER_HEARTBEAT_TIMEOUT;
/// A fenced owner is only believed to have stopped once nothing has been served for long enough to
/// rule out a probe that was already in flight.
const QUIET_PERIOD: Duration = Duration::from_millis(1_500);

const NODES: [&str; 2] = ["split-node-a", "split-node-b"];

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
struct ActivationIdentity {
    node_id: String,
    incarnation: String,
    activation: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct ProbeRecord {
    sequence: u64,
    requested_unix_millis: u128,
    unix_millis: u128,
    outcome: String,
    served_by: Option<ActivationIdentity>,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ServingWindow {
    activation: ActivationIdentity,
    first_served_millis: u128,
    last_served_millis: u128,
    observers: Vec<String>,
    samples: usize,
}

struct SplitCluster {
    run_id: String,
    directory: PathBuf,
    containers: BTreeMap<String, String>,
}

impl SplitCluster {
    fn resolve(artifacts: &Path) -> Result<Self, String> {
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

    fn container(&self, node: &str) -> Result<&str, String> {
        self.containers
            .get(node)
            .map(String::as_str)
            .ok_or_else(|| format!("{node} has no labeled container"))
    }

    fn peer(node: &str) -> &'static str {
        if node == NODES[0] { NODES[1] } else { NODES[0] }
    }

    fn probes(&self, node: &str, since: u128) -> Result<Vec<ProbeRecord>, String> {
        read_probe_journal(&self.directory.join(format!("{node}-probes.jsonl")), since)
    }

    fn latest(&self, node: &str, since: u128) -> Result<Option<ProbeRecord>, String> {
        let mut records = self.probes(node, since)?;
        Ok(records.pop())
    }

    /// Waits until every node's most recent probe was answered by the same activation, which is the
    /// only steady state a single-shard entity type may reach: one host activates the entity and
    /// the other proxies to it.
    fn wait_for_agreed_activation(
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
    fn wait_until_fenced(
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
    fn wait_for_replacement_activation(
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

fn now_millis() -> Result<u128, String> {
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
fn serving_windows(cluster: &SplitCluster, since: u128) -> Result<Vec<ServingWindow>, String> {
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

fn require_disjoint_activations(windows: &[ServingWindow]) -> Result<(), String> {
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
fn require_retired(
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

fn refusals_since(cluster: &SplitCluster, node: &str, since: u128) -> Result<usize, String> {
    Ok(cluster
        .probes(node, since)?
        .into_iter()
        .filter(|record| record.outcome == "rejected")
        .count())
}

/// The stretch an observer spent with nothing to talk to, from the last probe the outgoing
/// activation answered to the first probe its replacement answered.
fn gap_without_an_owner(
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
fn require_conserved_probes(
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

/// What the fault did to the owner's authority, which is what decides how long it may still answer
/// and whether the entity is expected to go unreachable at all.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Authority {
    /// The owner can no longer renew its claim and nothing outside it can tell it to stop, so the
    /// deadline it computed when the grant was installed is the only bound on its service window,
    /// and the entity has to refuse traffic until the slot is reinstalled elsewhere.
    Fenced,
    /// The owner still holds a valid claim and is handing it back, so it is entitled to answer until
    /// the Coordinator takes the slot off it. A drain that conserves every message need never refuse
    /// one, so demanding a refusal here would fail exactly the handoffs that went perfectly.
    HandedOver,
}

impl Authority {
    fn serving_budget(self) -> Duration {
        match self {
            Self::Fenced => CLAIM_TTL - CLAIM_SAFETY_MARGIN + LOCAL_FENCE_TOLERANCE,
            Self::HandedOver => DRAIN_HANDOFF_BUDGET,
        }
    }

    fn deadline_name(self) -> &'static str {
        match self {
            Self::Fenced => "claim deadline",
            Self::HandedOver => "handoff deadline",
        }
    }
}

struct FaultEvidence {
    scenario: &'static str,
    fault: &'static str,
    authority: Authority,
    window_start: u128,
    fault_start: u128,
    fenced: ActivationIdentity,
    fenced_node: String,
    stopped_serving_millis: u128,
    recovered: Option<ActivationIdentity>,
}

fn record_evidence(
    cluster: &SplitCluster,
    artifacts: &Path,
    evidence: FaultEvidence,
) -> Result<(), String> {
    let windows = serving_windows(cluster, evidence.window_start)?;
    require_disjoint_activations(&windows)?;
    let fenced_window = windows
        .iter()
        .find(|window| window.activation == evidence.fenced)
        .ok_or_else(|| format!("{} lost the pre-fault activation window", evidence.scenario))?;
    let budget = evidence.fault_start + evidence.authority.serving_budget().as_millis();
    if fenced_window.last_served_millis > budget {
        return Err(format!(
            "{} kept serving from {:?} until {}, {}ms past its {}",
            evidence.fenced_node,
            evidence.fenced,
            fenced_window.last_served_millis,
            fenced_window.last_served_millis - budget,
            evidence.authority.deadline_name()
        ));
    }
    let mut recovered_after_millis = None;
    if let Some(recovered) = &evidence.recovered {
        if recovered == &evidence.fenced {
            return Err(format!(
                "{} resumed the fenced activation {:?} instead of installing a new one",
                evidence.scenario, evidence.fenced
            ));
        }
        let window = windows
            .iter()
            .find(|window| &window.activation == recovered)
            .ok_or_else(|| format!("{} lost the recovered activation window", evidence.scenario))?;
        if window.first_served_millis <= fenced_window.last_served_millis {
            return Err(format!(
                "{} served from a second activation before the fenced one stopped",
                evidence.scenario
            ));
        }
        recovered_after_millis = Some(
            window
                .first_served_millis
                .saturating_sub(evidence.fault_start),
        );
    }
    let refusals = NODES
        .iter()
        .map(|node| {
            Ok((
                (*node).to_owned(),
                refusals_since(cluster, node, evidence.fault_start)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, String>>()?;
    if evidence.authority == Authority::Fenced && refusals.values().all(|count| *count == 0) {
        return Err(format!(
            "{} never refused a request while the entity had no live authority",
            evidence.scenario
        ));
    }
    let mut conserved = BTreeMap::new();
    for node in NODES {
        conserved.insert(
            node.to_owned(),
            require_conserved_probes(cluster, node, evidence.window_start)?,
        );
    }
    write_json(
        &artifacts.join(format!("{}.json", evidence.scenario)),
        &serde_json::json!({
            "run_id": cluster.run_id,
            "fault": evidence.fault,
            "window_start_unix_millis": evidence.window_start,
            "fault_start_unix_millis": evidence.fault_start,
            "fenced_node": evidence.fenced_node,
            "fenced_activation": evidence.fenced,
            "recovered_activation": evidence.recovered,
            "local_fence_deadline_millis": (CLAIM_TTL - CLAIM_SAFETY_MARGIN).as_millis(),
            "member_heartbeat_timeout_millis": MEMBER_HEARTBEAT_TIMEOUT.as_millis(),
            "serving_budget_millis": evidence.authority.serving_budget().as_millis(),
            "fenced_owner_stopped_after_millis": evidence
                .stopped_serving_millis
                .saturating_sub(evidence.fault_start),
            "recovered_after_millis": recovered_after_millis,
            "refusals_during_the_fault": refusals,
            "probes_with_an_outcome": conserved,
            "serving_windows": windows,
            "overlapping_activation_pairs": 0,
        }),
    )
}

/// Keeps the fault in place until the claim the owner held would have expired on its own, so the
/// scenario really covers the window the safety argument is about, and re-checks throughout that
/// nothing started answering again behind the partition. Healing has to happen before the member
/// heartbeat timeout, otherwise the Coordinator starts a recovery handoff instead.
fn hold_partition(
    cluster: &SplitCluster,
    fenced: &ActivationIdentity,
    fault_start: u128,
    stopped_serving_millis: u128,
) -> Result<(), String> {
    let hold_until = fault_start + (CLAIM_TTL - CLAIM_SAFETY_MARGIN).as_millis();
    let heal_by = fault_start + MEMBER_HEARTBEAT_TIMEOUT.as_millis();
    loop {
        let now = now_millis()?;
        if now >= hold_until {
            return Ok(());
        }
        if now >= heal_by {
            return Err(
                "the partition could not be held inside the member heartbeat timeout".to_owned(),
            );
        }
        require_retired(cluster, fenced, stopped_serving_millis + 1, Duration::ZERO)?;
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn interface_address(container: &str) -> Result<IpAddr, String> {
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
    address
        .parse()
        .map_err(|error| format!("invalid container address {address}: {error}"))
}

/// Cuts the entity owner off from its Coordinator and from the node that proxies to it while both
/// sides keep asking for the same entity id. Nothing outside the isolated process can stop it
/// answering, so its service window has to close on its own and no later than the claim deadline
/// it computed when the grant was installed. The partition is then held past that deadline, and
/// healed before the member heartbeat timeout turns the outage into a reassignment.
pub(super) fn symmetric(artifacts: &Path) -> Result<(), String> {
    let network = std::env::var("LATTICE_DOCKER_NETWORK")
        .map_err(|_| "split-brain scenarios require LATTICE_DOCKER_NETWORK".to_owned())?;
    let cluster = SplitCluster::resolve(artifacts)?;
    require_label("network", &network, &cluster.run_id)?;
    let window_start = now_millis()?;
    let fenced = cluster.wait_for_agreed_activation(window_start, Duration::from_secs(120))?;
    let owner = fenced.node_id.clone();
    let container = cluster.container(&owner)?.to_owned();
    let address = interface_address(&container)?.to_string();

    let fault_start = now_millis()?;
    command("docker", &["network", "disconnect", &network, &container])?;
    let quiesced = cluster.wait_until_fenced(
        &fenced,
        fault_start,
        CLAIM_TTL - CLAIM_SAFETY_MARGIN + LOCAL_FENCE_TOLERANCE,
    );
    let held = quiesced
        .as_ref()
        .map_err(Clone::clone)
        .and_then(|stopped| hold_partition(&cluster, &fenced, fault_start, *stopped));
    command(
        "docker",
        &[
            "network", "connect", "--ip", &address, "--alias", &owner, &network, &container,
        ],
    )?;
    let stopped_serving_millis = quiesced?;
    held?;
    record_evidence(
        &cluster,
        artifacts,
        FaultEvidence {
            scenario: "entity-partition-single-activation",
            authority: Authority::Fenced,
            fault: "docker-network-disconnect-owner",
            window_start,
            fault_start,
            fenced,
            fenced_node: owner,
            stopped_serving_millis,
            recovered: None,
        },
    )
}

/// Holds the partition past the member heartbeat timeout and the natural claim lease expiry, which
/// is the only path that reinstalls the slot on another host. The fenced owner must stop serving
/// before the replacement ever answers.
pub(super) fn owner_loss_reassignment(artifacts: &Path) -> Result<(), String> {
    let network = std::env::var("LATTICE_DOCKER_NETWORK")
        .map_err(|_| "split-brain scenarios require LATTICE_DOCKER_NETWORK".to_owned())?;
    let cluster = SplitCluster::resolve(artifacts)?;
    require_label("network", &network, &cluster.run_id)?;
    let window_start = now_millis()?;
    let fenced = cluster.wait_for_agreed_activation(window_start, Duration::from_secs(120))?;
    let owner = fenced.node_id.clone();
    let container = cluster.container(&owner)?.to_owned();
    let address = interface_address(&container)?.to_string();

    let fault_start = now_millis()?;
    command("docker", &["network", "disconnect", &network, &container])?;
    let partitioned = cluster.wait_for_replacement_activation(
        SplitCluster::peer(&owner),
        &fenced,
        fault_start,
        Duration::from_secs(180),
    );
    command(
        "docker",
        &[
            "network", "connect", "--ip", &address, "--alias", &owner, &network, &container,
        ],
    )?;
    let replacement = partitioned?;
    let converged = cluster.wait_for_agreed_activation(now_millis()?, Duration::from_secs(150))?;
    if converged != replacement {
        return Err(format!(
            "the healed cluster settled on {converged:?} rather than the replacement {replacement:?}"
        ));
    }
    record_evidence(
        &cluster,
        artifacts,
        FaultEvidence {
            scenario: "entity-owner-loss-reassignment",
            authority: Authority::Fenced,
            fault: "partition-owner-past-member-heartbeat-timeout",
            window_start,
            fault_start,
            fenced: fenced.clone(),
            fenced_node: owner,
            stopped_serving_millis: cluster.wait_until_fenced(
                &fenced,
                window_start,
                QUIET_PERIOD,
            )?,
            recovered: Some(replacement),
        },
    )
}

/// Freezes the owner's whole process instead of cutting its network, so the claim it holds runs out
/// on a clock that never stopped while nothing could renew it. Holding the freeze past the member
/// heartbeat timeout also carries it past the shorter claim lease, so the Coordinator both declares
/// the member dead and lets the lease lapse before anything is thawed. The thawed process still has
/// the activation in memory and nobody tells it to stop, so the only thing that can keep it quiet is
/// the deadline it computed for itself when the grant was installed.
pub(super) fn paused_owner_lease_expiry(artifacts: &Path) -> Result<(), String> {
    let cluster = SplitCluster::resolve(artifacts)?;
    let window_start = now_millis()?;
    let fenced = cluster.wait_for_agreed_activation(window_start, Duration::from_secs(120))?;
    let owner = fenced.node_id.clone();
    let container = cluster.container(&owner)?.to_owned();

    let fault_start = now_millis()?;
    command("docker", &["pause", &container])?;
    let frozen = cluster
        .wait_until_fenced(&fenced, fault_start, LOCAL_FENCE_TOLERANCE)
        .and_then(|stopped| {
            hold_frozen(&cluster, &fenced, fault_start, stopped)?;
            let replacement = cluster.wait_for_replacement_activation(
                SplitCluster::peer(&owner),
                &fenced,
                fault_start,
                Duration::from_secs(180),
            )?;
            Ok((stopped, replacement))
        });
    command("docker", &["unpause", &container])?;
    let thawed = now_millis()?;
    let (stopped_serving_millis, replacement) = frozen?;
    require_retired(&cluster, &fenced, thawed, QUIET_PERIOD)?;
    let converged = cluster.wait_for_agreed_activation(thawed, Duration::from_secs(150))?;
    if converged != replacement {
        return Err(format!(
            "the thawed cluster settled on {converged:?} rather than the replacement {replacement:?}"
        ));
    }
    record_evidence(
        &cluster,
        artifacts,
        FaultEvidence {
            scenario: "entity-paused-owner-lease-expiry",
            authority: Authority::Fenced,
            fault: "docker-pause-owner-past-claim-lease-and-member-heartbeat-timeout",
            window_start,
            fault_start,
            fenced,
            fenced_node: owner,
            stopped_serving_millis,
            recovered: Some(replacement),
        },
    )
}

/// A frozen process cannot renew anything, so waiting out the member heartbeat timeout is what makes
/// the reassignment that follows a Coordinator giving up on a dead member rather than a handoff. The
/// timeout is longer than the claim lease, so the lease has necessarily lapsed by the time it fires.
fn hold_frozen(
    cluster: &SplitCluster,
    fenced: &ActivationIdentity,
    fault_start: u128,
    stopped_serving_millis: u128,
) -> Result<(), String> {
    let thaw_after = fault_start + MEMBER_HEARTBEAT_TIMEOUT.as_millis();
    while now_millis()? < thaw_after {
        require_retired(cluster, fenced, stopped_serving_millis + 1, Duration::ZERO)?;
        std::thread::sleep(Duration::from_millis(200));
    }
    Ok(())
}

/// Drops everything the owner sends while leaving everything it receives alone. This is the failure
/// the Coordinator cannot see coming: it keeps handing grants to a member whose heartbeats never
/// arrive, so nothing outside the owner can tell it to stop and it has to work out on its own that
/// none of its replies are being heard. Both sides have to refuse traffic while that is true.
pub(super) fn egress_blackhole_reassignment(artifacts: &Path) -> Result<(), String> {
    let cluster = SplitCluster::resolve(artifacts)?;
    let window_start = now_millis()?;
    let fenced = cluster.wait_for_agreed_activation(window_start, Duration::from_secs(120))?;
    let owner = fenced.node_id.clone();
    let container = cluster.container(&owner)?.to_owned();

    let fault_start = now_millis()?;
    command(
        "docker",
        &[
            "exec", &container, "tc", "qdisc", "add", "dev", "eth0", "root", "netem", "loss",
            "100%",
        ],
    )?;
    let blackholed = cluster
        .wait_until_fenced(
            &fenced,
            fault_start,
            CLAIM_TTL - CLAIM_SAFETY_MARGIN + LOCAL_FENCE_TOLERANCE,
        )
        .and_then(|stopped| {
            let replacement = cluster.wait_for_replacement_activation(
                SplitCluster::peer(&owner),
                &fenced,
                fault_start,
                Duration::from_secs(180),
            )?;
            Ok((stopped, replacement))
        });
    command(
        "docker",
        &[
            "exec", &container, "tc", "qdisc", "del", "dev", "eth0", "root",
        ],
    )?;
    let healed = now_millis()?;
    let (stopped_serving_millis, replacement) = blackholed?;
    require_retired(&cluster, &fenced, healed, QUIET_PERIOD)?;
    for node in NODES {
        if refusals_since(&cluster, node, fault_start)? == 0 {
            return Err(format!(
                "{node} never refused a request while the owner's replies were being dropped"
            ));
        }
    }
    let converged = cluster.wait_for_agreed_activation(healed, Duration::from_secs(150))?;
    if converged != replacement {
        return Err(format!(
            "the healed cluster settled on {converged:?} rather than the replacement {replacement:?}"
        ));
    }
    record_evidence(
        &cluster,
        artifacts,
        FaultEvidence {
            scenario: "entity-egress-blackhole-reassignment",
            authority: Authority::Fenced,
            fault: "netem-drop-all-owner-egress-past-member-heartbeat-timeout",
            window_start,
            fault_start,
            fenced,
            fenced_node: owner,
            stopped_serving_millis,
            recovered: Some(replacement),
        },
    )
}

/// Asks the owner to leave while both sides keep probing at full rate. The fixture drains on
/// `SIGINT`, so the slot is handed over rather than abandoned: the owner never loses its authority,
/// it gives it back, and it is entitled to keep answering until the Coordinator has installed the
/// slot on the other host. Nothing here may be measured against the claim deadline the fenced
/// scenarios use, because that deadline exists for an owner nobody can reach. What is measured
/// instead is that the replacement is serving inside the drain budget, that the outgoing activation
/// answers nothing asked after it, that the window with no owner at all is far shorter than the
/// failure detector would have taken, and that not one probe sent across the move lost its outcome.
pub(super) fn drain_message_conservation(artifacts: &Path) -> Result<(), String> {
    let cluster = SplitCluster::resolve(artifacts)?;
    let window_start = now_millis()?;
    let drained = cluster.wait_for_agreed_activation(window_start, Duration::from_secs(120))?;
    let owner = drained.node_id.clone();
    let observer = SplitCluster::peer(&owner).to_owned();
    let container = cluster.container(&owner)?.to_owned();

    let fault_start = now_millis()?;
    command("docker", &["kill", "--signal=INT", &container])?;
    let replacement = cluster.wait_for_replacement_activation(
        &observer,
        &drained,
        fault_start,
        DRAIN_HANDOFF_BUDGET,
    )?;
    let handed_over = now_millis()?;
    let gap = gap_without_an_owner(&cluster, &observer, &drained, &replacement, window_start)?;
    if gap >= MEMBER_HEARTBEAT_TIMEOUT.as_millis() {
        return Err(format!(
            "{observer} had nothing to talk to for {gap}ms after the owner was asked to leave, so \
             the entity moved because the member was declared dead rather than handed over"
        ));
    }
    require_retired(&cluster, &drained, handed_over, QUIET_PERIOD)?;
    let stopped_serving_millis = cluster.wait_until_fenced(&drained, window_start, QUIET_PERIOD)?;
    record_evidence(
        &cluster,
        artifacts,
        FaultEvidence {
            scenario: "entity-drain-message-conservation",
            authority: Authority::HandedOver,
            fault: "sigint-graceful-drain-owner",
            window_start,
            fault_start,
            fenced: drained,
            fenced_node: owner,
            stopped_serving_millis,
            recovered: Some(replacement),
        },
    )
}
