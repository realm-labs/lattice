use std::path::Path;
use std::time::{Duration, Instant};

use serde::Serialize;

use super::testctl_split_cluster::{
    ClusterReleaseArtifact, NODES, QUIET_PERIOD, ReleaseIdentity, SplitCluster, SplitHostArtifact,
    now_millis, require_conserved_probes, require_disjoint_activations, require_retired,
    serving_windows,
};
use super::{MultiDomainHostArtifact, wait_for_scope_across_hosts, write_json, write_json_atomic};

/// The spare entity host. It runs the same binary and the same image as the two observers and
/// stays out of the cluster until a scenario gives it a release, so what it presents to the
/// Coordinator is a release manifest and nothing else.
const JOINER: &str = "split-node-c";

/// Every fixture host that can hold a placement slot for the shared entity.
const HOSTS: [&str; 3] = [NODES[0], NODES[1], JOINER];

/// The Coordinator hosts that can lead the scopes this entity depends on. They are control plane
/// nodes, never rollout participants, and they stay on the release they booted with for the whole
/// run.
/// The standby host is asked first because it is the only candidate for the entity's placement
/// domain, so both scopes are read from one directory and a scope that changed hands cannot be
/// confused with two hosts disagreeing about who holds it.
const COORDINATOR_ARTIFACTS: [&str; 5] = [
    "domain-standby.json",
    "domain-membership.json",
    "domain-alpha.json",
    "domain-beta.json",
    "domain-gamma.json",
];

const SPLIT_PLACEMENT_SCOPE: &str = "placement:domain-split";

/// How long a host is given to hand its slots back, leave, rejoin under another release and be seen
/// on it by every other member.
const UPGRADE_BUDGET: Duration = Duration::from_secs(90);
/// How long the entity is given to reappear somewhere after the host that owned it left.
const HANDOFF_BUDGET: Duration = Duration::from_secs(60);
/// How long a release the guard must refuse is held against the cluster. It is only meaningful
/// against the time an admissible release takes to get in, which every refusal scenario measures on
/// the same host immediately afterwards.
const REFUSAL_WINDOW: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_millis(200);

#[derive(Debug, Clone, Serialize)]
struct ReleaseObservation {
    node_id: String,
    lifecycle: String,
    release_id: Option<u64>,
    rollout_members: Vec<(String, u64)>,
}

impl ReleaseObservation {
    fn of(host: &SplitHostArtifact) -> Self {
        Self {
            node_id: host.node_id.clone(),
            lifecycle: host.lifecycle.clone(),
            release_id: host.release.as_ref().map(|release| release.release_id),
            rollout_members: host
                .rollout_members
                .iter()
                .map(|member| (member.node_id.clone(), member.release_id))
                .collect(),
        }
    }
}

/// Reads what a host published after `since`. Anything older describes a cluster that existed
/// before the scenario acted, so it is not an answer to the question being asked.
fn fresh_host(
    cluster: &SplitCluster,
    node: &str,
    since: u128,
) -> Result<Option<SplitHostArtifact>, String> {
    let path = cluster.host_artifact(node);
    let encoded = match std::fs::read(&path) {
        Ok(encoded) => encoded,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("{}: {error}", path.display())),
    };
    let host = serde_json::from_slice::<SplitHostArtifact>(&encoded)
        .map_err(|error| format!("{}: {error}", path.display()))?;
    require_legal_release_state(&host)?;
    Ok((host.unix_millis >= since).then_some(host))
}

/// A member set whose releases have no legal reading is the signature of an admission guard that
/// let something through. It is a failure wherever it is observed, not a state to wait out.
fn require_legal_release_state(host: &SplitHostArtifact) -> Result<(), String> {
    if let ClusterReleaseArtifact::Invalid { error } = host.cluster_release() {
        return Err(format!(
            "{} is in a cluster whose live releases have no legal state, which only an admission \
             guard that let one through can produce: {error}",
            host.node_id
        ));
    }
    Ok(())
}

fn wait_for_host(
    cluster: &SplitCluster,
    node: &str,
    since: u128,
    timeout: Duration,
    expectation: &str,
    want: impl Fn(&SplitHostArtifact) -> bool,
) -> Result<SplitHostArtifact, String> {
    let deadline = Instant::now() + timeout;
    let mut last = None;
    loop {
        if let Some(host) = fresh_host(cluster, node, since)? {
            if want(&host) {
                return Ok(host);
            }
            last = Some(ReleaseObservation::of(&host));
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{node} did not report {expectation} within {timeout:?}; last observation: {last:?}"
            ));
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Waits until both observers agree on exactly which releases the cluster is running. Agreement
/// matters: one host that has not caught up yet is not an upgrade that has finished.
fn wait_for_releases(
    cluster: &SplitCluster,
    since: u128,
    expected: &[u64],
    timeout: Duration,
) -> Result<(), String> {
    let mut wanted = expected.to_vec();
    wanted.sort_unstable();
    for node in NODES {
        wait_for_host(
            cluster,
            node,
            since,
            timeout,
            &format!("the cluster running releases {wanted:?}"),
            |host| host.cluster_release().releases() == wanted,
        )?;
    }
    Ok(())
}

/// Waits until every observer sees `node` as a rollout participant on `release`, which is the only
/// thing that proves a release reached the cluster rather than merely the host that runs it.
fn wait_for_member_release(
    cluster: &SplitCluster,
    node: &str,
    release_id: u64,
    since: u128,
    timeout: Duration,
) -> Result<(), String> {
    for observer in NODES {
        wait_for_host(
            cluster,
            observer,
            since,
            timeout,
            &format!("{node} as a member on release {release_id}"),
            |host| host.release_of(node) == Some(release_id),
        )?;
    }
    Ok(())
}

/// The releases both observers agree the cluster runs, once they agree at all.
fn stable_release(cluster: &SplitCluster, since: u128, timeout: Duration) -> Result<u64, String> {
    let deadline = Instant::now() + timeout;
    loop {
        let observed = NODES
            .iter()
            .map(|node| fresh_host(cluster, node, since))
            .collect::<Result<Vec<_>, _>>()?;
        if let [Some(first), Some(second)] = observed.as_slice()
            && let (
                ClusterReleaseArtifact::Stable { release_id },
                ClusterReleaseArtifact::Stable { release_id: agreed },
            ) = (first.cluster_release(), second.cluster_release())
            && release_id == agreed
            && first.rollout_members.len() == NODES.len()
        {
            return Ok(release_id);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "the cluster did not settle on one release across {:?} within {timeout:?}",
                NODES
            ));
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

fn set_release(
    cluster: &SplitCluster,
    node: &str,
    release: &ReleaseIdentity,
) -> Result<(), String> {
    write_json_atomic(&cluster.release_file(node), release)
}

fn clear_release(cluster: &SplitCluster, node: &str) -> Result<(), String> {
    match std::fs::remove_file(cluster.release_file(node)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("{node} release file: {error}")),
    }
}

/// Moves one host onto another release and waits until the whole cluster has seen it come back on
/// it. This is the upgrade motion itself: the host hands its slots back, leaves membership, and
/// rejoins under a new incarnation carrying the manifest it was given.
fn upgrade_host(
    cluster: &SplitCluster,
    node: &str,
    release: &ReleaseIdentity,
    timeout: Duration,
) -> Result<u128, String> {
    let requested = now_millis()?;
    set_release(cluster, node, release)?;
    wait_for_host(
        cluster,
        node,
        requested,
        timeout,
        &format!("release {} ready", release.release_id),
        |host| host.lifecycle == "Ready" && host.release.as_ref() == Some(release),
    )?;
    wait_for_member_release(cluster, node, release.release_id, requested, timeout)?;
    Ok(requested)
}

/// Removes a host from the cluster and waits until only the two observers are left holding the
/// entity, so the next scenario starts from the topology every scenario starts from.
fn retire_joiner(cluster: &SplitCluster, timeout: Duration) -> Result<(), String> {
    let requested = now_millis()?;
    clear_release(cluster, JOINER)?;
    for observer in NODES {
        wait_for_host(
            cluster,
            observer,
            requested,
            timeout,
            "a cluster of exactly the two observers",
            |host| {
                host.rollout_members.len() == NODES.len()
                    && host.release_of(JOINER).is_none()
                    && host.lifecycle == "Ready"
            },
        )?;
    }
    cluster.wait_for_agreed_activation(now_millis()?, timeout)?;
    Ok(())
}

fn served_since(cluster: &SplitCluster, node: &str, since: u128) -> Result<usize, String> {
    Ok(cluster
        .probes(node, since)?
        .into_iter()
        .filter(|record| record.outcome == "served")
        .count())
}

/// Holds a release the guard has to refuse against a live cluster and keeps checking, for the whole
/// window, that nothing about the cluster changed because of it: the refused host never becomes a
/// member, the releases in play stay exactly the ones that were already there, and the entity keeps
/// answering the observers that were already talking to it.
fn require_refused(
    cluster: &SplitCluster,
    release: &ReleaseIdentity,
    expected: &[u64],
    window: Duration,
) -> Result<RefusalEvidence, String> {
    let offered = now_millis()?;
    set_release(cluster, JOINER, release)?;
    let mut wanted = expected.to_vec();
    wanted.sort_unstable();
    let deadline = Instant::now() + window;
    let mut joiner_lifecycles = Vec::new();
    let mut startup_error = None;
    loop {
        for observer in NODES {
            let Some(host) = fresh_host(cluster, observer, offered)? else {
                continue;
            };
            if let Some(admitted) = host.release_of(JOINER) {
                return Err(format!(
                    "{observer} admitted {JOINER} on release {admitted} while the cluster was \
                     already running {wanted:?}"
                ));
            }
            let releases = host.cluster_release().releases();
            if releases != wanted {
                return Err(format!(
                    "{observer} reports the cluster running {releases:?} rather than the {wanted:?} \
                     it was running before a release that must be refused was offered"
                ));
            }
        }
        if let Some(host) = fresh_host(cluster, JOINER, offered)? {
            if host.lifecycle == "Ready" {
                return Err(format!(
                    "{JOINER} reached Ready on release {}, which the cluster must refuse",
                    release.release_id
                ));
            }
            if !joiner_lifecycles.contains(&host.lifecycle) {
                joiner_lifecycles.push(host.lifecycle.clone());
            }
            startup_error = host.startup_error.clone().or(startup_error);
        }
        if Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(POLL_INTERVAL);
    }
    let mut served = Vec::new();
    for observer in NODES {
        let count = served_since(cluster, observer, offered)?;
        if count == 0 {
            return Err(format!(
                "{observer} was not served once while a refused release was being offered, so the \
                 refusal cannot be told apart from an outage"
            ));
        }
        require_conserved_probes(cluster, observer, offered)?;
        served.push((observer.to_owned(), count));
    }
    Ok(RefusalEvidence {
        release: release.clone(),
        offered_unix_millis: offered,
        held_millis: window.as_millis(),
        joiner_lifecycles,
        startup_error: startup_error.map(|error| (error.kind, error.detail)),
        served_during_the_refusal: served,
        admitted_after_millis: None,
    })
}

/// Puts the same host into the same cluster under a release the guard must accept. A refusal only
/// means something if the host, the image and the cluster it was offered to would have taken it
/// otherwise, and the join has to be quicker than the window the refusal was held for.
fn require_admitted(
    cluster: &SplitCluster,
    release: &ReleaseIdentity,
    window: Duration,
) -> Result<u128, String> {
    let requested = upgrade_host(cluster, JOINER, release, UPGRADE_BUDGET)?;
    let admitted = now_millis()?.saturating_sub(requested);
    if admitted >= window.as_millis() {
        return Err(format!(
            "{JOINER} took {admitted}ms to be admitted on release {}, which is not shorter than \
             the {}ms a refused release was held for, so the refusal proves nothing",
            release.release_id,
            window.as_millis()
        ));
    }
    Ok(admitted)
}

#[derive(Debug, Clone, Serialize)]
struct RefusalEvidence {
    release: ReleaseIdentity,
    offered_unix_millis: u128,
    held_millis: u128,
    joiner_lifecycles: Vec<String>,
    startup_error: Option<(String, String)>,
    served_during_the_refusal: Vec<(String, usize)>,
    admitted_after_millis: Option<u128>,
}

/// Fails unless every probe outcome across the window was conserved and no two activations of the
/// entity ever overlapped. Nothing about a rolling upgrade may weaken either.
fn require_uninterrupted(
    cluster: &SplitCluster,
    window_start: u128,
) -> Result<serde_json::Value, String> {
    let windows = serving_windows(cluster, window_start)?;
    require_disjoint_activations(&windows)?;
    let mut conserved = Vec::new();
    for node in NODES {
        conserved.push((
            node.to_owned(),
            require_conserved_probes(cluster, node, window_start)?,
        ));
    }
    Ok(serde_json::json!({
        "probes_with_an_outcome": conserved,
        "serving_windows": windows,
        "overlapping_activation_pairs": 0,
    }))
}

/// Hands the entity from the host that owns it to whatever the Coordinator picks, by upgrading the
/// owner out from under it while an older release is still up and eligible. The replacement must be
/// running the newest release: that is the whole of the rolling upgrade placement rule, and the
/// old-release host being available the entire time is what makes the choice a decision rather than
/// the only option.
fn require_handoff_to_the_newest_release(
    cluster: &SplitCluster,
    owner: &super::testctl_split_cluster::ActivationIdentity,
    newest: &ReleaseIdentity,
) -> Result<String, String> {
    let observer = SplitCluster::peer(&owner.node_id);
    let started = now_millis()?;
    upgrade_host(cluster, &owner.node_id, newest, UPGRADE_BUDGET)?;
    let replacement =
        cluster.wait_for_replacement_activation(observer, owner, started, HANDOFF_BUDGET)?;
    if replacement.node_id == observer {
        return Err(format!(
            "the entity was handed to {observer}, which is still on the older release, while a \
             host on release {} was up and eligible",
            newest.release_id
        ));
    }
    let host = wait_for_host(
        cluster,
        observer,
        started,
        UPGRADE_BUDGET,
        &format!("the release of {}", replacement.node_id),
        |host| host.release_of(&replacement.node_id).is_some(),
    )?;
    let release = host.release_of(&replacement.node_id).unwrap_or_default();
    if release != newest.release_id {
        return Err(format!(
            "the entity was handed to {} on release {release} rather than to a host on the newest \
             release {}",
            replacement.node_id, newest.release_id
        ));
    }
    require_retired(cluster, owner, now_millis()?, QUIET_PERIOD)?;
    Ok(replacement.node_id)
}

/// Rolls a whole cluster from one release to the next the way a deployment does: a host on the new
/// release joins first, the hosts on the old release are moved onto it one at a time, and the extra
/// host is retired once nothing is left behind. The entity is expected to keep answering across all
/// of it.
pub(super) fn code_only_rolling_upgrade(artifacts: &Path) -> Result<(), String> {
    let cluster = SplitCluster::resolve(artifacts)?;
    let window_start = now_millis()?;
    let baseline = cluster.wait_for_agreed_activation(window_start, Duration::from_secs(120))?;
    let current = stable_release(&cluster, window_start, Duration::from_secs(120))?;
    let next = ReleaseIdentity::code_only(current + 1);

    // A host on the new release joins a cluster that is entirely on the old one. Two releases are
    // live from here until the last host has moved.
    let joined = upgrade_host(&cluster, JOINER, &next, UPGRADE_BUDGET)?;
    wait_for_releases(
        &cluster,
        joined,
        &[current, next.release_id],
        UPGRADE_BUDGET,
    )?;

    // The owner is moved onto the new release, which forces a handoff while both releases are live.
    let handed_to = require_handoff_to_the_newest_release(&cluster, &baseline, &next)?;

    // The last host on the old release moves, and the cluster must converge on one release.
    let last = SplitCluster::peer(&baseline.node_id);
    let converging = upgrade_host(&cluster, last, &next, UPGRADE_BUDGET)?;
    wait_for_releases(&cluster, converging, &[next.release_id], UPGRADE_BUDGET)?;

    retire_joiner(&cluster, UPGRADE_BUDGET)?;
    let settled = stable_release(&cluster, now_millis()?, UPGRADE_BUDGET)?;
    if settled != next.release_id {
        return Err(format!(
            "the cluster settled on release {settled} rather than on the release it was upgraded \
             to, {}",
            next.release_id
        ));
    }
    let evidence = require_uninterrupted(&cluster, window_start)?;
    write_json(
        &artifacts.join("entity-code-only-rolling-upgrade.json"),
        &serde_json::json!({
            "run_id": cluster.run_id,
            "window_start_unix_millis": window_start,
            "from_release": current,
            "to_release": next.release_id,
            "handed_over_from": baseline,
            "handed_over_to": handed_to,
            "converged_release": settled,
            "evidence": evidence,
        }),
    )
}

/// A third release has nowhere to go in a cluster that is already rolling from one release to
/// another, and the cluster it is refused by must not notice it happened.
pub(super) fn third_release_refused(artifacts: &Path) -> Result<(), String> {
    let cluster = SplitCluster::resolve(artifacts)?;
    let window_start = now_millis()?;
    cluster.wait_for_agreed_activation(window_start, Duration::from_secs(120))?;
    let current = stable_release(&cluster, window_start, Duration::from_secs(120))?;
    let next = ReleaseIdentity::code_only(current + 1);
    let third = ReleaseIdentity::code_only(current + 2);

    // Roll one observer forward so two releases are live, which is the most a code-only upgrade
    // ever allows.
    let rolling = upgrade_host(&cluster, NODES[0], &next, UPGRADE_BUDGET)?;
    wait_for_releases(
        &cluster,
        rolling,
        &[current, next.release_id],
        UPGRADE_BUDGET,
    )?;

    let mut refusal = require_refused(
        &cluster,
        &third,
        &[current, next.release_id],
        REFUSAL_WINDOW,
    )?;
    // The same host, offered a release that is already live, has to get in immediately. Without
    // this the refusal above could be anything at all about the host or the fixture.
    refusal.admitted_after_millis = Some(require_admitted(&cluster, &next, REFUSAL_WINDOW)?);
    retire_joiner(&cluster, UPGRADE_BUDGET)?;

    let converging = upgrade_host(&cluster, NODES[1], &next, UPGRADE_BUDGET)?;
    wait_for_releases(&cluster, converging, &[next.release_id], UPGRADE_BUDGET)?;
    let evidence = require_uninterrupted(&cluster, window_start)?;
    write_json(
        &artifacts.join("entity-third-release-refused.json"),
        &serde_json::json!({
            "run_id": cluster.run_id,
            "window_start_unix_millis": window_start,
            "live_releases": [current, next.release_id],
            "refused": refusal,
            "converged_release": next.release_id,
            "evidence": evidence,
        }),
    )
}

/// A release that changes the compatibility contract is not a rolling upgrade at all, whether the
/// contract it breaks is the one members compare with each other or the one the node compares with
/// the framework it is linked against.
pub(super) fn incompatible_release_refused(artifacts: &Path) -> Result<(), String> {
    let cluster = SplitCluster::resolve(artifacts)?;
    let window_start = now_millis()?;
    cluster.wait_for_agreed_activation(window_start, Duration::from_secs(120))?;
    let current = stable_release(&cluster, window_start, Duration::from_secs(120))?;

    // Same framework, different application fingerprint: the members cannot run side by side, so
    // the cluster must refuse the join rather than start a rolling upgrade.
    let incompatible = ReleaseIdentity {
        release_id: current + 1,
        protocol_fingerprint: Some(0x5a),
        control_generation: None,
    };
    let mut protocol_refusal =
        require_refused(&cluster, &incompatible, &[current], REFUSAL_WINDOW)?;

    // A release that claims a control plane generation the linked framework does not implement is
    // refused by the node itself, before a Coordinator is ever involved.
    let mismatched = ReleaseIdentity {
        release_id: current + 2,
        protocol_fingerprint: None,
        control_generation: Some(999),
    };
    let generation_refusal = require_refused(&cluster, &mismatched, &[current], REFUSAL_WINDOW)?;
    match &generation_refusal.startup_error {
        Some((kind, _)) if kind == "release" => {}
        other => {
            return Err(format!(
                "{JOINER} did not refuse a release built for another framework generation on its \
                 own; it reported {other:?}"
            ));
        }
    }

    // The same host takes the same release id the moment its compatibility contract matches, which
    // is what isolates the contract as the reason for both refusals.
    let admissible = ReleaseIdentity::code_only(current + 1);
    protocol_refusal.admitted_after_millis =
        Some(require_admitted(&cluster, &admissible, REFUSAL_WINDOW)?);
    retire_joiner(&cluster, UPGRADE_BUDGET)?;
    let settled = stable_release(&cluster, now_millis()?, UPGRADE_BUDGET)?;
    if settled != current {
        return Err(format!(
            "the observers left the release they were on: {settled} rather than {current}"
        ));
    }
    let evidence = require_uninterrupted(&cluster, window_start)?;
    write_json(
        &artifacts.join("entity-incompatible-release-refused.json"),
        &serde_json::json!({
            "run_id": cluster.run_id,
            "window_start_unix_millis": window_start,
            "live_release": current,
            "refused_incompatible_contract": protocol_refusal,
            "refused_framework_generation": generation_refusal,
            "evidence": evidence,
        }),
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct CoordinatorRelease {
    scope: String,
    node_id: String,
    term: u64,
    incarnation: String,
    release_id: u64,
}

fn coordinator_release(artifacts: &Path, scope: &str) -> Result<CoordinatorRelease, String> {
    let leader = wait_for_scope_across_hosts(
        artifacts,
        &COORDINATOR_ARTIFACTS,
        scope,
        0,
        Duration::from_secs(60),
    )?;
    let path = artifacts.join(format!("{}.json", leader.node_id));
    let encoded = std::fs::read(&path).map_err(|error| format!("{}: {error}", path.display()))?;
    let host = serde_json::from_slice::<MultiDomainHostArtifact>(&encoded)
        .map_err(|error| format!("{}: {error}", path.display()))?;
    Ok(CoordinatorRelease {
        scope: scope.to_owned(),
        node_id: leader.node_id,
        term: leader.term,
        incarnation: leader.incarnation.to_string(),
        release_id: host.release_id,
    })
}

/// The Coordinator that admits a release and reassigns the slots is not part of the rollout: it is
/// a control plane node, it is never a rollout participant, and in this fixture it stays on the
/// release it booted with. So every upgrade here is already decided by an older release than the
/// one being rolled out, and this scenario is what states that rather than assuming it: the leaders
/// are pinned before the wave, an entire wave runs, and the same leaders in the same terms must
/// still be the ones that decided it.
pub(super) fn upgrade_under_an_older_coordinator(artifacts: &Path) -> Result<(), String> {
    let cluster = SplitCluster::resolve(artifacts)?;
    let window_start = now_millis()?;
    let baseline = cluster.wait_for_agreed_activation(window_start, Duration::from_secs(120))?;
    let current = stable_release(&cluster, window_start, Duration::from_secs(120))?;
    let next = ReleaseIdentity::code_only(current + 1);
    let before = [
        coordinator_release(artifacts, "membership")?,
        coordinator_release(artifacts, SPLIT_PLACEMENT_SCOPE)?,
    ];
    for leader in &before {
        if HOSTS.contains(&leader.node_id.as_str()) {
            return Err(format!(
                "{} leads {} but is one of the hosts being upgraded, so this scenario cannot show \
                 an older Coordinator deciding the rollout",
                leader.node_id, leader.scope
            ));
        }
        if leader.release_id >= next.release_id {
            return Err(format!(
                "{} leads {} on release {}, which is not older than the release the hosts are \
                 being upgraded to, {}",
                leader.node_id, leader.scope, leader.release_id, next.release_id
            ));
        }
    }

    let joined = upgrade_host(&cluster, JOINER, &next, UPGRADE_BUDGET)?;
    wait_for_releases(
        &cluster,
        joined,
        &[current, next.release_id],
        UPGRADE_BUDGET,
    )?;
    let handed_to = require_handoff_to_the_newest_release(&cluster, &baseline, &next)?;
    let converging = upgrade_host(
        &cluster,
        SplitCluster::peer(&baseline.node_id),
        &next,
        UPGRADE_BUDGET,
    )?;
    wait_for_releases(&cluster, converging, &[next.release_id], UPGRADE_BUDGET)?;
    retire_joiner(&cluster, UPGRADE_BUDGET)?;

    let after = [
        coordinator_release(artifacts, "membership")?,
        coordinator_release(artifacts, SPLIT_PLACEMENT_SCOPE)?,
    ];
    for (before, after) in before.iter().zip(after.iter()) {
        if before != after {
            return Err(format!(
                "{} changed hands during the upgrade: {before:?} became {after:?}, so no single \
                 older Coordinator decided the whole of it",
                before.scope
            ));
        }
    }
    let evidence = require_uninterrupted(&cluster, window_start)?;
    write_json(
        &artifacts.join("entity-upgrade-under-an-older-coordinator.json"),
        &serde_json::json!({
            "run_id": cluster.run_id,
            "window_start_unix_millis": window_start,
            "coordinators": after,
            "from_release": current,
            "to_release": next.release_id,
            "handed_over_to": handed_to,
            "evidence": evidence,
        }),
    )
}
