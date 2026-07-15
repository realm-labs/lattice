use std::path::Path;
use std::time::{Duration, Instant};

use super::{MultiDomainHostArtifact, Profile, ScopedLeadershipArtifact};

pub(super) fn wait_for_host_scope(
    path: &Path,
    scope: &str,
    minimum_term: u64,
    timeout: Duration,
) -> Result<ScopedLeadershipArtifact, String> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(encoded) = std::fs::read(path)
            && let Ok(host) = serde_json::from_slice::<MultiDomainHostArtifact>(&encoded)
            && let Some(leader) = host.scopes.get(scope)
            && leader.term > minimum_term
        {
            return Ok(leader.clone());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{} did not publish {scope} above term {minimum_term}",
                path.display()
            ));
        }
        std::thread::yield_now();
    }
}

pub(super) fn wait_for_scope_across_hosts(
    artifacts: &Path,
    host_artifacts: &[&str],
    scope: &str,
    minimum_term: u64,
    timeout: Duration,
) -> Result<ScopedLeadershipArtifact, String> {
    let deadline = Instant::now() + timeout;
    loop {
        for name in host_artifacts {
            let path = artifacts.join(name);
            if let Ok(encoded) = std::fs::read(&path)
                && let Ok(host) = serde_json::from_slice::<MultiDomainHostArtifact>(&encoded)
                && let Some(leader) = host.scopes.get(scope)
                && leader.term > minimum_term
            {
                return Ok(leader.clone());
            }
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "no candidate host published {scope} above term {minimum_term}"
            ));
        }
        std::thread::yield_now();
    }
}

pub(super) fn for_profile(profile: Profile) -> Vec<&'static str> {
    match profile {
        Profile::Quality => vec!["fmt", "clippy", "workspace-tests"],
        Profile::Sim => vec![
            "seeded-production-reducers",
            "multi-domain-independent-elections",
            "cross-domain-trace-replay",
        ],
        Profile::Model => vec![
            "bounded-state-explorer",
            "multi-domain-bounded-state-explorer",
        ],
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
            "three-domain-leader-distribution",
            "one-domain-leader-loss-isolation",
            "multi-domain-host-loss",
            "membership-leader-loss",
            "multi-domain-logic-recovery",
        ],
        Profile::E2eHaEtcd => vec!["etcd-leader-failover", "coordinator-plan-recovery"],
        Profile::Chaos => vec![
            "one-domain-leader-loss",
            "membership-loss",
            "multi-domain-host-loss",
            "lease-expiry",
            "cross-domain-drain",
            "simultaneous-independent-handoffs",
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
