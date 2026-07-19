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
        Profile::Quality => vec!["structure", "fmt", "clippy", "workspace-tests"],
        Profile::Sim => vec!["seeded-simulation-suite"],
        Profile::Model => vec![
            "bounded-state-explorer",
            "multi-domain-bounded-state-explorer",
        ],
        Profile::E2e => vec![
            "exact-actor-ref-child-watch",
            "gateway-entity-ref-remote-shard",
            "static-discovery-lifecycle",
            "config-store-discovery-lifecycle",
            "multi-domain-failover",
            "single-member-etcd",
            "tcp",
            "mutual-tls",
            "claimed-entity-ref",
        ],
        Profile::E2eHaEtcd => vec![
            "etcd-coordinator-failover",
            "leader-recovery-resume",
            "singleton-forward-recovery",
        ],
        Profile::Scale => vec!["sixty-four-node-convergence"],
        Profile::Chaos => vec![
            "multi-domain-failover",
            "docker-fault-sequence",
            "one-domain-coordinator-loss",
            "membership-loss",
            "drain-force-remove",
            "etcd-lease-expiry",
            "multi-domain-trace-replay",
            "seed-corpus",
        ],
        Profile::K8s => vec!["k8s-lifecycle"],
        Profile::Soak => vec!["bounded-seeded-soak"],
    }
}
