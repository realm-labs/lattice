use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures_util::{StreamExt, stream};
use lattice_discovery::provider::{
    ClusterDiscovery, DiscoveryOrigin, DiscoverySnapshot, DiscoveryTarget,
};
use lattice_remoting::association::{Association, AssociationManager, AssociationState};
use lattice_remoting::bootstrap::{
    BootstrapHandler, BootstrapLeader, BootstrapProbeTarget, BootstrapRequest, BootstrapResult,
    BootstrapRoute,
};
use lattice_remoting::endpoint::{EndpointError, RemotingEndpoint};
use lattice_remoting::handshake::NodeIdentity;
use thiserror::Error;
use tokio::sync::{mpsc, watch};

use crate::config::ClusterJoinConfig;

#[derive(Debug)]
pub enum JoinEvent {
    Coordinator {
        leader: BootstrapLeader,
        association: Arc<Association>,
    },
    CoordinatorLost {
        leader: BootstrapLeader,
    },
    TerminalFailure(JoinError),
}

pub struct JoinController {
    discovery: Arc<dyn ClusterDiscovery>,
    endpoint: Arc<RemotingEndpoint>,
    associations: Arc<AssociationManager>,
    config: ClusterJoinConfig,
}

impl JoinController {
    pub fn new(
        discovery: Arc<dyn ClusterDiscovery>,
        endpoint: Arc<RemotingEndpoint>,
        associations: Arc<AssociationManager>,
        config: ClusterJoinConfig,
    ) -> Result<Self, JoinError> {
        config.validate().map_err(JoinError::Config)?;
        Ok(Self {
            discovery,
            endpoint,
            associations,
            config,
        })
    }

    pub async fn run(
        self: Arc<Self>,
        events: mpsc::Sender<JoinEvent>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let started = tokio::time::Instant::now();
        let mut snapshots = self.discovery.snapshots();
        let mut latest = None;
        let mut discovery_closed = false;
        let mut initial_join = true;
        let mut backoff = RetryBackoff::new(self.config.clone());
        let mut attempt = 0_u64;
        loop {
            if initial_join
                && self
                    .config
                    .join_timeout
                    .is_some_and(|timeout| started.elapsed() >= timeout)
            {
                let _ = events
                    .send(JoinEvent::TerminalFailure(JoinError::JoinTimeout))
                    .await;
                return;
            }
            if latest.is_none() {
                if discovery_closed {
                    let _ = events
                        .send(JoinEvent::TerminalFailure(JoinError::DiscoveryClosed))
                        .await;
                    return;
                }
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { return; }
                    }
                    snapshot = snapshots.next() => {
                        match snapshot {
                            Some(Ok(snapshot)) => {
                                tracing::info!(
                                    target: "lattice.cluster.discovery",
                                    generation = snapshot.generation,
                                    targets = snapshot.targets.len(),
                                    "discovery replacement snapshot"
                                );
                                latest = Some(snapshot);
                            }
                            Some(Err(error)) => {
                                tracing::warn!(
                                    target: "lattice.cluster.discovery",
                                    %error,
                                    "discovery provider retained its last valid snapshot"
                                );
                                continue;
                            }
                            None => discovery_closed = true,
                        }
                    }
                }
            }
            let Some(snapshot) = latest.clone() else {
                continue;
            };
            attempt = attempt.saturating_add(1);
            let probe_started = tokio::time::Instant::now();
            match probe_snapshot(&self.endpoint, snapshot, self.config.probe_concurrency).await {
                Ok(leader) => {
                    tracing::info!(
                        target: "lattice.cluster.join",
                        attempt,
                        latency_millis = probe_started.elapsed().as_millis() as u64,
                        leader_node_id = %leader.identity.node_id,
                        leader_incarnation = leader.identity.incarnation.get(),
                        coordinator_term = leader.term,
                        protocol_generation = leader.protocol_generation,
                        "authenticated Coordinator bootstrap leader selected"
                    );
                    if let Ok(association) =
                        establish_coordinator(&self.endpoint, &self.associations, &leader).await
                    {
                        initial_join = false;
                        backoff.reset();
                        if events
                            .send(JoinEvent::Coordinator {
                                leader: leader.clone(),
                                association: association.clone(),
                            })
                            .await
                            .is_err()
                        {
                            return;
                        }
                        loop {
                            if association.state() != AssociationState::Active {
                                tracing::warn!(
                                    target: "lattice.cluster.join",
                                    leader_node_id = %leader.identity.node_id,
                                    coordinator_term = leader.term,
                                    "Coordinator association lost; reconciliation required"
                                );
                                let _ = events
                                    .send(JoinEvent::CoordinatorLost {
                                        leader: leader.clone(),
                                    })
                                    .await;
                                break;
                            }
                            tokio::select! {
                                changed = shutdown.changed() => {
                                    if changed.is_err() || *shutdown.borrow() { return; }
                                }
                            snapshot = snapshots.next(), if !discovery_closed => {
                                match snapshot {
                                    Some(Ok(snapshot)) => latest = Some(snapshot),
                                    Some(Err(_)) => {}
                                    None => discovery_closed = true,
                                    }
                                }
                                () = tokio::time::sleep(Duration::from_millis(100)) => {}
                            }
                        }
                    }
                }
                Err(JoinError::ConflictingLeaders) => {
                    let _ = events
                        .send(JoinEvent::TerminalFailure(JoinError::ConflictingLeaders))
                        .await;
                    return;
                }
                Err(error) => {
                    tracing::warn!(
                        target: "lattice.cluster.join",
                        attempt,
                        latency_millis = probe_started.elapsed().as_millis() as u64,
                        %error,
                        "cluster join attempt remains retryable"
                    );
                }
            }
            let delay = backoff.next_delay();
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { return; }
                }
                snapshot = snapshots.next(), if !discovery_closed => {
                    match snapshot {
                        Some(Ok(snapshot)) => latest = Some(snapshot),
                        Some(Err(_)) => {}
                        None => discovery_closed = true,
                    }
                }
                () = tokio::time::sleep(delay) => {}
            }
        }
    }
}

async fn probe_snapshot(
    endpoint: &Arc<RemotingEndpoint>,
    snapshot: DiscoverySnapshot,
    concurrency: usize,
) -> Result<BootstrapLeader, JoinError> {
    if snapshot.targets.is_empty() {
        return Err(JoinError::NoCandidates);
    }
    let results = stream::iter(snapshot.targets.into_iter().map(|target| {
        let endpoint = endpoint.clone();
        async move { endpoint.probe_candidate(probe_target(target)).await }
    }))
    .buffer_unordered(concurrency)
    .collect::<Vec<_>>()
    .await;
    let mut leaders = Vec::new();
    for response in results.into_iter().flatten() {
        match response.result {
            BootstrapResult::Identity {
                remote,
                leader: Some(leader),
            }
            | BootstrapResult::ReverseDial {
                remote,
                leader: Some(leader),
            } => {
                let _ = remote;
                leaders.push(leader);
            }
            BootstrapResult::Redirect { leader, .. } => leaders.push(leader),
            BootstrapResult::Identity { leader: None, .. }
            | BootstrapResult::ReverseDial { leader: None, .. }
            | BootstrapResult::Rejected { .. }
            | BootstrapResult::RetryAfter { .. } => {}
        }
    }
    select_leader(leaders)
}

fn probe_target(target: DiscoveryTarget) -> BootstrapProbeTarget {
    let tls_server_name = target.source.origins().find_map(|origin| match origin {
        DiscoveryOrigin::Dns { server_name, .. } => Some(server_name.clone()),
        DiscoveryOrigin::Static { .. }
        | DiscoveryOrigin::ConfigStore { .. }
        | DiscoveryOrigin::KubernetesEndpointSlice { .. } => None,
    });
    BootstrapProbeTarget {
        address: target.address,
        expected_node_id: target.expected_node_id,
        tls_server_name,
    }
}

fn select_leader(mut leaders: Vec<BootstrapLeader>) -> Result<BootstrapLeader, JoinError> {
    if leaders.is_empty() {
        return Err(JoinError::NoLeader);
    }
    leaders.sort_by_key(|leader| (leader.term, leader.protocol_generation));
    let selected = leaders.pop().expect("nonempty leader candidates");
    if leaders
        .iter()
        .any(|leader| leader.term == selected.term && leader.identity != selected.identity)
    {
        return Err(JoinError::ConflictingLeaders);
    }
    Ok(selected)
}

async fn establish_coordinator(
    endpoint: &Arc<RemotingEndpoint>,
    associations: &Arc<AssociationManager>,
    leader: &BootstrapLeader,
) -> Result<Arc<Association>, JoinError> {
    associations
        .replace_remote_incarnation(leader.identity.address.clone(), leader.identity.incarnation);
    match endpoint.connect_peer(leader.identity.clone()).await {
        Ok(association) => Ok(association),
        Err(EndpointError::WrongDialDirection) => {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                if let Some(association) = associations.get_exact(
                    &leader.identity.cluster_id,
                    &leader.identity.address,
                    leader.identity.incarnation,
                ) && association.state() == AssociationState::Active
                {
                    return Ok(association);
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(JoinError::AssociationTimeout);
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
        Err(error) => Err(JoinError::Endpoint(error)),
    }
}

#[derive(Debug)]
pub struct BootstrapView {
    local: NodeIdentity,
    leader: RwLock<Option<BootstrapLeader>>,
}

impl BootstrapView {
    pub fn new(local: NodeIdentity) -> Self {
        Self {
            local,
            leader: RwLock::new(None),
        }
    }

    pub fn install(&self, leader: BootstrapLeader) {
        *self.leader.write().expect("bootstrap leader view poisoned") = Some(leader);
    }

    pub fn clear(&self) {
        *self.leader.write().expect("bootstrap leader view poisoned") = None;
    }
}

impl BootstrapHandler for BootstrapView {
    fn route(&self, _request: &BootstrapRequest) -> BootstrapRoute {
        let Some(leader) = self
            .leader
            .read()
            .expect("bootstrap leader view poisoned")
            .clone()
        else {
            return BootstrapRoute::RetryAfter {
                delay: Duration::from_secs(1),
                reason: "Coordinator leader is unavailable".to_string(),
            };
        };
        if leader.identity == self.local {
            BootstrapRoute::Accept {
                leader: Some(leader),
            }
        } else {
            BootstrapRoute::Redirect { leader }
        }
    }
}

struct RetryBackoff {
    config: ClusterJoinConfig,
    current: Duration,
    sequence: u64,
}

impl RetryBackoff {
    fn new(config: ClusterJoinConfig) -> Self {
        Self {
            current: config.retry_initial,
            config,
            sequence: 0,
        }
    }

    fn reset(&mut self) {
        self.current = self.config.retry_initial;
        self.sequence = 0;
    }

    fn next_delay(&mut self) -> Duration {
        self.sequence = self.sequence.wrapping_add(1);
        let unit = (self.sequence.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 11) as f64
            / ((1_u64 << 53) as f64);
        let factor = 1.0 + ((unit * 2.0) - 1.0) * self.config.retry_jitter;
        let delay = self.current.mul_f64(factor);
        self.current = self
            .current
            .mul_f64(self.config.retry_multiplier)
            .min(self.config.retry_max);
        delay
    }
}

#[derive(Debug, Error)]
pub enum JoinError {
    #[error("cluster join configuration is invalid")]
    Config(#[source] crate::config::ClusterJoinConfigError),
    #[error("cluster discovery stream ended")]
    DiscoveryClosed,
    #[error("cluster join timed out")]
    JoinTimeout,
    #[error("discovery snapshot contains no candidates")]
    NoCandidates,
    #[error("no candidate reported a Coordinator leader")]
    NoLeader,
    #[error("candidates reported conflicting leaders in the same term")]
    ConflictingLeaders,
    #[error("Coordinator reverse association did not become active")]
    AssociationTimeout,
    #[error("Coordinator endpoint failed")]
    Endpoint(#[source] EndpointError),
}

#[cfg(test)]
mod tests {
    use lattice_core::actor_ref::{ClusterId, NodeAddress, NodeIncarnation};
    use lattice_remoting::bootstrap::BootstrapLeader;
    use lattice_remoting::handshake::NodeIdentity;

    use super::{JoinError, select_leader};

    fn leader(node: &str, term: u64, generation: u64) -> BootstrapLeader {
        BootstrapLeader {
            identity: NodeIdentity {
                cluster_id: ClusterId::new("cluster").unwrap(),
                node_id: node.to_string(),
                address: NodeAddress::new(node, 7447).unwrap(),
                incarnation: NodeIncarnation::new(u128::from(term + generation)).unwrap(),
            },
            term,
            protocol_generation: generation,
        }
    }

    #[test]
    fn selects_highest_term_then_generation() {
        assert_eq!(
            select_leader(vec![leader("a", 1, 9), leader("b", 2, 1)])
                .unwrap()
                .identity
                .node_id,
            "b"
        );
    }

    #[test]
    fn rejects_different_leaders_in_same_term() {
        assert!(matches!(
            select_leader(vec![leader("a", 2, 1), leader("b", 2, 2)]),
            Err(JoinError::ConflictingLeaders)
        ));
    }
}
