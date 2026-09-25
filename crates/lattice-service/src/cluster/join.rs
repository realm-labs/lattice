use std::{
    collections::{BTreeMap, hash_map::RandomState},
    future::pending,
    hash::BuildHasher,
    sync::{Arc, RwLock},
    time::Duration,
};

use futures_util::{Stream, StreamExt, stream};
use lattice_discovery::provider::{
    CoordinatorDirectorySnapshot, CoordinatorDiscovery, DiscoveryError, DiscoveryTarget,
    validate_snapshot,
};
use lattice_discovery::shared::SharedDiscovery;
use lattice_model::cluster::CoordinatorScope;
use lattice_remoting::{
    association::{Association, AssociationManager, AssociationState},
    bootstrap::{
        BootstrapHandler, BootstrapLeader, BootstrapProbeTarget, BootstrapRequest, BootstrapResult,
        BootstrapRoute,
    },
    endpoint::{EndpointError, RemotingEndpoint},
    handshake::NodeIdentity,
};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Instant, MissedTickBehavior},
};

use crate::config::{ClusterJoinConfig, ClusterJoinConfigError};

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
    discovery: Arc<dyn CoordinatorDiscovery>,
    endpoint: Arc<RemotingEndpoint>,
    associations: Arc<AssociationManager>,
    config: ClusterJoinConfig,
}

impl JoinController {
    pub fn new(
        discovery: Arc<dyn CoordinatorDiscovery>,
        endpoint: Arc<RemotingEndpoint>,
        associations: Arc<AssociationManager>,
        config: ClusterJoinConfig,
    ) -> Result<Self, JoinError> {
        config.validate().map_err(JoinError::Config)?;
        Ok(Self {
            discovery: Arc::new(SharedDiscovery::new(discovery)),
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
        if *shutdown.borrow() {
            return;
        }
        let (joined_tx, mut joined_rx) = oneshot::channel();
        let timeout = self.config.join_timeout;
        let deadline = async move {
            match timeout {
                Some(timeout) => tokio::time::sleep(timeout).await,
                None => pending::<()>().await,
            }
        };
        let run = self.run_inner(events.clone(), shutdown.clone(), Some(joined_tx));
        tokio::pin!(run, deadline);
        let mut joining = true;
        loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { return; }
                }
                () = &mut run => return,
                _ = &mut joined_rx, if joining => joining = false,
                () = &mut deadline, if joining => {
                    let _ = events.send(JoinEvent::TerminalFailure(JoinError::JoinTimeout)).await;
                    return;
                }
            }
        }
    }

    async fn run_inner(
        self: Arc<Self>,
        events: mpsc::Sender<JoinEvent>,
        mut shutdown: watch::Receiver<bool>,
        mut joined: Option<oneshot::Sender<()>>,
    ) {
        let mut snapshots = self.discovery.snapshots();
        let mut latest = None;
        let mut discovery_closed = false;
        let mut backoff = RetryBackoff::new(self.config.clone());
        let mut attempt = 0_u64;
        loop {
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
            let probe_started = Instant::now();
            match probe_snapshot(&self.endpoint, snapshot, self.config.probe_concurrency).await {
                Ok(leader) => {
                    tracing::info!(
                        target: "lattice.cluster.join",
                        attempt,
                        latency_millis = probe_started.elapsed().as_millis() as u64,
                        leader_node_id = %leader.identity.node_id,
                        leader_incarnation = leader.identity.incarnation.get(),
                        coordinator_term = leader.term,
                        "authenticated Coordinator bootstrap leader selected"
                    );
                    if let Ok(association) =
                        establish_coordinator(&self.endpoint, &self.associations, &leader).await
                    {
                        if let Some(joined) = joined.take() {
                            let _ = joined.send(());
                        }
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
                        let mut leadership_refresh =
                            tokio::time::interval(self.config.leadership_refresh_interval);
                        leadership_refresh.set_missed_tick_behavior(MissedTickBehavior::Skip);
                        leadership_refresh.reset();
                        let mut leadership_confirmed_at = Instant::now();
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
                                _ = leadership_refresh.tick() => {
                                    let Some(snapshot) = latest.clone() else {
                                        continue;
                                    };
                                    match refresh_leadership(
                                        &self.endpoint,
                                        snapshot,
                                        &leader,
                                        self.config.probe_concurrency,
                                    ).await {
                                        Ok(LeadershipRefresh::Confirmed) => {
                                            leadership_confirmed_at = Instant::now();
                                        }
                                        Ok(LeadershipRefresh::Replaced(observed)) =>
                                        {
                                            tracing::warn!(
                                                target: "lattice.cluster.join",
                                                leader_node_id = %leader.identity.node_id,
                                                coordinator_term = leader.term,
                                                replacement_node_id = %observed.identity.node_id,
                                                replacement_term = observed.term,
                                                "Coordinator leadership changed on an active association; reconciliation required"
                                            );
                                            let _ = events
                                                .send(JoinEvent::CoordinatorLost {
                                                    leader: leader.clone(),
                                                })
                                                .await;
                                            break;
                                        }
                                        Err(JoinError::ConflictingLeaders) => {
                                            let _ = events
                                                .send(JoinEvent::TerminalFailure(
                                                    JoinError::ConflictingLeaders,
                                                ))
                                                .await;
                                            return;
                                        }
                                        Ok(LeadershipRefresh::Unconfirmed) | Err(_) => {
                                            if leadership_confirmed_at.elapsed()
                                                >= self.config.discovery_stale_grace
                                            {
                                                tracing::warn!(
                                                    target: "lattice.cluster.join",
                                                    leader_node_id = %leader.identity.node_id,
                                                    coordinator_term = leader.term,
                                                    stale_millis = leadership_confirmed_at.elapsed().as_millis() as u64,
                                                    "Coordinator leadership freshness expired; reconciliation required"
                                                );
                                                let _ = events
                                                    .send(JoinEvent::CoordinatorLost {
                                                        leader: leader.clone(),
                                                    })
                                                    .await;
                                                break;
                                            }
                                        }
                                    }
                                }
                                () = tokio::time::sleep(Duration::from_millis(100)) => {}
                            }
                        }
                    } else if let Some(snapshot) = latest.as_mut() {
                        // A hint may bootstrap successfully but name an
                        // unreachable leader. Retry through the candidate set
                        // until the provider supplies a fresh hint.
                        snapshot.leader_hint = None;
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
            let retry_at = Instant::now() + backoff.next_delay();
            if !wait_for_retry(
                retry_at,
                &mut snapshots,
                &mut latest,
                &mut discovery_closed,
                &mut shutdown,
            )
            .await
            {
                return;
            }
        }
    }
}

async fn wait_for_retry<S>(
    retry_at: Instant,
    snapshots: &mut S,
    latest: &mut Option<CoordinatorDirectorySnapshot>,
    discovery_closed: &mut bool,
    shutdown: &mut watch::Receiver<bool>,
) -> bool
where
    S: Stream<Item = Result<CoordinatorDirectorySnapshot, DiscoveryError>> + Unpin,
{
    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { return false; }
            }
            () = tokio::time::sleep_until(retry_at) => return true,
            snapshot = snapshots.next(), if !*discovery_closed => {
                match snapshot {
                    Some(Ok(snapshot)) => *latest = Some(snapshot),
                    Some(Err(_)) => {}
                    None => *discovery_closed = true,
                }
            }
        }
    }
}

fn leadership_replaced(current: &BootstrapLeader, observed: &BootstrapLeader) -> bool {
    observed.term >= current.term && observed != current
}

enum LeadershipRefresh {
    Confirmed,
    Replaced(BootstrapLeader),
    Unconfirmed,
}

async fn refresh_leadership(
    endpoint: &Arc<RemotingEndpoint>,
    snapshot: CoordinatorDirectorySnapshot,
    current: &BootstrapLeader,
    concurrency: usize,
) -> Result<LeadershipRefresh, JoinError> {
    let mut current_target = snapshot.clone();
    current_target.leader_hint = None;
    if let Some(hint) = &snapshot.leader_hint
        && !current_target
            .targets
            .iter()
            .any(|target| target.address == hint.address)
    {
        current_target.targets.push(hint.clone());
    }
    current_target.targets.retain(|target| {
        target.address == current.identity.address
            && target
                .expected_node_id
                .as_ref()
                .is_none_or(|node_id| node_id == &current.identity.node_id)
    });
    if !current_target.targets.is_empty() {
        match probe_snapshot(endpoint, current_target, 1).await {
            Ok(observed) if observed == *current => return Ok(LeadershipRefresh::Confirmed),
            Ok(observed) if leadership_replaced(current, &observed) => {
                return Ok(LeadershipRefresh::Replaced(observed));
            }
            Ok(_) => {}
            Err(JoinError::ConflictingLeaders) => return Err(JoinError::ConflictingLeaders),
            Err(_) => {}
        }
    }
    match probe_snapshot(endpoint, snapshot, concurrency).await {
        Ok(observed) if observed == *current => Ok(LeadershipRefresh::Confirmed),
        Ok(observed) if leadership_replaced(current, &observed) => {
            Ok(LeadershipRefresh::Replaced(observed))
        }
        Ok(_) | Err(JoinError::NoCandidates | JoinError::NoLeader) => {
            Ok(LeadershipRefresh::Unconfirmed)
        }
        Err(error) => Err(error),
    }
}

async fn probe_snapshot(
    endpoint: &Arc<RemotingEndpoint>,
    snapshot: CoordinatorDirectorySnapshot,
    concurrency: usize,
) -> Result<BootstrapLeader, JoinError> {
    validate_snapshot(&snapshot).map_err(JoinError::Discovery)?;
    if let Some(hint) = snapshot.leader_hint {
        match probe_targets(endpoint, snapshot.scope.clone(), vec![hint.clone()], 1).await {
            Ok(leader) => return Ok(leader),
            Err(JoinError::ConflictingLeaders) => return Err(JoinError::ConflictingLeaders),
            Err(_) => {}
        }
        let targets = snapshot
            .targets
            .into_iter()
            .filter(|target| target.address != hint.address)
            .collect();
        return probe_targets(endpoint, snapshot.scope, targets, concurrency).await;
    }
    probe_targets(endpoint, snapshot.scope, snapshot.targets, concurrency).await
}

async fn probe_targets(
    endpoint: &Arc<RemotingEndpoint>,
    scope: CoordinatorScope,
    targets: Vec<DiscoveryTarget>,
    concurrency: usize,
) -> Result<BootstrapLeader, JoinError> {
    if targets.is_empty() {
        return Err(JoinError::NoCandidates);
    }
    let results = stream::iter(targets.into_iter().map(|target| {
        let endpoint = endpoint.clone();
        let scope = scope.clone();
        async move { endpoint.probe_candidate(probe_target(scope, target)).await }
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

fn probe_target(scope: CoordinatorScope, target: DiscoveryTarget) -> BootstrapProbeTarget {
    let tls_server_name = target.tls_server_name().map(str::to_string);
    BootstrapProbeTarget {
        scope,
        address: target.address,
        expected_node_id: target.expected_node_id,
        tls_server_name,
    }
}

fn select_leader(mut leaders: Vec<BootstrapLeader>) -> Result<BootstrapLeader, JoinError> {
    if leaders.is_empty() {
        return Err(JoinError::NoLeader);
    }
    leaders.sort_by_key(|leader| leader.term);
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
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if let Some(association) = associations.get_exact(
                    &leader.identity.cluster_id,
                    &leader.identity.address,
                    leader.identity.incarnation,
                ) && association.state() == AssociationState::Active
                {
                    return Ok(association);
                }
                if Instant::now() >= deadline {
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
    leaders: RwLock<BTreeMap<CoordinatorScope, BootstrapLeader>>,
}

impl BootstrapView {
    pub fn new(local: NodeIdentity) -> Self {
        Self {
            local,
            leaders: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn install(&self, leader: BootstrapLeader) {
        self.leaders
            .write()
            .expect("bootstrap leader view poisoned")
            .insert(leader.scope.clone(), leader);
    }

    pub fn clear(&self, scope: &CoordinatorScope) {
        self.leaders
            .write()
            .expect("bootstrap leader view poisoned")
            .remove(scope);
    }

    pub fn replace(&self, leaders: Vec<BootstrapLeader>) {
        *self
            .leaders
            .write()
            .expect("bootstrap leader view poisoned") = leaders
            .into_iter()
            .map(|leader| (leader.scope.clone(), leader))
            .collect();
    }
}

impl BootstrapHandler for BootstrapView {
    fn route(&self, request: &BootstrapRequest) -> BootstrapRoute {
        let Some(leader) = self
            .leaders
            .read()
            .expect("bootstrap leader view poisoned")
            .get(&request.scope)
            .cloned()
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
            sequence: RandomState::new().hash_one("coordinator-retry"),
        }
    }

    fn reset(&mut self) {
        self.current = self.config.retry_initial;
    }

    fn next_delay(&mut self) -> Duration {
        self.sequence = self.sequence.wrapping_add(1);
        let unit = (self.sequence.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 11) as f64
            / ((1_u64 << 53) as f64);
        let factor = 1.0 + ((unit * 2.0) - 1.0) * self.config.retry_jitter;
        let delay = Duration::try_from_secs_f64(self.current.as_secs_f64() * factor)
            .unwrap_or(self.config.retry_max)
            .min(self.config.retry_max);
        self.current =
            Duration::try_from_secs_f64(self.current.as_secs_f64() * self.config.retry_multiplier)
                .unwrap_or(self.config.retry_max)
                .min(self.config.retry_max);
        delay
    }
}

#[derive(Debug, Error)]
pub enum JoinError {
    #[error("cluster discovery snapshot is invalid")]
    Discovery(#[source] DiscoveryError),
    #[error("cluster join configuration is invalid")]
    Config(#[source] ClusterJoinConfigError),
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
mod tests;
