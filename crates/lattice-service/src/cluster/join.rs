use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use futures_util::{StreamExt, stream};
use lattice_core::coordinator::CoordinatorScope;
use lattice_discovery::provider::{
    CoordinatorDirectorySnapshot, CoordinatorDiscovery, DiscoveryOrigin, DiscoveryTarget,
};
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
    sync::{mpsc, watch},
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
        let started = Instant::now();
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
    if snapshot.targets.is_empty() {
        return Err(JoinError::NoCandidates);
    }
    let scope = snapshot.scope;
    let results = stream::iter(snapshot.targets.into_iter().map(|target| {
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
    let tls_server_name = target.source.origins().find_map(|origin| match origin {
        DiscoveryOrigin::Dns { server_name, .. } => Some(server_name.clone()),
        DiscoveryOrigin::Static { .. }
        | DiscoveryOrigin::ConfigStore { .. }
        | DiscoveryOrigin::KubernetesEndpointSlice { .. } => None,
    });
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
mod tests {
    use std::{sync::Arc, time::Duration};

    use async_trait::async_trait;
    use bytes::Bytes;
    use lattice_core::{
        actor_ref::{ActorRef, ClusterId, NodeAddress, NodeIncarnation},
        coordinator::CoordinatorScope,
    };
    use lattice_discovery::static_provider::{StaticDiscovery, StaticEndpoint};
    use lattice_remoting::{
        association::{AssociationManager, AssociationState},
        bootstrap::BootstrapLeader,
        config::RemotingConfig,
        endpoint::RemotingEndpoint,
        handshake::NodeIdentity,
        messaging::{
            error::RemoteMessageError, inbound::InboundDispatch, outbound::OutboundMessaging,
            target::ExactActorTarget,
        },
    };
    use tokio::sync::watch;

    use super::{
        BootstrapView, JoinController, JoinError, JoinEvent, leadership_replaced, select_leader,
    };
    use crate::{
        config::ClusterJoinConfig,
        test_support::{network_test_guard, unused_address},
    };

    fn leader(node: &str, term: u64, generation: u64) -> BootstrapLeader {
        BootstrapLeader {
            scope: CoordinatorScope::Membership,
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

    #[test]
    fn active_association_is_reconciled_when_its_leadership_term_changes() {
        let current = leader("a", 1, 1);
        assert!(leadership_replaced(&current, &leader("a", 2, 1)));
        assert!(leadership_replaced(&current, &leader("b", 2, 1)));
        assert!(!leadership_replaced(&current, &current));
        assert!(!leadership_replaced(&current, &leader("a", 0, 1)));
    }

    struct RejectDispatch;

    #[async_trait]
    impl InboundDispatch for RejectDispatch {
        async fn tell(
            &self,
            _sender: Option<ActorRef>,
            _target: ExactActorTarget,
            _message_id: u64,
            _payload: Bytes,
        ) -> Result<(), RemoteMessageError> {
            Err(RemoteMessageError::Unauthorized)
        }

        async fn ask(
            &self,
            _target: ExactActorTarget,
            _message_id: u64,
            _payload: Bytes,
            _deadline: std::time::Instant,
        ) -> Result<Bytes, RemoteMessageError> {
            Err(RemoteMessageError::Unauthorized)
        }
    }

    fn endpoint(identity: NodeIdentity) -> (Arc<RemotingEndpoint>, Arc<AssociationManager>) {
        let config = RemotingConfig {
            heartbeat_interval: Duration::from_millis(50),
            ..RemotingConfig::default()
        };
        let associations = Arc::new(
            AssociationManager::new(
                identity.address.clone(),
                identity.incarnation,
                config.clone(),
            )
            .unwrap(),
        );
        let endpoint = Arc::new(
            RemotingEndpoint::builder(
                identity,
                config,
                associations.clone(),
                Arc::new(OutboundMessaging::new(16).unwrap()),
                Arc::new(RejectDispatch),
            )
            .build()
            .unwrap(),
        );
        (endpoint, associations)
    }

    #[tokio::test]
    async fn refreshes_leadership_while_the_transport_association_stays_active() {
        let _network = network_test_guard().await;
        let first = unused_address().await;
        let second = unused_address().await;
        let (client_address, server_address) = if first < second {
            (first, second)
        } else {
            (second, first)
        };
        let cluster_id = ClusterId::new("join-refresh-test").unwrap();
        let client_identity = NodeIdentity {
            cluster_id: cluster_id.clone(),
            node_id: "client".to_owned(),
            address: client_address,
            incarnation: NodeIncarnation::new(1).unwrap(),
        };
        let server_identity = NodeIdentity {
            cluster_id,
            node_id: "coordinator".to_owned(),
            address: server_address.clone(),
            incarnation: NodeIncarnation::new(2).unwrap(),
        };
        let (client, client_associations) = endpoint(client_identity);
        let (server, _) = endpoint(server_identity.clone());
        let view = Arc::new(BootstrapView::new(server_identity.clone()));
        let current = BootstrapLeader {
            scope: CoordinatorScope::Membership,
            identity: server_identity.clone(),
            term: 1,
            protocol_generation: 1,
        };
        view.install(current.clone());
        server.install_bootstrap_handler(view.clone());
        client.bind().await.unwrap();
        server.bind().await.unwrap();

        let discovery = Arc::new(
            StaticDiscovery::new(
                CoordinatorScope::Membership,
                "join-refresh",
                vec![StaticEndpoint {
                    address: server_address,
                    expected_node_id: Some(server_identity.node_id.clone()),
                    priority: 1,
                }],
            )
            .unwrap(),
        );
        let controller = Arc::new(
            JoinController::new(
                discovery,
                client,
                client_associations,
                ClusterJoinConfig {
                    retry_initial: Duration::from_millis(10),
                    retry_max: Duration::from_millis(20),
                    retry_jitter: 0.0,
                    leadership_refresh_interval: Duration::from_millis(25),
                    discovery_stale_grace: Duration::from_millis(100),
                    join_timeout: Some(Duration::from_secs(2)),
                    ..ClusterJoinConfig::default()
                },
            )
            .unwrap(),
        );
        let (events_tx, mut events) = tokio::sync::mpsc::channel(8);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(controller.run(events_tx, shutdown_rx));

        let initial_association = match tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap()
        {
            JoinEvent::Coordinator {
                leader,
                association,
            } => {
                assert_eq!(leader.term, 1);
                association
            }
            event => panic!("unexpected initial join event: {event:?}"),
        };
        view.install(BootstrapLeader { term: 2, ..current });
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), events.recv())
                .await
                .unwrap(),
            Some(JoinEvent::CoordinatorLost { leader }) if leader.term == 1
        ));
        match tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap()
        {
            JoinEvent::Coordinator {
                leader,
                association,
            } => {
                assert_eq!(leader.term, 2);
                assert!(Arc::ptr_eq(&initial_association, &association));
            }
            event => panic!("unexpected refreshed join event: {event:?}"),
        }
        view.clear(&CoordinatorScope::Membership);
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), events.recv())
                .await
                .unwrap(),
            Some(JoinEvent::CoordinatorLost { leader }) if leader.term == 2
        ));
        assert_eq!(initial_association.state(), AssociationState::Active);
        shutdown.send_replace(true);
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
    }
}
