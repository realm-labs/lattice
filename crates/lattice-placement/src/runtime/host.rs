use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use broadcast::error::RecvError;
use lattice_core::{
    actor_ref::{NodeIncarnation, PlacementDomainId},
    coordinator::CoordinatorScope,
};
use lattice_remoting::association::{AssociationKey, AssociationManager};
use tokio::{
    sync::{broadcast, mpsc, watch},
    task::JoinSet,
    time::MissedTickBehavior,
};

use super::{
    CoordinatorHandle, CoordinatorRuntimeError, PlacementDomainLeader, PlacementDomainLeaderConfig,
    membership_plane::{MembershipLeader, MembershipLeaderConfig},
};
use crate::{
    allocation::{
        ShardAllocationStrategy,
        registry::{ShardAllocationStrategies, StrategyRegistrationError},
    },
    control::PlacementControlEvent,
    coordinator::{LeaderRecord, MemberEvent, MemberHello},
    storage::{CoordinatorLeaseStore, MembershipStore, PlacementDomainStore, ScopedElectionStore},
    types::NodeKey,
};

#[cfg(test)]
mod cluster_tests;
mod election;
mod helpers;
mod member_fanout;
mod routing;
#[cfg(test)]
mod strategy_tests;
#[cfg(test)]
mod tests;

use election::{candidate_delay, elect_domain_leader, next_term};
use helpers::next_membership_event;

#[derive(Debug, Clone)]
pub struct CoordinatorHostConfig {
    pub membership: MembershipLeaderConfig,
    pub placement: PlacementDomainLeaderConfig,
    pub maximum_domains: usize,
    pub control_capacity_per_domain: usize,
    pub renewal_interval: Duration,
    pub election_interval: Duration,
    pub member_reconciliation_interval: Duration,
    pub maximum_candidate_jitter: Duration,
    pub allocation_strategies: ShardAllocationStrategies,
}

impl Default for CoordinatorHostConfig {
    fn default() -> Self {
        Self {
            membership: MembershipLeaderConfig::default(),
            placement: PlacementDomainLeaderConfig::default(),
            maximum_domains: 64,
            control_capacity_per_domain: 256,
            renewal_interval: Duration::from_secs(5),
            election_interval: Duration::from_secs(5),
            member_reconciliation_interval: Duration::from_secs(60),
            maximum_candidate_jitter: Duration::from_millis(25),
            allocation_strategies: ShardAllocationStrategies::default(),
        }
    }
}

impl CoordinatorHostConfig {
    pub fn with_allocation_strategy(
        mut self,
        strategy: Arc<dyn ShardAllocationStrategy>,
    ) -> Result<Self, StrategyRegistrationError> {
        self.allocation_strategies.register(strategy)?;
        Ok(self)
    }

    pub fn with_replaced_allocation_strategy(
        mut self,
        strategy: Arc<dyn ShardAllocationStrategy>,
    ) -> Result<Self, StrategyRegistrationError> {
        self.allocation_strategies.replace(strategy)?;
        Ok(self)
    }

    fn validate(
        &self,
        domains: &BTreeSet<PlacementDomainId>,
    ) -> Result<(), CoordinatorRuntimeError> {
        if self.maximum_domains == 0
            || self.control_capacity_per_domain == 0
            || self.renewal_interval.is_zero()
            || self.election_interval.is_zero()
            || self.member_reconciliation_interval.is_zero()
            || self.maximum_candidate_jitter >= self.membership.leader_lease_ttl
            || self.maximum_candidate_jitter >= self.placement.leader_lease_ttl
            || domains.len() > self.maximum_domains
        {
            return Err(CoordinatorRuntimeError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatorHostScopeState {
    Active(LeaderRecord),
    Standby,
    Failed,
}

struct HostedDomain<S>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + PlacementDomainStore,
{
    leader: Option<PlacementDomainLeader<S>>,
    sender: Option<mpsc::Sender<PlacementControlEvent>>,
    shutdown: Option<watch::Sender<bool>>,
    handle: Option<CoordinatorHandle>,
    state: CoordinatorHostScopeState,
}

/// Supervises independent membership and placement-domain candidates in one process.
///
/// A domain task owns its own lease, input queue and shutdown signal. Task loss is
/// recorded for that scope and never tears down another domain task or membership.
pub struct CoordinatorHost<S>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + PlacementDomainStore,
{
    store: Arc<S>,
    associations: Arc<AssociationManager>,
    node: NodeKey,
    membership: Option<MembershipLeader<S>>,
    membership_events: Option<broadcast::Receiver<MemberEvent>>,
    membership_state: CoordinatorHostScopeState,
    domains: BTreeMap<PlacementDomainId, HostedDomain<S>>,
    pending_member_hellos: BTreeMap<NodeIncarnation, MemberHello>,
    membership_associations: BTreeMap<NodeIncarnation, AssociationKey>,
    directory_events: watch::Sender<BTreeMap<CoordinatorScope, LeaderRecord>>,
    scope_events: watch::Sender<BTreeMap<CoordinatorScope, CoordinatorHostScopeState>>,
    config: CoordinatorHostConfig,
}

impl<S> CoordinatorHost<S>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + PlacementDomainStore,
{
    pub async fn elect(
        store: Arc<S>,
        associations: Arc<AssociationManager>,
        node: NodeKey,
        domains: BTreeSet<PlacementDomainId>,
        config: CoordinatorHostConfig,
    ) -> Result<Self, CoordinatorRuntimeError> {
        config.validate(&domains)?;
        store.ensure_schema_generation().await?;

        candidate_delay(
            &CoordinatorScope::Membership,
            &node,
            config.maximum_candidate_jitter,
        )
        .await;
        let membership_term = next_term(store.as_ref(), &CoordinatorScope::Membership).await?;
        let membership = match MembershipLeader::elect(
            store.clone(),
            node.clone(),
            membership_term,
            config.membership.clone(),
        )
        .await
        {
            Ok(leader) => Some(leader),
            Err(CoordinatorRuntimeError::NotLeader) => None,
            Err(error) => return Err(error),
        };
        let membership_state = membership
            .as_ref()
            .map_or(CoordinatorHostScopeState::Standby, |leader| {
                CoordinatorHostScopeState::Active(leader.leader().clone())
            });
        let membership_events = membership.as_ref().map(MembershipLeader::subscribe);

        let mut hosted = BTreeMap::new();
        for domain in domains {
            let scope = CoordinatorScope::Placement(domain.clone());
            candidate_delay(&scope, &node, config.maximum_candidate_jitter).await;
            let term = next_term(store.as_ref(), &scope).await?;
            let leader = match elect_domain_leader(
                store.clone(),
                associations.clone(),
                node.clone(),
                scope,
                term,
                &config,
            )
            .await
            {
                Ok(leader) => Some(leader),
                Err(CoordinatorRuntimeError::NotLeader) => None,
                Err(error) => return Err(error),
            };
            let state = leader
                .as_ref()
                .map_or(CoordinatorHostScopeState::Standby, |leader| {
                    CoordinatorHostScopeState::Active(leader.leader().clone())
                });
            hosted.insert(
                domain,
                HostedDomain {
                    handle: leader.as_ref().map(PlacementDomainLeader::handle),
                    leader,
                    sender: None,
                    shutdown: None,
                    state,
                },
            );
        }

        let mut directory = BTreeMap::new();
        if let CoordinatorHostScopeState::Active(record) = &membership_state {
            directory.insert(CoordinatorScope::Membership, record.clone());
        }
        for entry in hosted.values() {
            if let CoordinatorHostScopeState::Active(record) = &entry.state {
                directory.insert(record.scope.clone(), record.clone());
            }
        }
        let (directory_events, _) = watch::channel(directory);
        let mut scope_states = BTreeMap::new();
        scope_states.insert(CoordinatorScope::Membership, membership_state.clone());
        for (domain, hosted) in &hosted {
            scope_states.insert(
                CoordinatorScope::Placement(domain.clone()),
                hosted.state.clone(),
            );
        }
        let (scope_events, _) = watch::channel(scope_states);
        Ok(Self {
            store,
            associations,
            node,
            membership,
            membership_events,
            membership_state,
            domains: hosted,
            pending_member_hellos: BTreeMap::new(),
            membership_associations: BTreeMap::new(),
            directory_events,
            scope_events,
            config,
        })
    }

    pub fn node(&self) -> &NodeKey {
        &self.node
    }

    pub fn scope_state(&self, scope: &CoordinatorScope) -> Option<&CoordinatorHostScopeState> {
        match scope {
            CoordinatorScope::Membership => Some(&self.membership_state),
            CoordinatorScope::Placement(domain) => {
                self.domains.get(domain).map(|entry| &entry.state)
            }
        }
    }

    pub fn domain_handle(&self, domain: &PlacementDomainId) -> Option<CoordinatorHandle> {
        self.domains
            .get(domain)
            .and_then(|entry| entry.handle.clone())
    }

    pub fn subscribe_directory(&self) -> watch::Receiver<BTreeMap<CoordinatorScope, LeaderRecord>> {
        self.directory_events.subscribe()
    }

    pub fn subscribe_scope_states(
        &self,
    ) -> watch::Receiver<BTreeMap<CoordinatorScope, CoordinatorHostScopeState>> {
        self.scope_events.subscribe()
    }

    pub fn active_domain_leaders(
        &self,
    ) -> impl Iterator<Item = (&PlacementDomainId, &LeaderRecord)> {
        self.domains
            .iter()
            .filter_map(|(domain, entry)| match &entry.state {
                CoordinatorHostScopeState::Active(record) => Some((domain, record)),
                CoordinatorHostScopeState::Standby | CoordinatorHostScopeState::Failed => None,
            })
    }

    pub async fn run(
        mut self,
        mut controls: mpsc::Receiver<PlacementControlEvent>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), CoordinatorRuntimeError> {
        let mut tasks = JoinSet::new();
        for (domain, hosted) in &mut self.domains {
            let Some(leader) = hosted.leader.take() else {
                continue;
            };
            let (sender, receiver) = mpsc::channel(self.config.control_capacity_per_domain);
            let (stop, stop_rx) = watch::channel(false);
            hosted.sender = Some(sender);
            hosted.shutdown = Some(stop);
            let domain = domain.clone();
            tasks.spawn(async move { (domain, leader.run(receiver, stop_rx).await) });
        }

        // Membership renewal, placement-domain campaigning, and full member reconciliation each own
        // an independent cadence. A slow durable store must not let campaigning starve the
        // membership lease or stall control routing for every domain.
        let mut renewal = tokio::time::interval(self.config.renewal_interval);
        renewal.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut election = tokio::time::interval(self.config.election_interval);
        election.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut member_reconciliation =
            tokio::time::interval(self.config.member_reconciliation_interval);
        member_reconciliation.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut elections = JoinSet::new();
        let mut campaigning = BTreeSet::new();
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                _ = renewal.tick() => {
                    self.renew_membership().await;
                }
                _ = election.tick() => {
                    let inactive = self.domains
                        .iter()
                        .filter_map(|(domain, hosted)| hosted.sender.is_none().then_some(domain.clone()))
                        .collect::<Vec<_>>();
                    self.spawn_campaigns(inactive, &mut campaigning, &mut elections);
                }
                _ = member_reconciliation.tick() => {
                    if let Err(error) = self.fanout_global_member_removals().await {
                        tracing::warn!(
                            target: "lattice.cluster.membership",
                            %error,
                            "global member reconciliation deferred after durable store failure"
                        );
                    }
                }
                Some(result) = elections.join_next(), if !elections.is_empty() => {
                    if let Ok((domain, outcome)) = result {
                        campaigning.remove(&domain);
                        self.install_campaign_outcome(domain, outcome, &mut tasks);
                        self.publish_directory();
                    }
                }
                Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                    if let Ok((domain, result)) = result {
                        if let Some(hosted) = self.domains.get_mut(&domain) {
                            hosted.sender = None;
                            hosted.shutdown = None;
                            hosted.state = CoordinatorHostScopeState::Failed;
                        }
                        if let Err(error) = result {
                            tracing::warn!(target: "lattice.cluster.placement", domain = %domain.as_str(), %error, "placement-domain leader task stopped");
                        }
                        self.publish_directory();
                        if !*shutdown.borrow() {
                            self.spawn_campaigns([domain], &mut campaigning, &mut elections);
                        }
                    }
                }
                event = next_membership_event(&mut self.membership_events), if self.membership_events.is_some() => {
                    match event {
                        Ok(event) => self.apply_membership_event(event).await?,
                        Err(RecvError::Lagged(_)) => {
                            let associations = self
                                .membership_associations
                                .values()
                                .cloned()
                                .collect::<Vec<_>>();
                            for association in associations {
                                self.send_membership_snapshot(&association).await?;
                            }
                        }
                        Err(RecvError::Closed) => {
                            self.membership_events = None;
                        }
                    }
                }
                event = controls.recv() => {
                    let Some(event) = event else { break; };
                    self.route_control(event).await;
                }
            }
        }

        elections.abort_all();
        for hosted in self.domains.values() {
            if let Some(stop) = &hosted.shutdown {
                let _ = stop.send(true);
            }
        }
        while tasks.join_next().await.is_some() {}
        while elections.join_next().await.is_some() {}
        if let Some(membership) = self.membership.take() {
            membership.shutdown().await?;
        }
        Ok(())
    }

    fn publish_directory(&self) {
        let mut directory = BTreeMap::new();
        if let CoordinatorHostScopeState::Active(record) = &self.membership_state {
            directory.insert(CoordinatorScope::Membership, record.clone());
        }
        for hosted in self.domains.values() {
            if let CoordinatorHostScopeState::Active(record) = &hosted.state {
                directory.insert(record.scope.clone(), record.clone());
            }
        }
        self.directory_events.send_replace(directory);
        let mut scopes = BTreeMap::new();
        scopes.insert(CoordinatorScope::Membership, self.membership_state.clone());
        for (domain, hosted) in &self.domains {
            scopes.insert(
                CoordinatorScope::Placement(domain.clone()),
                hosted.state.clone(),
            );
        }
        self.scope_events.send_replace(scopes);
    }
}
