use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::Arc,
    time::Duration,
};

use broadcast::error::RecvError;
use lattice_model::{
    cluster::CoordinatorScope,
    cluster::{ActorGroupId, NodeIncarnation},
    run::{ClusterLifecycle, RunPhase},
};
use lattice_remoting::association::{AssociationKey, AssociationManager};
use tokio::{
    sync::{broadcast, mpsc, watch},
    time::MissedTickBehavior,
};

use super::{
    CoordinatorRuntimeError, GroupCoordinator, GroupCoordinatorConfig, GroupCoordinatorHandle,
    cluster::{ClusterCoordinator, ClusterCoordinatorConfig},
};
use crate::{
    administration::AdministratorAllowlist,
    allocation::{
        ShardAllocationStrategy,
        registry::{ShardAllocationStrategies, StrategyRegistrationError},
    },
    candidates::CandidateRegistration,
    control::PlacementControlEvent,
    coordinator::{LeaderRecord, MemberEvent, MemberHello},
    storage::{
        ActorGroupStore, CoordinatorLeaseStore, MembershipStore, ScopedElectionStore, StorageError,
    },
    types::NodeKey,
};

#[cfg(test)]
mod candidate_tests;
mod candidates;
#[cfg(test)]
mod cluster_tests;
mod election;
mod helpers;
mod member_fanout;
mod routing;
mod shutdown;
#[cfg(test)]
mod shutdown_tests;
#[cfg(test)]
mod strategy_tests;
mod tasks;
#[cfg(test)]
mod tests;

use election::{candidate_delay, elect_group_leader, next_term};
use helpers::next_membership_event;
use tasks::OwnedTasks;

#[derive(Debug, Clone)]
pub struct CoordinatorHostConfig {
    /// Remote management is denied unless an explicit mTLS administrator policy is supplied.
    pub administrators: Option<AdministratorAllowlist>,
    pub cluster: ClusterCoordinatorConfig,
    pub group: GroupCoordinatorConfig,
    pub maximum_groups: usize,
    pub control_capacity_per_group: usize,
    pub renewal_interval: Duration,
    pub election_interval: Duration,
    pub member_reconciliation_interval: Duration,
    pub maximum_candidate_jitter: Duration,
    pub allocation_strategies: ShardAllocationStrategies,
}

impl Default for CoordinatorHostConfig {
    fn default() -> Self {
        Self {
            administrators: None,
            cluster: ClusterCoordinatorConfig::default(),
            group: GroupCoordinatorConfig::default(),
            maximum_groups: 64,
            control_capacity_per_group: 256,
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

    fn validate(&self, groups: &BTreeSet<ActorGroupId>) -> Result<(), CoordinatorRuntimeError> {
        if self.maximum_groups == 0
            || self.control_capacity_per_group == 0
            || self.renewal_interval.is_zero()
            || self.election_interval.is_zero()
            || self.member_reconciliation_interval.is_zero()
        {
            return Err(CoordinatorRuntimeError::invalid_config(
                "Coordinator host capacities and maintenance intervals must be positive",
            ));
        }
        if self.maximum_candidate_jitter >= self.cluster.leader_lease_ttl
            || self.maximum_candidate_jitter >= self.group.leader_lease_ttl
        {
            return Err(CoordinatorRuntimeError::invalid_config(format!(
                "maximum_candidate_jitter={:?} must be shorter than membership \
                 leader_lease_ttl={:?} and placement leader_lease_ttl={:?}",
                self.maximum_candidate_jitter,
                self.cluster.leader_lease_ttl,
                self.group.leader_lease_ttl,
            )));
        }
        if groups.len() > self.maximum_groups {
            return Err(CoordinatorRuntimeError::invalid_config(format!(
                "configured placement group count {} exceeds maximum_groups={}",
                groups.len(),
                self.maximum_groups,
            )));
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
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + ActorGroupStore,
{
    leader: Option<GroupCoordinator<S>>,
    sender: Option<mpsc::Sender<PlacementControlEvent>>,
    shutdown: Option<watch::Sender<bool>>,
    handle: Option<GroupCoordinatorHandle>,
    state: CoordinatorHostScopeState,
}

#[derive(Debug)]
enum HostBackgroundCompletion {
    MembershipSnapshot(AssociationKey),
    DomainReconciliation(ActorGroupId),
}

/// Supervises independent membership and actor-group candidates in one process.
///
/// A group task owns its own lease, input queue and shutdown signal. Task loss is
/// recorded for that scope and never tears down another group task or membership.
pub struct CoordinatorHost<S>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + ActorGroupStore,
{
    lifecycle_events: watch::Sender<ClusterLifecycle>,
    store: Arc<S>,
    associations: Arc<AssociationManager>,
    node: NodeKey,
    candidate_registrations: BTreeMap<CoordinatorScope, CandidateRegistration>,
    membership: Option<ClusterCoordinator<S>>,
    membership_events: Option<broadcast::Receiver<MemberEvent>>,
    membership_state: CoordinatorHostScopeState,
    groups: BTreeMap<ActorGroupId, HostedDomain<S>>,
    pending_member_hellos: BTreeMap<NodeIncarnation, MemberHello>,
    membership_associations: BTreeMap<NodeIncarnation, AssociationKey>,
    directory_events: watch::Sender<BTreeMap<CoordinatorScope, LeaderRecord>>,
    scope_events: watch::Sender<BTreeMap<CoordinatorScope, CoordinatorHostScopeState>>,
    background_tasks: OwnedTasks<HostBackgroundCompletion, ()>,
    snapshotting_associations: HashSet<AssociationKey>,
    pending_snapshot_replays: HashSet<AssociationKey>,
    reconciling_groups: BTreeSet<ActorGroupId>,
    config: CoordinatorHostConfig,
}

impl<S> CoordinatorHost<S>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + ActorGroupStore,
{
    pub async fn elect(
        store: Arc<S>,
        associations: Arc<AssociationManager>,
        node: NodeKey,
        groups: BTreeSet<ActorGroupId>,
        config: CoordinatorHostConfig,
    ) -> Result<Self, CoordinatorRuntimeError> {
        config.validate(&groups)?;
        if let Err(error) = store.ensure_framework().await {
            let lifecycle = store.lifecycle().await?;
            if error == StorageError::RunNotRunning
                && matches!(lifecycle.phase, RunPhase::Closed { .. })
            {
                let (directory_events, _) = watch::channel(BTreeMap::new());
                let (scope_events, _) = watch::channel(BTreeMap::new());
                let (lifecycle_events, _) = watch::channel(lifecycle);
                return Ok(Self {
                    store,
                    associations,
                    node,
                    candidate_registrations: BTreeMap::new(),
                    membership: None,
                    membership_events: None,
                    membership_state: CoordinatorHostScopeState::Standby,
                    groups: BTreeMap::new(),
                    pending_member_hellos: BTreeMap::new(),
                    membership_associations: BTreeMap::new(),
                    directory_events,
                    scope_events,
                    lifecycle_events,
                    background_tasks: OwnedTasks::new(),
                    snapshotting_associations: HashSet::new(),
                    pending_snapshot_replays: HashSet::new(),
                    reconciling_groups: BTreeSet::new(),
                    config,
                });
            }
            return Err(error.into());
        }

        candidate_delay(
            &CoordinatorScope::Cluster,
            &node,
            config.maximum_candidate_jitter,
        )
        .await;
        let membership_term = next_term(store.as_ref(), &CoordinatorScope::Cluster).await?;
        let membership = match ClusterCoordinator::elect(
            store.clone(),
            node.clone(),
            membership_term,
            config.cluster.clone(),
        )
        .await
        {
            Ok(leader) => Some(leader),
            Err(
                CoordinatorRuntimeError::NotLeader
                | CoordinatorRuntimeError::Storage(StorageError::CandidateNotEligible),
            ) => None,
            Err(error) => return Err(error),
        };
        let membership_state = membership
            .as_ref()
            .map_or(CoordinatorHostScopeState::Standby, |leader| {
                CoordinatorHostScopeState::Active(leader.leader().clone())
            });
        let membership_events = membership.as_ref().map(ClusterCoordinator::subscribe);

        let mut hosted = BTreeMap::new();
        for group in groups {
            let scope = CoordinatorScope::Group(group.clone());
            candidate_delay(&scope, &node, config.maximum_candidate_jitter).await;
            let term = next_term(store.as_ref(), &scope).await?;
            let leader = match elect_group_leader(
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
                Err(
                    CoordinatorRuntimeError::NotLeader
                    | CoordinatorRuntimeError::Storage(StorageError::CandidateNotEligible),
                ) => None,
                Err(error) => return Err(error),
            };
            let state = leader
                .as_ref()
                .map_or(CoordinatorHostScopeState::Standby, |leader| {
                    CoordinatorHostScopeState::Active(leader.leader().clone())
                });
            hosted.insert(
                group,
                HostedDomain {
                    handle: leader.as_ref().map(GroupCoordinator::handle),
                    leader,
                    sender: None,
                    shutdown: None,
                    state,
                },
            );
        }

        let mut directory = BTreeMap::new();
        if let CoordinatorHostScopeState::Active(record) = &membership_state {
            directory.insert(CoordinatorScope::Cluster, record.clone());
        }
        for entry in hosted.values() {
            if let CoordinatorHostScopeState::Active(record) = &entry.state {
                directory.insert(record.scope.clone(), record.clone());
            }
        }
        let (directory_events, _) = watch::channel(directory);
        let mut scope_states = BTreeMap::new();
        scope_states.insert(CoordinatorScope::Cluster, membership_state.clone());
        for (group, hosted) in &hosted {
            scope_states.insert(CoordinatorScope::Group(group.clone()), hosted.state.clone());
        }
        let (scope_events, _) = watch::channel(scope_states);
        let (lifecycle_events, _) = watch::channel(store.lifecycle().await?);
        let mut host = Self {
            lifecycle_events,
            candidate_registrations: BTreeMap::new(),
            store,
            associations,
            node,
            membership,
            membership_events,
            membership_state,
            groups: hosted,
            pending_member_hellos: BTreeMap::new(),
            membership_associations: BTreeMap::new(),
            directory_events,
            scope_events,
            background_tasks: OwnedTasks::new(),
            snapshotting_associations: HashSet::new(),
            pending_snapshot_replays: HashSet::new(),
            reconciling_groups: BTreeSet::new(),
            config,
        };
        // Existing active elections registered themselves; reuse those exact
        // identities rather than replacing a registration underneath a leader.
        for scope in std::iter::once(CoordinatorScope::Cluster)
            .chain(host.groups.keys().cloned().map(CoordinatorScope::Group))
        {
            for registration in host.store.online_candidates(&scope).await? {
                if registration.node == host.node {
                    host.candidate_registrations
                        .insert(scope.clone(), registration);
                }
            }
        }
        host.refresh_candidate_registrations().await?;
        Ok(host)
    }

    pub fn node(&self) -> &NodeKey {
        &self.node
    }

    /// Persisted lifecycle observation, including terminal results when no
    /// serving leader exists. This stream is not serving authority.
    pub fn subscribe_lifecycle(&self) -> watch::Receiver<ClusterLifecycle> {
        self.lifecycle_events.subscribe()
    }

    pub fn scope_state(&self, scope: &CoordinatorScope) -> Option<&CoordinatorHostScopeState> {
        match scope {
            CoordinatorScope::Cluster => Some(&self.membership_state),
            CoordinatorScope::Group(group) => self.groups.get(group).map(|entry| &entry.state),
        }
    }

    pub fn group_handle(&self, group: &ActorGroupId) -> Option<GroupCoordinatorHandle> {
        self.groups
            .get(group)
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

    pub fn active_group_leaders(&self) -> impl Iterator<Item = (&ActorGroupId, &LeaderRecord)> {
        self.groups
            .iter()
            .filter_map(|(group, entry)| match &entry.state {
                CoordinatorHostScopeState::Active(record) => Some((group, record)),
                CoordinatorHostScopeState::Standby | CoordinatorHostScopeState::Failed => None,
            })
    }

    pub async fn run(
        mut self,
        mut controls: mpsc::Receiver<PlacementControlEvent>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), CoordinatorRuntimeError> {
        let mut tasks = OwnedTasks::new();
        for (group, hosted) in &mut self.groups {
            let Some(leader) = hosted.leader.take() else {
                continue;
            };
            let (sender, receiver) = mpsc::channel(self.config.control_capacity_per_group);
            let (stop, stop_rx) = watch::channel(false);
            hosted.sender = Some(sender);
            hosted.shutdown = Some(stop);
            let group = group.clone();
            tasks.spawn(group, async move { leader.run(receiver, stop_rx).await });
        }

        // Membership renewal, actor-group campaigning, and full member reconciliation each own
        // an independent cadence. A slow durable store must not let campaigning starve the
        // membership lease or stall control routing for every group.
        let mut renewal = tokio::time::interval(self.config.renewal_interval);
        renewal.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut election = tokio::time::interval(self.config.election_interval);
        election.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut member_reconciliation =
            tokio::time::interval(self.config.member_reconciliation_interval);
        member_reconciliation.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut elections = OwnedTasks::new();
        let mut campaigning = BTreeSet::new();
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                _ = renewal.tick() => {
                    if let Ok(lifecycle) = self.store.lifecycle().await {
                        if lifecycle.epoch != self.lifecycle_events.borrow().epoch {
                            // A process belongs to exactly one run; starting a new run
                            // never revives a stopped coordinator or its old identities.
                            continue;
                        }
                        self.lifecycle_events.send_replace(lifecycle);
                    }
                    if matches!(self.lifecycle_events.borrow().phase,RunPhase::Closed{..}) {
                        let _ = self.drive_cluster_shutdown().await;
                        continue;
                    }
                    if let Err(error) = self.refresh_candidate_registrations().await {
                        tracing::warn!(%error, "candidate registration refresh failed");
                    }
                    self.renew_membership().await;
                    if let Err(error) = self.drive_cluster_shutdown().await {
                        tracing::warn!(%error, "cluster shutdown progress deferred");
                    }
                    if let Ok(lifecycle)=self.store.lifecycle().await {self.lifecycle_events.send_replace(lifecycle);}
                }
                _ = election.tick() => {
                    if !self.lifecycle_events.borrow().is_running() {continue;}
                    let inactive = self.groups
                        .iter()
                        .filter_map(|(group, hosted)| hosted.sender.is_none().then_some(group.clone()))
                        .collect::<Vec<_>>();
                    self.spawn_campaigns(inactive, &mut campaigning, &mut elections);
                }
                _ = member_reconciliation.tick() => {
                    if !self.lifecycle_events.borrow().is_running() {continue;}
                    self.spawn_global_member_reconciliation();
                }
                Some((group, outcome)) = elections.join_next(), if !elections.is_empty() => {
                    campaigning.remove(&group);
                    match outcome {
                        Ok(outcome) => self.install_campaign_outcome(group, outcome, &mut tasks),
                        Err(error) => {
                            if let Some(hosted) = self.groups.get_mut(&group) {
                                hosted.state = CoordinatorHostScopeState::Failed;
                            }
                            tracing::warn!(target: "lattice.cluster.placement", group = %group.as_str(), %error, "actor-group campaign task failed");
                        }
                    }
                    self.publish_directory();
                }
                Some((group, outcome)) = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(hosted) = self.groups.get_mut(&group) {
                        hosted.sender = None;
                        hosted.shutdown = None;
                        hosted.handle = None;
                        hosted.state = CoordinatorHostScopeState::Failed;
                    }
                    match outcome {
                        Ok(Ok(())) => {},
                        Ok(Err(error)) => {
                            tracing::warn!(target: "lattice.cluster.placement", group = %group.as_str(), %error, "actor-group leader task stopped");
                        },
                        Err(error) => {
                            tracing::warn!(target: "lattice.cluster.placement", group = %group.as_str(), %error, "actor-group leader task failed");
                        }
                    }
                    self.publish_directory();
                    if !*shutdown.borrow() {
                        self.spawn_campaigns([group], &mut campaigning, &mut elections);
                    }
                }
                Some((completion, result)) = self.background_tasks.join_next(),
                    if !self.background_tasks.is_empty() => {
                        if let Err(error) = result {
                            tracing::warn!(target: "lattice.cluster.placement", ?completion, %error, "Coordinator background task failed");
                        }
                        match completion {
                            HostBackgroundCompletion::MembershipSnapshot(association) => {
                                self.snapshotting_associations.remove(&association);
                                if self.pending_snapshot_replays.remove(&association) {
                                    self.spawn_membership_snapshot(association, None);
                                }
                            }
                            HostBackgroundCompletion::DomainReconciliation(group) => {
                                self.reconciling_groups.remove(&group);
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
                                self.spawn_membership_snapshot(association, None);
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
        self.background_tasks.abort_all();
        for hosted in self.groups.values() {
            if let Some(stop) = &hosted.shutdown {
                let _ = stop.send(true);
            }
        }
        while tasks.join_next().await.is_some() {}
        while elections.join_next().await.is_some() {}
        while self.background_tasks.join_next().await.is_some() {}
        if let Some(membership) = self.membership.take() {
            membership.shutdown().await?;
        }
        for registration in self.candidate_registrations.values() {
            let _ = self.store.unregister_candidate(registration).await;
            let _ = self.store.revoke_lease(registration.lease_id).await;
        }
        Ok(())
    }

    fn publish_directory(&self) {
        let mut directory = BTreeMap::new();
        if let CoordinatorHostScopeState::Active(record) = &self.membership_state {
            directory.insert(CoordinatorScope::Cluster, record.clone());
        }
        for hosted in self.groups.values() {
            if let CoordinatorHostScopeState::Active(record) = &hosted.state {
                directory.insert(record.scope.clone(), record.clone());
            }
        }
        self.directory_events.send_replace(directory);
        let mut scopes = BTreeMap::new();
        scopes.insert(CoordinatorScope::Cluster, self.membership_state.clone());
        for (group, hosted) in &self.groups {
            scopes.insert(CoordinatorScope::Group(group.clone()), hosted.state.clone());
        }
        self.scope_events.send_replace(scopes);
    }
}
