use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use broadcast::error::RecvError;
use bytes::Bytes;
use lattice_core::{
    actor_ref::{NodeIncarnation, PlacementDomainId},
    coordinator::CoordinatorScope,
};
use lattice_remoting::{
    association::{AssociationKey, AssociationManager},
    control::ControlDispatchError,
};
use tokio::{
    sync::{broadcast, mpsc, oneshot, watch},
    task::JoinSet,
    time::MissedTickBehavior,
};

use super::{
    CoordinatorHandle, CoordinatorRuntimeError, PlacementDomainLeader, PlacementDomainLeaderConfig,
    membership::control_dispatch_error as coordinator_control_dispatch_error,
    membership_plane::{MembershipLeader, MembershipLeaderConfig},
};
use crate::{
    allocation::{
        ShardAllocationStrategy,
        registry::{ShardAllocationStrategies, StrategyRegistrationError},
    },
    control::{
        PlacementControlCommand, PlacementControlEvent, PlacementControlEventKind,
        encode_control_command_for_term,
    },
    coordinator::{
        LeaderRecord, MemberChange, MemberEvent, MemberHello, MemberRemovalReason, MemberStatus,
        SnapshotRecord, SnapshotVersion, build_snapshot,
    },
    storage::{CoordinatorLeaseStore, MembershipStore, PlacementDomainStore, ScopedElectionStore},
    types::{MembershipVersion, NodeKey},
};

mod election;
#[cfg(test)]
mod strategy_tests;

use election::{candidate_delay, elect_domain_leader, next_term};

#[derive(Debug, Clone)]
pub struct CoordinatorHostConfig {
    pub membership: MembershipLeaderConfig,
    pub placement: PlacementDomainLeaderConfig,
    pub maximum_domains: usize,
    pub control_capacity_per_domain: usize,
    pub renewal_interval: Duration,
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

        let mut renewal = tokio::time::interval(self.config.renewal_interval);
        renewal.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                _ = renewal.tick() => {
                    if let Some(membership) = self.membership.as_ref()
                        && let Err(error) = membership.renew_leadership().await
                    {
                        self.membership = None;
                        self.membership_events = None;
                        self.membership_state = CoordinatorHostScopeState::Failed;
                        tracing::warn!(target: "lattice.cluster.membership", %error, "membership leader lease renewal failed");
                    }
                    if let Err(error) = self.reenter_membership_election().await {
                        tracing::warn!(
                            target: "lattice.cluster.membership",
                            %error,
                            "membership election re-entry deferred after durable store failure"
                        );
                    }
                    let inactive = self.domains
                        .iter()
                        .filter_map(|(domain, hosted)| hosted.sender.is_none().then_some(domain.clone()))
                        .collect::<Vec<_>>();
                    for domain in inactive {
                        let scope = CoordinatorScope::Placement(domain.clone());
                        candidate_delay(&scope, &self.node, self.config.maximum_candidate_jitter).await;
                        let term = match next_term(self.store.as_ref(), &scope).await {
                            Ok(term) => term,
                            Err(error) => {
                                if let Some(hosted) = self.domains.get_mut(&domain) {
                                    hosted.state = CoordinatorHostScopeState::Failed;
                                }
                                tracing::warn!(
                                    target: "lattice.cluster.placement",
                                    domain = %domain.as_str(),
                                    %error,
                                    "placement-domain election re-entry deferred after durable store failure"
                                );
                                continue;
                            }
                        };
                        match elect_domain_leader(
                            self.store.clone(),
                            self.associations.clone(),
                            self.node.clone(),
                            scope,
                            term,
                            &self.config,
                        ).await {
                            Ok(leader) => {
                                let record = leader.leader().clone();
                                let handle = leader.handle();
                                let (sender, receiver) = mpsc::channel(self.config.control_capacity_per_domain);
                                let (stop, stop_rx) = watch::channel(false);
                                if let Some(hosted) = self.domains.get_mut(&domain) {
                                    hosted.sender = Some(sender);
                                    hosted.shutdown = Some(stop);
                                    hosted.handle = Some(handle);
                                    hosted.state = CoordinatorHostScopeState::Active(record);
                                }
                                tasks.spawn(async move { (domain, leader.run(receiver, stop_rx).await) });
                            }
                            Err(CoordinatorRuntimeError::NotLeader) => {
                                if let Some(hosted) = self.domains.get_mut(&domain) {
                                    hosted.state = CoordinatorHostScopeState::Standby;
                                }
                            }
                            Err(error) => {
                                if let Some(hosted) = self.domains.get_mut(&domain) {
                                    hosted.state = CoordinatorHostScopeState::Failed;
                                }
                                tracing::warn!(target: "lattice.cluster.placement", domain = %domain.as_str(), %error, "placement-domain election re-entry failed");
                            }
                        }
                    }
                    if let Err(error) = self.fanout_global_member_removals().await {
                        tracing::warn!(
                            target: "lattice.cluster.membership",
                            %error,
                            "global member reconciliation deferred after durable store failure"
                        );
                    }
                    self.publish_directory();
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
                    }
                }
                event = next_membership_event(&mut self.membership_events), if self.membership_events.is_some() => {
                    match event {
                        Ok(event) => self.broadcast_membership_event(event)?,
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

        for hosted in self.domains.values() {
            if let Some(stop) = &hosted.shutdown {
                let _ = stop.send(true);
            }
        }
        while tasks.join_next().await.is_some() {}
        if let Some(membership) = self.membership.take() {
            membership.shutdown().await?;
        }
        Ok(())
    }

    async fn route_control(&mut self, event: PlacementControlEvent) {
        match event.kind {
            PlacementControlEventKind::Command(inbound) => {
                match (inbound.coordinator_term, self.active_term(&inbound.scope)) {
                    (Some(received_term), Some(expected_term))
                        if expected_term == received_term => {}
                    (Some(_), Some(_)) | (None, _) => {
                        let _ = event
                            .completion
                            .send(Err(ControlDispatchError::InvalidCommand));
                        return;
                    }
                    (Some(_), None) => {
                        let _ = event
                            .completion
                            .send(Err(ControlDispatchError::Unavailable));
                        return;
                    }
                }
                match (&inbound.scope, &inbound.command) {
                    (CoordinatorScope::Membership, PlacementControlCommand::MemberHello(hello)) => {
                        let result = self.admit_member(hello.clone()).await;
                        if result.is_ok() {
                            self.pending_member_hellos
                                .insert(inbound.association.remote_incarnation, hello.clone());
                            self.membership_associations.insert(
                                inbound.association.remote_incarnation,
                                inbound.association.clone(),
                            );
                        }
                        let result = match result {
                            Ok(()) => self.send_membership_snapshot(&inbound.association).await,
                            Err(error) => Err(error),
                        };
                        let _ = event.completion.send(result.map_err(dispatch_error));
                    }
                    (
                        CoordinatorScope::Membership,
                        PlacementControlCommand::NodeHeartbeat {
                            incarnation,
                            sequence,
                        },
                    ) => {
                        let result = if *incarnation != inbound.association.remote_incarnation
                            || *sequence == 0
                        {
                            Err(CoordinatorRuntimeError::UnauthorizedCommand)
                        } else if let Some(hello) =
                            self.pending_member_hellos.get(incarnation).cloned()
                        {
                            self.admit_member(hello).await
                        } else {
                            Err(CoordinatorRuntimeError::UnknownSession)
                        };
                        let _ = event.completion.send(result.map_err(dispatch_error));
                    }
                    (
                        CoordinatorScope::Membership,
                        PlacementControlCommand::JoinReady { snapshot_version },
                    ) => {
                        let result = self
                            .complete_member_join(
                                inbound.association.remote_incarnation,
                                *snapshot_version,
                                &inbound.association,
                            )
                            .await;
                        let _ = event.completion.send(result.map_err(dispatch_error));
                    }
                    (
                        CoordinatorScope::Membership,
                        PlacementControlCommand::MembershipDrainComplete {
                            operation_id,
                            expected_incarnation,
                        },
                    ) => {
                        let result = self
                            .complete_membership_drain(
                                operation_id,
                                *expected_incarnation,
                                &inbound.association,
                            )
                            .await;
                        let _ = event.completion.send(result.map_err(dispatch_error));
                    }
                    (
                        CoordinatorScope::Placement(domain),
                        PlacementControlCommand::PlacementDomainHello(hello),
                    ) => {
                        let Some(sender) = self
                            .domains
                            .get(domain)
                            .and_then(|entry| entry.sender.clone())
                        else {
                            let _ = event
                                .completion
                                .send(Err(ControlDispatchError::Unavailable));
                            return;
                        };
                        let member_is_up = self
                            .store
                            .get_member(&hello.node.node_id)
                            .await
                            .ok()
                            .flatten()
                            .filter(|member| {
                                member.node == hello.node
                                    && member.status == MemberStatus::Up
                                    && hello.node.incarnation
                                        == inbound.association.remote_incarnation
                                    && hello.node.address == inbound.association.remote_address
                            })
                            .is_some();
                        if !member_is_up {
                            let _ = event
                                .completion
                                .send(Err(ControlDispatchError::InvalidCommand));
                            return;
                        }
                        if sender
                            .send(PlacementControlEvent {
                                kind: PlacementControlEventKind::Command(inbound),
                                completion: event.completion,
                            })
                            .await
                            .is_err()
                        {
                            // The original completion is dropped on a closed queue and the
                            // remoting caller observes Unavailable.
                        }
                    }
                    (CoordinatorScope::Placement(domain), _) => {
                        if let Some(sender) = self
                            .domains
                            .get(domain)
                            .and_then(|entry| entry.sender.clone())
                        {
                            let _ = sender
                                .send(PlacementControlEvent {
                                    kind: PlacementControlEventKind::Command(inbound),
                                    completion: event.completion,
                                })
                                .await;
                        } else {
                            let _ = event
                                .completion
                                .send(Err(ControlDispatchError::Unavailable));
                        }
                    }
                    _ => {
                        let _ = event
                            .completion
                            .send(Err(ControlDispatchError::InvalidCommand));
                    }
                }
            }
            PlacementControlEventKind::Reconcile { association, gap } => {
                for hosted in self.domains.values() {
                    if let Some(sender) = &hosted.sender {
                        let (completion, _) = oneshot::channel();
                        let _ = sender
                            .send(PlacementControlEvent {
                                kind: PlacementControlEventKind::Reconcile {
                                    association: association.clone(),
                                    gap,
                                },
                                completion,
                            })
                            .await;
                    }
                }
                let _ = event.completion.send(Ok(()));
            }
            PlacementControlEventKind::GlobalMemberRemoved { .. } => {
                let _ = event
                    .completion
                    .send(Err(ControlDispatchError::InvalidCommand));
            }
        }
    }

    async fn admit_member(&mut self, hello: MemberHello) -> Result<(), CoordinatorRuntimeError> {
        if let Some(membership) = self.membership.as_mut() {
            let member = membership.join(hello).await?;
            match member.status {
                MemberStatus::Joining | MemberStatus::Up => {}
                MemberStatus::Leaving => return Err(CoordinatorRuntimeError::StaleMember),
            }
            return Ok(());
        }
        let current = self
            .store
            .get_member(&hello.node.node_id)
            .await?
            .filter(|member| {
                member.node == hello.node
                    && member.hello == hello
                    && member.status == MemberStatus::Up
            })
            .ok_or(CoordinatorRuntimeError::NotLeader)?;
        self.store.keep_lease_alive(current.lease_id).await?;
        Ok(())
    }

    fn active_term(&self, scope: &CoordinatorScope) -> Option<u64> {
        let state = match scope {
            CoordinatorScope::Membership => &self.membership_state,
            CoordinatorScope::Placement(domain) => &self.domains.get(domain)?.state,
        };
        match state {
            CoordinatorHostScopeState::Active(leader) => Some(leader.term.get()),
            CoordinatorHostScopeState::Standby | CoordinatorHostScopeState::Failed => None,
        }
    }

    async fn complete_member_join(
        &mut self,
        incarnation: NodeIncarnation,
        snapshot_version: MembershipVersion,
        association: &AssociationKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        if association.remote_incarnation != incarnation
            || self.membership_associations.get(&incarnation) != Some(association)
        {
            return Err(CoordinatorRuntimeError::StaleMember);
        }
        let hello = self
            .pending_member_hellos
            .get(&incarnation)
            .cloned()
            .filter(|hello| {
                hello.node.incarnation == incarnation
                    && hello.node.address == association.remote_address
            })
            .ok_or(CoordinatorRuntimeError::StaleMember)?;
        let membership = self
            .membership
            .as_mut()
            .ok_or(CoordinatorRuntimeError::NotLeader)?;
        if !membership.version().satisfies(snapshot_version) {
            return Err(CoordinatorRuntimeError::StaleMember);
        }
        let member = self
            .store
            .get_member(&hello.node.node_id)
            .await?
            .filter(|member| member.node == hello.node && member.hello == hello)
            .ok_or(CoordinatorRuntimeError::StaleMember)?;
        match member.status {
            MemberStatus::Joining => {
                membership.mark_up(&member.node).await?;
            }
            MemberStatus::Up => {}
            MemberStatus::Leaving => return Err(CoordinatorRuntimeError::StaleMember),
        }
        Ok(())
    }

    async fn send_membership_snapshot(
        &self,
        association_key: &AssociationKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        let membership = self
            .membership
            .as_ref()
            .ok_or(CoordinatorRuntimeError::NotLeader)?;
        let records = self
            .store
            .list_members()
            .await?
            .into_iter()
            .map(|member| {
                Ok(SnapshotRecord {
                    key: format!("member/{}", member.node.node_id),
                    value: Bytes::from(
                        serde_json::to_vec(&member).map_err(|_| CoordinatorRuntimeError::Codec)?,
                    ),
                })
            })
            .collect::<Result<Vec<_>, CoordinatorRuntimeError>>()?;
        let (begin, chunks, end) = build_snapshot(
            SnapshotVersion::Membership(membership.version()),
            records,
            &self.config.placement.snapshot_limits,
        )
        .map_err(CoordinatorRuntimeError::Coordinator)?;
        let association = self
            .associations
            .get(association_key)
            .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
        for command in std::iter::once(PlacementControlCommand::SnapshotBegin(begin))
            .chain(
                chunks
                    .into_iter()
                    .map(PlacementControlCommand::SnapshotChunk),
            )
            .chain(std::iter::once(PlacementControlCommand::SnapshotEnd(end)))
        {
            let payload = encode_control_command_for_term(
                &CoordinatorScope::Membership,
                membership.version().term.get(),
                &command,
                self.config.placement.maximum_control_payload,
            )
            .map_err(CoordinatorRuntimeError::Control)?;
            association.admit_control_command(payload)?;
        }
        Ok(())
    }

    fn broadcast_membership_event(
        &mut self,
        event: MemberEvent,
    ) -> Result<(), CoordinatorRuntimeError> {
        let removed = match &event.change {
            MemberChange::Removed { node, .. } => Some(node.incarnation),
            MemberChange::Upsert(_) => None,
        };
        let coordinator_term = event.version.term.get();
        let payload = encode_control_command_for_term(
            &CoordinatorScope::Membership,
            coordinator_term,
            &PlacementControlCommand::MemberDelta(event),
            self.config.placement.maximum_control_payload,
        )
        .map_err(CoordinatorRuntimeError::Control)?;
        let mut stale = Vec::new();
        for (incarnation, key) in &self.membership_associations {
            let Some(association) = self.associations.get(key) else {
                stale.push(*incarnation);
                continue;
            };
            if association.admit_control_command(payload.clone()).is_err() {
                stale.push(*incarnation);
            }
        }
        for incarnation in stale {
            self.membership_associations.remove(&incarnation);
            self.pending_member_hellos.remove(&incarnation);
        }
        if let Some(incarnation) = removed {
            self.membership_associations.remove(&incarnation);
            self.pending_member_hellos.remove(&incarnation);
        }
        Ok(())
    }

    async fn reenter_membership_election(&mut self) -> Result<(), CoordinatorRuntimeError> {
        if self.membership.is_some() {
            return Ok(());
        }
        candidate_delay(
            &CoordinatorScope::Membership,
            &self.node,
            self.config.maximum_candidate_jitter,
        )
        .await;
        let term = next_term(self.store.as_ref(), &CoordinatorScope::Membership).await?;
        match MembershipLeader::elect(
            self.store.clone(),
            self.node.clone(),
            term,
            self.config.membership.clone(),
        )
        .await
        {
            Ok(leader) => {
                self.membership_state = CoordinatorHostScopeState::Active(leader.leader().clone());
                self.membership_events = Some(leader.subscribe());
                self.membership = Some(leader);
            }
            Err(CoordinatorRuntimeError::NotLeader) => {
                self.membership_state = CoordinatorHostScopeState::Standby;
            }
            Err(error) => {
                self.membership_state = CoordinatorHostScopeState::Failed;
                tracing::warn!(target: "lattice.cluster.membership", %error, "membership election re-entry failed");
            }
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

    async fn fanout_global_member_removals(&self) -> Result<(), CoordinatorRuntimeError> {
        for (domain, hosted) in &self.domains {
            let Some(sender) = &hosted.sender else {
                continue;
            };
            let participants = self.store.list_domain_members(domain).await?;
            for participant in participants
                .into_iter()
                .take(self.config.placement.maximum_reconciliation_work_per_pass)
            {
                let globally_up = self
                    .store
                    .get_member(&participant.node.node_id)
                    .await?
                    .is_some_and(|member| {
                        member.node == participant.node && member.status == MemberStatus::Up
                    });
                if globally_up {
                    continue;
                }
                self.remove_global_member_from_domain(
                    sender,
                    participant.node,
                    MemberRemovalReason::FailureDetected,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn fanout_global_member_removal(
        &self,
        node: NodeKey,
        reason: MemberRemovalReason,
    ) -> Result<(), CoordinatorRuntimeError> {
        for hosted in self.domains.values() {
            let Some(sender) = &hosted.sender else {
                continue;
            };
            self.remove_global_member_from_domain(sender, node.clone(), reason)
                .await?;
        }
        Ok(())
    }

    async fn remove_global_member_from_domain(
        &self,
        sender: &mpsc::Sender<PlacementControlEvent>,
        node: NodeKey,
        reason: MemberRemovalReason,
    ) -> Result<(), CoordinatorRuntimeError> {
        let (completion, completed) = oneshot::channel();
        sender
            .send(PlacementControlEvent {
                kind: PlacementControlEventKind::GlobalMemberRemoved { node, reason },
                completion,
            })
            .await
            .map_err(|_| CoordinatorRuntimeError::ControlClosed)?;
        completed
            .await
            .map_err(|_| CoordinatorRuntimeError::ControlClosed)?
            .map_err(|_| CoordinatorRuntimeError::ControlClosed)
    }

    async fn complete_membership_drain(
        &mut self,
        operation_id: &str,
        expected_incarnation: NodeIncarnation,
        association: &AssociationKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        if operation_id.is_empty()
            || operation_id.len() > 256
            || association.remote_incarnation != expected_incarnation
        {
            return Err(CoordinatorRuntimeError::StaleMember);
        }
        let hello = self
            .pending_member_hellos
            .get(&expected_incarnation)
            .cloned()
            .ok_or(CoordinatorRuntimeError::StaleMember)?;
        if self.membership_associations.get(&expected_incarnation) != Some(association) {
            return Err(CoordinatorRuntimeError::StaleMember);
        }
        let membership = self
            .membership
            .as_mut()
            .ok_or(CoordinatorRuntimeError::NotLeader)?;
        let member = self
            .store
            .get_member(&hello.node.node_id)
            .await?
            .filter(|member| {
                member.node == hello.node && member.node.incarnation == expected_incarnation
            })
            .ok_or(CoordinatorRuntimeError::StaleMember)?;
        match member.status {
            MemberStatus::Joining => return Err(CoordinatorRuntimeError::StaleMember),
            MemberStatus::Up => {
                membership.begin_leave(&member.node).await?;
            }
            MemberStatus::Leaving => {}
        }
        let removed = membership
            .remove(&member.node, MemberRemovalReason::GracefulLeave)
            .await?;
        self.fanout_global_member_removal(removed.node, MemberRemovalReason::GracefulLeave)
            .await?;
        Ok(())
    }
}

async fn next_membership_event(
    events: &mut Option<broadcast::Receiver<MemberEvent>>,
) -> Result<MemberEvent, RecvError> {
    events
        .as_mut()
        .expect("membership event branch requires a receiver")
        .recv()
        .await
}

fn dispatch_error(error: CoordinatorRuntimeError) -> ControlDispatchError {
    coordinator_control_dispatch_error(&error)
}

#[cfg(test)]
mod tests {
    use lattice_core::actor_ref::{NodeAddress, NodeIncarnation};
    use lattice_remoting::{config::RemotingConfig, control::CommandId};

    use super::*;
    use crate::{
        control::{DEFAULT_MAX_CONTROL_PAYLOAD, InboundPlacementControl, PlacementControlRouter},
        storage::InMemoryPlacementStore,
    };

    fn node(id: &str, incarnation: u128, port: u16) -> NodeKey {
        NodeKey {
            node_id: id.to_owned(),
            address: NodeAddress::new("127.0.0.1", port).unwrap(),
            incarnation: NodeIncarnation::new(incarnation).unwrap(),
        }
    }

    fn associations(node: &NodeKey) -> Arc<AssociationManager> {
        Arc::new(
            AssociationManager::new(
                node.address.clone(),
                node.incarnation,
                RemotingConfig::default(),
            )
            .unwrap(),
        )
    }

    fn config() -> CoordinatorHostConfig {
        CoordinatorHostConfig {
            membership: MembershipLeaderConfig {
                leader_lease_ttl: Duration::from_millis(500),
                member_lease_ttl: Duration::from_millis(500),
                renewal_interval: Duration::from_millis(50),
                ..MembershipLeaderConfig::default()
            },
            placement: PlacementDomainLeaderConfig {
                leader_lease_ttl: Duration::from_millis(500),
                member_lease_ttl: Duration::from_millis(500),
                claim_ttl: Duration::from_millis(500),
                renewal_interval: Duration::from_millis(50),
                ..PlacementDomainLeaderConfig::default()
            },
            renewal_interval: Duration::from_millis(50),
            ..CoordinatorHostConfig::default()
        }
    }

    #[test]
    fn unknown_membership_session_is_acknowledged_as_stale_control() {
        assert_eq!(
            dispatch_error(CoordinatorRuntimeError::UnknownSession),
            ControlDispatchError::InvalidCommand
        );
    }

    #[tokio::test]
    async fn stale_coordinator_term_is_fenced_before_membership_dispatch() {
        let store = Arc::new(InMemoryPlacementStore::new(32, 32).unwrap());
        let local = node("membership-host", 20, 33020);
        let remote = node("member", 21, 33021);
        let manager = associations(&local);
        let mut host =
            CoordinatorHost::elect(store, manager, local.clone(), BTreeSet::new(), config())
                .await
                .unwrap();
        let active_term = host
            .active_term(&CoordinatorScope::Membership)
            .expect("membership leader is active");
        let (completion, result) = oneshot::channel();
        host.route_control(PlacementControlEvent {
            kind: PlacementControlEventKind::Command(Box::new(InboundPlacementControl {
                association: AssociationKey {
                    cluster_id: lattice_core::actor_ref::ClusterId::new("term-fencing").unwrap(),
                    local_incarnation: local.incarnation,
                    remote_address: remote.address,
                    remote_incarnation: remote.incarnation,
                },
                command_id: CommandId::generate(),
                scope: CoordinatorScope::Membership,
                coordinator_term: Some(active_term.saturating_add(1)),
                command: PlacementControlCommand::NodeHeartbeat {
                    incarnation: remote.incarnation,
                    sequence: 1,
                },
            })),
            completion,
        })
        .await;
        assert_eq!(
            result.await.unwrap(),
            Err(ControlDispatchError::InvalidCommand)
        );
    }

    #[tokio::test]
    async fn dedicated_membership_host_needs_no_placement_domains() {
        let store = Arc::new(InMemoryPlacementStore::new(32, 32).unwrap());
        let local = node("membership-host", 10, 33010);
        let host = CoordinatorHost::elect(
            store.clone(),
            associations(&local),
            local.clone(),
            BTreeSet::new(),
            config(),
        )
        .await
        .unwrap();

        assert!(host.domains.is_empty());
        assert!(matches!(
            host.scope_state(&CoordinatorScope::Membership),
            Some(CoordinatorHostScopeState::Active(_))
        ));
        assert_eq!(
            store
                .get_leader(&CoordinatorScope::Membership)
                .await
                .unwrap()
                .unwrap()
                .node,
            local
        );
    }

    #[tokio::test]
    async fn competing_hosts_produce_exactly_one_active_leader_per_domain() {
        let store = Arc::new(InMemoryPlacementStore::new(32, 32).unwrap());
        let domain = PlacementDomainId::new("single-leader-domain").unwrap();
        let first = node("first-candidate", 11, 33011);
        let second = node("second-candidate", 12, 33012);
        let first_host = CoordinatorHost::elect(
            store.clone(),
            associations(&first),
            first.clone(),
            BTreeSet::from([domain.clone()]),
            config(),
        )
        .await
        .unwrap();
        let second_host = CoordinatorHost::elect(
            store.clone(),
            associations(&second),
            second,
            BTreeSet::from([domain.clone()]),
            config(),
        )
        .await
        .unwrap();

        assert!(matches!(
            first_host.scope_state(&CoordinatorScope::Placement(domain.clone())),
            Some(CoordinatorHostScopeState::Active(_))
        ));
        assert!(matches!(
            second_host.scope_state(&CoordinatorScope::Placement(domain.clone())),
            Some(CoordinatorHostScopeState::Standby)
        ));
        assert_eq!(
            store
                .get_leader(&CoordinatorScope::Placement(domain))
                .await
                .unwrap()
                .unwrap()
                .node,
            first
        );
    }

    #[tokio::test]
    async fn different_hosts_can_lead_different_domains_concurrently() {
        let store = Arc::new(InMemoryPlacementStore::new(32, 32).unwrap());
        let host_a_node = node("host-a", 1, 33001);
        let host_b_node = node("host-b", 2, 33002);
        let domain_a = PlacementDomainId::new("domain-a").unwrap();
        let domain_b = PlacementDomainId::new("domain-b").unwrap();
        let host_a = CoordinatorHost::elect(
            store.clone(),
            associations(&host_a_node),
            host_a_node.clone(),
            BTreeSet::from([domain_a.clone()]),
            config(),
        )
        .await
        .unwrap();
        let host_b = CoordinatorHost::elect(
            store.clone(),
            associations(&host_b_node),
            host_b_node.clone(),
            BTreeSet::from([domain_b.clone()]),
            config(),
        )
        .await
        .unwrap();

        assert!(matches!(
            host_a.scope_state(&CoordinatorScope::Membership),
            Some(CoordinatorHostScopeState::Active(_))
        ));
        assert!(matches!(
            host_b.scope_state(&CoordinatorScope::Membership),
            Some(CoordinatorHostScopeState::Standby)
        ));
        assert_eq!(
            store
                .get_leader(&CoordinatorScope::Placement(domain_a))
                .await
                .unwrap()
                .unwrap()
                .node,
            host_a_node
        );
        assert_eq!(
            store
                .get_leader(&CoordinatorScope::Placement(domain_b))
                .await
                .unwrap()
                .unwrap()
                .node,
            host_b_node
        );
    }

    #[tokio::test]
    async fn losing_one_domain_lease_reenters_only_that_election() {
        let store = Arc::new(InMemoryPlacementStore::new(32, 32).unwrap());
        let local = node("host", 3, 33103);
        let domain_a = PlacementDomainId::new("isolated-a").unwrap();
        let domain_b = PlacementDomainId::new("isolated-b").unwrap();
        let host = CoordinatorHost::elect(
            store.clone(),
            associations(&local),
            local,
            BTreeSet::from([domain_a.clone(), domain_b.clone()]),
            config(),
        )
        .await
        .unwrap();
        let lost_lease = host.domains[&domain_a]
            .leader
            .as_ref()
            .unwrap()
            .leader_lease_id;
        let original_a = store
            .get_leader(&CoordinatorScope::Placement(domain_a.clone()))
            .await
            .unwrap()
            .unwrap();
        let original_b = store
            .get_leader(&CoordinatorScope::Placement(domain_b.clone()))
            .await
            .unwrap()
            .unwrap();
        let (_router, controls) =
            PlacementControlRouter::bounded(32, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
        let (stop, stop_rx) = watch::channel(false);
        let task = tokio::spawn(host.run(controls, stop_rx));
        store.revoke_lease(lost_lease).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if store
                    .get_leader(&CoordinatorScope::Placement(domain_a.clone()))
                    .await
                    .unwrap()
                    .is_some_and(|leader| leader.term > original_a.term)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();

        assert!(
            store
                .get_leader(&CoordinatorScope::Placement(domain_a))
                .await
                .unwrap()
                .is_some_and(|leader| leader.term > original_a.term)
        );
        assert_eq!(
            store
                .get_leader(&CoordinatorScope::Placement(domain_b))
                .await
                .unwrap()
                .unwrap(),
            original_b
        );
        assert!(
            store
                .get_leader(&CoordinatorScope::Membership)
                .await
                .unwrap()
                .is_some()
        );
        let _ = stop.send(true);
        task.await.unwrap().unwrap();
    }
}
