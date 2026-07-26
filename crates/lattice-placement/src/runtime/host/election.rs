use std::{collections::BTreeSet, sync::Arc, time::Duration};

use lattice_core::{actor_ref::PlacementDomainId, coordinator::CoordinatorScope};
use lattice_remoting::association::AssociationManager;
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};

use crate::{
    storage::{CoordinatorLeaseStore, MembershipStore, PlacementDomainStore, ScopedElectionStore},
    types::{CoordinatorTerm, NodeKey},
};

use super::super::CoordinatorRuntimeError;
use super::{
    CoordinatorHost, CoordinatorHostConfig, CoordinatorHostScopeState, MembershipLeader,
    PlacementDomainLeader,
};

pub(super) async fn elect_domain_leader<S>(
    store: Arc<S>,
    associations: Arc<AssociationManager>,
    node: NodeKey,
    scope: CoordinatorScope,
    term: CoordinatorTerm,
    config: &CoordinatorHostConfig,
) -> Result<PlacementDomainLeader<S>, CoordinatorRuntimeError>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + PlacementDomainStore,
{
    PlacementDomainLeader::elect_with_strategies(
        store,
        associations,
        node,
        scope,
        term,
        config.placement.clone(),
        config.allocation_strategies.clone(),
    )
    .await
}

pub(super) async fn candidate_delay(scope: &CoordinatorScope, node: &NodeKey, maximum: Duration) {
    let delay = candidate_delay_duration(scope, node, maximum);
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
}

pub(super) fn candidate_delay_duration(
    scope: &CoordinatorScope,
    node: &NodeKey,
    maximum: Duration,
) -> Duration {
    let maximum_millis = u64::try_from(maximum.as_millis()).unwrap_or(u64::MAX);
    if maximum_millis == 0 {
        return Duration::ZERO;
    }
    let mut input = Vec::new();
    match scope {
        CoordinatorScope::Membership => input.extend_from_slice(b"membership"),
        CoordinatorScope::Placement(domain) => {
            input.extend_from_slice(b"placement/");
            input.extend_from_slice(domain.as_str().as_bytes());
        }
    }
    input.push(0);
    input.extend_from_slice(node.node_id.as_bytes());
    input.extend_from_slice(&node.incarnation.get().to_be_bytes());
    let digest = blake3::hash(&input);
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(&digest.as_bytes()[..8]);
    let delay = u64::from_be_bytes(prefix) % maximum_millis.saturating_add(1);
    Duration::from_millis(delay)
}

pub(super) async fn next_term<S>(
    store: &S,
    scope: &CoordinatorScope,
) -> Result<CoordinatorTerm, CoordinatorRuntimeError>
where
    S: ScopedElectionStore,
{
    let next = store
        .get_leader_term(scope)
        .await?
        .checked_add(1)
        .ok_or(CoordinatorRuntimeError::RevisionExhausted)?;
    CoordinatorTerm::new(next).map_err(|_| CoordinatorRuntimeError::RevisionExhausted)
}

/// Campaigning owns its jitter and term read so a slow durable store delays only this domain.
/// Term monotonicity still comes from the guarded campaign transaction, not from serialization.
async fn campaign_for_domain<S>(
    store: Arc<S>,
    associations: Arc<AssociationManager>,
    node: NodeKey,
    domain: PlacementDomainId,
    config: &CoordinatorHostConfig,
) -> Result<PlacementDomainLeader<S>, CoordinatorRuntimeError>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + PlacementDomainStore,
{
    let scope = CoordinatorScope::Placement(domain);
    candidate_delay(&scope, &node, config.maximum_candidate_jitter).await;
    let term = next_term(store.as_ref(), &scope).await?;
    elect_domain_leader(store, associations, node, scope, term, config).await
}

impl<S> CoordinatorHost<S>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + PlacementDomainStore,
{
    pub(super) async fn renew_membership(&mut self) {
        let mut membership_failed = false;
        if let Some(membership) = self.membership.as_mut() {
            let result = match membership.renew_leadership().await {
                Ok(()) => membership.reconcile_expired_members().await,
                Err(error) => Err(error),
            };
            if let Err(error) = result {
                self.membership = None;
                self.membership_events = None;
                self.membership_state = CoordinatorHostScopeState::Failed;
                membership_failed = true;
                tracing::warn!(target: "lattice.cluster.membership", %error, "membership leader renewal or expiration reconciliation failed");
            }
        }
        if membership_failed {
            // Stop advertising stale leadership before potentially blocking on a
            // durable-store election retry.
            self.publish_directory();
        }
        if let Err(error) = self.reenter_membership_election().await {
            tracing::warn!(
                target: "lattice.cluster.membership",
                %error,
                "membership election re-entry deferred after durable store failure"
            );
        }
        self.publish_directory();
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

    pub(super) fn spawn_campaigns(
        &self,
        domains: impl IntoIterator<Item = PlacementDomainId>,
        campaigning: &mut BTreeSet<PlacementDomainId>,
        elections: &mut JoinSet<(
            PlacementDomainId,
            Result<PlacementDomainLeader<S>, CoordinatorRuntimeError>,
        )>,
    ) {
        for domain in domains {
            if !self.domains.contains_key(&domain) || !campaigning.insert(domain.clone()) {
                continue;
            }
            let store = self.store.clone();
            let associations = self.associations.clone();
            let node = self.node.clone();
            let config = self.config.clone();
            elections.spawn(async move {
                let outcome =
                    campaign_for_domain(store, associations, node, domain.clone(), &config).await;
                (domain, outcome)
            });
        }
    }

    pub(super) fn install_campaign_outcome(
        &mut self,
        domain: PlacementDomainId,
        outcome: Result<PlacementDomainLeader<S>, CoordinatorRuntimeError>,
        tasks: &mut JoinSet<(PlacementDomainId, Result<(), CoordinatorRuntimeError>)>,
    ) {
        match outcome {
            Ok(leader) => {
                let record = leader.leader().clone();
                let handle = leader.handle();
                let (sender, receiver) = mpsc::channel(self.config.control_capacity_per_domain);
                let (stop, stop_rx) = watch::channel(false);
                let Some(hosted) = self.domains.get_mut(&domain) else {
                    return;
                };
                hosted.sender = Some(sender);
                hosted.shutdown = Some(stop);
                hosted.handle = Some(handle);
                hosted.state = CoordinatorHostScopeState::Active(record);
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
}

#[cfg(test)]
mod tests {
    use lattice_core::actor_ref::{NodeAddress, NodeIncarnation, PlacementDomainId};

    use super::*;

    #[test]
    fn candidate_preference_is_deterministic_scoped_and_bounded() {
        let local = NodeKey {
            node_id: "candidate".to_owned(),
            address: NodeAddress::new("127.0.0.1", 33009).unwrap(),
            incarnation: NodeIncarnation::new(9).unwrap(),
        };
        let maximum = Duration::from_millis(10_000);
        let membership = candidate_delay_duration(&CoordinatorScope::Membership, &local, maximum);
        let membership_again =
            candidate_delay_duration(&CoordinatorScope::Membership, &local, maximum);
        let placement = candidate_delay_duration(
            &CoordinatorScope::Placement(PlacementDomainId::new("candidate-domain").unwrap()),
            &local,
            maximum,
        );

        assert_eq!(membership, membership_again);
        assert!(membership <= maximum);
        assert!(placement <= maximum);
        assert_ne!(membership, placement);
        assert_eq!(
            candidate_delay_duration(&CoordinatorScope::Membership, &local, Duration::ZERO),
            Duration::ZERO
        );
    }
}
