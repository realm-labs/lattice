//! Explicit candidate provisioning for existing runtime test fixtures.
#[cfg(test)]
use crate::runtime::{
    cluster::{ClusterCoordinator, ClusterCoordinatorConfig},
    host::{CoordinatorHost, CoordinatorHostConfig},
};
use crate::{
    runtime::{CoordinatorRuntimeError, GroupCoordinator, GroupCoordinatorConfig},
    storage::{ActorGroupStore, MembershipStore, ScopedElectionStore},
    types::{CoordinatorTerm, NodeKey},
};
#[cfg(test)]
use lattice_model::cluster::ActorGroupId;
use lattice_model::cluster::CoordinatorScope;
use lattice_remoting::association::AssociationManager;
#[cfg(test)]
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::{
    coordinator::LeaderRecord,
    storage::{
        StorageError,
        candidates::{CandidateStore, provision_candidate},
    },
};

pub(crate) async fn prepare_leader<S: CandidateStore + ?Sized>(
    store: &S,
    mut leader: LeaderRecord,
    lease: i64,
) -> Result<LeaderRecord, StorageError> {
    let authorization =
        provision_candidate(store, leader.scope.clone(), leader.node.node_id.clone()).await?;
    leader.candidate_generation = authorization.generation;
    leader.candidate_lease_id = lease;
    store
        .register_candidate(&leader.candidate_registration())
        .await?;
    Ok(leader)
}

pub(crate) async fn elect_group<S: ScopedElectionStore + MembershipStore + ActorGroupStore>(
    store: Arc<S>,
    associations: Arc<AssociationManager>,
    node: NodeKey,
    scope: CoordinatorScope,
    term: CoordinatorTerm,
    config: GroupCoordinatorConfig,
) -> Result<GroupCoordinator<S>, CoordinatorRuntimeError> {
    provision_candidate(store.as_ref(), scope.clone(), node.node_id.clone()).await?;
    GroupCoordinator::elect(store, associations, node, scope, term, config).await
}

#[cfg(test)]
pub(crate) async fn elect_cluster<S: ScopedElectionStore + MembershipStore>(
    store: Arc<S>,
    node: NodeKey,
    term: CoordinatorTerm,
    config: ClusterCoordinatorConfig,
) -> Result<ClusterCoordinator<S>, CoordinatorRuntimeError> {
    provision_candidate(
        store.as_ref(),
        CoordinatorScope::Cluster,
        node.node_id.clone(),
    )
    .await?;
    ClusterCoordinator::elect(store, node, term, config).await
}

#[cfg(test)]
pub(crate) async fn elect_host<S: ScopedElectionStore + MembershipStore + ActorGroupStore>(
    store: Arc<S>,
    associations: Arc<AssociationManager>,
    node: NodeKey,
    groups: BTreeSet<ActorGroupId>,
    config: CoordinatorHostConfig,
) -> Result<CoordinatorHost<S>, CoordinatorRuntimeError> {
    for scope in std::iter::once(CoordinatorScope::Cluster)
        .chain(groups.iter().cloned().map(CoordinatorScope::Group))
    {
        provision_candidate(store.as_ref(), scope, node.node_id.clone()).await?;
    }
    CoordinatorHost::elect(store, associations, node, groups, config).await
}
