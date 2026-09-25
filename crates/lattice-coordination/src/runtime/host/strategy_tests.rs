use crate::candidate_fixture::elect_host;
use std::{collections::BTreeSet, sync::Arc, time::Duration};

use lattice_model::actor::ProtocolId;

use lattice_model::cluster::{ActorGroupId, EntityType, NodeEndpoint, NodeIncarnation};
use lattice_remoting::{association::AssociationManager, config::RemotingConfig};

use super::CoordinatorHostConfig;
use crate::{
    allocation::{
        AllocationDecision, AllocationError, AllocationRequest, PlacementView, RebalanceLimits,
        RebalanceProposal, RebalanceTrigger, ShardAllocationStrategy,
    },
    runtime::{GroupCoordinatorConfig, cluster::ClusterCoordinatorConfig},
    storage::InMemoryCoordinationStore,
    types::NodeKey,
};

struct TestAllocationStrategy;

impl ShardAllocationStrategy for TestAllocationStrategy {
    fn policy_id(&self) -> &'static str {
        "test-affinity"
    }

    fn policy_version(&self) -> u32 {
        1
    }

    fn allocate(
        &self,
        _request: &AllocationRequest,
        _view: &PlacementView,
    ) -> Result<AllocationDecision, AllocationError> {
        Err(AllocationError::NoEligibleNode)
    }

    fn rebalance(
        &self,
        _entity_type: &EntityType,
        _required_protocol: ProtocolId,
        _trigger: RebalanceTrigger,
        _view: &PlacementView,
        _limits: RebalanceLimits,
    ) -> Result<RebalanceProposal, AllocationError> {
        Err(AllocationError::NoEligibleNode)
    }
}

fn node() -> NodeKey {
    NodeKey {
        node_id: "strategy-host".to_owned(),
        address: NodeEndpoint::new("127.0.0.1", 33011).unwrap(),
        incarnation: NodeIncarnation::new(11).unwrap(),
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
        cluster: ClusterCoordinatorConfig {
            leader_lease_ttl: Duration::from_millis(500),
            member_lease_ttl: Duration::from_millis(500),
            renewal_interval: Duration::from_millis(50),
            ..ClusterCoordinatorConfig::default()
        },
        group: GroupCoordinatorConfig {
            leader_lease_ttl: Duration::from_millis(500),
            member_lease_ttl: Duration::from_millis(500),
            claim_ttl: Duration::from_millis(500),
            renewal_interval: Duration::from_millis(50),
            ..GroupCoordinatorConfig::default()
        },
        renewal_interval: Duration::from_millis(50),
        ..CoordinatorHostConfig::default()
    }
}

#[tokio::test]
async fn host_installs_custom_strategies_into_elected_group_leaders() {
    let store = Arc::new(InMemoryCoordinationStore::new(32, 32).unwrap());
    let local = node();
    let group = ActorGroupId::new("minecraft").unwrap();
    let config = config()
        .with_allocation_strategy(Arc::new(TestAllocationStrategy))
        .unwrap();
    let host = elect_host(
        store,
        associations(&local),
        local,
        BTreeSet::from([group.clone()]),
        config,
    )
    .await
    .unwrap();

    let leader = host
        .groups
        .get(&group)
        .and_then(|hosted| hosted.leader.as_ref())
        .unwrap();
    assert!(
        leader
            .strategies
            .contains_key(&("test-affinity".to_owned(), 1))
    );
}
