use std::collections::{BTreeMap, BTreeSet};

use lattice_core::actor_ref::{EntityType, NodeIncarnation, ProtocolId};
use thiserror::Error;

use crate::types::{
    AssignmentGeneration, CoordinatorTerm, MonotonicTime, NodeKey, Revision, ShardId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadSample {
    pub boot_incarnation: NodeIncarnation,
    pub sequence: u64,
    pub observed_at: MonotonicTime,
    pub weight: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementNode {
    pub key: NodeKey,
    pub ready: bool,
    pub eligible_entity_types: BTreeSet<EntityType>,
    pub protocols: BTreeSet<ProtocolId>,
    pub capacity_units: u64,
    pub joined_at: MonotonicTime,
    pub load: Option<LoadSample>,
    pub reserved_weight: u64,
    pub draining: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacedShard {
    pub entity_type: EntityType,
    pub shard_id: ShardId,
    pub owner: NodeKey,
    pub generation: AssignmentGeneration,
    pub measured_weight: Option<u64>,
    pub assigned_at: MonotonicTime,
    pub active_move: bool,
}

impl PlacedShard {
    pub fn weight(&self) -> u64 {
        self.measured_weight.unwrap_or(1).max(1)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementView {
    pub coordinator_term: CoordinatorTerm,
    pub revision: Revision,
    pub now: MonotonicTime,
    pub reconciled: bool,
    pub degraded: bool,
    pub nodes: Vec<PlacementNode>,
    pub shards: Vec<PlacedShard>,
    pub active_cluster_moves: usize,
    pub active_entity_moves: BTreeMap<EntityType, usize>,
    pub active_source_moves: BTreeMap<NodeKey, usize>,
    pub active_target_moves: BTreeMap<NodeKey, usize>,
    pub last_automatic_move_at: Option<MonotonicTime>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocationRequest {
    pub entity_type: EntityType,
    pub shard_id: ShardId,
    pub required_protocol: ProtocolId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocationDecision {
    pub target: NodeKey,
    pub policy_id: &'static str,
    pub policy_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebalanceTrigger {
    Recovery {
        owner: NodeKey,
    },
    Drain {
        node: NodeKey,
    },
    Manual {
        source: Option<NodeKey>,
        target: Option<NodeKey>,
        bypass_improvement: bool,
    },
    Automatic,
}

impl RebalanceTrigger {
    pub fn priority(&self) -> u8 {
        match self {
            Self::Recovery { .. } => 0,
            Self::Drain { .. } => 1,
            Self::Manual { .. } => 2,
            Self::Automatic => 3,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebalanceLimits {
    pub moves_per_round: usize,
    pub concurrent_cluster: usize,
    pub concurrent_entity: usize,
    pub concurrent_source: usize,
    pub concurrent_target: usize,
}

impl RebalanceLimits {
    pub fn validate(self) -> Result<Self, AllocationError> {
        if [
            self.moves_per_round,
            self.concurrent_cluster,
            self.concurrent_entity,
            self.concurrent_source,
            self.concurrent_target,
        ]
        .contains(&0)
        {
            return Err(AllocationError::ZeroLimit);
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposedMove {
    pub entity_type: EntityType,
    pub shard_id: ShardId,
    pub expected_generation: AssignmentGeneration,
    pub source: NodeKey,
    pub target: NodeKey,
    pub estimated_weight: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebalanceProposal {
    pub policy_id: &'static str,
    pub policy_version: u32,
    pub base_revision: Revision,
    pub trigger: RebalanceTrigger,
    pub moves: Vec<ProposedMove>,
}

pub trait ShardAllocationStrategy: Send + Sync + 'static {
    fn policy_id(&self) -> &'static str;
    fn policy_version(&self) -> u32;

    fn allocate(
        &self,
        request: &AllocationRequest,
        view: &PlacementView,
    ) -> Result<AllocationDecision, AllocationError>;

    fn rebalance(
        &self,
        entity_type: &EntityType,
        required_protocol: ProtocolId,
        trigger: RebalanceTrigger,
        view: &PlacementView,
        limits: RebalanceLimits,
    ) -> Result<RebalanceProposal, AllocationError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeightedLeastLoad {
    pub max_sample_age_millis: u64,
    pub minimum_residence_millis: u64,
    pub node_join_stability_millis: u64,
    pub cooldown_millis: u64,
    pub minimum_relative_improvement_bps: u32,
    pub minimum_absolute_improvement_micros: u64,
}

impl Default for WeightedLeastLoad {
    fn default() -> Self {
        Self {
            max_sample_age_millis: 15_000,
            minimum_residence_millis: 60_000,
            node_join_stability_millis: 30_000,
            cooldown_millis: 30_000,
            minimum_relative_improvement_bps: 500,
            minimum_absolute_improvement_micros: 10_000,
        }
    }
}

impl ShardAllocationStrategy for WeightedLeastLoad {
    fn policy_id(&self) -> &'static str {
        "weighted-least-load"
    }

    fn policy_version(&self) -> u32 {
        1
    }

    fn allocate(
        &self,
        request: &AllocationRequest,
        view: &PlacementView,
    ) -> Result<AllocationDecision, AllocationError> {
        require_reconciled(view)?;
        let totals = node_weights(view);
        let target = eligible_nodes(view, &request.entity_type, request.required_protocol)
            .min_by(|left, right| compare_normalized(left, right, &totals))
            .ok_or(AllocationError::NoEligibleNode)?;
        Ok(AllocationDecision {
            target: target.key.clone(),
            policy_id: self.policy_id(),
            policy_version: self.policy_version(),
        })
    }

    fn rebalance(
        &self,
        entity_type: &EntityType,
        required_protocol: ProtocolId,
        trigger: RebalanceTrigger,
        view: &PlacementView,
        limits: RebalanceLimits,
    ) -> Result<RebalanceProposal, AllocationError> {
        require_reconciled(view)?;
        let limits = limits.validate()?;
        if view.active_cluster_moves >= limits.concurrent_cluster
            || view
                .active_entity_moves
                .get(entity_type)
                .copied()
                .unwrap_or(0)
                >= limits.concurrent_entity
        {
            return Err(AllocationError::ConcurrencyLimit);
        }
        let proposal_limit = limits
            .moves_per_round
            .min(limits.concurrent_cluster - view.active_cluster_moves)
            .min(
                limits.concurrent_entity
                    - view
                        .active_entity_moves
                        .get(entity_type)
                        .copied()
                        .unwrap_or(0),
            );
        if trigger == RebalanceTrigger::Automatic {
            self.require_automatic_inputs(view)?;
        }
        let totals = node_weights(view);
        let mut shards = view
            .shards
            .iter()
            .filter(|shard| &shard.entity_type == entity_type && !shard.active_move)
            .collect::<Vec<_>>();
        shards.sort_by_key(|shard| (shard.owner.clone(), shard.shard_id));
        let mut moves = Vec::new();
        let mut source_counts = BTreeMap::<NodeKey, usize>::new();
        let mut target_counts = BTreeMap::<NodeKey, usize>::new();
        for shard in shards {
            if moves.len() == proposal_limit {
                break;
            }
            if !trigger_selects_source(&trigger, &shard.owner) {
                continue;
            }
            if trigger == RebalanceTrigger::Automatic
                && elapsed(view.now, shard.assigned_at) < self.minimum_residence_millis
            {
                continue;
            }
            let source = view.nodes.iter().find(|node| node.key == shard.owner);
            if source.is_none() && !matches!(trigger, RebalanceTrigger::Recovery { .. }) {
                return Err(AllocationError::InvalidView);
            }
            let target = eligible_nodes(view, entity_type, required_protocol)
                .filter(|node| node.key != shard.owner)
                .filter(|node| trigger_allows_target(&trigger, &node.key))
                .filter(|node| {
                    elapsed(view.now, node.joined_at) >= self.node_join_stability_millis
                        || !matches!(trigger, RebalanceTrigger::Automatic)
                })
                .filter(|node| {
                    existing_count(view, &view.active_target_moves, &node.key)
                        + target_counts.get(&node.key).copied().unwrap_or(0)
                        < limits.concurrent_target
                })
                .min_by(|left, right| compare_normalized(left, right, &totals));
            let Some(target) = target else {
                continue;
            };
            if existing_count(view, &view.active_source_moves, &shard.owner)
                + source_counts.get(&shard.owner).copied().unwrap_or(0)
                >= limits.concurrent_source
            {
                continue;
            }
            if !bypasses_improvement(&trigger)
                && !self.improves(
                    source.ok_or(AllocationError::InvalidView)?,
                    target,
                    shard.weight(),
                    &totals,
                )
            {
                continue;
            }
            *source_counts.entry(shard.owner.clone()).or_default() += 1;
            *target_counts.entry(target.key.clone()).or_default() += 1;
            moves.push(ProposedMove {
                entity_type: entity_type.clone(),
                shard_id: shard.shard_id,
                expected_generation: shard.generation,
                source: shard.owner.clone(),
                target: target.key.clone(),
                estimated_weight: shard.weight(),
            });
        }
        Ok(RebalanceProposal {
            policy_id: self.policy_id(),
            policy_version: self.policy_version(),
            base_revision: view.revision,
            trigger,
            moves,
        })
    }
}

impl WeightedLeastLoad {
    fn require_automatic_inputs(&self, view: &PlacementView) -> Result<(), AllocationError> {
        if view.degraded {
            return Err(AllocationError::AutomaticPaused);
        }
        if view
            .last_automatic_move_at
            .is_some_and(|last| elapsed(view.now, last) < self.cooldown_millis)
        {
            return Err(AllocationError::Cooldown);
        }
        if view.nodes.iter().any(|node| {
            node.ready
                && node.load.as_ref().is_none_or(|sample| {
                    sample.boot_incarnation != node.key.incarnation
                        || sample.sequence == 0
                        || elapsed(view.now, sample.observed_at) > self.max_sample_age_millis
                })
        }) {
            return Err(AllocationError::StaleLoad);
        }
        Ok(())
    }

    fn improves(
        &self,
        source: &PlacementNode,
        target: &PlacementNode,
        weight: u64,
        totals: &BTreeMap<NodeKey, u64>,
    ) -> bool {
        let source_load = totals.get(&source.key).copied().unwrap_or(0);
        let target_load = totals.get(&target.key).copied().unwrap_or(0);
        let denominator = u128::from(source.capacity_units) * u128::from(target.capacity_units);
        let before = normalized_gap(
            source_load,
            source.capacity_units,
            target_load,
            target.capacity_units,
        );
        let after = normalized_gap(
            source_load.saturating_sub(weight),
            source.capacity_units,
            target_load.saturating_add(weight),
            target.capacity_units,
        );
        if after >= before {
            return false;
        }
        let improvement = before - after;
        improvement.saturating_mul(10_000)
            >= before.saturating_mul(u128::from(self.minimum_relative_improvement_bps))
            && improvement.saturating_mul(1_000_000)
                >= denominator.saturating_mul(u128::from(self.minimum_absolute_improvement_micros))
    }
}

fn require_reconciled(view: &PlacementView) -> Result<(), AllocationError> {
    if !view.reconciled {
        Err(AllocationError::Unreconciled)
    } else {
        Ok(())
    }
}

fn eligible_nodes<'a>(
    view: &'a PlacementView,
    entity_type: &EntityType,
    protocol: ProtocolId,
) -> impl Iterator<Item = &'a PlacementNode> {
    view.nodes.iter().filter(move |node| {
        node.ready
            && !node.draining
            && node.capacity_units > 0
            && node.eligible_entity_types.contains(entity_type)
            && node.protocols.contains(&protocol)
    })
}

fn node_weights(view: &PlacementView) -> BTreeMap<NodeKey, u64> {
    let mut totals = view
        .nodes
        .iter()
        .map(|node| {
            (
                node.key.clone(),
                node.load
                    .as_ref()
                    .map(|sample| sample.weight)
                    .unwrap_or(0)
                    .saturating_add(node.reserved_weight),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for shard in &view.shards {
        if view
            .nodes
            .iter()
            .find(|node| node.key == shard.owner)
            .is_some_and(|node| node.load.is_none())
        {
            *totals.entry(shard.owner.clone()).or_default() += shard.weight();
        }
    }
    totals
}

fn compare_normalized(
    left: &&PlacementNode,
    right: &&PlacementNode,
    totals: &BTreeMap<NodeKey, u64>,
) -> std::cmp::Ordering {
    let left_load = totals
        .get(&left.key)
        .copied()
        .unwrap_or(0)
        .saturating_mul(right.capacity_units);
    let right_load = totals
        .get(&right.key)
        .copied()
        .unwrap_or(0)
        .saturating_mul(left.capacity_units);
    left_load
        .cmp(&right_load)
        .then_with(|| left.key.cmp(&right.key))
}

fn normalized_gap(left: u64, left_capacity: u64, right: u64, right_capacity: u64) -> u128 {
    let left = u128::from(left) * u128::from(right_capacity);
    let right = u128::from(right) * u128::from(left_capacity);
    left.abs_diff(right)
}

fn elapsed(now: MonotonicTime, before: MonotonicTime) -> u64 {
    now.as_millis().saturating_sub(before.as_millis())
}

fn trigger_selects_source(trigger: &RebalanceTrigger, source: &NodeKey) -> bool {
    match trigger {
        RebalanceTrigger::Recovery { owner } => owner == source,
        RebalanceTrigger::Drain { node } => node == source,
        RebalanceTrigger::Manual {
            source: Some(expected),
            ..
        } => expected == source,
        RebalanceTrigger::Manual { source: None, .. } | RebalanceTrigger::Automatic => true,
    }
}

fn trigger_allows_target(trigger: &RebalanceTrigger, target: &NodeKey) -> bool {
    !matches!(trigger, RebalanceTrigger::Manual { target: Some(expected), .. } if expected != target)
}

fn bypasses_improvement(trigger: &RebalanceTrigger) -> bool {
    matches!(
        trigger,
        RebalanceTrigger::Recovery { .. }
            | RebalanceTrigger::Drain { .. }
            | RebalanceTrigger::Manual {
                bypass_improvement: true,
                ..
            }
    )
}

fn existing_count(
    _view: &PlacementView,
    counts: &BTreeMap<NodeKey, usize>,
    node: &NodeKey,
) -> usize {
    counts.get(node).copied().unwrap_or(0)
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AllocationError {
    #[error("placement view is not reconciled")]
    Unreconciled,
    #[error("placement view is invalid")]
    InvalidView,
    #[error("no eligible node exists")]
    NoEligibleNode,
    #[error("rebalance limit must be nonzero")]
    ZeroLimit,
    #[error("rebalance concurrency limit is exhausted")]
    ConcurrencyLimit,
    #[error("automatic rebalance is paused while Coordinator is degraded")]
    AutomaticPaused,
    #[error("automatic rebalance requires fresh boot-scoped load samples")]
    StaleLoad,
    #[error("automatic rebalance cooldown has not elapsed")]
    Cooldown,
}

#[cfg(test)]
mod tests {
    use super::*;
    use lattice_core::actor_ref::{NodeAddress, NodeIncarnation};

    fn node(id: &str, incarnation: u128) -> NodeKey {
        NodeKey {
            node_id: id.to_owned(),
            address: NodeAddress::new("127.0.0.1", 27000 + incarnation as u16).unwrap(),
            incarnation: NodeIncarnation::new(incarnation).unwrap(),
        }
    }

    fn automatic_view() -> (EntityType, ProtocolId, NodeKey, NodeKey, PlacementView) {
        let entity = EntityType::new("automatic-policy").unwrap();
        let protocol = ProtocolId::new(91).unwrap();
        let source = node("source", 1);
        let target = node("target", 2);
        let placement_node = |key: NodeKey, weight| PlacementNode {
            key: key.clone(),
            ready: true,
            eligible_entity_types: [entity.clone()].into_iter().collect(),
            protocols: [protocol].into_iter().collect(),
            capacity_units: 10,
            joined_at: MonotonicTime::from_millis(0),
            load: Some(LoadSample {
                boot_incarnation: key.incarnation,
                sequence: 1,
                observed_at: MonotonicTime::from_millis(100_000),
                weight,
            }),
            reserved_weight: 0,
            draining: false,
        };
        let view = PlacementView {
            coordinator_term: CoordinatorTerm::new(1).unwrap(),
            revision: Revision::new(1).unwrap(),
            now: MonotonicTime::from_millis(100_000),
            reconciled: true,
            degraded: false,
            nodes: vec![
                placement_node(source.clone(), 100),
                placement_node(target.clone(), 0),
            ],
            shards: vec![PlacedShard {
                entity_type: entity.clone(),
                shard_id: ShardId::new(1),
                owner: source.clone(),
                generation: AssignmentGeneration::new(1).unwrap(),
                measured_weight: Some(20),
                assigned_at: MonotonicTime::from_millis(0),
                active_move: false,
            }],
            active_cluster_moves: 0,
            active_entity_moves: BTreeMap::new(),
            active_source_moves: BTreeMap::new(),
            active_target_moves: BTreeMap::new(),
            last_automatic_move_at: None,
        };
        (entity, protocol, source, target, view)
    }

    fn limits() -> RebalanceLimits {
        RebalanceLimits {
            moves_per_round: 4,
            concurrent_cluster: 4,
            concurrent_entity: 4,
            concurrent_source: 4,
            concurrent_target: 4,
        }
    }

    #[test]
    fn automatic_policy_requires_stable_fresh_inputs_and_hysteresis() {
        let strategy = WeightedLeastLoad::default();
        let (entity, protocol, _source, _target, view) = automatic_view();
        assert_eq!(
            strategy
                .rebalance(
                    &entity,
                    protocol,
                    RebalanceTrigger::Automatic,
                    &view,
                    limits()
                )
                .unwrap()
                .moves
                .len(),
            1
        );

        let mut stale = view.clone();
        stale.nodes[1].load.as_mut().unwrap().observed_at = MonotonicTime::from_millis(1);
        assert_eq!(
            strategy
                .rebalance(
                    &entity,
                    protocol,
                    RebalanceTrigger::Automatic,
                    &stale,
                    limits()
                )
                .unwrap_err(),
            AllocationError::StaleLoad
        );

        let mut cooldown = view.clone();
        cooldown.last_automatic_move_at = Some(MonotonicTime::from_millis(99_999));
        assert_eq!(
            strategy
                .rebalance(
                    &entity,
                    protocol,
                    RebalanceTrigger::Automatic,
                    &cooldown,
                    limits()
                )
                .unwrap_err(),
            AllocationError::Cooldown
        );

        let mut recent_assignment = view.clone();
        recent_assignment.shards[0].assigned_at = MonotonicTime::from_millis(99_999);
        assert!(
            strategy
                .rebalance(
                    &entity,
                    protocol,
                    RebalanceTrigger::Automatic,
                    &recent_assignment,
                    limits(),
                )
                .unwrap()
                .moves
                .is_empty()
        );

        let mut recent_join = view.clone();
        recent_join.nodes[1].joined_at = MonotonicTime::from_millis(99_999);
        assert!(
            strategy
                .rebalance(
                    &entity,
                    protocol,
                    RebalanceTrigger::Automatic,
                    &recent_join,
                    limits()
                )
                .unwrap()
                .moves
                .is_empty()
        );

        let mut below_hysteresis = view;
        below_hysteresis.nodes[0].load.as_mut().unwrap().weight = 10;
        below_hysteresis.nodes[1].load.as_mut().unwrap().weight = 9;
        below_hysteresis.shards[0].measured_weight = Some(1);
        assert!(
            strategy
                .rebalance(
                    &entity,
                    protocol,
                    RebalanceTrigger::Automatic,
                    &below_hysteresis,
                    limits(),
                )
                .unwrap()
                .moves
                .is_empty()
        );
    }

    #[test]
    fn proposal_respects_remaining_cluster_entity_source_and_target_limits() {
        let strategy = WeightedLeastLoad::default();
        let (entity, protocol, source, target, mut view) = automatic_view();
        let mut second = view.shards[0].clone();
        second.shard_id = ShardId::new(2);
        view.shards.push(second);
        view.active_cluster_moves = 1;
        view.active_entity_moves.insert(entity.clone(), 1);
        let bounded = RebalanceLimits {
            moves_per_round: 4,
            concurrent_cluster: 2,
            concurrent_entity: 2,
            concurrent_source: 2,
            concurrent_target: 2,
        };
        assert_eq!(
            strategy
                .rebalance(
                    &entity,
                    protocol,
                    RebalanceTrigger::Automatic,
                    &view,
                    bounded
                )
                .unwrap()
                .moves
                .len(),
            1
        );

        view.active_cluster_moves = 0;
        view.active_entity_moves.clear();
        view.active_source_moves.insert(source, 1);
        view.active_target_moves.insert(target, 1);
        let one_each = RebalanceLimits {
            concurrent_source: 1,
            concurrent_target: 1,
            ..bounded
        };
        assert!(
            strategy
                .rebalance(
                    &entity,
                    protocol,
                    RebalanceTrigger::Automatic,
                    &view,
                    one_each
                )
                .unwrap()
                .moves
                .is_empty()
        );
    }
}
