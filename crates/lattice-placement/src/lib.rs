#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

pub mod allocation;
pub mod authority;
pub mod control;
pub mod coordinator;
pub mod handoff;
pub mod plan;
pub mod region;
pub mod runtime;
pub mod session;
pub mod singleton;
pub mod storage;
pub mod types;

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::time::Duration;

    use lattice_core::actor_ref::{
        ClusterId, ConfigFingerprint, EntityId, EntityType, NodeAddress, NodeIncarnation,
        ProtocolId,
    };

    use crate::allocation::*;
    use crate::authority::*;
    use crate::region::*;
    use crate::types::*;

    fn node(name: &str, incarnation: u128, port: u16) -> NodeKey {
        NodeKey {
            node_id: name.to_owned(),
            address: NodeAddress::new("127.0.0.1", port).unwrap(),
            incarnation: NodeIncarnation::new(incarnation).unwrap(),
        }
    }

    fn running_slot(owner: NodeKey) -> PlacementSlot {
        PlacementSlot {
            key: PlacementSlotKey::Shard {
                entity_type: EntityType::new("world").unwrap(),
                shard_id: ShardId::new(3),
            },
            config_fingerprint: ConfigFingerprint::new([1; 32]),
            owner: Some(owner),
            target: None,
            assignment_generation: AssignmentGeneration::new(2).unwrap(),
            version: StateVersion::new(CoordinatorTerm::new(4).unwrap(), Revision::new(9).unwrap()),
            state: PlacementSlotState::Running,
            active_move: None,
            barrier_sessions: Default::default(),
        }
    }

    #[test]
    fn external_claim_loss_fences_even_after_stop_failed() {
        let local = node("a", 1, 1001);
        let slot = running_slot(local.clone());
        let mut authority = PlacementAuthority::new(local.clone(), Duration::from_secs(2)).unwrap();
        authority
            .transition(AuthorityEvent::ReconcileSlot(slot.clone()))
            .unwrap();
        authority
            .transition(AuthorityEvent::InstallGrant {
                grant: ClaimGrant {
                    slot: slot.key.clone(),
                    owner: local,
                    coordinator_term: slot.version.term,
                    assignment_generation: slot.assignment_generation,
                    grant_sequence: GrantSequence::new(1).unwrap(),
                    ttl: Duration::from_secs(15),
                },
                now: MonotonicTime::from_millis(100),
            })
            .unwrap();
        assert!(authority.admission_open());
        authority.transition(AuthorityEvent::StopFailed).unwrap();
        let effects = authority
            .transition(AuthorityEvent::ExternalClaimLost)
            .unwrap();
        assert!(!authority.admission_open());
        assert_eq!(effects[0], AuthorityEffect::FenceAdmission);
        assert!(effects.contains(&AuthorityEffect::StateLossPossible));
    }

    #[test]
    fn xxh3_v1_entity_mapping_has_a_fixed_golden_vector() {
        let config = EntityConfig::new(
            EntityType::new("world").unwrap(),
            ProtocolId::new(7).unwrap(),
            128,
            "weighted-least-load",
            1,
            Vec::new(),
        )
        .unwrap();
        let entity = EntityId::new(b"player-42".to_vec()).unwrap();
        assert_eq!(config.shard_for(&entity), ShardId::new(17));
        let reference: lattice_core::actor_ref::EntityRef = config
            .entity_ref(ClusterId::new("test").unwrap(), entity)
            .unwrap();
        assert_eq!(reference.config_fingerprint(), config.fingerprint());
    }

    #[test]
    fn weighted_allocation_is_capacity_normalized_and_deterministic() {
        let entity_type = EntityType::new("world").unwrap();
        let protocol = ProtocolId::new(7).unwrap();
        let a = node("a", 1, 1001);
        let b = node("b", 2, 1002);
        let placement_node = |key: NodeKey, capacity, weight| PlacementNode {
            key: key.clone(),
            ready: true,
            eligible_entity_types: BTreeSet::from([entity_type.clone()]),
            protocols: BTreeSet::from([protocol]),
            capacity_units: capacity,
            joined_at: MonotonicTime::from_millis(0),
            load: Some(LoadSample {
                boot_incarnation: key.incarnation,
                sequence: 1,
                observed_at: MonotonicTime::from_millis(99_000),
                weight,
            }),
            reserved_weight: 0,
            draining: false,
        };
        let view = PlacementView {
            version: StateVersion::new(CoordinatorTerm::new(1).unwrap(), Revision::new(1).unwrap()),
            now: MonotonicTime::from_millis(100_000),
            reconciled: true,
            degraded: false,
            nodes: vec![placement_node(a, 1, 10), placement_node(b.clone(), 4, 20)],
            shards: Vec::<PlacedShard>::new(),
            active_cluster_moves: 0,
            active_entity_moves: BTreeMap::new(),
            active_source_moves: BTreeMap::new(),
            active_target_moves: BTreeMap::new(),
            last_automatic_move_at: None,
        };
        let request = AllocationRequest {
            entity_type,
            shard_id: ShardId::new(1),
            required_protocol: protocol,
        };
        let strategy = WeightedLeastLoad::default();
        assert_eq!(strategy.allocate(&request, &view).unwrap().target, b);
        assert_eq!(strategy.allocate(&request, &view).unwrap().target, b);
    }

    #[test]
    fn handoff_barrier_contains_only_frozen_subscribers() {
        let subscribed = NodeIncarnation::new(1).unwrap();
        let unrelated = NodeIncarnation::new(2).unwrap();
        let revision = Revision::new(5).unwrap();
        let mut barrier = HandoffBarrier::freeze(
            EntityType::new("world").unwrap(),
            ShardId::new(1),
            revision,
            [subscribed],
        );
        assert!(barrier.apply_revision(unrelated, revision).is_err());
        barrier.apply_revision(subscribed, revision).unwrap();
        assert!(barrier.is_complete());
    }

    #[test]
    fn persisted_slot_rejects_orphaned_handoff_metadata() {
        let mut slot = running_slot(node("a", 1, 1001));
        slot.barrier_sessions
            .insert(NodeIncarnation::new(2).unwrap());
        assert_eq!(slot.validate(), Err(PlacementTypeError::InvalidSlotState));

        slot.barrier_sessions.clear();
        slot.active_move = Some(7);
        assert_eq!(slot.validate(), Err(PlacementTypeError::InvalidSlotState));

        slot.state = PlacementSlotState::Stopping;
        assert!(slot.validate().is_ok());
    }
}
