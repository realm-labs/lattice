use std::time::Duration;

use super::{AuthorityError, AuthorityEvent, PlacementAuthority};
use crate::types::{
    AssignmentGeneration, ClaimGrant, CoordinatorTerm, GrantSequence, MonotonicTime, NodeKey,
    PlacementSlot, PlacementSlotKey, PlacementSlotState, PlacementVersion, Revision, ShardId,
};
use lattice_model::cluster::{
    ActorGroupId, ConfigFingerprint, EntityType, NodeEndpoint, NodeIncarnation,
};

fn fixture() -> (PlacementAuthority, ClaimGrant) {
    let group = ActorGroupId::new("freshness").unwrap();
    let owner = NodeKey {
        node_id: "owner".into(),
        address: NodeEndpoint::new("localhost", 12000).unwrap(),
        incarnation: NodeIncarnation::new(1).unwrap(),
    };
    let slot = PlacementSlot {
        key: PlacementSlotKey::Shard {
            group: group.clone(),
            entity_type: EntityType::new("test").unwrap(),
            shard_id: ShardId::new(0),
        },
        config_fingerprint: ConfigFingerprint::new([1; 32]),
        owner: Some(owner.clone()),
        target: None,
        assignment_generation: AssignmentGeneration::new(1).unwrap(),
        version: PlacementVersion::new(
            group.clone(),
            CoordinatorTerm::new(1).unwrap(),
            Revision::new(1).unwrap(),
        ),
        state: PlacementSlotState::Running,
        active_move: None,
        barrier_sessions: Default::default(),
    };
    let grant = ClaimGrant {
        request_id: 7,
        group,
        slot: slot.key.clone(),
        owner: owner.clone(),
        coordinator_term: slot.version.term,
        assignment_generation: slot.assignment_generation,
        grant_sequence: GrantSequence::new(1).unwrap(),
        ttl: Duration::from_secs(5),
    };
    let mut authority = PlacementAuthority::new(owner, Duration::from_secs(1)).unwrap();
    authority
        .transition(AuthorityEvent::ReconcileSlot(slot))
        .unwrap();
    (authority, grant)
}

fn at(ms: u64) -> MonotonicTime {
    MonotonicTime::from_millis(ms)
}

#[test]
fn delayed_reply_consumes_the_original_request_interval() {
    let (mut authority, grant) = fixture();
    authority
        .transition(AuthorityEvent::BeginRenewal {
            request_id: 7,
            now: at(100),
        })
        .unwrap();
    authority
        .transition(AuthorityEvent::InstallGrant {
            grant: grant.clone(),
            now: at(3_000),
        })
        .unwrap();
    assert!(authority.admission_open_at(at(4_099)));
    assert!(!authority.admission_open_at(at(4_100)));
    assert_eq!(
        authority.transition(AuthorityEvent::InstallGrant {
            grant,
            now: at(3_100)
        }),
        Err(AuthorityError::UncorrelatedGrant)
    );
}

#[test]
fn unsolicited_reordered_and_expired_responses_cannot_open_admission() {
    let (mut authority, grant) = fixture();
    assert_eq!(
        authority.transition(AuthorityEvent::InstallGrant {
            grant: grant.clone(),
            now: at(0)
        }),
        Err(AuthorityError::UncorrelatedGrant)
    );
    authority
        .transition(AuthorityEvent::BeginRenewal {
            request_id: 8,
            now: at(10),
        })
        .unwrap();
    assert_eq!(
        authority.transition(AuthorityEvent::InstallGrant {
            grant: grant.clone(),
            now: at(20)
        }),
        Err(AuthorityError::UncorrelatedGrant)
    );
    authority
        .transition(AuthorityEvent::BeginRenewal {
            request_id: 7,
            now: at(30),
        })
        .unwrap();
    assert_eq!(
        authority.transition(AuthorityEvent::InstallGrant {
            grant,
            now: at(4_030)
        }),
        Err(AuthorityError::InvalidClaimDeadline)
    );
    assert!(!authority.admission_open_at(at(4_030)));
}

#[test]
fn renewal_after_expiry_retires_even_before_tick_and_cannot_reopen() {
    let (mut authority, grant) = fixture();
    authority
        .transition(AuthorityEvent::BeginRenewal {
            request_id: 7,
            now: at(0),
        })
        .unwrap();
    authority
        .transition(AuthorityEvent::InstallGrant { grant, now: at(10) })
        .unwrap();
    assert_eq!(
        authority.transition(AuthorityEvent::BeginRenewal {
            request_id: 9,
            now: at(4_000)
        }),
        Err(AuthorityError::Retired)
    );
    assert!(!authority.admission_open_at(at(4_001)));
    assert!(!authority.renewal_needed(at(5_000)));
}

#[test]
fn shutdown_or_claim_loss_cannot_be_repaired_by_a_delayed_reply() {
    let (mut authority, grant) = fixture();
    authority
        .transition(AuthorityEvent::BeginRenewal {
            request_id: 7,
            now: at(0),
        })
        .unwrap();
    authority
        .transition(AuthorityEvent::ExternalClaimLost)
        .unwrap();
    assert_eq!(
        authority.transition(AuthorityEvent::InstallGrant { grant, now: at(10) }),
        Err(AuthorityError::Retired)
    );
}
