use lattice_core::actor_ref::NodeAddress;

use super::*;
use crate::types::{CoordinatorTerm, Revision};

fn version(term: u64, revision: u64) -> PlacementVersion {
    PlacementVersion::new(
        PlacementDomainId::new("test").unwrap(),
        CoordinatorTerm::new(term).unwrap(),
        Revision::new(revision).unwrap(),
    )
}

#[test]
fn singleton_config_fingerprint_has_a_domain_scoped_golden_vector() {
    let config = SingletonConfig::new(
        PlacementDomainId::new("battle").unwrap(),
        SingletonKind::new("scheduler").unwrap(),
        ProtocolId::new(0x1112_1314_1516_1718).unwrap(),
    );
    assert_eq!(
        *config.fingerprint().as_bytes(),
        [
            217, 135, 248, 93, 126, 177, 148, 200, 159, 106, 130, 175, 125, 104, 218, 226, 30, 96,
            2, 62, 254, 44, 79, 26, 117, 71, 189, 8, 177, 11, 237, 216,
        ]
    );
    assert_ne!(
        config.fingerprint(),
        SingletonConfig::new(
            PlacementDomainId::new("world").unwrap(),
            config.kind.clone(),
            config.protocol_id,
        )
        .fingerprint()
    );
}

#[test]
fn placement_domain_hello_builder_preserves_defaults_and_configuration() {
    let domain = PlacementDomainId::new("battle").unwrap();
    let node = NodeKey {
        node_id: "node-a".to_owned(),
        address: NodeAddress::new("127.0.0.1", 25520).unwrap(),
        incarnation: NodeIncarnation::new(1).unwrap(),
    };
    let empty = PlacementDomainHello::builder(node.clone(), domain.clone(), 1).build();
    assert!(empty.hosted_entity_types.is_empty());
    assert!(empty.constraints.is_empty());
    assert_eq!(
        empty.domain_config_fingerprint,
        placement_domain_fingerprint(&domain)
    );
    empty.validate(&SessionLimits::default()).unwrap();

    let entity_type = EntityType::new("player").unwrap();
    let singleton_kind = SingletonKind::new("matchmaker").unwrap();
    let configured = PlacementDomainHello::builder(node, domain, 8)
        .hosted_entity_types(BTreeSet::from([entity_type.clone()]))
        .proxied_entity_types(BTreeSet::from([EntityType::new("chat").unwrap()]))
        .singleton_eligibility(BTreeSet::from([singleton_kind.clone()]))
        .used_singletons(BTreeSet::from([singleton_kind]))
        .constraints(BTreeMap::from([("zone".to_owned(), "east".to_owned())]))
        .build();
    assert_eq!(configured.capacity_units, 8);
    assert!(configured.hosted_entity_types.contains(&entity_type));
    configured.validate(&SessionLimits::default()).unwrap();
}

#[test]
fn higher_term_delta_requires_snapshot_and_lower_term_snapshot_is_stale() {
    let mut session = PlacementDomainState::new(PlacementDomainId::new("test").unwrap());
    session
        .install(SnapshotInstall {
            version: SnapshotVersion::Placement(version(1, 7)),
            records: Vec::new(),
        })
        .unwrap();
    assert_eq!(
        session
            .apply(CoordinatorDelta {
                version: version(2, 8),
                records: Vec::new(),
            })
            .unwrap_err(),
        PlacementDomainStateError::SnapshotRequired
    );
    assert!(!session.ready());
    session
        .install(SnapshotInstall {
            version: SnapshotVersion::Placement(version(2, 8)),
            records: Vec::new(),
        })
        .unwrap();
    assert_eq!(
        session
            .install(SnapshotInstall {
                version: SnapshotVersion::Placement(version(1, 9)),
                records: Vec::new(),
            })
            .unwrap_err(),
        PlacementDomainStateError::StaleTerm
    );
    assert!(!session.ready());
}

#[test]
fn snapshot_staging_timeout_is_renewed_by_chunk_progress() {
    let limits = SnapshotLimits {
        maximum_records: 2,
        maximum_bytes: 4,
        maximum_chunks: 2,
        maximum_chunk_bytes: 2,
        staging_timeout_millis: 10,
    };
    let (begin, chunks, end) = build_snapshot(
        SnapshotVersion::Placement(version(1, 1)),
        vec![
            SnapshotRecord {
                key: "a".to_owned(),
                value: Bytes::from_static(b"x"),
            },
            SnapshotRecord {
                key: "b".to_owned(),
                value: Bytes::from_static(b"y"),
            },
        ],
        &limits,
    )
    .unwrap();
    assert_eq!(chunks.len(), 2);

    let mut stager = SnapshotStager::begin(begin, limits, MonotonicTime::from_millis(0)).unwrap();
    stager
        .push(chunks[0].clone(), MonotonicTime::from_millis(9))
        .unwrap();
    stager
        .push(chunks[1].clone(), MonotonicTime::from_millis(18))
        .unwrap();
    stager.finish(end, MonotonicTime::from_millis(19)).unwrap();
}
