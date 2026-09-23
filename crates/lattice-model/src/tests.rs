use crate::actor::ActorId;
use crate::actor::{ActivationId, ActorAddress, ActorPath, EntityAddress, ProtocolId};
use crate::cluster::{
    ClusterId, ConfigFingerprint, EntityType, NodeEndpoint, NodeIncarnation, PlacementDomainId,
};
use crate::service::ServiceName;
use crate::service_name;
use crate::trace::TraceContext;
use bytes::Bytes;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

const WORLD_SERVICE: ServiceName = service_name!("World");

#[test]
fn actor_id_shares_storage_and_compares_by_bytes() {
    let original = Bytes::from(vec![1, 2, 3, 4]);
    let id = ActorId::new(original.clone()).unwrap();
    let cloned = id.clone();
    assert_eq!(original.as_ptr(), id.as_bytes().as_ptr());
    assert_eq!(id.as_bytes().as_ptr(), cloned.as_bytes().as_ptr());
    let independent = ActorId::new(vec![1, 2, 3, 4]).unwrap();
    assert_eq!(id, independent);
    let hash = |value: &ActorId| {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    };
    assert_eq!(hash(&id), hash(&independent));
    let shared: Bytes = cloned.into();
    assert_eq!(shared.as_ptr(), original.as_ptr());
    assert_eq!(
        ActorId::new("42".to_owned()).unwrap(),
        ActorId::new(vec![b'4', b'2']).unwrap()
    );
}

#[test]
fn actor_id_serialization_preserves_bytes_and_validates_bounds() {
    let id = ActorId::new(vec![0, 42, 255]).unwrap();
    assert_eq!(serde_json::to_string(&id).unwrap(), "[0,42,255]");
    assert_eq!(serde_json::from_str::<ActorId>("[0,42,255]").unwrap(), id);
    assert!(ActorId::new(Vec::new()).is_err());
    assert!(ActorId::new(vec![0; 256]).is_ok());
    assert!(ActorId::new(vec![0; 257]).is_err());
    assert!(serde_json::from_str::<ActorId>("[]").is_err());
    let oversized = serde_json::to_string(&vec![0_u8; 257]).unwrap();
    assert!(serde_json::from_str::<ActorId>(&oversized).is_err());
}

#[test]
fn service_name_macro_is_const() {
    assert_eq!(WORLD_SERVICE.as_str(), "World");
}

#[test]
fn actor_and_entity_refs_have_distinct_exact_and_logical_identity() {
    let node = NodeIncarnation::new(7).unwrap();
    let protocol = ProtocolId::new(11).unwrap();
    let actor = ActorAddress::new(
        ClusterId::new("test").unwrap(),
        NodeEndpoint::new("127.0.0.1", 19083).unwrap(),
        ActorPath::user(["user", "session-1"]).unwrap(),
        ActivationId::new(node, 1).unwrap(),
        protocol,
    )
    .unwrap();
    let entity = EntityAddress::new(
        ClusterId::new("test").unwrap(),
        PlacementDomainId::new("world").unwrap(),
        EntityType::new("world").unwrap(),
        ActorId::new(42_u64.to_be_bytes().to_vec()).unwrap(),
        protocol,
        ConfigFingerprint::new([3; 32]),
    )
    .unwrap();

    assert_eq!(actor.node_incarnation(), node);
    assert_eq!(actor.actor_path().to_string(), "/user/session-1");
    assert_eq!(entity.entity_id().as_bytes(), &42_u64.to_be_bytes());
    assert_eq!(entity.domain().as_str(), "world");
}

#[test]
fn node_address_supports_canonical_ipv6_literals() {
    let address = NodeEndpoint::new("2001:db8::1", 7447).unwrap();

    assert_eq!(address.host(), "2001:db8::1");
    assert_eq!(address.to_string(), "[2001:db8::1]:7447");
    assert!(NodeEndpoint::new("[2001:db8::1]", 7447).is_err());
}

#[test]
fn trace_context_reports_whether_propagation_fields_are_empty() {
    let empty = TraceContext::default();
    let trace = TraceContext {
        traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00".into()),
        tracestate: None,
    };

    assert!(empty.is_empty());
    assert!(!trace.is_empty());
}
