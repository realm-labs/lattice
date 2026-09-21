use crate::actor::{ActivationId, ActorAddress, ActorPath, EntityAddress, ProtocolId};
use crate::cluster::{
    ClusterId, ConfigFingerprint, EntityId, EntityType, NodeEndpoint, NodeIncarnation,
    PlacementDomainId,
};
use crate::service::ServiceName;
use crate::service_name;
use crate::trace::TraceContext;

const WORLD_SERVICE: ServiceName = service_name!("World");

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
        EntityId::new(42_u64.to_be_bytes()).unwrap(),
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
