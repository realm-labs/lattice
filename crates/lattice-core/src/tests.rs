use crate::actor_address::{
    ActivationId, ActorAddress, ActorPath, ClusterId, ConfigFingerprint, EntityAddress, EntityId,
    EntityType, NodeAddress, NodeIncarnation, PlacementDomainId, ProtocolId,
};
use crate::kind::{ActorKind, ServiceKind};
use crate::trace::TraceContext;
use crate::trace::TraceSpanKind;
use crate::{actor_kind, service_kind};

const WORLD_SERVICE: ServiceKind = service_kind!("World");
const WORLD_ACTOR: ActorKind = actor_kind!("World");

#[test]
fn actor_kind_and_service_kind_macros_are_const() {
    assert_eq!(WORLD_SERVICE.as_str(), "World");
    assert_eq!(WORLD_ACTOR.as_str(), "World");
}

#[test]
fn actor_and_entity_refs_have_distinct_exact_and_logical_identity() {
    let node = NodeIncarnation::new(7).unwrap();
    let protocol = ProtocolId::new(11).unwrap();
    let actor = ActorAddress::new(
        ClusterId::new("test").unwrap(),
        NodeAddress::new("127.0.0.1", 19083).unwrap(),
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
    let address = NodeAddress::new("2001:db8::1", 7447).unwrap();

    assert_eq!(address.host(), "2001:db8::1");
    assert_eq!(address.to_string(), "[2001:db8::1]:7447");
    assert!(NodeAddress::new("[2001:db8::1]", 7447).is_err());
}

#[test]
fn trace_context_reports_empty_and_span_kind_names() {
    let empty = TraceContext::default();
    let trace = TraceContext {
        traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00".into()),
        tracestate: None,
    };

    assert!(empty.is_empty());
    assert!(!trace.is_empty());
    assert_eq!(TraceSpanKind::Client.as_str(), "client");
    assert_eq!(TraceSpanKind::Consumer.as_str(), "consumer");
    let _span = trace.span("rpc.client", TraceSpanKind::Client);
}
