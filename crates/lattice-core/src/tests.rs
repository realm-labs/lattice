use super::*;
use crate::trace::TraceSpanKind;

const WORLD_SERVICE: ServiceKind = service_kind!("World");
const WORLD_ACTOR: ActorKind = actor_kind!("World");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorldId(u64);

impl ActorKey for WorldId {
    fn to_route_key(&self) -> RouteKey {
        RouteKey::U64(self.0)
    }

    fn to_actor_id(&self) -> ActorId {
        ActorId::U64(self.0)
    }

    fn try_from_actor_id(actor_id: &ActorId) -> Result<Self, ActorKeyDecodeError> {
        match actor_id {
            ActorId::U64(value) => Ok(Self(*value)),
            _ => Err(ActorKeyDecodeError {
                reason: "expected u64 actor id for WorldId".to_string(),
            }),
        }
    }
}

#[test]
fn actor_kind_and_service_kind_macros_are_const() {
    assert_eq!(WORLD_SERVICE.as_str(), "World");
    assert_eq!(WORLD_ACTOR.as_str(), "World");
}

#[test]
fn actor_key_converts_through_framework_ids() {
    let id = WorldId(42);

    assert_eq!(id.to_route_key(), RouteKey::U64(42));
    assert_eq!(id.to_actor_id(), ActorId::U64(42));
    assert_eq!(id.to_actor_id().to_route_key(), RouteKey::U64(42));
    assert_eq!(WorldId::try_from_actor_id(&ActorId::U64(42)), Ok(id));
    assert!(WorldId::try_from_actor_id(&ActorId::Str("42".into())).is_err());
}

#[test]
fn actor_ref_models_direct_and_routed_targets() {
    let direct = ActorRef::direct(
        service_kind!("Gateway"),
        actor_kind!("GatewaySession"),
        ActorId::Str("session-1".into()),
        InstanceId::new("gateway-a"),
        "http://127.0.0.1:19083".parse().unwrap(),
        Some(Epoch(7)),
    );
    let routed = ActorRef::routed(WORLD_SERVICE, WORLD_ACTOR, ActorId::U64(42));

    assert!(matches!(direct.target, ActorRefTarget::Direct { .. }));
    assert_eq!(
        direct.actor_id.to_route_key(),
        RouteKey::Str("session-1".into())
    );
    assert_eq!(routed.actor_id.to_route_key(), RouteKey::U64(42));
    assert_eq!(routed.target, ActorRefTarget::Routed);
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
