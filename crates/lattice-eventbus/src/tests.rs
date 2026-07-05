use std::sync::Arc;

use async_trait::async_trait;
use lattice_core::{ActorId, ActorKind, ActorRef, ActorRefTarget, RouteKey, actor_kind};
use lattice_core::{InstanceId, TraceContext, service_kind};
use lattice_rpc::{ActorRefRpcCore, RoutedRequest, RpcRequest, ShardedRpcCore};
use prost::Message as ProstMessage;
use tokio::sync::Mutex;

use crate::*;

#[tokio::test]
async fn local_event_bus_publishes_to_matching_subject_subscribers() {
    let bus = LocalEventBus::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_clone = seen.clone();
    bus.subscribe(
        EventSubscription::local(SubjectFilter::new("game.world.*")),
        move |event: EventEnvelope| {
            let seen = seen_clone.clone();
            async move {
                seen.lock().await.push(event.event_type);
                Ok(())
            }
        },
    )
    .await
    .unwrap();

    bus.publish(test_event("game.world.player_entered", "PlayerEntered"))
        .await
        .unwrap();
    bus.publish(test_event("game.guild.created", "GuildCreated"))
        .await
        .unwrap();

    assert_eq!(*seen.lock().await, vec!["PlayerEntered"]);
}

#[tokio::test]
async fn subscription_handle_cancels_local_delivery() {
    let bus = LocalEventBus::new();
    let seen = Arc::new(Mutex::new(0));
    let seen_clone = seen.clone();
    let handle = bus
        .subscribe(
            EventSubscription::local(SubjectFilter::new("system.config.reload")),
            move |_event: EventEnvelope| {
                let seen = seen_clone.clone();
                async move {
                    *seen.lock().await += 1;
                    Ok(())
                }
            },
        )
        .await
        .unwrap();

    handle.cancel();
    bus.publish(test_event("system.config.reload", "ConfigReload"))
        .await
        .unwrap();

    assert_eq!(*seen.lock().await, 0);
}

#[tokio::test]
async fn typed_publisher_fills_framework_metadata() {
    let bus = LocalEventBus::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_clone = seen.clone();
    bus.subscribe(
        EventSubscription::local(SubjectFilter::new("game.world.player_entered")),
        move |event: EventEnvelope| {
            let seen = seen_clone.clone();
            async move {
                seen.lock().await.push(event);
                Ok(())
            }
        },
    )
    .await
    .unwrap();
    let publisher = EventPublisher::new(bus, service_kind!("World"), InstanceId::new("world-a"));

    let event_id = publisher
        .publish_bytes(
            Subject::new("game.world.player_entered"),
            "PlayerEntered",
            vec![1, 2, 3],
            TraceContext::default(),
        )
        .await
        .unwrap();

    let seen = seen.lock().await;
    assert_eq!(seen[0].event_id, event_id);
    assert_eq!(seen[0].source_service, service_kind!("World"));
    assert_eq!(seen[0].source_instance, InstanceId::new("world-a"));
    assert_eq!(seen[0].payload, vec![1, 2, 3]);
}

fn test_event(subject: &str, event_type: &str) -> EventEnvelope {
    EventEnvelope {
        event_id: EventId::new("event-1"),
        subject: Subject::new(subject),
        event_type: event_type.to_string(),
        source_service: service_kind!("World"),
        source_instance: InstanceId::new("world-a"),
        actor_kind: None,
        actor_id: None,
        request_id: None,
        trace: TraceContext::default(),
        occurred_unix_ms: 1,
        payload: Vec::new(),
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct EventToActorRequest {
    #[prost(uint64, tag = "1")]
    world_id: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct EventToActorReply {}

impl RoutedRequest for EventToActorRequest {
    fn actor_kind(&self) -> ActorKind {
        actor_kind!("World")
    }

    fn route_key(&self) -> RouteKey {
        RouteKey::U64(self.world_id)
    }
}

impl RpcRequest for EventToActorRequest {
    type Reply = EventToActorReply;
    const METHOD: &'static str = "WorldRpc/Event";
}

#[derive(Clone, Default)]
struct RecordingCore {
    routed: Arc<Mutex<Vec<RouteKey>>>,
}

#[async_trait]
impl ShardedRpcCore for RecordingCore {
    async fn call<Req>(&self, req: Req) -> Result<Req::Reply, lattice_rpc::RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        self.routed.lock().await.push(req.route_key());
        Ok(Req::Reply::default())
    }
}

#[derive(Clone, Default)]
struct RecordingActorRefCore {
    refs: Arc<Mutex<Vec<ActorRef>>>,
}

#[async_trait]
impl ActorRefRpcCore for RecordingActorRefCore {
    async fn call_ref<Req>(
        &self,
        actor_ref: ActorRef,
        _req: Req,
    ) -> Result<Req::Reply, lattice_rpc::RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        self.refs.lock().await.push(actor_ref);
        Ok(Req::Reply::default())
    }
}

#[tokio::test]
async fn service_events_subscribe_actor_routes_through_rpc_core() {
    let bus = LocalEventBus::new();
    let events = ServiceEvents::new(bus.clone());
    let core = RecordingCore::default();
    let routed = core.routed.clone();
    events
        .subscribe_actor_mapped(
            EventSubscription::local(SubjectFilter::new("game.world.*")),
            core,
            |_event| EventToActorRequest { world_id: 42 },
        )
        .await
        .unwrap();

    bus.publish(test_event("game.world.player_entered", "PlayerEntered"))
        .await
        .unwrap();

    assert_eq!(*routed.lock().await, vec![RouteKey::U64(42)]);
}

#[tokio::test]
async fn service_events_subscribe_actor_decodes_typed_rpc_request() {
    let bus = LocalEventBus::new();
    let events = ServiceEvents::new(bus.clone());
    let core = RecordingCore::default();
    let routed = core.routed.clone();
    events
        .subscribe_actor::<EventToActorRequest, _>(
            EventSubscription::local(SubjectFilter::new("game.world.*")),
            core,
            DeliveryOptions::at_least_once(),
        )
        .await
        .unwrap();

    let mut event = test_event("game.world.player_entered", "PlayerEntered");
    event.payload = EventToActorRequest { world_id: 99 }.encode_to_vec();
    bus.publish(event).await.unwrap();

    assert_eq!(*routed.lock().await, vec![RouteKey::U64(99)]);
}

#[tokio::test]
async fn service_events_subscribe_actor_routed_uses_envelope_actor_ref() {
    let bus = LocalEventBus::new();
    let events = ServiceEvents::new(bus.clone());
    let core = RecordingActorRefCore::default();
    let refs = core.refs.clone();
    events
        .subscribe_actor_routed::<EventToActorRequest, _>(
            EventSubscription::local(SubjectFilter::new("game.world.*")),
            service_kind!("World"),
            core,
            DeliveryOptions::at_least_once(),
        )
        .await
        .unwrap();

    let mut event = test_event("game.world.player_entered", "PlayerEntered");
    event.actor_kind = Some(actor_kind!("World"));
    event.actor_id = Some(ActorId::U64(77));
    event.payload = EventToActorRequest { world_id: 77 }.encode_to_vec();
    bus.publish(event).await.unwrap();

    let refs = refs.lock().await;
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].service_kind, service_kind!("World"));
    assert_eq!(refs[0].actor_kind, actor_kind!("World"));
    assert_eq!(refs[0].actor_id, ActorId::U64(77));
    assert_eq!(refs[0].target, ActorRefTarget::Routed);
}

#[tokio::test]
async fn service_events_subscribe_actor_rejects_invalid_typed_payload() {
    let bus = LocalEventBus::new();
    let events = ServiceEvents::new(bus.clone());
    let core = RecordingCore::default();
    events
        .subscribe_actor::<EventToActorRequest, _>(
            EventSubscription::local(SubjectFilter::new("game.world.*")),
            core,
            DeliveryOptions::at_least_once(),
        )
        .await
        .unwrap();

    let mut event = test_event("game.world.player_entered", "PlayerEntered");
    event.payload = vec![0xff];
    let error = bus.publish(event).await.unwrap_err();

    assert!(matches!(error, EventBusError::Decode { .. }));
}

#[tokio::test]
async fn service_events_subscribe_actor_routed_requires_actor_target() {
    let bus = LocalEventBus::new();
    let events = ServiceEvents::new(bus.clone());
    events
        .subscribe_actor_routed::<EventToActorRequest, _>(
            EventSubscription::local(SubjectFilter::new("game.world.*")),
            service_kind!("World"),
            RecordingActorRefCore::default(),
            DeliveryOptions::at_least_once(),
        )
        .await
        .unwrap();

    let mut event = test_event("game.world.player_entered", "PlayerEntered");
    event.payload = EventToActorRequest { world_id: 77 }.encode_to_vec();
    let error = bus.publish(event).await.unwrap_err();

    assert_eq!(
        error,
        EventBusError::MissingActorTarget {
            field: "actor_kind"
        }
    );
}
