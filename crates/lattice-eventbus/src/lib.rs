use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use lattice_core::{ActorId, ActorKind, InstanceId, RequestId, ServiceKind, TraceContext};
use lattice_rpc::{RoutedRequest, RpcError, RpcRequest, ShardedRpcCore};
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Subject(String);

impl Subject {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventId(String);

impl EventId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventEnvelope {
    pub event_id: EventId,
    pub subject: Subject,
    pub event_type: String,
    pub source_service: ServiceKind,
    pub source_instance: InstanceId,
    pub actor_kind: Option<ActorKind>,
    pub actor_id: Option<ActorId>,
    pub request_id: Option<RequestId>,
    pub trace: TraceContext,
    pub occurred_unix_ms: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventSubscription {
    pub filter: SubjectFilter,
    pub durable_name: Option<String>,
}

impl EventSubscription {
    pub fn local(filter: SubjectFilter) -> Self {
        Self {
            filter,
            durable_name: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectFilter(String);

impl SubjectFilter {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    fn matches(&self, subject: &Subject) -> bool {
        if self.0 == subject.0 {
            return true;
        }
        if let Some(prefix) = self.0.strip_suffix(".*") {
            return subject.0.starts_with(prefix)
                && subject
                    .0
                    .as_bytes()
                    .get(prefix.len())
                    .is_some_and(|byte| *byte == b'.');
        }
        false
    }
}

#[async_trait]
pub trait EventHandler: Send + Sync + 'static {
    async fn handle(&self, event: EventEnvelope) -> Result<(), EventBusError>;
}

#[async_trait]
impl<F, Fut> EventHandler for F
where
    F: Fn(EventEnvelope) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), EventBusError>> + Send,
{
    async fn handle(&self, event: EventEnvelope) -> Result<(), EventBusError> {
        self(event).await
    }
}

#[async_trait]
pub trait EventBus: Clone + Send + Sync + 'static {
    async fn publish(&self, event: EventEnvelope) -> Result<(), EventBusError>;
    async fn subscribe<H>(
        &self,
        subscription: EventSubscription,
        handler: H,
    ) -> Result<EventSubscriptionHandle, EventBusError>
    where
        H: EventHandler;
}

#[derive(Debug, Clone)]
pub struct EventSubscriptionHandle {
    id: u64,
    cancelled: Arc<AtomicBool>,
}

impl EventSubscriptionHandle {
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

#[derive(Clone)]
pub struct LocalEventBus {
    inner: Arc<LocalEventBusInner>,
}

impl LocalEventBus {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(LocalEventBusInner {
                next_id: AtomicU64::new(1),
                subscribers: Mutex::new(HashMap::new()),
            }),
        }
    }
}

impl Default for LocalEventBus {
    fn default() -> Self {
        Self::new()
    }
}

struct LocalEventBusInner {
    next_id: AtomicU64,
    subscribers: Mutex<HashMap<u64, LocalSubscriber>>,
}

struct LocalSubscriber {
    subscription: EventSubscription,
    handler: Arc<dyn EventHandler>,
    cancelled: Arc<AtomicBool>,
}

#[async_trait]
impl EventBus for LocalEventBus {
    async fn publish(&self, event: EventEnvelope) -> Result<(), EventBusError> {
        let subscribers = self.inner.subscribers.lock().await;
        for subscriber in subscribers.values() {
            if subscriber.cancelled.load(Ordering::SeqCst) {
                continue;
            }
            if subscriber.subscription.filter.matches(&event.subject) {
                subscriber.handler.handle(event.clone()).await?;
            }
        }
        Ok(())
    }

    async fn subscribe<H>(
        &self,
        subscription: EventSubscription,
        handler: H,
    ) -> Result<EventSubscriptionHandle, EventBusError>
    where
        H: EventHandler,
    {
        let id = self.inner.next_id.fetch_add(1, Ordering::SeqCst);
        let cancelled = Arc::new(AtomicBool::new(false));
        self.inner.subscribers.lock().await.insert(
            id,
            LocalSubscriber {
                subscription,
                handler: Arc::new(handler),
                cancelled: cancelled.clone(),
            },
        );
        Ok(EventSubscriptionHandle { id, cancelled })
    }
}

#[derive(Debug, Clone)]
pub struct EventPublisher<B> {
    bus: B,
    source_service: ServiceKind,
    source_instance: InstanceId,
    next_id: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
pub struct ServiceEvents<B> {
    bus: B,
}

impl<B> ServiceEvents<B>
where
    B: EventBus,
{
    pub fn new(bus: B) -> Self {
        Self { bus }
    }

    pub async fn subscribe_actor<C, F, Req>(
        &self,
        subscription: EventSubscription,
        core: C,
        map: F,
    ) -> Result<EventSubscriptionHandle, EventBusError>
    where
        C: ShardedRpcCore,
        F: Fn(EventEnvelope) -> Req + Send + Sync + 'static,
        Req: RoutedRequest + RpcRequest,
    {
        self.bus
            .subscribe(subscription, move |event: EventEnvelope| {
                let core = core.clone();
                let req = map(event);
                async move {
                    core.call(req)
                        .await
                        .map(|_| ())
                        .map_err(EventBusError::from_rpc)
                }
            })
            .await
    }
}

impl<B> EventPublisher<B>
where
    B: EventBus,
{
    pub fn new(bus: B, source_service: ServiceKind, source_instance: InstanceId) -> Self {
        Self {
            bus,
            source_service,
            source_instance,
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub async fn publish_bytes(
        &self,
        subject: Subject,
        event_type: impl Into<String>,
        payload: Vec<u8>,
        trace: TraceContext,
    ) -> Result<EventId, EventBusError> {
        let event_id = EventId::new(format!(
            "{}:{}:{}",
            self.source_service.as_str(),
            self.source_instance.as_str(),
            self.next_id.fetch_add(1, Ordering::SeqCst)
        ));
        self.bus
            .publish(EventEnvelope {
                event_id: event_id.clone(),
                subject,
                event_type: event_type.into(),
                source_service: self.source_service.clone(),
                source_instance: self.source_instance.clone(),
                actor_kind: None,
                actor_id: None,
                request_id: None,
                trace,
                occurred_unix_ms: now_unix_ms(),
                payload,
            })
            .await?;
        Ok(event_id)
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EventBusError {
    #[error("event handler failed: {0}")]
    Handler(String),
    #[error("event actor delivery failed: {0}")]
    ActorDelivery(String),
}

impl EventBusError {
    fn from_rpc(error: RpcError) -> Self {
        Self::ActorDelivery(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use lattice_core::{ActorKind, RouteKey, actor_kind};
    use lattice_core::{InstanceId, service_kind};
    use lattice_rpc::{RpcRequest, ShardedRpcCore};
    use tokio::sync::Mutex;

    use super::*;

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
        let publisher =
            EventPublisher::new(bus, service_kind!("World"), InstanceId::new("world-a"));

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

    #[tokio::test]
    async fn service_events_subscribe_actor_routes_through_rpc_core() {
        let bus = LocalEventBus::new();
        let events = ServiceEvents::new(bus.clone());
        let core = RecordingCore::default();
        let routed = core.routed.clone();
        events
            .subscribe_actor(
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
}
