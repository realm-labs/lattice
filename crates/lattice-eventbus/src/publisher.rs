use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use lattice_actor::handle::ActorHandle;
use lattice_actor::protocol::SupportsTell;
use lattice_actor::recipient::ActorSystem;
use lattice_actor::state_machine::Accepts;
use lattice_actor::traits::{Actor, Handler, Message};
use lattice_core::actor_ref::ActorRef;
use lattice_core::instance::InstanceId;
use lattice_core::kind::ServiceKind;
use lattice_core::trace::TraceContext;
use prost::Message as ProstMessage;
use uuid::Uuid;

use crate::error::EventBusError;
use crate::local::{EventBus, EventSubscriptionHandle};
use crate::types::{EventEnvelope, EventId, EventSubscription, Subject};

#[derive(Debug, Clone)]
pub struct EventPublisher<B> {
    bus: B,
    source_service: ServiceKind,
    source_instance: InstanceId,
    incarnation: Arc<str>,
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

    pub async fn subscribe_recipient<P, M>(
        &self,
        subscription: EventSubscription,
        actor_system: ActorSystem,
        recipient: ActorRef<P>,
    ) -> Result<EventSubscriptionHandle, EventBusError>
    where
        P: SupportsTell<M>,
        M: Message + ProstMessage + Default,
    {
        self.bus
            .subscribe(subscription, move |event: EventEnvelope| {
                let actor_system = actor_system.clone();
                let recipient = recipient.clone();
                async move {
                    let message = M::decode(event.payload.as_slice()).map_err(|error| {
                        EventBusError::Decode {
                            message_type: std::any::type_name::<M>(),
                            reason: error.to_string(),
                        }
                    })?;
                    actor_system
                        .tell(&recipient, message)
                        .await
                        .map_err(|error| EventBusError::ActorDelivery(error.to_string()))
                }
            })
            .await
    }

    pub async fn subscribe_mapped<P, M, F>(
        &self,
        subscription: EventSubscription,
        actor_system: ActorSystem,
        recipient: ActorRef<P>,
        map: F,
    ) -> Result<EventSubscriptionHandle, EventBusError>
    where
        P: SupportsTell<M>,
        M: Message,
        F: Fn(EventEnvelope) -> M + Send + Sync + 'static,
    {
        self.bus
            .subscribe(subscription, move |event: EventEnvelope| {
                let actor_system = actor_system.clone();
                let recipient = recipient.clone();
                let message = map(event);
                async move {
                    actor_system
                        .tell(&recipient, message)
                        .await
                        .map_err(|error| EventBusError::ActorDelivery(error.to_string()))
                }
            })
            .await
    }

    /// Subscribes one exact local Actor activation to a typed event payload.
    ///
    /// The EventBus connection is shared; only the logical subscription and its delivery target
    /// belong to the Actor. The returned handle must be retained for the Actor lifecycle and shut
    /// down while the Actor is stopping.
    pub async fn subscribe_actor<A, M>(
        &self,
        subscription: EventSubscription,
        recipient: ActorHandle<A>,
    ) -> Result<EventSubscriptionHandle, EventBusError>
    where
        A: Actor + Handler<M>,
        A::Behavior: Accepts<M>,
        M: Message + ProstMessage + Default,
    {
        self.bus
            .subscribe(subscription, move |event: EventEnvelope| {
                let recipient = recipient.clone();
                async move {
                    let message = M::decode(event.payload.as_slice()).map_err(|error| {
                        EventBusError::Decode {
                            message_type: std::any::type_name::<M>(),
                            reason: error.to_string(),
                        }
                    })?;
                    recipient
                        .tell(message)
                        .await
                        .map_err(|error| EventBusError::ActorDelivery(error.to_string()))
                }
            })
            .await
    }

    /// Subscribes one exact local Actor activation and maps the full event envelope to its mailbox
    /// message. This preserves event ids, revisions, trace data, or other envelope metadata when
    /// the Actor needs them for idempotency.
    pub async fn subscribe_actor_mapped<A, M, F>(
        &self,
        subscription: EventSubscription,
        recipient: ActorHandle<A>,
        map: F,
    ) -> Result<EventSubscriptionHandle, EventBusError>
    where
        A: Actor + Handler<M>,
        A::Behavior: Accepts<M>,
        M: Message,
        F: Fn(EventEnvelope) -> M + Send + Sync + 'static,
    {
        self.bus
            .subscribe(subscription, move |event: EventEnvelope| {
                let recipient = recipient.clone();
                let message = map(event);
                async move {
                    recipient
                        .tell(message)
                        .await
                        .map_err(|error| EventBusError::ActorDelivery(error.to_string()))
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
            incarnation: Uuid::new_v4().simple().to_string().into(),
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
            "{}:{}:{}:{}",
            self.source_service.as_str(),
            self.source_instance.as_str(),
            self.incarnation,
            self.next_id.fetch_add(1, Ordering::SeqCst)
        ));
        self.bus
            .publish(EventEnvelope {
                event_id: event_id.clone(),
                subject,
                event_type: event_type.into(),
                source_service: self.source_service.clone(),
                source_instance: self.source_instance.clone(),
                recipient: None,
                correlation_id: None,
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

#[cfg(test)]
mod tests {
    use std::{io::Error as IoError, time::Duration};

    use lattice_actor::{
        context::HandlerContext,
        mailbox::MailboxConfig,
        runtime::spawn_actor,
        state_machine::Stateless,
        traits::{Actor, ActorLifecycleState, Handler, Message, StopReason},
    };
    use lattice_core::{instance::InstanceId, service_kind, trace::TraceContext};
    use prost::Message as ProstMessage;
    use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

    use super::*;
    use crate::{
        local::{EventBus, LocalEventBus},
        types::{EventEnvelope, EventId, EventSubscription, SubjectFilter},
    };

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestEvent {
        #[prost(uint64, tag = "1")]
        value: u64,
    }

    impl Message for TestEvent {}

    #[derive(Debug, Clone)]
    struct MappedEvent {
        event_id: String,
    }

    impl Message for MappedEvent {}

    struct EventActor {
        typed: UnboundedSender<u64>,
        mapped: UnboundedSender<String>,
    }

    impl Actor for EventActor {
        type Error = IoError;
        type Behavior = Stateless;
    }

    impl Handler<TestEvent> for EventActor {
        async fn handle(
            &mut self,
            _ctx: &mut HandlerContext<'_, Self>,
            message: TestEvent,
        ) -> Result<(), Self::Error> {
            self.typed
                .send(message.value)
                .map_err(|_| IoError::other("typed observation receiver closed"))
        }
    }

    impl Handler<MappedEvent> for EventActor {
        async fn handle(
            &mut self,
            _ctx: &mut HandlerContext<'_, Self>,
            message: MappedEvent,
        ) -> Result<(), Self::Error> {
            self.mapped
                .send(message.event_id)
                .map_err(|_| IoError::other("mapped observation receiver closed"))
        }
    }

    fn envelope(subject: &str, event_id: &str, payload: Vec<u8>) -> EventEnvelope {
        EventEnvelope {
            event_id: EventId::new(event_id),
            subject: Subject::new(subject),
            event_type: "test".to_owned(),
            source_service: service_kind!("Test"),
            source_instance: InstanceId::new("test-a"),
            recipient: None,
            correlation_id: None,
            trace: TraceContext::default(),
            occurred_unix_ms: 1,
            payload,
        }
    }

    async fn running_actor() -> (
        ActorHandle<EventActor>,
        tokio::sync::mpsc::UnboundedReceiver<u64>,
        tokio::sync::mpsc::UnboundedReceiver<String>,
    ) {
        let (typed, typed_rx) = unbounded_channel();
        let (mapped, mapped_rx) = unbounded_channel();
        let handle = spawn_actor(EventActor { typed, mapped }, MailboxConfig::bounded(8));
        while handle.lifecycle_state() == ActorLifecycleState::Starting {
            tokio::task::yield_now().await;
        }
        (handle, typed_rx, mapped_rx)
    }

    #[tokio::test]
    async fn typed_subscription_delivers_payload_to_exact_actor_activation() {
        let bus = LocalEventBus::new();
        let events = ServiceEvents::new(bus.clone());
        let (actor, mut typed, _) = running_actor().await;
        let subscription = events
            .subscribe_actor::<EventActor, TestEvent>(
                EventSubscription::local(SubjectFilter::new("player.7")),
                actor.clone(),
            )
            .await
            .unwrap();

        bus.publish(envelope(
            "player.7",
            "typed-1",
            TestEvent { value: 42 }.encode_to_vec(),
        ))
        .await
        .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), typed.recv())
                .await
                .unwrap(),
            Some(42)
        );

        assert!(subscription.shutdown(Duration::from_secs(1)).await);
        actor.stop(StopReason::Requested).await.unwrap();
    }

    #[tokio::test]
    async fn mapped_subscription_preserves_envelope_metadata_in_actor_message() {
        let bus = LocalEventBus::new();
        let events = ServiceEvents::new(bus.clone());
        let (actor, _, mut mapped) = running_actor().await;
        let subscription = events
            .subscribe_actor_mapped(
                EventSubscription::local(SubjectFilter::new("alliance.9")),
                actor.clone(),
                |event| MappedEvent {
                    event_id: event.event_id.as_str().to_owned(),
                },
            )
            .await
            .unwrap();

        bus.publish(envelope("alliance.9", "mapped-1", Vec::new()))
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), mapped.recv())
                .await
                .unwrap(),
            Some("mapped-1".to_owned())
        );

        assert!(subscription.shutdown(Duration::from_secs(1)).await);
        actor.stop(StopReason::Requested).await.unwrap();
    }
}
