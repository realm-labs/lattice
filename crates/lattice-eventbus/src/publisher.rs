use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use lattice_actor::recipient::ActorSystem;
use lattice_actor::traits::{Actor, Message};
use lattice_core::actor_ref::ActorRef;
use lattice_core::instance::InstanceId;
use lattice_core::kind::ServiceKind;
use lattice_core::trace::TraceContext;
use prost::Message as ProstMessage;

use crate::error::EventBusError;
use crate::local::{EventBus, EventSubscriptionHandle};
use crate::types::{EventEnvelope, EventId, EventSubscription, Subject};

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

    pub async fn subscribe_recipient<A, M>(
        &self,
        subscription: EventSubscription,
        actor_system: ActorSystem,
        recipient: ActorRef<A>,
    ) -> Result<EventSubscriptionHandle, EventBusError>
    where
        A: Actor,
        M: Message + ProstMessage + Default,
    {
        self.bus
            .subscribe(subscription, move |event: EventEnvelope| {
                let actor_system = actor_system.clone();
                let recipient: ActorRef<A> = recipient.cast();
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

    pub async fn subscribe_mapped<A, M, F>(
        &self,
        subscription: EventSubscription,
        actor_system: ActorSystem,
        recipient: ActorRef<A>,
        map: F,
    ) -> Result<EventSubscriptionHandle, EventBusError>
    where
        A: Actor,
        M: Message,
        F: Fn(EventEnvelope) -> M + Send + Sync + 'static,
    {
        self.bus
            .subscribe(subscription, move |event: EventEnvelope| {
                let actor_system = actor_system.clone();
                let recipient: ActorRef<A> = recipient.cast();
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
