use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use lattice_core::actor_ref::ActorRef;
use lattice_core::instance::InstanceId;
use lattice_core::kind::ServiceKind;
use lattice_core::trace::TraceContext;
use lattice_rpc::traits::{ActorRefRpcCore, RoutedRequest, RpcRequest, ShardedRpcCore};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryOptions {
    guarantee: DeliveryGuarantee,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeliveryGuarantee {
    AtLeastOnce,
}

impl DeliveryOptions {
    pub fn at_least_once() -> Self {
        Self {
            guarantee: DeliveryGuarantee::AtLeastOnce,
        }
    }
}

impl<B> ServiceEvents<B>
where
    B: EventBus,
{
    pub fn new(bus: B) -> Self {
        Self { bus }
    }

    pub async fn subscribe_actor<Req, C>(
        &self,
        subscription: EventSubscription,
        core: C,
        options: DeliveryOptions,
    ) -> Result<EventSubscriptionHandle, EventBusError>
    where
        Req: RoutedRequest + RpcRequest,
        C: ShardedRpcCore,
    {
        match options.guarantee {
            DeliveryGuarantee::AtLeastOnce => {}
        }

        self.bus
            .subscribe(subscription, move |event: EventEnvelope| {
                let core = core.clone();
                async move {
                    let req = <Req as ProstMessage>::decode(event.payload.as_slice()).map_err(
                        |error| EventBusError::Decode {
                            message_type: std::any::type_name::<Req>(),
                            reason: error.to_string(),
                        },
                    )?;
                    core.call(req)
                        .await
                        .map(|_| ())
                        .map_err(EventBusError::from_rpc)
                }
            })
            .await
    }

    pub async fn subscribe_actor_mapped<C, F, Req>(
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

    pub async fn subscribe_actor_routed<Req, C>(
        &self,
        subscription: EventSubscription,
        target_service: ServiceKind,
        core: C,
        options: DeliveryOptions,
    ) -> Result<EventSubscriptionHandle, EventBusError>
    where
        Req: RoutedRequest + RpcRequest,
        C: ActorRefRpcCore,
    {
        match options.guarantee {
            DeliveryGuarantee::AtLeastOnce => {}
        }

        self.bus
            .subscribe(subscription, move |event: EventEnvelope| {
                let target_service = target_service.clone();
                let core = core.clone();
                async move {
                    let actor_kind =
                        event
                            .actor_kind
                            .clone()
                            .ok_or(EventBusError::MissingActorTarget {
                                field: "actor_kind",
                            })?;
                    let actor_id = event
                        .actor_id
                        .clone()
                        .ok_or(EventBusError::MissingActorTarget { field: "actor_id" })?;
                    let req = <Req as ProstMessage>::decode(event.payload.as_slice()).map_err(
                        |error| EventBusError::Decode {
                            message_type: std::any::type_name::<Req>(),
                            reason: error.to_string(),
                        },
                    )?;
                    let actor_ref = ActorRef::routed(target_service, actor_kind, actor_id);
                    core.call_ref(actor_ref, req)
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
