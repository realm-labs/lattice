use std::sync::Arc;

use lattice_eventbus::local::EventBus;
use lattice_eventbus::nats::{InMemoryNatsClient, InMemoryNatsEventBus};
use lattice_eventbus::types::{EventEnvelope, EventId, EventSubscription, Subject, SubjectFilter};
use lattice_model::service::ServiceInstanceId;
use lattice_model::service_name;
use lattice_model::trace::TraceContext;
use tokio::sync::Mutex;

#[tokio::test]
async fn republished_event_ids_are_collapsed_before_durable_subscribers_see_them() {
    let bus = InMemoryNatsEventBus::new(InMemoryNatsClient::new());
    let durable_seen = Arc::new(Mutex::new(Vec::new()));
    let local_seen = Arc::new(Mutex::new(Vec::new()));

    let _durable = bus
        .subscribe(
            EventSubscription::durable(SubjectFilter::new("game.world.*"), "world-consumer"),
            {
                let durable_seen = durable_seen.clone();
                move |event: EventEnvelope| {
                    let durable_seen = durable_seen.clone();
                    async move {
                        durable_seen
                            .lock()
                            .await
                            .push(event.event_id.as_str().to_string());
                        Ok(())
                    }
                }
            },
        )
        .await
        .unwrap();
    let _core = bus
        .subscribe(
            EventSubscription::local(SubjectFilter::new("game.world.*")),
            {
                let local_seen = local_seen.clone();
                move |event: EventEnvelope| {
                    let local_seen = local_seen.clone();
                    async move {
                        local_seen
                            .lock()
                            .await
                            .push(event.event_id.as_str().to_string());
                        Ok(())
                    }
                }
            },
        )
        .await
        .unwrap();

    bus.publish(test_event("event-1")).await.unwrap();
    bus.publish(test_event("event-1")).await.unwrap();

    assert_eq!(*durable_seen.lock().await, vec!["event-1"]);
    assert_eq!(*local_seen.lock().await, vec!["event-1", "event-1"]);
}

fn test_event(event_id: &str) -> EventEnvelope {
    EventEnvelope {
        event_id: EventId::new(event_id),
        subject: Subject::new("game.world.entered"),
        event_type: "WorldEntered".to_string(),
        source_service: service_name!("World"),
        source_instance: ServiceInstanceId::new("world-a"),
        recipient: None,
        correlation_id: None,
        trace: TraceContext::default(),
        occurred_unix_ms: 1,
        payload: Vec::new(),
    }
}
