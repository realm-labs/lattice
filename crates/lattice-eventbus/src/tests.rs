use std::sync::Arc;

use lattice_core::instance::InstanceId;
use lattice_core::service_kind;
use lattice_core::trace::TraceContext;
use tokio::sync::Mutex;

use crate::local::{EventBus, LocalEventBus};
use crate::publisher::EventPublisher;
use crate::types::{EventEnvelope, EventSubscription, Subject, SubjectFilter};

#[tokio::test]
async fn local_event_bus_publishes_typed_envelope() {
    let bus = LocalEventBus::new();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let sink = observed.clone();
    let _subscription = bus
        .subscribe(
            EventSubscription::local(SubjectFilter::new("world.*")),
            move |event: EventEnvelope| {
                let sink = sink.clone();
                async move {
                    sink.lock().await.push(event.event_type);
                    Ok(())
                }
            },
        )
        .await
        .unwrap();
    let publisher = EventPublisher::new(bus, service_kind!("World"), InstanceId::new("world-a"));
    publisher
        .publish_bytes(
            Subject::new("world.login"),
            "login",
            vec![1],
            TraceContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(&*observed.lock().await, &["login"]);
}
