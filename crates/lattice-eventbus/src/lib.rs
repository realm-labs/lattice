mod error;
mod local;
mod nats;
mod publisher;
mod types;

pub use error::EventBusError;
pub use local::{EventBus, EventHandler, EventSubscriptionHandle, LocalEventBus};
pub use nats::{InMemoryNatsClient, NatsEventBus, NatsEventBusConfig};
pub use publisher::{EventPublisher, ServiceEvents};
pub use types::{EventEnvelope, EventId, EventSubscription, Subject, SubjectFilter};

#[cfg(test)]
mod tests;
