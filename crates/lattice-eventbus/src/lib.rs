pub mod error;
pub mod local;
pub mod nats;
pub mod publisher;
pub mod types;

pub use error::EventBusError;
pub use local::{EventBus, EventHandler, EventSubscriptionHandle, LocalEventBus};
pub use publisher::{EventPublisher, ServiceEvents};
pub use types::{EventEnvelope, EventId, EventSubscription, Subject, SubjectFilter};

#[cfg(test)]
mod tests;
