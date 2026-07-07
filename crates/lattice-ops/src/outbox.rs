use std::collections::HashMap;
use std::sync::Arc;

use serde::Serialize;
use tokio::sync::Mutex;

use crate::error::OpsError;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct OutboxEventId(String);

impl OutboxEventId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OutboxEvent {
    pub event_id: OutboxEventId,
    pub topic: String,
    pub payload: serde_json::Value,
    pub published: bool,
}

#[derive(Debug, Default, Clone)]
pub struct TransactionalOutbox {
    events: Arc<Mutex<HashMap<OutboxEventId, OutboxEvent>>>,
}

impl TransactionalOutbox {
    pub async fn enqueue(&self, event: OutboxEvent) -> Result<(), OpsError> {
        let mut events = self.events.lock().await;
        if events.contains_key(&event.event_id) {
            return Err(OpsError::DuplicateOutboxEvent);
        }
        events.insert(event.event_id.clone(), event);
        Ok(())
    }

    pub async fn unpublished(&self) -> Vec<OutboxEvent> {
        self.events
            .lock()
            .await
            .values()
            .filter(|event| !event.published)
            .cloned()
            .collect()
    }

    pub async fn mark_published(&self, event_id: &OutboxEventId) -> Result<(), OpsError> {
        let mut events = self.events.lock().await;
        let event = events
            .get_mut(event_id)
            .ok_or(OpsError::UnknownOutboxEvent)?;
        event.published = true;
        Ok(())
    }
}
