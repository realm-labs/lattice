use lattice_core::actor_ref::RecipientRef;
use lattice_core::instance::InstanceId;
use lattice_core::kind::ServiceKind;
use lattice_core::trace::TraceContext;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Subject(String);

impl Subject {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventId(String);

impl EventId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: EventId,
    pub subject: Subject,
    pub event_type: String,
    pub source_service: ServiceKind,
    pub source_instance: InstanceId,
    pub recipient: Option<RecipientRef>,
    pub correlation_id: Option<String>,
    pub trace: TraceContext,
    pub occurred_unix_ms: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

    pub fn durable(filter: SubjectFilter, durable_name: impl Into<String>) -> Self {
        Self {
            filter,
            durable_name: Some(durable_name.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SubjectFilter(String);

impl SubjectFilter {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn matches(&self, subject: &Subject) -> bool {
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
