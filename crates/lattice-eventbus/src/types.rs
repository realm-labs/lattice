use lattice_core::{ActorId, ActorKind, InstanceId, RequestId, ServiceKind, TraceContext};

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

    pub fn durable(filter: SubjectFilter, durable_name: impl Into<String>) -> Self {
        Self {
            filter,
            durable_name: Some(durable_name.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectFilter(String);

impl SubjectFilter {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
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
