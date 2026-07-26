use base64::engine::general_purpose::STANDARD as BASE64;
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
    #[serde(with = "base64_payload")]
    pub payload: Vec<u8>,
}

mod base64_payload {
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S>(payload: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&super::BASE64.encode(payload))
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        super::BASE64
            .decode(encoded)
            .map_err(serde::de::Error::custom)
    }
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
        matches_subject(&self.0, &subject.0)
    }
}

fn matches_subject(filter: &str, subject: &str) -> bool {
    if filter.is_empty() || subject.is_empty() {
        return filter == subject;
    }

    let mut filter_tokens = filter.split('.');
    let mut subject_tokens = subject.split('.');
    loop {
        match (filter_tokens.next(), subject_tokens.next()) {
            (Some(MULTI_TOKEN_WILDCARD), Some(_)) => return filter_tokens.next().is_none(),
            (Some(TOKEN_WILDCARD), Some(_)) => {}
            (Some(filter_token), Some(subject_token)) if filter_token == subject_token => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

const TOKEN_WILDCARD: &str = "*";
const MULTI_TOKEN_WILDCARD: &str = ">";

#[cfg(test)]
mod tests {
    use super::*;

    fn matches(filter: &str, subject: &str) -> bool {
        SubjectFilter::new(filter).matches(&Subject::new(subject))
    }

    #[test]
    fn exact_filter_matches_only_the_same_subject() {
        assert!(matches("game.world.entered", "game.world.entered"));
        assert!(!matches("game.world.entered", "game.world.left"));
        assert!(!matches("game.world", "game.world.entered"));
        assert!(!matches("game.world.entered", "game.world"));
    }

    #[test]
    fn token_wildcard_matches_exactly_one_token() {
        assert!(matches("game.*", "game.world"));
        assert!(matches("game.*.entered", "game.world.entered"));
        assert!(!matches("game.*", "game.world.entered"));
        assert!(!matches("game.*", "game"));
        assert!(!matches("*.entered", "game.world.entered"));
        assert!(matches("*.*", "game.world"));
    }

    #[test]
    fn multi_token_wildcard_matches_one_or_more_trailing_tokens() {
        assert!(matches("game.>", "game.world"));
        assert!(matches("game.>", "game.world.entered"));
        assert!(!matches("game.>", "game"));
        assert!(!matches("game.>", "lobby.world"));
        assert!(matches(">", "game"));
        assert!(matches(">", "game.world.entered"));
    }

    #[test]
    fn mixed_wildcards_follow_token_positions() {
        assert!(matches("game.*.>", "game.world.entered"));
        assert!(matches("game.*.>", "game.world.player.entered"));
        assert!(!matches("game.*.>", "game.world"));
    }

    #[test]
    fn multi_token_wildcard_is_only_a_wildcard_in_trailing_position() {
        assert!(!matches("game.>.entered", "game.world.entered"));
        assert!(!matches("game.>.entered", "game.world.player.entered"));
    }

    #[test]
    fn empty_filter_matches_only_the_empty_subject() {
        assert!(matches("", ""));
        assert!(!matches("", "game"));
        assert!(!matches(">", ""));
        assert!(!matches("game", ""));
    }

    #[test]
    fn envelope_payload_round_trips_as_base64() {
        let envelope = EventEnvelope {
            event_id: EventId::new("event-1"),
            subject: Subject::new("game.world.entered"),
            event_type: "WorldEntered".to_string(),
            source_service: lattice_core::service_kind!("World"),
            source_instance: InstanceId::new("world-a"),
            recipient: None,
            correlation_id: None,
            trace: TraceContext::default(),
            occurred_unix_ms: 1,
            payload: vec![0, 1, 2, 250, 251],
        };

        let encoded = serde_json::to_string(&envelope).unwrap();

        assert!(encoded.contains("\"payload\":\"AAEC+vs=\""));
        assert_eq!(
            serde_json::from_str::<EventEnvelope>(&encoded).unwrap(),
            envelope
        );
    }
}
