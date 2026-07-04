use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;

use http::Uri;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ServiceKind(Cow<'static, str>);

impl ServiceKind {
    pub const fn from_static(value: &'static str) -> Self {
        Self(Cow::Borrowed(value))
    }

    pub fn new(value: impl Into<String>) -> Self {
        Self(Cow::Owned(value.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ServiceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ActorKind(Cow<'static, str>);

impl ActorKind {
    pub const fn from_static(value: &'static str) -> Self {
        Self(Cow::Borrowed(value))
    }

    pub fn new(value: impl Into<String>) -> Self {
        Self(Cow::Owned(value.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ActorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct InstanceId(String);

impl InstanceId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for InstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Epoch(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RequestId(String);

impl RequestId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TraceContext {
    #[serde(default)]
    pub traceparent: Option<String>,
    #[serde(default)]
    pub tracestate: Option<String>,
}

impl TraceContext {
    pub fn is_empty(&self) -> bool {
        self.traceparent.is_none() && self.tracestate.is_none()
    }

    pub fn span(&self, name: &'static str, kind: TraceSpanKind) -> tracing::Span {
        tracing::info_span!(
            "lattice.trace_context",
            otel.name = name,
            otel.kind = kind.as_str(),
            traceparent = self.traceparent.as_deref().unwrap_or(""),
            tracestate = self.tracestate.as_deref().unwrap_or("")
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceSpanKind {
    Internal,
    Client,
    Server,
    Producer,
    Consumer,
}

impl TraceSpanKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Internal => "internal",
            Self::Client => "client",
            Self::Server => "server",
            Self::Producer => "producer",
            Self::Consumer => "consumer",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum RouteKey {
    Str(String),
    U64(u64),
    I64(i64),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ActorId {
    Str(String),
    U64(u64),
    I64(i64),
    Bytes(Vec<u8>),
}

impl ActorId {
    pub fn to_route_key(&self) -> RouteKey {
        match self {
            Self::Str(value) => RouteKey::Str(value.clone()),
            Self::U64(value) => RouteKey::U64(*value),
            Self::I64(value) => RouteKey::I64(*value),
            Self::Bytes(value) => RouteKey::Bytes(value.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorRef {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub actor_id: ActorId,
    pub target: ActorRefTarget,
}

impl ActorRef {
    pub fn direct(
        service_kind: ServiceKind,
        actor_kind: ActorKind,
        actor_id: ActorId,
        instance_id: InstanceId,
        endpoint: Uri,
        owner_epoch: Option<Epoch>,
    ) -> Self {
        Self {
            service_kind,
            actor_kind,
            actor_id,
            target: ActorRefTarget::Direct {
                instance_id,
                endpoint,
                owner_epoch,
            },
        }
    }

    pub fn routed(service_kind: ServiceKind, actor_kind: ActorKind, actor_id: ActorId) -> Self {
        Self {
            service_kind,
            actor_kind,
            actor_id,
            target: ActorRefTarget::Routed,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActorRefTarget {
    Direct {
        instance_id: InstanceId,
        #[serde(with = "uri_serde")]
        endpoint: Uri,
        #[serde(default)]
        owner_epoch: Option<Epoch>,
    },
    Routed,
}

pub trait ActorKey: Clone + Send + Sync + 'static {
    fn to_route_key(&self) -> RouteKey;
    fn to_actor_id(&self) -> ActorId;
    fn try_from_actor_id(actor_id: &ActorId) -> Result<Self, ActorKeyDecodeError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("failed to decode actor key: {reason}")]
pub struct ActorKeyDecodeError {
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceConfig {
    pub instance_id: InstanceId,
    #[serde(with = "uri_serde")]
    pub advertised_endpoint: Uri,
    #[serde(with = "uri_serde")]
    pub control_endpoint: Uri,
    pub version: String,
    #[serde(default)]
    pub capacity: InstanceCapacity,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

impl InstanceConfig {
    pub fn from_env() -> Result<Self, lattice_config::ConfigError> {
        lattice_config::ConfigSource::env("LATTICE")
            .load()?
            .section("instance")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceCapacity {
    #[serde(default)]
    pub max_actors: Option<u64>,
    #[serde(default)]
    pub max_connections: Option<u64>,
}

mod uri_serde {
    use std::str::FromStr;

    use http::Uri;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Uri, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Uri, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Uri::from_str(&value).map_err(serde::de::Error::custom)
    }
}

#[macro_export]
macro_rules! actor_kind {
    ($name:literal) => {
        $crate::ActorKind::from_static($name)
    };
}

#[macro_export]
macro_rules! service_kind {
    ($name:literal) => {
        $crate::ServiceKind::from_static($name)
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    const WORLD_SERVICE: ServiceKind = service_kind!("World");
    const WORLD_ACTOR: ActorKind = actor_kind!("World");

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct WorldId(u64);

    impl ActorKey for WorldId {
        fn to_route_key(&self) -> RouteKey {
            RouteKey::U64(self.0)
        }

        fn to_actor_id(&self) -> ActorId {
            ActorId::U64(self.0)
        }

        fn try_from_actor_id(actor_id: &ActorId) -> Result<Self, ActorKeyDecodeError> {
            match actor_id {
                ActorId::U64(value) => Ok(Self(*value)),
                _ => Err(ActorKeyDecodeError {
                    reason: "expected u64 actor id for WorldId".to_string(),
                }),
            }
        }
    }

    #[test]
    fn actor_kind_and_service_kind_macros_are_const() {
        assert_eq!(WORLD_SERVICE.as_str(), "World");
        assert_eq!(WORLD_ACTOR.as_str(), "World");
    }

    #[test]
    fn actor_key_converts_through_framework_ids() {
        let id = WorldId(42);

        assert_eq!(id.to_route_key(), RouteKey::U64(42));
        assert_eq!(id.to_actor_id(), ActorId::U64(42));
        assert_eq!(id.to_actor_id().to_route_key(), RouteKey::U64(42));
        assert_eq!(WorldId::try_from_actor_id(&ActorId::U64(42)), Ok(id));
        assert!(WorldId::try_from_actor_id(&ActorId::Str("42".into())).is_err());
    }

    #[test]
    fn actor_ref_models_direct_and_routed_targets() {
        let direct = ActorRef::direct(
            service_kind!("Gateway"),
            actor_kind!("GatewaySession"),
            ActorId::Str("session-1".into()),
            InstanceId::new("gateway-a"),
            "http://127.0.0.1:19083".parse().unwrap(),
            Some(Epoch(7)),
        );
        let routed = ActorRef::routed(WORLD_SERVICE, WORLD_ACTOR, ActorId::U64(42));

        assert!(matches!(direct.target, ActorRefTarget::Direct { .. }));
        assert_eq!(
            direct.actor_id.to_route_key(),
            RouteKey::Str("session-1".into())
        );
        assert_eq!(routed.actor_id.to_route_key(), RouteKey::U64(42));
        assert_eq!(routed.target, ActorRefTarget::Routed);
    }

    #[test]
    fn trace_context_reports_empty_and_span_kind_names() {
        let empty = TraceContext::default();
        let trace = TraceContext {
            traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00".into()),
            tracestate: None,
        };

        assert!(empty.is_empty());
        assert!(!trace.is_empty());
        assert_eq!(TraceSpanKind::Client.as_str(), "client");
        assert_eq!(TraceSpanKind::Consumer.as_str(), "consumer");
        let _span = trace.span("rpc.client", TraceSpanKind::Client);
    }
}
