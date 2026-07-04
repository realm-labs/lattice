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
    pub fn from_env() -> Result<Self, crate::ConfigError> {
        crate::ConfigSource::env("LATTICE")
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{actor_kind, service_kind};

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
        assert_eq!(WorldId::try_from_actor_id(&ActorId::U64(42)), Ok(id));
        assert!(WorldId::try_from_actor_id(&ActorId::Str("42".into())).is_err());
    }
}
