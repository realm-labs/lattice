use serde::{Deserialize, Serialize};
use thiserror::Error;

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
