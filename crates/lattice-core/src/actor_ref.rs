use http::Uri;
use serde::{Deserialize, Serialize};

use crate::id::ActorId;
use crate::instance::InstanceId;
use crate::kind::{ActorKind, ServiceKind};
use crate::uri_serde;

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

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
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
