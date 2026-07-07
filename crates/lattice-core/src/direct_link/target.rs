use http::Uri;
use serde::{Deserialize, Serialize};

use crate::actor_ref::ActorRef;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectLinkEndpoint {
    #[serde(with = "crate::uri_serde")]
    pub uri: Uri,
}

impl DirectLinkEndpoint {
    pub fn new(uri: Uri) -> Self {
        Self { uri }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LinkTarget {
    Actor(ActorRef),
    Endpoint {
        endpoint: DirectLinkEndpoint,
        target: ActorRef,
    },
}

impl From<ActorRef> for LinkTarget {
    fn from(value: ActorRef) -> Self {
        Self::Actor(value)
    }
}
