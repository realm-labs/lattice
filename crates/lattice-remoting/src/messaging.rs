use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use lattice_model::actor::{
    ActivationId, ActorAddress, ActorPath, EntityAddress, ProtocolId, ProtocolTag, SingletonAddress,
};
use lattice_model::cluster::{
    ClusterId, ConfigFingerprint, EntityId, EntityType, NodeEndpoint, NodeIncarnation,
    PlacementDomainId, SingletonKind,
};
use prost::Message;
use thiserror::Error;
use tokio::sync::oneshot;

use crate::association::{Association, AssociationError, AssociationId};
use crate::protocol::{CatalogueDecision, ProtocolFingerprint};

use crate::wire::{Frame, FrameKind};

pub mod codec;
mod encode;
pub mod error;
pub mod inbound;
pub mod outbound;
pub mod target;
pub(crate) mod target_cache;
pub(crate) mod target_dictionary;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod test_support;
