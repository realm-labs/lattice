use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use lattice_core::actor_ref::{
    ActivationId, ActorPath, ActorRef, ClusterId, ConfigFingerprint, EntityId, EntityRef,
    EntityType, NodeAddress, NodeIncarnation, ProtocolId, ProtocolTag, SingletonKind, SingletonRef,
};
use prost::Message;
use thiserror::Error;
use tokio::sync::oneshot;

use crate::association::{Association, AssociationError, AssociationId};
use crate::protocol::{CatalogueDecision, ProtocolFingerprint};
use crate::transport::{FramedConnection, RemotingIo};
use crate::wire::{Frame, FrameKind};

pub mod codec;
pub mod error;
pub mod inbound;
pub mod outbound;
pub mod target;

#[cfg(test)]
mod tests;
