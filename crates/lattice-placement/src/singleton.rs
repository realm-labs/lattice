use std::collections::VecDeque;

use bytes::Bytes;
use lattice_core::actor_ref::{
    ClusterId, ConfigFingerprint, PlacementDomainId, ProtocolId, ProtocolTag, ReferenceError,
    SingletonKind, SingletonRef,
};
use thiserror::Error;

use crate::authority::{AuthorityEffect, AuthorityError, AuthorityEvent, PlacementAuthority};
use crate::region::BufferedMessageMode;
use crate::types::{MonotonicTime, NodeKey};

pub struct SingletonManager {
    pub domain: PlacementDomainId,
    pub kind: SingletonKind,
    authority: PlacementAuthority,
}

impl SingletonManager {
    pub fn new(
        domain: PlacementDomainId,
        kind: SingletonKind,
        local_node: NodeKey,
        safety_margin: std::time::Duration,
    ) -> Result<Self, SingletonError> {
        Ok(Self {
            domain,
            kind,
            authority: PlacementAuthority::new(local_node, safety_margin)
                .map_err(SingletonError::Authority)?,
        })
    }

    pub fn transition(
        &mut self,
        event: AuthorityEvent,
    ) -> Result<Vec<AuthorityEffect>, SingletonError> {
        self.authority
            .transition(event)
            .map_err(SingletonError::Authority)
    }

    pub fn accepts_messages(&self) -> bool {
        self.authority.admission_open()
    }
}

#[derive(Debug, Clone)]
pub struct SingletonProxyConfig {
    pub maximum_buffered_messages: usize,
    pub maximum_buffered_bytes: usize,
    pub maximum_buffer_age_millis: u64,
}

impl Default for SingletonProxyConfig {
    fn default() -> Self {
        Self {
            maximum_buffered_messages: 1024,
            maximum_buffered_bytes: 4 * 1024 * 1024,
            maximum_buffer_age_millis: 5_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonBufferedMessage {
    pub message_id: u64,
    pub mode: BufferedMessageMode,
    pub payload: Bytes,
    pub expires_at: MonotonicTime,
}

pub struct SingletonProxy {
    domain: PlacementDomainId,
    kind: SingletonKind,
    protocol_id: ProtocolId,
    fingerprint: ConfigFingerprint,
    config: SingletonProxyConfig,
    owner: Option<NodeKey>,
    buffer: VecDeque<SingletonBufferedMessage>,
    buffered_bytes: usize,
}

impl SingletonProxy {
    pub fn new(
        domain: PlacementDomainId,
        kind: SingletonKind,
        protocol_id: ProtocolId,
        fingerprint: ConfigFingerprint,
        config: SingletonProxyConfig,
    ) -> Result<Self, SingletonError> {
        if config.maximum_buffered_messages == 0
            || config.maximum_buffered_bytes == 0
            || config.maximum_buffer_age_millis == 0
        {
            return Err(SingletonError::ZeroLimit);
        }
        Ok(Self {
            domain,
            kind,
            protocol_id,
            fingerprint,
            config,
            owner: None,
            buffer: VecDeque::new(),
            buffered_bytes: 0,
        })
    }

    pub fn singleton_ref<P: ProtocolTag>(
        &self,
        cluster_id: ClusterId,
    ) -> Result<SingletonRef<P>, ReferenceError> {
        SingletonRef::new(
            cluster_id,
            self.domain.clone(),
            self.kind.clone(),
            self.protocol_id,
            self.fingerprint,
        )?
        .try_typed()
    }

    pub fn update_owner(&mut self, owner: Option<NodeKey>) -> Vec<SingletonBufferedMessage> {
        self.owner = owner;
        if self.owner.is_some() {
            self.buffered_bytes = 0;
            self.buffer.drain(..).collect()
        } else {
            Vec::new()
        }
    }

    pub fn route_or_buffer(
        &mut self,
        message_id: u64,
        mode: BufferedMessageMode,
        payload: Bytes,
        now: MonotonicTime,
    ) -> Result<Option<NodeKey>, SingletonError> {
        if message_id == 0 {
            return Err(SingletonError::InvalidMessage);
        }
        if let Some(owner) = &self.owner {
            return Ok(Some(owner.clone()));
        }
        while self
            .buffer
            .front()
            .is_some_and(|message| message.expires_at <= now)
        {
            if let Some(expired) = self.buffer.pop_front() {
                self.buffered_bytes = self.buffered_bytes.saturating_sub(expired.payload.len());
            }
        }
        if self.buffer.len() == self.config.maximum_buffered_messages
            || self.buffered_bytes.saturating_add(payload.len())
                > self.config.maximum_buffered_bytes
        {
            return Err(SingletonError::Unavailable);
        }
        let residence_deadline = now
            .checked_add(std::time::Duration::from_millis(
                self.config.maximum_buffer_age_millis,
            ))
            .ok_or(SingletonError::InvalidTime)?;
        let expires_at = match mode {
            BufferedMessageMode::Tell => residence_deadline,
            BufferedMessageMode::Ask { deadline } => deadline.min(residence_deadline),
        };
        if expires_at <= now {
            return Err(SingletonError::MessageExpired);
        }
        self.buffered_bytes += payload.len();
        self.buffer.push_back(SingletonBufferedMessage {
            message_id,
            mode,
            payload,
            expires_at,
        });
        Ok(None)
    }
}

#[derive(Debug, Error)]
pub enum SingletonError {
    #[error("singleton placement authority rejected a transition")]
    Authority(#[source] AuthorityError),
    #[error("singleton proxy limits must be nonzero")]
    ZeroLimit,
    #[error("singleton owner is unavailable and its bounded buffer is full")]
    Unavailable,
    #[error("singleton buffer deadline cannot be represented")]
    InvalidTime,
    #[error("singleton buffered message ID is zero")]
    InvalidMessage,
    #[error("singleton message deadline elapsed before buffering")]
    MessageExpired,
}
