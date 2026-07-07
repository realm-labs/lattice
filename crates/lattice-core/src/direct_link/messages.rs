use std::collections::BTreeSet;
use std::time::Instant;

use crate::ActorRef;

use super::{
    BackpressurePolicy, DirectLinkMessageId, DirectLinkMode, LinkCloseReason, LinkDirection,
    LinkId, LinkSequence,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkMessageFlags {
    bits: u32,
}

impl LinkMessageFlags {
    pub const EMPTY: Self = Self { bits: 0 };

    pub fn from_bits(bits: u32) -> Self {
        Self { bits }
    }

    pub fn bits(&self) -> u32 {
        self.bits
    }
}

impl Default for LinkMessageFlags {
    fn default() -> Self {
        Self::EMPTY
    }
}

#[derive(Debug, Clone)]
pub struct LinkMessageContext {
    pub link_id: LinkId,
    pub source: ActorRef,
    pub target: ActorRef,
    pub sequence: u64,
    pub received_at: Instant,
    pub flags: LinkMessageFlags,
}

#[derive(Debug, Clone)]
pub struct Linked<T, M = ()> {
    pub payload: T,
    pub metadata: M,
    pub context: LinkMessageContext,
}

#[derive(Debug, Clone)]
pub struct LinkOpened {
    pub link_id: LinkId,
    pub source: ActorRef,
    pub target: ActorRef,
    pub mode: DirectLinkMode,
    pub inbound_stream: String,
    pub inbound_accepted_message_types: BTreeSet<DirectLinkMessageId>,
    pub outbound_stream: Option<String>,
    pub outbound_accepted_message_types: BTreeSet<DirectLinkMessageId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkDirectionClosed {
    pub link_id: LinkId,
    pub direction: LinkDirection,
    pub stream: String,
    pub reason: LinkCloseReason,
    pub last_sequence_seen: Option<LinkSequence>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkClosed {
    pub link_id: LinkId,
    pub reason: LinkCloseReason,
    pub closed_directions: BTreeSet<LinkDirection>,
    pub last_sequence_seen: Option<LinkSequence>,
}

#[derive(Debug, Clone)]
pub struct LinkBackpressure {
    pub link_id: LinkId,
    pub policy: BackpressurePolicy,
    pub pending: usize,
    pub dropped: u64,
    pub coalesced: u64,
}

#[derive(Debug, Clone)]
pub struct LinkProtocolError {
    pub link_id: LinkId,
    pub error: String,
    pub close_action: LinkCloseReason,
}
