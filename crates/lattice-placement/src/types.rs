use std::collections::BTreeSet;
use std::time::Duration;

use lattice_core::actor_ref::{
    ConfigFingerprint, EntityType, NodeAddress, NodeIncarnation, SingletonKind,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

macro_rules! nonzero_counter {
    ($name:ident, $field:literal) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(u64);

        impl $name {
            pub fn new(value: u64) -> Result<Self, PlacementTypeError> {
                if value == 0 {
                    Err(PlacementTypeError::Zero($field))
                } else {
                    Ok(Self(value))
                }
            }

            pub const fn get(self) -> u64 {
                self.0
            }

            pub fn next(self) -> Result<Self, PlacementTypeError> {
                self.0
                    .checked_add(1)
                    .ok_or(PlacementTypeError::Exhausted($field))
                    .map(Self)
            }
        }
    };
}

nonzero_counter!(CoordinatorTerm, "Coordinator term");
nonzero_counter!(Revision, "Coordinator revision");
nonzero_counter!(PlanRevision, "rebalance plan record revision");
nonzero_counter!(AssignmentGeneration, "assignment generation");
nonzero_counter!(GrantSequence, "grant sequence");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StateVersion {
    pub term: CoordinatorTerm,
    pub revision: Revision,
}

impl StateVersion {
    pub const fn new(term: CoordinatorTerm, revision: Revision) -> Self {
        Self { term, revision }
    }

    pub fn next_revision(self) -> Result<Self, PlacementTypeError> {
        Ok(Self {
            term: self.term,
            revision: self.revision.next()?,
        })
    }

    pub fn accepts_delta_after(self, next: Self) -> bool {
        next.term == self.term && next.revision.get() == self.revision.get().saturating_add(1)
    }

    pub fn satisfies(self, barrier: Self) -> bool {
        self.term == barrier.term && self.revision >= barrier.revision
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ShardId(u32);

impl ShardId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MonotonicTime(u64);

impl MonotonicTime {
    pub const fn from_millis(value: u64) -> Self {
        Self(value)
    }

    pub const fn as_millis(self) -> u64 {
        self.0
    }

    pub fn checked_add(self, duration: Duration) -> Option<Self> {
        let millis = u64::try_from(duration.as_millis()).ok()?;
        self.0.checked_add(millis).map(Self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeKey {
    pub node_id: String,
    pub address: NodeAddress,
    pub incarnation: NodeIncarnation,
}

impl NodeKey {
    pub fn validate(&self) -> Result<(), PlacementTypeError> {
        if self.node_id.is_empty()
            || self.node_id.len() > 128
            || self.node_id.contains(['/', '\\'])
            || self.node_id.chars().any(char::is_control)
        {
            return Err(PlacementTypeError::InvalidNodeId);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PlacementSlotKey {
    Shard {
        entity_type: EntityType,
        shard_id: ShardId,
    },
    Singleton(SingletonKind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementSlotState {
    Unallocated,
    Allocating,
    Running,
    BeginHandoff,
    Stopping,
    StopFailed,
    Fenced,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementSlot {
    pub key: PlacementSlotKey,
    pub config_fingerprint: ConfigFingerprint,
    pub owner: Option<NodeKey>,
    pub target: Option<NodeKey>,
    pub assignment_generation: AssignmentGeneration,
    pub version: StateVersion,
    pub state: PlacementSlotState,
    pub active_move: Option<u128>,
    #[serde(default)]
    pub barrier_sessions: BTreeSet<NodeIncarnation>,
}

impl PlacementSlot {
    pub fn validate(&self) -> Result<(), PlacementTypeError> {
        if let Some(owner) = &self.owner {
            owner.validate()?;
        }
        if let Some(target) = &self.target {
            target.validate()?;
        }
        if (!self.barrier_sessions.is_empty() && self.active_move.is_none())
            || (self.active_move.is_some()
                && !matches!(
                    self.state,
                    PlacementSlotState::BeginHandoff
                        | PlacementSlotState::Stopping
                        | PlacementSlotState::StopFailed
                        | PlacementSlotState::Fenced
                        | PlacementSlotState::Allocating
                ))
        {
            return Err(PlacementTypeError::InvalidSlotState);
        }
        match self.state {
            PlacementSlotState::Unallocated if self.owner.is_some() => {
                Err(PlacementTypeError::InvalidSlotState)
            }
            PlacementSlotState::BeginHandoff if self.owner.is_none() || self.target.is_none() => {
                Err(PlacementTypeError::InvalidSlotState)
            }
            PlacementSlotState::Running
            | PlacementSlotState::Stopping
            | PlacementSlotState::StopFailed
            | PlacementSlotState::Fenced
                if self.owner.is_none() =>
            {
                Err(PlacementTypeError::InvalidSlotState)
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimGrant {
    pub slot: PlacementSlotKey,
    pub owner: NodeKey,
    pub coordinator_term: CoordinatorTerm,
    pub assignment_generation: AssignmentGeneration,
    pub grant_sequence: GrantSequence,
    pub ttl: Duration,
}

impl ClaimGrant {
    pub fn validate(&self, safety_margin: Duration) -> Result<(), PlacementTypeError> {
        self.owner.validate()?;
        if self.ttl.is_zero() || self.ttl <= safety_margin {
            return Err(PlacementTypeError::InvalidClaimTtl);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PlacementTypeError {
    #[error("{0} must be nonzero")]
    Zero(&'static str),
    #[error("{0} is exhausted")]
    Exhausted(&'static str),
    #[error("node ID is not canonical")]
    InvalidNodeId,
    #[error("placement slot fields do not match its state")]
    InvalidSlotState,
    #[error("claim TTL must exceed the nonzero safety margin")]
    InvalidClaimTtl,
}
