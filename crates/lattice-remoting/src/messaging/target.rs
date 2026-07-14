use super::error::RemoteFailureCode;
use super::{
    ActivationId, ActorPath, ActorRef, Bytes, ClusterId, Duration, EntityRef, NodeAddress,
    NodeIncarnation, ProtocolId, SingletonRef,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SenderIdentity {
    Actor(ActorRef),
    Process(u128),
}

impl SenderIdentity {
    pub(super) fn stable_bytes(&self) -> Vec<u8> {
        match self {
            Self::Actor(reference) => ExactActorTarget::from(reference).stable_bytes(),
            Self::Process(value) => value.to_be_bytes().to_vec(),
        }
    }

    pub(super) fn actor_ref(&self) -> Option<&ActorRef> {
        match self {
            Self::Actor(reference) => Some(reference),
            Self::Process(_) => None,
        }
    }
}

impl<A: lattice_core::actor_ref::ProtocolTag> From<&ActorRef<A>> for SenderIdentity {
    fn from(value: &ActorRef<A>) -> Self {
        Self::Actor(value.erase())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ExactActorTarget {
    pub cluster_id: ClusterId,
    pub node_address: NodeAddress,
    pub node_incarnation: NodeIncarnation,
    pub actor_path: ActorPath,
    pub activation_id: ActivationId,
    pub protocol_id: ProtocolId,
}

impl<A: lattice_core::actor_ref::ProtocolTag> From<&ActorRef<A>> for ExactActorTarget {
    fn from(value: &ActorRef<A>) -> Self {
        Self {
            cluster_id: value.cluster_id().clone(),
            node_address: value.node_address().clone(),
            node_incarnation: value.node_incarnation(),
            actor_path: value.actor_path().clone(),
            activation_id: value.activation_id(),
            protocol_id: value.protocol_id(),
        }
    }
}

impl ExactActorTarget {
    pub(super) fn stable_bytes(&self) -> Vec<u8> {
        format!(
            "{}:{}:{}:{}:{}",
            self.node_address,
            self.node_incarnation.get(),
            self.actor_path,
            self.activation_id.local_sequence(),
            self.protocol_id.get()
        )
        .into_bytes()
    }

    pub fn actor_ref<A: lattice_core::actor_ref::ProtocolTag>(
        &self,
    ) -> Result<ActorRef<A>, lattice_core::actor_ref::ReferenceError> {
        ActorRef::new(
            self.cluster_id.clone(),
            self.node_address.clone(),
            self.node_incarnation,
            self.actor_path.clone(),
            self.activation_id,
            self.protocol_id,
        )?
        .try_typed()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CorrelationId {
    caller_incarnation: u128,
    sequence: u64,
}

impl CorrelationId {
    pub const fn new(caller_incarnation: u128, sequence: u64) -> Option<Self> {
        if caller_incarnation == 0 || sequence == 0 {
            None
        } else {
            Some(Self {
                caller_incarnation,
                sequence,
            })
        }
    }

    pub const fn caller_incarnation(self) -> u128 {
        self.caller_incarnation
    }

    pub const fn sequence(self) -> u64 {
        self.sequence
    }

    pub(super) fn to_bytes(self) -> [u8; 24] {
        let mut bytes = [0_u8; 24];
        bytes[..16].copy_from_slice(&self.caller_incarnation.to_be_bytes());
        bytes[16..].copy_from_slice(&self.sequence.to_be_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 24 {
            return None;
        }
        let caller_incarnation = u128::from_be_bytes(bytes[..16].try_into().ok()?);
        let sequence = u64::from_be_bytes(bytes[16..].try_into().ok()?);
        Self::new(caller_incarnation, sequence)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundTell {
    pub sender: Option<ActorRef>,
    pub target: ExactActorTarget,
    pub message_id: u64,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundAsk {
    pub target: ExactActorTarget,
    pub correlation_id: CorrelationId,
    pub timeout_budget: Duration,
    pub message_id: u64,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalEntityTarget {
    pub reference: EntityRef,
    pub owner_address: NodeAddress,
    pub owner_incarnation: NodeIncarnation,
    pub assignment_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalSingletonTarget {
    pub reference: SingletonRef,
    pub owner_address: NodeAddress,
    pub owner_incarnation: NodeIncarnation,
    pub assignment_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundEntityTell {
    pub sender: Option<ActorRef>,
    pub target: LogicalEntityTarget,
    pub message_id: u64,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundEntityAsk {
    pub target: LogicalEntityTarget,
    pub correlation_id: CorrelationId,
    pub timeout_budget: Duration,
    pub message_id: u64,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundSingletonTell {
    pub sender: Option<ActorRef>,
    pub target: LogicalSingletonTarget,
    pub message_id: u64,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundSingletonAsk {
    pub target: LogicalSingletonTarget,
    pub correlation_id: CorrelationId,
    pub timeout_budget: Duration,
    pub message_id: u64,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteFailure {
    pub correlation_id: CorrelationId,
    pub code: RemoteFailureCode,
    pub safe_detail: Option<String>,
}
