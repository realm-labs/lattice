//! Cluster identity, placement, and coordination models.

pub use crate::coordination::CoordinatorScope;
pub use crate::primitives::{
    ClusterId, ConfigFingerprint, EntityType, NodeEndpoint, NodeIncarnation, PlacementDomainId,
    SingletonKind,
};
