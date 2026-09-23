//! Cluster identity, placement, coordination, and release models.

pub use crate::coordination::CoordinatorScope;
pub use crate::primitives::{
    ClusterId, ConfigFingerprint, EntityType, NodeEndpoint, NodeIncarnation, PlacementDomainId,
    SingletonKind,
};
pub use crate::release_model::{
    ClusterReleaseState, ReleaseCompatibility, ReleaseError, ReleaseId, ReleaseManifest,
};
