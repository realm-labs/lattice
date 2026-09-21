use serde::{Deserialize, Serialize};

use crate::primitives::PlacementDomainId;

/// Independently elected control-plane scope.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CoordinatorScope {
    Membership,
    Placement(PlacementDomainId),
}
