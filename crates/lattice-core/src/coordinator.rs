use serde::{Deserialize, Serialize};

use crate::actor_ref::PlacementDomainId;

/// Independently elected control-plane scope.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CoordinatorScope {
    Membership,
    Placement(PlacementDomainId),
}
