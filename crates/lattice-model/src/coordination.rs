use serde::{Deserialize, Serialize};

use crate::primitives::ActorGroupId;

/// Independently elected control-plane scope.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CoordinatorScope {
    Cluster,
    Group(ActorGroupId),
}

#[cfg(test)]
mod tests {
    use super::{ActorGroupId, CoordinatorScope};

    #[test]
    fn scope_names_round_trip() {
        assert_eq!(
            serde_json::to_string(&CoordinatorScope::Cluster).unwrap(),
            "\"Cluster\""
        );
        let group = CoordinatorScope::Group(ActorGroupId::new("gameplay").unwrap());
        let encoded = serde_json::to_string(&group).unwrap();
        assert_eq!(encoded, "{\"Group\":\"gameplay\"}");
        assert_eq!(
            serde_json::from_str::<CoordinatorScope>(&encoded).unwrap(),
            group
        );
    }
}
