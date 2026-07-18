use std::time::Duration;

use lattice_core::coordinator::CoordinatorScope;

use crate::storage::ScopedElectionStore;
use crate::types::{CoordinatorTerm, NodeKey};

use super::super::CoordinatorRuntimeError;

pub(super) async fn candidate_delay(scope: &CoordinatorScope, node: &NodeKey, maximum: Duration) {
    let delay = candidate_delay_duration(scope, node, maximum);
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
}

pub(super) fn candidate_delay_duration(
    scope: &CoordinatorScope,
    node: &NodeKey,
    maximum: Duration,
) -> Duration {
    let maximum_millis = u64::try_from(maximum.as_millis()).unwrap_or(u64::MAX);
    if maximum_millis == 0 {
        return Duration::ZERO;
    }
    let mut input = Vec::new();
    match scope {
        CoordinatorScope::Membership => input.extend_from_slice(b"membership"),
        CoordinatorScope::Placement(domain) => {
            input.extend_from_slice(b"placement/");
            input.extend_from_slice(domain.as_str().as_bytes());
        }
    }
    input.push(0);
    input.extend_from_slice(node.node_id.as_bytes());
    input.extend_from_slice(&node.incarnation.get().to_be_bytes());
    let digest = blake3::hash(&input);
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(&digest.as_bytes()[..8]);
    let delay = u64::from_be_bytes(prefix) % maximum_millis.saturating_add(1);
    Duration::from_millis(delay)
}

pub(super) async fn next_term<S>(
    store: &S,
    scope: &CoordinatorScope,
) -> Result<CoordinatorTerm, CoordinatorRuntimeError>
where
    S: ScopedElectionStore,
{
    let next = store
        .get_leader_term(scope)
        .await?
        .checked_add(1)
        .ok_or(CoordinatorRuntimeError::RevisionExhausted)?;
    CoordinatorTerm::new(next).map_err(|_| CoordinatorRuntimeError::RevisionExhausted)
}

#[cfg(test)]
mod tests {
    use lattice_core::actor_ref::{NodeAddress, NodeIncarnation, PlacementDomainId};

    use super::*;

    #[test]
    fn candidate_preference_is_deterministic_scoped_and_bounded() {
        let local = NodeKey {
            node_id: "candidate".to_owned(),
            address: NodeAddress::new("127.0.0.1", 33009).unwrap(),
            incarnation: NodeIncarnation::new(9).unwrap(),
        };
        let maximum = Duration::from_millis(10_000);
        let membership = candidate_delay_duration(&CoordinatorScope::Membership, &local, maximum);
        let membership_again =
            candidate_delay_duration(&CoordinatorScope::Membership, &local, maximum);
        let placement = candidate_delay_duration(
            &CoordinatorScope::Placement(PlacementDomainId::new("candidate-domain").unwrap()),
            &local,
            maximum,
        );

        assert_eq!(membership, membership_again);
        assert!(membership <= maximum);
        assert!(placement <= maximum);
        assert_ne!(membership, placement);
        assert_eq!(
            candidate_delay_duration(&CoordinatorScope::Membership, &local, Duration::ZERO),
            Duration::ZERO
        );
    }
}
