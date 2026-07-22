use super::{
    codec::ExactActorTargetWire, error::RemoteMessageError, target::ExactActorTarget,
    target_cache::ExactTargetCache,
};

pub(crate) const MAX_EXACT_TARGET_DICTIONARY_ENTRIES: usize = 1024;

pub(crate) struct ExactTargetDictionary {
    entries: Vec<Option<ExactActorTarget>>,
}

impl ExactTargetDictionary {
    pub(crate) fn new() -> Self {
        Self {
            entries: vec![None; MAX_EXACT_TARGET_DICTIONARY_ENTRIES],
        }
    }

    pub(super) fn resolve(
        &mut self,
        id: u64,
        wire: Option<ExactActorTargetWire>,
        cache: &mut ExactTargetCache,
    ) -> Result<ExactActorTarget, RemoteMessageError> {
        let index = usize::try_from(
            id.checked_sub(1)
                .ok_or(RemoteMessageError::InvalidPayload)?,
        )
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
        let slot = self
            .entries
            .get_mut(index)
            .ok_or(RemoteMessageError::InvalidPayload)?;
        if let Some(wire) = wire {
            let target = cache.resolve(wire)?;
            if slot
                .as_ref()
                .is_some_and(|registered| registered != &target)
            {
                return Err(RemoteMessageError::InvalidPayload);
            }
            *slot = Some(target.clone());
            return Ok(target);
        }
        slot.clone().ok_or(RemoteMessageError::InvalidPayload)
    }
}

#[cfg(test)]
mod tests {
    use lattice_core::actor_ref::{
        ActivationId, ActorPath, ActorRef, ClusterId, NodeAddress, NodeIncarnation, ProtocolId,
    };

    use super::*;
    use crate::messaging::codec::target_to_wire;

    fn actor(sequence: u64) -> ActorRef {
        let incarnation = NodeIncarnation::new(1).unwrap();
        ActorRef::new(
            ClusterId::new("dictionary-test").unwrap(),
            NodeAddress::new("127.0.0.1", 25520).unwrap(),
            incarnation,
            ActorPath::user(["user", "target"]).unwrap(),
            ActivationId::new(incarnation, sequence).unwrap(),
            ProtocolId::new(7).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn registration_is_bounded_reusable_and_collision_checked() {
        let mut dictionary = ExactTargetDictionary::new();
        let mut cache = ExactTargetCache::new(8);
        let first = actor(1);
        assert_eq!(
            dictionary
                .resolve(1, Some(target_to_wire(&first)), &mut cache)
                .unwrap(),
            ExactActorTarget::from(&first)
        );
        assert_eq!(
            dictionary.resolve(1, None, &mut cache).unwrap(),
            ExactActorTarget::from(&first)
        );
        assert!(
            dictionary
                .resolve(1, Some(target_to_wire(&actor(2))), &mut cache)
                .is_err()
        );
        assert!(dictionary.resolve(0, None, &mut cache).is_err());
        assert!(
            dictionary
                .resolve(
                    MAX_EXACT_TARGET_DICTIONARY_ENTRIES as u64 + 1,
                    None,
                    &mut cache
                )
                .is_err()
        );
    }
}
