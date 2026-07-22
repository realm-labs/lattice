use std::collections::{HashMap, VecDeque};

use super::{
    codec::{ExactActorTargetWire, target_from_wire},
    error::RemoteMessageError,
    target::ExactActorTarget,
};

pub(crate) struct ExactTargetCache {
    capacity: usize,
    entries: HashMap<ExactActorTargetWire, ExactActorTarget>,
    insertion_order: VecDeque<ExactActorTargetWire>,
    hits: u64,
    misses: u64,
}

impl ExactTargetCache {
    pub(crate) fn new(capacity: usize) -> Self {
        debug_assert!(capacity > 0);
        Self {
            capacity,
            entries: HashMap::new(),
            insertion_order: VecDeque::new(),
            hits: 0,
            misses: 0,
        }
    }

    pub(super) fn resolve(
        &mut self,
        wire: ExactActorTargetWire,
    ) -> Result<ExactActorTarget, RemoteMessageError> {
        if let Some(target) = self.entries.get(&wire) {
            self.hits = self.hits.saturating_add(1);
            return Ok(target.clone());
        }
        self.misses = self.misses.saturating_add(1);
        let key = detached_key(&wire);
        let target = target_from_wire(wire)?;
        if self.entries.len() == self.capacity
            && let Some(oldest) = self.insertion_order.pop_front()
        {
            self.entries.remove(&oldest);
        }
        self.insertion_order.push_back(key.clone());
        self.entries.insert(key, target.clone());
        Ok(target)
    }

    #[cfg(test)]
    pub(crate) fn metrics(&self) -> (u64, u64) {
        (self.hits, self.misses)
    }

    pub(crate) fn take_metrics_if_ready(&mut self) -> Option<(u64, u64)> {
        const REPORT_INTERVAL: u64 = 1024;
        (self.hits.saturating_add(self.misses) >= REPORT_INTERVAL).then(|| self.take_metrics())
    }

    pub(crate) fn take_metrics(&mut self) -> (u64, u64) {
        let metrics = (self.hits, self.misses);
        self.hits = 0;
        self.misses = 0;
        metrics
    }
}

fn detached_key(wire: &ExactActorTargetWire) -> ExactActorTargetWire {
    ExactActorTargetWire {
        cluster_id: bytes::Bytes::copy_from_slice(&wire.cluster_id),
        host: bytes::Bytes::copy_from_slice(&wire.host),
        port: wire.port,
        node_incarnation: bytes::Bytes::copy_from_slice(&wire.node_incarnation),
        actor_path: bytes::Bytes::copy_from_slice(&wire.actor_path),
        activation_sequence: wire.activation_sequence,
        protocol_id: wire.protocol_id,
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
            ClusterId::new("cache-test").unwrap(),
            NodeAddress::new("127.0.0.1", 25520).unwrap(),
            incarnation,
            ActorPath::user(["user", "target"]).unwrap(),
            ActivationId::new(incarnation, sequence).unwrap(),
            ProtocolId::new(7).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn cache_reuses_validated_targets_and_evicts_at_the_bound() {
        let mut cache = ExactTargetCache::new(1);
        let first = target_to_wire(&actor(1));
        let second = target_to_wire(&actor(2));
        assert_eq!(cache.entries.capacity(), 0);

        assert_eq!(
            cache.resolve(first.clone()).unwrap().activation_id,
            actor(1).activation_id()
        );
        let cached = cache.entries.keys().next().unwrap();
        assert_ne!(cached.cluster_id.as_ptr(), first.cluster_id.as_ptr());
        assert_eq!(
            cache.resolve(first.clone()).unwrap().activation_id,
            actor(1).activation_id()
        );
        assert_eq!(cache.metrics(), (1, 1));

        cache.resolve(second).unwrap();
        cache.resolve(first).unwrap();
        assert_eq!(cache.metrics(), (1, 3));
    }
}
