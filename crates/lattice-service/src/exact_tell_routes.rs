use std::{
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use dashmap::{DashMap, mapref::entry::Entry};
use lattice_core::actor_ref::ActorRef;
use lattice_remoting::{
    association::{Association, AssociationError, AssociationId},
    messaging::{
        error::TellError,
        outbound::{OutboundMessage, OutboundMessaging, PreparedExactTellRoute},
        target::SenderIdentity,
    },
    protocol::ProtocolFingerprint,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExactTellRouteKey {
    sender: SenderIdentity,
    target: ActorRef,
    fingerprint: ProtocolFingerprint,
}

impl Hash for ExactTellRouteKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.target.node_incarnation().hash(state);
        self.target.activation_id().hash(state);
        self.target.protocol_id().hash(state);
        match &self.sender {
            SenderIdentity::Actor(sender) => {
                0_u8.hash(state);
                sender.node_incarnation().hash(state);
                sender.activation_id().hash(state);
                sender.protocol_id().hash(state);
            }
            SenderIdentity::Process(incarnation) => {
                1_u8.hash(state);
                incarnation.hash(state);
            }
        }
    }
}

pub(crate) struct ExactTellRouteCache {
    hot: ArcSwapOption<HotExactTellRoute>,
    routes: DashMap<ExactTellRouteKey, PreparedExactTellRoute>,
    occupied: AtomicUsize,
    maximum: usize,
}

struct HotExactTellRoute {
    key: ExactTellRouteKey,
    route: PreparedExactTellRoute,
}

pub(crate) struct ExactTellMessage {
    pub(crate) fingerprint: ProtocolFingerprint,
    pub(crate) message_id: u64,
    pub(crate) payload: Bytes,
}

impl ExactTellRouteCache {
    pub(crate) fn new(maximum: usize) -> Self {
        Self {
            hot: ArcSwapOption::empty(),
            routes: DashMap::new(),
            occupied: AtomicUsize::new(0),
            maximum,
        }
    }

    pub(crate) fn tell<F>(
        &self,
        messaging: &OutboundMessaging,
        sender: SenderIdentity,
        target: ActorRef,
        message: ExactTellMessage,
        association: F,
    ) -> Result<(), TellError>
    where
        F: FnOnce(&ActorRef) -> Result<Arc<Association>, TellError>,
    {
        let ExactTellMessage {
            fingerprint,
            message_id,
            payload,
        } = message;
        let key = ExactTellRouteKey {
            sender,
            target,
            fingerprint,
        };
        if self.maximum == 0 {
            let association = association(&key.target)?;
            return messaging
                .tell(
                    &association,
                    &key.sender,
                    &key.target,
                    OutboundMessage::new(key.fingerprint, message_id, payload),
                )
                .map(|_| ());
        }
        if let Some(route) = self.hot.load().as_ref()
            && route.key == key
        {
            let association_id = route.route.association_id();
            let result = route.route.tell(message_id, payload).map(|_| ());
            if is_inactive(&result) {
                self.hot.store(None);
                self.remove_if_association(&key, association_id);
            }
            return result;
        }
        if let Some(route) = self.routes.get(&key) {
            let association_id = route.association_id();
            let result = route.tell(message_id, payload).map(|_| ());
            drop(route);
            self.evict_inactive(&key, association_id, &result);
            return result;
        }

        let association = association(&key.target)?;
        let route = messaging.prepare_exact_tell_route(
            association,
            &key.sender,
            &key.target,
            key.fingerprint,
        )?;
        if !self.admit_slot() {
            return route.tell(message_id, payload).map(|_| ());
        }

        match self.routes.entry(key) {
            Entry::Occupied(entry) => {
                self.release_slot();
                let association_id = entry.get().association_id();
                let result = entry.get().tell(message_id, payload).map(|_| ());
                if is_inactive(&result) {
                    let key = entry.key().clone();
                    drop(entry);
                    self.remove_if_association(&key, association_id);
                }
                result
            }
            Entry::Vacant(entry) => {
                let route = entry.insert(route);
                let association_id = route.association_id();
                let result = route.tell(message_id, payload).map(|_| ());
                let hot = (!is_inactive(&result)).then(|| HotExactTellRoute {
                    key: route.key().clone(),
                    route: route.clone(),
                });
                if is_inactive(&result) {
                    let key = route.key().clone();
                    drop(route);
                    self.remove_if_association(&key, association_id);
                } else if let Some(hot) = hot {
                    drop(route);
                    self.hot.store(Some(Arc::new(hot)));
                }
                result
            }
        }
    }

    fn reserve_slot(&self) -> bool {
        self.occupied
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |occupied| {
                (occupied < self.maximum).then_some(occupied + 1)
            })
            .is_ok()
    }

    fn admit_slot(&self) -> bool {
        if self.reserve_slot() {
            return true;
        }
        let victim = self.routes.iter().next().map(|route| {
            let key = route.key().clone();
            let association_id = route.association_id();
            (key, association_id)
        });
        if let Some((key, association_id)) = victim {
            self.remove_if_association(&key, association_id);
        }
        self.reserve_slot()
    }

    fn release_slot(&self) {
        self.occupied.fetch_sub(1, Ordering::AcqRel);
    }

    fn evict_inactive(
        &self,
        key: &ExactTellRouteKey,
        association_id: AssociationId,
        result: &Result<(), TellError>,
    ) {
        if is_inactive(result) {
            self.remove_if_association(key, association_id);
        }
    }

    fn remove_if_association(&self, key: &ExactTellRouteKey, association_id: AssociationId) {
        if self
            .routes
            .remove_if(key, |_, route| route.association_id() == association_id)
            .is_some()
        {
            self.release_slot();
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.routes.len()
    }
}

fn is_inactive(result: &Result<(), TellError>) -> bool {
    matches!(
        result,
        Err(TellError::Association(
            AssociationError::NotActive | AssociationError::Closed
        ))
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use lattice_core::actor_ref::{
        ActivationId, ActorPath, ClusterId, NodeAddress, NodeIncarnation, ProtocolId,
    };
    use lattice_remoting::{
        association::{AssociationKey, LaneAttachment, LaneKind},
        config::RemotingConfig,
        protocol::ProtocolDescriptor,
    };

    use super::*;

    fn active_association(
        protocol_id: ProtocolId,
        fingerprint: ProtocolFingerprint,
        queue_frames: usize,
    ) -> Arc<Association> {
        let key = AssociationKey {
            cluster_id: ClusterId::new("test").unwrap(),
            local_incarnation: NodeIncarnation::new(1).unwrap(),
            remote_address: NodeAddress::new("remote", 25520).unwrap(),
            remote_incarnation: NodeIncarnation::new(2).unwrap(),
        };
        let association = Arc::new(
            Association::new(
                key.clone(),
                RemotingConfig {
                    bulk_queue_frames_per_stripe: queue_frames,
                    ..RemotingConfig::default()
                },
            )
            .unwrap(),
        );
        for (lane, nonce) in [
            (LaneKind::Control, 1),
            (LaneKind::Interactive, 2),
            (LaneKind::Bulk(0), 3),
        ] {
            association
                .attach(LaneAttachment {
                    association_id: association.id(),
                    key: key.clone(),
                    lane,
                    connection_nonce: nonce,
                })
                .unwrap();
        }
        association
            .install_peer_catalogue([ProtocolDescriptor {
                protocol_id,
                fingerprint,
            }])
            .unwrap();
        association
    }

    fn target(protocol_id: ProtocolId, sequence: u64) -> ActorRef {
        let incarnation = NodeIncarnation::new(2).unwrap();
        ActorRef::new(
            ClusterId::new("test").unwrap(),
            NodeAddress::new("remote", 25520).unwrap(),
            incarnation,
            ActorPath::user(["user", &format!("target-{sequence}")]).unwrap(),
            ActivationId::new(incarnation, sequence).unwrap(),
            protocol_id,
        )
        .unwrap()
    }

    #[test]
    fn repeated_tell_uses_cached_route_without_an_association_lookup() {
        let protocol_id = ProtocolId::new(7).unwrap();
        let fingerprint = ProtocolFingerprint::digest(b"test");
        let association = active_association(protocol_id, fingerprint, 8);
        let messaging = OutboundMessaging::new(8).unwrap();
        let cache = ExactTellRouteCache::new(2);
        let lookups = AtomicUsize::new(0);

        for message_id in 1..=2 {
            cache
                .tell(
                    &messaging,
                    SenderIdentity::Process(1),
                    target(protocol_id, 1),
                    ExactTellMessage {
                        fingerprint,
                        message_id,
                        payload: Bytes::from_static(b"message"),
                    },
                    |_| {
                        lookups.fetch_add(1, Ordering::Relaxed);
                        Ok(association.clone())
                    },
                )
                .unwrap();
        }

        assert_eq!(lookups.load(Ordering::Relaxed), 1);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn cache_bound_replaces_an_old_route() {
        let protocol_id = ProtocolId::new(7).unwrap();
        let fingerprint = ProtocolFingerprint::digest(b"test");
        let association = active_association(protocol_id, fingerprint, 8);
        let messaging = OutboundMessaging::new(8).unwrap();
        let cache = ExactTellRouteCache::new(1);
        let lookups = AtomicUsize::new(0);

        for sequence in [1, 2, 2] {
            cache
                .tell(
                    &messaging,
                    SenderIdentity::Process(1),
                    target(protocol_id, sequence),
                    ExactTellMessage {
                        fingerprint,
                        message_id: sequence,
                        payload: Bytes::new(),
                    },
                    |_| {
                        lookups.fetch_add(1, Ordering::Relaxed);
                        Ok(association.clone())
                    },
                )
                .unwrap();
        }

        assert_eq!(lookups.load(Ordering::Relaxed), 2);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn zero_capacity_disables_prepared_route_caching() {
        let protocol_id = ProtocolId::new(7).unwrap();
        let fingerprint = ProtocolFingerprint::digest(b"test");
        let association = active_association(protocol_id, fingerprint, 8);
        let messaging = OutboundMessaging::new(8).unwrap();
        let cache = ExactTellRouteCache::new(0);
        let lookups = AtomicUsize::new(0);

        for message_id in 1..=2 {
            cache
                .tell(
                    &messaging,
                    SenderIdentity::Process(1),
                    target(protocol_id, 1),
                    ExactTellMessage {
                        fingerprint,
                        message_id,
                        payload: Bytes::new(),
                    },
                    |_| {
                        lookups.fetch_add(1, Ordering::Relaxed);
                        Ok(association.clone())
                    },
                )
                .unwrap();
        }

        assert_eq!(lookups.load(Ordering::Relaxed), 2);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn inactive_association_is_evicted_but_backpressure_keeps_the_route() {
        let protocol_id = ProtocolId::new(7).unwrap();
        let fingerprint = ProtocolFingerprint::digest(b"test");
        let association = active_association(protocol_id, fingerprint, 1);
        let messaging = OutboundMessaging::new(8).unwrap();
        let cache = ExactTellRouteCache::new(1);

        cache
            .tell(
                &messaging,
                SenderIdentity::Process(1),
                target(protocol_id, 1),
                ExactTellMessage {
                    fingerprint,
                    message_id: 1,
                    payload: Bytes::new(),
                },
                |_| Ok(association.clone()),
            )
            .unwrap();
        assert!(matches!(
            cache.tell(
                &messaging,
                SenderIdentity::Process(1),
                target(protocol_id, 1),
                ExactTellMessage {
                    fingerprint,
                    message_id: 2,
                    payload: Bytes::new(),
                },
                |_| panic!("cached route must not look up the association"),
            ),
            Err(TellError::Association(AssociationError::QueueFull))
        ));
        assert_eq!(cache.len(), 1);

        association.begin_close();
        assert!(matches!(
            cache.tell(
                &messaging,
                SenderIdentity::Process(1),
                target(protocol_id, 1),
                ExactTellMessage {
                    fingerprint,
                    message_id: 3,
                    payload: Bytes::new(),
                },
                |_| panic!("cached route must fail before another lookup"),
            ),
            Err(TellError::Association(AssociationError::NotActive))
        ));
        assert_eq!(cache.len(), 0);
    }
}
