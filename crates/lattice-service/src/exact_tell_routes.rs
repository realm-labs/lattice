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

#[derive(Debug)]
pub(crate) struct RejectedExactTell {
    pub(crate) error: TellError,
    pub(crate) sender: SenderIdentity,
    pub(crate) target: ActorRef,
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
    ) -> Result<(), Box<RejectedExactTell>>
    where
        F: FnOnce(&ActorRef) -> Result<Arc<Association>, TellError>,
    {
        let key = ExactTellRouteKey {
            sender,
            target,
            fingerprint: message.fingerprint,
        };
        if self.maximum == 0 {
            let association = match association(&key.target) {
                Ok(association) => association,
                Err(error) => return Err(rejected_exact_tell(error, &key, message)),
            };
            return messaging
                .try_tell_retained(
                    &association,
                    &key.sender,
                    &key.target,
                    OutboundMessage::new(message.fingerprint, message.message_id, message.payload),
                )
                .map(|_| ())
                .map_err(|(error, payload)| {
                    rejected_exact_tell(
                        error,
                        &key,
                        ExactTellMessage {
                            fingerprint: message.fingerprint,
                            message_id: message.message_id,
                            payload,
                        },
                    )
                });
        }
        if let Some(route) = self.hot.load().as_ref()
            && route.key == key
        {
            let association_id = route.route.association_id();
            let result = send_retained(&route.route, &key, message);
            self.evict_inactive(
                &key,
                association_id,
                result.as_ref().err().map(|error| &error.error),
            );
            return result;
        }
        if let Some(route) = self.routes.get(&key) {
            let association_id = route.association_id();
            let result = send_retained(&route, &key, message);
            drop(route);
            self.evict_inactive(
                &key,
                association_id,
                result.as_ref().err().map(|error| &error.error),
            );
            return result;
        }

        let (route, message) = prepare_retained(messaging, &key, message, association)?;
        if !self.admit_slot() {
            return send_retained(&route, &key, message);
        }

        match self.routes.entry(key) {
            Entry::Occupied(entry) => {
                self.release_slot();
                let association_id = entry.get().association_id();
                let result = send_retained(entry.get(), entry.key(), message);
                if result
                    .as_ref()
                    .err()
                    .is_some_and(|error| is_inactive_error(&error.error))
                {
                    let key = entry.key().clone();
                    drop(entry);
                    self.remove_if_association(&key, association_id);
                }
                result
            }
            Entry::Vacant(entry) => {
                let route = entry.insert(route);
                let association_id = route.association_id();
                let result = send_retained(&route, route.key(), message);
                let inactive = result
                    .as_ref()
                    .err()
                    .is_some_and(|error| is_inactive_error(&error.error));
                let hot = (!inactive).then(|| HotExactTellRoute {
                    key: route.key().clone(),
                    route: route.clone(),
                });
                if inactive {
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

    pub(crate) async fn tell_wait<F>(
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
        let key = ExactTellRouteKey {
            sender,
            target,
            fingerprint: message.fingerprint,
        };
        if self.maximum == 0 {
            let association = association(&key.target)?;
            return messaging
                .tell_wait(
                    &association,
                    &key.sender,
                    &key.target,
                    OutboundMessage::new(message.fingerprint, message.message_id, message.payload),
                )
                .await
                .map(|_| ());
        }
        let route = self
            .hot
            .load_full()
            .filter(|route| route.key == key)
            .map(|route| route.route.clone())
            .or_else(|| self.routes.get(&key).map(|route| route.clone()));
        let route = match route {
            Some(route) => route,
            None => messaging.prepare_exact_tell_route(
                association(&key.target)?,
                &key.sender,
                &key.target,
                key.fingerprint,
            )?,
        };
        let association_id = route.association_id();
        let result = route
            .tell_wait(message.message_id, message.payload)
            .await
            .map(|_| ());
        self.evict_inactive(&key, association_id, result.as_ref().err());
        result
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
        error: Option<&TellError>,
    ) {
        if error.is_some_and(is_inactive_error) {
            if self
                .hot
                .load()
                .as_ref()
                .is_some_and(|hot| hot.key == *key && hot.route.association_id() == association_id)
            {
                self.hot.store(None);
            }
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

fn send_retained(
    route: &PreparedExactTellRoute,
    key: &ExactTellRouteKey,
    message: ExactTellMessage,
) -> Result<(), Box<RejectedExactTell>> {
    route
        .try_tell_retained(message.message_id, message.payload)
        .map(|_| ())
        .map_err(|(error, payload)| {
            rejected_exact_tell(
                error,
                key,
                ExactTellMessage {
                    fingerprint: message.fingerprint,
                    message_id: message.message_id,
                    payload,
                },
            )
        })
}

fn prepare_retained<F>(
    messaging: &OutboundMessaging,
    key: &ExactTellRouteKey,
    message: ExactTellMessage,
    association: F,
) -> Result<(PreparedExactTellRoute, ExactTellMessage), Box<RejectedExactTell>>
where
    F: FnOnce(&ActorRef) -> Result<Arc<Association>, TellError>,
{
    let association = match association(&key.target) {
        Ok(association) => association,
        Err(error) => return Err(rejected_exact_tell(error, key, message)),
    };
    match messaging.prepare_exact_tell_route(association, &key.sender, &key.target, key.fingerprint)
    {
        Ok(route) => Ok((route, message)),
        Err(error) => Err(rejected_exact_tell(error, key, message)),
    }
}

fn rejected_exact_tell(
    error: TellError,
    key: &ExactTellRouteKey,
    message: ExactTellMessage,
) -> Box<RejectedExactTell> {
    Box::new(RejectedExactTell {
        error,
        sender: key.sender.clone(),
        target: key.target.clone(),
        fingerprint: message.fingerprint,
        message_id: message.message_id,
        payload: message.payload,
    })
}

fn is_inactive_error(error: &TellError) -> bool {
    matches!(
        error,
        TellError::Association(AssociationError::NotActive | AssociationError::Closed)
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
            Err(rejected)
                if matches!(
                    rejected.error,
                    TellError::Association(AssociationError::QueueFull)
                )
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
            Err(rejected)
                if matches!(
                    rejected.error,
                    TellError::Association(AssociationError::NotActive)
                )
        ));
        assert_eq!(cache.len(), 0);
    }

    #[tokio::test]
    async fn deferred_tell_waits_for_cached_route_capacity() {
        let protocol_id = ProtocolId::new(7).unwrap();
        let fingerprint = ProtocolFingerprint::digest(b"test");
        let association = active_association(protocol_id, fingerprint, 1);
        let messaging = OutboundMessaging::new(8).unwrap();
        let cache = ExactTellRouteCache::new(1);
        let target = target(protocol_id, 1);

        cache
            .tell(
                &messaging,
                SenderIdentity::Process(1),
                target.clone(),
                ExactTellMessage {
                    fingerprint,
                    message_id: 1,
                    payload: Bytes::from_static(b"first"),
                },
                |_| Ok(association.clone()),
            )
            .unwrap();
        let rejected = cache
            .tell(
                &messaging,
                SenderIdentity::Process(1),
                target.clone(),
                ExactTellMessage {
                    fingerprint,
                    message_id: 2,
                    payload: Bytes::from_static(b"second"),
                },
                |_| panic!("cached route must not look up the association"),
            )
            .unwrap_err();
        assert!(matches!(
            rejected.error,
            TellError::Association(AssociationError::QueueFull)
        ));

        let mut receiver = association.take_lane_receiver(LaneKind::Bulk(0)).unwrap();
        let send = cache.tell_wait(
            &messaging,
            rejected.sender,
            rejected.target,
            ExactTellMessage {
                fingerprint: rejected.fingerprint,
                message_id: rejected.message_id,
                payload: rejected.payload,
            },
            |_| panic!("cached route must not look up the association"),
        );
        let release_capacity = receiver.recv();
        let (result, first) = tokio::join!(send, release_capacity);

        result.unwrap();
        assert!(first.is_some());
        assert!(receiver.recv().await.is_some());
    }
}
