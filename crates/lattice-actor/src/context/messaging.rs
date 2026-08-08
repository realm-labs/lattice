//! Typed messaging performed from inside an actor turn.
//!
//! The methods here are the only place an [`ActorContext`] reaches the surrounding
//! [`ActorSystem`], so self/sender propagation and deadline inheritance stay in one place.

use std::time::Duration;

use lattice_core::actor_ref::{ActorRef, RecipientRef};

use super::{ActorContext, ActorTurnMessaging};
use crate::{
    error::ActorTellError,
    handle::ActorHandle,
    protocol::{SupportsAsk, SupportsTell},
    recipient::{ActorSystem, RecipientError, deadline_from_timeout},
    traits::{Actor, Handler, Message, Request},
};

impl ActorTurnMessaging {
    /// Sends with the current Actor as the envelope sender.
    pub async fn tell<P, M>(
        &self,
        target: impl Into<RecipientRef<P>>,
        message: M,
    ) -> Result<(), RecipientError>
    where
        P: SupportsTell<M>,
        M: Message,
    {
        self.actor_system
            .tell_with_sender(
                target.into(),
                message,
                self.self_ref.as_ref().map(ActorRef::erase),
            )
            .await
    }

    /// Sends a typed request without extending the current request deadline.
    pub async fn ask<P, R>(
        &self,
        target: impl Into<RecipientRef<P>>,
        request: R,
        timeout: Duration,
    ) -> Result<R::Response, RecipientError>
    where
        P: SupportsAsk<R>,
        R: Request,
    {
        let requested_deadline = deadline_from_timeout(timeout)?;
        let deadline = self
            .deadline
            .map_or(requested_deadline, |parent| parent.min(requested_deadline));
        self.actor_system
            .ask_until(target.into(), request, deadline)
            .await
    }

    /// Forwards while preserving the original envelope sender.
    pub async fn forward<P, M>(
        &self,
        target: impl Into<RecipientRef<P>>,
        message: M,
    ) -> Result<(), RecipientError>
    where
        P: SupportsTell<M>,
        M: Message,
    {
        self.actor_system
            .tell_with_sender(
                target.into(),
                message,
                self.sender.as_ref().map(ActorRef::erase),
            )
            .await
    }
}

impl<A: Actor> ActorContext<A> {
    /// Snapshots the current turn's typed messaging authority.
    ///
    /// The returned handle does not expose placement, mailbox, child, task, or
    /// extension internals. It preserves self/sender propagation and the
    /// current request deadline for adapters that must perform typed messaging
    /// after releasing the `ActorContext` borrow.
    pub fn turn_messaging(&self) -> Result<ActorTurnMessaging, RecipientError> {
        Ok(ActorTurnMessaging {
            actor_system: self.actor_system()?.clone(),
            self_ref: self.self_ref.clone(),
            sender: self.sender.clone(),
            deadline: self.current_deadline,
        })
    }

    /// Sends to a process-local handle with this actor as the envelope sender.
    pub fn tell_local<B, M>(
        &self,
        target: &ActorHandle<B>,
        message: M,
    ) -> Result<(), ActorTellError<M>>
    where
        B: Actor + Handler<M>,
        <B as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<M>,
        M: Message,
    {
        let sender = self.self_ref.as_ref().map(ActorRef::erase);
        target.try_tell_from(message, sender)
    }

    /// Forwards a one-way message while preserving the current envelope sender.
    ///
    /// If the current message has no actor sender, the forwarded message also
    /// has no actor sender.
    pub fn forward_local<B, M>(
        &self,
        target: &ActorHandle<B>,
        message: M,
    ) -> Result<(), ActorTellError<M>>
    where
        B: Actor + Handler<M>,
        <B as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<M>,
        M: Message,
    {
        target.try_tell_from(message, self.sender.as_ref().map(ActorRef::erase))
    }

    /// Sends to an exact or logical actor reference with this actor as sender.
    pub async fn tell<P, M>(
        &mut self,
        target: impl Into<RecipientRef<P>>,
        message: M,
    ) -> Result<(), RecipientError>
    where
        P: SupportsTell<M>,
        M: Message,
    {
        self.actor_system()?
            .tell_with_sender(
                target.into(),
                message,
                self.self_ref.as_ref().map(ActorRef::erase),
            )
            .await
    }

    /// Sends a request using a relative timeout.
    ///
    /// While handling another request, the downstream ask cannot outlive the
    /// current request's remaining deadline.
    pub async fn ask<P, R>(
        &mut self,
        target: impl Into<RecipientRef<P>>,
        request: R,
        timeout: Duration,
    ) -> Result<R::Response, RecipientError>
    where
        P: SupportsAsk<R>,
        R: Request,
    {
        let requested_deadline = deadline_from_timeout(timeout)?;
        let deadline = self
            .current_deadline
            .map_or(requested_deadline, |parent| parent.min(requested_deadline));
        self.actor_system()?
            .ask_until(target.into(), request, deadline)
            .await
    }

    /// Forwards to an exact or logical actor reference while preserving the
    /// current envelope sender.
    pub async fn forward<P, M>(
        &mut self,
        target: impl Into<RecipientRef<P>>,
        message: M,
    ) -> Result<(), RecipientError>
    where
        P: SupportsTell<M>,
        M: Message,
    {
        self.actor_system()?
            .tell_with_sender(
                target.into(),
                message,
                self.sender.as_ref().map(ActorRef::erase),
            )
            .await
    }

    pub(super) fn actor_system(&self) -> Result<&ActorSystem, RecipientError> {
        self.actor_system
            .as_ref()
            .and_then(|actor_system| actor_system.get())
            .ok_or(RecipientError::ActorSystemUnavailable)
    }
}

#[cfg(test)]
#[allow(dead_code)]
mod turn_messaging_tests {
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use async_trait::async_trait;
    use bytes::Bytes;
    use lattice_core::actor_ref::{
        ActivationId, ActorPath, ActorRef, ClusterId, EntityRef, NodeAddress, NodeIncarnation,
        ProtocolId, SingletonRef,
    };
    use lattice_remoting::messaging::error::{AskError, TellError};
    use lattice_remoting::protocol::ProtocolFingerprint;
    use lattice_remoting::watch::{RegisteredWatch, WatchError, WatchId};

    use super::ActorTurnMessaging;
    use crate::{
        actor_protocol,
        protocol::ProstCodec,
        recipient::{ActorSystem, RecipientBackend, RegisteredActorProtocol},
        traits::{Message, Request},
    };

    #[derive(Clone, PartialEq, prost::Message)]
    struct Probe {
        #[prost(uint64, tag = "1")]
        value: u64,
    }

    impl Message for Probe {}

    #[derive(Clone, PartialEq, prost::Message)]
    struct Query {
        #[prost(uint64, tag = "1")]
        value: u64,
    }

    impl Request for Query {
        type Response = QueryReply;
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct QueryReply {
        #[prost(uint64, tag = "1")]
        value: u64,
    }

    actor_protocol! {
        TurnProtocol {
            protocol_id: 97;
            name: "actor-turn-messaging/v1";
            tell 1 => Probe {
                schema_version: 1,
                codec: ProstCodec,
            }
            ask 2 => Query {
                request_schema_version: 1,
                response_schema_version: 1,
                request_codec: ProstCodec,
                response_codec: ProstCodec,
            }
        }
    }

    #[derive(Default)]
    struct RecordingBackend {
        tell_senders: Mutex<Vec<Option<ActorRef>>>,
        ask_deadlines: Mutex<Vec<Instant>>,
    }

    #[async_trait]
    impl RecipientBackend for RecordingBackend {
        async fn tell(
            &self,
            sender: Option<ActorRef>,
            _target: lattice_core::actor_ref::RecipientRef,
            _protocol_fingerprint: ProtocolFingerprint,
            _message_id: u64,
            _payload: Bytes,
        ) -> Result<(), TellError> {
            self.tell_senders
                .lock()
                .expect("tell sender mutex")
                .push(sender);
            Ok(())
        }

        async fn ask(
            &self,
            _target: lattice_core::actor_ref::RecipientRef,
            _protocol_fingerprint: ProtocolFingerprint,
            _message_id: u64,
            _payload: Bytes,
            deadline: Instant,
        ) -> Result<Bytes, AskError> {
            self.ask_deadlines
                .lock()
                .expect("ask deadline mutex")
                .push(deadline);
            Ok(Bytes::from(prost::Message::encode_to_vec(&QueryReply {
                value: 41,
            })))
        }

        async fn watch_actor(&self, _target: ActorRef) -> Result<RegisteredWatch, WatchError> {
            unimplemented!("watching is outside this fixture")
        }

        async fn watch_entity_current(
            &self,
            _target: EntityRef,
        ) -> Result<RegisteredWatch, WatchError> {
            unimplemented!("watching is outside this fixture")
        }

        async fn watch_singleton_current(
            &self,
            _target: SingletonRef,
        ) -> Result<RegisteredWatch, WatchError> {
            unimplemented!("watching is outside this fixture")
        }

        fn unwatch(&self, _watch_id: WatchId) -> Result<(), WatchError> {
            unimplemented!("watching is outside this fixture")
        }
    }

    #[tokio::test]
    async fn owned_turn_messaging_preserves_sender_and_parent_deadline() {
        let backend = Arc::new(RecordingBackend::default());
        let protocol = Arc::new(TurnProtocol::build().expect("turn protocol"));
        let actor_system =
            ActorSystem::new(backend.clone(), [RegisteredActorProtocol::new(protocol)])
                .expect("actor system");
        let self_ref = actor_ref("self", 1);
        let original_sender = actor_ref("sender", 2);
        let target = actor_ref("target", 3)
            .try_typed::<TurnProtocol>()
            .expect("typed target");
        let parent_deadline = Instant::now() + Duration::from_secs(1);
        let messaging = ActorTurnMessaging {
            actor_system,
            self_ref: Some(self_ref.clone()),
            sender: Some(original_sender.clone()),
            deadline: Some(parent_deadline),
        };

        messaging
            .tell(target.clone(), Probe { value: 1 })
            .await
            .expect("typed tell");
        messaging
            .forward(target.clone(), Probe { value: 2 })
            .await
            .expect("typed forward");
        let reply = messaging
            .ask(target, Query { value: 3 }, Duration::from_secs(30))
            .await
            .expect("typed ask");

        assert_eq!(reply.value, 41);
        let tell_senders = backend.tell_senders.lock().expect("tell sender mutex");
        assert!(
            tell_senders[0]
                .as_ref()
                .is_some_and(|sender| { sender.same_activation(&self_ref) })
        );
        assert!(
            tell_senders[1]
                .as_ref()
                .is_some_and(|sender| { sender.same_activation(&original_sender) })
        );
        let ask_deadlines = backend.ask_deadlines.lock().expect("ask deadline mutex");
        assert_eq!(ask_deadlines.as_slice(), &[parent_deadline]);
    }

    fn actor_ref(segment: &str, sequence: u64) -> ActorRef {
        let node_incarnation = NodeIncarnation::new(7).expect("node incarnation");
        ActorRef::new(
            ClusterId::new("turn-test").expect("cluster ID"),
            NodeAddress::new("127.0.0.1", 19097).expect("node address"),
            node_incarnation,
            ActorPath::user([segment]).expect("actor path"),
            ActivationId::new(node_incarnation, sequence).expect("activation ID"),
            ProtocolId::new(97).expect("protocol ID"),
        )
        .expect("actor ref")
    }
}
