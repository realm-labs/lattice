//! Typed messaging performed from inside an actor turn.
//!
//! Local handles and distributed references implement the same [`TellTarget`]
//! capability. Callers therefore choose a target, not a transport.

use std::future::Future;

#[cfg(feature = "distributed")]
use std::time::Duration;

#[cfg(feature = "distributed")]
use lattice_core::actor_ref::{ActorRef, EntityRef, RecipientRef, SingletonRef};

use super::ActorContext;
#[cfg(feature = "distributed")]
use super::ActorTurnMessaging;
use crate::{
    error::ActorTellError,
    handle::ActorHandle,
    traits::{Actor, Handler, Message},
};

#[cfg(feature = "distributed")]
use crate::{
    protocol::{Protocol, SupportsAsk, SupportsTell},
    recipient::{ActorSystem, RecipientError, deadline_from_timeout},
    traits::Request,
};

#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct OutboundTell {
    #[cfg(feature = "distributed")]
    actor_system: Option<ActorSystem>,
}

mod private {
    pub trait Sealed {}
}

/// A statically typed destination for a one-way Actor message.
///
/// [`ActorHandle`] implements this capability through its process-local
/// mailbox. With the `distributed` feature enabled, exact and logical Actor
/// references implement it through the current Actor system.
pub trait TellTarget<M: Message>: private::Sealed + Sync {
    type Error;

    #[doc(hidden)]
    fn deliver(
        &self,
        outbound: OutboundTell,
        message: M,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

impl<B: Actor> private::Sealed for ActorHandle<B> {}

impl<B, M> TellTarget<M> for ActorHandle<B>
where
    B: Actor + Handler<M>,
    B::Behavior: crate::state_machine::Accepts<M>,
    M: Message,
{
    type Error = ActorTellError<M>;

    fn deliver(
        &self,
        _outbound: OutboundTell,
        message: M,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let handle = self.clone();
        async move { handle.tell(message).await }
    }
}

#[cfg(feature = "distributed")]
async fn deliver_reference<P, M>(
    target: RecipientRef<P>,
    outbound: OutboundTell,
    message: M,
) -> Result<(), RecipientError>
where
    P: Protocol + SupportsTell<M>,
    M: Message,
{
    let actor_system = outbound
        .actor_system
        .ok_or(RecipientError::ActorSystemUnavailable)?;
    actor_system.tell(target, message).await
}

#[cfg(feature = "distributed")]
macro_rules! impl_reference_target {
    ($target:ident) => {
        impl<P: Protocol> private::Sealed for $target<P> {}

        impl<P, M> TellTarget<M> for $target<P>
        where
            P: Protocol + SupportsTell<M>,
            M: Message,
        {
            type Error = RecipientError;

            fn deliver(
                &self,
                outbound: OutboundTell,
                message: M,
            ) -> impl Future<Output = Result<(), Self::Error>> + Send {
                deliver_reference(self.clone().into(), outbound, message)
            }
        }
    };
}

#[cfg(feature = "distributed")]
impl_reference_target!(ActorRef);
#[cfg(feature = "distributed")]
impl_reference_target!(EntityRef);
#[cfg(feature = "distributed")]
impl_reference_target!(SingletonRef);

#[cfg(feature = "distributed")]
impl<P: Protocol> private::Sealed for RecipientRef<P> {}

#[cfg(feature = "distributed")]
impl<P, M> TellTarget<M> for RecipientRef<P>
where
    P: Protocol + SupportsTell<M>,
    M: Message,
{
    type Error = RecipientError;

    fn deliver(
        &self,
        outbound: OutboundTell,
        message: M,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        deliver_reference(self.clone(), outbound, message)
    }
}

#[cfg(feature = "distributed")]
impl ActorTurnMessaging {
    /// Sends to a local handle or distributed Actor reference.
    pub async fn tell<T, M>(&self, target: &T, message: M) -> Result<(), T::Error>
    where
        T: TellTarget<M>,
        M: Message,
    {
        target
            .deliver(
                OutboundTell {
                    actor_system: Some(self.actor_system.clone()),
                },
                message,
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
}

impl<A: Actor> ActorContext<A> {
    fn outbound_tell(&self) -> OutboundTell {
        OutboundTell {
            #[cfg(feature = "distributed")]
            actor_system: self
                .actor_system
                .as_ref()
                .and_then(|actor_system| actor_system.get())
                .cloned(),
        }
    }

    /// Sends to a local handle or distributed Actor reference.
    pub fn tell<'a, T, M>(
        &self,
        target: &'a T,
        message: M,
    ) -> impl Future<Output = Result<(), T::Error>> + Send + 'a
    where
        T: TellTarget<M> + 'a,
        M: Message,
    {
        target.deliver(self.outbound_tell(), message)
    }

    #[cfg(feature = "distributed")]
    /// Snapshots the current turn's typed messaging authority.
    ///
    /// The returned handle does not expose placement, mailbox, child, task, or
    /// extension internals. It preserves the current request deadline for
    /// adapters that must perform typed messaging after releasing the
    /// `ActorContext` borrow.
    pub fn turn_messaging(&self) -> Result<ActorTurnMessaging, RecipientError> {
        Ok(ActorTurnMessaging {
            actor_system: self.actor_system()?.clone(),
            deadline: self.current_deadline,
        })
    }

    #[cfg(feature = "distributed")]
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

    #[cfg(feature = "distributed")]
    pub(super) fn actor_system(&self) -> Result<&ActorSystem, RecipientError> {
        self.actor_system
            .as_ref()
            .and_then(|actor_system| actor_system.get())
            .ok_or(RecipientError::ActorSystemUnavailable)
    }
}

#[cfg(all(test, feature = "distributed"))]
#[allow(dead_code)]
mod turn_messaging_tests {
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use async_trait::async_trait;
    use bytes::Bytes;
    use lattice_core::{
        actor_ref::{
            ActivationId, ActorPath, ActorRef, ClusterId, EntityRef, NodeAddress, NodeIncarnation,
            ProtocolId, SingletonRef,
        },
        watch::WatchId,
    };
    use lattice_remoting::messaging::error::{AskError, TellError};
    use lattice_remoting::protocol::ProtocolFingerprint;
    use lattice_remoting::watch::{RegisteredWatch, WatchError};

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
        tell_count: Mutex<usize>,
        ask_deadlines: Mutex<Vec<Instant>>,
    }

    #[async_trait]
    impl RecipientBackend for RecordingBackend {
        async fn tell(
            &self,
            _target: lattice_core::actor_ref::RecipientRef,
            _protocol_fingerprint: ProtocolFingerprint,
            _message_id: u64,
            _payload: Bytes,
        ) -> Result<(), TellError> {
            let mut count = self.tell_count.lock().expect("tell count mutex");
            *count += 1;
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
    async fn owned_turn_messaging_sends_and_preserves_parent_deadline() {
        let backend = Arc::new(RecordingBackend::default());
        let protocol = Arc::new(TurnProtocol::build().expect("turn protocol"));
        let actor_system =
            ActorSystem::new(backend.clone(), [RegisteredActorProtocol::new(protocol)])
                .expect("actor system");
        let target = actor_ref("target", 3)
            .try_typed::<TurnProtocol>()
            .expect("typed target");
        let parent_deadline = Instant::now() + Duration::from_secs(1);
        let messaging = ActorTurnMessaging {
            actor_system,
            deadline: Some(parent_deadline),
        };

        messaging
            .tell(&target, Probe { value: 1 })
            .await
            .expect("typed tell");
        let reply = messaging
            .ask(target, Query { value: 3 }, Duration::from_secs(30))
            .await
            .expect("typed ask");

        assert_eq!(reply.value, 41);
        assert_eq!(*backend.tell_count.lock().expect("tell count mutex"), 1);
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
