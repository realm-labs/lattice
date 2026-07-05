use std::any::type_name;
use std::fmt;
use std::marker::PhantomData;

use lattice_actor::{Actor, Handler};
use lattice_core::{
    ActorKind, DirectLinkMessage, DirectLinkMessageDescriptor, DirectLinkMessageId,
    DirectLinkStreamDescriptor, DirectLinkStreamSpec, Linked,
};

pub struct DirectLinkStream<Messages = ()> {
    descriptor: DirectLinkStreamDescriptor,
    _messages: PhantomData<fn() -> Messages>,
}

impl<Messages> Clone for DirectLinkStream<Messages> {
    fn clone(&self) -> Self {
        Self {
            descriptor: self.descriptor.clone(),
            _messages: PhantomData,
        }
    }
}

impl<Messages> fmt::Debug for DirectLinkStream<Messages> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DirectLinkStream")
            .field("descriptor", &self.descriptor)
            .finish()
    }
}

impl DirectLinkStream<()> {
    pub fn new(stream_name: impl Into<String>) -> Self {
        Self {
            descriptor: DirectLinkStreamDescriptor::new(stream_name),
            _messages: PhantomData,
        }
    }
}

impl<Messages> DirectLinkStream<Messages> {
    pub fn message<T>(self) -> DirectLinkStream<(T, Messages)>
    where
        T: DirectLinkMessage,
    {
        let message_id =
            DirectLinkMessageId::for_proto(&self.descriptor.stream_name, T::PROTO_FULL_NAME);
        self.message_with_id::<T>(message_id)
    }

    pub fn manual_id<T>(self, message_id: u64) -> DirectLinkStream<(T, Messages)>
    where
        T: DirectLinkMessage,
    {
        self.message_with_id::<T>(DirectLinkMessageId(message_id))
    }

    pub fn descriptor(&self) -> DirectLinkStreamDescriptor {
        self.descriptor.clone()
    }

    pub fn for_actor<A>(&self, actor_kind: ActorKind) -> DirectLinkActorBinding<A, Messages>
    where
        A: Actor + DirectLinkHandlers<Messages>,
    {
        DirectLinkActorBinding {
            actor_kind,
            stream: self.descriptor.clone(),
            _actor: PhantomData,
            _messages: PhantomData,
        }
    }

    fn message_with_id<T>(
        mut self,
        message_id: DirectLinkMessageId,
    ) -> DirectLinkStream<(T, Messages)>
    where
        T: DirectLinkMessage,
    {
        self.descriptor.messages.push(DirectLinkMessageDescriptor {
            message_id,
            proto_full_name: T::PROTO_FULL_NAME.to_string(),
            rust_type_name: type_name::<T>().to_string(),
        });
        DirectLinkStream {
            descriptor: self.descriptor,
            _messages: PhantomData,
        }
    }
}

impl<Messages> DirectLinkStreamSpec for DirectLinkStream<Messages>
where
    Messages: Send + Sync + 'static,
{
    fn descriptor(&self) -> DirectLinkStreamDescriptor {
        self.descriptor.clone()
    }
}

#[derive(Debug, Clone)]
pub struct DirectLinkActorBinding<A, Messages> {
    actor_kind: ActorKind,
    stream: DirectLinkStreamDescriptor,
    _actor: PhantomData<fn() -> A>,
    _messages: PhantomData<fn() -> Messages>,
}

impl<A, Messages> DirectLinkActorBinding<A, Messages> {
    pub fn actor_kind(&self) -> &ActorKind {
        &self.actor_kind
    }

    pub fn stream(&self) -> &DirectLinkStreamDescriptor {
        &self.stream
    }
}

pub trait DirectLinkHandlers<Messages>: Actor {}

impl<A> DirectLinkHandlers<()> for A where A: Actor {}

impl<A, Head, Tail> DirectLinkHandlers<(Head, Tail)> for A
where
    A: Actor + Handler<Linked<Head>> + DirectLinkHandlers<Tail>,
    Head: Send + 'static,
    Tail: Send + Sync + 'static,
{
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use async_trait::async_trait;
    use lattice_actor::{ActorContext, Handler};
    use lattice_core::{DirectLinkMessage, Linked, actor_kind};

    use super::*;

    #[derive(Clone, PartialEq, ::prost::Message)]
    struct PositionUpdate {
        #[prost(uint64, tag = "1")]
        entity_id: u64,
    }

    impl DirectLinkMessage for PositionUpdate {
        const PROTO_FULL_NAME: &'static str = "game.PositionUpdate";
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    struct StateDelta {
        #[prost(uint64, tag = "1")]
        tick: u64,
    }

    impl DirectLinkMessage for StateDelta {
        const PROTO_FULL_NAME: &'static str = "game.StateDelta";
    }

    struct BattleActor;

    #[async_trait]
    impl lattice_actor::Actor for BattleActor {
        type Error = Infallible;
    }

    #[async_trait]
    impl Handler<Linked<PositionUpdate>> for BattleActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: Linked<PositionUpdate>,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<Linked<StateDelta>> for BattleActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: Linked<StateDelta>,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[test]
    fn stream_uses_deterministic_proto_message_ids() {
        let stream = DirectLinkStream::new("movement")
            .message::<PositionUpdate>()
            .message::<StateDelta>();

        let descriptor = stream.descriptor();

        assert_eq!(descriptor.stream_name, "movement");
        assert_eq!(descriptor.messages.len(), 2);
        assert_eq!(
            descriptor.messages[0].message_id,
            DirectLinkMessageId::for_proto("movement", "game.PositionUpdate")
        );
        assert_eq!(
            descriptor.messages[1].message_id,
            DirectLinkMessageId::for_proto("movement", "game.StateDelta")
        );
    }

    #[test]
    fn manual_id_override_is_recorded_in_descriptor() {
        let stream = DirectLinkStream::new("legacy").manual_id::<PositionUpdate>(9001);

        assert_eq!(
            stream.descriptor().messages[0].message_id,
            DirectLinkMessageId(9001)
        );
    }

    #[test]
    fn stream_for_actor_requires_linked_handlers() {
        let stream = DirectLinkStream::new("movement")
            .message::<PositionUpdate>()
            .message::<StateDelta>();

        let binding = stream.for_actor::<BattleActor>(actor_kind!("Battle"));

        assert_eq!(binding.actor_kind().as_str(), "Battle");
        assert_eq!(binding.stream().accepted_message_ids().len(), 2);
    }
}
