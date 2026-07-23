use std::sync::Arc;

use bytes::Bytes;
use lattice_core::actor_ref::ActorRef;

use crate::{
    handle::ActorHandle,
    traits::{Actor, Handler, Message, MessageKind},
};

use super::{
    ActorProtocolBinding, ActorProtocolBindingBuilder, DispatchError, DispatchFuture, DispatchMode,
    DispatchReply, Protocol, ServerDispatch, SharedCodec, WireCodec, protocol_failure,
};

pub(crate) enum ProtocolTellDispatch {
    Accepted,
    Deferred {
        payload: Bytes,
        sender: Option<ActorRef>,
        completion: DispatchFuture,
    },
    Rejected(DispatchError),
}

impl<A: Actor, P: Protocol> ActorProtocolBinding<A, P> {
    pub(crate) fn try_dispatch_tell(
        &self,
        handle: &ActorHandle<A>,
        message_id: u64,
        payload: Bytes,
        sender: Option<ActorRef>,
    ) -> ProtocolTellDispatch {
        let payload_size = payload.len();
        let result = match self.protocol.bindings_by_id.get(&message_id) {
            None => ProtocolTellDispatch::Rejected(DispatchError::UnknownMessage(message_id)),
            Some(binding_index) => {
                let client = &self.protocol.bindings[*binding_index];
                if client.descriptor.mode != DispatchMode::Tell {
                    ProtocolTellDispatch::Rejected(DispatchError::ModeMismatch)
                } else if payload.len() > client.descriptor.max_payload {
                    ProtocolTellDispatch::Rejected(DispatchError::PayloadTooLarge {
                        actual: payload.len(),
                        maximum: client.descriptor.max_payload,
                    })
                } else {
                    match self.dispatch.get(&message_id) {
                        Some(ServerDispatch::Tell(dispatch)) => dispatch(handle, payload, sender),
                        Some(ServerDispatch::Async(_)) => {
                            ProtocolTellDispatch::Rejected(DispatchError::ModeMismatch)
                        }
                        None => ProtocolTellDispatch::Rejected(DispatchError::UnknownMessage(
                            message_id,
                        )),
                    }
                }
            }
        };
        if let ProtocolTellDispatch::Rejected(error) = &result {
            handle.observer().protocol_failed(
                handle.observation_metadata(),
                message_id,
                MessageKind::Tell,
                payload_size,
                protocol_failure(error),
            );
        }
        result
    }
}

impl<A: Actor, P: Protocol> ActorProtocolBindingBuilder<A, P> {
    pub fn tell<M, C>(mut self, message_id: u64, schema_version: u32, codec: C) -> Self
    where
        A: Handler<M>,
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<M>,
        M: Message,
        C: WireCodec<M>,
    {
        let codec = Arc::new(codec);
        self.client =
            self.client
                .tell::<M, _>(message_id, schema_version, SharedCodec(codec.clone()));
        self.dispatch.push((
            message_id,
            ServerDispatch::Tell(Arc::new(move |handle, payload, sender| {
                let message = match codec.decode(&payload) {
                    Ok(message) => message,
                    Err(error) => {
                        return ProtocolTellDispatch::Rejected(DispatchError::Decode(error));
                    }
                };
                let retry_sender = sender.clone();
                match handle.try_tell_from(message, sender) {
                    Ok(()) => ProtocolTellDispatch::Accepted,
                    Err(crate::error::ActorTellError::MailboxFull(message)) => {
                        let completion_sender = retry_sender.clone();
                        let handle = handle.clone();
                        let observer = handle.observer().clone();
                        let actor = handle.observation_metadata().clone();
                        let payload_size = payload.len();
                        let completion = Box::pin(async move {
                            let result = handle
                                .tell_from(message, completion_sender)
                                .await
                                .map(|()| DispatchReply::TellAccepted)
                                .map_err(|_| DispatchError::MailboxRejected);
                            if let Err(error) = &result {
                                observer.protocol_failed(
                                    &actor,
                                    message_id,
                                    MessageKind::Tell,
                                    payload_size,
                                    protocol_failure(error),
                                );
                            }
                            result
                        });
                        ProtocolTellDispatch::Deferred {
                            payload,
                            sender: retry_sender,
                            completion,
                        }
                    }
                    Err(_) => ProtocolTellDispatch::Rejected(DispatchError::MailboxRejected),
                }
            })),
        ));
        self
    }
}
