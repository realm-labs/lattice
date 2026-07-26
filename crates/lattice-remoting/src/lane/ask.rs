use std::{sync::Arc, time::Instant};

use bytes::Bytes;

use crate::{
    messaging::{
        codec::{failure_frame, reply_frame},
        error::RemoteMessageError,
        inbound::{InboundDispatch, failure_code},
        target::{CorrelationId, InboundAsk, InboundEntityAsk, InboundSingletonAsk, RemoteFailure},
    },
    wire::Frame,
};

pub(super) enum InboundAskWork {
    Exact(InboundAsk),
    Entity(InboundEntityAsk),
    Singleton(InboundSingletonAsk),
}

pub(super) async fn dispatch_inbound_ask(
    dispatch: Arc<dyn InboundDispatch>,
    work: InboundAskWork,
) -> Result<Frame, RemoteMessageError> {
    match work {
        InboundAskWork::Exact(ask) => {
            let deadline = Instant::now()
                .checked_add(ask.timeout_budget)
                .ok_or(RemoteMessageError::DeadlineExceeded)?;
            let result = dispatch
                .ask(ask.target, ask.message_id, ask.payload, deadline)
                .await;
            Ok(inbound_ask_response(ask.correlation_id, result))
        }
        InboundAskWork::Entity(ask) => {
            let deadline = Instant::now()
                .checked_add(ask.timeout_budget)
                .ok_or(RemoteMessageError::DeadlineExceeded)?;
            let result = dispatch
                .ask_entity(ask.target, ask.message_id, ask.payload, deadline)
                .await;
            Ok(inbound_ask_response(ask.correlation_id, result))
        }
        InboundAskWork::Singleton(ask) => {
            let deadline = Instant::now()
                .checked_add(ask.timeout_budget)
                .ok_or(RemoteMessageError::DeadlineExceeded)?;
            let result = dispatch
                .ask_singleton(ask.target, ask.message_id, ask.payload, deadline)
                .await;
            Ok(inbound_ask_response(ask.correlation_id, result))
        }
    }
}

fn inbound_ask_response(
    correlation_id: CorrelationId,
    result: Result<Bytes, RemoteMessageError>,
) -> Frame {
    match result {
        Ok(payload) => reply_frame(correlation_id, payload),
        Err(error) => failure_frame(&RemoteFailure {
            correlation_id,
            code: failure_code(&error),
            safe_detail: None,
        }),
    }
}
