use std::{any::type_name, iter::once, panic::AssertUnwindSafe, time::Instant};

use futures_util::FutureExt;
use tracing::{Instrument, debug};

use crate::{
    context::ActorContext,
    error::ActorAdminError,
    handle::ActorHandle,
    mailbox::{ActorCommand, MailboxLane, QueuedRejection},
    traits::{Actor, MessageOutcome, StopReason},
};

use super::{panic::ActorPanic, rejection::reject_prefetched_commands};

pub(super) struct ActorInstance<'a, A: Actor> {
    pub(super) actor: &'a mut A,
    pub(super) behavior: &'a mut A::Behavior,
}

pub(super) async fn handle_command<A>(
    command: ActorCommand<A>,
    lane: MailboxLane,
    handle: &ActorHandle<A>,
    instance: ActorInstance<'_, A>,
    ctx: &mut ActorContext<A>,
    stop_reason: &mut Option<StopReason>,
) -> Result<bool, ActorPanic>
where
    A: Actor,
{
    match command {
        ActorCommand::Envelope(mut envelope) => {
            if handle.business_admission_fenced() {
                reject_prefetched_commands(
                    once(ActorCommand::Envelope(envelope)),
                    lane,
                    handle,
                    QueuedRejection::MailboxClosed,
                );
                *stop_reason = Some(StopReason::Requested);
                return Ok(true);
            }
            let metadata = envelope.metadata(lane);
            let actor_metadata = handle.observation_metadata();
            let observation_started_at = handle.observer().is_enabled().then(Instant::now);
            if observation_started_at.is_some() {
                handle.observer().message_started(actor_metadata, &metadata);
            }
            let span = tracing::info_span!(
                "actor.message",
                otel.kind = "consumer",
                actor.type = type_name::<A>(),
                message.type = metadata.type_name(),
                message.kind = ?metadata.kind(),
                mailbox.lane = lane.as_str()
            );
            debug!(
                actor.type = type_name::<A>(),
                message.type = metadata.type_name(),
                message.kind = ?metadata.kind(),
                mailbox.lane = lane.as_str(),
                "handling actor message"
            );
            let handled = {
                let future = envelope.handle(instance.actor, instance.behavior, ctx, &metadata);
                if span.is_disabled() {
                    AssertUnwindSafe(future).catch_unwind().await
                } else {
                    AssertUnwindSafe(future.instrument(span))
                        .catch_unwind()
                        .await
                }
            };
            let outcome = match handled {
                Ok(outcome) => outcome,
                Err(payload) => {
                    if let Some(completion) = envelope.reject(QueuedRejection::ActorPanicked) {
                        handle
                            .observer()
                            .request_completed(actor_metadata, &metadata, completion);
                    }
                    if let Some(started_at) = observation_started_at {
                        handle.observer().message_finished(
                            actor_metadata,
                            &metadata,
                            MessageOutcome::Panicked,
                            started_at.elapsed(),
                        );
                    }
                    return Err(ActorPanic::new("message", payload));
                }
            };
            if let Some(started_at) = observation_started_at {
                handle.observer().message_finished(
                    actor_metadata,
                    &metadata,
                    outcome,
                    started_at.elapsed(),
                );
            }
            ctx.reap_runtime_work();
            debug!(
                actor.type = type_name::<A>(),
                message.type = metadata.type_name(),
                message.kind = ?metadata.kind(),
                message.outcome = ?outcome,
                mailbox.lane = lane.as_str(),
                "actor message handled"
            );
            if handle.business_admission_fenced() {
                *stop_reason = Some(StopReason::Requested);
                return Ok(true);
            }
            if let Some(requested_reason) = ctx.take_lifecycle_request() {
                *stop_reason = Some(requested_reason);
                return Ok(true);
            }
        }
        ActorCommand::Stop(reason) => {
            debug!(
                actor.type = type_name::<A>(),
                mailbox.lane = lane.as_str(),
                stop.reason = ?reason,
                "actor stop requested"
            );
            *stop_reason = Some(reason);
            return Ok(true);
        }
        ActorCommand::RetryStop(result) => {
            let _ = result.send(Err(ActorAdminError::InvalidState {
                operation: "retry_stop",
                state: handle.lifecycle_state(),
            }));
        }
        ActorCommand::ForceStop { result, .. } => {
            let _ = result.send(Err(ActorAdminError::InvalidState {
                operation: "force_stop",
                state: handle.lifecycle_state(),
            }));
        }
    }

    Ok(false)
}
