use std::{
    any::{Any, type_name},
    panic::AssertUnwindSafe,
};

use tracing::error;

use super::rejection::reject_queued_commands;
use crate::{
    context::ActorContext,
    error::ActorCallError,
    handle::ActorHandle,
    mailbox::{ActorCommand, MailboxLane, QueuedRejection, channel::Receiver},
    observation::ActorLifecycleEvent,
    traits::{Actor, ActorLifecycleState, StopReason},
    watch::{ActorTermination, TerminatedReason},
};

#[derive(Debug)]
pub(super) struct ActorPanic {
    phase: &'static str,
    message: String,
}

impl ActorPanic {
    pub(super) fn new(phase: &'static str, payload: Box<dyn Any + Send>) -> Self {
        let message = match payload.downcast::<String>() {
            Ok(message) => *message,
            Err(payload) => match payload.downcast::<&'static str>() {
                Ok(message) => (*message).to_owned(),
                Err(_) => "non-string panic payload".to_owned(),
            },
        };
        Self { phase, message }
    }
}

pub(super) fn terminate_panicked_actor<A>(
    actor: A,
    ctx: &mut ActorContext<A>,
    handle: &ActorHandle<A>,
    normal_rx: &mut Receiver<ActorCommand<A>>,
    system_rx: &mut Receiver<ActorCommand<A>>,
    panic: ActorPanic,
) where
    A: Actor,
{
    handle.set_lifecycle_state(ActorLifecycleState::Stopping);
    normal_rx.close();
    system_rx.close();
    ctx.cancel_deferred_replies(ActorCallError::ActorPanicked);
    ctx.cancel_all_tasks();
    ctx.stop_all_children(StopReason::Requested);
    reject_queued_commands(
        normal_rx,
        MailboxLane::Normal,
        handle,
        QueuedRejection::ActorPanicked,
    );
    reject_queued_commands(
        system_rx,
        MailboxLane::System,
        handle,
        QueuedRejection::ActorPanicked,
    );

    if let Err(payload) = std::panic::catch_unwind(AssertUnwindSafe(|| drop(actor))) {
        let secondary = ActorPanic::new("drop", payload);
        error!(
            actor.type = type_name::<A>(),
            actor.local_ref = handle.local_ref().id(),
            panic.phase = secondary.phase,
            panic.message = %secondary.message,
            "actor panicked again while being dropped"
        );
    }

    finalize_panicked_actor(handle, panic);
}

pub(super) fn finalize_panicked_actor<A>(handle: &ActorHandle<A>, panic: ActorPanic)
where
    A: Actor,
{
    error!(
        actor.type = type_name::<A>(),
        actor.local_ref = handle.local_ref().id(),
        panic.phase = panic.phase,
        panic.message = %panic.message,
        "actor callback panicked; terminating actor"
    );
    handle.mark_terminal_cleanup_started();
    handle.run_terminal_hook();
    if handle.clear_stop_failure() {
        crate::observation::record_abandoned_stop_failure();
    }
    handle.set_lifecycle_state(ActorLifecycleState::Stopped);
    handle
        .observer()
        .lifecycle(handle.observation_metadata(), ActorLifecycleEvent::Panicked);
    handle.publish_terminated(ActorTermination {
        target: handle.local_ref(),
        reason: TerminatedReason::Panicked,
    });
}
