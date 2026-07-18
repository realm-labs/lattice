use std::any::Any;

#[derive(Debug)]
struct ActorPanic {
    phase: &'static str,
    message: String,
}

impl ActorPanic {
    fn new(phase: &'static str, payload: Box<dyn Any + Send>) -> Self {
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

fn terminate_panicked_actor<A>(
    actor: A,
    ctx: &mut ActorContext<A>,
    handle: &ActorHandle<A>,
    normal_rx: &mut mpsc::Receiver<ActorCommand<A>>,
    system_rx: &mut mpsc::Receiver<ActorCommand<A>>,
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
    reject_queued_commands(normal_rx, MailboxLane::Normal, handle);
    reject_queued_commands(system_rx, MailboxLane::System, handle);

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

fn reject_queued_commands<A>(
    receiver: &mut mpsc::Receiver<ActorCommand<A>>,
    lane: MailboxLane,
    handle: &ActorHandle<A>,
) where
    A: Actor,
{
    while let Ok(command) = receiver.try_recv() {
        match command {
            ActorCommand::Envelope(mut envelope) => {
                let metadata = envelope.metadata(lane);
                if let Some(completion) = envelope.reject_panicked() {
                    handle.observer().request_completed(
                        handle.observation_metadata(),
                        &metadata,
                        completion,
                    );
                }
            }
            ActorCommand::RetryStop(result)
            | ActorCommand::Quarantine(result)
            | ActorCommand::ForceStop { result, .. } => {
                let _ = result.send(Err(ActorAdminError::MailboxClosed));
            }
            ActorCommand::Stop(_) => {}
        }
    }
}

fn finalize_panicked_actor<A>(handle: &ActorHandle<A>, panic: ActorPanic)
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
    handle.observer().lifecycle(
        handle.observation_metadata(),
        ActorLifecycleEvent::Panicked,
    );
    handle.publish_terminated(ActorTerminated {
        target: handle.local_ref(),
        incarnation: ActorIncarnation::new(handle.local_ref().id()),
        reason: TerminatedReason::Panicked,
    });
}
