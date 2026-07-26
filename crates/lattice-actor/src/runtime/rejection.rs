use crate::{
    error::ActorAdminError,
    handle::ActorHandle,
    mailbox::{ActorCommand, MailboxLane, QueuedRejection, channel::Receiver},
    observation::MailboxRejection,
    traits::Actor,
};

/// Completes every command still queued on one lane so that no admitted message
/// leaves the runtime without a caller result and an observer completion.
pub(super) fn reject_queued_commands<A>(
    receiver: &mut Receiver<ActorCommand<A>>,
    lane: MailboxLane,
    handle: &ActorHandle<A>,
    rejection: QueuedRejection,
) where
    A: Actor,
{
    while let Ok(command) = receiver.try_recv() {
        reject_command(command, lane, handle, rejection);
    }
}

/// Completes commands that a turn had already prefetched out of the mailbox and
/// will no longer dispatch.
pub(super) fn reject_prefetched_commands<A, I>(
    commands: I,
    lane: MailboxLane,
    handle: &ActorHandle<A>,
    rejection: QueuedRejection,
) where
    A: Actor,
    I: IntoIterator<Item = ActorCommand<A>>,
{
    for command in commands {
        reject_command(command, lane, handle, rejection);
    }
}

fn reject_command<A>(
    command: ActorCommand<A>,
    lane: MailboxLane,
    handle: &ActorHandle<A>,
    rejection: QueuedRejection,
) where
    A: Actor,
{
    match command {
        ActorCommand::Envelope(mut envelope) => {
            let metadata = envelope.metadata(lane);
            handle.observer().mailbox_rejected(
                handle.observation_metadata(),
                &metadata,
                MailboxRejection::Closed,
            );
            if let Some(completion) = envelope.reject(rejection) {
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
