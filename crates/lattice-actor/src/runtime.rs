use tokio::sync::mpsc;

use crate::mailbox::{ActorCommand, MailboxConfig};
use crate::{Actor, ActorContext, ActorHandle, StopReason};

pub fn spawn_actor<A>(actor: A, mailbox: MailboxConfig) -> ActorHandle<A>
where
    A: Actor,
{
    let (normal_tx, normal_rx) = mpsc::channel(mailbox.normal_capacity());
    let (system_tx, system_rx) = mpsc::channel(mailbox.system_capacity());
    let handle = ActorHandle::new(normal_tx, system_tx);

    tokio::spawn(run_actor(actor, handle.clone(), normal_rx, system_rx));

    handle
}

async fn run_actor<A>(
    mut actor: A,
    handle: ActorHandle<A>,
    mut normal_rx: mpsc::Receiver<ActorCommand<A>>,
    mut system_rx: mpsc::Receiver<ActorCommand<A>>,
) where
    A: Actor,
{
    let mut ctx = ActorContext::new(handle);

    if actor.started(&mut ctx).await.is_err() {
        let _ = actor.stopping(&mut ctx, StopReason::StartFailed).await;
        ctx.cancel_all_tasks();
        return;
    }

    let mut stop_reason = None;

    while stop_reason.is_none() {
        while let Ok(command) = system_rx.try_recv() {
            if handle_command(command, &mut actor, &mut ctx, &mut stop_reason).await {
                break;
            }
        }

        if stop_reason.is_some() {
            break;
        }

        tokio::select! {
            biased;

            command = system_rx.recv() => {
                match command {
                    Some(command) => {
                        handle_command(command, &mut actor, &mut ctx, &mut stop_reason).await;
                    }
                    None if normal_rx.is_closed() => {
                        stop_reason = Some(StopReason::MailboxClosed);
                    }
                    None => {}
                }
            }
            command = normal_rx.recv() => {
                match command {
                    Some(command) => {
                        handle_command(command, &mut actor, &mut ctx, &mut stop_reason).await;
                    }
                    None if system_rx.is_closed() => {
                        stop_reason = Some(StopReason::MailboxClosed);
                    }
                    None => {}
                }
            }
        }
    }

    let _ = actor
        .stopping(&mut ctx, stop_reason.unwrap_or(StopReason::Requested))
        .await;
    ctx.cancel_all_tasks();
}

async fn handle_command<A>(
    command: ActorCommand<A>,
    actor: &mut A,
    ctx: &mut ActorContext<A>,
    stop_reason: &mut Option<StopReason>,
) -> bool
where
    A: Actor,
{
    match command {
        ActorCommand::Envelope(envelope) => {
            envelope.handle(actor, ctx).await;
            if let Some(requested_reason) = ctx.take_lifecycle_request() {
                *stop_reason = Some(requested_reason);
                return true;
            }
        }
        ActorCommand::Stop(reason) => {
            *stop_reason = Some(reason);
            return true;
        }
    }

    false
}
