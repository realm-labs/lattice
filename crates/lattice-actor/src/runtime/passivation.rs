use tokio::sync::watch;

use super::PassivationPolicy;
use crate::{
    error::ActorTellError,
    handle::ActorHandle,
    traits::{Actor, PassivationReason, StopReason},
};

pub(super) fn spawn_passivation_monitor<A>(
    handle: &ActorHandle<A>,
    passivation: PassivationPolicy,
) -> Option<watch::Sender<u64>>
where
    A: Actor,
{
    let PassivationPolicy::IdleTimeout(timeout) = passivation else {
        return None;
    };

    let (activity_tx, mut activity_rx) = watch::channel(0_u64);
    let handle = handle.clone();
    tokio::spawn(async move {
        loop {
            let observed = *activity_rx.borrow();
            tokio::select! {
                _ = tokio::time::sleep(timeout) => {
                    if *activity_rx.borrow() != observed {
                        continue;
                    }
                    // A transiently full system lane must not retire the monitor; the actor would
                    // then stay resident forever. Only a closed mailbox ends the retry loop.
                    match handle.try_stop_internal(StopReason::Passivated(
                        PassivationReason::IdleTimeout,
                    )) {
                        Err(ActorTellError::MailboxFull(_)) => {}
                        Ok(()) | Err(_) => break,
                    }
                }
                changed = activity_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
            }
        }
    });
    Some(activity_tx)
}

pub(super) fn record_activity(activity_tx: Option<&watch::Sender<u64>>) {
    if let Some(activity_tx) = activity_tx {
        let next = activity_tx.borrow().wrapping_add(1);
        activity_tx.send_replace(next);
    }
}
