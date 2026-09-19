use std::{any::type_name, panic::AssertUnwindSafe, time::SystemTime};

use futures_util::FutureExt;
use tokio::sync::oneshot;
use tracing::{Instrument, debug, error, info};

use crate::{
    context::ActorContext,
    error::{ActorAdminError, ActorCallError},
    handle::{ActorHandle, ForcedDataLossEvent, StopFailureRecord},
    mailbox::{ActorCommand, MailboxLane, QueuedRejection, channel::Receiver},
    observation::{ActorLifecycleEvent, record_new_stop_failure, record_resolved_stop_failure},
    traits::{Actor, ActorLifecycleState, PassivationReason, StopReason},
    watch::{ActorTermination, TerminatedReason},
};

use super::{
    ActorRuntimeParts, PassivationPolicy,
    dispatch::{ActorInstance, handle_command},
    panic::{ActorPanic, finalize_panicked_actor, terminate_panicked_actor},
    passivation::wait_for_idle_timeout,
    rejection::{reject_prefetched_commands, reject_queued_commands},
};

// Match the default turn budget so the common saturated path releases mailbox capacity once per
// turn instead of once per message. Smaller turn budgets still cap the prefetch.
const NORMAL_RECEIVE_BATCH_SIZE: usize = 64;

pub(super) async fn run_actor<A>(
    mut actor: A,
    parts: ActorRuntimeParts<A>,
    passivation: PassivationPolicy,
) where
    A: Actor,
{
    let ActorRuntimeParts {
        handle,
        mut normal_rx,
        mut system_rx,
        service,
        runtime_attachments,
        spawner,
        deferred_capacity,
        turn_budget,
    } = parts;
    let mut ctx = ActorContext::new(
        handle.clone(),
        service,
        runtime_attachments,
        spawner,
        deferred_capacity,
    );
    let (mut behavior, initial_stop_reason) = match start_actor(&mut actor, &mut ctx, &handle).await
    {
        Ok(started) => started,
        Err(panic) => {
            terminate_panicked_actor(
                actor,
                &mut ctx,
                &handle,
                &mut normal_rx,
                &mut system_rx,
                panic,
            );
            return;
        }
    };
    let reason = match run_actor_loop(
        ActiveActor {
            actor: &mut actor,
            behavior: &mut behavior,
            ctx: &mut ctx,
            handle: &handle,
        },
        &mut normal_rx,
        &mut system_rx,
        passivation,
        turn_budget,
        initial_stop_reason,
    )
    .await
    {
        Ok(reason) => reason,
        Err(panic) => {
            terminate_panicked_actor(
                actor,
                &mut ctx,
                &handle,
                &mut normal_rx,
                &mut system_rx,
                panic,
            );
            return;
        }
    };

    ctx.cancel_deferred_replies(ActorCallError::MailboxClosed);
    ctx.cancel_all_tasks();
    ctx.stop_all_children(reason);
    let previous_phase = match reason {
        StopReason::Passivated(_) => ActorLifecycleState::Passivating,
        StopReason::Requested | StopReason::MailboxClosed | StopReason::StartFailed => {
            ActorLifecycleState::Stopping
        }
    };

    let (forced, terminal_completion) = match run_stopping_phase(
        &mut actor,
        &mut ctx,
        &handle,
        &mut normal_rx,
        &mut system_rx,
        reason,
        previous_phase,
    )
    .await
    {
        Ok(completion) => completion,
        Err(panic) => {
            terminate_panicked_actor(
                actor,
                &mut ctx,
                &handle,
                &mut normal_rx,
                &mut system_rx,
                panic,
            );
            return;
        }
    };

    finish_actor(
        actor,
        &handle,
        &mut normal_rx,
        &mut system_rx,
        reason,
        forced,
        terminal_completion,
    );
}

async fn start_actor<A>(
    actor: &mut A,
    ctx: &mut ActorContext<A>,
    handle: &ActorHandle<A>,
) -> Result<(A::Behavior, Option<StopReason>), ActorPanic>
where
    A: Actor,
{
    let behavior = std::panic::catch_unwind(AssertUnwindSafe(|| actor.initial_behavior()))
        .map_err(|payload| ActorPanic::new("initial_behavior", payload))?;
    if handle.business_admission_fenced() {
        return Ok((behavior, Some(StopReason::Requested)));
    }

    let actor_type = type_name::<A>();
    let local_ref = handle.local_ref().id();
    let started_span = tracing::info_span!(
        "actor.started",
        otel.kind = "internal",
        actor.type = actor_type,
        actor.local_ref = local_ref
    );
    let stop_reason = match AssertUnwindSafe(actor.started(ctx).instrument(started_span))
        .catch_unwind()
        .await
    {
        Ok(Err(error)) => {
            handle.observer().lifecycle(
                handle.observation_metadata(),
                ActorLifecycleEvent::StartFailed,
            );
            error!(
                actor.type = actor_type,
                actor.local_ref = local_ref,
                %error,
                "actor failed to start"
            );
            Some(StopReason::StartFailed)
        }
        Ok(Ok(())) => {
            handle.set_lifecycle_state(ActorLifecycleState::Running);
            handle
                .observer()
                .lifecycle(handle.observation_metadata(), ActorLifecycleEvent::Started);
            info!(
                actor.type = actor_type,
                actor.local_ref = local_ref,
                "actor started"
            );
            handle
                .business_admission_fenced()
                .then_some(StopReason::Requested)
        }
        Err(payload) => return Err(ActorPanic::new("started", payload)),
    };

    Ok((behavior, stop_reason))
}

struct ActiveActor<'a, A>
where
    A: Actor,
{
    actor: &'a mut A,
    behavior: &'a mut A::Behavior,
    ctx: &'a mut ActorContext<A>,
    handle: &'a ActorHandle<A>,
}

impl<A> ActiveActor<'_, A>
where
    A: Actor,
{
    async fn handle_command(
        &mut self,
        command: ActorCommand<A>,
        lane: MailboxLane,
        stop_reason: &mut Option<StopReason>,
    ) -> Result<bool, ActorPanic> {
        handle_command(
            command,
            lane,
            self.handle,
            ActorInstance {
                actor: self.actor,
                behavior: self.behavior,
            },
            self.ctx,
            stop_reason,
        )
        .await
    }
}

async fn run_actor_loop<A>(
    mut active: ActiveActor<'_, A>,
    normal_rx: &mut Receiver<ActorCommand<A>>,
    system_rx: &mut Receiver<ActorCommand<A>>,
    passivation: PassivationPolicy,
    turn_budget: usize,
    mut stop_reason: Option<StopReason>,
) -> Result<StopReason, ActorPanic>
where
    A: Actor,
{
    let mut normal_batch = Vec::with_capacity(NORMAL_RECEIVE_BATCH_SIZE.min(turn_budget));
    while stop_reason.is_none() {
        if active.handle.business_admission_fenced() {
            stop_reason = Some(StopReason::Requested);
            break;
        }
        drain_system_lane(&mut active, system_rx, &mut stop_reason).await?;

        if stop_reason.is_some() {
            break;
        }

        tokio::select! {
            biased;

            _ = active.handle.wait_business_fenced() => {
                stop_reason = Some(StopReason::Requested);
            }
            command = system_rx.recv() => {
                match command {
                    Some(command) => {
                        active
                            .handle_command(command, MailboxLane::System, &mut stop_reason)
                            .await?;
                    }
                    None if normal_rx.is_closed() => {
                        stop_reason = Some(StopReason::MailboxClosed);
                    }
                    None => {}
                }
            }
            command = normal_rx.recv() => {
                match command {
                    Some(first_command) => {
                        run_normal_turn(
                            first_command,
                            &mut active,
                            normal_rx,
                            &mut normal_batch,
                            turn_budget,
                            &mut stop_reason,
                        )
                        .await?;
                    }
                    None if system_rx.is_closed() => {
                        stop_reason = Some(StopReason::MailboxClosed);
                    }
                    None => {}
                }
            }
            _ = wait_for_idle_timeout(passivation) => {
                stop_reason = Some(StopReason::Passivated(
                    PassivationReason::IdleTimeout,
                ));
            }
        }
    }

    Ok(stop_reason.unwrap_or(StopReason::Requested))
}

async fn drain_system_lane<A>(
    active: &mut ActiveActor<'_, A>,
    system_rx: &mut Receiver<ActorCommand<A>>,
    stop_reason: &mut Option<StopReason>,
) -> Result<(), ActorPanic>
where
    A: Actor,
{
    while let Ok(command) = system_rx.try_recv() {
        let should_stop = active
            .handle_command(command, MailboxLane::System, stop_reason)
            .await?;
        if should_stop {
            break;
        }
    }
    Ok(())
}

async fn run_normal_turn<A>(
    first_command: ActorCommand<A>,
    active: &mut ActiveActor<'_, A>,
    normal_rx: &mut Receiver<ActorCommand<A>>,
    normal_batch: &mut Vec<ActorCommand<A>>,
    turn_budget: usize,
    stop_reason: &mut Option<StopReason>,
) -> Result<(), ActorPanic>
where
    A: Actor,
{
    let mut remaining = turn_budget;
    normal_batch.push(first_command);
    normal_rx.try_recv_batch(
        normal_batch,
        NORMAL_RECEIVE_BATCH_SIZE.min(remaining).saturating_sub(1),
    );
    loop {
        let mut actor_panic = None;
        let mut prefetched = normal_batch.drain(..);
        for current in prefetched.by_ref() {
            match active
                .handle_command(current, MailboxLane::Normal, stop_reason)
                .await
            {
                Ok(_) => {}
                Err(panic) => {
                    actor_panic = Some(panic);
                    break;
                }
            }
            remaining -= 1;
            if stop_reason.is_some() || remaining == 0 {
                break;
            }
        }
        // Messages already taken out of the channel can no longer be rejected by the shutdown
        // drain, so complete them here with the same reason their still-queued peers receive.
        reject_prefetched_commands(
            prefetched,
            MailboxLane::Normal,
            active.handle,
            if actor_panic.is_some() {
                QueuedRejection::ActorPanicked
            } else {
                QueuedRejection::MailboxClosed
            },
        );
        if let Some(panic) = actor_panic {
            return Err(panic);
        }
        if stop_reason.is_some() || remaining == 0 {
            break;
        }
        let received =
            normal_rx.try_recv_batch(normal_batch, NORMAL_RECEIVE_BATCH_SIZE.min(remaining));
        if received == 0 {
            break;
        }
    }
    Ok(())
}

fn finish_actor<A>(
    actor: A,
    handle: &ActorHandle<A>,
    normal_rx: &mut Receiver<ActorCommand<A>>,
    system_rx: &mut Receiver<ActorCommand<A>>,
    reason: StopReason,
    forced: bool,
    terminal_completion: Option<oneshot::Sender<Result<(), ActorAdminError>>>,
) where
    A: Actor,
{
    handle.clear_stop_failure();
    // A stopped Actor never dispatches what is still queued, so both lanes are closed and drained
    // here instead of being discarded silently when the receivers drop.
    normal_rx.close();
    system_rx.close();
    reject_queued_commands(
        normal_rx,
        MailboxLane::Normal,
        handle,
        QueuedRejection::MailboxClosed,
    );
    reject_queued_commands(
        system_rx,
        MailboxLane::System,
        handle,
        QueuedRejection::MailboxClosed,
    );
    if let Err(payload) = std::panic::catch_unwind(AssertUnwindSafe(|| drop(actor))) {
        finalize_panicked_actor(handle, ActorPanic::new("drop", payload));
        return;
    }
    handle.mark_terminal_cleanup_started();
    handle.run_terminal_hook();
    handle.set_lifecycle_state(ActorLifecycleState::Stopped);
    handle.observer().lifecycle(
        handle.observation_metadata(),
        if forced {
            ActorLifecycleEvent::ForcedDataLoss(reason)
        } else {
            ActorLifecycleEvent::Stopped(reason)
        },
    );
    handle.publish_terminated(ActorTermination {
        target: handle.local_ref(),
        reason: TerminatedReason::from(reason),
    });
    if let Some(completion) = terminal_completion {
        let _ = completion.send(Ok(()));
    }
    info!(
        actor.type = type_name::<A>(),
        actor.local_ref = handle.local_ref().id(),
        stop.reason = ?reason,
        forced,
        "actor stopped"
    );
}

async fn run_stopping_phase<A>(
    actor: &mut A,
    ctx: &mut ActorContext<A>,
    handle: &ActorHandle<A>,
    normal_rx: &mut Receiver<ActorCommand<A>>,
    system_rx: &mut Receiver<ActorCommand<A>>,
    reason: StopReason,
    previous_phase: ActorLifecycleState,
) -> Result<(bool, Option<oneshot::Sender<Result<(), ActorAdminError>>>), ActorPanic>
where
    A: Actor,
{
    let actor_type = type_name::<A>();
    let local_ref = handle.local_ref().id();
    let mut failure: Option<StopFailureRecord> = None;
    let mut retry_result: Option<oneshot::Sender<Result<(), ActorAdminError>>> = None;
    loop {
        handle.set_lifecycle_state(previous_phase);
        if failure.is_some() {
            handle.observer().lifecycle(
                handle.observation_metadata(),
                ActorLifecycleEvent::StopRetried(reason),
            );
        }
        let stopping_span = tracing::info_span!(
            "actor.stopping",
            otel.kind = "internal",
            actor.type = actor_type,
            actor.local_ref = local_ref,
            stop.reason = ?reason
        );
        match AssertUnwindSafe(actor.stopping(ctx, reason).instrument(stopping_span))
            .catch_unwind()
            .await
        {
            Err(payload) => return Err(ActorPanic::new("stopping", payload)),
            Ok(Ok(())) => {
                if failure.is_some() {
                    record_resolved_stop_failure(false);
                }
                return Ok((false, retry_result.take()));
            }
            Ok(Err(stop_error)) => {
                let now = SystemTime::now();
                let record = match failure.take() {
                    Some(mut record) => {
                        record.error = stop_error.message().to_owned();
                        record.latest_attempt_time = now;
                        record.attempt_count = record.attempt_count.saturating_add(1);
                        record
                    }
                    None => {
                        record_new_stop_failure();
                        StopFailureRecord {
                            reason,
                            previous_phase,
                            error: stop_error.message().to_owned(),
                            first_failure_time: now,
                            latest_attempt_time: now,
                            attempt_count: 1,
                        }
                    }
                };
                error!(
                    actor.type = actor_type,
                    actor.local_ref = local_ref,
                    stop.reason = ?reason,
                    stop.attempt = record.attempt_count,
                    error = %stop_error,
                    "actor failed to persist while stopping; retaining actor instance"
                );
                handle.record_stop_failure(record.clone());
                failure = Some(record);
                handle.set_lifecycle_state(ActorLifecycleState::StopFailed);
                handle.observer().lifecycle(
                    handle.observation_metadata(),
                    ActorLifecycleEvent::StopFailed(reason),
                );
                normal_rx.close();
                reject_queued_commands(
                    normal_rx,
                    MailboxLane::Normal,
                    handle,
                    QueuedRejection::MailboxClosed,
                );
                if let Some(result) = retry_result.take() {
                    let _ = result.send(Err(ActorAdminError::StopFailed(stop_error)));
                }
            }
        }

        loop {
            match system_rx.recv().await {
                Some(ActorCommand::RetryStop(result)) => {
                    retry_result = Some(result);
                    break;
                }
                Some(ActorCommand::ForceStop {
                    authorization,
                    result,
                }) => {
                    let failed_attempts = failure.as_ref().map_or(0, |record| record.attempt_count);
                    error!(
                        actor.type = actor_type,
                        actor.local_ref = local_ref,
                        stop.reason = ?reason,
                        force.reason = %authorization.reason,
                        force.ticket = %authorization.ticket,
                        failed_attempts,
                        "operator-authorized force stop discards retained actor state"
                    );
                    handle.publish_forced_data_loss(ForcedDataLossEvent {
                        target: handle.local_ref(),
                        stop_reason: reason,
                        reason: authorization.reason,
                        ticket: authorization.ticket,
                        failed_attempts,
                    });
                    record_resolved_stop_failure(true);
                    return Ok((true, Some(result)));
                }
                Some(ActorCommand::Stop(requested_reason)) => {
                    debug!(
                        actor.type = actor_type,
                        actor.local_ref = local_ref,
                        original.reason = ?reason,
                        requested.reason = ?requested_reason,
                        "retained actor ignored duplicate stop request"
                    );
                }
                Some(ActorCommand::Envelope(_)) => {
                    error!(
                        actor.type = actor_type,
                        actor.local_ref = local_ref,
                        "retained actor rejected a system-lane business envelope"
                    );
                }
                None => {
                    // ActorContext retains a self handle, so this is reachable only during runtime
                    // teardown. Keep the actor alive until that teardown drops the task.
                    std::future::pending::<()>().await;
                }
            }
        }
    }
}
