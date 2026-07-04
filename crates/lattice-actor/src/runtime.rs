use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::{broadcast, watch};

use crate::mailbox::{ActorCommand, MailboxConfig};
use crate::{
    Actor, ActorContext, ActorHandle, ActorIncarnation, ActorLifecycleState, ActorTerminated,
    LocalActorRef, PassivationReason, StopReason, TerminatedReason,
};

static NEXT_LOCAL_ACTOR_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorExecutionPolicy {
    TaskPerActor,
    ShardWorker { worker_count: usize },
    DedicatedThreadPool { worker_count: usize },
}

#[derive(Debug, Clone)]
pub struct ActorRuntimeConfig {
    pub default_execution: ActorExecutionPolicy,
}

impl Default for ActorRuntimeConfig {
    fn default() -> Self {
        Self {
            default_execution: ActorExecutionPolicy::TaskPerActor,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ActorSpawnOptions {
    pub mailbox: MailboxConfig,
    pub execution: Option<ActorExecutionPolicy>,
    pub passivation: PassivationPolicy,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PassivationPolicy {
    #[default]
    Disabled,
    IdleTimeout(Duration),
}

#[derive(Debug, Clone)]
pub struct ActorRuntime {
    config: ActorRuntimeConfig,
    scheduler: ActorScheduler,
}

impl ActorRuntime {
    pub fn new(config: ActorRuntimeConfig) -> Self {
        Self {
            config,
            scheduler: ActorScheduler,
        }
    }

    pub fn scheduler(&self) -> &ActorScheduler {
        &self.scheduler
    }

    pub async fn spawn_actor<A>(
        &self,
        actor: A,
        options: ActorSpawnOptions,
    ) -> Result<ActorHandle<A>, crate::ActorSpawnError>
    where
        A: Actor,
    {
        self.spawn_actor_now(actor, options)
    }

    pub(crate) fn spawn_actor_now<A>(
        &self,
        actor: A,
        options: ActorSpawnOptions,
    ) -> Result<ActorHandle<A>, crate::ActorSpawnError>
    where
        A: Actor,
    {
        let execution = options.execution.unwrap_or(self.config.default_execution);
        self.scheduler.spawn(actor, options, execution)
    }
}

impl Default for ActorRuntime {
    fn default() -> Self {
        Self::new(ActorRuntimeConfig::default())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ActorScheduler;

impl ActorScheduler {
    fn spawn<A>(
        &self,
        actor: A,
        options: ActorSpawnOptions,
        execution: ActorExecutionPolicy,
    ) -> Result<ActorHandle<A>, crate::ActorSpawnError>
    where
        A: Actor,
    {
        match execution {
            ActorExecutionPolicy::TaskPerActor => Ok(spawn_task_per_actor(actor, options)),
            ActorExecutionPolicy::ShardWorker { .. }
            | ActorExecutionPolicy::DedicatedThreadPool { .. } => {
                Err(crate::ActorSpawnError::UnsupportedExecutionPolicy { policy: execution })
            }
        }
    }
}

pub fn spawn_actor<A>(actor: A, mailbox: MailboxConfig) -> ActorHandle<A>
where
    A: Actor,
{
    ActorRuntime::default()
        .spawn_actor_now(
            actor,
            ActorSpawnOptions {
                mailbox,
                execution: Some(ActorExecutionPolicy::TaskPerActor),
                passivation: PassivationPolicy::Disabled,
            },
        )
        .expect("TaskPerActor execution is supported")
}

fn spawn_task_per_actor<A>(actor: A, options: ActorSpawnOptions) -> ActorHandle<A>
where
    A: Actor,
{
    let mailbox = options.mailbox;
    let (normal_tx, normal_rx) = mpsc::channel(mailbox.normal_capacity());
    let (system_tx, system_rx) = mpsc::channel(mailbox.system_capacity());
    let local_ref = LocalActorRef::new(NEXT_LOCAL_ACTOR_ID.fetch_add(1, Ordering::Relaxed));
    let (terminated_tx, _terminated_rx) = broadcast::channel(16);
    let (lifecycle_tx, _lifecycle_rx) = watch::channel(ActorLifecycleState::Empty);
    let handle = ActorHandle::new(local_ref, terminated_tx, lifecycle_tx, normal_tx, system_tx);

    tokio::spawn(run_actor(
        actor,
        handle.clone(),
        normal_rx,
        system_rx,
        options.passivation,
    ));

    handle
}

async fn run_actor<A>(
    mut actor: A,
    handle: ActorHandle<A>,
    mut normal_rx: mpsc::Receiver<ActorCommand<A>>,
    mut system_rx: mpsc::Receiver<ActorCommand<A>>,
    passivation: PassivationPolicy,
) where
    A: Actor,
{
    let mut ctx = ActorContext::new(handle.clone());
    let activity_tx = spawn_passivation_monitor(&handle, passivation);

    if actor.started(&mut ctx).await.is_err() {
        handle.set_lifecycle_state(ActorLifecycleState::Stopping);
        if actor
            .stopping(&mut ctx, StopReason::StartFailed)
            .await
            .is_err()
        {
            handle.set_lifecycle_state(ActorLifecycleState::StopFailed);
        } else {
            handle.set_lifecycle_state(ActorLifecycleState::Stopped);
        }
        ctx.cancel_all_tasks();
        return;
    }
    handle.set_lifecycle_state(ActorLifecycleState::Running);

    let mut stop_reason = None;

    while stop_reason.is_none() {
        while let Ok(command) = system_rx.try_recv() {
            if handle_command(
                command,
                &mut actor,
                &mut ctx,
                &mut stop_reason,
                activity_tx.as_ref(),
            )
            .await
            {
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
                        handle_command(
                            command,
                            &mut actor,
                            &mut ctx,
                            &mut stop_reason,
                            activity_tx.as_ref(),
                        )
                        .await;
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
                        handle_command(
                            command,
                            &mut actor,
                            &mut ctx,
                            &mut stop_reason,
                            activity_tx.as_ref(),
                        )
                        .await;
                    }
                    None if system_rx.is_closed() => {
                        stop_reason = Some(StopReason::MailboxClosed);
                    }
                    None => {}
                }
            }
        }
    }

    let reason = stop_reason.unwrap_or(StopReason::Requested);
    handle.set_lifecycle_state(match reason {
        StopReason::Passivated(_) => ActorLifecycleState::Passivating,
        StopReason::Requested | StopReason::MailboxClosed | StopReason::StartFailed => {
            ActorLifecycleState::Stopping
        }
    });
    if actor.stopping(&mut ctx, reason).await.is_err() {
        ctx.cancel_all_tasks();
        ctx.stop_all_children(reason);
        handle.set_lifecycle_state(ActorLifecycleState::StopFailed);
        return;
    }
    ctx.cancel_all_tasks();
    ctx.stop_all_children(reason);
    handle.set_lifecycle_state(ActorLifecycleState::Stopped);
    handle.publish_terminated(ActorTerminated {
        target: handle.local_ref(),
        incarnation: ActorIncarnation::new(handle.local_ref().id()),
        reason: TerminatedReason::from(reason),
    });
}

async fn handle_command<A>(
    command: ActorCommand<A>,
    actor: &mut A,
    ctx: &mut ActorContext<A>,
    stop_reason: &mut Option<StopReason>,
    activity_tx: Option<&watch::Sender<u64>>,
) -> bool
where
    A: Actor,
{
    match command {
        ActorCommand::Envelope(envelope) => {
            envelope.handle(actor, ctx).await;
            record_activity(activity_tx);
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

fn spawn_passivation_monitor<A>(
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
                    if *activity_rx.borrow() == observed {
                        let _ = handle.try_stop_internal(StopReason::Passivated(
                            PassivationReason::IdleTimeout,
                        ));
                        break;
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

fn record_activity(activity_tx: Option<&watch::Sender<u64>>) {
    if let Some(activity_tx) = activity_tx {
        let next = activity_tx.borrow().wrapping_add(1);
        activity_tx.send_replace(next);
    }
}
