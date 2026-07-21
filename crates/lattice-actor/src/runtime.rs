use std::{
    any::{TypeId, type_name},
    collections::HashMap,
    fmt::{Debug, Formatter, Result as FmtResult},
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use futures_util::FutureExt;
use lattice_core::{
    actor_ref::{ActivationId, ActorRef, NodeIncarnation},
    id::ActorId,
    service_context::ServiceContext,
};
use tokio::sync::{broadcast, oneshot, watch};
use tracing::{Instrument, debug, error, info};

use crate::{
    context::ActorContext,
    error::{ActorAdminError, ActorCallError, ActorSpawnError},
    handle::{ActorHandle, ActorHandleInit, ForcedDataLossEvent, StopFailureRecord, TerminalHook},
    mailbox::{
        ActorCommand, MailboxConfig, MailboxLane,
        channel::{self, Receiver},
    },
    observation::{ActorLifecycleEvent, ActorObserverHandle},
    recipient::ActorSystem,
    traits::{Actor, ActorLifecycleState, MessageOutcome, PassivationReason, StopReason},
    watch::{ActorIncarnation, ActorTerminated, LocalActorRef, TerminatedReason},
};

pub(crate) mod spawner;
mod worker_pool;

use spawner::ActorSpawner;
use worker_pool::{ActorWorkerPool, WorkerPoolKind};

static NEXT_LOCAL_ACTOR_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_ACTIVATION_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub(crate) fn next_activation_id(node_incarnation: NodeIncarnation) -> ActivationId {
    let sequence = NEXT_ACTIVATION_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    ActivationId::new(node_incarnation, sequence).expect("process activation sequence is nonzero")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorExecutionPolicy {
    TaskPerActor,
    KeyedWorkerPool { worker_count: usize },
    DedicatedThreadPool { worker_count: usize },
}

#[derive(Debug, Clone)]
pub struct ActorRuntimeConfig {
    pub default_execution: ActorExecutionPolicy,
    pub observer: ActorObserverHandle,
}

impl Default for ActorRuntimeConfig {
    fn default() -> Self {
        Self {
            default_execution: ActorExecutionPolicy::TaskPerActor,
            observer: ActorObserverHandle::default(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ActorSpawnOptions {
    pub mailbox: MailboxConfig,
    pub execution: Option<ActorExecutionPolicy>,
    pub scheduler_key: Option<ActorId>,
    pub passivation: PassivationPolicy,
    pub self_ref: Option<ActorRef>,
    pub service: ServiceContext,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PassivationPolicy {
    #[default]
    Disabled,
    IdleTimeout(Duration),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ShardMigrationPolicy {
    #[default]
    BlockRunningActors,
    PassivateRunningActors,
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
            scheduler: ActorScheduler::default(),
        }
    }

    pub fn scheduler(&self) -> &ActorScheduler {
        &self.scheduler
    }

    pub async fn spawn_actor<A>(
        &self,
        actor: A,
        options: ActorSpawnOptions,
    ) -> Result<ActorHandle<A>, ActorSpawnError>
    where
        A: Actor,
    {
        self.spawn_actor_now(actor, options)
    }

    pub(crate) fn spawn_actor_now<A>(
        &self,
        actor: A,
        options: ActorSpawnOptions,
    ) -> Result<ActorHandle<A>, ActorSpawnError>
    where
        A: Actor,
    {
        let spawner = ActorSpawner::new(self.scheduler.clone(), self.config.default_execution);
        spawner.spawn(
            actor,
            ActorSpawnContext {
                options,
                actor_system: None,
                observer: self.config.observer.clone(),
                terminal_hook: None,
                spawner: spawner.clone(),
            },
        )
    }
}

impl Default for ActorRuntime {
    fn default() -> Self {
        Self::new(ActorRuntimeConfig::default())
    }
}

#[derive(Clone)]
pub struct ActorScheduler {
    pools: Arc<SchedulerPools>,
}

impl Debug for ActorScheduler {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("ActorScheduler").finish_non_exhaustive()
    }
}

impl Default for ActorScheduler {
    fn default() -> Self {
        Self {
            pools: Arc::new(SchedulerPools::default()),
        }
    }
}

#[derive(Default)]
struct SchedulerPools {
    keyed_workers: Mutex<HashMap<usize, Arc<ActorWorkerPool>>>,
    dedicated_workers: Mutex<HashMap<DedicatedPoolKey, Arc<ActorWorkerPool>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct DedicatedPoolKey {
    actor_type: TypeId,
    worker_count: usize,
}

impl ActorScheduler {
    pub fn keyed_worker_index(
        actor_id: &ActorId,
        worker_count: usize,
    ) -> Result<usize, ActorSpawnError> {
        if worker_count == 0 {
            return Err(ActorSpawnError::InvalidExecutionPolicy {
                reason: "KeyedWorkerPool worker_count must be greater than zero",
            });
        }
        Ok((stable_actor_id_hash(actor_id) % worker_count as u64) as usize)
    }

    fn spawn<A>(
        &self,
        actor: A,
        context: ActorSpawnContext,
        execution: ActorExecutionPolicy,
    ) -> Result<ActorHandle<A>, ActorSpawnError>
    where
        A: Actor,
    {
        match execution {
            ActorExecutionPolicy::TaskPerActor => Ok(spawn_task_per_actor(actor, context)),
            ActorExecutionPolicy::KeyedWorkerPool { worker_count } => {
                if worker_count == 0 {
                    return Err(ActorSpawnError::InvalidExecutionPolicy {
                        reason: "KeyedWorkerPool worker_count must be greater than zero",
                    });
                }
                self.spawn_keyed_worker_pool_actor(actor, context, worker_count)
            }
            ActorExecutionPolicy::DedicatedThreadPool { worker_count } => {
                if worker_count == 0 {
                    return Err(ActorSpawnError::InvalidExecutionPolicy {
                        reason: "DedicatedThreadPool worker_count must be greater than zero",
                    });
                }
                self.spawn_dedicated_pool_actor(actor, context, worker_count)
            }
        }
    }

    fn spawn_keyed_worker_pool_actor<A>(
        &self,
        actor: A,
        context: ActorSpawnContext,
        worker_count: usize,
    ) -> Result<ActorHandle<A>, ActorSpawnError>
    where
        A: Actor,
    {
        let pool = self.keyed_worker_pool(worker_count)?;
        let (parts, passivation, scheduler_key) = context.into_parts();
        let scheduler_key =
            scheduler_key.unwrap_or_else(|| ActorId::U64(parts.handle.local_ref().id()));
        let worker_index = Self::keyed_worker_index(&scheduler_key, worker_count)?;
        Ok(spawn_actor_on_pool(
            actor,
            parts,
            passivation,
            &pool,
            worker_index,
            "keyed_worker_pool",
        ))
    }

    fn spawn_dedicated_pool_actor<A>(
        &self,
        actor: A,
        context: ActorSpawnContext,
        worker_count: usize,
    ) -> Result<ActorHandle<A>, ActorSpawnError>
    where
        A: Actor,
    {
        let pool = self.dedicated_worker_pool::<A>(worker_count)?;
        let worker_index = pool.next_worker_index();
        let (parts, passivation, _scheduler_key) = context.into_parts();
        Ok(spawn_actor_on_pool(
            actor,
            parts,
            passivation,
            &pool,
            worker_index,
            "dedicated_thread_pool",
        ))
    }

    fn keyed_worker_pool(
        &self,
        worker_count: usize,
    ) -> Result<Arc<ActorWorkerPool>, ActorSpawnError> {
        let mut pools = self
            .pools
            .keyed_workers
            .lock()
            .expect("actor worker pool mutex poisoned");
        if let Some(pool) = pools.get(&worker_count) {
            return Ok(pool.clone());
        }

        let pool = Arc::new(ActorWorkerPool::start(WorkerPoolKind::Keyed, worker_count)?);
        pools.insert(worker_count, pool.clone());
        Ok(pool)
    }

    fn dedicated_worker_pool<A>(
        &self,
        worker_count: usize,
    ) -> Result<Arc<ActorWorkerPool>, ActorSpawnError>
    where
        A: Actor,
    {
        let key = DedicatedPoolKey {
            actor_type: TypeId::of::<A>(),
            worker_count,
        };
        let mut pools = self
            .pools
            .dedicated_workers
            .lock()
            .expect("actor worker pool mutex poisoned");
        if let Some(pool) = pools.get(&key) {
            return Ok(pool.clone());
        }

        let pool = Arc::new(ActorWorkerPool::start(
            WorkerPoolKind::Dedicated {
                actor_type: type_name::<A>(),
            },
            worker_count,
        )?);
        pools.insert(key, pool.clone());
        Ok(pool)
    }
}

fn stable_actor_id_hash(actor_id: &ActorId) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    fn write(hash: &mut u64, bytes: &[u8]) {
        for byte in bytes {
            *hash ^= u64::from(*byte);
            *hash = hash.wrapping_mul(0x100000001b3);
        }
    }

    match actor_id {
        ActorId::Str(value) => {
            write(&mut hash, b"str");
            write(&mut hash, value.as_bytes());
        }
        ActorId::U64(value) => {
            write(&mut hash, b"u64");
            write(&mut hash, &value.to_be_bytes());
        }
        ActorId::I64(value) => {
            write(&mut hash, b"i64");
            write(&mut hash, &value.to_be_bytes());
        }
        ActorId::Bytes(value) => {
            write(&mut hash, b"bytes");
            write(&mut hash, value);
        }
    }
    hash
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
                scheduler_key: None,
                passivation: PassivationPolicy::Disabled,
                self_ref: None,
                service: ServiceContext::empty(),
            },
        )
        .expect("TaskPerActor execution is supported")
}

pub fn spawn_actor_with_context<A>(
    actor: A,
    mailbox: MailboxConfig,
    service: ServiceContext,
) -> ActorHandle<A>
where
    A: Actor,
{
    ActorRuntime::default()
        .spawn_actor_now(
            actor,
            ActorSpawnOptions {
                mailbox,
                execution: Some(ActorExecutionPolicy::TaskPerActor),
                scheduler_key: None,
                passivation: PassivationPolicy::Disabled,
                self_ref: None,
                service,
            },
        )
        .expect("TaskPerActor execution is supported")
}

pub(crate) struct ActorSpawnContext {
    pub(crate) options: ActorSpawnOptions,
    pub(crate) actor_system: Option<Arc<OnceLock<ActorSystem>>>,
    pub(crate) observer: ActorObserverHandle,
    pub(crate) terminal_hook: Option<TerminalHook>,
    pub(crate) spawner: ActorSpawner,
}

impl ActorSpawnContext {
    fn into_parts<A>(self) -> (ActorRuntimeParts<A>, PassivationPolicy, Option<ActorId>)
    where
        A: Actor,
    {
        let ActorSpawnContext {
            options,
            actor_system,
            observer,
            terminal_hook,
            spawner,
        } = self;
        let ActorSpawnOptions {
            mailbox,
            passivation,
            self_ref,
            service,
            scheduler_key,
            execution: _,
        } = options;
        (
            create_actor_parts(
                mailbox,
                self_ref,
                actor_system,
                service,
                observer,
                terminal_hook,
                spawner,
            ),
            passivation,
            scheduler_key,
        )
    }
}

pub(crate) fn spawn_actor_with_self_ref<A>(
    actor: A,
    context: ActorSpawnContext,
) -> Result<ActorHandle<A>, ActorSpawnError>
where
    A: Actor,
{
    let spawner = context.spawner.clone();
    spawner.spawn(actor, context)
}

fn spawn_task_per_actor<A>(actor: A, context: ActorSpawnContext) -> ActorHandle<A>
where
    A: Actor,
{
    let (parts, passivation, _scheduler_key) = context.into_parts();
    spawn_actor_as_tokio_task(actor, parts, passivation, "task_per_actor")
}

struct ActorRuntimeParts<A: Actor> {
    handle: ActorHandle<A>,
    normal_rx: Receiver<ActorCommand<A>>,
    system_rx: Receiver<ActorCommand<A>>,
    self_ref: Option<ActorRef>,
    actor_system: Option<Arc<OnceLock<ActorSystem>>>,
    service: ServiceContext,
    spawner: ActorSpawner,
    deferred_capacity: usize,
    turn_budget: usize,
}

fn create_actor_parts<A>(
    mailbox: MailboxConfig,
    self_ref: Option<ActorRef>,
    actor_system: Option<Arc<OnceLock<ActorSystem>>>,
    service: ServiceContext,
    observer: ActorObserverHandle,
    terminal_hook: Option<TerminalHook>,
    spawner: ActorSpawner,
) -> ActorRuntimeParts<A>
where
    A: Actor,
{
    let (normal_tx, normal_rx) = channel::channel(mailbox.normal_capacity());
    let (system_tx, system_rx) = channel::channel(mailbox.system_capacity());
    let local_ref = LocalActorRef::new(NEXT_LOCAL_ACTOR_ID.fetch_add(1, Ordering::Relaxed));
    let (terminated_tx, _terminated_rx) = broadcast::channel(16);
    let (lifecycle_tx, _lifecycle_rx) = watch::channel(ActorLifecycleState::Starting);
    let stop_failure = Arc::new(Mutex::new(None));
    let (forced_data_loss_tx, _forced_data_loss_rx) = broadcast::channel(16);
    let terminal_hook = Arc::new(Mutex::new(terminal_hook));
    let actor_ref = self_ref.clone();
    let handle = ActorHandle::new(ActorHandleInit {
        local_ref,
        terminated_tx,
        lifecycle_tx,
        stop_failure,
        forced_data_loss_tx,
        terminal_hook,
        normal_tx,
        system_tx,
        actor_ref,
        observer,
    });

    ActorRuntimeParts {
        handle,
        normal_rx,
        system_rx,
        self_ref,
        actor_system,
        service,
        spawner,
        deferred_capacity: mailbox.deferred_capacity(),
        turn_budget: mailbox.turn_budget(),
    }
}

fn spawn_actor_as_tokio_task<A>(
    actor: A,
    parts: ActorRuntimeParts<A>,
    passivation: PassivationPolicy,
    execution_policy: &'static str,
) -> ActorHandle<A>
where
    A: Actor,
{
    let handle = parts.handle.clone();
    let span = tracing::info_span!(
        "actor.spawn",
        otel.kind = "internal",
        actor.type = type_name::<A>(),
        actor.local_ref = handle.local_ref().id(),
        execution.policy = execution_policy
    );
    tokio::spawn(run_actor(actor, parts, passivation).instrument(span));

    handle
}

fn spawn_actor_on_pool<A>(
    actor: A,
    parts: ActorRuntimeParts<A>,
    passivation: PassivationPolicy,
    pool: &ActorWorkerPool,
    worker_index: usize,
    execution_policy: &'static str,
) -> ActorHandle<A>
where
    A: Actor,
{
    let handle = parts.handle.clone();
    let span = tracing::info_span!(
        "actor.spawn",
        otel.kind = "internal",
        actor.type = type_name::<A>(),
        actor.local_ref = handle.local_ref().id(),
        execution.policy = execution_policy,
        execution.worker = worker_index
    );
    pool.spawn(
        worker_index,
        run_actor(actor, parts, passivation).instrument(span),
    );

    handle
}

async fn run_actor<A>(mut actor: A, parts: ActorRuntimeParts<A>, passivation: PassivationPolicy)
where
    A: Actor,
{
    let ActorRuntimeParts {
        handle,
        mut normal_rx,
        mut system_rx,
        self_ref,
        actor_system,
        service,
        spawner,
        deferred_capacity,
        turn_budget,
    } = parts;
    let mut ctx = ActorContext::new(
        handle.clone(),
        self_ref,
        actor_system,
        service,
        spawner,
        deferred_capacity,
    );
    let activity_tx = spawn_passivation_monitor(&handle, passivation);
    let actor_type = type_name::<A>();
    let local_ref = handle.local_ref().id();
    let mut behavior = match std::panic::catch_unwind(AssertUnwindSafe(|| actor.initial_behavior()))
    {
        Ok(behavior) => behavior,
        Err(payload) => {
            terminate_panicked_actor(
                actor,
                &mut ctx,
                &handle,
                &mut normal_rx,
                &mut system_rx,
                ActorPanic::new("initial_behavior", payload),
            );
            return;
        }
    };

    let started_span = tracing::info_span!(
        "actor.started",
        otel.kind = "internal",
        actor.type = actor_type,
        actor.local_ref = local_ref
    );
    let startup_failure = match AssertUnwindSafe(actor.started(&mut ctx).instrument(started_span))
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
            true
        }
        Ok(Ok(())) => {
            if handle.lifecycle_state() != ActorLifecycleState::Quarantined {
                handle.set_lifecycle_state(ActorLifecycleState::Running);
            }
            handle
                .observer()
                .lifecycle(handle.observation_metadata(), ActorLifecycleEvent::Started);
            info!(
                actor.type = actor_type,
                actor.local_ref = local_ref,
                "actor started"
            );
            false
        }
        Err(payload) => {
            terminate_panicked_actor(
                actor,
                &mut ctx,
                &handle,
                &mut normal_rx,
                &mut system_rx,
                ActorPanic::new("started", payload),
            );
            return;
        }
    };

    let externally_fenced = handle.lifecycle_state() == ActorLifecycleState::Quarantined;
    let mut stop_reason = if startup_failure {
        Some(StopReason::StartFailed)
    } else if externally_fenced {
        Some(StopReason::AuthorityLost)
    } else {
        None
    };

    let mut actor_panic = None;
    while stop_reason.is_none() && actor_panic.is_none() {
        while let Ok(command) = system_rx.try_recv() {
            match handle_command(
                command,
                MailboxLane::System,
                &handle,
                ActorInstance {
                    actor: &mut actor,
                    behavior: &mut behavior,
                },
                &mut ctx,
                &mut stop_reason,
                activity_tx.as_ref(),
            )
            .await
            {
                Ok(true) => break,
                Ok(false) => {}
                Err(panic) => {
                    actor_panic = Some(panic);
                    break;
                }
            }
        }

        if stop_reason.is_some() || actor_panic.is_some() {
            break;
        }

        tokio::select! {
            biased;

            command = system_rx.recv() => {
                match command {
                    Some(command) => {
                        if let Err(panic) = handle_command(
                            command,
                            MailboxLane::System,
                            &handle,
                            ActorInstance {
                                actor: &mut actor,
                                behavior: &mut behavior,
                            },
                            &mut ctx,
                            &mut stop_reason,
                            activity_tx.as_ref(),
                        )
                        .await
                        {
                            actor_panic = Some(panic);
                        }
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
                        let mut command = Some(first_command);
                        let mut remaining = turn_budget;
                        while let Some(current) = command.take() {
                            match handle_command(
                                current,
                                MailboxLane::Normal,
                                &handle,
                                ActorInstance {
                                    actor: &mut actor,
                                    behavior: &mut behavior,
                                },
                                &mut ctx,
                                &mut stop_reason,
                                activity_tx.as_ref(),
                            )
                            .await
                            {
                                Ok(_) => {}
                                Err(panic) => {
                                    actor_panic = Some(panic);
                                    break;
                                }
                            }
                            if stop_reason.is_some() {
                                break;
                            }
                            remaining -= 1;
                            if remaining == 0 {
                                break;
                            }
                            command = normal_rx.try_recv().ok();
                        }
                    }
                    None if system_rx.is_closed() => {
                        stop_reason = Some(StopReason::MailboxClosed);
                    }
                    None => {}
                }
            }
        }
    }

    if let Some(panic) = actor_panic {
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

    let reason = stop_reason.unwrap_or(StopReason::Requested);
    ctx.cancel_deferred_replies(ActorCallError::MailboxClosed);
    ctx.cancel_all_tasks();
    ctx.stop_all_children(reason);
    let previous_phase = match reason {
        StopReason::Passivated(_) => ActorLifecycleState::Passivating,
        StopReason::Requested
        | StopReason::MailboxClosed
        | StopReason::StartFailed
        | StopReason::AuthorityLost => ActorLifecycleState::Stopping,
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

    handle.clear_stop_failure();
    if let Err(payload) = std::panic::catch_unwind(AssertUnwindSafe(|| drop(actor))) {
        normal_rx.close();
        system_rx.close();
        reject_queued_commands(&mut normal_rx, MailboxLane::Normal, &handle);
        reject_queued_commands(&mut system_rx, MailboxLane::System, &handle);
        finalize_panicked_actor(&handle, ActorPanic::new("drop", payload));
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
    handle.publish_terminated(ActorTerminated {
        target: handle.local_ref(),
        incarnation: ActorIncarnation::new(handle.local_ref().id()),
        reason: TerminatedReason::from(reason),
    });
    if let Some(completion) = terminal_completion {
        let _ = completion.send(Ok(()));
    }
    info!(
        actor.type = actor_type,
        actor.local_ref = local_ref,
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
    let mut quarantined = handle.lifecycle_state() == ActorLifecycleState::Quarantined;

    loop {
        handle.set_lifecycle_state(if quarantined {
            ActorLifecycleState::Quarantined
        } else {
            previous_phase
        });
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
                    crate::observation::record_resolved_stop_failure(false);
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
                        crate::observation::record_new_stop_failure();
                        StopFailureRecord {
                            reason,
                            previous_phase,
                            error: stop_error.message().to_owned(),
                            first_failure_time: now,
                            latest_attempt_time: now,
                            attempt_count: 1,
                            authoritative: !quarantined,
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
                handle.set_lifecycle_state(if quarantined {
                    ActorLifecycleState::Quarantined
                } else {
                    ActorLifecycleState::StopFailed
                });
                handle.observer().lifecycle(
                    handle.observation_metadata(),
                    ActorLifecycleEvent::StopFailed(reason),
                );
                normal_rx.close();
                while normal_rx.try_recv().is_ok() {}
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
                Some(ActorCommand::Quarantine(result)) => {
                    quarantined = true;
                    handle.mark_stop_failure_quarantined();
                    handle.set_lifecycle_state(ActorLifecycleState::Quarantined);
                    let _ = result.send(Ok(()));
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
                    crate::observation::record_resolved_stop_failure(true);
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

struct ActorInstance<'a, A: Actor> {
    actor: &'a mut A,
    behavior: &'a mut A::Behavior,
}

async fn handle_command<A>(
    command: ActorCommand<A>,
    lane: MailboxLane,
    handle: &ActorHandle<A>,
    instance: ActorInstance<'_, A>,
    ctx: &mut ActorContext<A>,
    stop_reason: &mut Option<StopReason>,
    activity_tx: Option<&watch::Sender<u64>>,
) -> Result<bool, ActorPanic>
where
    A: Actor,
{
    match command {
        ActorCommand::Envelope(mut envelope) => {
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
                tokio::pin!(future);
                AssertUnwindSafe(future.as_mut().instrument(span))
                    .catch_unwind()
                    .await
            };
            let outcome = match handled {
                Ok(outcome) => outcome,
                Err(payload) => {
                    if let Some(completion) = envelope.reject_panicked() {
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
            record_activity(activity_tx);
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
        ActorCommand::Quarantine(result) => {
            let _ = result.send(Err(ActorAdminError::InvalidState {
                operation: "quarantine_after_authority_loss",
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

include!("runtime/panic.rs");
include!("runtime/passivation.rs");
