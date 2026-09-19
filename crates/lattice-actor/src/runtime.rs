use std::{
    any::{TypeId, type_name},
    collections::HashMap,
    fmt::{Debug, Formatter, Result as FmtResult},
    num::NonZeroUsize,
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use futures_util::FutureExt;
use lattice_core::service_context::ServiceContext;
use tokio::sync::{broadcast, oneshot, watch};
use tracing::{Instrument, debug, error, info};

use crate::{
    attachments::ActorRuntimeAttachments,
    context::ActorContext,
    error::{ActorAdminError, ActorCallError, ActorSpawnError},
    handle::{ActorHandle, ActorHandleInit, ForcedDataLossEvent, StopFailureRecord, TerminalHook},
    mailbox::{
        ActorCommand, MailboxConfig, MailboxLane, QueuedRejection,
        channel::{self, Receiver},
    },
    observation::{
        ActorLifecycleEvent, ActorObserverHandle, record_new_stop_failure,
        record_resolved_stop_failure,
    },
    traits::{Actor, ActorLifecycleState, PassivationReason, StopReason},
    watch::{ActorTermination, LocalActorRef, TerminatedReason},
};

mod dispatch;
mod panic;
mod passivation;
mod rejection;
pub(crate) mod spawner;
mod task_runtime;
mod worker_pool;

use dispatch::{ActorInstance, handle_command};
use panic::{ActorPanic, finalize_panicked_actor, terminate_panicked_actor};
use passivation::wait_for_idle_timeout;
use rejection::{reject_prefetched_commands, reject_queued_commands};
use spawner::ActorSpawner;
use task_runtime::ActorTaskRuntime;
use worker_pool::{ActorWorkerPool, WorkerPoolKind};

static NEXT_LOCAL_ACTOR_ID: AtomicU64 = AtomicU64::new(1);
// Match the default turn budget so the common saturated path releases mailbox capacity once per
// turn instead of once per message. Smaller turn budgets still cap the prefetch.
const NORMAL_RECEIVE_BATCH_SIZE: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorExecutionPolicy {
    TaskPerActor,
    KeyedWorkerPool { worker_count: usize },
    DedicatedThreadPool { worker_count: usize },
}

/// Stable affinity key for actors scheduled on a keyed worker pool.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SchedulerKey {
    String(String),
    U64(u64),
    I64(i64),
    Bytes(Vec<u8>),
}

impl From<String> for SchedulerKey {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for SchedulerKey {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<u64> for SchedulerKey {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}

impl From<i64> for SchedulerKey {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl From<Vec<u8>> for SchedulerKey {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

#[derive(Debug, Clone)]
pub struct ActorRuntimeConfig {
    pub default_execution: ActorExecutionPolicy,
    /// Number of worker threads in the dedicated Tokio runtime used by `TaskPerActor`.
    pub task_worker_count: usize,
    pub observer: ActorObserverHandle,
    /// Shared environment inherited by every Actor spawned by this runtime.
    pub service: ServiceContext,
}

impl Default for ActorRuntimeConfig {
    fn default() -> Self {
        Self {
            default_execution: ActorExecutionPolicy::TaskPerActor,
            task_worker_count: std::thread::available_parallelism().map_or(1, NonZeroUsize::get),
            observer: ActorObserverHandle::default(),
            service: ServiceContext::empty(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ActorSpawnOptions {
    pub mailbox: MailboxConfig,
    pub execution: Option<ActorExecutionPolicy>,
    pub scheduler_key: Option<SchedulerKey>,
    pub passivation: PassivationPolicy,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PassivationPolicy {
    #[default]
    Disabled,
    /// Passivates the Actor after it spends this long waiting for its next
    /// mailbox command.
    ///
    /// Startup, message handlers, and lifecycle hooks do not count as idle
    /// time. A mailbox command that is ready at the timeout boundary takes
    /// priority over passivation.
    IdleTimeout(Duration),
}

/// Owner of the execution resources shared by a group of Actors.
///
/// `TaskPerActor` activations run on a dedicated Tokio runtime rather than the caller's ambient
/// runtime. Dropping the runtime shuts its executors down, so the owner must remain alive for as
/// long as its Actors.
pub struct ActorRuntime {
    config: ActorRuntimeConfig,
    resources: Arc<SchedulerResources>,
    scheduler: ActorScheduler,
}

impl Debug for ActorRuntime {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter
            .debug_struct("ActorRuntime")
            .field("config", &self.config)
            .field("scheduler", &self.scheduler)
            .finish_non_exhaustive()
    }
}

impl ActorRuntime {
    pub fn new(config: ActorRuntimeConfig) -> Self {
        let resources = Arc::new(SchedulerResources::new(config.task_worker_count));
        Self {
            scheduler: ActorScheduler::new(Arc::downgrade(&resources)),
            config,
            resources,
        }
    }

    pub fn scheduler(&self) -> &ActorScheduler {
        &self.scheduler
    }

    /// Shuts down every executor owned by this runtime.
    ///
    /// This is an execution-level shutdown: active Actor tasks are cancelled and their graceful
    /// `stopping` hooks are not run. A service should stop or drain its Actors before consuming the
    /// runtime. Dropping `ActorRuntime` has the same fallback behavior.
    pub fn shutdown(self) {
        let Self { resources, .. } = self;
        drop(resources);
    }

    pub fn spawn_actor<A>(
        &self,
        actor: A,
        options: ActorSpawnOptions,
    ) -> Result<ActorHandle<A>, ActorSpawnError>
    where
        A: Actor,
    {
        self.spawn_managed_actor(actor, options, ActorRuntimeAttachments::empty(), None)
    }

    /// Spawns an Actor with immutable integration capabilities and a terminal hook.
    ///
    /// Runtime integrations use this boundary to associate external routing or
    /// persistence state without adding those concepts to the local Actor
    /// runtime.
    #[doc(hidden)]
    pub fn spawn_managed_actor<A>(
        &self,
        actor: A,
        options: ActorSpawnOptions,
        runtime_attachments: ActorRuntimeAttachments,
        terminal_hook: Option<Box<dyn FnOnce(LocalActorRef) + Send + 'static>>,
    ) -> Result<ActorHandle<A>, ActorSpawnError>
    where
        A: Actor,
    {
        let spawner = ActorSpawner::new(self.scheduler.clone(), self.config.default_execution);
        spawner.spawn(
            actor,
            ActorSpawnContext {
                options,
                service: self.config.service.clone(),
                observer: self.config.observer.clone(),
                terminal_hook,
                runtime_attachments,
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
    resources: Weak<SchedulerResources>,
}

impl Debug for ActorScheduler {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("ActorScheduler").finish_non_exhaustive()
    }
}

struct SchedulerResources {
    task_runtime: ActorTaskRuntime,
    keyed_workers: Mutex<HashMap<usize, Arc<ActorWorkerPool>>>,
    dedicated_workers: Mutex<HashMap<DedicatedPoolKey, Arc<ActorWorkerPool>>>,
}

impl SchedulerResources {
    fn new(task_worker_count: usize) -> Self {
        Self {
            task_runtime: ActorTaskRuntime::new(task_worker_count),
            keyed_workers: Mutex::new(HashMap::new()),
            dedicated_workers: Mutex::new(HashMap::new()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct DedicatedPoolKey {
    actor_type: TypeId,
    worker_count: usize,
}

impl ActorScheduler {
    fn new(resources: Weak<SchedulerResources>) -> Self {
        Self { resources }
    }

    fn resources(&self) -> Result<Arc<SchedulerResources>, ActorSpawnError> {
        self.resources
            .upgrade()
            .ok_or(ActorSpawnError::ExecutorStartFailed {
                reason: "ActorRuntime was dropped before the Actor could spawn",
            })
    }

    pub fn keyed_worker_index(
        scheduler_key: &SchedulerKey,
        worker_count: usize,
    ) -> Result<usize, ActorSpawnError> {
        if worker_count == 0 {
            return Err(ActorSpawnError::InvalidExecutionPolicy {
                reason: "KeyedWorkerPool worker_count must be greater than zero",
            });
        }
        Ok((stable_scheduler_key_hash(scheduler_key) % worker_count as u64) as usize)
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
            ActorExecutionPolicy::TaskPerActor => self.spawn_task_per_actor(actor, context),
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

    fn spawn_task_per_actor<A>(
        &self,
        actor: A,
        context: ActorSpawnContext,
    ) -> Result<ActorHandle<A>, ActorSpawnError>
    where
        A: Actor,
    {
        let resources = self.resources()?;
        let (parts, passivation, _scheduler_key) = context.into_parts();
        spawn_actor_as_tokio_task(
            actor,
            parts,
            passivation,
            &resources.task_runtime,
            "task_per_actor",
        )
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
            scheduler_key.unwrap_or_else(|| SchedulerKey::U64(parts.handle.local_ref().id()));
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
        let resources = self.resources()?;
        let mut pools = resources
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
        let resources = self.resources()?;
        let mut pools = resources
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

fn stable_scheduler_key_hash(scheduler_key: &SchedulerKey) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    fn write(hash: &mut u64, bytes: &[u8]) {
        for byte in bytes {
            *hash ^= u64::from(*byte);
            *hash = hash.wrapping_mul(0x100000001b3);
        }
    }

    match scheduler_key {
        SchedulerKey::String(value) => {
            write(&mut hash, b"str");
            write(&mut hash, value.as_bytes());
        }
        SchedulerKey::U64(value) => {
            write(&mut hash, b"u64");
            write(&mut hash, &value.to_be_bytes());
        }
        SchedulerKey::I64(value) => {
            write(&mut hash, b"i64");
            write(&mut hash, &value.to_be_bytes());
        }
        SchedulerKey::Bytes(value) => {
            write(&mut hash, b"bytes");
            write(&mut hash, value);
        }
    }
    hash
}

pub(crate) struct ActorSpawnContext {
    pub(crate) options: ActorSpawnOptions,
    pub(crate) service: ServiceContext,
    pub(crate) observer: ActorObserverHandle,
    pub(crate) terminal_hook: Option<TerminalHook>,
    pub(crate) runtime_attachments: ActorRuntimeAttachments,
    pub(crate) spawner: ActorSpawner,
}

impl ActorSpawnContext {
    fn into_parts<A>(
        self,
    ) -> (
        ActorRuntimeParts<A>,
        PassivationPolicy,
        Option<SchedulerKey>,
    )
    where
        A: Actor,
    {
        let ActorSpawnContext {
            options,
            service,
            observer,
            terminal_hook,
            runtime_attachments,
            spawner,
        } = self;
        let ActorSpawnOptions {
            mailbox,
            passivation,
            scheduler_key,
            execution: _,
        } = options;
        (
            create_actor_parts(
                mailbox,
                service,
                observer,
                terminal_hook,
                runtime_attachments,
                spawner,
            ),
            passivation,
            scheduler_key,
        )
    }
}

pub(crate) fn spawn_actor_from_context<A>(
    actor: A,
    context: ActorSpawnContext,
) -> Result<ActorHandle<A>, ActorSpawnError>
where
    A: Actor,
{
    let spawner = context.spawner.clone();
    spawner.spawn(actor, context)
}

struct ActorRuntimeParts<A: Actor> {
    handle: ActorHandle<A>,
    normal_rx: Receiver<ActorCommand<A>>,
    system_rx: Receiver<ActorCommand<A>>,
    service: ServiceContext,
    runtime_attachments: ActorRuntimeAttachments,
    spawner: ActorSpawner,
    deferred_capacity: usize,
    turn_budget: usize,
}

fn create_actor_parts<A>(
    mailbox: MailboxConfig,
    service: ServiceContext,
    observer: ActorObserverHandle,
    terminal_hook: Option<TerminalHook>,
    runtime_attachments: ActorRuntimeAttachments,
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
    let handle = ActorHandle::new(ActorHandleInit {
        local_ref,
        terminated_tx,
        lifecycle_tx,
        stop_failure,
        forced_data_loss_tx,
        terminal_hook,
        normal_tx,
        system_tx,
        observer,
    });

    ActorRuntimeParts {
        handle,
        normal_rx,
        system_rx,
        service,
        runtime_attachments,
        spawner,
        deferred_capacity: mailbox.deferred_capacity(),
        turn_budget: mailbox.turn_budget(),
    }
}

fn spawn_actor_as_tokio_task<A>(
    actor: A,
    parts: ActorRuntimeParts<A>,
    passivation: PassivationPolicy,
    runtime: &ActorTaskRuntime,
    execution_policy: &'static str,
) -> Result<ActorHandle<A>, ActorSpawnError>
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
    runtime.spawn(run_actor(actor, parts, passivation).instrument(span))?;

    Ok(handle)
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
    let startup_failure = if handle.business_admission_fenced() {
        false
    } else {
        match AssertUnwindSafe(actor.started(&mut ctx).instrument(started_span))
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
                handle.set_lifecycle_state(ActorLifecycleState::Running);
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
        }
    };

    let mut stop_reason = if startup_failure {
        Some(StopReason::StartFailed)
    } else if handle.business_admission_fenced() {
        Some(StopReason::Requested)
    } else {
        None
    };

    let mut actor_panic = None;
    let mut normal_batch = Vec::with_capacity(NORMAL_RECEIVE_BATCH_SIZE.min(turn_budget));
    while stop_reason.is_none() && actor_panic.is_none() {
        if handle.business_admission_fenced() {
            stop_reason = Some(StopReason::Requested);
            break;
        }
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

            _ = handle.wait_business_fenced() => {
                stop_reason = Some(StopReason::Requested);
            }
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
                        let mut remaining = turn_budget;
                        normal_batch.push(first_command);
                        normal_rx.try_recv_batch(
                            &mut normal_batch,
                            NORMAL_RECEIVE_BATCH_SIZE
                                .min(remaining)
                                .saturating_sub(1),
                        );
                        loop {
                            let mut prefetched = normal_batch.drain(..);
                            for current in prefetched.by_ref() {
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
                                )
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
                            // Messages already taken out of the channel can no longer be rejected
                            // by the shutdown drain, so they are completed here with the same
                            // reason their still-queued peers receive.
                            reject_prefetched_commands(
                                prefetched,
                                MailboxLane::Normal,
                                &handle,
                                if actor_panic.is_some() {
                                    QueuedRejection::ActorPanicked
                                } else {
                                    QueuedRejection::MailboxClosed
                                },
                            );
                            if stop_reason.is_some()
                                || actor_panic.is_some()
                                || remaining == 0
                            {
                                break;
                            }
                            let received = normal_rx.try_recv_batch(
                                &mut normal_batch,
                                NORMAL_RECEIVE_BATCH_SIZE.min(remaining),
                            );
                            if received == 0 {
                                break;
                            }
                        }
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

    handle.clear_stop_failure();
    // A stopped Actor never dispatches what is still queued, so both lanes are closed and drained
    // here instead of being discarded silently when the receivers drop.
    normal_rx.close();
    system_rx.close();
    reject_queued_commands(
        &mut normal_rx,
        MailboxLane::Normal,
        &handle,
        QueuedRejection::MailboxClosed,
    );
    reject_queued_commands(
        &mut system_rx,
        MailboxLane::System,
        &handle,
        QueuedRejection::MailboxClosed,
    );
    if let Err(payload) = std::panic::catch_unwind(AssertUnwindSafe(|| drop(actor))) {
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
    handle.publish_terminated(ActorTermination {
        target: handle.local_ref(),
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
