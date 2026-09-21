use std::{
    any::type_name,
    collections::HashMap,
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    num::NonZeroUsize,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use thiserror::Error;
use tokio::sync::{broadcast, watch};
use tracing::Instrument;

use crate::{
    attachments::ActorRuntimeAttachments,
    environment::ActorEnvironment,
    error::ActorSpawnError,
    handle::{ActorHandle, ActorHandleInit, TerminalHook},
    mailbox::{
        ActorCommand, MailboxConfig,
        channel::{self, Receiver},
    },
    observation::ActorObserverHandle,
    traits::{Actor, ActorLifecycleState},
    watch::LocalActorRef,
};

mod actor;
mod dispatch;
mod panic;
mod passivation;
mod rejection;
pub(crate) mod spawner;
mod task_runtime;
mod worker_pool;

use actor::run_actor;
use spawner::ActorSpawner;
use task_runtime::ActorTaskRuntime;
use worker_pool::ActorWorkerPool;

static NEXT_LOCAL_ACTOR_ID: AtomicU64 = AtomicU64::new(1);

/// Selects the executor and worker-placement policy for an Actor activation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActorExecutionPolicy {
    /// Runs the Actor as an independent task on the Tokio runtime owned by [`ActorRuntime`].
    TaskPerActor,
    /// Runs the Actor on a named, fixed-size worker pool owned by [`ActorRuntime`].
    WorkerPool {
        /// Identifies the pool to share within this runtime.
        pool_key: WorkerPoolKey,
        /// Number of single-threaded workers in the pool.
        worker_count: usize,
        /// Selects one worker for this activation.
        placement: WorkerPlacement,
    },
}

/// Stable identity of a worker pool owned by one [`ActorRuntime`].
///
/// Actors that use the same key intentionally share the same worker threads, regardless of their
/// Rust type. Reusing a key with a different worker count is rejected.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkerPoolKey(Arc<str>);

impl WorkerPoolKey {
    pub fn new(value: impl Into<Arc<str>>) -> Result<Self, WorkerPoolKeyError> {
        let value = value.into();
        if value.contains('\0') {
            return Err(WorkerPoolKeyError::ContainsNul);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for WorkerPoolKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter.write_str(&self.0)
    }
}

impl TryFrom<String> for WorkerPoolKey {
    type Error = WorkerPoolKeyError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for WorkerPoolKey {
    type Error = WorkerPoolKeyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// Failure returned when a worker-pool identity cannot be used safely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum WorkerPoolKeyError {
    /// Thread names cannot contain a NUL character.
    #[error("worker pool key must not contain a NUL character")]
    ContainsNul,
}

/// Strategy used to select one worker within a named worker pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerPlacement {
    /// Distributes new activations across workers in spawn order.
    RoundRobin,
    /// Hashes [`ActorSpawnOptions::scheduler_key`] to preserve worker affinity.
    Affinity,
}

/// Stable affinity key for actors using [`WorkerPlacement::Affinity`].
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
    /// Immutable environment inherited by every Actor spawned by this runtime.
    pub environment: ActorEnvironment,
}

impl Default for ActorRuntimeConfig {
    fn default() -> Self {
        Self {
            default_execution: ActorExecutionPolicy::TaskPerActor,
            task_worker_count: std::thread::available_parallelism().map_or(1, NonZeroUsize::get),
            observer: ActorObserverHandle::default(),
            environment: ActorEnvironment::empty(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ActorSpawnOptions {
    pub mailbox: MailboxConfig,
    pub execution: Option<ActorExecutionPolicy>,
    /// Stable key required when the selected policy uses [`WorkerPlacement::Affinity`].
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
        let spawner = ActorSpawner::new(
            self.scheduler.clone(),
            self.config.default_execution.clone(),
        );
        spawner.spawn(
            actor,
            ActorSpawnContext {
                options,
                environment: self.config.environment.clone(),
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
    worker_pools: Mutex<HashMap<WorkerPoolKey, WorkerPoolEntry>>,
}

impl SchedulerResources {
    fn new(task_worker_count: usize) -> Self {
        Self {
            task_runtime: ActorTaskRuntime::new(task_worker_count),
            worker_pools: Mutex::new(HashMap::new()),
        }
    }
}

struct WorkerPoolEntry {
    worker_count: usize,
    pool: Arc<ActorWorkerPool>,
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

    pub fn affinity_worker_index(
        scheduler_key: &SchedulerKey,
        worker_count: usize,
    ) -> Result<usize, ActorSpawnError> {
        if worker_count == 0 {
            return Err(ActorSpawnError::InvalidExecutionPolicy {
                reason: "WorkerPool worker_count must be greater than zero",
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
            ActorExecutionPolicy::WorkerPool {
                pool_key,
                worker_count,
                placement,
            } => {
                if worker_count == 0 {
                    return Err(ActorSpawnError::InvalidExecutionPolicy {
                        reason: "WorkerPool worker_count must be greater than zero",
                    });
                }
                self.spawn_worker_pool_actor(actor, context, pool_key, worker_count, placement)
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

    fn spawn_worker_pool_actor<A>(
        &self,
        actor: A,
        context: ActorSpawnContext,
        pool_key: WorkerPoolKey,
        worker_count: usize,
        placement: WorkerPlacement,
    ) -> Result<ActorHandle<A>, ActorSpawnError>
    where
        A: Actor,
    {
        let (parts, passivation, scheduler_key) = context.into_parts();
        let affinity_worker_index = match placement {
            WorkerPlacement::RoundRobin => None,
            WorkerPlacement::Affinity => {
                let scheduler_key =
                    scheduler_key.ok_or(ActorSpawnError::InvalidExecutionPolicy {
                        reason: "WorkerPool affinity placement requires a scheduler_key",
                    })?;
                Some(Self::affinity_worker_index(&scheduler_key, worker_count)?)
            }
        };
        let pool = self.worker_pool(pool_key, worker_count)?;
        let worker_index = affinity_worker_index.unwrap_or_else(|| pool.next_worker_index());
        Ok(spawn_actor_on_pool(
            actor,
            parts,
            passivation,
            &pool,
            worker_index,
            "worker_pool",
        ))
    }

    fn worker_pool(
        &self,
        pool_key: WorkerPoolKey,
        worker_count: usize,
    ) -> Result<Arc<ActorWorkerPool>, ActorSpawnError> {
        let resources = self.resources()?;
        let mut pools = resources
            .worker_pools
            .lock()
            .expect("actor worker pool mutex poisoned");
        if let Some(entry) = pools.get(&pool_key) {
            if entry.worker_count != worker_count {
                return Err(ActorSpawnError::WorkerPoolConfigurationConflict {
                    pool_key,
                    configured_worker_count: entry.worker_count,
                    requested_worker_count: worker_count,
                });
            }
            return Ok(entry.pool.clone());
        }

        let pool = Arc::new(ActorWorkerPool::start(pool_key.as_str(), worker_count)?);
        pools.insert(
            pool_key,
            WorkerPoolEntry {
                worker_count,
                pool: pool.clone(),
            },
        );
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
    pub(crate) environment: ActorEnvironment,
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
            environment,
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
                environment,
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
    environment: ActorEnvironment,
    runtime_attachments: ActorRuntimeAttachments,
    spawner: ActorSpawner,
    deferred_capacity: usize,
    turn_budget: usize,
}

fn create_actor_parts<A>(
    mailbox: MailboxConfig,
    environment: ActorEnvironment,
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
        environment,
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
