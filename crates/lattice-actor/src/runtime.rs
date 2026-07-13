use std::any::{TypeId, type_name};
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::{broadcast, oneshot, watch};
use tracing::{Instrument, debug, error, info};

use crate::context::ActorContext;
use crate::error::ActorSpawnError;
use crate::handle::ActorHandle;
use crate::mailbox::{ActorCommand, MailboxConfig, MailboxLane};
use crate::recipient::ActorSystem;
use crate::traits::{Actor, ActorLifecycleState, PassivationReason, StopReason};
use crate::watch::{ActorIncarnation, ActorTerminated, LocalActorRef, TerminatedReason};
use lattice_core::actor_ref::ActorRef;
use lattice_core::id::ActorId;
use lattice_core::service_context::ServiceContext;

static NEXT_LOCAL_ACTOR_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_ACTIVATION_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub(crate) fn next_activation_id(
    node_incarnation: lattice_core::actor_ref::NodeIncarnation,
) -> lattice_core::actor_ref::ActivationId {
    let sequence = NEXT_ACTIVATION_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    lattice_core::actor_ref::ActivationId::new(node_incarnation, sequence)
        .expect("process activation sequence is nonzero")
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
}

impl Default for ActorRuntimeConfig {
    fn default() -> Self {
        Self {
            default_execution: ActorExecutionPolicy::TaskPerActor,
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
        let execution = options.execution.unwrap_or(self.config.default_execution);
        self.scheduler.spawn(actor, options, execution)
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

impl std::fmt::Debug for ActorScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
        options: ActorSpawnOptions,
        execution: ActorExecutionPolicy,
    ) -> Result<ActorHandle<A>, ActorSpawnError>
    where
        A: Actor,
    {
        match execution {
            ActorExecutionPolicy::TaskPerActor => Ok(spawn_task_per_actor(actor, options)),
            ActorExecutionPolicy::KeyedWorkerPool { worker_count } => {
                if worker_count == 0 {
                    return Err(ActorSpawnError::InvalidExecutionPolicy {
                        reason: "KeyedWorkerPool worker_count must be greater than zero",
                    });
                }
                self.spawn_keyed_worker_pool_actor(actor, options, worker_count)
            }
            ActorExecutionPolicy::DedicatedThreadPool { worker_count } => {
                if worker_count == 0 {
                    return Err(ActorSpawnError::InvalidExecutionPolicy {
                        reason: "DedicatedThreadPool worker_count must be greater than zero",
                    });
                }
                self.spawn_dedicated_pool_actor(actor, options, worker_count)
            }
        }
    }

    fn spawn_keyed_worker_pool_actor<A>(
        &self,
        actor: A,
        options: ActorSpawnOptions,
        worker_count: usize,
    ) -> Result<ActorHandle<A>, ActorSpawnError>
    where
        A: Actor,
    {
        let pool = self.keyed_worker_pool(worker_count)?;
        let ActorSpawnOptions {
            mailbox,
            scheduler_key,
            passivation,
            self_ref,
            service,
            execution: _,
        } = options;
        let parts = create_actor_parts(mailbox, self_ref, None, service);
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
        options: ActorSpawnOptions,
        worker_count: usize,
    ) -> Result<ActorHandle<A>, ActorSpawnError>
    where
        A: Actor,
    {
        let pool = self.dedicated_worker_pool::<A>(worker_count)?;
        let worker_index = pool.next_worker_index();
        let ActorSpawnOptions {
            mailbox,
            passivation,
            self_ref,
            service,
            execution: _,
            scheduler_key: _,
        } = options;
        Ok(spawn_actor_on_pool(
            actor,
            create_actor_parts(mailbox, self_ref, None, service),
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

#[derive(Debug, Clone, Copy)]
enum WorkerPoolKind {
    Keyed,
    Dedicated { actor_type: &'static str },
}

impl WorkerPoolKind {
    fn thread_name(self, worker_index: usize) -> String {
        match self {
            Self::Keyed => format!("lattice-keyed-worker-{worker_index}"),
            Self::Dedicated { actor_type } => {
                format!("lattice-dedicated-worker-{worker_index}-{actor_type}")
            }
        }
    }
}

struct ActorWorkerPool {
    workers: Vec<ActorWorker>,
    next_worker: AtomicU64,
}

impl ActorWorkerPool {
    fn start(kind: WorkerPoolKind, worker_count: usize) -> Result<Self, ActorSpawnError> {
        let mut workers = Vec::with_capacity(worker_count);
        for worker_index in 0..worker_count {
            workers.push(ActorWorker::start(kind, worker_index)?);
        }
        Ok(Self {
            workers,
            next_worker: AtomicU64::new(0),
        })
    }

    fn spawn<F>(&self, worker_index: usize, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.workers[worker_index].handle.spawn(future);
    }

    fn next_worker_index(&self) -> usize {
        (self.next_worker.fetch_add(1, Ordering::Relaxed) % self.workers.len() as u64) as usize
    }
}

impl Drop for ActorWorkerPool {
    fn drop(&mut self) {
        for worker in &mut self.workers {
            if let Some(shutdown_tx) = worker.shutdown_tx.take() {
                let _ = shutdown_tx.send(());
            }
        }
        for worker in &mut self.workers {
            if let Some(join_handle) = worker.join_handle.take()
                && join_handle.thread().id() != std::thread::current().id()
            {
                let _ = join_handle.join();
            }
        }
    }
}

struct ActorWorker {
    handle: tokio::runtime::Handle,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: Option<JoinHandle<()>>,
}

impl ActorWorker {
    fn start(kind: WorkerPoolKind, worker_index: usize) -> Result<Self, ActorSpawnError> {
        let (handle_tx, handle_rx) = std_mpsc::sync_channel(1);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let join_handle = std::thread::Builder::new()
            .name(kind.thread_name(worker_index))
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("actor worker runtime should build");
                let handle = runtime.handle().clone();
                let _ = handle_tx.send(handle);
                runtime.block_on(async {
                    let _ = shutdown_rx.await;
                });
            })
            .map_err(|_| ActorSpawnError::ExecutorStartFailed {
                reason: "failed to spawn actor worker thread",
            })?;
        let handle = handle_rx
            .recv()
            .map_err(|_| ActorSpawnError::ExecutorStartFailed {
                reason: "actor worker runtime stopped before publishing its handle",
            })?;

        Ok(Self {
            handle,
            shutdown_tx: Some(shutdown_tx),
            join_handle: Some(join_handle),
        })
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

pub(crate) fn spawn_actor_with_self_ref<A>(
    actor: A,
    mailbox: MailboxConfig,
    passivation: PassivationPolicy,
    self_ref: Option<ActorRef>,
    actor_system: Option<Arc<OnceLock<ActorSystem>>>,
    service: ServiceContext,
) -> ActorHandle<A>
where
    A: Actor,
{
    let parts = create_actor_parts(mailbox, self_ref, actor_system, service);
    spawn_actor_as_tokio_task(actor, parts, passivation, "task_per_actor")
}

fn spawn_task_per_actor<A>(actor: A, options: ActorSpawnOptions) -> ActorHandle<A>
where
    A: Actor,
{
    let ActorSpawnOptions {
        mailbox,
        passivation,
        self_ref,
        service,
        execution: _,
        scheduler_key: _,
    } = options;
    let parts = create_actor_parts(mailbox, self_ref, None, service);
    spawn_actor_as_tokio_task(actor, parts, passivation, "task_per_actor")
}

struct ActorRuntimeParts<A: Actor> {
    handle: ActorHandle<A>,
    normal_rx: mpsc::Receiver<ActorCommand<A>>,
    system_rx: mpsc::Receiver<ActorCommand<A>>,
    self_ref: Option<ActorRef>,
    actor_system: Option<Arc<OnceLock<ActorSystem>>>,
    service: ServiceContext,
    deferred_capacity: usize,
}

fn create_actor_parts<A>(
    mailbox: MailboxConfig,
    self_ref: Option<ActorRef>,
    actor_system: Option<Arc<OnceLock<ActorSystem>>>,
    service: ServiceContext,
) -> ActorRuntimeParts<A>
where
    A: Actor,
{
    let (normal_tx, normal_rx) = mpsc::channel(mailbox.normal_capacity());
    let (system_tx, system_rx) = mpsc::channel(mailbox.system_capacity());
    let local_ref = LocalActorRef::new(NEXT_LOCAL_ACTOR_ID.fetch_add(1, Ordering::Relaxed));
    let (terminated_tx, _terminated_rx) = broadcast::channel(16);
    let (lifecycle_tx, _lifecycle_rx) = watch::channel(ActorLifecycleState::Empty);
    let actor_ref = self_ref.as_ref().map(ActorRef::cast);
    let handle = ActorHandle::new(
        local_ref,
        terminated_tx,
        lifecycle_tx,
        normal_tx,
        system_tx,
        actor_ref,
    );

    ActorRuntimeParts {
        handle,
        normal_rx,
        system_rx,
        self_ref,
        actor_system,
        service,
        deferred_capacity: mailbox.deferred_capacity(),
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
        deferred_capacity,
    } = parts;
    let mut ctx = ActorContext::new(
        handle.clone(),
        self_ref,
        actor_system,
        service,
        deferred_capacity,
    );
    let activity_tx = spawn_passivation_monitor(&handle, passivation);
    let actor_type = type_name::<A>();
    let local_ref = handle.local_ref().id();

    let started_span = tracing::info_span!(
        "actor.started",
        otel.kind = "internal",
        actor.type = actor_type,
        actor.local_ref = local_ref
    );
    if let Err(error) = actor.started(&mut ctx).instrument(started_span).await {
        error!(
            actor.type = actor_type,
            actor.local_ref = local_ref,
            %error,
            "actor failed to start"
        );
        handle.set_lifecycle_state(ActorLifecycleState::Stopping);
        let stopping_span = tracing::info_span!(
            "actor.stopping",
            otel.kind = "internal",
            actor.type = actor_type,
            actor.local_ref = local_ref,
            stop.reason = ?StopReason::StartFailed
        );
        if let Err(error) = actor
            .stopping(&mut ctx, StopReason::StartFailed)
            .instrument(stopping_span)
            .await
        {
            error!(
                actor.type = actor_type,
                actor.local_ref = local_ref,
                %error,
                "actor failed to stop after start failure"
            );
            handle.set_lifecycle_state(ActorLifecycleState::StopFailed);
        } else {
            handle.set_lifecycle_state(ActorLifecycleState::Stopped);
        }
        ctx.cancel_all_tasks();
        ctx.stop_all_children(StopReason::StartFailed);
        handle.publish_terminated(ActorTerminated {
            target: handle.local_ref(),
            incarnation: ActorIncarnation::new(handle.local_ref().id()),
            reason: TerminatedReason::from(StopReason::StartFailed),
        });
        return;
    }
    handle.set_lifecycle_state(ActorLifecycleState::Running);
    info!(
        actor.type = actor_type,
        actor.local_ref = local_ref,
        "actor started"
    );

    let mut stop_reason = None;

    while stop_reason.is_none() {
        while let Ok(command) = system_rx.try_recv() {
            if handle_command(
                command,
                MailboxLane::System,
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
                            MailboxLane::System,
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
                            MailboxLane::Normal,
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
    ctx.cancel_deferred_replies(crate::error::ActorCallError::MailboxClosed);
    handle.set_lifecycle_state(match reason {
        StopReason::Passivated(_) => ActorLifecycleState::Passivating,
        StopReason::Requested | StopReason::MailboxClosed | StopReason::StartFailed => {
            ActorLifecycleState::Stopping
        }
    });
    let stopping_span = tracing::info_span!(
        "actor.stopping",
        otel.kind = "internal",
        actor.type = actor_type,
        actor.local_ref = local_ref,
        stop.reason = ?reason
    );
    if let Err(error) = actor
        .stopping(&mut ctx, reason)
        .instrument(stopping_span)
        .await
    {
        error!(
            actor.type = actor_type,
            actor.local_ref = local_ref,
            stop.reason = ?reason,
            %error,
            "actor failed to stop"
        );
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
    info!(
        actor.type = actor_type,
        actor.local_ref = local_ref,
        stop.reason = ?reason,
        "actor stopped"
    );
}

async fn handle_command<A>(
    command: ActorCommand<A>,
    lane: MailboxLane,
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
            let message_type = envelope.message_type();
            let span = tracing::info_span!(
                "actor.message",
                otel.kind = "consumer",
                actor.type = type_name::<A>(),
                message.type = message_type,
                mailbox.lane = lane.as_str()
            );
            debug!(
                actor.type = type_name::<A>(),
                message.type = message_type,
                mailbox.lane = lane.as_str(),
                "handling actor message"
            );
            envelope.handle(actor, ctx).instrument(span).await;
            ctx.reap_runtime_work();
            debug!(
                actor.type = type_name::<A>(),
                message.type = message_type,
                mailbox.lane = lane.as_str(),
                "actor message handled"
            );
            record_activity(activity_tx);
            if let Some(requested_reason) = ctx.take_lifecycle_request() {
                *stop_reason = Some(requested_reason);
                return true;
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
