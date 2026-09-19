use lattice_actor::context::HandlerContext;
use std::sync::{Arc, mpsc as std_mpsc};
use std::time::Duration;

use lattice_actor::context::ActorContext;
use lattice_actor::error::{ActorFailure, ActorSpawnError};
use lattice_actor::handle::ActorHandle;
use lattice_actor::mailbox::MailboxConfig;
use lattice_actor::reply::ReplyTo;
use lattice_actor::runtime::{
    ActorExecutionPolicy, ActorRuntime, ActorRuntimeConfig, ActorScheduler, ActorSpawnOptions,
    PassivationPolicy, SchedulerKey, WorkerPlacement, WorkerPoolKey, WorkerPoolKeyError,
};
use lattice_actor::traits::{
    Actor, ChildActorKey, ChildActorOptions, ChildSupervision, Handler, Responder, StopReason,
};
use tokio::sync::{Mutex, mpsc};

const ASK_TIMEOUT: Duration = Duration::from_secs(5);

fn round_robin_pool(pool_key: &str, worker_count: usize) -> ActorExecutionPolicy {
    ActorExecutionPolicy::WorkerPool {
        pool_key: WorkerPoolKey::new(pool_key).unwrap(),
        worker_count,
        placement: WorkerPlacement::RoundRobin,
    }
}

fn affinity_pool(pool_key: &str, worker_count: usize) -> ActorExecutionPolicy {
    ActorExecutionPolicy::WorkerPool {
        pool_key: WorkerPoolKey::new(pool_key).unwrap(),
        worker_count,
        placement: WorkerPlacement::Affinity,
    }
}

#[derive(Debug, lattice_actor::Request)]
#[request(response = String)]
struct Ping(&'static str);

#[derive(lattice_actor::Request)]
#[request(response = String)]
struct CurrentThread;

struct TestActor {
    events: Arc<Mutex<Vec<&'static str>>>,
}

struct OtherActor;

struct StandaloneActor {
    started: std_mpsc::Sender<String>,
}

struct ShutdownActor {
    started: std_mpsc::Sender<()>,
    dropped: std_mpsc::Sender<()>,
}

#[derive(lattice_actor::Request)]
#[request(response = Vec<String>)]
struct ChildThreads;

struct ParentActor {
    children: Vec<ActorHandle<TestActor>>,
}

#[derive(Debug, lattice_actor::Message)]
struct RestartChild;

struct ReportingChild {
    started: mpsc::UnboundedSender<String>,
}

struct RestartingParent {
    child: Option<ActorHandle<ReportingChild>>,
    started: mpsc::UnboundedSender<String>,
}

impl Actor for TestActor {
    type Error = ActorFailure;
    type Behavior = ::lattice_actor::state_machine::Stateless;
}

impl Actor for OtherActor {
    type Error = ActorFailure;
    type Behavior = ::lattice_actor::state_machine::Stateless;
}

impl Actor for StandaloneActor {
    type Error = ActorFailure;
    type Behavior = ::lattice_actor::state_machine::Stateless;

    async fn started(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), Self::Error> {
        let _ = self
            .started
            .send(format!("{:?}", std::thread::current().id()));
        Ok(())
    }
}

impl Actor for ShutdownActor {
    type Error = ActorFailure;
    type Behavior = ::lattice_actor::state_machine::Stateless;

    async fn started(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), Self::Error> {
        let _ = self.started.send(());
        Ok(())
    }
}

impl Drop for ShutdownActor {
    fn drop(&mut self) {
        let _ = self.dropped.send(());
    }
}

impl Actor for ParentActor {
    type Error = ActorFailure;
    type Behavior = ::lattice_actor::state_machine::Stateless;

    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), Self::Error> {
        for (key, execution, scheduler_key) in [
            (
                "affinity-a",
                affinity_pool("child-affinity", 2),
                Some(SchedulerKey::from("shared-child-key")),
            ),
            (
                "affinity-b",
                affinity_pool("child-affinity", 2),
                Some(SchedulerKey::from("shared-child-key")),
            ),
            (
                "round-robin-a",
                round_robin_pool("child-round-robin", 1),
                None,
            ),
            (
                "round-robin-b",
                round_robin_pool("child-round-robin", 1),
                None,
            ),
        ] {
            self.children.push(ctx.spawn_child(
                ChildActorKey::new(key),
                TestActor {
                    events: Arc::new(Mutex::new(Vec::new())),
                },
                ChildActorOptions {
                    mailbox: MailboxConfig::bounded(8),
                    execution,
                    scheduler_key,
                    ..ChildActorOptions::default()
                },
            )?);
        }
        Ok(())
    }
}

impl Actor for ReportingChild {
    type Error = ActorFailure;
    type Behavior = ::lattice_actor::state_machine::Stateless;

    async fn started(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), Self::Error> {
        let _ = self
            .started
            .send(format!("{:?}", std::thread::current().id()));
        Ok(())
    }
}

impl Actor for RestartingParent {
    type Error = ActorFailure;
    type Behavior = ::lattice_actor::state_machine::Stateless;

    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), Self::Error> {
        let started = self.started.clone();
        self.child = Some(ctx.spawn_child_with_factory(
            ChildActorKey::new("restarting-child"),
            move || ReportingChild {
                started: started.clone(),
            },
            ChildActorOptions {
                mailbox: MailboxConfig::bounded(8),
                supervision: ChildSupervision::RestartChild,
                execution: round_robin_pool("restarting-children", 1),
                ..ChildActorOptions::default()
            },
        )?);
        Ok(())
    }
}

impl Handler<RestartChild> for RestartingParent {
    async fn handle(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        _message: RestartChild,
    ) -> Result<(), ActorFailure> {
        self.child
            .as_ref()
            .expect("child should be running")
            .stop(StopReason::Requested)?;
        Ok(())
    }
}

impl Responder<ChildThreads> for ParentActor {
    async fn respond(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        _request: ChildThreads,
        reply_to: ReplyTo<Vec<String>>,
    ) -> Result<(), ActorFailure> {
        let mut threads = Vec::with_capacity(self.children.len());
        for child in &self.children {
            threads.push(
                child
                    .ask(CurrentThread, ASK_TIMEOUT)
                    .await
                    .map_err(|error| ActorFailure::new(error.to_string()))?,
            );
        }
        let _ = reply_to.send(threads);
        Ok(())
    }
}

impl Responder<Ping> for TestActor {
    async fn respond(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        request: Ping,
        reply_to: ReplyTo<String>,
    ) -> Result<(), ActorFailure> {
        self.events.lock().await.push(request.0);
        let _ = reply_to.send(format!("pong:{}", request.0));
        Ok(())
    }
}

impl Responder<CurrentThread> for TestActor {
    async fn respond(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        _request: CurrentThread,
        reply_to: ReplyTo<String>,
    ) -> Result<(), ActorFailure> {
        let _ = reply_to.send(format!("{:?}", std::thread::current().id()));
        Ok(())
    }
}

impl Responder<CurrentThread> for OtherActor {
    async fn respond(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        _request: CurrentThread,
        reply_to: ReplyTo<String>,
    ) -> Result<(), ActorFailure> {
        let _ = reply_to.send(format!("{:?}", std::thread::current().id()));
        Ok(())
    }
}

#[test]
fn task_per_actor_uses_one_owned_runtime_without_an_ambient_tokio_context() {
    let runtime = ActorRuntime::new(ActorRuntimeConfig {
        task_worker_count: 1,
        ..ActorRuntimeConfig::default()
    });
    let (started_tx, started_rx) = std_mpsc::channel();

    let _first = runtime
        .spawn_actor(
            StandaloneActor {
                started: started_tx.clone(),
            },
            ActorSpawnOptions::default(),
        )
        .unwrap();
    let _second = runtime
        .spawn_actor(
            StandaloneActor {
                started: started_tx,
            },
            ActorSpawnOptions::default(),
        )
        .unwrap();

    let first_thread = started_rx.recv_timeout(ASK_TIMEOUT).unwrap();
    let second_thread = started_rx.recv_timeout(ASK_TIMEOUT).unwrap();
    assert_eq!(first_thread, second_thread);
}

#[test]
fn task_per_actor_rejects_zero_runtime_workers() {
    let runtime = ActorRuntime::new(ActorRuntimeConfig {
        task_worker_count: 0,
        ..ActorRuntimeConfig::default()
    });
    let (started, _started_rx) = std_mpsc::channel();

    assert!(matches!(
        runtime.spawn_actor(StandaloneActor { started }, ActorSpawnOptions::default()),
        Err(ActorSpawnError::InvalidExecutionPolicy { .. })
    ));
}

#[test]
fn runtime_shutdown_releases_actors_from_every_owned_executor() {
    let runtime = ActorRuntime::new(ActorRuntimeConfig {
        task_worker_count: 1,
        ..ActorRuntimeConfig::default()
    });
    let (started_tx, started_rx) = std_mpsc::channel();
    let (dropped_tx, dropped_rx) = std_mpsc::channel();

    for (execution, scheduler_key) in [
        (ActorExecutionPolicy::TaskPerActor, None),
        (round_robin_pool("shutdown-round-robin", 1), None),
        (
            affinity_pool("shutdown-affinity", 1),
            Some(SchedulerKey::from("shutdown-actor")),
        ),
    ] {
        let _handle = runtime
            .spawn_actor(
                ShutdownActor {
                    started: started_tx.clone(),
                    dropped: dropped_tx.clone(),
                },
                ActorSpawnOptions {
                    execution: Some(execution),
                    scheduler_key,
                    ..ActorSpawnOptions::default()
                },
            )
            .unwrap();
    }

    for _ in 0..3 {
        started_rx.recv_timeout(ASK_TIMEOUT).unwrap();
    }
    runtime.shutdown();
    for _ in 0..3 {
        dropped_rx.recv_timeout(ASK_TIMEOUT).unwrap();
    }
}

#[tokio::test]
async fn worker_pool_policy_runs_actor_with_same_mailbox_semantics() {
    let runtime = ActorRuntime::default();
    let events = Arc::new(Mutex::new(Vec::new()));
    let handle = runtime
        .spawn_actor(
            TestActor {
                events: events.clone(),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                execution: Some(round_robin_pool("mailbox-semantics", 2)),
                scheduler_key: None,
                passivation: PassivationPolicy::Disabled,
            },
        )
        .unwrap();

    let reply = handle.ask(Ping("worker-pool"), ASK_TIMEOUT).await.unwrap();

    assert_eq!(reply, "pong:worker-pool");
    assert_eq!(*events.lock().await, vec!["worker-pool"]);
}

#[tokio::test]
async fn affinity_worker_pool_runs_actor_with_same_mailbox_semantics() {
    let runtime = ActorRuntime::default();
    let events = Arc::new(Mutex::new(Vec::new()));
    let handle = runtime
        .spawn_actor(
            TestActor {
                events: events.clone(),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                execution: Some(affinity_pool("affinity-semantics", 4)),
                scheduler_key: Some(SchedulerKey::U64(42)),
                passivation: PassivationPolicy::Disabled,
            },
        )
        .unwrap();

    let reply = handle.ask(Ping("shard-worker"), ASK_TIMEOUT).await.unwrap();

    assert_eq!(reply, "pong:shard-worker");
    assert_eq!(*events.lock().await, vec!["shard-worker"]);
}

#[tokio::test]
async fn child_actors_use_selected_execution_policies_and_scheduler_affinity() {
    let runtime = ActorRuntime::default();
    let parent = runtime
        .spawn_actor(
            ParentActor {
                children: Vec::new(),
            },
            ActorSpawnOptions::default(),
        )
        .unwrap();

    let threads = parent.ask(ChildThreads, ASK_TIMEOUT).await.unwrap();
    assert_eq!(threads.len(), 4);
    assert_eq!(threads[0], threads[1]);
    assert_eq!(threads[2], threads[3]);
    assert_ne!(threads[0], threads[2]);
}

#[tokio::test]
async fn supervised_child_restart_preserves_the_selected_execution_policy() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let runtime = ActorRuntime::default();
    let parent = runtime
        .spawn_actor(
            RestartingParent {
                child: None,
                started: started_tx,
            },
            ActorSpawnOptions::default(),
        )
        .unwrap();

    let first = tokio::time::timeout(ASK_TIMEOUT, started_rx.recv())
        .await
        .unwrap()
        .unwrap();
    parent.tell(RestartChild).await.unwrap();
    let restarted = tokio::time::timeout(ASK_TIMEOUT, started_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first, restarted);
}

#[tokio::test]
async fn worker_pool_rejects_zero_workers() {
    let runtime = ActorRuntime::default();
    let shard = runtime.spawn_actor(
        TestActor {
            events: Arc::new(Mutex::new(Vec::new())),
        },
        ActorSpawnOptions {
            mailbox: MailboxConfig::bounded(8),
            execution: Some(round_robin_pool("zero-round-robin", 0)),
            scheduler_key: None,
            passivation: PassivationPolicy::Disabled,
        },
    );
    let affinity = runtime.spawn_actor(
        TestActor {
            events: Arc::new(Mutex::new(Vec::new())),
        },
        ActorSpawnOptions {
            mailbox: MailboxConfig::bounded(8),
            execution: Some(affinity_pool("zero-affinity", 0)),
            scheduler_key: Some(SchedulerKey::from("zero")),
            passivation: PassivationPolicy::Disabled,
        },
    );

    assert!(matches!(
        shard,
        Err(ActorSpawnError::InvalidExecutionPolicy { .. })
    ));
    assert!(matches!(
        affinity,
        Err(ActorSpawnError::InvalidExecutionPolicy { .. })
    ));
}

#[tokio::test]
async fn named_worker_pool_reuses_configured_worker_threads() {
    let runtime = ActorRuntime::default();
    let first = runtime
        .spawn_actor(
            TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                execution: Some(round_robin_pool("shared-test-actors", 1)),
                scheduler_key: None,
                passivation: PassivationPolicy::Disabled,
            },
        )
        .unwrap();
    let second = runtime
        .spawn_actor(
            TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                execution: Some(round_robin_pool("shared-test-actors", 1)),
                scheduler_key: None,
                passivation: PassivationPolicy::Disabled,
            },
        )
        .unwrap();

    assert_eq!(
        first.ask(CurrentThread, ASK_TIMEOUT).await.unwrap(),
        second.ask(CurrentThread, ASK_TIMEOUT).await.unwrap()
    );
}

#[tokio::test]
async fn named_worker_pool_can_be_shared_across_actor_types() {
    let runtime = ActorRuntime::default();
    let first = runtime
        .spawn_actor(
            TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                execution: Some(round_robin_pool("cross-type", 1)),
                scheduler_key: None,
                passivation: PassivationPolicy::Disabled,
            },
        )
        .unwrap();
    let second = runtime
        .spawn_actor(
            OtherActor,
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                execution: Some(round_robin_pool("cross-type", 1)),
                scheduler_key: None,
                passivation: PassivationPolicy::Disabled,
            },
        )
        .unwrap();

    assert_eq!(
        first.ask(CurrentThread, ASK_TIMEOUT).await.unwrap(),
        second.ask(CurrentThread, ASK_TIMEOUT).await.unwrap()
    );
}

#[tokio::test]
async fn different_pool_keys_isolate_actors_of_the_same_type() {
    let runtime = ActorRuntime::default();
    let first = runtime
        .spawn_actor(
            TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
            },
            ActorSpawnOptions {
                execution: Some(round_robin_pool("latency-sensitive", 1)),
                ..ActorSpawnOptions::default()
            },
        )
        .unwrap();
    let second = runtime
        .spawn_actor(
            TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
            },
            ActorSpawnOptions {
                execution: Some(round_robin_pool("background", 1)),
                ..ActorSpawnOptions::default()
            },
        )
        .unwrap();

    assert_ne!(
        first.ask(CurrentThread, ASK_TIMEOUT).await.unwrap(),
        second.ask(CurrentThread, ASK_TIMEOUT).await.unwrap()
    );
}

#[tokio::test]
async fn worker_pool_uses_scheduler_key_for_worker_affinity() {
    let runtime = ActorRuntime::default();
    let first = runtime
        .spawn_actor(
            TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                execution: Some(affinity_pool("affinity", 2)),
                scheduler_key: Some(SchedulerKey::from("same-key")),
                passivation: PassivationPolicy::Disabled,
            },
        )
        .unwrap();
    let second = runtime
        .spawn_actor(
            TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                execution: Some(affinity_pool("affinity", 2)),
                scheduler_key: Some(SchedulerKey::from("same-key")),
                passivation: PassivationPolicy::Disabled,
            },
        )
        .unwrap();

    assert_eq!(
        first.ask(CurrentThread, ASK_TIMEOUT).await.unwrap(),
        second.ask(CurrentThread, ASK_TIMEOUT).await.unwrap()
    );
}

#[tokio::test]
async fn affinity_requires_an_explicit_scheduler_key() {
    let runtime = ActorRuntime::default();
    let result = runtime.spawn_actor(
        TestActor {
            events: Arc::new(Mutex::new(Vec::new())),
        },
        ActorSpawnOptions {
            execution: Some(affinity_pool("missing-affinity-key", 1)),
            scheduler_key: None,
            ..ActorSpawnOptions::default()
        },
    );

    assert!(matches!(
        result,
        Err(ActorSpawnError::InvalidExecutionPolicy { .. })
    ));
}

#[tokio::test]
async fn worker_pool_rejects_a_conflicting_worker_count() {
    let runtime = ActorRuntime::default();
    let _first = runtime
        .spawn_actor(
            TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
            },
            ActorSpawnOptions {
                execution: Some(round_robin_pool("configured-once", 1)),
                ..ActorSpawnOptions::default()
            },
        )
        .unwrap();
    let second = runtime.spawn_actor(
        TestActor {
            events: Arc::new(Mutex::new(Vec::new())),
        },
        ActorSpawnOptions {
            execution: Some(round_robin_pool("configured-once", 2)),
            ..ActorSpawnOptions::default()
        },
    );

    assert!(matches!(
        second,
        Err(ActorSpawnError::WorkerPoolConfigurationConflict {
            pool_key,
            configured_worker_count: 1,
            requested_worker_count: 2,
        }) if pool_key == WorkerPoolKey::new("configured-once").unwrap()
    ));
}

#[test]
fn affinity_maps_scheduler_key_deterministically_to_worker() {
    let scheduler_key = SchedulerKey::U64(42);

    let first = ActorScheduler::affinity_worker_index(&scheduler_key, 8).unwrap();
    let second = ActorScheduler::affinity_worker_index(&scheduler_key, 8).unwrap();
    let zero = ActorScheduler::affinity_worker_index(&scheduler_key, 0);

    assert_eq!(first, second);
    assert!(first < 8);
    assert!(matches!(
        zero,
        Err(ActorSpawnError::InvalidExecutionPolicy { .. })
    ));
}

#[test]
fn worker_pool_key_rejects_nul_characters() {
    assert_eq!(
        WorkerPoolKey::new("invalid\0pool"),
        Err(WorkerPoolKeyError::ContainsNul)
    );
    assert_eq!(
        WorkerPoolKey::try_from("invalid\0pool"),
        Err(WorkerPoolKeyError::ContainsNul)
    );
}
