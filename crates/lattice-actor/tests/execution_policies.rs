use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use lattice_actor::context::ActorContext;
use lattice_actor::error::{ActorError, ActorSpawnError};
use lattice_actor::handle::ActorHandle;
use lattice_actor::mailbox::MailboxConfig;
use lattice_actor::reply::ReplyTo;
use lattice_actor::runtime::{ActorExecutionPolicy, ActorScheduler, PassivationPolicy};
use lattice_actor::runtime::{ActorRuntime, ActorSpawnOptions};
use lattice_actor::traits::{
    Actor, ChildActorKey, ChildActorOptions, ChildSupervision, Handler, Responder, StopReason,
};
use lattice_core::id::ActorId;
use lattice_core::service_context::ServiceContext;
use tokio::sync::{Mutex, mpsc};

const ASK_TIMEOUT: Duration = Duration::from_secs(5);

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

#[async_trait]
impl Actor for TestActor {
    type Error = ActorError;
}

#[async_trait]
impl Actor for OtherActor {
    type Error = ActorError;
}

#[async_trait]
impl Actor for ParentActor {
    type Error = ActorError;

    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), Self::Error> {
        for (key, execution, scheduler_key) in [
            (
                "keyed-a",
                ActorExecutionPolicy::KeyedWorkerPool { worker_count: 2 },
                Some(ActorId::Str("shared-child-key".to_owned())),
            ),
            (
                "keyed-b",
                ActorExecutionPolicy::KeyedWorkerPool { worker_count: 2 },
                Some(ActorId::Str("shared-child-key".to_owned())),
            ),
            (
                "dedicated-a",
                ActorExecutionPolicy::DedicatedThreadPool { worker_count: 1 },
                None,
            ),
            (
                "dedicated-b",
                ActorExecutionPolicy::DedicatedThreadPool { worker_count: 1 },
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

#[async_trait]
impl Actor for ReportingChild {
    type Error = ActorError;

    async fn started(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), Self::Error> {
        let _ = self
            .started
            .send(format!("{:?}", std::thread::current().id()));
        Ok(())
    }
}

#[async_trait]
impl Actor for RestartingParent {
    type Error = ActorError;

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
                execution: ActorExecutionPolicy::DedicatedThreadPool { worker_count: 1 },
                ..ChildActorOptions::default()
            },
        )?);
        Ok(())
    }
}

#[async_trait]
impl Handler<RestartChild> for RestartingParent {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _message: RestartChild,
    ) -> Result<(), ActorError> {
        self.child
            .as_ref()
            .expect("child should be running")
            .stop(StopReason::Requested)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Responder<ChildThreads> for ParentActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _request: ChildThreads,
        reply_to: ReplyTo<Vec<String>>,
    ) -> Result<(), ActorError> {
        let mut threads = Vec::with_capacity(self.children.len());
        for child in &self.children {
            threads.push(
                child
                    .ask(CurrentThread, ASK_TIMEOUT)
                    .await
                    .map_err(|error| ActorError::new(error.to_string()))?,
            );
        }
        let _ = reply_to.send(threads);
        Ok(())
    }
}

#[async_trait]
impl Responder<Ping> for TestActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        request: Ping,
        reply_to: ReplyTo<String>,
    ) -> Result<(), ActorError> {
        self.events.lock().await.push(request.0);
        let _ = reply_to.send(format!("pong:{}", request.0));
        Ok(())
    }
}

#[async_trait]
impl Responder<CurrentThread> for TestActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _request: CurrentThread,
        reply_to: ReplyTo<String>,
    ) -> Result<(), ActorError> {
        let _ = reply_to.send(format!("{:?}", std::thread::current().id()));
        Ok(())
    }
}

#[async_trait]
impl Responder<CurrentThread> for OtherActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _request: CurrentThread,
        reply_to: ReplyTo<String>,
    ) -> Result<(), ActorError> {
        let _ = reply_to.send(format!("{:?}", std::thread::current().id()));
        Ok(())
    }
}

#[tokio::test]
async fn dedicated_thread_pool_policy_runs_actor_with_same_mailbox_semantics() {
    let runtime = ActorRuntime::default();
    let events = Arc::new(Mutex::new(Vec::new()));
    let handle = runtime
        .spawn_actor(
            TestActor {
                events: events.clone(),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                execution: Some(ActorExecutionPolicy::DedicatedThreadPool { worker_count: 2 }),
                scheduler_key: None,
                passivation: PassivationPolicy::Disabled,
                self_ref: None,
                service: ServiceContext::empty(),
            },
        )
        .await
        .unwrap();

    let reply = handle.ask(Ping("dedicated"), ASK_TIMEOUT).await.unwrap();

    assert_eq!(reply, "pong:dedicated");
    assert_eq!(*events.lock().await, vec!["dedicated"]);
}

#[tokio::test]
async fn keyed_worker_pool_execution_policy_runs_actor_with_same_mailbox_semantics() {
    let runtime = ActorRuntime::default();
    let events = Arc::new(Mutex::new(Vec::new()));
    let handle = runtime
        .spawn_actor(
            TestActor {
                events: events.clone(),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                execution: Some(ActorExecutionPolicy::KeyedWorkerPool { worker_count: 4 }),
                scheduler_key: Some(ActorId::U64(42)),
                passivation: PassivationPolicy::Disabled,
                self_ref: None,
                service: ServiceContext::empty(),
            },
        )
        .await
        .unwrap();

    let reply = handle.ask(Ping("shard-worker"), ASK_TIMEOUT).await.unwrap();

    assert_eq!(reply, "pong:shard-worker");
    assert_eq!(*events.lock().await, vec!["shard-worker"]);
}

#[tokio::test]
async fn child_actors_use_selected_execution_policies_and_scheduler_affinity() {
    let parent = ActorRuntime::default()
        .spawn_actor(
            ParentActor {
                children: Vec::new(),
            },
            ActorSpawnOptions::default(),
        )
        .await
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
    let parent = ActorRuntime::default()
        .spawn_actor(
            RestartingParent {
                child: None,
                started: started_tx,
            },
            ActorSpawnOptions::default(),
        )
        .await
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
async fn execution_policies_reject_zero_workers() {
    let runtime = ActorRuntime::default();
    let shard = runtime
        .spawn_actor(
            TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                execution: Some(ActorExecutionPolicy::KeyedWorkerPool { worker_count: 0 }),
                scheduler_key: None,
                passivation: PassivationPolicy::Disabled,
                self_ref: None,
                service: ServiceContext::empty(),
            },
        )
        .await;
    let dedicated = runtime
        .spawn_actor(
            TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                execution: Some(ActorExecutionPolicy::DedicatedThreadPool { worker_count: 0 }),
                scheduler_key: None,
                passivation: PassivationPolicy::Disabled,
                self_ref: None,
                service: ServiceContext::empty(),
            },
        )
        .await;

    assert!(matches!(
        shard,
        Err(ActorSpawnError::InvalidExecutionPolicy { .. })
    ));
    assert!(matches!(
        dedicated,
        Err(ActorSpawnError::InvalidExecutionPolicy { .. })
    ));
}

#[tokio::test]
async fn dedicated_thread_pool_reuses_configured_worker_threads() {
    let runtime = ActorRuntime::default();
    let first = runtime
        .spawn_actor(
            TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                execution: Some(ActorExecutionPolicy::DedicatedThreadPool { worker_count: 1 }),
                scheduler_key: None,
                passivation: PassivationPolicy::Disabled,
                self_ref: None,
                service: ServiceContext::empty(),
            },
        )
        .await
        .unwrap();
    let second = runtime
        .spawn_actor(
            TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                execution: Some(ActorExecutionPolicy::DedicatedThreadPool { worker_count: 1 }),
                scheduler_key: None,
                passivation: PassivationPolicy::Disabled,
                self_ref: None,
                service: ServiceContext::empty(),
            },
        )
        .await
        .unwrap();

    assert_eq!(
        first.ask(CurrentThread, ASK_TIMEOUT).await.unwrap(),
        second.ask(CurrentThread, ASK_TIMEOUT).await.unwrap()
    );
}

#[tokio::test]
async fn dedicated_thread_pool_is_scoped_by_actor_type() {
    let runtime = ActorRuntime::default();
    let first = runtime
        .spawn_actor(
            TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                execution: Some(ActorExecutionPolicy::DedicatedThreadPool { worker_count: 1 }),
                scheduler_key: None,
                passivation: PassivationPolicy::Disabled,
                self_ref: None,
                service: ServiceContext::empty(),
            },
        )
        .await
        .unwrap();
    let second = runtime
        .spawn_actor(
            OtherActor,
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                execution: Some(ActorExecutionPolicy::DedicatedThreadPool { worker_count: 1 }),
                scheduler_key: None,
                passivation: PassivationPolicy::Disabled,
                self_ref: None,
                service: ServiceContext::empty(),
            },
        )
        .await
        .unwrap();

    assert_ne!(
        first.ask(CurrentThread, ASK_TIMEOUT).await.unwrap(),
        second.ask(CurrentThread, ASK_TIMEOUT).await.unwrap()
    );
}

#[tokio::test]
async fn keyed_worker_pool_uses_scheduler_key_for_worker_affinity() {
    let runtime = ActorRuntime::default();
    let first = runtime
        .spawn_actor(
            TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                execution: Some(ActorExecutionPolicy::KeyedWorkerPool { worker_count: 2 }),
                scheduler_key: Some(ActorId::Str("same-key".to_string())),
                passivation: PassivationPolicy::Disabled,
                self_ref: None,
                service: ServiceContext::empty(),
            },
        )
        .await
        .unwrap();
    let second = runtime
        .spawn_actor(
            TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                execution: Some(ActorExecutionPolicy::KeyedWorkerPool { worker_count: 2 }),
                scheduler_key: Some(ActorId::Str("same-key".to_string())),
                passivation: PassivationPolicy::Disabled,
                self_ref: None,
                service: ServiceContext::empty(),
            },
        )
        .await
        .unwrap();

    assert_eq!(
        first.ask(CurrentThread, ASK_TIMEOUT).await.unwrap(),
        second.ask(CurrentThread, ASK_TIMEOUT).await.unwrap()
    );
}

#[test]
fn keyed_worker_pool_maps_actor_identity_deterministically_to_worker() {
    let actor_id = ActorId::U64(42);

    let first = ActorScheduler::keyed_worker_index(&actor_id, 8).unwrap();
    let second = ActorScheduler::keyed_worker_index(&actor_id, 8).unwrap();
    let zero = ActorScheduler::keyed_worker_index(&actor_id, 0);

    assert_eq!(first, second);
    assert!(first < 8);
    assert!(matches!(
        zero,
        Err(ActorSpawnError::InvalidExecutionPolicy { .. })
    ));
}
