use std::sync::Arc;

use async_trait::async_trait;
use lattice_actor::context::ActorContext;
use lattice_actor::error::{ActorError, ActorSpawnError};
use lattice_actor::mailbox::MailboxConfig;
use lattice_actor::reply::ReplyTo;
use lattice_actor::runtime::{ActorExecutionPolicy, ActorScheduler, PassivationPolicy};
use lattice_actor::runtime::{ActorRuntime, ActorSpawnOptions};
use lattice_actor::traits::{Actor, Responder};
use lattice_core::id::ActorId;
use lattice_core::service_context::ServiceContext;
use tokio::sync::Mutex;

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

#[async_trait]
impl Actor for TestActor {
    type Error = ActorError;
}

#[async_trait]
impl Actor for OtherActor {
    type Error = ActorError;
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

    let reply = handle.ask(Ping("dedicated")).await.unwrap();

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

    let reply = handle.ask(Ping("shard-worker")).await.unwrap();

    assert_eq!(reply, "pong:shard-worker");
    assert_eq!(*events.lock().await, vec!["shard-worker"]);
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
        first.ask(CurrentThread).await.unwrap(),
        second.ask(CurrentThread).await.unwrap()
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
        first.ask(CurrentThread).await.unwrap(),
        second.ask(CurrentThread).await.unwrap()
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
        first.ask(CurrentThread).await.unwrap(),
        second.ask(CurrentThread).await.unwrap()
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
