use std::sync::Arc;

use async_trait::async_trait;
use lattice_actor::{
    Actor, ActorContext, ActorError, ActorExecutionPolicy, ActorRuntime, ActorScheduler,
    ActorSpawnError, ActorSpawnOptions, Handler, MailboxConfig, Message, PassivationPolicy,
};
use lattice_core::ActorId;
use tokio::sync::Mutex;

#[derive(Debug)]
struct Ping(&'static str);

impl Message for Ping {
    type Reply = String;
}

struct TestActor {
    events: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait]
impl Actor for TestActor {}

#[async_trait]
impl Handler<Ping> for TestActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: Ping,
    ) -> Result<String, ActorError> {
        self.events.lock().await.push(msg.0);
        Ok(format!("pong:{}", msg.0))
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
                passivation: PassivationPolicy::Disabled,
            },
        )
        .await
        .unwrap();

    let reply = handle.call(Ping("dedicated")).await.unwrap();

    assert_eq!(reply, "pong:dedicated");
    assert_eq!(*events.lock().await, vec!["dedicated"]);
}

#[tokio::test]
async fn shard_worker_execution_policy_runs_actor_with_same_mailbox_semantics() {
    let runtime = ActorRuntime::default();
    let events = Arc::new(Mutex::new(Vec::new()));
    let handle = runtime
        .spawn_actor(
            TestActor {
                events: events.clone(),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                execution: Some(ActorExecutionPolicy::ShardWorker { worker_count: 4 }),
                passivation: PassivationPolicy::Disabled,
            },
        )
        .await
        .unwrap();

    let reply = handle.call(Ping("shard-worker")).await.unwrap();

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
                execution: Some(ActorExecutionPolicy::ShardWorker { worker_count: 0 }),
                passivation: PassivationPolicy::Disabled,
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
                passivation: PassivationPolicy::Disabled,
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

#[test]
fn shard_worker_maps_actor_identity_deterministically_to_worker() {
    let actor_id = ActorId::U64(42);

    let first = ActorScheduler::shard_worker_index(&actor_id, 8).unwrap();
    let second = ActorScheduler::shard_worker_index(&actor_id, 8).unwrap();
    let zero = ActorScheduler::shard_worker_index(&actor_id, 0);

    assert_eq!(first, second);
    assert!(first < 8);
    assert!(matches!(
        zero,
        Err(ActorSpawnError::InvalidExecutionPolicy { .. })
    ));
}
