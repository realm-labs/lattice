use std::{
    sync::Arc,
    thread::{self, ThreadId},
    time::Duration,
};

use lattice_actor::{
    context::HandlerContext,
    environment::ActorEnvironment,
    error::{ActorFailure, ActorSpawnError},
    reply::ReplyTo,
    runtime::{
        ActorExecutionPolicy, ActorRuntime, ActorRuntimeConfig, ActorSpawnOptions, WorkerPlacement,
        WorkerPoolKey,
    },
    state_machine::Stateless,
    traits::{Actor, Responder},
};
use lattice_actor_distributed::registry::{
    ActorDefinition, ActorKey, ActorRegistry, ActorRegistryConfig,
};
use lattice_model::actor::ErasedProtocol;

struct First;
struct Second;

impl ActorDefinition for First {
    const NAME: &'static str = "first";
    type Protocol = ErasedProtocol;
}

impl ActorDefinition for Second {
    const NAME: &'static str = "second";
    type Protocol = ErasedProtocol;
}

struct ProbeActor;
impl Actor for ProbeActor {
    type Error = ActorFailure;
    type Behavior = Stateless;
}

struct SharedDependency;

#[derive(lattice_actor::Request)]
#[request(response = (ThreadId, Arc<SharedDependency>))]
struct Probe;

impl Responder<Probe> for ProbeActor {
    async fn respond(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        _: Probe,
        reply: ReplyTo<(ThreadId, Arc<SharedDependency>)>,
    ) -> Result<(), ActorFailure> {
        let _ = reply.send((
            thread::current().id(),
            ctx.environment().get::<SharedDependency>().unwrap(),
        ));
        Ok(())
    }
}

async fn verify_sharing(execution: ActorExecutionPolicy) {
    let mut environment = ActorEnvironment::builder();
    environment.insert(SharedDependency).unwrap();
    let environment = environment.build();
    let dependency = environment.get::<SharedDependency>().unwrap();
    let runtime = ActorRuntime::new(ActorRuntimeConfig {
        default_execution: execution,
        task_worker_count: 1,
        environment,
        ..Default::default()
    });
    let first =
        ActorRegistry::<First, ProbeActor>::new(runtime.spawner(), ActorRegistryConfig::default());
    let second =
        ActorRegistry::<Second, ProbeActor>::new(runtime.spawner(), ActorRegistryConfig::default());
    let a = first.start(ActorKey::U64(1), ProbeActor).await.unwrap();
    let b = second.start(ActorKey::U64(1), ProbeActor).await.unwrap();
    let (first_thread, first_dependency) = a.ask(Probe, Duration::from_secs(3)).await.unwrap();
    let (second_thread, second_dependency) = b.ask(Probe, Duration::from_secs(3)).await.unwrap();
    assert_eq!(first_thread, second_thread);
    assert!(Arc::ptr_eq(&first_dependency, &dependency));
    assert!(Arc::ptr_eq(&second_dependency, &dependency));

    first.drain().await;
    drop(first);
    assert_eq!(
        b.ask(Probe, Duration::from_secs(3)).await.unwrap().0,
        second_thread
    );
    second.drain().await;
    runtime.shutdown();
    assert!(second.start(ActorKey::U64(2), ProbeActor).await.is_err());
}

#[tokio::test]
async fn registries_share_task_executor_and_environment() {
    verify_sharing(ActorExecutionPolicy::TaskPerActor).await;
}

#[tokio::test]
async fn registries_share_named_worker_pool() {
    verify_sharing(ActorExecutionPolicy::WorkerPool {
        pool_key: WorkerPoolKey::new("shared").unwrap(),
        worker_count: 1,
        placement: WorkerPlacement::RoundRobin,
    })
    .await;
}

#[test]
fn spawner_does_not_keep_runtime_alive() {
    let runtime = ActorRuntime::default();
    let spawner = runtime.spawner();
    drop(runtime);
    assert!(matches!(
        spawner.spawn_actor(ProbeActor, ActorSpawnOptions::default()),
        Err(ActorSpawnError::ExecutorStartFailed { .. })
    ));
}
