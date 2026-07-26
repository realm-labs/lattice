//! Runtime spawn options: execution policy defaults and service-context propagation.

use std::sync::Arc;

use lattice_core::{instance::InstanceId, service_context::ServiceContext, service_kind};
use tokio::sync::Mutex;

use super::{ASK_TIMEOUT, Ping, ReadContextInstance, SpawnContextChild, TestActor};
use crate::{
    mailbox::MailboxConfig,
    runtime::{
        ActorExecutionPolicy, ActorRuntime, ActorRuntimeConfig, ActorSpawnOptions, spawn_actor,
    },
};

#[test]
fn actor_execution_policy_defaults_to_task_per_actor() {
    assert_eq!(
        ActorRuntimeConfig::default().default_execution,
        ActorExecutionPolicy::TaskPerActor
    );
    assert_eq!(ActorSpawnOptions::default().execution, None);
}

#[tokio::test]
async fn actor_runtime_spawns_task_per_actor() {
    let runtime = ActorRuntime::new(ActorRuntimeConfig::default());
    let events = Arc::new(Mutex::new(Vec::new()));
    let actor = TestActor {
        events: events.clone(),
        start_gate: None,
        stopped: None,
    };

    let handle = runtime
        .spawn_actor(actor, ActorSpawnOptions::default())
        .await
        .unwrap();
    let reply = handle.ask(Ping("runtime"), ASK_TIMEOUT).await.unwrap();

    assert_eq!(reply, "pong:runtime");
    assert_eq!(*events.lock().await, vec!["runtime"]);
}

#[tokio::test]
async fn standalone_actor_receives_empty_service_context() {
    let handle = spawn_actor(
        TestActor {
            events: Arc::new(Mutex::new(Vec::new())),
            start_gate: None,
            stopped: None,
        },
        MailboxConfig::default(),
    );

    let instance = handle.ask(ReadContextInstance, ASK_TIMEOUT).await.unwrap();

    assert_eq!(instance, InstanceId::new("local"));
}

#[tokio::test]
async fn actor_spawn_options_pass_service_context_to_handler_and_child() {
    let runtime = ActorRuntime::default();
    let service = ServiceContext::new(service_kind!("World"), InstanceId::new("world-service"));
    let handle = runtime
        .spawn_actor(
            TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
                start_gate: None,
                stopped: None,
            },
            ActorSpawnOptions {
                service: service.clone(),
                ..ActorSpawnOptions::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(
        handle.ask(ReadContextInstance, ASK_TIMEOUT).await.unwrap(),
        InstanceId::new("world-service")
    );
    assert_eq!(
        handle.ask(SpawnContextChild, ASK_TIMEOUT).await.unwrap(),
        InstanceId::new("world-service")
    );
}
