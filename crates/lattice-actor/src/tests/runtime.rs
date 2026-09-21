//! Runtime configuration, execution-policy defaults, and service-context propagation.

use std::sync::Arc;

use tokio::sync::{Mutex, oneshot};

use super::{
    ASK_TIMEOUT, ContextMarker, Ping, ReadContextExtension, SpawnContextChild, TestActor,
    spawn_actor,
};
use crate::{
    attachments::ActorRuntimeAttachments,
    context::ActorContext,
    error::ActorFailure,
    mailbox::MailboxConfig,
    runtime::{ActorExecutionPolicy, ActorRuntime, ActorRuntimeConfig, ActorSpawnOptions},
    service::ServiceContext,
    traits::{Actor, ChildActorKey, ChildActorOptions},
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

    let marker = handle.ask(ReadContextExtension, ASK_TIMEOUT).await.unwrap();

    assert_eq!(marker, None);
}

#[tokio::test]
async fn actor_runtime_passes_service_context_to_handler_and_child() {
    let service = ServiceContext::empty()
        .with_extension(ContextMarker("world-service"))
        .unwrap();
    let runtime = ActorRuntime::new(ActorRuntimeConfig {
        service: service.clone(),
        ..ActorRuntimeConfig::default()
    });
    let handle = runtime
        .spawn_actor(
            TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
                start_gate: None,
                stopped: None,
            },
            ActorSpawnOptions::default(),
        )
        .unwrap();

    assert_eq!(
        handle.ask(ReadContextExtension, ASK_TIMEOUT).await.unwrap(),
        Some("world-service")
    );
    assert_eq!(
        handle.ask(SpawnContextChild, ASK_TIMEOUT).await.unwrap(),
        Some("world-service")
    );
}

struct RuntimeAttachmentMarker;

struct AttachmentChild {
    result: Option<oneshot::Sender<bool>>,
}

impl Actor for AttachmentChild {
    type Error = ActorFailure;
    type Behavior = crate::state_machine::Stateless;

    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), Self::Error> {
        let attachment_is_absent = ctx
            .runtime_attachment::<RuntimeAttachmentMarker>()
            .is_none();
        if let Some(result) = self.result.take() {
            let _ = result.send(attachment_is_absent);
        }
        Ok(())
    }
}

struct AttachmentParent {
    child_result: Option<oneshot::Sender<bool>>,
}

impl Actor for AttachmentParent {
    type Error = ActorFailure;
    type Behavior = crate::state_machine::Stateless;

    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), Self::Error> {
        assert!(
            ctx.runtime_attachment::<RuntimeAttachmentMarker>()
                .is_some()
        );
        ctx.spawn_child(
            ChildActorKey::new("attachment-child"),
            AttachmentChild {
                result: self.child_result.take(),
            },
            ChildActorOptions::default(),
        )?;
        Ok(())
    }
}

#[tokio::test]
async fn runtime_attachments_are_scoped_to_one_activation() {
    let runtime = ActorRuntime::default();
    let mut attachments = ActorRuntimeAttachments::builder();
    attachments.insert(RuntimeAttachmentMarker).unwrap();
    let (child_result, observed) = oneshot::channel();

    let _parent = runtime
        .spawn_managed_actor(
            AttachmentParent {
                child_result: Some(child_result),
            },
            ActorSpawnOptions::default(),
            attachments.build(),
            None,
        )
        .unwrap();

    assert!(observed.await.unwrap());
}
