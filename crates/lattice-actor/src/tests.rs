//! Crate-level integration tests.
//!
//! The shared actor fixtures live here; each submodule holds the tests for one runtime concern
//! and reaches the fixtures through `super`.

use std::{sync::Arc, time::Duration};

use lattice_core::instance::InstanceId;
use thiserror::Error;
use tokio::sync::{Mutex, Semaphore};

use crate::{
    context::{ActorContext, HandlerContext},
    error::{ActorCallError, ActorError, ActorStopError, PipeToSelfError},
    reply::ReplyTo,
    traits::{
        Actor, ChildActorKey, ChildActorOptions, Handler, MessageMetadata, PassivationReason,
        Responder, ResponderErrorAction, StopReason,
    },
};

mod children;
mod deferred;
mod handlers;
mod lifecycle;
mod mailbox;
#[cfg(feature = "distributed")]
mod registry;
mod runtime;
mod tasks;
mod turn_budget;

const ASK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, crate::Request)]
#[request(response = String)]
struct Ping(&'static str);

#[derive(Debug, crate::Message)]
struct Record {
    value: &'static str,
    processed: Option<Arc<Semaphore>>,
}

impl Record {
    fn new(value: &'static str) -> Self {
        Self {
            value,
            processed: None,
        }
    }

    fn with_processed_signal(value: &'static str, processed: Arc<Semaphore>) -> Self {
        Self {
            value,
            processed: Some(processed),
        }
    }
}

#[derive(Debug, crate::Request)]
#[request(response = &'static str)]
struct StopAfterReply;

#[derive(Debug, crate::Message)]
struct Tick;

#[derive(Debug, crate::Request)]
#[request(response = &'static str)]
struct DeferredReply {
    gate: Arc<Semaphore>,
    entered: Arc<Semaphore>,
}

#[derive(crate::Message)]
struct DeferredReady {
    reply_to: ReplyTo<&'static str>,
}

#[derive(crate::Message)]
struct PipeRecord {
    gate: Arc<Semaphore>,
    entered: Arc<Semaphore>,
    processed: Arc<Semaphore>,
}

#[derive(crate::Request)]
#[request(response = bool)]
struct ProbePipeCapacity {
    gate: Arc<Semaphore>,
}

#[derive(Debug, crate::Request)]
#[request(response = InstanceId)]
struct ReadContextInstance;

#[derive(Debug, crate::Request)]
#[request(response = InstanceId)]
struct SpawnContextChild;

#[derive(crate::Message)]
struct ContextChildResolved {
    result: Result<InstanceId, ActorCallError>,
    reply_to: ReplyTo<InstanceId>,
}

struct TestActor {
    events: Arc<Mutex<Vec<&'static str>>>,
    start_gate: Option<Arc<Semaphore>>,
    stopped: Option<Arc<Semaphore>>,
}

#[derive(Debug, crate::Message)]
struct Fail;

impl Actor for TestActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;
    async fn started(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        if let Some(gate) = self.start_gate.take() {
            let permit = gate
                .acquire()
                .await
                .map_err(|_| ActorError::new("start gate was closed"))?;
            permit.forget();
        }
        Ok(())
    }

    async fn stopping(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _reason: StopReason,
    ) -> Result<(), ActorStopError> {
        if let Some(stopped) = self.stopped.take() {
            stopped.add_permits(1);
        }
        Ok(())
    }
}

impl Responder<Ping> for TestActor {
    async fn respond(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        request: Ping,
        reply_to: ReplyTo<String>,
    ) -> Result<(), ActorError> {
        self.events.lock().await.push(request.0);
        let _ = ctx;
        let _ = reply_to.send(format!("pong:{}", request.0));
        Ok(())
    }
}

impl Handler<Record> for TestActor {
    async fn handle(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        msg: Record,
    ) -> Result<(), ActorError> {
        self.events.lock().await.push(msg.value);
        if let Some(processed) = msg.processed {
            processed.add_permits(1);
        }
        Ok(())
    }
}

impl Handler<PipeRecord> for TestActor {
    async fn handle(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        message: PipeRecord,
    ) -> Result<(), ActorError> {
        message.entered.add_permits(1);
        let gate = message.gate;
        let processed = message.processed;
        ctx.pipe_to_self(
            async move {
                if let Ok(permit) = gate.acquire_owned().await {
                    permit.forget();
                }
                "piped"
            },
            move |value| Record::with_processed_signal(value, processed),
        )?;
        Ok(())
    }
}

impl Responder<ProbePipeCapacity> for TestActor {
    async fn respond(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        request: ProbePipeCapacity,
        reply_to: ReplyTo<bool>,
    ) -> Result<(), ActorError> {
        let gate = request.gate;
        ctx.pipe_to_self(
            async move {
                if let Ok(permit) = gate.acquire_owned().await {
                    permit.forget();
                }
            },
            |()| Tick,
        )?;
        let rejected = matches!(
            ctx.pipe_to_self(async {}, |()| Tick),
            Err(PipeToSelfError::Capacity { capacity: 1 })
        );
        reply_to.send(rejected)?;
        Ok(())
    }
}

impl Responder<StopAfterReply> for TestActor {
    async fn respond(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        _request: StopAfterReply,
        reply_to: ReplyTo<&'static str>,
    ) -> Result<(), ActorError> {
        self.events.lock().await.push("handled");
        let _ = reply_to.send("reply-before-stop");
        ctx.request_passivation(PassivationReason::BusinessIdle)?;
        Ok(())
    }
}

impl Handler<Tick> for TestActor {
    async fn handle(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        _msg: Tick,
    ) -> Result<(), ActorError> {
        self.events.lock().await.push("tick");
        Ok(())
    }
}

impl Responder<ReadContextInstance> for TestActor {
    async fn respond(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        _request: ReadContextInstance,
        reply_to: ReplyTo<InstanceId>,
    ) -> Result<(), ActorError> {
        let _ = reply_to.send(ctx.service().instance_id().clone());
        Ok(())
    }
}

impl Responder<SpawnContextChild> for TestActor {
    async fn respond(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        _request: SpawnContextChild,
        reply_to: ReplyTo<InstanceId>,
    ) -> Result<(), ActorError> {
        let child = TestActor {
            events: Arc::new(Mutex::new(Vec::new())),
            start_gate: None,
            stopped: None,
        };
        let handle = ctx.spawn_child(
            ChildActorKey::new("context-child"),
            child,
            ChildActorOptions::default(),
        )?;
        ctx.defer_reply(
            reply_to,
            async move { handle.ask(ReadContextInstance, ASK_TIMEOUT).await },
            |result, reply_to| ContextChildResolved { result, reply_to },
        )?;
        Ok(())
    }
}

impl Handler<ContextChildResolved> for TestActor {
    async fn handle(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        message: ContextChildResolved,
    ) -> Result<(), ActorError> {
        match message.result {
            Ok(instance) => message.reply_to.send(instance)?,
            Err(error) => message.reply_to.fail_with(error)?,
        }
        Ok(())
    }
}

impl Handler<Fail> for TestActor {
    async fn handle(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        _msg: Fail,
    ) -> Result<(), ActorError> {
        Err(ActorError::new("handler failed"))
    }
}

impl Responder<DeferredReply> for TestActor {
    async fn respond(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        request: DeferredReply,
        reply_to: ReplyTo<&'static str>,
    ) -> Result<(), ActorError> {
        request.entered.add_permits(1);
        ctx.defer_reply(
            reply_to,
            async move {
                if let Ok(permit) = request.gate.acquire().await {
                    permit.forget();
                }
            },
            |(), reply_to| DeferredReady { reply_to },
        )?;
        Ok(())
    }
}

impl Handler<DeferredReady> for TestActor {
    async fn handle(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        message: DeferredReady,
    ) -> Result<(), ActorError> {
        self.events.lock().await.push("deferred-ready");
        let _ = message.reply_to.send("done");
        Ok(())
    }
}

#[derive(Debug, Error)]
enum BusinessActorError {
    #[error("business store is unavailable")]
    StoreUnavailable,
    #[error(transparent)]
    Framework(#[from] ActorError),
}

struct BusinessErrorActor {
    observed_errors: Arc<Mutex<Vec<&'static str>>>,
}

impl Actor for BusinessErrorActor {
    type Error = BusinessActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;

    async fn on_error<M>(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _metadata: &MessageMetadata,
        error: &BusinessActorError,
    ) where
        M: Send + 'static,
    {
        let label = match error {
            BusinessActorError::StoreUnavailable => "store_unavailable",
            BusinessActorError::Framework(_) => "framework",
        };
        self.observed_errors.lock().await.push(label);
    }
}

#[derive(crate::Request)]
#[request(response = ())]
struct LoadBusinessState;

#[derive(crate::Request)]
#[request(response = &'static str)]
struct RecoverBusinessState;

fn load_business_state() -> Result<(), BusinessActorError> {
    Err(BusinessActorError::StoreUnavailable)
}

impl Responder<LoadBusinessState> for BusinessErrorActor {
    async fn respond(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        _request: LoadBusinessState,
        _reply_to: ReplyTo<()>,
    ) -> Result<(), BusinessActorError> {
        ctx.request_passivation(PassivationReason::BusinessIdle)?;
        load_business_state()?;
        Ok(())
    }
}

impl Responder<RecoverBusinessState> for BusinessErrorActor {
    async fn respond(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        _request: RecoverBusinessState,
        reply_to: ReplyTo<&'static str>,
    ) -> Result<(), BusinessActorError> {
        load_business_state()?;
        let _ = reply_to.send("loaded");
        Ok(())
    }

    async fn respond_error(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        error: BusinessActorError,
    ) -> ResponderErrorAction<&'static str, BusinessActorError> {
        match error {
            BusinessActorError::StoreUnavailable => ResponderErrorAction::Respond("fallback"),
            other => ResponderErrorAction::Propagate(other),
        }
    }
}
