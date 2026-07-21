use std::{future::Future, sync::Arc, time::Duration};

use lattice_actor::{
    context::{ActorContext, HandlerContext},
    error::ActorError,
    mailbox::MailboxConfig,
    reply::ReplyTo,
    runtime::spawn_actor,
    state_machine::Stateless,
    traits::{
        Actor, Handler, MessageKind, MessageMetadata, MessageOutcome, MessageView, Request,
        Responder, StopReason,
    },
};
use tokio::sync::Semaphore;

const TIMEOUT: Duration = Duration::from_secs(1);

#[derive(lattice_actor::Message)]
struct StartWorkflow {
    first_gate: Arc<Semaphore>,
    second_gate: Arc<Semaphore>,
    entered: Arc<Semaphore>,
    first_completed: Arc<Semaphore>,
    completed: Arc<Semaphore>,
}

#[derive(lattice_actor::Message)]
struct Record(u8);

#[derive(lattice_actor::Message)]
struct StartFailure {
    gate: Arc<Semaphore>,
}

#[derive(lattice_actor::Message)]
struct StartPending {
    gate: Arc<Semaphore>,
    started: Arc<Semaphore>,
    dropped: Arc<Semaphore>,
    continuation_ran: Arc<Semaphore>,
}

struct ReadState;

impl Request for ReadState {
    type Response = WorkflowState;
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkflowState {
    values: Vec<u8>,
    observed_outputs: Vec<u8>,
    continuation_outcomes: Vec<MessageOutcome>,
    error_count: usize,
}

struct WorkflowActor {
    values: Vec<u8>,
    observed_outputs: Vec<u8>,
    continuation_outcomes: Vec<MessageOutcome>,
    error_count: usize,
    error_signal: Arc<Semaphore>,
}

impl Actor for WorkflowActor {
    type Error = ActorError;
    type Behavior = Stateless;

    fn before_message(&mut self, _context: &mut ActorContext<Self>, message: MessageView<'_>) {
        if message.metadata().kind() == MessageKind::Continuation
            && let Some(output) = message.downcast_ref::<u8>()
        {
            self.observed_outputs.push(*output);
        }
    }

    fn after_message(
        &mut self,
        _context: &mut ActorContext<Self>,
        metadata: &MessageMetadata,
        outcome: MessageOutcome,
    ) {
        if metadata.kind() == MessageKind::Continuation {
            self.continuation_outcomes.push(outcome);
        }
    }

    fn on_error<M>(
        &mut self,
        _context: &mut ActorContext<Self>,
        metadata: &MessageMetadata,
        _error: &Self::Error,
    ) -> impl Future<Output = ()> + Send
    where
        M: Send + 'static,
    {
        assert_eq!(metadata.kind(), MessageKind::Continuation);
        self.error_count += 1;
        self.error_signal.add_permits(1);
        async {}
    }
}

impl Handler<StartWorkflow> for WorkflowActor {
    async fn handle(
        &mut self,
        context: &mut HandlerContext<'_, Self>,
        message: StartWorkflow,
    ) -> Result<(), Self::Error> {
        message.entered.add_permits(1);
        let second_gate = message.second_gate;
        let first_completed = message.first_completed;
        let completed = message.completed;
        context.continue_with(
            wait_for(message.first_gate, 1),
            move |actor, context, first| {
                actor.values.push(first);
                context.continue_with(
                    wait_for(second_gate, 2),
                    move |actor, _context, second| {
                        actor.values.push(second);
                        completed.add_permits(1);
                        Ok(())
                    },
                )?;
                first_completed.add_permits(1);
                Ok(())
            },
        )?;
        Ok(())
    }
}

impl Handler<Record> for WorkflowActor {
    async fn handle(
        &mut self,
        _context: &mut HandlerContext<'_, Self>,
        message: Record,
    ) -> Result<(), Self::Error> {
        self.values.push(message.0);
        Ok(())
    }
}

impl Handler<StartFailure> for WorkflowActor {
    async fn handle(
        &mut self,
        context: &mut HandlerContext<'_, Self>,
        message: StartFailure,
    ) -> Result<(), Self::Error> {
        context.continue_with(wait_for(message.gate, 3), |_actor, _context, _output| {
            Err(ActorError::new("expected continuation failure"))
        })?;
        Ok(())
    }
}

impl Handler<StartPending> for WorkflowActor {
    async fn handle(
        &mut self,
        context: &mut HandlerContext<'_, Self>,
        message: StartPending,
    ) -> Result<(), Self::Error> {
        let continuation_ran = message.continuation_ran;
        context.continue_with(
            async move {
                let _drop_signal = DropSignal(message.dropped);
                message.started.add_permits(1);
                message.gate.acquire_owned().await.unwrap().forget();
            },
            move |_actor, _context, ()| {
                continuation_ran.add_permits(1);
                Ok(())
            },
        )?;
        Ok(())
    }
}

impl Responder<ReadState> for WorkflowActor {
    async fn respond(
        &mut self,
        _context: &mut HandlerContext<'_, Self>,
        _request: ReadState,
        reply_to: ReplyTo<WorkflowState>,
    ) -> Result<(), Self::Error> {
        reply_to.send(WorkflowState {
            values: self.values.clone(),
            observed_outputs: self.observed_outputs.clone(),
            continuation_outcomes: self.continuation_outcomes.clone(),
            error_count: self.error_count,
        })?;
        Ok(())
    }
}

async fn wait_for(gate: Arc<Semaphore>, output: u8) -> u8 {
    gate.acquire_owned().await.unwrap().forget();
    output
}

struct DropSignal(Arc<Semaphore>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}

fn actor(error_signal: Arc<Semaphore>) -> WorkflowActor {
    WorkflowActor {
        values: Vec::new(),
        observed_outputs: Vec::new(),
        continuation_outcomes: Vec::new(),
        error_count: 0,
        error_signal,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn continuation_can_mutate_actor_and_chain_at_capacity_one() {
    let error_signal = Arc::new(Semaphore::new(0));
    let handle = spawn_actor(
        actor(error_signal),
        MailboxConfig::bounded(8).with_deferred_capacity(1),
    );
    let first_gate = Arc::new(Semaphore::new(0));
    let second_gate = Arc::new(Semaphore::new(0));
    let entered = Arc::new(Semaphore::new(0));
    let first_completed = Arc::new(Semaphore::new(0));
    let completed = Arc::new(Semaphore::new(0));

    handle
        .tell(StartWorkflow {
            first_gate: first_gate.clone(),
            second_gate: second_gate.clone(),
            entered: entered.clone(),
            first_completed: first_completed.clone(),
            completed: completed.clone(),
        })
        .await
        .unwrap();
    entered.acquire().await.unwrap().forget();

    handle.tell(Record(0)).await.unwrap();
    assert_eq!(
        handle.ask(ReadState, TIMEOUT).await.unwrap().values,
        vec![0]
    );

    first_gate.add_permits(1);
    tokio::time::timeout(TIMEOUT, first_completed.acquire())
        .await
        .expect("first continuation should schedule the second step")
        .unwrap()
        .forget();
    second_gate.add_permits(1);
    tokio::time::timeout(TIMEOUT, completed.acquire())
        .await
        .expect("chained continuation should complete")
        .unwrap()
        .forget();

    let state = handle.ask(ReadState, TIMEOUT).await.unwrap();
    assert_eq!(state.values, vec![0, 1, 2]);
    assert_eq!(state.observed_outputs, vec![1, 2]);
    assert_eq!(
        state.continuation_outcomes,
        vec![MessageOutcome::Handled, MessageOutcome::Handled]
    );
}

#[tokio::test]
async fn continuation_errors_use_actor_error_handling() {
    let error_signal = Arc::new(Semaphore::new(0));
    let handle = spawn_actor(actor(error_signal.clone()), MailboxConfig::bounded(8));
    let gate = Arc::new(Semaphore::new(0));

    handle
        .tell(StartFailure { gate: gate.clone() })
        .await
        .unwrap();
    gate.add_permits(1);
    tokio::time::timeout(TIMEOUT, error_signal.acquire())
        .await
        .expect("continuation error should reach Actor::on_error")
        .unwrap()
        .forget();

    let state = handle.ask(ReadState, TIMEOUT).await.unwrap();
    assert_eq!(state.error_count, 1);
    assert_eq!(state.observed_outputs, vec![3]);
    assert_eq!(
        state.continuation_outcomes,
        vec![MessageOutcome::HandlerFailed]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actor_stop_cancels_pending_continuation_work() {
    let handle = spawn_actor(
        actor(Arc::new(Semaphore::new(0))),
        MailboxConfig::bounded(8),
    );
    let gate = Arc::new(Semaphore::new(0));
    let started = Arc::new(Semaphore::new(0));
    let dropped = Arc::new(Semaphore::new(0));
    let continuation_ran = Arc::new(Semaphore::new(0));
    let mut terminated = handle.subscribe_terminated();

    handle
        .tell(StartPending {
            gate,
            started: started.clone(),
            dropped: dropped.clone(),
            continuation_ran: continuation_ran.clone(),
        })
        .await
        .unwrap();
    tokio::time::timeout(TIMEOUT, started.acquire())
        .await
        .expect("continuation future should start")
        .unwrap()
        .forget();

    handle.stop(StopReason::Requested).await.unwrap();
    tokio::time::timeout(TIMEOUT, terminated.recv())
        .await
        .expect("actor should stop")
        .expect("termination should be delivered");
    tokio::time::timeout(TIMEOUT, dropped.acquire())
        .await
        .expect("stopping the actor should drop pending continuation work")
        .unwrap()
        .forget();
    assert_eq!(continuation_ran.available_permits(), 0);
}
