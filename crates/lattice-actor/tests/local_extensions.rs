use std::{
    cell::Cell,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use lattice_actor::{
    context::{ActorContext, ActorLocalExtensions, HandlerContext},
    error::{ActorError, ActorStopError},
    handle::ActorHandle,
    mailbox::MailboxConfig,
    reply::ReplyTo,
    runtime::spawn_actor,
    traits::{
        Actor, ActorLifecycleState, ChildActorKey, ChildActorOptions, ChildSupervision, Handler,
        PassivationReason, Responder, StopReason,
    },
    watch::TerminatedReason,
};
use tokio::sync::mpsc;

const TIMEOUT: Duration = Duration::from_secs(2);

#[test]
fn actor_local_extensions_are_type_indexed_and_lazy() {
    let mut extensions = ActorLocalExtensions::new();

    assert!(extensions.get::<u32>().is_none());
    assert_eq!(extensions.insert(7_u32), None);
    assert_eq!(extensions.insert(String::from("alpha")), None);
    assert_eq!(extensions.insert(11_u32), Some(7));
    assert_eq!(extensions.get::<u32>(), Some(&11));
    assert_eq!(
        extensions.get::<String>().map(String::as_str),
        Some("alpha")
    );

    *extensions.get_mut::<u32>().unwrap() += 1;
    assert_eq!(extensions.remove::<u32>(), Some(12));
    assert!(extensions.remove::<u32>().is_none());

    let create_calls = Cell::new(0_u32);
    let value = extensions.get_or_insert_with::<Vec<u8>>(|| {
        create_calls.set(create_calls.get() + 1);
        vec![1]
    });
    value.push(2);
    let value = extensions.get_or_insert_with::<Vec<u8>>(|| {
        create_calls.set(create_calls.get() + 1);
        vec![9]
    });
    assert_eq!(value, &[1, 2]);
    assert_eq!(create_calls.get(), 1);

    // Cell is Send but not Sync; actor-local values intentionally need only Send.
    extensions.insert(Cell::new(3_u32));
    extensions.get::<Cell<u32>>().unwrap().set(4);
    assert_eq!(extensions.remove::<Cell<u32>>().unwrap().get(), 4);
    assert!(format!("{extensions:?}").contains("extension_count"));
}

#[test]
fn actor_local_extension_can_be_removed_for_a_call_and_restored() {
    #[derive(Debug, PartialEq, Eq)]
    struct RuntimeSlot(u64);

    let mut extensions = ActorLocalExtensions::new();
    extensions.insert(RuntimeSlot(17));

    let runtime = extensions
        .remove::<RuntimeSlot>()
        .expect("runtime slot should be available to the call");
    assert!(extensions.get::<RuntimeSlot>().is_none());

    let temporary_result = runtime.0 + 1;
    assert_eq!(extensions.insert(runtime), None);
    assert_eq!(temporary_result, 18);
    assert_eq!(extensions.get::<RuntimeSlot>(), Some(&RuntimeSlot(17)));
}

#[derive(Debug, lattice_actor::Message)]
struct Bump;

#[derive(Debug, lattice_actor::Message)]
struct ResumeLater;

#[derive(Debug, lattice_actor::Request)]
#[request(response = u32)]
struct BumpAndRead;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExtensionEvent {
    Started(u32),
    Handled(u32),
    Continued(u32),
    Stopping { attempt: usize, value: u32 },
}

struct LocalCounter(Cell<u32>);

struct ExtensionLifecycleActor {
    events: mpsc::UnboundedSender<ExtensionEvent>,
    stop_attempts: usize,
}

impl Actor for ExtensionLifecycleActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;

    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        assert!(ctx.local_extensions().get::<LocalCounter>().is_none());
        ctx.local_extensions_mut()
            .insert(LocalCounter(Cell::new(10)));
        self.events.send(ExtensionEvent::Started(10)).unwrap();
        Ok(())
    }

    async fn stopping(
        &mut self,
        ctx: &mut ActorContext<Self>,
        _reason: StopReason,
    ) -> Result<(), ActorStopError> {
        self.stop_attempts += 1;
        let value = ctx
            .local_extensions()
            .get::<LocalCounter>()
            .expect("local counter should survive until stopping")
            .0
            .get();
        self.events
            .send(ExtensionEvent::Stopping {
                attempt: self.stop_attempts,
                value,
            })
            .unwrap();
        if self.stop_attempts == 1 {
            Err(ActorStopError::new("retry extension persistence"))
        } else {
            Ok(())
        }
    }
}

impl Handler<Bump> for ExtensionLifecycleActor {
    async fn handle(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        _message: Bump,
    ) -> Result<(), ActorError> {
        let counter = ctx
            .local_extensions_mut()
            .get_mut::<LocalCounter>()
            .expect("handler should observe started extensions");
        let value = counter.0.get() + 1;
        counter.0.set(value);
        self.events.send(ExtensionEvent::Handled(value)).unwrap();
        Ok(())
    }
}

impl Handler<ResumeLater> for ExtensionLifecycleActor {
    async fn handle(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        _message: ResumeLater,
    ) -> Result<(), ActorError> {
        let events = self.events.clone();
        ctx.continue_with(async {}, move |_actor, ctx, ()| {
            let counter = ctx
                .local_extensions_mut()
                .get_mut::<LocalCounter>()
                .expect("continuation should observe actor-local extensions");
            let value = counter.0.get() + 1;
            counter.0.set(value);
            events.send(ExtensionEvent::Continued(value)).unwrap();
            Ok(())
        })?;
        Ok(())
    }
}

impl Responder<BumpAndRead> for ExtensionLifecycleActor {
    async fn respond(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        _request: BumpAndRead,
        reply_to: ReplyTo<u32>,
    ) -> Result<(), ActorError> {
        let counter = ctx
            .local_extensions_mut()
            .get_mut::<LocalCounter>()
            .expect("responder should observe actor-local extensions");
        let value = counter.0.get() + 1;
        counter.0.set(value);
        reply_to
            .send(value)
            .map_err(|_| ActorError::new("request receiver dropped"))
    }
}

async fn next_event(events: &mut mpsc::UnboundedReceiver<ExtensionEvent>) -> ExtensionEvent {
    tokio::time::timeout(TIMEOUT, events.recv())
        .await
        .expect("extension event timed out")
        .expect("extension event channel closed")
}

#[tokio::test]
async fn extensions_survive_actor_turns_continuations_and_stop_retries() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let handle = spawn_actor(
        ExtensionLifecycleActor {
            events: events_tx,
            stop_attempts: 0,
        },
        MailboxConfig::bounded(8),
    );
    let mut lifecycle = handle.subscribe_lifecycle();

    assert_eq!(
        next_event(&mut events_rx).await,
        ExtensionEvent::Started(10)
    );
    handle.tell(Bump).await.unwrap();
    assert_eq!(
        next_event(&mut events_rx).await,
        ExtensionEvent::Handled(11)
    );
    handle.tell(Bump).await.unwrap();
    assert_eq!(
        next_event(&mut events_rx).await,
        ExtensionEvent::Handled(12)
    );
    handle.tell(ResumeLater).await.unwrap();
    assert_eq!(
        next_event(&mut events_rx).await,
        ExtensionEvent::Continued(13)
    );
    assert_eq!(handle.ask(BumpAndRead, TIMEOUT).await.unwrap(), 14);

    handle.stop(StopReason::Requested).await.unwrap();
    assert_eq!(
        next_event(&mut events_rx).await,
        ExtensionEvent::Stopping {
            attempt: 1,
            value: 14,
        }
    );
    tokio::time::timeout(TIMEOUT, async {
        while *lifecycle.borrow() != ActorLifecycleState::StopFailed {
            lifecycle.changed().await.unwrap();
        }
    })
    .await
    .unwrap();

    handle.retry_stop().await.unwrap();
    assert_eq!(
        next_event(&mut events_rx).await,
        ExtensionEvent::Stopping {
            attempt: 2,
            value: 14,
        }
    );
    assert_eq!(handle.lifecycle_state(), ActorLifecycleState::Stopped);
}

struct RestartMarker;

struct RestartedChild {
    starts: mpsc::UnboundedSender<bool>,
}

impl Actor for RestartedChild {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;

    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        let fresh = ctx.local_extensions().get::<RestartMarker>().is_none();
        ctx.local_extensions_mut().insert(RestartMarker);
        self.starts.send(fresh).unwrap();
        Ok(())
    }
}

#[derive(Debug, lattice_actor::Message)]
struct StopSupervisedChild;

struct RestartingParent {
    child: Option<ActorHandle<RestartedChild>>,
    starts: mpsc::UnboundedSender<bool>,
}

impl Actor for RestartingParent {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;

    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        let starts = self.starts.clone();
        self.child = Some(ctx.spawn_child_with_factory(
            ChildActorKey::new("extension-child"),
            move || RestartedChild {
                starts: starts.clone(),
            },
            ChildActorOptions {
                protocol_id: None,
                mailbox: MailboxConfig::bounded(8),
                supervision: ChildSupervision::RestartChild,
                ..ChildActorOptions::default()
            },
        )?);
        Ok(())
    }
}

impl Handler<StopSupervisedChild> for RestartingParent {
    async fn handle(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        _message: StopSupervisedChild,
    ) -> Result<(), ActorError> {
        self.child
            .as_ref()
            .expect("supervised child should exist")
            .stop(StopReason::Requested)
            .await
            .map_err(|error| ActorError::new(error.to_string()))
    }
}

#[tokio::test]
async fn supervision_restart_starts_with_empty_extensions() {
    let (starts_tx, mut starts_rx) = mpsc::unbounded_channel();
    let parent = spawn_actor(
        RestartingParent {
            child: None,
            starts: starts_tx,
        },
        MailboxConfig::bounded(8),
    );

    assert!(
        tokio::time::timeout(TIMEOUT, starts_rx.recv())
            .await
            .unwrap()
            .unwrap(),
        "initial actor context should be empty"
    );
    parent.tell(StopSupervisedChild).await.unwrap();
    assert!(
        tokio::time::timeout(TIMEOUT, starts_rx.recv())
            .await
            .unwrap()
            .unwrap(),
        "replacement actor context should be empty"
    );
    parent.stop(StopReason::Requested).await.unwrap();
}

struct DropProbe(Arc<AtomicUsize>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Debug, lattice_actor::Request)]
#[request(response = ())]
struct Passivate;

struct PassivatingExtensionActor {
    drops: Arc<AtomicUsize>,
}

impl Actor for PassivatingExtensionActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;

    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        ctx.local_extensions_mut()
            .insert(DropProbe(self.drops.clone()));
        Ok(())
    }
}

impl Responder<Passivate> for PassivatingExtensionActor {
    async fn respond(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        _request: Passivate,
        reply_to: ReplyTo<()>,
    ) -> Result<(), ActorError> {
        ctx.request_passivation(PassivationReason::BusinessIdle)?;
        reply_to
            .send(())
            .map_err(|_| ActorError::new("request receiver dropped"))
    }
}

#[tokio::test]
async fn passivation_drops_actor_local_extensions() {
    let drops = Arc::new(AtomicUsize::new(0));
    let handle = spawn_actor(
        PassivatingExtensionActor {
            drops: drops.clone(),
        },
        MailboxConfig::bounded(8),
    );
    let mut lifecycle = handle.subscribe_lifecycle();

    handle.ask(Passivate, TIMEOUT).await.unwrap();
    tokio::time::timeout(TIMEOUT, async {
        while *lifecycle.borrow() != ActorLifecycleState::Stopped {
            lifecycle.changed().await.unwrap();
        }
    })
    .await
    .unwrap();

    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[derive(Debug, lattice_actor::Message)]
struct PanicNow;

struct PanickingExtensionActor {
    drops: Arc<AtomicUsize>,
}

impl Actor for PanickingExtensionActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;

    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        ctx.local_extensions_mut()
            .insert(DropProbe(self.drops.clone()));
        Ok(())
    }
}

impl Handler<PanicNow> for PanickingExtensionActor {
    async fn handle(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        _message: PanicNow,
    ) -> Result<(), ActorError> {
        panic!("extension cleanup panic fixture")
    }
}

#[tokio::test]
async fn panic_drops_actor_local_extensions() {
    let drops = Arc::new(AtomicUsize::new(0));
    let handle = spawn_actor(
        PanickingExtensionActor {
            drops: drops.clone(),
        },
        MailboxConfig::bounded(8),
    );
    let mut terminated = handle.subscribe_terminated();

    handle.tell(PanicNow).await.unwrap();
    let event = tokio::time::timeout(TIMEOUT, terminated.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(event.reason, TerminatedReason::Panicked);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
