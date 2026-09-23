use lattice_actor::runtime::ActorRuntime;
use lattice_actor_distributed::registry::ActorDefinition;
use lattice_model::actor::ErasedProtocol;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use lattice_actor_distributed::ActorId;
use lattice_actor_distributed::{
    context::HandlerContext,
    error::ActorFailure,
    observation::{
        ActorLifecycleEvent, ActorMetadata, ActorObserver, ActorObserverHandle, RequestCompletion,
    },
    registry::{ActorRegistry, ActorRegistryConfig},
    reply::ReplyTo,
    traits::{Actor, ActorLifecycleState, MessageMetadata, MessageOutcome, Responder, StopReason},
};

const TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PanicPhase {
    Started,
    Enqueued,
    MessageStarted,
    MessageFinished,
    RequestCompleted,
    Stopped,
}

struct PanickingObserver {
    phase: PanicPhase,
    panics: Arc<AtomicUsize>,
}

impl PanickingObserver {
    fn observe(&self, phase: PanicPhase) {
        if self.phase == phase {
            self.panics.fetch_add(1, Ordering::SeqCst);
            panic!("observer failed in {phase:?}");
        }
    }
}

impl ActorObserver for PanickingObserver {
    fn message_enqueued(&self, _: &ActorMetadata, _: &MessageMetadata, _: usize) {
        self.observe(PanicPhase::Enqueued);
    }

    fn message_started(&self, _: &ActorMetadata, _: &MessageMetadata) {
        self.observe(PanicPhase::MessageStarted);
    }

    fn message_finished(
        &self,
        _: &ActorMetadata,
        _: &MessageMetadata,
        _: MessageOutcome,
        _: Duration,
    ) {
        self.observe(PanicPhase::MessageFinished);
    }

    fn request_completed(&self, _: &ActorMetadata, _: &MessageMetadata, _: RequestCompletion) {
        self.observe(PanicPhase::RequestCompleted);
    }

    fn lifecycle(&self, _: &ActorMetadata, event: ActorLifecycleEvent) {
        match event {
            ActorLifecycleEvent::Started => self.observe(PanicPhase::Started),
            ActorLifecycleEvent::Stopped(_) => self.observe(PanicPhase::Stopped),
            _ => {}
        }
    }
}

struct HealthyActor;

impl Actor for HealthyActor {
    type Error = ActorFailure;
    type Behavior = lattice_actor_distributed::state_machine::Stateless;
}

#[derive(lattice_actor::Request)]
#[request(response = u32)]
struct Ping;

impl Responder<Ping> for HealthyActor {
    async fn respond(
        &mut self,
        _: &mut HandlerContext<'_, Self>,
        _: Ping,
        reply: ReplyTo<u32>,
    ) -> Result<(), ActorFailure> {
        reply.send(7)?;
        Ok(())
    }
}

#[tokio::test]
async fn observer_panics_preserve_messaging_registry_cleanup_and_deathwatch() {
    for phase in [
        PanicPhase::Started,
        PanicPhase::Enqueued,
        PanicPhase::MessageStarted,
        PanicPhase::MessageFinished,
        PanicPhase::RequestCompleted,
        PanicPhase::Stopped,
    ] {
        let panics = Arc::new(AtomicUsize::new(0));
        let actor_runtime = ActorRuntime::default();
        let registry = ActorRegistry::<ObserverIsolationDefinition, HealthyActor>::new(
            actor_runtime.spawner(),
            ActorRegistryConfig::default(),
        )
        .with_observer(ActorObserverHandle::new(PanickingObserver {
            phase,
            panics: panics.clone(),
        }));
        let actor_id = ActorId::new(1_u64.to_be_bytes().to_vec()).expect("valid actor ID");
        let handle = registry
            .start(actor_id.clone(), HealthyActor)
            .await
            .unwrap();
        let mut termination = handle.subscribe_terminated();
        assert_eq!(handle.ask(Ping, TIMEOUT).await.unwrap(), 7, "{phase:?}");
        assert_eq!(handle.ask(Ping, TIMEOUT).await.unwrap(), 7, "{phase:?}");
        handle.stop(StopReason::Requested).unwrap();
        tokio::time::timeout(TIMEOUT, termination.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            handle.lifecycle_state(),
            ActorLifecycleState::Stopped,
            "{phase:?}"
        );
        assert!(registry.get_running(&actor_id).is_none(), "{phase:?}");
        assert!(registry.active_actor_ids().is_empty(), "{phase:?}");
        assert!(termination.try_recv().is_err(), "{phase:?}");
        assert_eq!(
            panics.load(Ordering::SeqCst),
            1,
            "observer must be disabled after its first panic: {phase:?}"
        );
    }
}

#[derive(Debug)]
struct ObserverIsolationDefinition;

impl ActorDefinition for ObserverIsolationDefinition {
    const NAME: &'static str = "ObserverIsolation";
    type Protocol = ErasedProtocol;
}
