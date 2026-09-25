use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use lattice_actor::{
    attachments::{ActorExecutionGate, ActorRuntimeAttachments},
    context::HandlerContext,
    runtime::{ActorRuntime, ActorSpawnOptions},
    state_machine::Stateless,
    traits::{Actor, Handler},
};
use tokio::sync::Semaphore;

struct GateActor {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    calls: Arc<AtomicUsize>,
}
#[derive(Debug, lattice_actor::Message)]
struct Turn(bool);
impl Actor for GateActor {
    type Error = Infallible;
    type Behavior = Stateless;
}
impl Handler<Turn> for GateActor {
    async fn handle(
        &mut self,
        _: &mut HandlerContext<'_, Self>,
        Turn(block): Turn,
    ) -> Result<(), Infallible> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if block {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        Ok(())
    }
}

#[tokio::test]
async fn execution_gate_rejects_queued_work_and_cannot_revive_a_cached_handle() {
    let runtime = ActorRuntime::default();
    let live = Arc::new(AtomicBool::new(true));
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut attachments = ActorRuntimeAttachments::builder();
    let predicate = live.clone();
    attachments
        .insert(ActorExecutionGate(Arc::new(move || {
            predicate.load(Ordering::SeqCst)
        })))
        .unwrap();
    let handle = runtime
        .spawner()
        .spawn_managed_actor(
            GateActor {
                entered: entered.clone(),
                release: release.clone(),
                calls: calls.clone(),
            },
            ActorSpawnOptions::default(),
            attachments.build(),
            None,
        )
        .unwrap();
    handle.tell(Turn(true)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    handle.tell(Turn(false)).await.unwrap();
    live.store(false, Ordering::SeqCst);
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), async {
        while !handle.business_admission_fenced() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    live.store(true, Ordering::SeqCst);
    assert!(handle.tell(Turn(false)).await.is_err());
    assert!(handle.business_admission_fenced());
}
