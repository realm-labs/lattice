use lattice_actor::context::HandlerContext;
use std::sync::Arc;
use std::time::Duration;

use lattice_actor::context::ActorContext;
use lattice_actor::error::{ActorError, ActorStopError};
use lattice_actor::reply::ReplyTo;
use lattice_actor::runtime::{ActorRuntime, ActorSpawnOptions};
use lattice_actor::traits::{Actor, Handler, Responder, StopReason};
use tokio::sync::{Mutex, Semaphore};

struct WorldActor {
    ticks: Arc<Mutex<u64>>,
    stopped: Option<Arc<Semaphore>>,
}

impl Actor for WorldActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;
    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        ctx.notify_interval(Duration::from_millis(5), || WorldTick { delta_ms: 5 });
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

#[derive(Debug, lattice_actor::Message)]
struct WorldTick {
    delta_ms: u64,
}

#[derive(Debug, lattice_actor::Request)]
#[request(response = u64)]
struct InspectTicks;

impl Handler<WorldTick> for WorldActor {
    async fn handle(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        msg: WorldTick,
    ) -> Result<(), ActorError> {
        assert_eq!(msg.delta_ms, 5);
        let mut ticks = self.ticks.lock().await;
        *ticks += 1;
        if *ticks >= 2 {
            ctx.request_stop();
        }
        Ok(())
    }
}

impl Responder<InspectTicks> for WorldActor {
    async fn respond(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        _request: InspectTicks,
        reply_to: ReplyTo<u64>,
    ) -> Result<(), ActorError> {
        let _ = reply_to.send(*self.ticks.lock().await);
        Ok(())
    }
}

#[tokio::test]
async fn interval_timer_drives_tick_and_business_request_stop() {
    let runtime = ActorRuntime::default();
    let ticks = Arc::new(Mutex::new(0));
    let stopped = Arc::new(Semaphore::new(0));
    let _handle = runtime
        .spawn_actor(
            WorldActor {
                ticks: ticks.clone(),
                stopped: Some(stopped.clone()),
            },
            ActorSpawnOptions::default(),
        )
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_millis(100), stopped.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    assert_eq!(*ticks.lock().await, 2);
}
