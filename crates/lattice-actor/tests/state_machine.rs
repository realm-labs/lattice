use std::collections::VecDeque;
use std::time::Duration;

use async_trait::async_trait;
use lattice_actor::context::ActorContext;
use lattice_actor::error::ActorError;
use lattice_actor::reply::ReplyTo;
use lattice_actor::runtime::{ActorRuntime, ActorSpawnOptions};
use lattice_actor::traits::{Actor, Handler, Responder};

const ASK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MatchState {
    Loading,
    WaitingPlayers,
    Running { tick: u64 },
}

struct MatchActor {
    state: MatchState,
    pending_starts: VecDeque<StartMatch>,
}

#[async_trait]
impl Actor for MatchActor {
    type Error = ActorError;
    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        ctx.notify_after(Duration::from_millis(10), LoadingFinished);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, lattice_actor::Request)]
#[request(response = StartMatchReply)]
struct StartMatch {
    operation_id: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct StartMatchReply {
    accepted: bool,
    queued: bool,
}

#[derive(Debug, lattice_actor::Message)]
struct LoadingFinished;

#[derive(Debug, lattice_actor::Message)]
struct WorldTick;

#[derive(Debug, lattice_actor::Request)]
#[request(response = (MatchState, Vec<u64>))]
struct InspectState;

#[async_trait]
impl Responder<StartMatch> for MatchActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        request: StartMatch,
        reply_to: ReplyTo<StartMatchReply>,
    ) -> Result<(), ActorError> {
        let reply = match self.state {
            MatchState::Loading => {
                self.pending_starts.push_back(request);
                StartMatchReply {
                    accepted: false,
                    queued: true,
                }
            }
            MatchState::WaitingPlayers => {
                self.state = MatchState::Running { tick: 0 };
                StartMatchReply {
                    accepted: true,
                    queued: false,
                }
            }
            MatchState::Running { .. } => StartMatchReply {
                accepted: false,
                queued: false,
            },
        };
        let _ = reply_to.send(reply);
        Ok(())
    }
}

#[async_trait]
impl Handler<LoadingFinished> for MatchActor {
    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        _msg: LoadingFinished,
    ) -> Result<(), ActorError> {
        self.state = MatchState::WaitingPlayers;
        if self.pending_starts.pop_front().is_some() {
            self.state = MatchState::Running { tick: 0 };
            ctx.notify_after(Duration::from_millis(10), WorldTick);
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<WorldTick> for MatchActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: WorldTick,
    ) -> Result<(), ActorError> {
        if let MatchState::Running { tick } = &mut self.state {
            *tick += 1;
        }
        Ok(())
    }
}

#[async_trait]
impl Responder<InspectState> for MatchActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _request: InspectState,
        reply_to: ReplyTo<(MatchState, Vec<u64>)>,
    ) -> Result<(), ActorError> {
        let _ = reply_to.send((
            self.state,
            self.pending_starts
                .iter()
                .map(|start| start.operation_id)
                .collect::<Vec<_>>(),
        ));
        Ok(())
    }
}

#[tokio::test]
async fn business_actor_models_state_machine_with_typed_messages_and_timer() {
    let runtime = ActorRuntime::default();
    let handle = runtime
        .spawn_actor(
            MatchActor {
                state: MatchState::Loading,
                pending_starts: VecDeque::new(),
            },
            ActorSpawnOptions::default(),
        )
        .await
        .unwrap();

    let reply = handle
        .ask(StartMatch { operation_id: 7 }, ASK_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(
        reply,
        StartMatchReply {
            accepted: false,
            queued: true
        }
    );
    assert_eq!(
        handle.ask(InspectState, ASK_TIMEOUT).await.unwrap(),
        (MatchState::Loading, vec![7])
    );

    let (state, pending) = tokio::time::timeout(Duration::from_millis(250), async {
        loop {
            let snapshot = handle.ask(InspectState, ASK_TIMEOUT).await.unwrap();
            if matches!(snapshot.0, MatchState::Running { tick } if tick >= 1) {
                return snapshot;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();

    assert!(matches!(state, MatchState::Running { tick } if tick >= 1));
    assert!(pending.is_empty());
}
