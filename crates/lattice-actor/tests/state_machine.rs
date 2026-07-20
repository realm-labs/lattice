use lattice_actor::context::HandlerContext;
use std::collections::VecDeque;
use std::time::Duration;

use lattice_actor::actor_behavior;
use lattice_actor::error::{ActorCallError, ActorError};
use lattice_actor::reply::ReplyTo;
use lattice_actor::runtime::{ActorRuntime, ActorSpawnOptions};
use lattice_actor::state_machine::Accepts;
use lattice_actor::traits::{Actor, Handler, Responder};

const ASK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum MatchState {
    #[default]
    Loading,
    WaitingPlayers,
    Running {
        tick: u64,
    },
}

struct MatchActor {
    pending_starts: VecDeque<StartMatch>,
}

impl Actor for MatchActor {
    type Error = ActorError;
    type Behavior = MatchState;
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

#[derive(Debug, lattice_actor::Request)]
#[request(response = u64)]
struct CurrentTick;

actor_behavior! {
    MatchState {
        always => [StartMatch, InspectState];
        MatchState::Loading => [LoadingFinished];
        MatchState::Running { .. } => [WorldTick, CurrentTick];
    }
}

const _: () = {
    assert!(<MatchState as Accepts<StartMatch>>::ALWAYS);
    assert!(!<MatchState as Accepts<WorldTick>>::ALWAYS);
};

impl Responder<StartMatch> for MatchActor {
    async fn respond(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        request: StartMatch,
        reply_to: ReplyTo<StartMatchReply>,
    ) -> Result<(), ActorError> {
        let reply = match ctx.behavior() {
            MatchState::Loading => {
                self.pending_starts.push_back(request);
                StartMatchReply {
                    accepted: false,
                    queued: true,
                }
            }
            MatchState::WaitingPlayers => {
                ctx.transition_to(MatchState::Running { tick: 0 });
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

impl Handler<LoadingFinished> for MatchActor {
    async fn handle(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        _msg: LoadingFinished,
    ) -> Result<(), ActorError> {
        ctx.transition_to(MatchState::WaitingPlayers);
        if self.pending_starts.pop_front().is_some() {
            ctx.transition_to(MatchState::Running { tick: 0 });
            ctx.notify_after(Duration::from_millis(10), WorldTick);
        }
        Ok(())
    }
}

impl Handler<WorldTick> for MatchActor {
    async fn handle(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        _msg: WorldTick,
    ) -> Result<(), ActorError> {
        let MatchState::Running { tick } = ctx.behavior_mut() else {
            unreachable!("state admission guarantees WorldTick is handled only while running")
        };
        *tick += 1;
        Ok(())
    }
}

impl Responder<InspectState> for MatchActor {
    async fn respond(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        _request: InspectState,
        reply_to: ReplyTo<(MatchState, Vec<u64>)>,
    ) -> Result<(), ActorError> {
        let _ = reply_to.send((
            *ctx.behavior(),
            self.pending_starts
                .iter()
                .map(|start| start.operation_id)
                .collect::<Vec<_>>(),
        ));
        Ok(())
    }
}

impl Responder<CurrentTick> for MatchActor {
    async fn respond(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        _request: CurrentTick,
        reply_to: ReplyTo<u64>,
    ) -> Result<(), ActorError> {
        let MatchState::Running { tick } = ctx.behavior() else {
            unreachable!("state admission guarantees CurrentTick is handled only while running")
        };
        let _ = reply_to.send(*tick);
        Ok(())
    }
}

#[tokio::test]
async fn business_actor_models_state_machine_with_typed_messages_and_timer() {
    assert!(!Accepts::<WorldTick>::accepts(&MatchState::Loading));
    assert!(Accepts::<WorldTick>::accepts(&MatchState::Running {
        tick: 0
    }));

    let runtime = ActorRuntime::default();
    let handle = runtime
        .spawn_actor(
            MatchActor {
                pending_starts: VecDeque::new(),
            },
            ActorSpawnOptions::default(),
        )
        .await
        .unwrap();

    handle.try_tell(WorldTick).unwrap();
    assert!(matches!(
        handle.ask(CurrentTick, ASK_TIMEOUT).await,
        Err(ActorCallError::UnhandledInCurrentState)
    ));

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
    handle.try_tell(LoadingFinished).unwrap();

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
    assert!(handle.ask(CurrentTick, ASK_TIMEOUT).await.unwrap() >= 1);
}
