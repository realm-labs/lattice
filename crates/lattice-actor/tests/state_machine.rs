use std::collections::VecDeque;
use std::time::Duration;

use async_trait::async_trait;
use lattice_actor::context::ActorContext;
use lattice_actor::error::ActorError;
use lattice_actor::runtime::{ActorRuntime, ActorSpawnOptions};
use lattice_actor::traits::{Actor, Handler, Message};

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

#[derive(Debug, Clone, Copy)]
struct StartMatch {
    operation_id: u64,
}

impl Message for StartMatch {
    type Reply = StartMatchReply;
}

#[derive(Debug, PartialEq, Eq)]
struct StartMatchReply {
    accepted: bool,
    queued: bool,
}

#[derive(Debug)]
struct LoadingFinished;

impl Message for LoadingFinished {
    type Reply = ();
}

#[derive(Debug)]
struct WorldTick;

impl Message for WorldTick {
    type Reply = ();
}

#[derive(Debug)]
struct InspectState;

impl Message for InspectState {
    type Reply = (MatchState, Vec<u64>);
}

#[async_trait]
impl Handler<StartMatch> for MatchActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: StartMatch,
    ) -> Result<StartMatchReply, ActorError> {
        match self.state {
            MatchState::Loading => {
                self.pending_starts.push_back(msg);
                Ok(StartMatchReply {
                    accepted: false,
                    queued: true,
                })
            }
            MatchState::WaitingPlayers => {
                self.state = MatchState::Running { tick: 0 };
                Ok(StartMatchReply {
                    accepted: true,
                    queued: false,
                })
            }
            MatchState::Running { .. } => Ok(StartMatchReply {
                accepted: false,
                queued: false,
            }),
        }
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
impl Handler<InspectState> for MatchActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: InspectState,
    ) -> Result<(MatchState, Vec<u64>), ActorError> {
        Ok((
            self.state,
            self.pending_starts
                .iter()
                .map(|start| start.operation_id)
                .collect(),
        ))
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

    let reply = handle.call(StartMatch { operation_id: 7 }).await.unwrap();
    assert_eq!(
        reply,
        StartMatchReply {
            accepted: false,
            queued: true
        }
    );
    assert_eq!(
        handle.call(InspectState).await.unwrap(),
        (MatchState::Loading, vec![7])
    );

    let (state, pending) = tokio::time::timeout(Duration::from_millis(250), async {
        loop {
            let snapshot = handle.call(InspectState).await.unwrap();
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
