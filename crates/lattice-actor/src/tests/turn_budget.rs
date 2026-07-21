use std::sync::Arc;

use tokio::sync::{Mutex, Semaphore};

use crate::{
    context::HandlerContext,
    error::ActorError,
    mailbox::MailboxConfig,
    runtime::spawn_actor,
    state_machine::Stateless,
    traits::{Actor, Handler},
};

#[derive(crate::Message)]
struct BlockNormalTurn {
    gate: Arc<Semaphore>,
    entered: Arc<Semaphore>,
}

#[derive(crate::Message)]
struct Record {
    value: &'static str,
    processed: Arc<Semaphore>,
}

struct TurnActor {
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl Actor for TurnActor {
    type Error = ActorError;
    type Behavior = Stateless;
}

impl Handler<BlockNormalTurn> for TurnActor {
    async fn handle(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        message: BlockNormalTurn,
    ) -> Result<(), ActorError> {
        self.events.lock().await.push("blocked");
        message.entered.add_permits(1);
        let permit = message
            .gate
            .acquire_owned()
            .await
            .map_err(|_| ActorError::new("normal turn gate was closed"))?;
        permit.forget();
        Ok(())
    }
}

impl Handler<Record> for TurnActor {
    async fn handle(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        message: Record,
    ) -> Result<(), ActorError> {
        self.events.lock().await.push(message.value);
        message.processed.add_permits(1);
        Ok(())
    }
}

#[tokio::test]
async fn normal_turn_rechecks_system_lane_at_budget_boundary() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let gate = Arc::new(Semaphore::new(0));
    let entered = Arc::new(Semaphore::new(0));
    let processed = Arc::new(Semaphore::new(0));
    let actor = TurnActor {
        events: events.clone(),
    };
    let handle = spawn_actor(actor, MailboxConfig::bounded(16).with_turn_budget(4));

    handle
        .try_tell_for_test(BlockNormalTurn {
            gate: gate.clone(),
            entered: entered.clone(),
        })
        .unwrap();
    entered.acquire().await.unwrap().forget();
    for value in ["n1", "n2", "n3", "n4", "n5", "n6"] {
        handle
            .try_tell_for_test(Record {
                value,
                processed: processed.clone(),
            })
            .unwrap();
    }
    handle
        .try_tell_system_for_test(Record {
            value: "system",
            processed: processed.clone(),
        })
        .unwrap();
    gate.add_permits(1);
    processed.acquire_many(7).await.unwrap().forget();

    assert_eq!(
        *events.lock().await,
        vec!["blocked", "n1", "n2", "n3", "system", "n4", "n5", "n6"]
    );
}

#[test]
#[should_panic(expected = "actor mailbox turn budget must be nonzero")]
fn mailbox_turn_budget_must_be_nonzero() {
    let _ = MailboxConfig::bounded(1).with_turn_budget(0);
}
