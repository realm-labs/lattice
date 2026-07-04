mod context;
mod error;
mod handle;
mod mailbox;
mod runtime;
mod traits;

pub use context::ActorContext;
pub use error::{ActorCallError, ActorError, ActorStopError, ActorTellError};
pub use handle::ActorHandle;
pub use mailbox::MailboxConfig;
pub use runtime::spawn_actor;
pub use traits::{Actor, Handler, Message, StopReason};

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use tokio::sync::{Mutex, oneshot};

    use crate::{
        Actor, ActorCallError, ActorContext, ActorError, ActorTellError, Handler, MailboxConfig,
        Message, StopReason, spawn_actor,
    };

    #[derive(Debug)]
    struct Ping(&'static str);

    impl Message for Ping {
        type Reply = String;
    }

    #[derive(Debug)]
    struct Record(&'static str);

    impl Message for Record {
        type Reply = ();
    }

    struct TestActor {
        events: Arc<Mutex<Vec<&'static str>>>,
        start_gate: Option<oneshot::Receiver<()>>,
    }

    #[async_trait]
    impl Actor for TestActor {
        async fn started(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
            if let Some(gate) = self.start_gate.take() {
                gate.await
                    .map_err(|_| ActorError::new("start gate was dropped"))?;
            }
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<Ping> for TestActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            msg: Ping,
        ) -> Result<String, ActorError> {
            self.events.lock().await.push(msg.0);
            Ok(format!("pong:{}", msg.0))
        }
    }

    #[async_trait]
    impl Handler<Record> for TestActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            msg: Record,
        ) -> Result<(), ActorError> {
            self.events.lock().await.push(msg.0);
            Ok(())
        }
    }

    fn assert_handler_bound<A, M>()
    where
        A: Handler<M>,
        M: Message,
    {
    }

    #[test]
    fn handler_compile_time_bounds_are_typed() {
        assert_handler_bound::<TestActor, Ping>();
        assert_handler_bound::<TestActor, Record>();
    }

    #[tokio::test]
    async fn actor_handle_call_and_tell_deliver_typed_messages() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let actor = TestActor {
            events: events.clone(),
            start_gate: None,
        };
        let handle = spawn_actor(actor, MailboxConfig::bounded(8));

        let reply = handle.call(Ping("one")).await.unwrap();
        handle.tell(Record("two")).await.unwrap();

        assert_eq!(reply, "pong:one");
        assert_eq!(*events.lock().await, vec!["one", "two"]);
    }

    #[tokio::test]
    async fn system_mailbox_has_priority_over_normal_mailbox() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let (release, gate) = oneshot::channel();
        let actor = TestActor {
            events: events.clone(),
            start_gate: Some(gate),
        };
        let handle = spawn_actor(actor, MailboxConfig::bounded(8));

        let normal_handle = handle.clone();
        let normal = tokio::spawn(async move { normal_handle.call(Record("normal")).await });
        let system = tokio::spawn(async move { handle.call_system(Record("system")).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        release.send(()).unwrap();

        normal.await.unwrap().unwrap();
        system.await.unwrap().unwrap();

        assert_eq!(*events.lock().await, vec!["system", "normal"]);
    }

    #[tokio::test]
    async fn mailbox_full_returns_explicit_error() {
        let (_release, gate) = oneshot::channel();
        let actor = TestActor {
            events: Arc::new(Mutex::new(Vec::new())),
            start_gate: Some(gate),
        };
        let handle = spawn_actor(actor, MailboxConfig::bounded(1));

        let first_handle = handle.clone();
        let first = tokio::spawn(async move { first_handle.tell(Record("first")).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        let second = handle.tell(Record("second")).await;

        assert!(matches!(second, Err(ActorTellError::MailboxFull)));
        first.abort();
    }

    #[tokio::test]
    async fn stop_uses_system_lane_and_closes_actor() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let actor = TestActor {
            events,
            start_gate: None,
        };
        let handle = spawn_actor(actor, MailboxConfig::bounded(8));

        handle.stop(StopReason::Requested).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        let result = handle.call(Ping("after-stop")).await;
        assert!(matches!(result, Err(ActorCallError::MailboxClosed)));
    }
}
