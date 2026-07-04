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

    use async_trait::async_trait;
    use tokio::sync::{Mutex, Semaphore};

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
    struct Record {
        value: &'static str,
        processed: Option<Arc<Semaphore>>,
    }

    impl Record {
        fn new(value: &'static str) -> Self {
            Self {
                value,
                processed: None,
            }
        }

        fn with_processed_signal(value: &'static str, processed: Arc<Semaphore>) -> Self {
            Self {
                value,
                processed: Some(processed),
            }
        }
    }

    impl Message for Record {
        type Reply = ();
    }

    struct TestActor {
        events: Arc<Mutex<Vec<&'static str>>>,
        start_gate: Option<Arc<Semaphore>>,
        stopped: Option<Arc<Semaphore>>,
    }

    #[async_trait]
    impl Actor for TestActor {
        async fn started(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
            if let Some(gate) = self.start_gate.take() {
                let permit = gate
                    .acquire()
                    .await
                    .map_err(|_| ActorError::new("start gate was closed"))?;
                permit.forget();
            }
            Ok(())
        }

        async fn stopping(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _reason: StopReason,
        ) -> Result<(), crate::ActorStopError> {
            if let Some(stopped) = self.stopped.take() {
                stopped.add_permits(1);
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
            self.events.lock().await.push(msg.value);
            if let Some(processed) = msg.processed {
                processed.add_permits(1);
            }
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
            stopped: None,
        };
        let handle = spawn_actor(actor, MailboxConfig::bounded(8));

        let reply = handle.call(Ping("one")).await.unwrap();
        handle.tell(Record::new("two")).await.unwrap();

        assert_eq!(reply, "pong:one");
        assert_eq!(*events.lock().await, vec!["one", "two"]);
    }

    #[tokio::test]
    async fn system_mailbox_has_priority_over_normal_mailbox() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let start_gate = Arc::new(Semaphore::new(0));
        let processed = Arc::new(Semaphore::new(0));
        let actor = TestActor {
            events: events.clone(),
            start_gate: Some(start_gate.clone()),
            stopped: None,
        };
        let handle = spawn_actor(actor, MailboxConfig::bounded(8));

        handle
            .try_tell_for_test(Record::with_processed_signal("normal", processed.clone()))
            .unwrap();
        handle
            .try_tell_system_for_test(Record::with_processed_signal("system", processed.clone()))
            .unwrap();
        start_gate.add_permits(1);
        processed.acquire_many(2).await.unwrap().forget();

        assert_eq!(*events.lock().await, vec!["system", "normal"]);
    }

    #[tokio::test]
    async fn mailbox_full_returns_explicit_error() {
        let start_gate = Arc::new(Semaphore::new(0));
        let actor = TestActor {
            events: Arc::new(Mutex::new(Vec::new())),
            start_gate: Some(start_gate.clone()),
            stopped: None,
        };
        let handle = spawn_actor(actor, MailboxConfig::bounded(1));

        handle.try_tell_for_test(Record::new("first")).unwrap();
        let second = handle.try_tell_for_test(Record::new("second"));

        assert!(matches!(second, Err(ActorTellError::MailboxFull)));
        start_gate.add_permits(1);
    }

    #[tokio::test]
    async fn stop_uses_system_lane_and_closes_actor() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let stopped = Arc::new(Semaphore::new(0));
        let actor = TestActor {
            events,
            start_gate: None,
            stopped: Some(stopped.clone()),
        };
        let handle = spawn_actor(actor, MailboxConfig::bounded(8));

        handle.stop(StopReason::Requested).await.unwrap();
        stopped.acquire().await.unwrap().forget();

        let result = handle.call(Ping("after-stop")).await;
        assert!(matches!(result, Err(ActorCallError::MailboxClosed)));
    }
}
