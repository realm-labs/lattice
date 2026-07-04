use std::future::Future;
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::{Actor, ActorError, ActorHandle, Handler, Message, PassivationReason, StopReason};

pub struct ActorContext<A: Actor> {
    handle: ActorHandle<A>,
    lifecycle_request: Option<StopReason>,
    tasks: Vec<JoinHandle<()>>,
}

impl<A: Actor> ActorContext<A> {
    pub(crate) fn new(handle: ActorHandle<A>) -> Self {
        Self {
            handle,
            lifecycle_request: None,
            tasks: Vec::new(),
        }
    }

    pub fn request_stop(&mut self) {
        self.lifecycle_request = Some(StopReason::Requested);
    }

    pub fn request_passivation(&mut self, reason: PassivationReason) -> Result<(), ActorError> {
        self.lifecycle_request = Some(StopReason::Passivated(reason));
        Ok(())
    }

    pub fn notify_after<M>(&mut self, delay: Duration, msg: M)
    where
        A: Handler<M>,
        M: Message<Reply = ()>,
    {
        let handle = self.handle.clone();
        self.spawn_scoped(async move {
            tokio::time::sleep(delay).await;
            let _ = handle.try_tell_internal(msg);
        });
    }

    pub fn notify_interval<M, F>(&mut self, interval: Duration, mut make_msg: F)
    where
        A: Handler<M>,
        M: Message<Reply = ()>,
        F: FnMut() -> M + Send + 'static,
    {
        let handle = self.handle.clone();
        self.spawn_scoped(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                if handle.try_tell_internal(make_msg()).is_err() {
                    break;
                }
            }
        });
    }

    pub fn spawn_scoped<F>(&mut self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.tasks.push(tokio::spawn(future));
    }

    pub fn cancel_all_tasks(&mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
        }
    }

    pub(crate) fn take_lifecycle_request(&mut self) -> Option<StopReason> {
        self.lifecycle_request.take()
    }
}

impl<A: Actor> Drop for ActorContext<A> {
    fn drop(&mut self) {
        self.cancel_all_tasks();
    }
}
