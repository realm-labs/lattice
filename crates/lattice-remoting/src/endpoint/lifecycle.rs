use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
};

use crate::failpoints;
use tokio::{sync::watch, time::Instant};

use super::{EndpointError, RemotingEndpoint};

impl RemotingEndpoint {
    /// Stops endpoint admission and joins owned tasks. Cancelling this future or returning a task
    /// error leaves unfinished tasks owned by the endpoint so a later call can continue cleanup.
    pub async fn shutdown(&self) -> Result<(), EndpointError> {
        self.shutdown_tx.send_replace(true);
        lattice_failpoint::hit(failpoints::SHUTDOWN_AFTER_FENCE_BEFORE_TASK_JOIN);
        let deadline = Instant::now() + self.config.shutdown_timeout;
        let _shutdown = tokio::time::timeout_at(deadline, self.shutdown_lock.lock())
            .await
            .map_err(|_| EndpointError::ShutdownTimeout)?;
        let mut timed_out = false;
        loop {
            let completed = if timed_out {
                self.join_next_shutdown_task().await
            } else {
                match tokio::time::timeout_at(deadline, self.join_next_shutdown_task()).await {
                    Ok(completed) => completed,
                    Err(_) => {
                        timed_out = true;
                        for task in self
                            .tasks
                            .lock()
                            .expect("endpoint task list poisoned")
                            .iter()
                        {
                            task.abort();
                        }
                        continue;
                    }
                }
            };
            match completed {
                Some(Ok(result)) => result?,
                Some(Err(error)) if error.is_cancelled() => {}
                Some(Err(error)) => return Err(EndpointError::Join(error)),
                None => break,
            }
        }
        if timed_out {
            Err(EndpointError::ShutdownTimeout)
        } else {
            Ok(())
        }
    }

    async fn join_next_shutdown_task(
        &self,
    ) -> Option<Result<Result<(), EndpointError>, tokio::task::JoinError>> {
        // Poll in place: cancelling shutdown or returning an earlier task error must not detach
        // the remaining tasks. The async shutdown lock ensures only one join waiter at a time.
        poll_fn(|cx| {
            let mut tasks = self.tasks.lock().expect("endpoint task list poisoned");
            let Some(task) = tasks.first_mut() else {
                return Poll::Ready(None);
            };
            let Poll::Ready(result) = Pin::new(task).poll(cx) else {
                return Poll::Pending;
            };
            drop(tasks.remove(0));
            Poll::Ready(Some(result))
        })
        .await
    }

    pub(super) fn ensure_running(&self) -> Result<(), EndpointError> {
        if *self.shutdown_tx.borrow() {
            Err(EndpointError::ShuttingDown)
        } else {
            Ok(())
        }
    }
}

pub(super) async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow() || shutdown.changed().await.is_err() {
            return;
        }
    }
}
