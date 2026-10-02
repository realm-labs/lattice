//! Socket work stays on the endpoint's Tokio executor, outside actor turns.
//!
//! Each join waiter aborts its work when dropped. The ledger also survives actor cancellation,
//! so endpoint shutdown can wait until all socket futures have actually released their resources.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use tokio::{
    runtime::Handle,
    sync::Notify,
    task::{AbortHandle, JoinError, JoinHandle},
};

/// Cancellation ledger for actual socket futures, independent of completion delivery.
///
/// A completed task's output may still retain a lease in actor-scoped work. Joining actor
/// termination handles that output; [`Self::wait_empty`] covers destruction of I/O futures.
#[derive(Default)]
pub(super) struct IoTasks {
    /// Futures not yet destroyed, including cancelled tasks awaiting executor cleanup.
    active: AtomicUsize,
    /// Wakes cleanup waiters after a tracked future's destruction.
    changed: Notify,
    /// Abort handles retained for lifetime-owner cleanup; finished handles are reaped on spawn.
    handles: Mutex<Vec<AbortHandle>>,
}

impl IoTasks {
    /// Spawns tracked socket work and returns an abort-on-drop join waiter.
    ///
    /// Accounting is established before spawn, so even a never-polled cancellation decrements
    /// the ledger only after the I/O future and its captured resources have been destroyed.
    pub(super) fn spawn<T: Send + 'static>(
        self: &Arc<Self>,
        executor: &Handle,
        future: impl Future<Output = T> + Send + 'static,
    ) -> IoTask<T> {
        self.active.fetch_add(1, Ordering::AcqRel);
        let guard = TaskCompletion(self.clone());
        let task = executor.spawn(TrackedWork {
            future: Box::pin(future),
            _completion: guard,
        });
        let mut handles = self.handles.lock().expect("socket task ledger poisoned");
        handles.retain(|handle| !handle.is_finished());
        handles.push(task.abort_handle());
        IoTask(task)
    }

    /// Requests cancellation of tracked tasks without waiting for resource destruction.
    pub(super) fn abort_all(&self) {
        for handle in self
            .handles
            .lock()
            .expect("socket task ledger poisoned")
            .iter()
        {
            handle.abort();
        }
    }

    /// Waits until every tracked I/O future has been destroyed.
    ///
    /// The owner must prevent new spawns first. Completed task outputs are outside this count
    /// and are reclaimed by joining the actor's scoped completion work.
    pub(super) async fn wait_empty(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.active.load(Ordering::Acquire) == 0 {
                return;
            }
            changed.await;
        }
    }
}

/// Decrements the ledger after a tracked I/O future has released its captured resources.
struct TaskCompletion(Arc<IoTasks>);

// Field order is deliberate: even a never-polled, cancelled task drops all of its socket
// resources before decrementing the ledger. A guard captured first in an async block would
// allow shutdown to observe zero while the captured I/O future was still being destroyed.
/// Pins work separately and makes its destruction precede ledger completion by field order.
struct TrackedWork<F> {
    /// Socket future and its captured resources; must remain the first field.
    future: Pin<Box<F>>,
    /// Completion guard destroyed only after the future.
    _completion: TaskCompletion,
}

impl<F: Future> Future for TrackedWork<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(cx)
    }
}

impl Drop for TaskCompletion {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}

/// Join waiter whose destruction aborts its socket task instead of detaching it.
pub(super) struct IoTask<T>(JoinHandle<T>);

impl<T> IoTask<T> {
    /// Returns the task output, retaining abort-on-drop behavior while the wait is pending.
    pub(super) async fn join(mut self) -> Result<T, JoinError> {
        (&mut self.0).await
    }
}

impl<T> Drop for IoTask<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests;
