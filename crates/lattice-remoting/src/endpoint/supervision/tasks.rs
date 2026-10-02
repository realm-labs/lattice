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

#[derive(Default)]
pub(super) struct IoTasks {
    active: AtomicUsize,
    changed: Notify,
    handles: Mutex<Vec<AbortHandle>>,
}

impl IoTasks {
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

struct TaskCompletion(Arc<IoTasks>);

// Field order is deliberate: even a never-polled, cancelled task drops all of its socket
// resources before decrementing the ledger. A guard captured first in an async block would
// allow shutdown to observe zero while the captured I/O future was still being destroyed.
struct TrackedWork<F> {
    future: Pin<Box<F>>,
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

pub(super) struct IoTask<T>(JoinHandle<T>);

impl<T> IoTask<T> {
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
