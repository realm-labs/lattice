use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::{Arc, Mutex as SyncMutex},
    task::Poll,
    time::Duration,
};

use tokio::{
    sync::{Mutex, Notify, watch},
    task::JoinHandle,
    time::{Instant, timeout_at},
};

use crate::error::EventBusError;

/// Admission and completion share one lock, so closing cannot miss an admitted operation.
#[derive(Debug, Default)]
pub(crate) struct Activity {
    state: SyncMutex<ActivityState>,
    idle: Notify,
}

#[derive(Debug, Default)]
struct ActivityState {
    closed: bool,
    in_flight: usize,
}

impl Activity {
    pub(crate) fn enter(self: &Arc<Self>) -> Result<ActivityGuard, EventBusError> {
        let mut state = self.state.lock().expect("activity lock poisoned");
        if state.closed {
            return Err(EventBusError::Closed);
        }
        state.in_flight += 1;
        Ok(ActivityGuard(self.clone()))
    }

    pub(crate) fn close(&self) {
        self.state.lock().expect("activity lock poisoned").closed = true;
    }

    fn is_closed(&self) -> bool {
        self.state.lock().expect("activity lock poisoned").closed
    }

    pub(crate) async fn wait_idle(&self) {
        loop {
            let idle = self.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            if self.state.lock().expect("activity lock poisoned").in_flight == 0 {
                return;
            }
            idle.await;
        }
    }
}

pub(crate) struct ActivityGuard(Arc<Activity>);

impl Drop for ActivityGuard {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().expect("activity lock poisoned");
        state.in_flight -= 1;
        if state.in_flight == 0 {
            self.0.idle.notify_waiters();
        }
    }
}

#[derive(Debug)]
pub(crate) struct SubscriptionState {
    pub(crate) activity: Arc<Activity>,
    cancel: watch::Sender<bool>,
    task: SyncMutex<Option<JoinHandle<()>>>,
    draining: Mutex<()>,
}

impl SubscriptionState {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            activity: Arc::default(),
            cancel: watch::channel(false).0,
            task: SyncMutex::new(None),
            draining: Mutex::new(()),
        })
    }

    pub(crate) fn attach(&self, task: JoinHandle<()>) {
        let mut slot = self.task.lock().expect("subscription task lock poisoned");
        assert!(slot.is_none(), "subscription task attached twice");
        *slot = Some(task);
    }

    /// Must be acquired before the new state becomes visible to shutdown.
    pub(crate) fn setup(self: &Arc<Self>) -> SubscriptionSetup {
        SubscriptionSetup {
            state: self.clone(),
            _active: self.activity.enter().expect("new subscription is open"),
            complete: false,
        }
    }

    pub(crate) fn cancellation(&self) -> Cancellation {
        Cancellation {
            receiver: self.cancel.subscribe(),
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.activity.is_closed()
    }

    pub(crate) fn cancel(&self) {
        self.activity.close();
        self.cancel.send_replace(true);
    }

    pub(crate) async fn shutdown(&self, deadline: Duration) -> bool {
        self.shutdown_until(Instant::now() + deadline).await
    }

    pub(crate) async fn shutdown_until(&self, deadline: Instant) -> bool {
        self.cancel();
        timeout_at(deadline, self.wait_finished()).await.is_ok()
    }

    pub(crate) async fn wait_finished(&self) {
        // Serialize join waiters, keeping the handle in its owner even if a waiter is cancelled.
        let _draining = self.draining.lock().await;
        self.activity.wait_idle().await;
        poll_fn(|cx| {
            let mut slot = self.task.lock().expect("subscription task lock poisoned");
            if let Some(task) = slot.as_mut()
                && Pin::new(task).poll(cx).is_pending()
            {
                return Poll::Pending;
            }
            *slot = None;
            Poll::Ready(())
        })
        .await;
    }
}

impl Drop for SubscriptionState {
    fn drop(&mut self) {
        if let Some(task) = self
            .task
            .get_mut()
            .expect("subscription task lock poisoned")
            .take()
        {
            task.abort();
        }
    }
}

pub(crate) struct SubscriptionSetup {
    state: Arc<SubscriptionState>,
    _active: ActivityGuard,
    complete: bool,
}

impl SubscriptionSetup {
    pub(crate) fn complete(mut self) -> Result<(), EventBusError> {
        if self.state.is_cancelled() {
            return Err(EventBusError::Closed);
        }
        self.complete = true;
        Ok(())
    }
}

impl Drop for SubscriptionSetup {
    fn drop(&mut self) {
        if !self.complete {
            self.state.cancel();
        }
    }
}

pub(crate) struct Cancellation {
    receiver: watch::Receiver<bool>,
}

impl Cancellation {
    pub(crate) async fn cancelled(&mut self) {
        loop {
            if *self.receiver.borrow_and_update() {
                return;
            }
            if self.receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

pub(crate) async fn drain(
    activity: &Activity,
    states: Vec<Arc<SubscriptionState>>,
    deadline: Instant,
) -> bool {
    // Stop all admissions before waiting for the first handler.
    for state in &states {
        state.cancel();
    }
    let mut drained = true;
    for state in states {
        drained &= state.shutdown_until(deadline).await;
    }
    drained & timeout_at(deadline, activity.wait_idle()).await.is_ok()
}
