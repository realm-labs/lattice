use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::Poll,
    time::Duration,
};

use tokio::{
    task::{AbortHandle, JoinHandle},
    time::Instant,
};

use crate::error::ServiceError;

/// Invoked once for every supervised task that terminates abnormally.
pub(crate) type TaskFailureHandler = Arc<dyn Fn() + Send + Sync>;

struct SupervisedTask {
    abort: AbortHandle,
    monitor: JoinHandle<()>,
}

pub struct TaskSupervisor {
    maximum: usize,
    tasks: Mutex<Vec<SupervisedTask>>,
    shutdown_lock: tokio::sync::Mutex<()>,
    stopping: AtomicBool,
    failed: Arc<AtomicU64>,
    on_failure: Mutex<Option<TaskFailureHandler>>,
}

impl TaskSupervisor {
    pub fn new(maximum: usize) -> Result<Self, ServiceError> {
        if maximum == 0 {
            return Err(ServiceError::TaskCapacity);
        }
        Ok(Self {
            maximum,
            tasks: Mutex::new(Vec::new()),
            shutdown_lock: tokio::sync::Mutex::new(()),
            stopping: AtomicBool::new(false),
            failed: Arc::new(AtomicU64::new(0)),
            on_failure: Mutex::new(None),
        })
    }

    /// Number of supervised tasks that panicked instead of returning.
    pub fn failed_tasks(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }

    /// Number of supervised tasks that have not completed yet.
    pub fn active_tasks(&self) -> usize {
        let mut tasks = self.tasks.lock().expect("service task supervisor poisoned");
        tasks.retain(|task| !task.monitor.is_finished());
        tasks.len()
    }

    pub(crate) fn on_task_failure(&self, handler: TaskFailureHandler) {
        *self
            .on_failure
            .lock()
            .expect("service task supervisor poisoned") = Some(handler);
    }

    pub fn spawn<F>(&self, future: F) -> Result<(), ServiceError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_abortable(future).map(|_| ())
    }

    pub(crate) fn spawn_abortable<F>(&self, future: F) -> Result<AbortHandle, ServiceError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut tasks = self.tasks.lock().expect("service task supervisor poisoned");
        if self.stopping.load(Ordering::Acquire) {
            return Err(ServiceError::ShuttingDown);
        }
        tasks.retain(|task| !task.monitor.is_finished());
        if tasks.len() == self.maximum {
            return Err(ServiceError::TaskCapacity);
        }
        let failed = self.failed.clone();
        let handler = self
            .on_failure
            .lock()
            .expect("service task supervisor poisoned")
            .clone();
        let task = tokio::spawn(future);
        let abort = task.abort_handle();
        let monitor = tokio::spawn(async move {
            if let Err(error) = task.await
                && !error.is_cancelled()
            {
                failed.fetch_add(1, Ordering::Relaxed);
                tracing::error!(
                    target: "lattice.cluster.lifecycle",
                    %error,
                    "supervised task terminated abnormally"
                );
                if let Some(handler) = handler {
                    handler();
                }
            }
        });
        tasks.push(SupervisedTask {
            abort: abort.clone(),
            monitor,
        });
        Ok(abort)
    }

    /// Stops every supervised task, aborting the ones that miss the graceful deadline.
    ///
    /// Aborted tasks are still joined before returning, so a completed abort is a
    /// successful shutdown. Only a task that cannot be joined at all is reported as a
    /// timeout, because that is the only case where a supervised task is still live.
    /// Cancellation retains every unjoined task in the supervisor for a later retry.
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), ServiceError> {
        let deadline = Instant::now() + timeout;
        let abort_deadline = deadline + timeout;
        {
            // Serialize admission closure with spawn's final capacity check and insertion.
            let _tasks = self.tasks.lock().expect("service task supervisor poisoned");
            self.stopping.store(true, Ordering::Release);
        }
        let _shutdown = tokio::time::timeout_at(deadline, self.shutdown_lock.lock())
            .await
            .map_err(|_| ServiceError::ShutdownTimeout)?;
        if tokio::time::timeout_at(deadline, self.join_tasks())
            .await
            .is_ok()
        {
            return Ok(());
        }
        {
            let tasks = self.tasks.lock().expect("service task supervisor poisoned");
            for task in tasks.iter() {
                task.abort.abort();
            }
        }
        if tokio::time::timeout_at(abort_deadline, self.join_tasks())
            .await
            .is_err()
        {
            let unresolved = self.active_tasks();
            tracing::error!(
                target: "lattice.cluster.lifecycle",
                unresolved,
                "supervised tasks did not stop before the shutdown deadline"
            );
            return Err(ServiceError::ShutdownTimeout);
        }
        Ok(())
    }

    async fn join_tasks(&self) {
        poll_fn(|context| {
            let mut tasks = self.tasks.lock().expect("service task supervisor poisoned");
            while let Some(task) = tasks.first_mut() {
                if Pin::new(&mut task.monitor).poll(context).is_pending() {
                    return Poll::Pending;
                }
                tasks.remove(0);
            }
            Poll::Ready(())
        })
        .await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn cancelled_shutdown_keeps_task_ownership_for_retry() {
        let supervisor = TaskSupervisor::new(4).unwrap();
        let (finish, finished) = tokio::sync::oneshot::channel();
        supervisor
            .spawn(async move {
                let _ = finished.await;
            })
            .unwrap();
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                supervisor.shutdown(Duration::from_secs(2)),
            )
            .await
            .is_err()
        );
        assert_eq!(
            supervisor.active_tasks(),
            1,
            "cancelled shutdown lost a live task"
        );
        finish.send(()).unwrap();
        supervisor.shutdown(Duration::from_secs(1)).await.unwrap();
        assert_eq!(supervisor.active_tasks(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn waiting_for_concurrent_shutdown_consumes_the_same_budget() {
        let supervisor = TaskSupervisor::new(4).unwrap();
        let _gate = supervisor.shutdown_lock.lock().await;
        let deadline = Instant::now() + Duration::from_millis(10);
        assert!(matches!(
            supervisor.shutdown(Duration::from_millis(10)).await,
            Err(ServiceError::ShutdownTimeout)
        ));
        assert_eq!(Instant::now(), deadline);
        assert!(matches!(
            supervisor.spawn(async {}),
            Err(ServiceError::ShuttingDown)
        ));
    }

    #[tokio::test]
    async fn completed_shutdown_rejects_new_tasks() {
        let supervisor = TaskSupervisor::new(4).unwrap();
        supervisor.shutdown(Duration::from_secs(1)).await.unwrap();
        assert!(matches!(
            supervisor.spawn(async {}),
            Err(ServiceError::ShuttingDown)
        ));
    }

    #[tokio::test]
    async fn shutdown_reports_success_after_aborting_a_stuck_task() {
        let supervisor = TaskSupervisor::new(4).unwrap();
        supervisor.spawn(std::future::pending()).unwrap();

        supervisor
            .shutdown(Duration::from_millis(50))
            .await
            .unwrap();

        assert_eq!(supervisor.failed_tasks(), 0);
    }

    #[tokio::test]
    async fn panicking_task_is_counted_and_reported() {
        let supervisor = TaskSupervisor::new(4).unwrap();
        let observed = Arc::new(AtomicUsize::new(0));
        let handler = observed.clone();
        supervisor.on_task_failure(Arc::new(move || {
            handler.fetch_add(1, Ordering::SeqCst);
        }));

        supervisor
            .spawn(async { panic!("supervised task under test") })
            .unwrap();
        supervisor
            .shutdown(Duration::from_millis(200))
            .await
            .unwrap();

        assert_eq!(supervisor.failed_tasks(), 1);
        assert_eq!(observed.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn spawn_reuses_capacity_released_by_finished_tasks() {
        let supervisor = TaskSupervisor::new(1).unwrap();
        supervisor.spawn(async {}).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while supervisor.spawn(async {}).is_err() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        supervisor
            .shutdown(Duration::from_millis(200))
            .await
            .unwrap();
    }
}
