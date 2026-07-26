use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
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
        let mut tasks = self.tasks.lock().expect("service task supervisor poisoned");
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
        tasks.push(SupervisedTask { abort, monitor });
        Ok(())
    }

    /// Stops every supervised task, aborting the ones that miss the graceful deadline.
    ///
    /// Aborted tasks are still joined before returning, so a completed abort is a
    /// successful shutdown. Only a task that cannot be joined at all is reported as a
    /// timeout, because that is the only case where a supervised task is still live.
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), ServiceError> {
        let tasks =
            std::mem::take(&mut *self.tasks.lock().expect("service task supervisor poisoned"));
        let deadline = Instant::now() + timeout;
        let abort_deadline = deadline + timeout;
        let mut unresolved = 0usize;
        for SupervisedTask { abort, mut monitor } in tasks {
            if tokio::time::timeout_at(deadline, &mut monitor)
                .await
                .is_ok()
            {
                continue;
            }
            abort.abort();
            if tokio::time::timeout_at(abort_deadline, &mut monitor)
                .await
                .is_err()
            {
                unresolved += 1;
            }
        }
        if unresolved > 0 {
            tracing::error!(
                target: "lattice.cluster.lifecycle",
                unresolved,
                "supervised tasks did not stop before the shutdown deadline"
            );
            return Err(ServiceError::ShutdownTimeout);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

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
