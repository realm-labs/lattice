use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::{sync::Mutex, task::AbortHandle};

#[derive(Debug, Clone)]
pub struct ServiceScheduler {
    inner: Arc<ServiceSchedulerInner>,
}

impl ServiceScheduler {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ServiceSchedulerInner {
                stopped: Arc::new(AtomicBool::new(false)),
                tasks: Mutex::new(Vec::new()),
            }),
        }
    }

    pub async fn interval<F, Fut>(&self, every: Duration, mut job: F) -> ServiceTaskHandle
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let cancelled = Arc::new(AtomicBool::new(false));
        let task_cancelled = cancelled.clone();
        let stopped = self.inner.stopped.clone();
        let join = tokio::spawn(async move {
            let mut interval = tokio::time::interval(every);
            loop {
                interval.tick().await;
                if stopped.load(Ordering::SeqCst) || task_cancelled.load(Ordering::SeqCst) {
                    break;
                }
                job().await;
            }
        });
        self.inner.tasks.lock().await.push(join.abort_handle());
        ServiceTaskHandle { cancelled }
    }

    pub async fn after<Fut>(&self, delay: Duration, job: Fut) -> ServiceTaskHandle
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let cancelled = Arc::new(AtomicBool::new(false));
        let task_cancelled = cancelled.clone();
        let stopped = self.inner.stopped.clone();
        let join = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            if !stopped.load(Ordering::SeqCst) && !task_cancelled.load(Ordering::SeqCst) {
                job.await;
            }
        });
        self.inner.tasks.lock().await.push(join.abort_handle());
        ServiceTaskHandle { cancelled }
    }

    pub async fn shutdown(&self) {
        self.inner.stopped.store(true, Ordering::SeqCst);
        for task in self.inner.tasks.lock().await.drain(..) {
            task.abort();
        }
    }
}

impl Default for ServiceScheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct ServiceSchedulerInner {
    stopped: Arc<AtomicBool>,
    tasks: Mutex<Vec<AbortHandle>>,
}

#[derive(Debug, Clone)]
pub struct ServiceTaskHandle {
    cancelled: Arc<AtomicBool>,
}

impl ServiceTaskHandle {
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }
}
