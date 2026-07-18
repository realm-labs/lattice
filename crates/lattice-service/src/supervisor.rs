use std::{future::Future, sync::Mutex, time::Duration};

use tokio::{task::JoinHandle, time::Instant};

use crate::error::ServiceError;

pub struct TaskSupervisor {
    maximum: usize,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl TaskSupervisor {
    pub fn new(maximum: usize) -> Result<Self, ServiceError> {
        if maximum == 0 {
            return Err(ServiceError::TaskCapacity);
        }
        Ok(Self {
            maximum,
            tasks: Mutex::new(Vec::new()),
        })
    }

    pub fn spawn<F>(&self, future: F) -> Result<(), ServiceError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut tasks = self.tasks.lock().expect("service task supervisor poisoned");
        tasks.retain(|task| !task.is_finished());
        if tasks.len() == self.maximum {
            return Err(ServiceError::TaskCapacity);
        }
        tasks.push(tokio::spawn(future));
        Ok(())
    }

    pub async fn shutdown(&self, timeout: Duration) -> Result<(), ServiceError> {
        let tasks =
            std::mem::take(&mut *self.tasks.lock().expect("service task supervisor poisoned"));
        let deadline = Instant::now() + timeout;
        for mut task in tasks {
            let now = Instant::now();
            if now < deadline && tokio::time::timeout_at(deadline, &mut task).await.is_ok() {
                continue;
            }
            task.abort();
            let _ = task.await;
        }
        if Instant::now() > deadline {
            Err(ServiceError::ShutdownTimeout)
        } else {
            Ok(())
        }
    }
}
