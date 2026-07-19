use lattice_core::failpoint::Failpoint;
use tokio::{sync::watch, time::Instant};

use super::{EndpointError, RemotingEndpoint};

impl RemotingEndpoint {
    pub async fn shutdown(&self) -> Result<(), EndpointError> {
        self.shutdown_tx.send_replace(true);
        lattice_core::failpoint::hit(Failpoint::ShutdownAfterFenceBeforeTaskJoin);
        let tasks = {
            let mut tasks = self.tasks.lock().expect("endpoint task list poisoned");
            std::mem::take(&mut *tasks)
        };
        let deadline = Instant::now() + self.config.shutdown_timeout;
        let mut timed_out = false;
        for mut task in tasks {
            match tokio::time::timeout_at(deadline, &mut task).await {
                Ok(Ok(result)) => result?,
                Ok(Err(error)) if error.is_cancelled() => {}
                Ok(Err(error)) => return Err(EndpointError::Join(error)),
                Err(_) => {
                    timed_out = true;
                    task.abort();
                    let _ = task.await;
                }
            }
        }
        if timed_out {
            Err(EndpointError::ShutdownTimeout)
        } else {
            Ok(())
        }
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
