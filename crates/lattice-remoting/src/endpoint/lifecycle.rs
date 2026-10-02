//! Persistent shutdown fencing and cancellation-safe joining of endpoint-owned tasks.
//!
//! The JoinSet remains in the endpoint throughout shutdown; a cancelled wait cannot detach its
//! tasks. One absolute deadline covers lock acquisition and normal joins. After that deadline,
//! owners are cancelled and socket ledgers are joined to finish releasing actual I/O resources.

use std::future::{Future, poll_fn};

use crate::failpoints;
use tokio::{
    sync::watch,
    task::JoinError,
    time::{Instant, sleep_until},
};

use super::{EndpointError, RemotingEndpoint};

impl RemotingEndpoint {
    /// Stops endpoint admission and joins owned tasks. Cancelling this future or returning a task
    /// error leaves unfinished tasks owned by the endpoint so a later call can continue cleanup.
    /// The shutdown fence remains set; the endpoint cannot be restarted.
    ///
    /// On deadline expiry, remaining owners are cancelled and their I/O ledgers are drained.
    /// Resource destruction can therefore extend the call beyond the configured timeout.
    ///
    /// # Errors
    ///
    /// Returns [`EndpointError::ShutdownTimeout`] on deadline expiry, or an owned task's error
    /// if joining fails. A later call may continue cleanup after an early error or cancellation.
    pub async fn shutdown(&self) -> Result<(), EndpointError> {
        self.shutdown_tx.send_replace(true);
        lattice_failpoint::hit(failpoints::SHUTDOWN_AFTER_FENCE_BEFORE_TASK_JOIN);
        let deadline = Instant::now() + self.config.shutdown_timeout;
        let _shutdown = tokio::time::timeout_at(deadline, self.shutdown_lock.lock())
            .await
            .map_err(|_| EndpointError::ShutdownTimeout)?;
        let mut timed_out = false;
        // Lifetime owners are cancelled at the deadline. Keep their socket ledgers reachable
        // until cancellation has actually dropped every I/O future and connection permit.
        let supervisors: Vec<_> = self
            .supervisors
            .lock()
            .expect("association supervisors poisoned")
            .values()
            .cloned()
            .collect();
        loop {
            let completed = if timed_out {
                self.join_next_shutdown_task().await
            } else {
                match tokio::time::timeout_at(deadline, self.join_next_shutdown_task()).await {
                    Ok(completed) => completed,
                    Err(_) => {
                        timed_out = true;
                        self.tasks
                            .lock()
                            .expect("endpoint task list poisoned")
                            .abort_all();
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
            for supervisor in supervisors {
                supervisor.abort_and_join_io().await;
            }
            Err(EndpointError::ShutdownTimeout)
        } else {
            Ok(())
        }
    }

    async fn join_next_shutdown_task(
        &self,
    ) -> Option<Result<Result<(), EndpointError>, JoinError>> {
        // Keep the JoinSet owned by the endpoint so cancelling shutdown or returning a task
        // error leaves the remaining tasks available for a retry. Only hold the synchronous
        // lock during each poll; the async shutdown lock ensures one join waiter at a time.
        poll_fn(|cx| {
            self.tasks
                .lock()
                .expect("endpoint task list poisoned")
                .poll_join_next(cx)
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

/// Waits for a cancellable inbound setup step within the connection's shared deadline.
/// `None` means shutdown; an elapsed deadline takes priority over the operation.
/// The losing operation is dropped, so callers must not place work that requires
/// explicit asynchronous cleanup (such as an attached lane) inside this helper.
pub(super) async fn during_setup<T, E>(
    shutdown: &mut watch::Receiver<bool>,
    deadline: Instant,
    operation: impl Future<Output = Result<T, E>>,
) -> Result<Option<T>, EndpointError>
where
    E: Into<EndpointError>,
{
    tokio::select! {
        biased;
        () = wait_for_shutdown(shutdown) => Ok(None),
        () = sleep_until(deadline) => Err(EndpointError::InboundSetupTimeout),
        result = operation => result.map(Some).map_err(Into::into),
    }
}

pub(super) async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow() || shutdown.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{future::pending, time::Duration};

    use tokio::{
        sync::{oneshot, watch},
        time::Instant,
    };

    use super::during_setup;
    use crate::{endpoint::EndpointError, wire::WireError};

    #[tokio::test]
    async fn setup_preserves_success_and_converts_operation_errors() {
        let (_sender, mut shutdown) = watch::channel(false);
        let deadline = Instant::now() + Duration::from_secs(60);
        assert_eq!(
            during_setup(&mut shutdown, deadline, async { Ok::<_, WireError>(42) })
                .await
                .unwrap(),
            Some(42)
        );
        assert!(matches!(
            during_setup(&mut shutdown, deadline, async {
                Err::<(), _>(WireError::Tls("server handshake failed"))
            })
            .await,
            Err(EndpointError::Wire(WireError::Tls(
                "server handshake failed"
            )))
        ));
    }

    #[tokio::test]
    async fn setup_shutdown_wins_over_deadline_and_ready_operation() {
        let (_sender, mut shutdown) = watch::channel(true);
        let result = during_setup(
            &mut shutdown,
            Instant::now() - Duration::from_secs(1),
            async { Err::<(), _>(EndpointError::WrongDialDirection) },
        )
        .await;
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn setup_elapsed_deadline_wins_over_ready_operation() {
        let (_sender, mut shutdown) = watch::channel(false);
        assert!(matches!(
            during_setup(
                &mut shutdown,
                Instant::now() - Duration::from_secs(1),
                async { Ok::<_, EndpointError>(42) },
            )
            .await,
            Err(EndpointError::InboundSetupTimeout)
        ));
    }

    #[tokio::test]
    async fn setup_closed_watch_is_shutdown() {
        let (sender, mut shutdown) = watch::channel(false);
        drop(sender);
        assert!(
            during_setup(
                &mut shutdown,
                Instant::now() + Duration::from_secs(60),
                pending::<Result<(), EndpointError>>(),
            )
            .await
            .unwrap()
            .is_none()
        );
    }

    #[tokio::test]
    async fn setup_shutdown_cancels_and_drops_a_pending_operation() {
        let (sender, mut shutdown) = watch::channel(false);
        let (started, waiting) = oneshot::channel();
        let (alive, dropped) = oneshot::channel::<()>();
        let operation = async move {
            let _alive = alive;
            started.send(()).unwrap();
            pending::<Result<(), EndpointError>>().await
        };
        let (result, ()) = tokio::join!(
            during_setup(
                &mut shutdown,
                Instant::now() + Duration::from_secs(60),
                operation,
            ),
            async {
                waiting.await.unwrap();
                sender.send_replace(true);
            },
        );
        assert!(result.unwrap().is_none());
        assert!(dropped.await.is_err());
    }
}
