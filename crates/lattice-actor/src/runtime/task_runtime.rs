use std::{
    fmt,
    future::Future,
    sync::{Mutex, mpsc as std_mpsc},
    thread::{Builder as ThreadBuilder, JoinHandle},
};

use tokio::{
    runtime::{Builder as RuntimeBuilder, Handle},
    sync::oneshot,
};

use crate::error::ActorSpawnError;

/// Owner of the dedicated Tokio executor used by one Actor runtime.
///
/// The runtime starts lazily on the first task-per-Actor activation. Actor schedulers retain only a
/// weak reference, so running Actors cannot accidentally keep their own executor alive after the
/// external [`super::ActorRuntime`] owner is dropped.
pub(super) struct ActorTaskRuntime {
    worker_count: usize,
    running: Mutex<Option<RunningTaskRuntime>>,
}

impl fmt::Debug for ActorTaskRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorTaskRuntime")
            .field("worker_count", &self.worker_count)
            .finish_non_exhaustive()
    }
}

impl ActorTaskRuntime {
    pub(super) fn new(worker_count: usize) -> Self {
        Self {
            worker_count,
            running: Mutex::new(None),
        }
    }

    pub(super) fn spawn<F>(&self, future: F) -> Result<(), ActorSpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut running = self
            .running
            .lock()
            .expect("actor task runtime mutex poisoned");
        if running.is_none() {
            *running = Some(RunningTaskRuntime::start(self.worker_count)?);
        }
        running
            .as_ref()
            .expect("actor task runtime was initialized")
            .handle
            .spawn(future);
        Ok(())
    }
}

struct RunningTaskRuntime {
    handle: Handle,
    shutdown_tx: Option<oneshot::Sender<()>>,
    owner_thread: Option<JoinHandle<()>>,
}

impl RunningTaskRuntime {
    fn start(worker_count: usize) -> Result<Self, ActorSpawnError> {
        if worker_count == 0 {
            return Err(ActorSpawnError::InvalidExecutionPolicy {
                reason: "TaskPerActor worker_count must be greater than zero",
            });
        }

        let (handle_tx, handle_rx) = std_mpsc::sync_channel(1);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let owner_thread = ThreadBuilder::new()
            .name("lattice-actor-runtime-owner".to_owned())
            .spawn(move || {
                let runtime = match RuntimeBuilder::new_multi_thread()
                    .worker_threads(worker_count)
                    .thread_name("lattice-actor-runtime")
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(_) => {
                        let _ = handle_tx.send(None);
                        return;
                    }
                };
                let handle = runtime.handle().clone();
                if handle_tx.send(Some(handle)).is_err() {
                    runtime.shutdown_background();
                    return;
                }

                runtime.block_on(async {
                    let _ = shutdown_rx.await;
                });
                // The runtime is destroyed on its owner thread. Background shutdown keeps this
                // safe even when an ActorRuntime owner is dropped from an asynchronous context.
                runtime.shutdown_background();
            })
            .map_err(|_| ActorSpawnError::ExecutorStartFailed {
                reason: "failed to spawn task-per-Actor runtime owner thread",
            })?;

        let handle = match handle_rx.recv() {
            Ok(Some(handle)) => handle,
            Ok(None) => {
                let _ = owner_thread.join();
                return Err(ActorSpawnError::ExecutorStartFailed {
                    reason: "failed to build task-per-Actor Tokio runtime",
                });
            }
            Err(_) => {
                let _ = owner_thread.join();
                return Err(ActorSpawnError::ExecutorStartFailed {
                    reason: "task-per-Actor runtime stopped before publishing its handle",
                });
            }
        };

        Ok(Self {
            handle,
            shutdown_tx: Some(shutdown_tx),
            owner_thread: Some(owner_thread),
        })
    }
}

impl Drop for RunningTaskRuntime {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(owner_thread) = self.owner_thread.take()
            && owner_thread.thread().id() != std::thread::current().id()
        {
            let _ = owner_thread.join();
        }
    }
}
