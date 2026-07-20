use std::{
    future::Future,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc as std_mpsc,
    },
    thread::{Builder as ThreadBuilder, JoinHandle},
};

use tokio::{
    runtime::{Builder as RuntimeBuilder, Handle},
    sync::oneshot,
};

use crate::error::ActorSpawnError;

#[derive(Debug, Clone, Copy)]
pub(super) enum WorkerPoolKind {
    Keyed,
    Dedicated { actor_type: &'static str },
}

impl WorkerPoolKind {
    fn thread_name(self, worker_index: usize) -> String {
        match self {
            Self::Keyed => format!("lattice-keyed-worker-{worker_index}"),
            Self::Dedicated { actor_type } => {
                format!("lattice-dedicated-worker-{worker_index}-{actor_type}")
            }
        }
    }
}

pub(super) struct ActorWorkerPool {
    workers: Vec<ActorWorker>,
    next_worker: AtomicU64,
}

impl ActorWorkerPool {
    pub(super) fn start(
        kind: WorkerPoolKind,
        worker_count: usize,
    ) -> Result<Self, ActorSpawnError> {
        let mut workers = Vec::with_capacity(worker_count);
        for worker_index in 0..worker_count {
            workers.push(ActorWorker::start(kind, worker_index)?);
        }
        Ok(Self {
            workers,
            next_worker: AtomicU64::new(0),
        })
    }

    pub(super) fn spawn<F>(&self, worker_index: usize, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.workers[worker_index].handle.spawn(future);
    }

    pub(super) fn next_worker_index(&self) -> usize {
        (self.next_worker.fetch_add(1, Ordering::Relaxed) % self.workers.len() as u64) as usize
    }
}

impl Drop for ActorWorkerPool {
    fn drop(&mut self) {
        for worker in &mut self.workers {
            if let Some(shutdown_tx) = worker.shutdown_tx.take() {
                let _ = shutdown_tx.send(());
            }
        }
        for worker in &mut self.workers {
            if let Some(join_handle) = worker.join_handle.take()
                && join_handle.thread().id() != std::thread::current().id()
            {
                let _ = join_handle.join();
            }
        }
    }
}

struct ActorWorker {
    handle: Handle,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: Option<JoinHandle<()>>,
}

impl ActorWorker {
    fn start(kind: WorkerPoolKind, worker_index: usize) -> Result<Self, ActorSpawnError> {
        let (handle_tx, handle_rx) = std_mpsc::sync_channel(1);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let join_handle = ThreadBuilder::new()
            .name(kind.thread_name(worker_index))
            .spawn(move || {
                let runtime = RuntimeBuilder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("actor worker runtime should build");
                let handle = runtime.handle().clone();
                let _ = handle_tx.send(handle);
                runtime.block_on(async {
                    let _ = shutdown_rx.await;
                });
            })
            .map_err(|_| ActorSpawnError::ExecutorStartFailed {
                reason: "failed to spawn actor worker thread",
            })?;
        let handle = handle_rx
            .recv()
            .map_err(|_| ActorSpawnError::ExecutorStartFailed {
                reason: "actor worker runtime stopped before publishing its handle",
            })?;

        Ok(Self {
            handle,
            shutdown_tx: Some(shutdown_tx),
            join_handle: Some(join_handle),
        })
    }
}
