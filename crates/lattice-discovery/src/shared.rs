//! Process-shared, latest-value discovery subscriptions.
//!
//! Construct one wrapper per provider/scope and share its clones with consumers.
//! This is not a global cache: unrelated clusters or credential sets must not be
//! merged just because they use the same Coordinator scope.

use std::{
    pin::Pin,
    sync::{Arc, Mutex},
};

use futures_util::{Stream, StreamExt};
use lattice_model::{cluster::CoordinatorScope, run::RunEpoch};
use tokio::{
    runtime::Handle,
    sync::watch,
    task::{JoinHandle, yield_now},
};

use crate::provider::{
    CoordinatorDirectorySnapshot, CoordinatorDiscovery, DiscoveryError, TerminalLifecycleFuture,
    validate_snapshot,
};

/// Fans out one provider stream and retains its latest validated snapshot.
///
/// Construction does not spawn or require a Tokio runtime. The first polled
/// subscription starts the worker on the current runtime. Later subscribers
/// immediately receive the cached snapshot, even after an upstream error or a
/// finite provider's completion. Slow consumers may skip intermediate updates:
/// snapshots replace the directory; they are not an event log.
///
/// Upstream errors do not erase the last valid endpoints. They also do not renew
/// authority: all cached endpoints still require bootstrap/session validation.
/// The worker stays alive while this wrapper (or a subscription) is retained,
/// and is aborted when the last owner is dropped. Providers own their reconnect
/// and watch-reconciliation policy; this wrapper does not restart ended streams.
#[derive(Clone)]
pub struct SharedDiscovery {
    inner: Arc<SharedState>,
}

struct SharedState {
    provider: Arc<dyn CoordinatorDiscovery>,
    worker: Mutex<Option<JoinHandle<()>>>,
    cache: watch::Receiver<Cache>,
    initial_sender: Mutex<Option<watch::Sender<Cache>>>,
}

#[derive(Clone, Default)]
struct Cache {
    snapshot: Option<CoordinatorDirectorySnapshot>,
    error: Option<(u64, DiscoveryError)>,
}

impl SharedDiscovery {
    pub fn new(provider: Arc<dyn CoordinatorDiscovery>) -> Self {
        let (tx, rx) = watch::channel(Cache::default());
        Self {
            inner: Arc::new(SharedState {
                provider,
                worker: Mutex::new(None),
                cache: rx,
                initial_sender: Mutex::new(Some(tx)),
            }),
        }
    }

    fn subscribe(&self) -> watch::Receiver<Cache> {
        let runtime = Handle::current();
        let mut worker = self
            .inner
            .worker
            .lock()
            .expect("discovery worker lock poisoned");
        if worker.is_none() {
            let sender = self
                .inner
                .initial_sender
                .lock()
                .expect("discovery sender lock poisoned")
                .take()
                .expect("discovery worker starts once");
            *worker = Some(runtime.spawn(publish(self.inner.provider.clone(), sender)));
        }
        self.inner.cache.clone()
    }
}

impl Drop for SharedState {
    fn drop(&mut self) {
        if let Some(worker) = self
            .worker
            .get_mut()
            .expect("discovery worker lock poisoned")
            .take()
        {
            worker.abort();
        }
    }
}

impl CoordinatorDiscovery for SharedDiscovery {
    fn terminal_lifecycle(&self, epoch: RunEpoch) -> TerminalLifecycleFuture<'_> {
        self.inner.provider.terminal_lifecycle(epoch)
    }
    fn scope(&self) -> &CoordinatorScope {
        self.inner.provider.scope()
    }

    fn snapshots(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<CoordinatorDirectorySnapshot, DiscoveryError>> + Send + '_>>
    {
        let shared = self.clone();
        Box::pin(async_stream::stream! {
            let mut cache = shared.subscribe();
            let mut generation = 0;
            let mut error_sequence = 0;
            loop {
                let current = cache.borrow_and_update().clone();
                if let Some(snapshot) = current.snapshot
                    && snapshot.generation > generation
                {
                    generation = snapshot.generation;
                    yield Ok(snapshot);
                }
                if let Some((sequence, error)) = current.error
                    && sequence != error_sequence
                {
                    error_sequence = sequence;
                    yield Err(error);
                }
                if cache.changed().await.is_err() {
                    break;
                }
            }
        })
    }
}

async fn publish(provider: Arc<dyn CoordinatorDiscovery>, sender: watch::Sender<Cache>) {
    let mut source = provider.snapshots();
    let mut cached = Cache::default();
    let mut generation = 0;
    let mut error_sequence = 0_u64;
    while let Some(update) = source.next().await {
        let update = update.and_then(|snapshot| {
            validate_snapshot(&snapshot)?;
            if snapshot.scope != *provider.scope() || snapshot.generation <= generation {
                return Err(DiscoveryError::InvalidSnapshot {
                    message: "snapshot scope changed or generation did not advance".into(),
                });
            }
            Ok(snapshot)
        });
        match update {
            Ok(snapshot) => {
                generation = snapshot.generation;
                cached.snapshot = Some(snapshot);
                cached.error = None;
            }
            Err(error) => {
                error_sequence = error_sequence.wrapping_add(1);
                cached.error = Some((error_sequence, error));
            }
        }
        sender.send_replace(cached.clone());
        // A custom provider may return an always-ready stream. Do not let it
        // monopolize a runtime worker or starve subscribers/shutdown.
        yield_now().await;
    }
}

#[cfg(test)]
mod tests;
