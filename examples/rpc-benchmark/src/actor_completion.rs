use lattice_actor::context::HandlerContext;
use std::{
    error::Error,
    sync::atomic::{AtomicUsize, Ordering},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bytes::Bytes;
use lattice_actor::{
    error::{ActorError, ActorTellError},
    mailbox::MailboxConfig,
    observation::{ActorMetadata, ActorObserver, ActorObserverHandle},
    registry::{ActorRegistry, ActorRegistryConfig},
    traits::{Actor, Handler, MessageMetadata, MessageOutcome},
};
use lattice_core::{actor_kind, id::ActorId};
use tokio::sync::Notify;
use tokio::sync::mpsc;

use crate::metrics::WorkloadReport;

#[derive(Debug, Clone)]
pub struct ActorCompletionReport {
    pub workload: WorkloadReport,
    pub queue_times: Vec<Duration>,
    pub processing_times: Vec<Duration>,
    pub mailbox_full_retries: usize,
    pub maximum_queue_depth: usize,
}

impl ActorCompletionReport {
    pub fn queue_percentile(&self, percentile: f64) -> Duration {
        crate::metrics::percentile_duration(&self.queue_times, percentile)
    }

    pub fn processing_percentile(&self, percentile: f64) -> Duration {
        crate::metrics::percentile_duration(&self.processing_times, percentile)
    }
}

#[derive(Debug, Clone)]
pub struct RawActorCompletionReport {
    pub requests: usize,
    pub elapsed: Duration,
    pub mailbox_full_retries: usize,
}

impl RawActorCompletionReport {
    pub fn throughput_per_second(&self) -> f64 {
        self.requests as f64 / self.elapsed.as_secs_f64()
    }
}

#[derive(Default)]
struct CompletionObserver {
    queue_times: Mutex<Vec<Duration>>,
    processing_times: Mutex<Vec<Duration>>,
    maximum_queue_depth: AtomicUsize,
    finished: AtomicUsize,
}

impl CompletionObserver {
    fn reset(&self) {
        self.queue_times
            .lock()
            .expect("queue-time metrics poisoned")
            .clear();
        self.processing_times
            .lock()
            .expect("processing-time metrics poisoned")
            .clear();
        self.maximum_queue_depth.store(0, Ordering::Relaxed);
        self.finished.store(0, Ordering::Release);
    }

    fn snapshot(&self) -> (Vec<Duration>, Vec<Duration>, usize) {
        (
            self.queue_times
                .lock()
                .expect("queue-time metrics poisoned")
                .clone(),
            self.processing_times
                .lock()
                .expect("processing-time metrics poisoned")
                .clone(),
            self.maximum_queue_depth.load(Ordering::Relaxed),
        )
    }
}

impl ActorObserver for CompletionObserver {
    fn message_enqueued(
        &self,
        _actor: &ActorMetadata,
        _message: &MessageMetadata,
        queue_depth: usize,
    ) {
        self.maximum_queue_depth
            .fetch_max(queue_depth, Ordering::Relaxed);
    }

    fn message_started(
        &self,
        _actor: &ActorMetadata,
        _message: &MessageMetadata,
        queue_time: Duration,
    ) {
        self.queue_times
            .lock()
            .expect("queue-time metrics poisoned")
            .push(queue_time);
    }

    fn message_finished(
        &self,
        _actor: &ActorMetadata,
        _message: &MessageMetadata,
        _outcome: MessageOutcome,
        processing_time: Duration,
    ) {
        self.processing_times
            .lock()
            .expect("processing-time metrics poisoned")
            .push(processing_time);
        self.finished.fetch_add(1, Ordering::Release);
    }
}

#[derive(lattice_actor::Message)]
struct CompletionTell {
    admitted_from: Instant,
    payload: Bytes,
    completed: mpsc::UnboundedSender<Duration>,
}

#[derive(lattice_actor::Message)]
struct RawCompletionTell {
    payload: Bytes,
}

#[derive(lattice_actor::Message)]
struct CompletionBarrier {
    completed: Arc<Notify>,
}

#[derive(Default)]
struct CompletionActor {
    processed_bytes: usize,
}

impl Actor for CompletionActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;
}

impl Handler<CompletionTell> for CompletionActor {
    async fn handle(
        &mut self,
        _context: &mut HandlerContext<'_, Self>,
        message: CompletionTell,
    ) -> Result<(), Self::Error> {
        self.processed_bytes = self.processed_bytes.wrapping_add(message.payload.len());
        let _ = message.completed.send(message.admitted_from.elapsed());
        Ok(())
    }
}

impl Handler<RawCompletionTell> for CompletionActor {
    async fn handle(
        &mut self,
        _context: &mut HandlerContext<'_, Self>,
        message: RawCompletionTell,
    ) -> Result<(), Self::Error> {
        self.processed_bytes = self.processed_bytes.wrapping_add(message.payload.len());
        Ok(())
    }
}

impl Handler<CompletionBarrier> for CompletionActor {
    async fn handle(
        &mut self,
        _context: &mut HandlerContext<'_, Self>,
        message: CompletionBarrier,
    ) -> Result<(), Self::Error> {
        message.completed.notify_one();
        Ok(())
    }
}

pub struct ActorCompletionTopology {
    registry: Arc<ActorRegistry<CompletionActor>>,
    handle: lattice_actor::handle::ActorHandle<CompletionActor>,
    observer: Option<Arc<CompletionObserver>>,
}

impl ActorCompletionTopology {
    pub async fn start(mailbox_capacity: usize) -> Result<Self, Box<dyn Error>> {
        Self::start_with_observation(mailbox_capacity, true).await
    }

    pub async fn start_timing(mailbox_capacity: usize) -> Result<Self, Box<dyn Error>> {
        Self::start_with_observation(mailbox_capacity, false).await
    }

    async fn start_with_observation(
        mailbox_capacity: usize,
        observe: bool,
    ) -> Result<Self, Box<dyn Error>> {
        let observer = observe.then(|| Arc::new(CompletionObserver::default()));
        let registry = ActorRegistry::new(
            actor_kind!("BenchmarkCompletion"),
            ActorRegistryConfig {
                mailbox: MailboxConfig::bounded(mailbox_capacity),
                ..ActorRegistryConfig::default()
            },
        );
        let registry = match &observer {
            Some(observer) => {
                registry.with_observer(ActorObserverHandle::from_arc(observer.clone()))
            }
            None => registry,
        };
        let registry = Arc::new(registry);
        let handle = registry
            .start(ActorId::U64(1), CompletionActor::default())
            .await?;
        Ok(Self {
            registry,
            handle,
            observer,
        })
    }

    pub async fn run(
        &self,
        requests: usize,
        payload_bytes: usize,
    ) -> Result<ActorCompletionReport, Box<dyn Error>> {
        if let Some(observer) = &self.observer {
            observer.reset();
        }
        let payload = Bytes::from(vec![0_u8; payload_bytes]);
        let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
        let started = Instant::now();
        let mut mailbox_full_retries = 0;
        for _ in 0..requests {
            let admitted_from = Instant::now();
            let mut message = CompletionTell {
                admitted_from,
                payload: payload.clone(),
                completed: completed_tx.clone(),
            };
            loop {
                match self.handle.try_tell(message) {
                    Ok(()) => break,
                    Err(ActorTellError::MailboxFull(returned)) => {
                        message = returned;
                        mailbox_full_retries += 1;
                        tokio::task::yield_now().await;
                    }
                    Err(error) => return Err(Box::new(error)),
                }
            }
        }
        drop(completed_tx);

        let mut latencies = Vec::with_capacity(requests);
        while latencies.len() < requests {
            let latency = completed_rx
                .recv()
                .await
                .ok_or("completion actor closed before acknowledging the workload")?;
            latencies.push(latency);
        }
        if let Some(observer) = &self.observer {
            while observer.finished.load(Ordering::Acquire) < requests {
                tokio::task::yield_now().await;
            }
        }
        let elapsed = started.elapsed();
        let (queue_times, processing_times, maximum_queue_depth) =
            self.observer.as_ref().map_or_else(
                || (Vec::new(), Vec::new(), 0),
                |observer| observer.snapshot(),
            );
        Ok(ActorCompletionReport {
            workload: WorkloadReport {
                name: "local_actor_tell_completion",
                requests,
                successes: latencies.len(),
                errors: requests.saturating_sub(latencies.len()),
                elapsed,
                latencies,
                observed_actor_ids: [self.handle.local_ref().id()].into_iter().collect(),
            },
            queue_times,
            processing_times,
            mailbox_full_retries,
            maximum_queue_depth,
        })
    }

    pub async fn run_raw(
        &self,
        requests: usize,
        payload_bytes: usize,
    ) -> Result<RawActorCompletionReport, Box<dyn Error>> {
        let payload = Bytes::from(vec![0_u8; payload_bytes]);
        let started = Instant::now();
        let mut mailbox_full_retries = 0;
        for _ in 0..requests {
            let mut message = RawCompletionTell {
                payload: payload.clone(),
            };
            loop {
                match self.handle.try_tell(message) {
                    Ok(()) => break,
                    Err(ActorTellError::MailboxFull(returned)) => {
                        message = returned;
                        mailbox_full_retries += 1;
                        tokio::task::yield_now().await;
                    }
                    Err(error) => return Err(Box::new(error)),
                }
            }
        }

        let completed = Arc::new(Notify::new());
        let mut barrier = CompletionBarrier {
            completed: completed.clone(),
        };
        loop {
            match self.handle.try_tell(barrier) {
                Ok(()) => break,
                Err(ActorTellError::MailboxFull(returned)) => {
                    barrier = returned;
                    mailbox_full_retries += 1;
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(Box::new(error)),
            }
        }
        completed.notified().await;

        Ok(RawActorCompletionReport {
            requests,
            elapsed: started.elapsed(),
            mailbox_full_retries,
        })
    }

    pub async fn shutdown(&self) -> Result<(), Box<dyn Error>> {
        let drained = self.registry.drain().await;
        if !drained.completed() {
            return Err("completion benchmark actor did not drain cleanly".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ActorCompletionTopology;

    #[tokio::test]
    async fn completion_workload_observes_real_handler_progress() {
        let topology = ActorCompletionTopology::start(2).await.unwrap();
        let report = topology.run(16, 32).await.unwrap();
        assert_eq!(report.workload.successes, 16);
        assert_eq!(report.queue_times.len(), 16);
        assert_eq!(report.processing_times.len(), 16);
        assert!(report.maximum_queue_depth <= 2);
        topology.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn raw_completion_workload_waits_for_the_batch_barrier() {
        let topology = ActorCompletionTopology::start_timing(2).await.unwrap();
        let report = topology.run_raw(16, 32).await.unwrap();
        assert_eq!(report.requests, 16);
        assert!(report.throughput_per_second().is_finite());
        topology.shutdown().await.unwrap();
    }
}
