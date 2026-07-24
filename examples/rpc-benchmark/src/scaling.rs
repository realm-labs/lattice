use std::{
    error::Error,
    io::Error as IoError,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use lattice_actor::{
    context::HandlerContext,
    error::{ActorError, ActorTellError},
    handle::ActorHandle,
    mailbox::MailboxConfig,
    registry::{ActorRegistry, ActorRegistryConfig},
    traits::{Actor, Handler},
};
use lattice_core::{actor_kind, id::ActorId};
use tokio::sync::Notify;

#[derive(lattice_actor::Message)]
struct ScaleTell(Bytes);

#[derive(lattice_actor::Message)]
struct ScaleBarrier(Arc<Notify>);

#[derive(Default)]
struct ScaleActor {
    processed_bytes: usize,
}

impl Actor for ScaleActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;
}

impl Handler<ScaleTell> for ScaleActor {
    async fn handle(
        &mut self,
        _context: &mut HandlerContext<'_, Self>,
        message: ScaleTell,
    ) -> Result<(), Self::Error> {
        self.processed_bytes = self.processed_bytes.wrapping_add(message.0.len());
        Ok(())
    }
}

impl Handler<ScaleBarrier> for ScaleActor {
    async fn handle(
        &mut self,
        _context: &mut HandlerContext<'_, Self>,
        message: ScaleBarrier,
    ) -> Result<(), Self::Error> {
        message.0.notify_one();
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ScaleReport {
    pub requests: usize,
    pub actor_count: usize,
    pub producer_count: usize,
    pub mailbox_full_retries: usize,
    pub elapsed: Duration,
}

impl ScaleReport {
    pub fn throughput_per_second(&self) -> f64 {
        self.requests as f64 / self.elapsed.as_secs_f64()
    }
}

#[derive(Debug, Clone)]
pub struct MailboxContentionReport {
    pub requests: usize,
    pub producer_count: usize,
    pub rounds: usize,
    pub admission_elapsed: Duration,
    pub completion_elapsed: Duration,
}

impl MailboxContentionReport {
    pub fn admission_throughput_per_second(&self) -> f64 {
        self.requests as f64 / self.admission_elapsed.as_secs_f64()
    }

    pub fn completion_throughput_per_second(&self) -> f64 {
        self.requests as f64 / self.completion_elapsed.as_secs_f64()
    }
}

pub struct ActorScaleTopology {
    registry: Arc<ActorRegistry<ScaleActor>>,
    handles: Arc<Vec<ActorHandle<ScaleActor>>>,
}

impl ActorScaleTopology {
    pub async fn start(
        actor_count: usize,
        mailbox_capacity: usize,
    ) -> Result<Self, Box<dyn Error>> {
        Self::start_with_mailbox(actor_count, MailboxConfig::bounded(mailbox_capacity.max(1))).await
    }

    pub async fn start_contention(requests_per_round: usize) -> Result<Self, Box<dyn Error>> {
        Self::start_with_mailbox(
            1,
            MailboxConfig::with_lanes(requests_per_round.max(1), 8).with_deferred_capacity(8),
        )
        .await
    }

    async fn start_with_mailbox(
        actor_count: usize,
        mailbox: MailboxConfig,
    ) -> Result<Self, Box<dyn Error>> {
        let actor_count = actor_count.max(1);
        let registry = Arc::new(ActorRegistry::new(
            actor_kind!("BenchmarkScale"),
            ActorRegistryConfig {
                mailbox,
                ..ActorRegistryConfig::default()
            },
        ));
        let mut handles = Vec::with_capacity(actor_count);
        for index in 0..actor_count {
            handles.push(
                registry
                    .start(ActorId::U64((index + 1) as u64), ScaleActor::default())
                    .await?,
            );
        }
        Ok(Self {
            registry,
            handles: Arc::new(handles),
        })
    }

    pub async fn run_contention(
        &self,
        requests_per_round: usize,
        payload_bytes: usize,
        producer_count: usize,
        rounds: usize,
    ) -> Result<MailboxContentionReport, Box<dyn Error>> {
        let producer_count = producer_count.max(1);
        let rounds = rounds.max(1);
        let payload = Bytes::from(vec![0_u8; payload_bytes]);
        let mut admission_elapsed = Duration::ZERO;
        let mut completion_elapsed = Duration::ZERO;

        for _ in 0..rounds {
            let started = Instant::now();
            let mut tasks = Vec::with_capacity(producer_count);
            for producer in 0..producer_count {
                let handle = self.handles[0].clone();
                let payload = payload.clone();
                let producer_requests = requests_per_round / producer_count
                    + usize::from(producer < requests_per_round % producer_count);
                tasks.push(tokio::spawn(async move {
                    for _ in 0..producer_requests {
                        handle.try_tell(ScaleTell(payload.clone())).map_err(
                            |error| match error {
                                ActorTellError::MailboxFull(_) => {
                                    "contention mailbox unexpectedly reached capacity".to_owned()
                                }
                                error => error.to_string(),
                            },
                        )?;
                    }
                    Ok::<_, String>(())
                }));
            }
            for task in tasks {
                task.await?.map_err(IoError::other)?;
            }
            admission_elapsed += started.elapsed();

            let completed = Arc::new(Notify::new());
            self.handles[0]
                .tell(ScaleBarrier(completed.clone()))
                .await?;
            completed.notified().await;
            completion_elapsed += started.elapsed();
        }

        Ok(MailboxContentionReport {
            requests: requests_per_round.saturating_mul(rounds),
            producer_count,
            rounds,
            admission_elapsed,
            completion_elapsed,
        })
    }

    pub async fn run(
        &self,
        requests: usize,
        payload_bytes: usize,
        producer_count: usize,
    ) -> Result<ScaleReport, Box<dyn Error>> {
        let producer_count = producer_count.max(1);
        let payload = Bytes::from(vec![0_u8; payload_bytes]);
        let started = Instant::now();
        let mut tasks = Vec::with_capacity(producer_count);
        for producer in 0..producer_count {
            let handles = self.handles.clone();
            let payload = payload.clone();
            let producer_requests =
                requests / producer_count + usize::from(producer < requests % producer_count);
            tasks.push(tokio::spawn(async move {
                let mut retries = 0;
                for offset in 0..producer_requests {
                    let target = (producer + offset) % handles.len();
                    let mut message = ScaleTell(payload.clone());
                    loop {
                        match handles[target].try_tell(message) {
                            Ok(()) => break,
                            Err(ActorTellError::MailboxFull(returned)) => {
                                message = returned;
                                retries += 1;
                                tokio::task::yield_now().await;
                            }
                            Err(error) => return Err(error.to_string()),
                        }
                    }
                }
                Ok::<_, String>(retries)
            }));
        }
        let mut mailbox_full_retries = 0;
        for task in tasks {
            mailbox_full_retries += task.await?.map_err(IoError::other)?;
        }
        let mut barriers = Vec::with_capacity(self.handles.len());
        for handle in self.handles.iter() {
            let completed = Arc::new(Notify::new());
            handle.tell(ScaleBarrier(completed.clone())).await?;
            barriers.push(completed);
        }
        for completed in barriers {
            completed.notified().await;
        }
        Ok(ScaleReport {
            requests,
            actor_count: self.handles.len(),
            producer_count,
            mailbox_full_retries,
            elapsed: started.elapsed(),
        })
    }

    pub async fn shutdown(&self) -> Result<(), Box<dyn Error>> {
        let drained = self.registry.drain().await;
        if !drained.completed() {
            return Err("scaling benchmark actors did not drain cleanly".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ActorScaleTopology;

    #[tokio::test]
    async fn contention_workload_separates_admission_from_completion() {
        let topology = ActorScaleTopology::start_contention(128).await.unwrap();
        let report = topology.run_contention(128, 64, 3, 2).await.unwrap();
        assert_eq!(report.requests, 256);
        assert_eq!(report.producer_count, 3);
        assert_eq!(report.rounds, 2);
        assert!(report.admission_throughput_per_second().is_finite());
        assert!(report.completion_throughput_per_second().is_finite());
        assert!(report.admission_elapsed <= report.completion_elapsed);
        topology.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn scaling_workload_waits_for_every_actor() {
        let topology = ActorScaleTopology::start(4, 2).await.unwrap();
        let report = topology.run(128, 64, 3).await.unwrap();
        assert_eq!(report.requests, 128);
        assert_eq!(report.actor_count, 4);
        assert_eq!(report.producer_count, 3);
        assert!(report.throughput_per_second().is_finite());
        topology.shutdown().await.unwrap();
    }
}
