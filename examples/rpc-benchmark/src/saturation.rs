use std::{
    error::Error,
    io::Error as IoError,
    sync::{Arc, Barrier, Mutex},
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

use crate::metrics::percentile_duration;

#[derive(lattice_actor::Message)]
struct SaturationTell {
    payload: Bytes,
    sampled_at: Option<Instant>,
}

#[derive(lattice_actor::Message)]
struct SaturationBarrier(Arc<Notify>);

#[derive(Default)]
struct SaturationState {
    sampled_latencies: Mutex<Vec<Duration>>,
}

impl SaturationState {
    fn reset_samples(&self) {
        self.sampled_latencies
            .lock()
            .expect("saturation samples poisoned")
            .clear();
    }

    fn record_sample(&self, sampled_at: Option<Instant>) {
        if let Some(sampled_at) = sampled_at {
            self.sampled_latencies
                .lock()
                .expect("saturation samples poisoned")
                .push(sampled_at.elapsed());
        }
    }

    fn samples(&self) -> Vec<Duration> {
        self.sampled_latencies
            .lock()
            .expect("saturation samples poisoned")
            .clone()
    }
}

struct SaturationActor {
    state: Arc<SaturationState>,
    processed_bytes: usize,
}

impl Actor for SaturationActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;
}

impl Handler<SaturationTell> for SaturationActor {
    async fn handle(
        &mut self,
        _context: &mut HandlerContext<'_, Self>,
        message: SaturationTell,
    ) -> Result<(), Self::Error> {
        self.processed_bytes = self.processed_bytes.wrapping_add(message.payload.len());
        self.state.record_sample(message.sampled_at);
        Ok(())
    }
}

impl Handler<SaturationBarrier> for SaturationActor {
    async fn handle(
        &mut self,
        _context: &mut HandlerContext<'_, Self>,
        message: SaturationBarrier,
    ) -> Result<(), Self::Error> {
        message.0.notify_one();
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SaturationReport {
    pub target_rate_per_second: u64,
    pub producer_count: usize,
    pub offered: usize,
    pub admitted: usize,
    pub rejected: usize,
    pub offer_elapsed: Duration,
    pub completion_elapsed: Duration,
    pub generator_missed: usize,
    pub maximum_schedule_lag: Duration,
    pub sampled_latencies: Vec<Duration>,
}

#[derive(Debug, Clone, Copy)]
pub struct SaturationCalibrationReport {
    pub requests: usize,
    pub producer_count: usize,
    pub mailbox_full_retries: usize,
    pub elapsed: Duration,
}

impl SaturationCalibrationReport {
    pub fn throughput_per_second(self) -> f64 {
        self.requests as f64 / self.elapsed.as_secs_f64()
    }
}

impl SaturationReport {
    pub fn offered_per_second(&self) -> f64 {
        self.offered as f64 / self.offer_elapsed.as_secs_f64()
    }

    pub fn completed_per_second(&self) -> f64 {
        self.admitted as f64 / self.completion_elapsed.as_secs_f64()
    }

    pub fn rejection_ratio(&self) -> f64 {
        if self.offered == 0 {
            return 0.0;
        }
        self.rejected as f64 / self.offered as f64
    }

    pub fn percentile_latency(&self, percentile: f64) -> Duration {
        percentile_duration(&self.sampled_latencies, percentile)
    }
}

pub struct SaturationTopology {
    registry: Arc<ActorRegistry<SaturationActor>>,
    handle: ActorHandle<SaturationActor>,
    state: Arc<SaturationState>,
    mailbox_capacity: usize,
}

impl SaturationTopology {
    pub async fn start(mailbox_capacity: usize) -> Result<Self, Box<dyn Error>> {
        let mailbox_capacity = mailbox_capacity.max(1);
        let registry = Arc::new(ActorRegistry::new(
            actor_kind!("BenchmarkSaturation"),
            ActorRegistryConfig {
                mailbox: MailboxConfig::bounded(mailbox_capacity),
                ..ActorRegistryConfig::default()
            },
        ));
        let state = Arc::new(SaturationState::default());
        let handle = registry
            .start(
                ActorId::U64(1),
                SaturationActor {
                    state: state.clone(),
                    processed_bytes: 0,
                },
            )
            .await?;
        Ok(Self {
            registry,
            handle,
            state,
            mailbox_capacity,
        })
    }

    pub async fn run(
        &self,
        target_rate_per_second: u64,
        duration: Duration,
        burst_horizon: Duration,
        payload_bytes: usize,
        sample_every: usize,
        producer_count: usize,
    ) -> Result<SaturationReport, Box<dyn Error>> {
        let target_rate_per_second = target_rate_per_second.max(1);
        let duration = duration.max(Duration::from_millis(1));
        let burst_horizon = burst_horizon.max(Duration::from_micros(1)).min(duration);
        let sample_every = sample_every.max(1);
        let producer_count = producer_count
            .max(1)
            .min(usize::try_from(target_rate_per_second).unwrap_or(usize::MAX));
        let payload = Bytes::from(vec![0_u8; payload_bytes]);
        self.state.reset_samples();
        let start = Arc::new(Barrier::new(producer_count));
        let mut tasks = Vec::with_capacity(producer_count);
        for producer in 0..producer_count {
            let producer_rate = target_rate_per_second / producer_count as u64
                + u64::from((producer as u64) < target_rate_per_second % producer_count as u64);
            let maximum_burst = messages_for_elapsed(producer_rate, burst_horizon)
                .max(1)
                .min((self.mailbox_capacity / (4 * producer_count)).max(1));
            let handle = self.handle.clone();
            let payload = payload.clone();
            let start = start.clone();
            tasks.push(tokio::task::spawn_blocking(move || {
                start.wait();
                generate_open_loop_load(
                    handle,
                    payload,
                    producer_rate,
                    duration,
                    maximum_burst,
                    sample_every.saturating_mul(producer_count),
                )
            }));
        }
        let mut generated = Vec::with_capacity(producer_count);
        for task in tasks {
            generated.push(
                task.await
                    .map_err(|_| IoError::other("saturation load generator panicked"))?
                    .map_err(IoError::other)?,
            );
        }
        let generated = GeneratedLoad::combine(generated);
        self.wait_for_barrier().await?;
        Ok(SaturationReport {
            target_rate_per_second,
            producer_count,
            offered: generated.offered,
            admitted: generated.admitted,
            rejected: generated.rejected,
            offer_elapsed: generated.offer_elapsed,
            completion_elapsed: generated.started.elapsed(),
            generator_missed: generated.missed,
            maximum_schedule_lag: generated.maximum_schedule_lag,
            sampled_latencies: self.state.samples(),
        })
    }

    pub async fn calibrate(
        &self,
        requests: usize,
        payload_bytes: usize,
        producer_count: usize,
    ) -> Result<SaturationCalibrationReport, Box<dyn Error>> {
        let producer_count = producer_count.max(1).min(requests.max(1));
        let payload = Bytes::from(vec![0_u8; payload_bytes]);
        let start = Arc::new(Barrier::new(producer_count));
        let mut tasks = Vec::with_capacity(producer_count);
        for producer in 0..producer_count {
            let producer_requests =
                requests / producer_count + usize::from(producer < requests % producer_count);
            let handle = self.handle.clone();
            let payload = payload.clone();
            let start = start.clone();
            tasks.push(tokio::task::spawn_blocking(move || {
                start.wait();
                generate_unbounded_load(handle, payload, producer_requests)
            }));
        }
        let mut generated = Vec::with_capacity(producer_count);
        for task in tasks {
            generated.push(
                task.await
                    .map_err(|_| IoError::other("saturation calibration generator panicked"))?
                    .map_err(IoError::other)?,
            );
        }
        let started = generated
            .iter()
            .map(|load| load.started)
            .min()
            .ok_or_else(|| IoError::other("saturation calibration started no producers"))?;
        let mailbox_full_retries = generated.iter().map(|load| load.mailbox_full_retries).sum();
        self.wait_for_barrier().await?;
        Ok(SaturationCalibrationReport {
            requests,
            producer_count,
            mailbox_full_retries,
            elapsed: started.elapsed(),
        })
    }

    pub async fn shutdown(&self) -> Result<(), Box<dyn Error>> {
        let drained = self.registry.drain().await;
        if !drained.completed() {
            return Err("saturation benchmark actor did not drain cleanly".into());
        }
        Ok(())
    }

    async fn wait_for_barrier(&self) -> Result<(), Box<dyn Error>> {
        let completed = Arc::new(Notify::new());
        self.handle
            .tell(SaturationBarrier(completed.clone()))
            .await?;
        completed.notified().await;
        Ok(())
    }
}

struct UnboundedLoad {
    started: Instant,
    mailbox_full_retries: usize,
}

struct GeneratedLoad {
    started: Instant,
    offered: usize,
    admitted: usize,
    rejected: usize,
    missed: usize,
    offer_elapsed: Duration,
    maximum_schedule_lag: Duration,
}

impl GeneratedLoad {
    fn combine(loads: Vec<Self>) -> Self {
        let started = loads
            .iter()
            .map(|load| load.started)
            .min()
            .expect("at least one saturation producer");
        let finished = loads
            .iter()
            .map(|load| load.started + load.offer_elapsed)
            .max()
            .expect("at least one saturation producer");
        Self {
            started,
            offered: loads.iter().map(|load| load.offered).sum(),
            admitted: loads.iter().map(|load| load.admitted).sum(),
            rejected: loads.iter().map(|load| load.rejected).sum(),
            missed: loads.iter().map(|load| load.missed).sum(),
            offer_elapsed: finished.saturating_duration_since(started),
            maximum_schedule_lag: loads
                .iter()
                .map(|load| load.maximum_schedule_lag)
                .max()
                .unwrap_or_default(),
        }
    }
}

fn generate_unbounded_load(
    handle: ActorHandle<SaturationActor>,
    payload: Bytes,
    requests: usize,
) -> Result<UnboundedLoad, String> {
    let started = Instant::now();
    let mut mailbox_full_retries = 0;
    for _ in 0..requests {
        let mut message = SaturationTell {
            payload: payload.clone(),
            sampled_at: None,
        };
        loop {
            match handle.try_tell(message) {
                Ok(()) => break,
                Err(ActorTellError::MailboxFull(returned)) => {
                    message = returned;
                    mailbox_full_retries += 1;
                    if mailbox_full_retries % 64 == 0 {
                        std::thread::yield_now();
                    } else {
                        std::hint::spin_loop();
                    }
                }
                Err(error) => return Err(error.to_string()),
            }
        }
    }
    Ok(UnboundedLoad {
        started,
        mailbox_full_retries,
    })
}

fn generate_open_loop_load(
    handle: ActorHandle<SaturationActor>,
    payload: Bytes,
    target_rate_per_second: u64,
    duration: Duration,
    maximum_burst: usize,
    sample_every: usize,
) -> Result<GeneratedLoad, String> {
    let started = Instant::now();
    let mut scheduled = 0;
    let mut offered = 0;
    let mut admitted = 0;
    let mut rejected = 0;
    let mut missed = 0;
    let mut maximum_schedule_lag = Duration::ZERO;
    let total_scheduled = messages_for_elapsed(target_rate_per_second, duration);
    while scheduled < total_scheduled {
        pace_until_batch(
            started,
            scheduled.saturating_add(maximum_burst).min(total_scheduled),
            target_rate_per_second,
            duration,
        );
        let elapsed = started.elapsed();
        let bounded_elapsed = elapsed.min(duration);
        let due =
            messages_for_elapsed(target_rate_per_second, bounded_elapsed).min(total_scheduled);
        let backlog = due.saturating_sub(scheduled);
        if backlog > 0 {
            let oldest_deadline = duration_for_message(scheduled + 1, target_rate_per_second);
            maximum_schedule_lag =
                maximum_schedule_lag.max(elapsed.saturating_sub(oldest_deadline));
            let skipped = backlog.saturating_sub(maximum_burst);
            scheduled += skipped;
            missed += skipped;
            let batch = backlog - skipped;
            for _ in 0..batch {
                let sampled = offered % sample_every == 0;
                let message = SaturationTell {
                    payload: payload.clone(),
                    sampled_at: sampled.then(Instant::now),
                };
                scheduled += 1;
                offered += 1;
                match handle.try_tell(message) {
                    Ok(()) => admitted += 1,
                    Err(ActorTellError::MailboxFull(_)) => rejected += 1,
                    Err(error) => return Err(error.to_string()),
                }
            }
        }
    }
    Ok(GeneratedLoad {
        started,
        offered,
        admitted,
        rejected,
        missed,
        offer_elapsed: started.elapsed(),
        maximum_schedule_lag,
    })
}

fn pace_until_batch(
    started: Instant,
    next_message: usize,
    rate_per_second: u64,
    duration: Duration,
) {
    let deadline = duration_for_message(next_message, rate_per_second).min(duration);
    const SPIN_THRESHOLD: Duration = Duration::from_millis(1);
    loop {
        let remaining = deadline.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return;
        }
        if remaining > SPIN_THRESHOLD {
            std::thread::sleep(remaining - SPIN_THRESHOLD);
        } else {
            std::hint::spin_loop();
        }
    }
}

fn messages_for_elapsed(rate_per_second: u64, elapsed: Duration) -> usize {
    let messages = u128::from(rate_per_second).saturating_mul(elapsed.as_nanos()) / 1_000_000_000;
    usize::try_from(messages).unwrap_or(usize::MAX)
}

fn duration_for_message(message: usize, rate_per_second: u64) -> Duration {
    let nanos = (message as u128)
        .saturating_mul(1_000_000_000)
        .div_ceil(u128::from(rate_per_second));
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{SaturationTopology, messages_for_elapsed};

    #[test]
    fn rate_conversion_is_deterministic() {
        assert_eq!(
            messages_for_elapsed(100_000, Duration::from_millis(10)),
            1_000
        );
    }

    #[tokio::test]
    async fn saturation_reports_admission_and_completion() {
        let topology = SaturationTopology::start(8).await.unwrap();
        let calibration = topology.calibrate(128, 64, 2).await.unwrap();
        assert!(calibration.throughput_per_second().is_finite());
        let report = topology
            .run(
                10_000,
                Duration::from_millis(20),
                Duration::from_millis(1),
                64,
                4,
                2,
            )
            .await
            .unwrap();
        assert_eq!(report.offered, report.admitted + report.rejected);
        assert!(!report.sampled_latencies.is_empty());
        topology.shutdown().await.unwrap();
    }
}
