use std::collections::BTreeSet;
use std::fmt;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct WorkloadReport {
    pub name: &'static str,
    pub requests: usize,
    pub successes: usize,
    pub errors: usize,
    pub elapsed: Duration,
    pub latencies: Vec<Duration>,
    pub observed_actor_ids: BTreeSet<u64>,
}

impl WorkloadReport {
    pub fn throughput_per_second(&self) -> f64 {
        if self.elapsed.is_zero() {
            return 0.0;
        }
        self.successes as f64 / self.elapsed.as_secs_f64()
    }

    pub fn average_latency(&self) -> Duration {
        if self.latencies.is_empty() {
            return Duration::ZERO;
        }
        let total_nanos: u128 = self
            .latencies
            .iter()
            .map(|latency| latency.as_nanos())
            .sum();
        Duration::from_nanos((total_nanos / self.latencies.len() as u128) as u64)
    }

    pub fn percentile_latency(&self, percentile: f64) -> Duration {
        percentile_duration(&self.latencies, percentile)
    }
}

impl fmt::Display for WorkloadReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}: requests={} successes={} errors={} throughput={:.2}/s avg={} p50={} p95={} p99={} actors={}",
            self.name,
            self.requests,
            self.successes,
            self.errors,
            self.throughput_per_second(),
            format_duration(self.average_latency()),
            format_duration(self.percentile_latency(0.50)),
            format_duration(self.percentile_latency(0.95)),
            format_duration(self.percentile_latency(0.99)),
            self.observed_actor_ids.len()
        )
    }
}

pub(crate) fn percentile_duration(latencies: &[Duration], percentile: f64) -> Duration {
    if latencies.is_empty() {
        return Duration::ZERO;
    }
    let mut sorted = latencies.to_vec();
    sorted.sort_unstable();
    let capped = percentile.clamp(0.0, 1.0);
    let index = ((sorted.len() - 1) as f64 * capped).ceil() as usize;
    sorted[index]
}

fn format_duration(duration: Duration) -> String {
    if duration.as_millis() > 0 {
        format!("{:.3}ms", duration.as_secs_f64() * 1_000.0)
    } else {
        format!("{:.3}us", duration.as_secs_f64() * 1_000_000.0)
    }
}
