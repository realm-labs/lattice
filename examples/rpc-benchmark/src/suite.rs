use std::time::Duration;

#[derive(Debug, Clone)]
pub struct PerformanceSuiteConfig {
    pub tell_requests: usize,
    pub ask_requests: usize,
    pub scaling_requests: usize,
    pub calibration_requests: usize,
    pub calibration_rounds: usize,
    pub payload_bytes: Vec<usize>,
    pub ask_windows: Vec<usize>,
    pub producer_counts: Vec<usize>,
    pub actor_counts: Vec<usize>,
    pub mailbox_capacity: usize,
    pub saturation_duration: Duration,
    pub saturation_fractions: Vec<f64>,
    pub saturation_sample_every: usize,
    pub saturation_burst_horizon: Duration,
    pub saturation_producers: usize,
    pub bulk_stripes: usize,
}

impl PerformanceSuiteConfig {
    pub fn from_env() -> Self {
        Self {
            tell_requests: env_usize("LATTICE_BENCH_TELL_REQUESTS", 100_000).max(1),
            ask_requests: env_usize("LATTICE_BENCH_ASK_REQUESTS", 10_000).max(1),
            scaling_requests: env_usize("LATTICE_BENCH_SCALING_REQUESTS", 1_000_000).max(1),
            calibration_requests: env_usize("LATTICE_BENCH_CALIBRATION_REQUESTS", 1_000_000).max(1),
            calibration_rounds: env_usize("LATTICE_BENCH_CALIBRATION_ROUNDS", 3).max(1),
            payload_bytes: env_usize_list_allow_zero(
                "LATTICE_BENCH_PAYLOAD_MATRIX",
                &[0, 128, 1_024, 16_384],
            ),
            ask_windows: env_usize_list("LATTICE_BENCH_ASK_WINDOWS", &[1, 64, 256]),
            producer_counts: env_usize_list("LATTICE_BENCH_PRODUCERS", &[1, 4, 16]),
            actor_counts: env_usize_list("LATTICE_BENCH_ACTORS", &[1, 16, 256]),
            mailbox_capacity: env_usize("LATTICE_BENCH_MAILBOX_CAPACITY", 1_024).max(1),
            saturation_duration: Duration::from_millis(
                env_usize("LATTICE_BENCH_SATURATION_MILLIS", 10_000).max(1) as u64,
            ),
            saturation_fractions: env_f64_list(
                "LATTICE_BENCH_SATURATION_FRACTIONS",
                &[0.25, 0.50, 0.75, 0.90, 1.00, 1.10],
            ),
            saturation_sample_every: env_usize("LATTICE_BENCH_SATURATION_SAMPLE_EVERY", 1_024)
                .max(1),
            saturation_burst_horizon: Duration::from_micros(
                env_usize("LATTICE_BENCH_SATURATION_BURST_MICROS", 1_000).max(1) as u64,
            ),
            saturation_producers: env_usize("LATTICE_BENCH_SATURATION_PRODUCERS", 4).max(1),
            bulk_stripes: env_usize("LATTICE_BENCH_BULK_STRIPES", 1).clamp(1, 4),
        }
    }

    pub fn primary_payload_bytes(&self) -> usize {
        self.payload_bytes
            .iter()
            .copied()
            .find(|size| *size == 128)
            .or_else(|| self.payload_bytes.first().copied())
            .unwrap_or(128)
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize_list(name: &str, default: &[usize]) -> Vec<usize> {
    parse_list(name, |value| value.parse::<usize>().ok())
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| default.to_vec())
        .into_iter()
        .map(|value| value.max(1))
        .collect()
}

fn env_usize_list_allow_zero(name: &str, default: &[usize]) -> Vec<usize> {
    parse_list(name, |value| value.parse::<usize>().ok())
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| default.to_vec())
}

fn env_f64_list(name: &str, default: &[f64]) -> Vec<f64> {
    parse_list(name, |value| value.parse::<f64>().ok())
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| default.to_vec())
        .into_iter()
        .filter(|value| value.is_finite() && *value > 0.0)
        .collect()
}

fn parse_list<T>(name: &str, parse: impl Fn(&str) -> Option<T>) -> Option<Vec<T>> {
    std::env::var(name).ok().map(|value| {
        value
            .split(',')
            .filter_map(|part| parse(part.trim()))
            .collect()
    })
}

#[cfg(test)]
mod tests {
    use super::PerformanceSuiteConfig;

    #[test]
    fn primary_payload_prefers_cross_framework_default() {
        let mut config = PerformanceSuiteConfig::from_env();
        config.payload_bytes = vec![0, 128, 1_024];
        assert_eq!(config.primary_payload_bytes(), 128);
        config.payload_bytes = vec![64, 1_024];
        assert_eq!(config.primary_payload_bytes(), 64);
    }
}
