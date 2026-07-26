//! Backoff computation and retry scheduling for retained flushes.

use std::time::{Duration, Instant};

use super::{MongoPersistenceCoordinator, RetryPolicy};

impl RetryPolicy {
    pub(super) fn delay(self, attempt: u32, entropy: u64) -> Duration {
        let exponent = attempt.saturating_sub(1).min(self.max_exponent);
        let step = self
            .initial_delay
            .saturating_mul(1_u32.checked_shl(exponent).unwrap_or(u32::MAX))
            .min(self.max_delay);
        self.jittered(step, entropy)
    }

    fn jittered(self, step: Duration, entropy: u64) -> Duration {
        let percent = u128::from(self.jitter_percent.min(100));
        if percent == 0 {
            return step;
        }
        let span = step.as_nanos().saturating_mul(percent) / 100;
        let removed = u64::try_from(u128::from(entropy) % (span + 1)).unwrap_or(u64::MAX);
        step.saturating_sub(Duration::from_nanos(removed))
    }
}

impl MongoPersistenceCoordinator {
    pub fn retry_delay(&self) -> Option<Duration> {
        self.retry_not_before
            .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
    }

    /// Whether a failed flush is retained and waiting for its backoff to
    /// expire. Unlike [`Self::retry_delay`] this stays true once the deadline
    /// has already passed.
    pub fn has_pending_retry(&self) -> bool {
        self.retry_not_before.is_some()
    }

    /// Reserves the next self-addressed retry wakeup and returns how long the
    /// owner should wait before re-preparing.
    ///
    /// `None` means no wakeup is needed: nothing is retained, a flush is
    /// already in flight and will drive the next step, or a wakeup for the same
    /// deadline is still pending. An elapsed deadline always yields a wakeup so
    /// an undelivered notification cannot strand the retained write.
    pub(in crate::persistence) fn arm_retry_wakeup(&mut self) -> Option<Duration> {
        if self.in_flight.is_some() {
            return None;
        }
        let deadline = self.retry_not_before?;
        let delay = deadline.checked_duration_since(Instant::now());
        if delay.is_some() && self.retry_wakeup_armed == Some(deadline) {
            return None;
        }
        self.retry_wakeup_armed = Some(deadline);
        Some(delay.unwrap_or_default())
    }

    pub const fn retry_attempt(&self) -> u32 {
        self.retry_attempt
    }

    pub(super) fn schedule_retry(&mut self) {
        self.retry_attempt = self.retry_attempt.saturating_add(1);
        let entropy = self.next_retry_entropy();
        self.retry_not_before =
            Some(Instant::now() + self.retry_policy.delay(self.retry_attempt, entropy));
    }

    /// Advances the activation-local xorshift sequence seeded when the
    /// coordinator was created. Every activation therefore spreads its backoff
    /// differently without a shared random source.
    fn next_retry_entropy(&mut self) -> u64 {
        self.retry_entropy ^= self.retry_entropy << 13;
        self.retry_entropy ^= self.retry_entropy >> 7;
        self.retry_entropy ^= self.retry_entropy << 17;
        self.retry_entropy
    }
}
