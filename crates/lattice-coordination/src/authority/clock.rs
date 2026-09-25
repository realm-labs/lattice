//! Serving deadlines use elapsed time including host suspension. VM snapshot restoration
//! is not supported: restored processes must restart with a fresh incarnation.
//!
//! Windows documents GetTickCount64 as including sleep/hibernation; Linux CLOCK_BOOTTIME
//! includes suspend. Other platforms deliberately cannot enable automatic authority.
//! The clock-rate contract permits at most 1,000 ppm error, with 32 ms resolution slack.
//! Deployment operators must not run this protocol on clocks outside that bound.

use std::time::Duration;

use crate::types::MonotonicTime;

/// Protocol-wide cap, independent of per-node lease configuration. A new leader
/// with a shorter configured lease must still wait out the predecessor's grants.
pub const MAXIMUM_GRANT_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Copy)]
pub struct AuthorityClock {
    #[cfg(test)]
    origin: tokio::time::Instant,
}

impl AuthorityClock {
    pub fn new() -> Option<Self> {
        if !Self::supported() {
            return None;
        }
        let clock = Self {
            #[cfg(test)]
            origin: tokio::time::Instant::now(),
        };
        (clock.now().as_millis() != u64::MAX).then_some(clock)
    }

    pub const fn supported() -> bool {
        cfg!(any(test, target_os = "linux", target_os = "windows"))
    }

    pub fn now(self) -> MonotonicTime {
        #[cfg(test)]
        let milliseconds = u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX);
        #[cfg(all(not(test), target_os = "windows"))]
        let milliseconds = {
            #[link(name = "kernel32")]
            unsafe extern "system" {
                fn GetTickCount64() -> u64;
            }
            // SAFETY: this OS function takes no pointers and has no caller preconditions.
            unsafe { GetTickCount64() }
        };
        #[cfg(all(not(test), target_os = "linux"))]
        let milliseconds = {
            let mut time = std::mem::MaybeUninit::<libc::timespec>::uninit();
            // SAFETY: time points to writable storage for one timespec. Inspect it only
            // after a successful call; clock failure closes admission rather than extending it.
            if unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, time.as_mut_ptr()) } != 0 {
                u64::MAX
            } else {
                let time = unsafe { time.assume_init() };
                u64::try_from(time.tv_sec)
                    .unwrap_or(u64::MAX)
                    .saturating_mul(1000)
                    .saturating_add(u64::try_from(time.tv_nsec).unwrap_or(u64::MAX) / 1_000_000)
            }
        };
        #[cfg(all(not(test), not(any(target_os = "linux", target_os = "windows"))))]
        let milliseconds = u64::MAX;
        MonotonicTime::from_millis(milliseconds)
    }

    /// Clock failure and backwards readings are not evidence that time passed.
    pub fn elapsed_since(self, earlier: MonotonicTime) -> Option<Duration> {
        checked_elapsed(self.now(), earlier)
    }

    /// Deduct clock-rate and timer-resolution uncertainty from a lease observation.
    pub fn usable_duration(duration: Duration) -> Duration {
        duration.saturating_sub(duration / 500 + Duration::from_millis(32))
    }

    /// A takeover wait uses the opposite rounding direction from a serving deadline.
    pub fn takeover_interval(maximum_grant: Duration) -> Duration {
        maximum_grant.saturating_add(maximum_grant / 500 + Duration::from_millis(32))
    }
}

fn checked_elapsed(now: MonotonicTime, earlier: MonotonicTime) -> Option<Duration> {
    if now.as_millis() == u64::MAX || earlier.as_millis() == u64::MAX {
        return None;
    }
    now.as_millis()
        .checked_sub(earlier.as_millis())
        .map(Duration::from_millis)
}

#[cfg(test)]
mod tests {
    use super::{Duration, MonotonicTime, checked_elapsed};

    #[test]
    fn failed_or_backwards_clock_never_completes_a_takeover_wait() {
        let at = MonotonicTime::from_millis;
        assert_eq!(checked_elapsed(at(u64::MAX), at(0)), None);
        assert_eq!(checked_elapsed(at(u64::MAX), at(u64::MAX)), None);
        assert_eq!(checked_elapsed(at(50), at(100)), None);
        assert_eq!(
            checked_elapsed(at(100), at(50)),
            Some(Duration::from_millis(50))
        );
    }
}
