//! Conservative actor-local mutation tracking.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use tokio::sync::Notify;

const DIRTY: u8 = 1 << 0;
const QUEUED: u8 = 1 << 1;
const SUSPENDED: u8 = 1 << 2;

/// One coalesced mutation edge shared by all tracked values registered with a
/// persistence coordinator.
#[derive(Debug, Clone)]
pub(crate) struct TrackedMutationSignal {
    inner: Arc<TrackedMutationSignalInner>,
}

#[derive(Debug)]
struct TrackedMutationSignalInner {
    state: AtomicU8,
    pump_started: AtomicBool,
    changed: Notify,
}

impl TrackedMutationSignal {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(TrackedMutationSignalInner {
                state: AtomicU8::new(0),
                pump_started: AtomicBool::new(false),
                changed: Notify::new(),
            }),
        }
    }

    pub(crate) fn start_pump(&self) -> bool {
        self.inner
            .pump_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub(crate) fn mark_dirty(&self) {
        let previous = self.inner.state.fetch_or(DIRTY, Ordering::AcqRel);
        if previous & (DIRTY | QUEUED | SUSPENDED) == 0 {
            self.inner.changed.notify_one();
        }
    }

    /// Waits until dirty work exists and atomically reserves its one mailbox
    /// notification. `Notify::notify_one` retains a permit when this future is
    /// not currently waiting, so mutation-before-registration is not lost.
    pub(crate) async fn wait_to_queue(&self) {
        loop {
            let notified = self.inner.changed.notified();
            if self.try_mark_queued() {
                return;
            }
            notified.await;
        }
    }

    fn try_mark_queued(&self) -> bool {
        let mut state = self.inner.state.load(Ordering::Acquire);
        loop {
            if state & DIRTY == 0 || state & (QUEUED | SUSPENDED) != 0 {
                return false;
            }
            match self.inner.state.compare_exchange_weak(
                state,
                state | QUEUED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => state = actual,
            }
        }
    }

    /// Acknowledges the dedicated persistence message and consumes the dirty
    /// edge that caused it. Mutation epochs remain the persistence source of
    /// truth; this signal only controls scheduling.
    pub(crate) fn begin_message(&self) -> bool {
        self.inner
            .state
            .fetch_and(!(DIRTY | QUEUED | SUSPENDED), Ordering::AcqRel)
            & DIRTY
            != 0
    }

    pub(crate) fn suspend(&self) {
        self.inner.state.fetch_or(SUSPENDED, Ordering::AcqRel);
    }

    pub(crate) fn resume(&self) {
        let previous = self.inner.state.fetch_and(!SUSPENDED, Ordering::AcqRel);
        if previous & DIRTY != 0 && previous & QUEUED == 0 {
            self.inner.changed.notify_one();
        }
    }

    pub(crate) fn cancel_queued(&self) {
        let previous = self.inner.state.fetch_and(!QUEUED, Ordering::AcqRel);
        if previous & DIRTY != 0 {
            self.inner.changed.notify_one();
        }
    }
}

/// Wraps an ordinary value and advances an epoch whenever mutable access is
/// requested. The epoch is a conservative dirty indicator: mutable access may
/// cause a false-positive scan even when the value is unchanged, but persisted
/// state must never change without advancing it.
pub struct Tracked<T> {
    value: T,
    mutation_epoch: u64,
    mutation_signal: Option<TrackedMutationSignal>,
}

impl<T> Tracked<T> {
    pub const fn clean(value: T) -> Self {
        Self {
            value,
            mutation_epoch: 0,
            mutation_signal: None,
        }
    }

    pub(crate) fn signaled(value: T, mutation_signal: TrackedMutationSignal) -> Self {
        Self {
            value,
            mutation_epoch: 0,
            mutation_signal: Some(mutation_signal),
        }
    }

    pub const fn read(&self) -> &T {
        &self.value
    }

    pub fn write(&mut self) -> &mut T {
        self.mutation_epoch = self
            .mutation_epoch
            .checked_add(1)
            .expect("tracked mutation epoch exhausted");
        if let Some(signal) = &self.mutation_signal {
            signal.mark_dirty();
        }
        &mut self.value
    }

    pub const fn mutation_epoch(&self) -> u64 {
        self.mutation_epoch
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Tracked<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Tracked")
            .field("value", &self.value)
            .field("mutation_epoch", &self.mutation_epoch)
            .finish()
    }
}

impl<T: Clone> Clone for Tracked<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            mutation_epoch: self.mutation_epoch,
            mutation_signal: self.mutation_signal.clone(),
        }
    }
}

impl<T: PartialEq> PartialEq for Tracked<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.mutation_epoch == other.mutation_epoch
    }
}

impl<T: Eq> Eq for Tracked<T> {}

impl<T> std::ops::Deref for Tracked<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.read()
    }
}

impl<T> std::ops::DerefMut for Tracked<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.write()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Tracked, TrackedMutationSignal};

    #[test]
    fn mutable_access_advances_epoch_even_when_value_is_unchanged() {
        let mut value = Tracked::clean(vec![1]);
        assert_eq!(value.mutation_epoch(), 0);
        let _ = value.write();
        assert_eq!(value.mutation_epoch(), 1);
        value.push(2);
        assert_eq!(value.mutation_epoch(), 2);
    }

    #[tokio::test]
    async fn shared_signal_retains_and_coalesces_mutations_before_a_waiter_exists() {
        let signal = TrackedMutationSignal::new();
        let mut first = Tracked::signaled(vec![1], signal.clone());
        let mut second = Tracked::signaled(vec![2], signal.clone());

        first.push(3);
        second.push(4);

        tokio::time::timeout(Duration::from_millis(100), signal.wait_to_queue())
            .await
            .expect("a mutation before waiter registration must retain one wakeup");
        assert!(signal.begin_message());

        first.push(5);
        first.push(6);
        tokio::time::timeout(Duration::from_millis(100), signal.wait_to_queue())
            .await
            .expect("later mutations must produce the next coalesced wakeup");
        assert!(signal.begin_message());
    }

    #[tokio::test]
    async fn suspended_signal_retains_dirty_work_until_resumed() {
        let signal = TrackedMutationSignal::new();
        let mut value = Tracked::signaled(vec![1], signal.clone());
        signal.suspend();
        value.push(2);

        assert!(
            tokio::time::timeout(Duration::from_millis(10), signal.wait_to_queue())
                .await
                .is_err(),
            "in-flight persistence must suppress redundant mailbox messages"
        );

        signal.resume();
        tokio::time::timeout(Duration::from_millis(100), signal.wait_to_queue())
            .await
            .expect("resuming after completion must release retained dirty work");
        assert!(signal.begin_message());
    }
}
