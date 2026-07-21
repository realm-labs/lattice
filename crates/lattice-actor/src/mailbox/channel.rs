use std::future::poll_fn;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::task::{Context, Poll};

use concurrent_queue::ConcurrentQueue;
use event_listener::Event;
use futures_util::task::AtomicWaker;

const CLOSED_BIT: usize = 1 << (usize::BITS - 1);
const AVAILABLE_MASK: usize = !CLOSED_BIT;

pub(crate) fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "actor mailbox capacity must be nonzero");
    assert!(
        capacity < CLOSED_BIT,
        "actor mailbox capacity exceeds the supported range"
    );
    let inner = Arc::new(Inner {
        queue: ConcurrentQueue::bounded(capacity),
        capacity,
        state: AtomicUsize::new(capacity),
        receiver_alive: AtomicBool::new(true),
        sender_count: AtomicUsize::new(1),
        receiver_waiting: AtomicBool::new(false),
        receiver_waker: AtomicWaker::new(),
        capacity_available: Event::new(),
    });
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner },
    )
}

struct Inner<T> {
    queue: ConcurrentQueue<T>,
    capacity: usize,
    state: AtomicUsize,
    receiver_alive: AtomicBool,
    sender_count: AtomicUsize,
    receiver_waiting: AtomicBool,
    receiver_waker: AtomicWaker,
    capacity_available: Event,
}

impl<T> Inner<T> {
    fn try_acquire(&self) -> Result<(), TrySendError<()>> {
        if !self.receiver_alive.load(Ordering::Acquire) {
            return Err(TrySendError::Closed(()));
        }

        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            if state & CLOSED_BIT != 0 {
                return Err(TrySendError::Closed(()));
            }
            let available = state & AVAILABLE_MASK;
            if available == 0 {
                return Err(TrySendError::Full(()));
            }
            match self.state.compare_exchange_weak(
                state,
                state - 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => state = observed,
            }
        }
    }

    fn release_slot(&self) {
        let previous = self.state.fetch_add(1, Ordering::Release);
        debug_assert!(previous & AVAILABLE_MASK < self.capacity);
        if previous & AVAILABLE_MASK == 0 {
            self.wake_capacity_waiters();
        }
    }

    fn wake_capacity_waiters(&self) {
        if self.capacity_available.total_listeners() != 0 {
            self.capacity_available.notify(usize::MAX);
        }
    }

    fn wake_receiver(&self) {
        if self.receiver_waiting.swap(false, Ordering::AcqRel) {
            self.receiver_waker.wake();
        }
    }

    fn is_drained_and_closed(&self) -> bool {
        let state = self.state.load(Ordering::Acquire);
        let closed = state & CLOSED_BIT != 0 || self.sender_count.load(Ordering::Acquire) == 0;
        closed && state & AVAILABLE_MASK == self.capacity
    }
}

pub(crate) struct Sender<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Sender<T> {
    pub(crate) fn try_reserve(&self) -> Result<Permit<'_, T>, TrySendError<()>> {
        self.inner.try_acquire()?;
        Ok(Permit {
            inner: &self.inner,
            active: true,
        })
    }

    pub(crate) async fn reserve(&self) -> Result<Permit<'_, T>, TrySendError<()>> {
        match self.try_reserve() {
            Ok(permit) => return Ok(permit),
            Err(TrySendError::Closed(())) => return Err(TrySendError::Closed(())),
            Err(TrySendError::Full(())) => {}
        }

        loop {
            // Registration is synchronous. A concurrent release must therefore either observe
            // this listener or make the state retry succeed.
            event_listener::listener!(self.inner.capacity_available => capacity_available);
            match self.try_reserve() {
                Ok(permit) => return Ok(permit),
                Err(TrySendError::Closed(())) => return Err(TrySendError::Closed(())),
                Err(TrySendError::Full(())) => capacity_available.await,
            }
        }
    }

    pub(crate) fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let permit = match self.try_reserve() {
            Ok(permit) => permit,
            Err(TrySendError::Full(())) => return Err(TrySendError::Full(value)),
            Err(TrySendError::Closed(())) => return Err(TrySendError::Closed(value)),
        };
        permit.send(value);
        Ok(())
    }

    pub(crate) fn max_capacity(&self) -> usize {
        self.inner.capacity
    }

    pub(crate) fn capacity(&self) -> usize {
        self.inner.state.load(Ordering::Relaxed) & AVAILABLE_MASK
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        let previous = self.inner.sender_count.fetch_add(1, Ordering::Relaxed);
        assert!(
            previous < usize::MAX,
            "actor mailbox sender count overflowed"
        );
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if self.inner.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.wake_capacity_waiters();
            self.inner.wake_receiver();
        }
    }
}

pub(crate) struct Permit<'a, T> {
    inner: &'a Inner<T>,
    active: bool,
}

impl<T> Permit<'_, T> {
    pub(crate) fn send(mut self, value: T) {
        self.active = false;
        if !self.inner.receiver_alive.load(Ordering::Acquire) {
            self.inner.release_slot();
            drop(value);
            return;
        }
        if let Err(error) = self.inner.queue.push(value) {
            self.inner.release_slot();
            drop(error.into_inner());
            return;
        }
        self.inner.wake_receiver();
    }
}

impl<T> Drop for Permit<'_, T> {
    fn drop(&mut self) {
        if self.active {
            self.inner.release_slot();
            self.inner.wake_receiver();
        }
    }
}

pub(crate) struct Receiver<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Receiver<T> {
    pub(crate) fn try_recv(&mut self) -> Result<T, TryRecvError> {
        if let Ok(value) = self.inner.queue.pop() {
            self.inner.release_slot();
            return Ok(value);
        }
        if self.inner.is_drained_and_closed() {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    pub(crate) async fn recv(&mut self) -> Option<T> {
        poll_fn(|context| self.poll_recv(context)).await
    }

    fn poll_recv(&mut self, context: &mut Context<'_>) -> Poll<Option<T>> {
        if let Ok(value) = self.inner.queue.pop() {
            self.inner.release_slot();
            return Poll::Ready(Some(value));
        }
        if self.inner.is_drained_and_closed() {
            return Poll::Ready(None);
        }

        self.inner.receiver_waiting.store(true, Ordering::Release);
        self.inner.receiver_waker.register(context.waker());

        if let Ok(value) = self.inner.queue.pop() {
            self.inner.receiver_waiting.store(false, Ordering::Release);
            self.inner.release_slot();
            return Poll::Ready(Some(value));
        }
        if self.inner.is_drained_and_closed() {
            self.inner.receiver_waiting.store(false, Ordering::Release);
            return Poll::Ready(None);
        }
        Poll::Pending
    }

    pub(crate) fn close(&mut self) {
        self.inner.state.fetch_or(CLOSED_BIT, Ordering::Release);
        self.inner.wake_capacity_waiters();
        self.inner.wake_receiver();
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.inner.state.load(Ordering::Acquire) & CLOSED_BIT != 0
            || self.inner.sender_count.load(Ordering::Acquire) == 0
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.inner.receiver_alive.store(false, Ordering::Release);
        self.inner.state.fetch_or(CLOSED_BIT, Ordering::Release);
        self.inner.queue.close();
        while self.inner.queue.pop().is_ok() {
            self.inner.release_slot();
        }
        self.inner.wake_capacity_waiters();
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TrySendError<T> {
    Full(T),
    Closed(T),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TryRecvError {
    Empty,
    Disconnected,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn permit_enforces_capacity_and_restores_it_when_dropped() {
        let (sender, mut receiver) = channel(1);
        let permit = sender.try_reserve().expect("first permit is available");
        assert!(matches!(sender.try_reserve(), Err(TrySendError::Full(()))));
        drop(permit);
        sender
            .try_send(7)
            .expect("dropped permit restores capacity");
        assert_eq!(receiver.try_recv(), Ok(7));
        assert_eq!(sender.capacity(), 1);
    }

    #[tokio::test]
    async fn waiting_sender_resumes_after_receive() {
        let (sender, mut receiver) = channel(1);
        sender.try_send(1).unwrap();
        let waiting = tokio::spawn({
            let sender = sender.clone();
            async move {
                let permit = sender.reserve().await.unwrap();
                permit.send(2);
            }
        });
        tokio::task::yield_now().await;
        assert_eq!(receiver.recv().await, Some(1));
        waiting.await.unwrap();
        assert_eq!(receiver.recv().await, Some(2));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn capacity_transition_wakes_all_waiting_senders() {
        const WAITERS: usize = 8;

        let (sender, mut receiver) = channel(2);
        sender.try_send(usize::MAX).unwrap();
        sender.try_send(usize::MAX).unwrap();
        let mut waiters = Vec::with_capacity(WAITERS);
        for value in 0..WAITERS {
            let sender = sender.clone();
            waiters.push(tokio::spawn(async move {
                sender.reserve().await.unwrap().send(value);
            }));
        }
        tokio::task::yield_now().await;

        let received = tokio::time::timeout(Duration::from_secs(1), async {
            let mut values = Vec::with_capacity(WAITERS);
            for _ in 0..WAITERS + 2 {
                let value = receiver.recv().await.unwrap();
                if value != usize::MAX {
                    values.push(value);
                }
            }
            values
        })
        .await
        .expect("all capacity waiters wake as slots cycle");
        for waiter in waiters {
            waiter.await.unwrap();
        }
        assert_eq!(received.len(), WAITERS);
    }

    #[tokio::test]
    async fn close_rejects_waiters_but_drains_queued_values() {
        let (sender, mut receiver) = channel(1);
        sender.try_send(1).unwrap();
        let waiting = tokio::spawn({
            let sender = sender.clone();
            async move { sender.reserve().await.map(|_| ()) }
        });
        tokio::task::yield_now().await;
        receiver.close();
        assert_eq!(receiver.recv().await, Some(1));
        assert_eq!(receiver.recv().await, None);
        assert_eq!(waiting.await.unwrap(), Err(TrySendError::Closed(())));
    }

    #[tokio::test]
    async fn close_waits_for_an_already_reserved_slot() {
        let (sender, mut receiver) = channel(1);
        let permit = sender.try_reserve().unwrap();
        receiver.close();
        assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));
        permit.send(7);
        assert_eq!(receiver.recv().await, Some(7));
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn last_sender_wakes_receiver() {
        let (sender, mut receiver) = channel::<u8>(1);
        let waiting = tokio::spawn(async move { receiver.recv().await });
        tokio::time::sleep(Duration::from_millis(1)).await;
        drop(sender);
        assert_eq!(waiting.await.unwrap(), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_senders_deliver_every_value_once() {
        const PRODUCERS: usize = 16;
        const VALUES_PER_PRODUCER: usize = 2_000;

        let (sender, mut receiver) = channel::<usize>(64);
        let consumer = tokio::spawn(async move {
            let mut seen = vec![false; PRODUCERS * VALUES_PER_PRODUCER];
            for _ in 0..seen.len() {
                let value = receiver.recv().await.expect("all values are delivered");
                assert!(!seen[value], "value {value} was delivered twice");
                seen[value] = true;
            }
            seen
        });

        let mut producers = Vec::with_capacity(PRODUCERS);
        for producer in 0..PRODUCERS {
            let sender = sender.clone();
            producers.push(tokio::spawn(async move {
                for offset in 0..VALUES_PER_PRODUCER {
                    let mut value = producer * VALUES_PER_PRODUCER + offset;
                    loop {
                        match sender.try_send(value) {
                            Ok(()) => break,
                            Err(TrySendError::Full(returned)) => {
                                value = returned;
                                tokio::task::yield_now().await;
                            }
                            Err(TrySendError::Closed(_)) => panic!("receiver closed early"),
                        }
                    }
                }
            }));
        }
        for producer in producers {
            producer.await.unwrap();
        }
        let seen = consumer.await.unwrap();
        assert!(seen.into_iter().all(|value| value));
    }
}
